"""跨模块数据链路：把22个独立领域服务串联成10条端到端生产流水线。

每条链路从上游模块取输入数据，做领域校验、转换与状态推进后，把产物写入下游模块：
链路1  流程模板发布 → 排程触发 → 任务队列派发 → 机器人节点执行 → 步骤轨迹
链路2  文件归集校验 → 采集批次确认 → 字段映射生效 → 数据落库 → 交接包消费
链路3  凭据轮换 → 审计轨迹
链路4  异常规则匹配 → 重试策略执行 → 审计轨迹
链路5  元素定位漂移 → 流程执行异常 → 异常规则匹配
链路6  表单字段校验 → 步骤轨迹异常 → 重试恢复
链路7  版本灰度发布 → 流程执行回滚 → 版本绑定切换
链路8  资源额度耗尽 → 机器人节点降级 → 任务队列回收
链路9  OCR识别区域 → 采集批次归集 → 抽检样本 → 交接包驳回
链路10 运行指标汇总 → 执行实例逐条核对 → 审计轨迹

额外能力：水位推进（data_landing watermark）、冲突版本仲裁（version_release rollback =
旧版本胜出）、失败恢复（retry_policy 重试→放弃→审计）、去重（capture_batch 按采集窗
口与上游来源判重，重复批次不落库）、迟到数据（schedule_plan 生效止日已过则拒绝投递）。
"""
from __future__ import annotations

import hashlib
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services import service_for

# ── 通用工具 ──────────────────────────────────────────────────


def _epoch_ms() -> int:
    return int(time.time() * 1000)


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


def _checksum(payload: str) -> str:
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _find_entry(module: str, entry_id: int) -> dict[str, Any]:
    svc = service_for(module)
    if svc is None:
        raise RuntimeError(f"服务未注册: {module}")
    entry = svc.get_entry(entry_id)
    if entry is None:
        raise RuntimeError(f"{module} {entry_id} 不存在")
    return entry


# ── 链路1: 流程模板发布 → 排程触发 → 任务队列派发 → 机器人节点执行 → 步骤轨迹 ─

def publish_flow_and_schedule(template_id: int, plan_payload: dict[str, Any]) -> dict[str, Any]:
    """把流程模板发布，并创建排程计划将任务投递到队列再派发到在线节点执行。"""
    tpl_svc = service_for("flow_template")
    tpl_svc.run_action(template_id, "发布模板")
    template = tpl_svc.get_entry(template_id)

    # 创建排程计划
    plan_svc = service_for("schedule_plan")
    plan_values = {
        "计划编号": plan_payload.get("计划编号", f"SCHE-A{template_id}"),
        "关联模板": template.get("模板编号", ""),
        "触发周期": plan_payload.get("触发周期", "daily"),
        "时区设定": plan_payload.get("时区设定", "Asia/Shanghai"),
        "生效起日": plan_payload.get("生效起日", _now_iso()[:10]),
        "生效止日": plan_payload.get("生效止日", "2099-12-31"),
        "并发上限": plan_payload.get("并发上限", "2"),
        "下次触发": plan_payload.get("下次触发", _now_iso()),
    }
    plan_result, _ = plan_svc.create_entry(plan_values)
    plan_svc.run_action(plan_result["id"], "启效排程")

    # 迟到数据防护：生效止日已过则拒绝投递
    stop_date = plan_values.get("生效止日", "")
    if stop_date < _now_iso()[:10]:
        return {"status": "rejected", "reason": "排程已过期（迟到数据），不投递任务队列"}

    # 投递到任务队列
    queue_svc = service_for("task_queue")
    queue_values = {
        "队列编号": f"TASK-P{template_id}",
        "关联模板": template.get("模板编号", ""),
        "优先级": plan_payload.get("优先级", "high"),
        "期望开始": _now_iso(),
        "重试次数": "0",
        "占用节点": "",
        "等待秒数": "0",
        "入队时间": _now_iso(),
    }
    task_result, _ = queue_svc.create_entry(queue_values)

    # 查找在线机器人节点，执行派发
    node_svc = service_for("robot_node")
    all_nodes = node_svc.list_entries(size=1000)[0]
    online_nodes = [n for n in all_nodes if n.get("status") == "在线"]
    if not online_nodes:
        return {"status": "pending", "task_id": task_result["id"], "reason": "无在线节点，任务排队等待"}

    target_node = online_nodes[0]
    queue_svc.run_action(task_result["id"], "派发任务")

    # 创建流程执行实例
    run_svc = service_for("flow_run")
    run_values = {
        "执行编号": f"RUN-P{template_id}",
        "关联模板": template.get("模板编号", ""),
        "执行节点": target_node.get("节点编号", ""),
        "触发方式": "排程触发",
        "开始时间": _now_iso(),
        "结束时间": "",
        "步骤进度": "0/1",
        "产出记录数": "0",
    }
    run_result, _ = run_svc.create_entry(run_values)
    run_svc.run_action(run_result["id"], "派发执行")

    # 写步骤轨迹
    trace_svc = service_for("step_trace")
    trace_values = {
        "轨迹编号": f"STEP-R{run_result['id']}",
        "关联执行": run_values["执行编号"],
        "步骤序号": "1",
        "步骤名称": "采集步骤-打开页面",
        "输入快照": '{"url":"' + template.get("起始地址", "") + '"}',
        "输出快照": '{"status":"opened"}',
        "耗时毫秒": str(_epoch_ms() % 2000 + 100),
        "退出码": "0",
    }
    trace_result, _ = trace_svc.create_entry(trace_values)

    return {
        "status": "dispatched",
        "template_id": template_id,
        "plan_id": plan_result["id"],
        "task_id": task_result["id"],
        "node": target_node.get("节点编号"),
        "run_id": run_result["id"],
        "trace_id": trace_result["id"],
    }


# ── 链路2: 文件归集校验 → 采集批次确认 → 字段映射生效 → 数据落库 → 交接包消费 ─

# 按上游来源（同一执行的同一归集内容）串行化，保证值班员重试与机器人补报
# 同时发生时也只有一个执行者推进批次，恢复全程幂等。
_batch_locks_guard = threading.Lock()
_batch_locks: dict[str, threading.Lock] = {}


@contextmanager
def _batch_lock(source_key: str):
    with _batch_locks_guard:
        lock = _batch_locks.setdefault(source_key, threading.Lock())
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


def _file_fingerprint(fp: dict[str, Any]) -> str:
    """文件内容指纹：优先采用显式校验值，否则按文件名称+字节大小计算。"""
    explicit = str(fp.get("校验值") or "").strip()
    if explicit:
        return explicit
    return _checksum(f"{fp.get('文件名称', '')}:{fp.get('字节大小', '')}")


def _landing_watermark(batch_code: str) -> int:
    """基于批次生成确定性水位：同一批次恢复得到相同水位，保持单调可复算。"""
    return 1_700_000_000 + int(_checksum(batch_code), 16) % 100_000_000


def _rollback_new_files(file_svc, created_ids: set[int]) -> None:
    """判重拦截时回滚本次调用新建的归集文件，复用文件保持不动。"""
    if created_ids:
        file_svc.remove_entries(created_ids)


def collect_files_and_land_data(run_id: int, file_payloads: list[dict[str, Any]],
                                 mapping_ids: list[int],
                                 fail_point: str | None = None) -> dict[str, Any]:
    """归集流程执行产生的文件，确认采集批次后落库并打包交接。

    全流程按阶段幂等、可恢复：
    - 同一执行重传同一份内容只计入一组归集文件（按内容指纹复用，不新增）；
    - 批次确认后落库中断时，重放按上游来源找回原批次并补齐落库回执，
      不新建批次、不重复落库、不重复签收交接；
    - fail_point="landing" 模拟批次确认后落库中断，用于恢复演练。
    """
    run_entry = _find_entry("flow_run", run_id)
    run_code = run_entry.get("执行编号", "")
    source_key = f"{run_code}:{run_id}"

    with _batch_lock(source_key):
        return _collect_and_land_locked(
            run_id, run_entry, run_code, source_key, file_payloads,
            mapping_ids, fail_point,
        )


def _collect_and_land_locked(run_id, run_entry, run_code, source_key,
                             file_payloads, mapping_ids, fail_point):
    file_svc = service_for("file_drop")
    batch_svc = service_for("capture_batch")
    landing_svc = service_for("data_landing")
    handoff_svc = service_for("handoff_pack")

    # ── 阶段1：幂等文件归集（先登记文件，再判批次，避免判重误增归集文件）──
    batch_payload = "\n".join(fp.get("文件名称", "") for fp in file_payloads)
    batch_checksum = _checksum(batch_payload)
    batch_code = f"CAPT-R{run_id}"
    now_iso = _now_iso()

    fingerprints = [_file_fingerprint(fp) for fp in file_payloads]
    collected: list[dict[str, Any]] = []
    created_ids: set[int] = set()
    reused_files = 0
    for index, (fp, fingerprint) in enumerate(zip(file_payloads, fingerprints), start=1):
        record, reused = file_svc.register_collected(
            run_code=run_code,
            file_payload=fp,
            fingerprint=fingerprint,
            batch_code=batch_code,
            sequence=index,
            now_iso=now_iso,
            run_id=run_id,
        )
        collected.append(record)
        if reused:
            reused_files += 1
        else:
            created_ids.add(record["id"])

    # ── 阶段2：批次确认（已存在则恢复，不新建批次；不同内容仍按来源判重拦截）──
    batch = batch_svc.find_by_source(source_key)
    recovered = batch is not None
    if batch is not None and batch.get("完整性校验") != batch_checksum:
        # 同一执行来源重传了不同内容：判重拦截，并回滚本次预登记的新归集文件
        _rollback_new_files(file_svc, created_ids)
        return {"status": "duplicate", "reason": "采集窗口与上游来源重复，批次已存在"}
    if batch is None:
        batch_values = {
            "批次编号": batch_code,
            "关联执行": run_code,
            "记录行数": str(len(collected)),
            "采集窗口": f"[{now_iso[:10]}]",
            "完整性校验": batch_checksum,
            "批次标签": "file-collect",
            "上游来源": source_key,
            "生成时间": now_iso,
        }
        batch, _ = batch_svc.create_entry(batch_values)
        batch_svc.run_action(batch["id"], "启动校验")
        batch_svc.run_action(batch["id"], "确认批次")

    # 恢复路径：把已归集文件重新挂回原批次，保证批次列表可回查文件关联
    for record in collected:
        if not record.get("归集批次"):
            record["归集批次"] = batch_code

    # ── 阶段3：字段映射生效（重复生效保持幂等）──
    mapping_svc = service_for("field_mapping")
    for mid in mapping_ids:
        mapping = mapping_svc.get_entry(mid)
        if mapping is None:
            continue
        if mapping.get("status") != "试算中":
            mapping_svc.run_action(mid, "提交试算")
        mapping_svc.run_action(mid, "生效映射")

    # ── 阶段4：数据落库（中断可恢复，成功后只产出一份回执）──
    landing = landing_svc.get_by_batch(batch_code)
    receipt = landing_svc.get_success_receipt(batch_code)
    if receipt is None:
        raw_row_count = len(collected) * 10
        conflict_rows = max(0, raw_row_count % 3)  # 模拟冲突行，upsert 后仍成功写入
        watermark = _landing_watermark(batch_code)
        if landing is None:
            landing_values = {
                "落库编号": f"LAND-R{run_id}",
                "关联批次": batch_code,
                "目标存储": "datalake_rpa_collect",
                "写入模式": "upsert",
                "写入行数": str(raw_row_count),
                "冲突行数": str(conflict_rows),
                "落库水位": str(watermark),
                "完成时间": "",
                "落库回执": "",
                "恢复次数": "0",
            }
            landing, _ = landing_svc.create_entry(landing_values)
        else:
            # 中断恢复：累计恢复次数，沿用原落库记录，不新建第二条
            landing_svc.mark_recovered(landing["id"])

        landing_svc.run_action(landing["id"], "启动写入")

        # 演练：批次已确认、落库进行中时中断，尚无成功回执
        if fail_point == "landing":
            return {
                "status": "landing_interrupted",
                "batch_id": batch["id"],
                "landing_id": landing["id"],
                "reason": "落库中断，尚未产生成功回执",
                "recovered": recovered,
            }

        # 冲突行按 upsert 处理后仍完成写入，生成确定性回执（恢复得到同一份）
        landing["冲突行数"] = str(conflict_rows)
        landing["落库水位"] = str(watermark)
        landing["完成时间"] = now_iso
        landing["落库回执"] = landing_svc.build_receipt_no(batch_code)
        landing_svc.run_action(landing["id"], "确认写入")
        receipt = landing_svc.get_success_receipt(batch_code)

    # ── 阶段5：交接包（凭回执交付，清单对账，重复确认不重复签收）──
    handoff = None
    packs = handoff_svc.get_by_batch(batch_code)
    if packs:
        handoff = packs[0]
    else:
        handoff_values = {
            "交接编号": f"HAND-R{run_id}",
            "关联批次": batch_code,
            "目标系统": "datalake_downstream",
            "清单文件数": str(len(collected)),
            "记录行数": str(receipt["written_rows"]),
            "校验摘要": batch_checksum,
            "交接版本": "v1",
            "交接时间": now_iso,
            "落库回执": "",
        }
        handoff, _ = handoff_svc.create_entry(handoff_values)
        handoff_svc.run_action(handoff["id"], "启动打包")

    delivery = handoff_svc.confirm_delivery(handoff["id"])

    return {
        "status": "landed",
        "batch_id": batch["id"],
        "landing_id": landing["id"],
        "handoff_id": handoff["id"],
        "receipt_no": receipt["receipt_no"],
        "watermark": receipt["watermark"],
        "conflict_rows": int(landing.get("冲突行数", "0") or "0"),
        "collected_files": len(collected),
        "reused_files": reused_files,
        "recovered": recovered,
        "delivery": delivery,
    }


# ── 链路3: 凭据轮换 → 审计轨迹 ─

def rotate_credential(cred_id: int, operator: str = "system") -> dict[str, Any]:
    """对临近失效的凭据触发轮换，并写审计轨迹记录轮换事件。"""
    cred_svc = service_for("credential_vault")
    cred = _find_entry("credential_vault", cred_id)

    # 触发轮换
    cred_svc.run_action(cred_id, "触发轮换")
    updated = cred_svc.get_entry(cred_id)

    # 写审计轨迹
    audit_svc = service_for("audit_trail")
    audit_values = {
        "条目编号": f"AUDI-C{cred_id}",
        "操作对象": f"登录凭据.{cred.get('凭据编号', '')}",
        "操作类型": "凭据轮换",
        "操作者标识": operator,
        "操作时间": _now_iso(),
        "变更摘要": f"凭据{cred.get('凭据编号')}轮换：由{cred.get('适用站点')}触发，状态{cred.get('status')}→{updated.get('status')}",
        "关联执行": "",
        "追溯标记": f"cred-rotation-{_epoch_ms()}",
    }
    audit_result, _ = audit_svc.create_entry(audit_values)

    return {
        "status": "rotated",
        "cred_id": cred_id,
        "new_status": updated.get("status"),
        "audit_id": audit_result["id"],
        "trace_mark": audit_values["追溯标记"],
    }


# ── 链路4: 异常规则匹配 → 重试策略执行 → 审计轨迹 ─

def match_exception_and_retry(exception_signature: str, rule_id: int,
                               policy_id: int | None = None) -> dict[str, Any]:
    """匹配异常规则，按关联的重试策略执行重试或放弃，并写入审计轨迹。"""
    rule_svc = service_for("exception_rule")
    rule = _find_entry("exception_rule", rule_id)
    rule_svc.run_action(rule_id, "启效规则")

    # 获取关联重试策略
    retry_svc = service_for("retry_policy")
    if policy_id is None:
        all_policies = retry_svc.list_entries(size=1000)[0]
        matching = [p for p in all_policies if p.get("关联规则") == rule.get("规则编号")]
        if not matching:
            return {"status": "no_policy", "reason": "无关联重试策略，异常未处理"}
        policy = matching[0]
        policy_id = policy["id"]

    policy = _find_entry("retry_policy", policy_id)
    retry_svc.run_action(policy_id, "启用策略")

    # 模拟重试执行：读退避参数
    max_attempts = int(policy.get("最大次数", "3"))
    current_retries = int(policy.get("累计重试", "0"))

    if current_retries >= max_attempts:
        # 达到放弃阈值，标记放弃，写审计
        retry_svc.run_action(policy_id, "标记放弃")
        audit_svc = service_for("audit_trail")
        audit_values = {
            "条目编号": f"AUDI-R{policy_id}",
            "操作对象": f"重试策略.{policy.get('策略编号', '')}",
            "操作类型": "重试放弃",
            "操作者标识": "system",
            "操作时间": _now_iso(),
            "变更摘要": f"异常{exception_signature}经{max_attempts}次重试后放弃，策略标记为已放弃",
            "关联执行": "",
            "追溯标记": f"retry-abandon-{_epoch_ms()}",
        }
        audit_result, _ = audit_svc.create_entry(audit_values)
        return {
            "status": "abandoned",
            "attempts": current_retries + 1,
            "max_attempts": max_attempts,
            "audit_id": audit_result["id"],
        }

    # 仍在重试中，模拟一次重试
    retry_svc.run_action(policy_id, "启用策略")
    audit_svc = service_for("audit_trail")
    audit_values = {
        "条目编号": f"AUDI-R{policy_id}-A{current_retries + 1}",
        "操作对象": f"重试策略.{policy.get('策略编号', '')}",
        "操作类型": "重试执行",
        "操作者标识": "system",
        "操作时间": _now_iso(),
        "变更摘要": f"异常{exception_signature}第{current_retries + 1}/{max_attempts}次重试",
        "关联执行": "",
        "追溯标记": f"retry-attempt-{_epoch_ms()}",
    }
    audit_result, _ = audit_svc.create_entry(audit_values)
    return {
        "status": "retrying",
        "attempt": current_retries + 1,
        "max_attempts": max_attempts,
        "audit_id": audit_result["id"],
    }


# ── 链路5: 元素定位漂移 → 流程执行异常 → 异常规则匹配 ─

def detect_element_drift(locator_id: int, flow_run_id: int) -> dict[str, Any]:
    """检测元素定位漂移，关联流程执行触发异常规则处理。"""
    loc_svc = service_for("element_locator")
    loc = _find_entry("element_locator", locator_id)
    loc_svc.run_action(locator_id, "标记漂移")

    run_entry = _find_entry("flow_run", flow_run_id)

    # 查找匹配的异常规则
    rule_svc = service_for("exception_rule")
    all_rules = rule_svc.list_entries(size=1000)[0]
    matching_rule = None
    for rule in all_rules:
        if rule.get("status") in ("已生效",) and rule.get("适用范围", "").find("定位") >= 0:
            matching_rule = rule
            break

    if matching_rule:
        result = match_exception_and_retry(
            f"元素定位漂移:{loc.get('定位编号')}:flow_run_{flow_run_id}",
            matching_rule["id"]
        )
        result["locator_id"] = locator_id
        result["drifted"] = True
        return result

    return {"status": "drifted_no_rule", "locator_id": locator_id, "drifted": True}


# ── 链路6: 表单字段校验 → 步骤轨迹异常 → 重试恢复 ─

def validate_form_field(field_id: int, trace_id: int) -> dict[str, Any]:
    """校验表单字段取值，校验失败时写入步骤轨迹异常并触发重试策略恢复。"""
    field_svc = service_for("form_field")
    field = _find_entry("form_field", field_id)

    # 检查取值来源是否可用
    value_source = field.get("取值来源", "")
    validation_rule = field.get("校验规则", "")

    # 模拟校验逻辑
    is_valid = len(value_source) > 0 and value_source != field.get("字段名称", "")
    if not is_valid:
        field_svc.run_action(field_id, "重新校验")
        field_svc.run_action(field_id, "重新校验")  # 第二次标记为失败

    if field.get("status") == "校验失败":
        # 写步骤轨迹异常
        trace_svc = service_for("step_trace")
        trace = _find_entry("step_trace", trace_id)
        trace_svc.run_action(trace_id, "标记异常")

        # 触发重试恢复
        rule_svc = service_for("exception_rule")
        all_rules = rule_svc.list_entries(size=1000)[0]
        for rule in all_rules:
            if rule.get("status") == "已生效" and rule.get("适用范围", "").find("字段") >= 0:
                return match_exception_and_retry(
                    f"表单字段校验失败:{field.get('字段编号')}:trace_{trace_id}",
                    rule["id"]
                )

    return {"status": "validated", "field_id": field_id, "is_valid": is_valid}


# ── 链路7: 版本灰度发布 → 流程执行回滚 → 版本绑定切换 ─

def release_with_rollback(version_id: int, target_run_ids: list[int]) -> dict[str, Any]:
    """灰度发布流程模板版本，回滚后执行实例重新绑定旧版本。"""
    ver_svc = service_for("version_release")
    ver = _find_entry("version_release", version_id)

    # 启动灰度
    ver_svc.run_action(version_id, "启动灰度")

    # 冲突版本仲裁：回滚时旧版本胜出
    ver_svc.run_action(version_id, "执行回滚")

    ver = ver_svc.get_entry(version_id)
    if ver.get("status") == "已回滚":
        # 流程执行实例重新绑定旧版本
        run_svc = service_for("flow_run")
        old_version = ver.get("回滚版本", "")
        for rid in target_run_ids:
            run_entry = _find_entry("flow_run", rid)
            run_entry["_bound_version"] = old_version

        return {
            "status": "rolled_back",
            "version_id": version_id,
            "old_version": old_version,
            "affected_runs": len(target_run_ids),
        }

    return {"status": "released", "version_id": version_id}


# ── 链路8: 资源额度耗尽 → 机器人节点降级 → 任务队列回收 ─

def resource_exhaustion_downgrade(resource_id: int) -> dict[str, Any]:
    """资源额度耗尽时降级机器人节点并回收任务队列中的待派发任务。"""
    res_svc = service_for("resource_pool")
    res = _find_entry("resource_pool", resource_id)

    if res.get("status") == "耗尽":
        # 降级关联的在线节点
        node_svc = service_for("robot_node")
        all_nodes = node_svc.list_entries(size=1000)[0]
        affected_nodes = []
        for node in all_nodes:
            if node.get("status") == "在线":
                node_svc.run_action(node["id"], "降级限流")
                affected_nodes.append(node["id"])

        # 回收任务队列中未派发的任务
        queue_svc = service_for("task_queue")
        all_tasks = queue_svc.list_entries(size=1000)[0]
        reclaimed = []
        for task in all_tasks:
            if task.get("status") == "待派发":
                queue_svc.run_action(task["id"], "回收任务")
                reclaimed.append(task["id"])

        return {
            "status": "downgraded",
            "resource_id": resource_id,
            "affected_nodes": affected_nodes,
            "reclaimed_tasks": reclaimed,
        }

    return {"status": "adequate", "resource_id": resource_id}


# ── 链路9: OCR识别区域 → 采集批次 → 抽检样本 → 交接包驳回 ─

def ocr_capture_to_sample_check(ocr_id: int, batch_id: int) -> dict[str, Any]:
    """OCR识别区域置信度偏低时触发采集批次抽检，不合格则驳回交接包。"""
    ocr_svc = service_for("ocr_region")
    ocr = _find_entry("ocr_region", ocr_id)

    if ocr.get("status") != "置信偏低":
        ocr_svc.run_action(ocr_id, "标记低置信")

    batch = _find_entry("capture_batch", batch_id)

    # 抽检样本
    sample_svc = service_for("sample_check")
    sample_values = {
        "样本编号": f"SAMP-B{batch_id}",
        "关联批次": batch.get("批次编号", ""),
        "抽样比例": "0.15",
        "抽样行数": str(max(1, int(batch.get("记录行数", "10")) // 6)),
        "字段通过率": "0.67",
        "缺陷描述": f"OCR区域{ocr.get('区域编号')}置信偏低导致字段误识别",
        "抽检结论": "驳回",
        "抽检时间": _now_iso(),
    }
    sample_result, _ = sample_svc.create_entry(sample_values)
    sample_svc.run_action(sample_result["id"], "启动抽检")
    sample_svc.run_action(sample_result["id"], "驳回样本")

    # 驳回关联交接包
    handoff_svc = service_for("handoff_pack")
    all_packs = handoff_svc.list_entries(size=1000)[0]
    rejected = []
    for pack in all_packs:
        if pack.get("关联批次") == batch.get("批次编号"):
            handoff_svc.run_action(pack["id"], "记录退回")
            rejected.append(pack["id"])

    return {
        "status": "rejected",
        "ocr_id": ocr_id,
        "batch_id": batch_id,
        "sample_id": sample_result["id"],
        "rejected_handoffs": rejected,
    }


# ── 链路10: 运行指标汇总 → 执行实例逐条核对 → 审计轨迹 ─

def summarize_metrics(metric_id: int) -> dict[str, Any]:
    """按统计窗口汇总流程执行指标，逐条核对手实例数据，发现不一致写审计轨迹。"""
    metric_svc = service_for("metric_board")
    metric = _find_entry("metric_board", metric_id)
    metric_svc.run_action(metric_id, "触发汇总")

    # 获取执行实例数据做逐条核对
    run_svc = service_for("flow_run")
    all_runs = run_svc.list_entries(size=10000)[0]
    completed = [r for r in all_runs if r.get("status") == "已完成"]
    failed = [r for r in all_runs if r.get("status") == "执行失败"]

    actual_success_rate = len(completed) / max(1, len(all_runs))
    stated_success_rate = float(metric.get("成功率", "0") or "0")

    # 核对一致性
    tolerance = 0.01
    mismatch = abs(actual_success_rate - stated_success_rate) > tolerance

    if mismatch:
        metric_svc.run_action(metric_id, "核对读数")

        # 写审计轨迹记录不一致
        audit_svc = service_for("audit_trail")
        audit_values = {
            "条目编号": f"AUDI-M{metric_id}",
            "操作对象": f"运行指标.{metric.get('读数编号', '')}",
            "操作类型": "指标核对",
            "操作者标识": "system",
            "操作时间": _now_iso(),
            "变更摘要": (f"指标{metric.get('统计维度')}成功率{stated_success_rate}与执行实例"
                         f"实际{actual_success_rate:.4f}不一致，窗口{metric.get('统计窗口')}"),
            "关联执行": "",
            "追溯标记": f"metric-mismatch-{_epoch_ms()}",
        }
        audit_result, _ = audit_svc.create_entry(audit_values)
    else:
        metric_svc.run_action(metric_id, "核对读数")

    # 写审计轨迹记录汇总事件
    audit_svc = service_for("audit_trail")
    audit_values = {
        "条目编号": f"AUDI-M{metric_id}-SUM",
        "操作对象": f"运行指标.{metric.get('读数编号', '')}",
        "操作类型": "指标汇总",
        "操作者标识": "system",
        "操作时间": _now_iso(),
        "变更摘要": (f"维度{metric.get('统计维度')}窗口{metric.get('统计窗口')}"
                     f"汇总：总执行{len(all_runs)}，成功{len(completed)}，"
                     f"失败{len(failed)}，成功率{actual_success_rate:.4f}"),
        "关联执行": "",
        "追溯标记": f"metric-summary-{_epoch_ms()}",
    }
    audit_result, _ = audit_svc.create_entry(audit_values)

    return {
        "status": "summarized",
        "metric_id": metric_id,
        "total_runs": len(all_runs),
        "success_rate_actual": round(actual_success_rate, 4),
        "mismatch": mismatch,
        "audit_id": audit_result["id"],
    }