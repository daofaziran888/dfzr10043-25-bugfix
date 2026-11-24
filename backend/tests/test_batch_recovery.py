"""批次落库中断恢复与幂等重传测试。

场景：用三份不同文件发起归集，确认批次后故意中断落库；
批次列表仍能回查文件关联，再同时重放两次。
断言：文件归集只保留三条文件、只有一份成功落库回执、
落库成功后才能确认交接、两笔重放不能各自签收。
"""
from __future__ import annotations

import threading

import pytest

from app.pipeline import collect_files_and_land_data
from app.services import service_for


THREE_FILES = [
    {"文件名称": "alpha.csv", "文件类型": "text/csv", "字节大小": "1024",
     "校验值": "check-alpha-001"},
    {"文件名称": "beta.csv", "文件类型": "text/csv", "字节大小": "2048",
     "校验值": "check-beta-002"},
    {"文件名称": "gamma.csv", "文件类型": "text/csv", "字节大小": "4096",
     "校验值": "check-gamma-003"},
]


@pytest.fixture()
def run_id():
    run_svc = service_for("flow_run")
    runs = run_svc.list_entries(size=1000)[0]
    completed = next((r for r in runs if r.get("status") == "已完成"), runs[0])
    return completed["id"]


class TestBatchLandingRecovery:
    """批次确认后落库中断 → 重放恢复的完整链路。"""

    def test_interrupted_landing_has_no_receipt_and_no_delivery(self, run_id):
        result = collect_files_and_land_data(run_id, THREE_FILES, [],
                                             fail_point="landing")
        assert result["status"] == "landing_interrupted"

        batch_svc = service_for("capture_batch")
        batch = batch_svc.get_entry(result["batch_id"])
        assert batch["status"] == "已确认"  # 批次仍存在且已确认

        landing_svc = service_for("data_landing")
        landing = landing_svc.get_entry(result["landing_id"])
        assert landing["status"] == "写入中"  # 中断在写入中
        assert landing_svc.get_success_receipt(batch["批次编号"]) is None

        # 没有成功回执时不得交接
        handoff_svc = service_for("handoff_pack")
        assert handoff_svc.get_by_batch(batch["批次编号"]) == []

    def test_batch_list_still_lists_three_file_associations(self, run_id):
        result = collect_files_and_land_data(run_id, THREE_FILES, [],
                                             fail_point="landing")
        batch_svc = service_for("capture_batch")
        files = batch_svc.list_batch_files(result["batch_id"])
        assert len(files) == 3
        assert {f["文件名称"] for f in files} == {"alpha.csv", "beta.csv", "gamma.csv"}
        # 文件归集页面按执行回查同样是三条
        run_code = service_for("flow_run").get_entry(run_id)["执行编号"]
        run_files = service_for("file_drop").get_files_by_run(run_code)
        assert len(run_files) == 3

    def test_concurrent_double_replay_single_receipt_single_delivery(self, run_id):
        interrupted = collect_files_and_land_data(run_id, THREE_FILES, [],
                                                  fail_point="landing")
        batch_svc = service_for("capture_batch")
        batch = batch_svc.get_entry(interrupted["batch_id"])
        batch_code = batch["批次编号"]

        outcomes: list[dict] = []
        errors: list[Exception] = []

        def replay():
            try:
                outcomes.append(
                    collect_files_and_land_data(run_id, THREE_FILES, []))
            except Exception as exc:  # pragma: no cover - 并发不应抛错
                errors.append(exc)

        t1 = threading.Thread(target=replay)
        t2 = threading.Thread(target=replay)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert errors == []
        assert len(outcomes) == 2
        assert all(o["status"] == "landed" for o in outcomes)

        # 两笔重放收敛到同一批次、同一落库记录、同一回执、同一交接包
        assert {o["batch_id"] for o in outcomes} == {interrupted["batch_id"]}
        assert {o["landing_id"] for o in outcomes} == {interrupted["landing_id"]}
        assert len({o["handoff_id"] for o in outcomes}) == 1
        receipt_nos = {o["receipt_no"] for o in outcomes}
        assert len(receipt_nos) == 1

        landing_svc = service_for("data_landing")
        # 全库该批次只有一条落库记录，且只持有一份可用回执
        landings = [
            row for row in landing_svc.list_entries(size=10000)[0]
            if row.get("关联批次") == batch_code
        ]
        assert len(landings) == 1
        receipt = landing_svc.get_success_receipt(batch_code)
        assert receipt is not None
        assert receipt["receipt_no"] == receipt_nos.pop()

        # 归集文件始终只有三条，重放没有制造重复文件
        run_code = service_for("flow_run").get_entry(run_id)["执行编号"]
        files = service_for("file_drop").get_files_by_run(run_code)
        assert len(files) == 3

        # 交接包只有一个、已交接，且只签收一次
        handoff_svc = service_for("handoff_pack")
        packs = handoff_svc.get_by_batch(batch_code)
        assert len(packs) == 1
        pack = packs[0]
        assert pack["status"] == "已交接"
        assert pack["落库回执"] == receipt["receipt_no"]
        # 两笔重放中只有一笔真正执行签收，另一笔幂等返回
        deliveries = [o["delivery"] for o in outcomes]
        assert sum(1 for d in deliveries if d.get("delivered")) == 1
        assert sum(1 for d in deliveries if d.get("idempotent")) == 1

    def test_manifest_matches_aggregated_file_count(self, run_id):
        collect_files_and_land_data(run_id, THREE_FILES, [],
                                    fail_point="landing")
        recovered = collect_files_and_land_data(run_id, THREE_FILES, [])
        handoff_svc = service_for("handoff_pack")
        pack = handoff_svc.get_entry(recovered["handoff_id"])
        reconciliation = handoff_svc.reconcile_manifest(pack["id"])
        assert reconciliation["matched"] is True
        assert reconciliation["declared_files"] == 3
        assert reconciliation["actual_files"] == 3
        assert int(pack["清单文件数"]) == 3

    def test_repeated_replay_keeps_single_file_group(self, run_id):
        collect_files_and_land_data(run_id, THREE_FILES, [])
        collect_files_and_land_data(run_id, THREE_FILES, [])
        third = collect_files_and_land_data(run_id, THREE_FILES, [])
        # 后两次全部命中已有归集文件，没有新增
        assert third["reused_files"] == 3
        run_code = service_for("flow_run").get_entry(run_id)["执行编号"]
        assert len(service_for("file_drop").get_files_by_run(run_code)) == 3


class TestHandoffGateAndLegacyCompatibility:
    """交付闸口与历史已交接批次兼容性。"""

    def test_delivery_blocked_without_success_receipt(self, run_id):
        collect_files_and_land_data(run_id, THREE_FILES, [],
                                    fail_point="landing")
        batch_code = f"CAPT-R{run_id}"
        handoff_svc = service_for("handoff_pack")
        # 手工建一个待打包交接包，模拟落库未完成时尝试交付
        pack, _ = handoff_svc.create_entry({
            "交接编号": f"HAND-MANUAL-{run_id}",
            "关联批次": batch_code,
            "目标系统": "datalake_downstream",
            "清单文件数": "3",
            "记录行数": "30",
            "校验摘要": "manual",
            "交接版本": "v1",
            "交接时间": "",
            "落库回执": "",
        })
        result = handoff_svc.confirm_delivery(pack["id"])
        assert result["ok"] is False
        assert "落库回执" in result["reason"]
        assert handoff_svc.get_entry(pack["id"])["status"] == "待打包"

    def test_legacy_delivered_pack_remains_reviewable(self):
        """历史已交接批次（无回执字段、文件未挂批次）仍可按原清单复核。"""
        handoff_svc = service_for("handoff_pack")
        legacy_pack = next(
            p for p in handoff_svc.list_entries(size=1000)[0]
            if p.get("status") == "已交接"
        )
        reconciliation = handoff_svc.reconcile_manifest(legacy_pack["id"])
        assert reconciliation["ok"] is True
        assert reconciliation["legacy"] is True
        # 重复确认历史包不产生第二次签收
        again = handoff_svc.confirm_delivery(legacy_pack["id"])
        assert again["ok"] is True
        assert again["idempotent"] is True
        assert again["delivered"] is False

    def test_manifest_mismatch_blocks_delivery(self, run_id):
        """清单文件数与归集汇总对不上时拒绝交付。"""
        # 先完成落库拿到成功回执，再单独验证清单对账闸口
        collect_files_and_land_data(run_id, THREE_FILES, [])
        batch_code = f"CAPT-R{run_id}"
        handoff_svc = service_for("handoff_pack")
        # 移除正常交接包，换成清单文件数错误的包
        for pack in handoff_svc.get_by_batch(batch_code):
            handoff_svc.list_entries(size=10000)[0].remove(pack)
        pack, _ = handoff_svc.create_entry({
            "交接编号": f"HAND-MISMATCH-{run_id}",
            "关联批次": batch_code,
            "目标系统": "datalake_downstream",
            "清单文件数": "99",  # 与实际三条归集文件不符
            "记录行数": "30",
            "校验摘要": "manual",
            "交接版本": "v1",
            "交接时间": "",
            "落库回执": "",
        })
        result = handoff_svc.confirm_delivery(pack["id"])
        assert result["ok"] is False
        assert "不一致" in result["reason"]
        assert handoff_svc.get_entry(pack["id"])["status"] == "待打包"
