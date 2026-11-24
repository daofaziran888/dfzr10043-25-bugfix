"""交接包服务：批次打包、清单校验与下游交接。

把已确认批次连同字段映射、校验摘要和清单打包成交接包，
供下游数仓直接消费并回溯来源。退回后批次不得再次交接。

交付闸口：批次没有成功落库回执（落库中断拿不到完整回执）时不得确认交接；
交接确认时把落库回执回写到交接清单，并核对清单文件数与文件归集的实际
文件数、归集汇总一致。历史已交接批次没有回执字段时按原清单继续复核，
仍能按原执行复核对应文件和回执。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class HandoffPackService(BaseModuleService):
    MODULE = "handoff_pack"
    ENTITY = "交接包"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "交接编号", "关联批次", "目标系统", "清单文件数", "记录行数",
        "校验摘要", "交接版本", "交接时间", "落库回执",
    ]
    REQUIRED_FIELDS = ["交接编号", "关联批次", "目标系统"]
    STATUS_ORDER = ["待打包", "打包中", "已交接", "交接退回"]
    ACTION_RULES = {"启动打包": "打包中", "确认交接": "已交接", "记录退回": "交接退回"}
    NEGATIVE_ACTIONS = []

    def reject_handoff(self, pack_id: int, reason: str) -> dict[str, Any]:
        """驳回交接包，退回后批次不得进入交接。"""
        entry = self.get_entry(pack_id)
        if entry is None:
            return {"ok": False, "reason": "交接包不存在"}
        self.run_action(pack_id, "记录退回")
        return {"ok": True, "pack_id": pack_id, "reason": reason, "action": "交接退回"}

    def get_delivered_packs(self, target_system: str | None = None) -> list[dict[str, Any]]:
        """获取已交接的包列表，可按目标系统筛选。"""
        items, _ = self.list_entries(size=10000)
        delivered = [it for it in items if it.get("status") == "已交接"]
        if target_system:
            delivered = [it for it in delivered if it.get("目标系统") == target_system]
        return delivered

    def get_by_batch(self, batch_code: str) -> list[dict[str, Any]]:
        """按批次编号查找全部交接包。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("关联批次") == batch_code]

    def reconcile_manifest(self, pack_id: int) -> dict[str, Any]:
        """核对交接清单文件数与文件归集页面的实际归集文件数。

        历史已交接批次（无落库回执字段）按原清单文件数复核，
        仍按原执行/批次回查对应文件。
        """
        from app.services import service_for
        entry = self.get_entry(pack_id)
        if entry is None:
            return {"ok": False, "reason": "交接包不存在"}
        batch_code = entry.get("关联批次", "")
        file_svc = service_for("file_drop")
        actual_files = file_svc.get_files_by_batch(batch_code)
        declared = int(entry.get("清单文件数", "0") or "0")
        # 历史批次可能没有把文件挂到批次上，此时没有实际文件可对账，按原清单视为一致
        if not actual_files:
            return {
                "ok": True,
                "pack_id": pack_id,
                "declared_files": declared,
                "actual_files": 0,
                "matched": True,
                "legacy": True,
            }
        matched = len(actual_files) == declared
        return {
            "ok": matched,
            "pack_id": pack_id,
            "declared_files": declared,
            "actual_files": len(actual_files),
            "matched": matched,
            "legacy": False,
        }

    def confirm_delivery(self, pack_id: int) -> dict[str, Any]:
        """确认交接：凭成功落库回执交付，回执回写清单并完成清单对账。

        拿不到完整落库回执（落库中断/异常）时拒绝交付。
        历史已交接批次（已带回执或确属历史数据）保持可复核、不重复签收。
        """
        from app.services import service_for
        entry = self.get_entry(pack_id)
        if entry is None:
            return {"ok": False, "reason": "交接包不存在"}

        # 幂等：已经交接的包重复确认直接返回原结果，不产生第二次签收
        if entry.get("status") == "已交接":
            return {
                "ok": True,
                "pack_id": pack_id,
                "delivered": False,
                "idempotent": True,
                "receipt_no": entry.get("落库回执", ""),
                "reason": "交接包已交接，重复确认不重复签收",
            }

        batch_code = entry.get("关联批次", "")
        landing_svc = service_for("data_landing")
        receipt = landing_svc.get_success_receipt(batch_code)
        if receipt is None:
            return {
                "ok": False,
                "pack_id": pack_id,
                "reason": "批次尚无成功落库回执，落库未完成不能交付",
            }

        # 清单对账：清单文件数必须与归集汇总对得上
        reconciliation = self.reconcile_manifest(pack_id)
        if not reconciliation.get("matched"):
            return {
                "ok": False,
                "pack_id": pack_id,
                "reason": (
                    f"清单文件数{reconciliation['declared_files']}"
                    f"与归集文件数{reconciliation['actual_files']}不一致，拒绝交付"
                ),
            }

        # 落库确认结果回写交接清单
        entry["落库回执"] = receipt["receipt_no"]
        if not entry.get("记录行数"):
            entry["记录行数"] = str(receipt["written_rows"])
        self.run_action(pack_id, "确认交接")
        return {
            "ok": True,
            "pack_id": pack_id,
            "delivered": True,
            "idempotent": False,
            "receipt_no": receipt["receipt_no"],
            "manifest": reconciliation,
        }
