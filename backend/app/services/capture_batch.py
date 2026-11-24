"""采集批次服务：执行产出归集、完整性校验、窗口去重和批次确认。

一次或多次流程执行的产出记录归集成批次，附带采集窗口和
完整性校验值供下游引用。去重逻辑按采集窗口+上游来源判重。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


class CaptureBatchService(BaseModuleService):
    MODULE = "capture_batch"
    ENTITY = "采集批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "批次编号", "关联执行", "记录行数", "采集窗口", "完整性校验",
        "批次标签", "上游来源", "生成时间",
    ]
    REQUIRED_FIELDS = ["批次编号", "关联执行", "记录行数"]
    STATUS_ORDER = ["归集中", "校验中", "已确认", "校验失败"]
    ACTION_RULES = {"启动校验": "校验中", "确认批次": "已确认", "标记失败": "校验失败"}
    NEGATIVE_ACTIONS = []

    def find_duplicate(self, checksum: str, source: str) -> dict[str, Any] | None:
        """按完整性校验值和上游来源查找重复批次。"""
        items, _ = self.list_entries(size=10000)
        for it in items:
            if it.get("完整性校验") == checksum and it.get("上游来源") == source:
                return it
        return None

    def find_by_source(self, source: str) -> dict[str, Any] | None:
        """按上游来源（执行实例判重键）找回批次，落库中断恢复时复用原批次。"""
        items, _ = self.list_entries(size=10000)
        for it in items:
            if it.get("上游来源") == source:
                return it
        return None

    def list_batch_files(self, batch_id: int) -> list[dict[str, Any]]:
        """回查批次关联的归集文件，批次确认后仍可追溯文件关联。"""
        entry = self.get_entry(batch_id)
        if entry is None:
            return []
        from app.services import service_for
        file_svc = service_for("file_drop")
        return file_svc.get_files_by_batch(entry.get("批次编号", ""))

    def get_confirmed_batches(self) -> list[dict[str, Any]]:
        """获取所有已确认的采集批次，供下游交接打包。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("status") == "已确认"]

    def verify_integrity(self, batch_id: int) -> dict[str, Any]:
        """启动校验并返回校验结果。"""
        entry = self.get_entry(batch_id)
        if entry is None:
            return {"ok": False, "reason": "批次不存在"}
        self.run_action(batch_id, "启动校验")
        row_count = int(entry.get("记录行数", "0") or "0")
        if row_count <= 0:
            self.run_action(batch_id, "标记失败")
            return {"ok": False, "reason": "批次记录行数为0，校验失败"}
        self.run_action(batch_id, "确认批次")
        return {"ok": True, "batch_id": batch_id, "row_count": row_count}