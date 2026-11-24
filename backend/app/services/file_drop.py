"""文件归集服务：中间文件收集、校验值索引与目录管理。

归集流程运行过程中下载或生成的中间文件，按执行实例建立
目录与校验值索引，校验不符的文件标记后供排查。

幂等归集：同一次执行重传同一份内容（内容指纹相同）时只复用已有归集
记录，不再新增文件，保证重复落库恢复不会让文件归集页面多出文件。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class FileDropService(BaseModuleService):
    MODULE = "file_drop"
    ENTITY = "归集文件"
    OPERATION_TERM = "存储"
    LIST_FIELDS = [
        "归集编号", "关联执行", "文件名称", "文件类型", "字节大小",
        "校验值", "归集目录", "归集时间", "归集批次", "内容指纹",
    ]
    REQUIRED_FIELDS = ["归集编号", "关联执行", "文件名称"]
    STATUS_ORDER = ["待归集", "已归集", "校验不符", "已清理"]
    ACTION_RULES = {"触发归集": "已归集", "校验文件": "校验不符", "清理归集": "已清理"}
    NEGATIVE_ACTIONS = []

    def verify_file(self, file_id: int, expected_checksum: str) -> dict[str, Any]:
        """校验文件完整性：比对校验值，不符则标记校验不符。"""
        entry = self.get_entry(file_id)
        if entry is None:
            return {"ok": False, "reason": "文件不存在"}
        actual = entry.get("校验值", "")
        if actual != expected_checksum:
            self.run_action(file_id, "校验文件")
            return {"ok": True, "file_id": file_id, "match": False}
        return {"ok": True, "file_id": file_id, "match": True}

    def get_files_by_run(self, run_code: str) -> list[dict[str, Any]]:
        """按关联执行获取归集文件列表。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("关联执行") == run_code]

    def get_files_by_batch(self, batch_code: str) -> list[dict[str, Any]]:
        """按采集批次编号回查该批次关联的全部归集文件。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("归集批次") == batch_code]

    def find_collected(self, run_code: str, fingerprint: str) -> dict[str, Any] | None:
        """按执行编号与内容指纹查找已归集文件，用于重传幂等判重。"""
        for it in self.get_files_by_run(run_code):
            if it.get("内容指纹") == fingerprint:
                return it
        return None

    def register_collected(
        self,
        run_code: str,
        file_payload: dict[str, Any],
        fingerprint: str,
        batch_code: str,
        sequence: int,
        now_iso: str,
        run_id: int,
    ) -> tuple[dict[str, Any], bool]:
        """幂等登记一份归集文件。

        同执行 + 同内容指纹的文件已存在时直接复用原记录（reused=True），
        不新增归集文件；否则新建并触发归集、按校验值标记校验结果。
        返回 (归集记录, 是否复用已有记录)。
        """
        existing = self.find_collected(run_code, fingerprint)
        if existing is not None:
            # 中断恢复时把文件补挂到原批次，关联关系只写一次
            if not existing.get("归集批次"):
                existing["归集批次"] = batch_code
            return existing, True

        checksum = file_payload.get("校验值") or fingerprint
        file_values = {
            "归集编号": file_payload.get("归集编号") or f"FILE-R{run_id}-{sequence}",
            "关联执行": run_code,
            "文件名称": file_payload.get("文件名称", "capture_output.csv"),
            "文件类型": file_payload.get("文件类型", "text/csv"),
            "字节大小": file_payload.get("字节大小", "1024"),
            "校验值": checksum,
            "归集目录": file_payload.get("归集目录", f"/captures/run_{run_id}"),
            "归集时间": now_iso,
            "归集批次": batch_code,
            "内容指纹": fingerprint,
        }
        result, _ = self.create_entry(file_values)
        self.run_action(result["id"], "触发归集")
        # 显式给出的校验值与内容指纹不一致时标记校验不符
        if file_payload.get("校验值") and file_payload["校验值"] != fingerprint:
            self.run_action(result["id"], "校验文件")
        return result, False

    def remove_entries(self, entry_ids: set[int]) -> int:
        """按 id 删除归集记录（仅用于判重拦截时回滚本次预登记的新文件）。"""
        from app.store import store
        rows = store.rows(self.MODULE)
        before = len(rows)
        rows[:] = [row for row in rows if row.get("id") not in entry_ids]
        return before - len(rows)
