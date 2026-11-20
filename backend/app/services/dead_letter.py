"""死信处理业务规则：状态流转、字段校验、筛选口径与血缘边修复核对。

协作约定：
- 死信重放前必须核对流式血缘的血缘边修复记录：重放目标转换节点若已停用、或已被
  已完成修复项列为失效边来源，则拒绝「标记重放」，避免向失效节点重放。
- 存在替代来源时走「改投重放」：重放目标切换为修复项里的替代转换节点，跨模块
  交接时保留来源批次（source_batch）。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.stream_lineage import StreamLineageService

MODULE = "dead_letter"
LIST_FIELDS = ["记录编号", "原始主题", "原始分区", "失败原因", "原始消息体", "重试次数", "入库时间", "处置状态"]
REQUIRED_FIELDS = ["记录编号", "原始主题", "原始分区"]
STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃", "改投重放": "已重放"}
NEGATIVE_ACTIONS = []


class DeadLetterService:
    def __init__(self) -> None:
        self.lineage_service = StreamLineageService()

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("记录编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        # 跨模块交接字段：重放目标节点与来源批次，允许登记时一并带入
        entry["重放目标节点"] = values.get("重放目标节点")
        entry["来源批次"] = values.get("来源批次")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"死信记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于死信处理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        if action == "标记重放":
            blocked = self._check_replay_target(entry)
            if blocked:
                return None, blocked

        if action == "改投重放":
            message = self._redirect_replay(entry)
            if message:
                return None, message

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"死信记录已{action}"

    # ───────────────────────────────
    # 与流式血缘围绕血缘边修复记录协作
    # ───────────────────────────────

    def _check_replay_target(self, entry: dict[str, Any]) -> str | None:
        """重放前核对：目标转换节点停用或被修复记录撤下时拦截，返回拦截原因。"""
        target = entry.get("重放目标节点")
        if not target:
            return None
        # 优先核对血缘边修复记录：它带有替代来源，拦截信息可直接指导改投
        repair = self.lineage_service.find_repair_for_transform(str(target))
        if repair is not None:
            replacement = repair.get("replacement_edge", {})
            return (f"重放目标 {target} 已被修复批次 {repair.get('batch_id')} 列为失效边来源，"
                    f"替代来源为 {replacement.get('transform_node')}，请使用「改投重放」")
        if self.lineage_service.is_transform_deactivated(str(target)):
            return (f"重放目标转换节点 {target} 已停用，禁止向失效节点重放；"
                    "请核对血缘边修复记录后改投替代来源")
        return None

    def _redirect_replay(self, entry: dict[str, Any]) -> str | None:
        """按血缘边修复记录把重放目标切换为替代转换节点，保留来源批次。"""
        target = entry.get("重放目标节点")
        repair = self.lineage_service.find_repair_for_transform(str(target)) if target else None
        if repair is None:
            # 没有失效边修复记录时，改投没有依据
            if target and self.lineage_service.is_transform_deactivated(str(target)):
                return f"目标 {target} 已停用，但暂无血缘边修复记录提供替代来源，无法改投"
            return "未找到对应的血缘边修复记录，缺少替代来源，改投重放没有依据"
        replacement = repair.get("replacement_edge", {})
        entry["原重放目标节点"] = target
        entry["重放目标节点"] = replacement.get("transform_node")
        entry["改投依据批次"] = repair.get("batch_id")
        # 跨模块交接保留来源批次：修复项没有则沿用死信自身批次
        entry["来源批次"] = repair.get("source_batch") or entry.get("来源批次")
        entry["替代血缘记录"] = replacement.get("record_code")
        return None

    def replay_cross_check(self) -> dict[str, Any]:
        """死信处理工作台核对口径：列出每条待处置记录的重放目标是否仍有效。"""
        results: list[dict[str, Any]] = []
        for entry in store.rows(MODULE):
            target = entry.get("重放目标节点")
            if not target:
                continue
            repair = self.lineage_service.find_repair_for_transform(str(target))
            deactivated = self.lineage_service.is_transform_deactivated(str(target))
            results.append({
                "dead_letter_id": entry.get("id"),
                "record_code": entry.get("记录编号"),
                "replay_target": target,
                "source_batch": entry.get("来源批次"),
                "target_deactivated": deactivated,
                "blocked": bool(deactivated or repair is not None),
                "repair_batch_id": repair.get("batch_id") if repair else None,
                "replacement_node": (repair or {}).get("replacement_edge", {}).get("transform_node"),
            })
        blocked_count = sum(1 for r in results if r["blocked"])
        return {"items": results, "total": len(results), "blocked": blocked_count}
