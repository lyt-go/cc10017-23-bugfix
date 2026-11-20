"""流式血缘 DAG 图服务：节点/边构建、血缘边修复记录与流转规则。

设计要点：
- 图结构按「上游主题 → 转换节点 → 下游表名」三段式构建，而不是把每条血缘记录
  当成孤立节点后按行序机械连边。
- 转换节点下线（血缘记录「已停用」）后，相关入边必须从有效边集移除；被修复流程
  撤下的失效边统一收敛到 lineage_edge_repairs 产物表，查询时只出现在失效边列表，
  不再进入有效图，保证历史查询仍可凭修复记录解释来源。
- 血缘边修复记录同时保留失效边快照（旧边版本、旧链路版本）与替代来源，生成过程
  幂等（同一失效边键不重复建项），支持中断后续跑补齐。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "stream_lineage"
REPAIR_MODULE = "lineage_edge_repairs"
REQUIRED_FIELDS = ["记录编号", "上游主题", "下游表名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用记录": "已停用"}
NEGATIVE_ACTIONS = ["停用记录"]
DEACTIVATED_STATUS = "已停用"

# 修复批次/修复项状态
REPAIR_ITEM_STATUSES = ["待处理", "处理中", "已完成", "已跳过"]
REPAIR_BATCH_STATUSES = ["进行中", "已完成"]
# 同一查询窗口内撤旧边与挂新边的先后取舍
REPAIR_POLICIES = ["先撤旧边", "先挂新边"]
DEFAULT_POLICY = "先撤旧边"


def csv_cell(value: Any) -> str:
    """CSV 单元格防注入：公式字符前加单引号；引号交给 csv.writer 统一处理。"""
    text = "" if value is None else str(value)
    if text and text[0] in "=+-@":
        text = "'" + text
    return text


def edge_key(upstream: str, transform: str, downstream: str) -> str:
    """失效边的唯一边界：同主题、同转换节点、同目标表即同一条边。

    重复判定的边界由「覆盖旧边会让历史查询无法解释」给出——只要失效边键相同，
    就不允许再生成第二条修复项，避免重复修复把数据版本顶乱。
    """
    return f"{upstream}>>{transform}>>{downstream}"


def now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


class StreamLineageService:
    # ───────────────────────────────
    # 图构建：下线节点不再保留入边
    # ───────────────────────────────

    def _inactive_edge_keys(self) -> set[str]:
        """汇总已完成修复项撤下的失效边键（恢复旧记录前的统一判定口径）。"""
        keys: set[str] = set()
        for item in store.rows(REPAIR_MODULE):
            if item.get("repair_status") == "已完成":
                keys.add(str(item.get("invalid_edge_key", "")))
        return keys

    def get_graph(self) -> dict[str, Any]:
        """返回有效图与失效边视图。

        - nodes：主题节点、转换节点、下游表节点三类，停用的转换节点仍保留
          （供流式血缘页定位下线节点），但带 status=已停用。
        - edges：只包含有效边。来源记录已停用、或已被修复流程撤下的边一律剔除，
          下游查询不会再把停用转换当作有效来源。
        - invalid_edges：被剔除的失效边及其剔除原因，页面以虚线展示。
        """
        rows = store.rows(MODULE)
        inactive_keys = self._inactive_edge_keys()

        nodes: list[dict[str, Any]] = []
        topic_seen: set[str] = set()
        table_seen: set[str] = set()
        edges: list[dict[str, Any]] = []
        invalid_edges: list[dict[str, Any]] = []

        def add_topic(name: str, status: str) -> None:
            if name in topic_seen:
                return
            topic_seen.add(name)
            nodes.append({"id": f"topic::{name}", "name": name, "type": "topic",
                          "status": status, "label": name})

        def add_table(name: str) -> None:
            if name in table_seen:
                return
            table_seen.add(name)
            nodes.append({"id": f"table::{name}", "name": name, "type": "table",
                          "status": "已就绪", "label": name})

        for i, row in enumerate(rows):
            upstream = str(row.get("上游主题", ""))
            transform = str(row.get("转换节点", ""))
            downstream = str(row.get("下游表名", ""))
            status = str(row.get("status", STATUS_ORDER[0]))
            topic_status = self._topic_status(upstream)

            add_topic(upstream, topic_status)
            add_table(downstream)
            transform_node_id = f"transform::{transform}"
            if not any(n["id"] == transform_node_id for n in nodes):
                nodes.append({
                    "id": transform_node_id, "name": transform, "type": "transform",
                    "status": status, "label": transform, "lineage_id": int(row.get("id", i + 1)),
                    "记录编号": row.get("记录编号"), "链路版本": row.get("链路版本"),
                })

            key = edge_key(upstream, transform, downstream)
            base_pair = [
                {"from": f"topic::{upstream}", "to": transform_node_id},
                {"from": transform_node_id, "to": f"table::{downstream}"},
            ]
            if status == DEACTIVATED_STATUS:
                for pair in base_pair:
                    invalid_edges.append({**pair, "label": "依赖", "invalid_edge_key": key,
                                          "invalid_reason": "转换节点已停用",
                                          "lineage_id": int(row.get("id", i + 1)),
                                          "链路版本": row.get("链路版本")})
                continue
            if key in inactive_keys:
                for pair in base_pair:
                    invalid_edges.append({**pair, "label": "依赖", "invalid_edge_key": key,
                                          "invalid_reason": "失效边已由血缘边修复记录撤下",
                                          "lineage_id": int(row.get("id", i + 1)),
                                          "链路版本": row.get("链路版本")})
                continue

            for pair in base_pair:
                edges.append({**pair, "label": "依赖", "invalid_edge_key": key,
                              "lineage_id": int(row.get("id", i + 1)),
                              "转换节点": transform, "链路版本": row.get("链路版本")})

        return {"nodes": nodes, "edges": edges, "invalid_edges": invalid_edges}

    def _topic_status(self, topic_code: str) -> str:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return str(row.get("status", ""))
        return "未注册"

    # ───────────────────────────────
    # 血缘记录维护（沿用原有状态机）
    # ───────────────────────────────

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"血缘记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式血缘可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"血缘记录已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    # ───────────────────────────────
    # 血缘边修复记录：生成、续跑、查询
    # ───────────────────────────────

    def list_repairs(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(REPAIR_MODULE)
        if batch_id:
            rows = [r for r in rows if r.get("batch_id") == batch_id]
        return rows

    def get_repair_batch(self, batch_id: str) -> dict[str, Any] | None:
        for row in store.rows(REPAIR_MODULE):
            if row.get("batch_id") == batch_id and row.get("batch_header"):
                return row
        return None

    def generate_repairs(
        self,
        *,
        topic_code: str | None = None,
        target_table: str | None = None,
        policy: str = DEFAULT_POLICY,
        source_batch: str | None = None,
        max_items: int | None = None,
    ) -> dict[str, Any]:
        """读取订单主题（或指定范围）血缘记录，生成血缘边修复记录。

        对每个下游表：停用记录对应的边是失效边，同表最新的「已生效」记录是替代
        来源；修复项保留失效边快照与替代边来源，同一失效边键只建一次（幂等）。
        max_items 用于模拟处理中断：本批次只落前 N 项，剩余项靠 resume 补齐。
        """
        if policy not in REPAIR_POLICIES:
            return {"ok": False, "message": f"修复策略须为 {REPAIR_POLICIES} 之一"}

        rows = store.rows(MODULE)
        if topic_code:
            rows = [r for r in rows if r.get("上游主题") == topic_code]
        if target_table:
            rows = [r for r in rows if r.get("下游表名") == target_table]

        # 按目标表分组，识别失效边与同表替代来源
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row.get("下游表名", "")), []).append(row)

        existing_keys = {str(r.get("invalid_edge_key", "")) for r in store.rows(REPAIR_MODULE)}
        candidates: list[dict[str, Any]] = []
        skipped_no_replacement: list[str] = []

        for table, table_rows in grouped.items():
            active = [r for r in table_rows if r.get("status") != DEACTIVATED_STATUS]
            inactive = [r for r in table_rows if r.get("status") == DEACTIVATED_STATUS]
            if not inactive:
                continue
            replacement = max(active, key=lambda r: int(r.get("id", 0)), default=None)
            for old in inactive:
                key = edge_key(str(old.get("上游主题", "")),
                               str(old.get("转换节点", "")), table)
                if key in existing_keys:
                    continue
                if replacement is None:
                    skipped_no_replacement.append(key)
                    continue
                candidates.append(self._build_repair_item(old, replacement, policy, source_batch))

        if skipped_no_replacement and not candidates:
            return {"ok": False,
                    "message": f"失效边 {('、'.join(skipped_no_replacement))} 缺少同表替代来源，暂不生成修复记录"}
        if not candidates:
            return {"ok": False, "message": "没有需要修复的失效边（可能已生成过修复记录）"}

        planned = candidates if max_items is None else candidates[: max(0, max_items)]
        interrupted = len(planned) < len(candidates)
        batch_id = self._next_batch_id()
        header = self._write_batch(batch_id, policy, source_batch, planned,
                                   total_candidates=len(candidates), interrupted=interrupted)
        return {"ok": True, "batch_id": batch_id, "batch": header,
                "created": len(planned), "remaining": len(candidates) - len(planned),
                "interrupted": interrupted,
                "skipped_without_replacement": skipped_no_replacement}

    def resume_repairs(self, batch_id: str, *, max_items: int | None = None) -> dict[str, Any]:
        """中断后续跑：补齐同批次后续修复项，已落库的失效边键不重复生成。"""
        header = self.get_repair_batch(batch_id)
        if header is None:
            return {"ok": False, "message": f"修复批次 {batch_id} 不存在"}
        if header.get("batch_status") == "已完成":
            return {"ok": False, "message": f"修复批次 {batch_id} 已完成，无需续跑"}

        topic_code = header.get("topic_code")
        target_table = header.get("target_table")
        policy = str(header.get("policy", DEFAULT_POLICY))
        source_batch = header.get("source_batch")

        rows = store.rows(MODULE)
        if topic_code:
            rows = [r for r in rows if r.get("上游主题") == topic_code]
        if target_table:
            rows = [r for r in rows if r.get("下游表名") == target_table]

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row.get("下游表名", "")), []).append(row)

        existing_keys = {str(r.get("invalid_edge_key", "")) for r in store.rows(REPAIR_MODULE)}
        pending: list[dict[str, Any]] = []
        for table, table_rows in grouped.items():
            active = [r for r in table_rows if r.get("status") != DEACTIVATED_STATUS]
            inactive = [r for r in table_rows if r.get("status") == DEACTIVATED_STATUS]
            replacement = max(active, key=lambda r: int(r.get("id", 0)), default=None)
            for old in inactive:
                key = edge_key(str(old.get("上游主题", "")),
                               str(old.get("转换节点", "")), table)
                if key in existing_keys or replacement is None:
                    continue
                pending.append(self._build_repair_item(old, replacement, policy, source_batch, batch_id))

        if not pending:
            self._finish_batch(header)
            return {"ok": True, "batch_id": batch_id, "batch": header,
                    "created": 0, "remaining": 0, "interrupted": False, "completed": True}

        planned = pending if max_items is None else pending[: max(0, max_items)]
        interrupted = len(planned) < len(pending)
        store.rows(REPAIR_MODULE).extend(planned)
        header["total_candidates"] = int(header.get("total_candidates", 0)) + len(planned)
        header["updated_at"] = now_text()
        if interrupted:
            header["batch_status"] = "进行中"
        else:
            self._finish_batch(header)
        return {"ok": True, "batch_id": batch_id, "batch": header,
                "created": len(planned), "remaining": len(pending) - len(planned),
                "interrupted": interrupted, "completed": not interrupted}

    def _build_repair_item(
        self,
        old_row: dict[str, Any],
        replacement: dict[str, Any],
        policy: str,
        source_batch: str | None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """构造一条修复项：失效边快照（含旧边版本）+ 替代边来源并存。"""
        upstream = str(old_row.get("上游主题", ""))
        transform = str(old_row.get("转换节点", ""))
        table = str(old_row.get("下游表名", ""))
        key = edge_key(upstream, transform, table)
        # 数据版本：先撤后挂的窗口内可见来源数为 0，先挂后撤的窗口内可见来源数为 2，
        # 两种取舍最终有效边集都只剩替代来源一个，但留下不同的数据版本号。
        data_version = "v1-withdraw-first" if policy == "先撤旧边" else "v2-attach-first"
        return {
            "id": self._next_repair_id(),
            "batch_id": batch_id or "",  # 生成时先建表头再回填
            "batch_header": False,
            "repair_status": "待处理",
            "topic_code": upstream,
            "target_table": table,
            "invalid_edge_key": key,
            # 失效边快照：保留旧边版本与旧链路版本，历史查询仍可解释
            "invalid_edge": {
                "from": f"topic::{upstream}", "to": f"transform::{transform}",
                "transform_node": transform, "lineage_id": old_row.get("id"),
                "record_code": old_row.get("记录编号"),
                "edge_version": f"edge-{old_row.get('链路版本', 'v1')}",
                "lineage_version": old_row.get("链路版本"),
                "field_mapping": old_row.get("字段映射"),
            },
            # 替代边来源：同目标表当前有效的转换节点
            "replacement_edge": {
                "transform_node": replacement.get("转换节点"),
                "lineage_id": replacement.get("id"),
                "record_code": replacement.get("记录编号"),
                "lineage_version": replacement.get("链路版本"),
                "field_mapping": replacement.get("字段映射"),
            },
            "policy": policy,
            "data_version": data_version,
            "source_batch": source_batch,
            "steps": self._policy_steps(policy),
            "step_index": 0,
            "created_at": now_text(),
            "updated_at": now_text(),
        }

    def _write_batch(
        self,
        batch_id: str,
        policy: str,
        source_batch: str | None,
        items: list[dict[str, Any]],
        *,
        total_candidates: int,
        interrupted: bool,
    ) -> dict[str, Any]:
        first = items[0]
        header = {
            "id": self._next_repair_id(),
            "batch_id": batch_id,
            "batch_header": True,
            "batch_status": "进行中" if interrupted else "已完成",
            "topic_code": first.get("topic_code"),
            "target_table": first.get("target_table"),
            "policy": policy,
            "source_batch": source_batch,
            "total_candidates": total_candidates,
            "item_count": len(items),
            "created_at": now_text(),
            "updated_at": now_text(),
        }
        for item in items:
            item["batch_id"] = batch_id
            # 未中断时一次性把修复项推进到已完成，失效边立即移出有效图
            if not interrupted:
                item["repair_status"] = "已完成"
                item["step_index"] = len(item["steps"])
        store.rows(REPAIR_MODULE).append(header)
        store.rows(REPAIR_MODULE).extend(items)
        return header

    def _finish_batch(self, header: dict[str, Any]) -> None:
        header["batch_status"] = "已完成"
        header["updated_at"] = now_text()
        for row in store.rows(REPAIR_MODULE):
            if row.get("batch_id") == header["batch_id"] and not row.get("batch_header"):
                row["repair_status"] = "已完成"
                row["step_index"] = len(row.get("steps", []))
                row["updated_at"] = now_text()
        header["item_count"] = sum(
            1 for row in store.rows(REPAIR_MODULE)
            if row.get("batch_id") == header["batch_id"] and not row.get("batch_header"))

    @staticmethod
    def _policy_steps(policy: str) -> list[str]:
        if policy == "先撤旧边":
            return ["撤下失效边", "挂接替代边", "核对有效边集只剩一个来源"]
        return ["挂接替代边", "撤下失效边", "核对有效边集只剩一个来源"]

    def _next_batch_id(self) -> str:
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        serial = sum(1 for r in store.rows(REPAIR_MODULE) if r.get("batch_header")) + 1
        return f"REPAIR-{stamp}-{serial:03d}"

    def _next_repair_id(self) -> int:
        return max((int(r.get("id", 0)) for r in store.rows(REPAIR_MODULE)), default=0) + 1

    # ───────────────────────────────
    # 与死信处理的协作口径
    # ───────────────────────────────

    def find_repair_for_transform(self, transform_node: str) -> dict[str, Any] | None:
        """死信重放前核对：转换节点是否已被某条已完成修复项判定为失效来源。"""
        for item in store.rows(REPAIR_MODULE):
            if item.get("batch_header"):
                continue
            invalid = item.get("invalid_edge", {})
            if invalid.get("transform_node") == transform_node and item.get("repair_status") == "已完成":
                return item
        return None

    def is_transform_deactivated(self, transform_node: str) -> bool:
        for row in store.rows(MODULE):
            if row.get("转换节点") == transform_node:
                return row.get("status") == DEACTIVATED_STATUS
        return False
