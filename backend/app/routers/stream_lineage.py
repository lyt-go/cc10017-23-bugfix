"""流式血缘 DAG 拓扑接口：返回节点、连线图结构与血缘边修复记录。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.stream_lineage import DEFAULT_POLICY, REPAIR_POLICIES, StreamLineageService

router = APIRouter(prefix="/api/stream_lineage", tags=["流式血缘"])
service = StreamLineageService()
LIST_FIELDS = ["记录编号", "上游主题", "下游表名", "转换节点", "字段映射", "采集时间", "延迟偏移", "链路版本"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def csv_cell(value: Any) -> str:
    """CSV 单元格防注入：公式字符前加单引号；引号交给 csv.writer 统一处理。"""
    text = "" if value is None else str(value)
    if text and text[0] in "=+-@":
        text = "'" + text
    return text


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点、有效边与失效边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/repairs")
def list_repairs(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    """查询血缘边修复记录：保留失效边快照与替代边来源，可按批次过滤。"""
    items = service.list_repairs(batch_id)
    headers = [r for r in items if r.get("batch_header")]
    details = [r for r in items if not r.get("batch_header")]
    return {
        "items": details,
        "batches": headers,
        "total": len(details),
        "policies": REPAIR_POLICIES,
    }


@router.post("/repairs/generate")
def generate_repairs(payload: dict[str, Any]) -> ActionResult:
    """读取血缘记录生成血缘边修复记录；幂等，中断后可用 resume 补齐。"""
    policy = str(payload.get("policy") or DEFAULT_POLICY)
    result = service.generate_repairs(
        topic_code=payload.get("topic_code") or None,
        target_table=payload.get("target_table") or None,
        policy=policy,
        source_batch=payload.get("source_batch") or None,
        max_items=payload.get("max_items"),
    )
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("message", "生成失败"))
    return ActionResult(ok=True,
                        message=f"修复批次 {result['batch_id']} 已生成 {result['created']} 条修复项",
                        entry=result)


@router.post("/repairs/{batch_id}/resume")
def resume_repairs(batch_id: str, payload: dict[str, Any] | None = None) -> ActionResult:
    """处理中断后重试：续跑指定批次，补齐后续修复项。"""
    payload = payload or {}
    result = service.resume_repairs(batch_id, max_items=payload.get("max_items"))
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("message", "续跑失败"))
    state = "已完成" if result.get("completed") else "仍有后续项"
    return ActionResult(ok=True,
                        message=f"修复批次 {batch_id} 续跑 {result['created']} 条，{state}",
                        entry=result)


@router.get("/export")
def export_entries() -> StreamingResponse:
    graph = service.get_graph()
    nodes, edges, invalid_edges = graph["nodes"], graph["edges"], graph["invalid_edges"]
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status", "invalid_reason"])
    for node in nodes:
        writer.writerow(["node", node.get("id", ""), csv_cell(node.get("name", "")),
                         "", "", "", node.get("status", ""), ""])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from", ""), edge.get("to", ""),
                         edge.get("label", ""), "有效", ""])
    for edge in invalid_edges:
        writer.writerow(["edge", "", "", edge.get("from", ""), edge.get("to", ""),
                         edge.get("label", ""), "失效", edge.get("invalid_reason", "")])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=stream_lineage.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"血缘记录 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="血缘记录已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
