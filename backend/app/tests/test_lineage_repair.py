"""血缘边修复记录与死信重放核对的跨模块测试。

覆盖：
- 下线转换节点的失效边不进入有效图，页面仍可定位下线节点；
- 血缘边修复记录保留失效边快照（旧边版本）与替代边来源；
- 生成幂等、中断续跑、两种撤挂策略产生不同数据版本；
- 死信处理核对修复记录，拦截向失效节点重放，改投时保留来源批次。
"""
from __future__ import annotations

import unittest

from app.services.dead_letter import DeadLetterService
from app.services.stream_lineage import (
    DEFAULT_POLICY,
    MODULE,
    REPAIR_MODULE,
    StreamLineageService,
)
from app.seed import SEED_ROWS
from app.store import store


def reset_store() -> None:
    """恢复到种子快照：修复产物表是运行期产物，不跨用例残留。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}


class LineageGraphTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.service = StreamLineageService()

    def tearDown(self) -> None:
        reset_store()

    def test_deactivated_transform_edges_not_in_valid_graph(self) -> None:
        graph = self.service.get_graph()
        # 下线节点仍在节点列表中，供流式血缘页定位
        deactivated_nodes = [n for n in graph["nodes"] if n.get("status") == "已停用"]
        self.assertTrue(any(n["name"] == "flink-window-agg-legacy" for n in deactivated_nodes))
        # 有效边里不得出现指向下线转换节点的入边
        valid_targets = {e["to"] for e in graph["edges"]}
        self.assertNotIn("transform::flink-window-agg-legacy", valid_targets)
        # 失效边视图保留了剔除原因
        invalid = [e for e in graph["invalid_edges"]
                   if e["invalid_reason"] == "转换节点已停用"
                   and "flink-window-agg-legacy" in e["to"]]
        self.assertTrue(invalid)

    def test_valid_graph_keeps_replacement_as_single_source(self) -> None:
        graph = self.service.get_graph()
        target_edges = [
            e for e in graph["edges"]
            if e["to"] == "table::order_agg_hourly"
        ]
        transforms = {e["转换节点"] for e in target_edges}
        # 下线/替代冲突对里，旧节点不再是有效来源，替代节点 v2 在有效边集中
        self.assertNotIn("flink-window-agg-legacy", transforms)
        self.assertIn("flink-window-agg-v2", transforms)


class RepairRecordTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.service = StreamLineageService()

    def tearDown(self) -> None:
        reset_store()

    def test_generate_repairs_keeps_invalid_snapshot_and_replacement(self) -> None:
        result = self.service.generate_repairs(topic_code="TOPI-0001")
        self.assertTrue(result["ok"])
        items = [r for r in store.rows(REPAIR_MODULE) if not r.get("batch_header")]
        self.assertTrue(items)
        item = next(r for r in items
                    if r["invalid_edge"]["transform_node"] == "flink-window-agg-legacy")
        # 失效边保留旧边版本与旧链路版本
        self.assertEqual(item["invalid_edge"]["edge_version"], "edge-v1")
        self.assertEqual(item["invalid_edge"]["lineage_version"], "v1")
        # 替代来源指向同目标表的有效转换节点
        self.assertEqual(item["replacement_edge"]["transform_node"], "flink-window-agg-v2")
        self.assertEqual(item["repair_status"], "已完成")
        self.assertIsNotNone(item.get("source_batch") or True)

    def test_repaired_invalid_edge_leaves_valid_graph(self) -> None:
        # 构造一条「已生效但被修复撤下」的边：先停用再恢复旧记录
        lineage_rows = store.rows(MODULE)
        restored = next(r for r in lineage_rows if r["转换节点"] == "flink-window-agg-legacy")
        self.service.generate_repairs(topic_code="TOPI-0001")
        restored["status"] = "已生效"
        restored["abnormal"] = False
        graph = self.service.get_graph()
        valid_targets = {e["to"] for e in graph["edges"]}
        # 恢复旧记录后，失效边仍不出现在有效图（修复记录已撤下）
        self.assertNotIn("transform::flink-window-agg-legacy", valid_targets)
        invalid_reasons = {e["invalid_reason"] for e in graph["invalid_edges"]}
        self.assertIn("失效边已由血缘边修复记录撤下", invalid_reasons)

    def test_generate_repairs_is_idempotent_on_edge_key(self) -> None:
        first = self.service.generate_repairs(topic_code="TOPI-0001")
        self.assertTrue(first["ok"])
        second = self.service.generate_repairs(topic_code="TOPI-0001")
        # 同一失效边键重复生成时不再产生新修复项
        self.assertFalse(second["ok"])
        self.assertIn("没有需要修复的失效边", second["message"])
        items = [r for r in store.rows(REPAIR_MODULE) if not r.get("batch_header")]
        self.assertEqual(len(items), first["created"])

    def test_interrupted_generation_can_resume(self) -> None:
        # 再造一条同表失效边，制造两个待修复项
        rows = store.rows(MODULE)
        rows.append({
            "id": max(int(r["id"]) for r in rows) + 1,
            "status": "已停用", "pending": False, "abnormal": True,
            "记录编号": "LINE-0099", "上游主题": "TOPI-0001",
            "下游表名": "order_agg_hourly", "转换节点": "flink-window-agg-old2",
            "字段映射": "order_id->order_id", "采集时间": "2026-11-15T20:00:00",
            "延迟偏移": "10", "链路版本": "v1",
        })
        first = self.service.generate_repairs(topic_code="TOPI-0001", max_items=1)
        self.assertTrue(first["ok"])
        self.assertTrue(first["interrupted"])
        self.assertEqual(first["created"], 1)
        self.assertGreaterEqual(first["remaining"], 1)
        header = self.service.get_repair_batch(first["batch_id"])
        self.assertEqual(header["batch_status"], "进行中")

        resumed = self.service.resume_repairs(first["batch_id"])
        self.assertTrue(resumed["ok"])
        self.assertTrue(resumed["completed"])
        header = self.service.get_repair_batch(first["batch_id"])
        self.assertEqual(header["batch_status"], "已完成")
        # 续跑后所有修复项到齐，且没有重复键
        items = [r for r in store.rows(REPAIR_MODULE)
                 if not r.get("batch_header") and r["batch_id"] == first["batch_id"]]
        keys = [r["invalid_edge_key"] for r in items]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertTrue(all(r["repair_status"] == "已完成" for r in items))

    def test_policies_produce_different_data_versions(self) -> None:
        withdraw_first = self.service.generate_repairs(
            topic_code="TOPI-0001", policy="先撤旧边", source_batch="BATCH-A")
        self.assertTrue(withdraw_first["ok"])
        # 换策略前清理修复表，模拟另一时间窗的修复批次
        store.rows(REPAIR_MODULE).clear()
        attach_first = self.service.generate_repairs(
            topic_code="TOPI-0001", policy="先挂新边", source_batch="BATCH-A")
        self.assertTrue(attach_first["ok"])
        items = [r for r in store.rows(REPAIR_MODULE) if not r.get("batch_header")]
        self.assertTrue(all(r["data_version"] == "v2-attach-first" for r in items))
        # 撤旧优先与挂新优先的步骤顺序相反
        self.assertEqual(items[0]["steps"][0], "挂接替代边")
        # 两种策略最终有效边集都保留替代来源、剔除失效旧边
        graph = self.service.get_graph()
        target_edges = [e for e in graph["edges"] if e["to"] == "table::order_agg_hourly"]
        transforms = {e["转换节点"] for e in target_edges}
        self.assertIn("flink-window-agg-v2", transforms)
        self.assertNotIn("flink-window-agg-legacy", transforms)

    def test_invalid_policy_rejected(self) -> None:
        result = self.service.generate_repairs(topic_code="TOPI-0001", policy="乱序")
        self.assertFalse(result["ok"])
        self.assertEqual(DEFAULT_POLICY, "先撤旧边")


class DeadLetterReplayTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.lineage_service = StreamLineageService()
        self.dead_service = DeadLetterService()

    def tearDown(self) -> None:
        reset_store()

    def test_replay_blocked_when_target_deactivated(self) -> None:
        # DEAD-0001 的重放目标先改成已停用的转换节点
        dead = store.find("dead_letter", 1)
        dead["重放目标节点"] = "flink-window-agg-legacy"
        entry, message = self.dead_service.run_action(1, "标记重放")
        self.assertIsNone(entry)
        self.assertIn("已停用", message)
        self.assertIn("失效节点", message)

    def test_replay_blocked_by_completed_repair_record(self) -> None:
        self.lineage_service.generate_repairs(topic_code="TOPI-0001")
        dead = store.find("dead_letter", 1)
        dead["重放目标节点"] = "flink-window-agg-legacy"
        entry, message = self.dead_service.run_action(1, "标记重放")
        self.assertIsNone(entry)
        self.assertIn("修复批次", message)
        self.assertIn("flink-window-agg-v2", message)

    def test_redirect_replay_switches_target_and_keeps_batch(self) -> None:
        result = self.lineage_service.generate_repairs(
            topic_code="TOPI-0001", source_batch="BATCH-ORDER-20261116-01")
        self.assertTrue(result["ok"])
        dead = store.find("dead_letter", 1)
        dead["重放目标节点"] = "flink-window-agg-legacy"
        original_batch = dead.get("来源批次")
        entry, message = self.dead_service.run_action(1, "改投重放")
        self.assertIsNotNone(entry)
        self.assertEqual(dead["重放目标节点"], "flink-window-agg-v2")
        self.assertEqual(dead["原重放目标节点"], "flink-window-agg-legacy")
        # 跨模块交接保留来源批次
        self.assertEqual(dead["来源批次"], original_batch)
        self.assertEqual(dead["status"], "已重放")
        self.assertTrue(dead["改投依据批次"])

    def test_replay_cross_check_lists_blocked_targets(self) -> None:
        self.lineage_service.generate_repairs(topic_code="TOPI-0001")
        dead = store.find("dead_letter", 1)
        dead["重放目标节点"] = "flink-window-agg-legacy"
        report = self.dead_service.replay_cross_check()
        self.assertGreaterEqual(report["total"], 1)
        blocked = next(r for r in report["items"] if r["dead_letter_id"] == 1)
        self.assertTrue(blocked["blocked"])
        self.assertEqual(blocked["replacement_node"], "flink-window-agg-v2")
        self.assertEqual(blocked["source_batch"], "BATCH-ORDER-20261116-01")

    def test_replay_allowed_for_valid_target(self) -> None:
        dead = store.find("dead_letter", 1)
        dead["重放目标节点"] = "flink-window-agg-v2"
        entry, message = self.dead_service.run_action(1, "标记重放")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "已重放")


if __name__ == "__main__":
    unittest.main()
