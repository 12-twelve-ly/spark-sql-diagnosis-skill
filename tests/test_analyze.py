"""回归用例：用这次会话里的三类真实形态做合成数据断言。"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from analyze import analyze_execution, parse_quantity  # noqa: E402
from spark_diagnose import parse_log  # noqa: E402
from spark_ui import resolve_endpoint  # noqa: E402


def make_task(index: int, duration: float, records: float, read_bytes: float,
              deserialize: float = 0, launch: str | None = None,
              executor: str = "1", host: str = "worker-1") -> dict:
    return {
        "taskId": 100 + index,
        "index": index,
        "attempt": 0,
        "executorId": executor,
        "host": host,
        "status": "SUCCESS",
        "launchTime": launch,
        "duration": duration,
        "taskMetrics": {
            "executorRunTime": max(0, duration - deserialize),
            "executorDeserializeTime": deserialize,
            "jvmGcTime": 0,
            "shuffleReadMetrics": {
                "recordsRead": records,
                "remoteBytesRead": read_bytes // 2,
                "localBytesRead": read_bytes - read_bytes // 2,
            },
            "shuffleWriteMetrics": {"bytesWritten": 0, "recordsWritten": 0},
            "inputMetrics": {"bytesRead": 0, "recordsRead": 0},
            "outputMetrics": {"bytesWritten": 0, "recordsWritten": 0},
            "memoryBytesSpilled": 0,
            "diskBytesSpilled": 0,
            "peakExecutionMemory": 0,
        },
    }


def make_stage(stage_id: int, tasks: list[dict], submission: str, completion: str,
               memory_spill: float = 0, disk_spill: float = 0,
               read_records: float | None = None, read_bytes: float | None = None) -> dict:
    return {
        "stageId": stage_id,
        "attemptId": 0,
        "status": "COMPLETE",
        "numTasks": len(tasks),
        "submissionTime": submission,
        "completionTime": completion,
        "executorRunTime": sum(t["taskMetrics"]["executorRunTime"] for t in tasks),
        "jvmGcTime": 0,
        "inputBytes": 0,
        "inputRecords": 0,
        "shuffleReadBytes": read_bytes if read_bytes is not None else sum(
            t["taskMetrics"]["shuffleReadMetrics"]["remoteBytesRead"]
            + t["taskMetrics"]["shuffleReadMetrics"]["localBytesRead"]
            for t in tasks
        ),
        "shuffleReadRecords": read_records if read_records is not None else sum(
            t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"] for t in tasks
        ),
        "shuffleWriteBytes": 0,
        "shuffleWriteRecords": 0,
        "memoryBytesSpilled": memory_spill,
        "diskBytesSpilled": disk_spill,
        "peakExecutionMemory": 0,
    }


def make_node(node_id: int, name: str, metrics: dict) -> dict:
    return {
        "nodeId": node_id,
        "nodeName": name,
        "metrics": [{"name": key, "value": value} for key, value in metrics.items()],
    }


def make_sql(exec_id: int, description: str, nodes: list[dict], plan_text: str,
             duration: int) -> dict:
    return {
        "id": exec_id,
        "status": "COMPLETED",
        "description": description,
        "duration": duration,
        "submissionTime": "2026-11-02T08:06:50.516GMT",
        "successJobIds": [25],
        "nodes": nodes,
        "planDescription": plan_text,
    }


def make_context(sql: dict, stages: list[dict], task_lists: dict[int, list[dict]],
                 props: list[list[str]] | None = None) -> dict:
    return {
        "app_id": "application_1_2",
        "sql": sql,
        "jobs": [{"jobId": 25, "stageIds": [s["stageId"] for s in stages]}],
        "stages": stages,
        "task_lists": task_lists,
        "summaries": {},
        "environment": {"sparkProperties": props or []},
        "executors": [],
    }


class AnalyzeTests(unittest.TestCase):
    def test_parse_quantity_units(self) -> None:
        self.assertAlmostEqual(parse_quantity("3.8 MiB", "bytes"), 3.8 * 1024**2)
        self.assertAlmostEqual(parse_quantity("417 ms", "ms"), 417)
        self.assertAlmostEqual(parse_quantity("3.2 s", "ms"), 3200)
        self.assertAlmostEqual(parse_quantity("400m", "bytes"), 400 * 1024**2)
        self.assertAlmostEqual(parse_quantity("20,000,000"), 20_000_000)

    def test_resolve_endpoint_forms(self) -> None:
        proxy = resolve_endpoint("http://h:8088/proxy/application_1_2/")
        self.assertEqual(proxy.api_base, "http://h:8088/proxy/application_1_2/api/v1")
        self.assertEqual(proxy.app_id, "application_1_2")

        history = resolve_endpoint("http://h:18080/history/application_1_2/")
        self.assertEqual(history.api_base, "http://h:18080/api/v1")
        self.assertEqual(history.app_id, "application_1_2")

        running = resolve_endpoint("http://h:4040/jobs/")
        self.assertEqual(running.api_base, "http://h:4040/api/v1")

    def test_join_skew(self) -> None:
        normal = [make_task(i, 149, 275_000, 1_436_324) for i in range(38)]
        hot = make_task(29, 3136, 12_275_975, 4_077_740, deserialize=4,
                        executor="17", host="worker-004")
        cold = make_task(2, 2919, 274_226, 1_433_184, deserialize=1356,
                         executor="20", host="worker-006")
        tasks = normal + [hot, cold]

        stage = make_stage(
            37, tasks,
            "2026-11-02T08:07:03.011GMT",
            "2026-11-02T08:07:07.827GMT",
            memory_spill=201_326_592,
            disk_spill=2_099_653,
            read_records=23_000_000,
            read_bytes=60_072_545,
        )
        sql = make_sql(
            94,
            "SELECT count(*) AS row_cnt FROM fact_order o JOIN dim_item d ON o.item_id = d.item_id",
            [
                make_node(10, "Scan parquet spark_catalog.test.fact", {"number of output rows": "20,000,000"}),
                make_node(20, "Scan parquet spark_catalog.test.dim", {"number of output rows": "3,000,000"}),
                make_node(9, "Exchange", {"number of partitions": "40", "shuffle bytes written": "3.8 MiB",
                                          "shuffle records written": "23,000,000"}),
                make_node(11, "SortMergeJoin", {"number of output rows": "20,000,000"}),
                make_node(8, "Sort", {"spill size": "192.0 MiB", "peak memory": "224.0 MiB"}),
                make_node(4, "HashAggregate", {"number of output rows": "40", "spill size": "0.0 B"}),
            ],
            "SortMergeJoin\nExchange hashpartitioning(item_id, 40)\nHashAggregate partial_count",
            17_476,
        )
        props = [
            ["spark.sql.adaptive.enabled", "false"],
            ["spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5"],
            ["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "400m"],
            ["spark.sql.shuffle.partitions", "40"],
            ["spark.sql.autoBroadcastJoinThreshold", "-1"],
            ["spark.dynamicAllocation.enabled", "true"],
            ["spark.dynamicAllocation.minExecutors", "0"],
        ]
        result = analyze_execution(make_context(sql, [stage], {37: tasks}, props))
        types = {finding["type"] for finding in result["findings"]}

        self.assertIn("data_skew", types)
        skew = next(f for f in result["findings"] if f["type"] == "data_skew")
        self.assertGreater(skew["evidence"]["recordsRatio"], 40)
        self.assertGreater(skew["evidence"]["durationRatio"], 20)
        self.assertIn("spill", types)
        self.assertIn("cold_start", types)
        self.assertIn("aqe_threshold_miss", types)
        self.assertTrue(result["plan"]["sortMergeJoin"])

    def test_count_distinct_is_balanced(self) -> None:
        tasks = [make_task(i, 66, 97_250, 855_000) for i in range(40)]
        tasks[1] = make_task(1, 202, 97_332, 856_488, deserialize=14)

        stage43 = make_stage(
            43, tasks,
            "2026-11-02T12:42:31.231GMT",
            "2026-11-02T12:42:32.764GMT",
            read_records=3_892_616,
            read_bytes=34_269_538,
        )
        stage44_tasks = [make_task(i, 37, 52_306, 357_625) for i in range(40)]
        stage44 = make_stage(
            44, stage44_tasks,
            "2026-11-02T12:42:32.768GMT",
            "2026-11-02T12:42:33.592GMT",
            read_records=2_092_847,
            read_bytes=14_310_852,
        )
        sql = make_sql(
            109,
            "SELECT item_id, count(DISTINCT buyer_id) AS buyer_cnt FROM order_wide GROUP BY item_id",
            [
                make_node(10, "Scan parquet spark_catalog.test.order_wide", {"number of output rows": "10,000,000"}),
                make_node(6, "Exchange", {"number of partitions": "40", "shuffle bytes written": "32.7 MiB",
                                          "shuffle records written": "3,892,616"}),
                make_node(5, "HashAggregate", {"number of output rows": "2,973,083", "spill size": "0.0 B"}),
                make_node(4, "HashAggregate", {"number of output rows": "2,092,847", "spill size": "0.0 B"}),
            ],
            "HashAggregate keys [item_id, buyer_id]\n"
            "Exchange hashpartitioning(item_id, buyer_id, 40)\n"
            "HashAggregate keys [item_id] Functions [partial_count(distinct buyer_id)]",
            4_229,
        )
        result = analyze_execution(make_context(sql, [stage43, stage44], {43: tasks, 44: stage44_tasks}))
        types = {finding["type"] for finding in result["findings"]}

        self.assertNotIn("data_skew", types)
        self.assertIn("distinct_skew_resistant", types)
        self.assertIn("partial_aggregation_hides_skew", types)

    def test_cold_start_and_over_partitioning(self) -> None:
        tasks = [
            make_task(0, 4711, 10_000_000, 0, deserialize=1183,
                      launch="2026-11-02T07:44:24.968GMT", executor="15", host="worker-005"),
            make_task(1, 4398, 10_000_000, 0, deserialize=1132,
                      launch="2026-11-02T07:44:28.317GMT", executor="16", host="worker-005"),
        ]
        stage = make_stage(
            32, tasks,
            "2026-11-02T07:44:19.865GMT",
            "2026-11-02T07:44:32.716GMT",
        )
        stage["inputRecords"] = 20_000_000
        stage["inputBytes"] = 44_673_501
        stage["shuffleWriteBytes"] = 3_988_286
        stage["shuffleWriteRecords"] = 387_465
        sql = make_sql(
            83,
            "SELECT item_id, count(*) AS cnt FROM fact_order TABLESAMPLE (5 PERCENT) GROUP BY item_id ORDER BY cnt DESC LIMIT 10",
            [
                make_node(1, "Scan parquet spark_catalog.test.fact", {"number of output rows": "20,000,000"}),
                make_node(4, "Exchange", {"number of partitions": "8192", "shuffle bytes written": "3.8 MiB",
                                          "shuffle records written": "387,465"}),
                make_node(3, "HashAggregate", {"number of output rows": "386,839", "spill size": "0.0 B"}),
            ],
            "Exchange hashpartitioning(item_id, 8192)\nHashAggregate partial_count",
            14_210,
        )
        props = [
            ["spark.sql.adaptive.enabled", "true"],
            ["spark.dynamicAllocation.enabled", "true"],
            ["spark.dynamicAllocation.minExecutors", "0"],
        ]
        result = analyze_execution(make_context(sql, [stage], {32: tasks}, props))
        types = {finding["type"] for finding in result["findings"]}

        self.assertNotIn("data_skew", types)
        self.assertIn("scheduling", types)
        self.assertIn("over_partitioning", types)

    def test_parse_log_extracts_app_ui_stage_and_stats(self) -> None:
        log_text = """
2026-11-02 16:06:50.434 INFO OperationAuditLogger: operation=test-operation-123 state=RUNNING
26/11/02 16:06:50 INFO ExecuteStatement:
           application ID: application_1_2
           application web UI: http://h:8088/proxy/application_1_2,http://h2:8088/proxy/application_1_2
26/11/02 16:07:01 INFO SQLOperationListener: Finished stage: Stage(37, 0); Name: 'x'; Status: succeeded; numTasks: 40; Took: 4816 msec
26/11/02 16:07:01 INFO StatsReportListener: task runtime:(count: 40, mean: 322.225000, stdev: 645.097143, max: 3136.000000, min: 68.000000)
26/11/02 16:07:01 INFO StatsReportListener: shuffle bytes written:(count: 40, mean: 10.0, stdev: 1.0, max: 21.4, min: 7.2)
"""
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False, encoding="utf-8") as handle:
            handle.write(log_text)
            path = handle.name
        try:
            parsed = parse_log(path)
        finally:
            Path(path).unlink(missing_ok=True)

        self.assertEqual(parsed["app_id"], "application_1_2")
        self.assertEqual(len(parsed["ui_urls"]), 2)
        self.assertEqual(parsed["operation_id"], "test-operation-123")
        self.assertEqual(parsed["stages"][0]["tookMs"], 4816)
        self.assertEqual(parsed["stages"][0]["stats"]["task runtime"]["max"], 3136.0)


if __name__ == "__main__":
    unittest.main()
