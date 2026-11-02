"""Spark SQL 执行指标分析与归因。

输入是 spark_ui.SparkClient.fetch_context 的结构，输出结构化 findings。
"""
from __future__ import annotations

import re
import statistics
from typing import Any

from spark_ui import epoch_ms

UNIT_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s*(GiB|MiB|KiB|GB|MB|KB|B|ms|s|us|m)\b", re.I)
NUMBER_RE = re.compile(r"([\d,]+(?:\.\d+)?)")

BYTE_UNITS = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3,
              "kb": 1000, "mb": 1000**2, "gb": 1000**3, "m": 1024**2}
TIME_UNITS = {"us": 0.001, "ms": 1, "s": 1000}


def parse_quantity(value: Any, kind: str = "auto") -> float | None:
    """解析 Spark UI 里的数值，支持 20,000,000 / 3.8 MiB / 417 ms / 3.2 s。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value)
    match = UNIT_RE.search(text)
    if not match:
        fallback = NUMBER_RE.search(text)
        if not fallback:
            return None
        cleaned = fallback.group(1).replace(",", "")
        try:
            return float(cleaned)
        except ValueError:
            return None
    number = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower()
    if kind == "bytes" or unit in BYTE_UNITS:
        return number * BYTE_UNITS.get(unit, 1)
    if kind == "ms" or unit in TIME_UNITS:
        return number * TIME_UNITS.get(unit, 1)
    return number


def first_number(*values: Any) -> float:
    for value in values:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).replace(",", ""))
        except ValueError:
            continue
    return 0.0


def metric_kind(name: str) -> str:
    lowered = name.lower()
    if any(word in lowered for word in ("bytes", "size", "memory", "spill")):
        return "bytes"
    if any(word in lowered for word in ("time", "duration")):
        return "ms"
    return "count"


def metric_raw(node: dict[str, Any], name: str) -> Any:
    metrics = node.get("metrics")
    if isinstance(metrics, dict):
        return metrics.get(name)
    for metric in metrics or []:
        if metric.get("name") == name:
            return metric.get("value")
    return None


def metric_value(node: dict[str, Any], name: str) -> float | None:
    raw = metric_raw(node, name)
    return parse_quantity(raw, metric_kind(name)) if raw is not None else None


def normalize_environment(environment: dict[str, Any] | None) -> dict[str, str]:
    props = (environment or {}).get("sparkProperties")
    if isinstance(props, list):
        return {str(k): str(v) for k, v in props}
    if isinstance(props, dict):
        return {str(k): str(v) for k, v in props.items()}
    return {}


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def stats(values: list[float]) -> dict[str, float | None]:
    cleaned = [float(v) for v in values if v is not None]
    if not cleaned:
        return {"min": None, "p50": None, "p90": None, "p95": None, "max": None,
                "max_over_p50": None}
    median = statistics.median(cleaned)
    maximum = max(cleaned)
    return {
        "min": min(cleaned),
        "p50": median,
        "p90": percentile(cleaned, 0.9),
        "p95": percentile(cleaned, 0.95),
        "max": maximum,
        "max_over_p50": (maximum / median) if median else None,
    }


def normalize_task(task: dict[str, Any]) -> dict[str, Any]:
    metrics = task.get("taskMetrics") or task.get("metrics") or {}
    read = metrics.get("shuffleReadMetrics") or {}
    write = metrics.get("shuffleWriteMetrics") or {}
    input_metrics = metrics.get("inputMetrics") or {}
    output_metrics = metrics.get("outputMetrics") or {}

    records = first_number(
        read.get("recordsRead"),
        read.get("totalRecordsRead"),
        read.get("readRecords"),
    )
    input_records = first_number(input_metrics.get("recordsRead"))
    # map 阶段没有 shuffle 读，用输入记录数作为工作量指标
    if not records:
        records = input_records
    read_bytes = first_number(read.get("remoteBytesRead")) + first_number(read.get("localBytesRead"))
    if not read_bytes:
        read_bytes = first_number(read.get("readBytes"))

    return {
        "index": task.get("index"),
        "taskId": task.get("taskId"),
        "attempt": task.get("attempt", 0),
        "executorId": task.get("executorId"),
        "host": task.get("host"),
        "status": task.get("status"),
        "locality": task.get("taskLocality"),
        "speculative": task.get("speculative", False),
        "launchTime": task.get("launchTime"),
        "duration": first_number(task.get("duration")),
        "runTime": first_number(metrics.get("executorRunTime")),
        "deserialize": first_number(metrics.get("executorDeserializeTime")),
        "gc": first_number(metrics.get("jvmGcTime"), task.get("gcTime")),
        "schedulerDelay": first_number(task.get("schedulerDelay"), metrics.get("schedulerDelay")),
        "recordsRead": records,
        "readBytes": read_bytes,
        "remoteBytes": first_number(read.get("remoteBytesRead")),
        "localBytes": first_number(read.get("localBytesRead")),
        "writeBytes": first_number(write.get("bytesWritten")),
        "writeRecords": first_number(write.get("recordsWritten")),
        "inputRecords": input_records,
        "inputBytes": first_number(input_metrics.get("bytesRead")),
        "outputRecords": first_number(output_metrics.get("recordsWritten")),
        "outputBytes": first_number(output_metrics.get("bytesWritten")),
        "memorySpilled": first_number(metrics.get("memoryBytesSpilled")),
        "diskSpilled": first_number(metrics.get("diskBytesSpilled")),
        "peakMemory": first_number(metrics.get("peakExecutionMemory")),
    }


def analyze_stage(stage: dict[str, Any], tasks: list[dict[str, Any]]) -> dict[str, Any]:
    stage_id = stage.get("stageId")
    attempt = stage.get("attemptId", 0)
    submission_ms = epoch_ms(stage.get("submissionTime"))
    completion_ms = epoch_ms(stage.get("completionTime"))
    wall_ms = (completion_ms - submission_ms) if submission_ms and completion_ms else 0.0

    normalized = [normalize_task(task) for task in tasks]
    for task in normalized:
        launch_ms = epoch_ms(task.get("launchTime"))
        task["launchDelay"] = (launch_ms - submission_ms) if launch_ms and submission_ms else None

    duration_stats = stats([t["duration"] for t in normalized])
    records_stats = stats([t["recordsRead"] for t in normalized])
    read_bytes_stats = stats([t["readBytes"] for t in normalized])
    write_records_stats = stats([t["writeRecords"] for t in normalized])
    deserialize_stats = stats([t["deserialize"] for t in normalized])
    gc_stats = stats([t["gc"] for t in normalized])
    launch_stats = stats([t["launchDelay"] for t in normalized if t["launchDelay"] is not None])

    total_records = sum(t["recordsRead"] for t in normalized)
    max_records = max((t["recordsRead"] for t in normalized), default=0.0)
    max_duration = max((t["duration"] for t in normalized), default=0.0)
    median_records = records_stats.get("p50") or 0.0
    median_duration = duration_stats.get("p50") or 0.0
    ratio_records = (max_records / median_records) if median_records else None
    ratio_duration = (max_duration / median_duration) if median_duration else None
    max_share = (max_records / total_records) if total_records else None

    reliable_distribution = len(normalized) >= 4
    is_skewed = bool(
        reliable_distribution
        and ratio_records is not None and ratio_duration is not None and max_share is not None
        and ratio_records >= 5
        and ratio_duration >= 3
        and max_share >= 0.2
    )

    hot_task = None
    if normalized:
        hot_task = max(normalized, key=lambda t: (t["recordsRead"], t["duration"]))

    slow_threshold = median_duration * 2 if median_duration else 0
    slow_tasks = [t for t in normalized if slow_threshold and t["duration"] >= slow_threshold]
    cold_deser_tasks = [
        t for t in slow_tasks
        if t["deserialize"] >= 300
        and t["duration"] > 0
        and t["deserialize"] / t["duration"] >= 0.3
        and (median_records == 0 or t["recordsRead"] <= median_records * 1.5)
    ]
    max_launch_delay = launch_stats.get("max")
    scheduling_gap = (wall_ms - max_duration) if wall_ms and max_duration else 0.0
    scheduling_suspected = bool(
        wall_ms > 0
        and scheduling_gap > wall_ms * 0.5
        and (
            (max_launch_delay is not None and max_launch_delay >= 1000)
            or (max_duration > 0 and scheduling_gap >= 3000)
        )
    )

    memory_spilled = first_number(stage.get("memoryBytesSpilled"))
    disk_spilled = first_number(stage.get("diskBytesSpilled"))

    return {
        "stageId": stage_id,
        "attemptId": attempt,
        "status": stage.get("status"),
        "numTasks": stage.get("numTasks", len(normalized)),
        "wallMs": wall_ms,
        "executorRunTimeMs": first_number(stage.get("executorRunTime")),
        "jvmGcTimeMs": first_number(stage.get("jvmGcTime")),
        "inputBytes": first_number(stage.get("inputBytes")),
        "inputRecords": first_number(stage.get("inputRecords")),
        "shuffleReadBytes": first_number(stage.get("shuffleReadBytes")),
        "shuffleReadRecords": first_number(stage.get("shuffleReadRecords")),
        "shuffleWriteBytes": first_number(stage.get("shuffleWriteBytes")),
        "shuffleWriteRecords": first_number(stage.get("shuffleWriteRecords")),
        "memorySpilled": memory_spilled,
        "diskSpilled": disk_spilled,
        "peakExecutionMemory": first_number(stage.get("peakExecutionMemory")),
        "taskCount": len(normalized),
        "duration": duration_stats,
        "records": records_stats,
        "readBytes": read_bytes_stats,
        "writeRecords": write_records_stats,
        "deserialize": deserialize_stats,
        "gc": gc_stats,
        "launchDelay": launch_stats,
        "totalRecords": total_records,
        "maxRecords": max_records,
        "maxShare": max_share,
        "ratioRecords": ratio_records,
        "ratioDuration": ratio_duration,
        "reliableDistribution": reliable_distribution,
        "isSkewed": is_skewed,
        "hotTask": hot_task,
        "slowTasks": slow_tasks,
        "coldDeserializeTasks": cold_deser_tasks,
        "maxLaunchDelay": max_launch_delay,
        "schedulingGap": scheduling_gap,
        "schedulingSuspected": scheduling_suspected,
        "hasSpill": memory_spilled > 0 or disk_spilled > 0,
        "tasks": normalized,
    }


def normalize_node(node: dict[str, Any]) -> dict[str, Any]:
    metrics: dict[str, dict[str, Any]] = {}
    for metric in node.get("metrics") or []:
        name = metric.get("name")
        if not name:
            continue
        raw = metric.get("value")
        metrics[name] = {"raw": raw, "value": parse_quantity(raw, metric_kind(name))}
    return {
        "nodeId": node.get("nodeId"),
        "nodeName": node.get("nodeName"),
        "metrics": metrics,
    }


def node_metric(node: dict[str, Any], name: str) -> float | None:
    metric = (node.get("metrics") or {}).get(name)
    return metric.get("value") if metric else None


def analyze_plan(sql: dict[str, Any], log_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    nodes = [normalize_node(n) for n in sql.get("nodes") or []]
    plan_text = (sql.get("planDescription") or "") + "\n" + ((log_summary or {}).get("final_plan") or "")
    lowered = plan_text.lower()

    scans = [n for n in nodes if str(n.get("nodeName", "")).lower().startswith("scan")]
    exchanges = [n for n in nodes if "exchange" in str(n.get("nodeName", "")).lower()]
    joins = [
        n for n in nodes
        if any(word in str(n.get("nodeName", "")).lower()
               for word in ("join", "cartesianproduct"))
    ]
    aggregates = [n for n in nodes if "aggregate" in str(n.get("nodeName", "")).lower()]
    sorts = [n for n in nodes if str(n.get("nodeName", "")).lower().startswith("sort")]

    scan_rows = [node_metric(n, "number of output rows") or 0 for n in scans]
    join_rows = [node_metric(n, "number of output rows") or 0 for n in joins]
    max_scan_rows = max(scan_rows) if scan_rows else 0
    max_join_rows = max(join_rows) if join_rows else 0

    exchange_info = []
    for node in exchanges:
        partitions = node_metric(node, "number of partitions")
        shuffle_bytes = node_metric(node, "shuffle bytes written")
        shuffle_records = node_metric(node, "shuffle records written")
        bytes_per_partition = (shuffle_bytes / partitions) if partitions and shuffle_bytes else None
        exchange_info.append({
            "partitions": partitions,
            "shuffleBytes": shuffle_bytes,
            "shuffleRecords": shuffle_records,
            "bytesPerPartition": bytes_per_partition,
            "blocks": {
                "local": node_metric(node, "local blocks read"),
                "remote": node_metric(node, "remote blocks read"),
            },
        })

    over_partitioned = any(
        (item["partitions"] or 0) >= 512
        and item["bytesPerPartition"] is not None
        and item["bytesPerPartition"] < 128 * 1024
        for item in exchange_info
    )

    join_inflation = bool(max_scan_rows > 0 and max_join_rows > max_scan_rows * 2)
    description = (sql.get("description") or "").lower()
    distinct = "count(distinct" in lowered or "count(distinct" in description
    partial_agg = "partial_" in lowered
    sort_merge_join = any("sortmergejoin" in str(n.get("nodeName", "")).lower() for n in joins)
    broadcast_join = any("broadcast" in str(n.get("nodeName", "")).lower() for n in joins)

    table_names = sorted(set(
        match.group(1)
        for match in re.finditer(r"scan\s+parquet\s+(\S+)", plan_text, re.I)
    ))

    return {
        "nodes": nodes,
        "scans": scans,
        "exchanges": exchanges,
        "joins": joins,
        "aggregates": aggregates,
        "sorts": sorts,
        "scanRows": scan_rows,
        "joinRows": join_rows,
        "maxScanRows": max_scan_rows,
        "maxJoinRows": max_join_rows,
        "exchangeInfo": exchange_info,
        "overPartitioned": over_partitioned,
        "joinInflation": join_inflation,
        "distinct": distinct,
        "partialAggregation": partial_agg,
        "sortMergeJoin": sort_merge_join,
        "broadcastJoin": broadcast_join,
        "tableNames": table_names,
        "planText": plan_text,
    }


def analyze_config(environment: dict[str, Any] | None, plan: dict[str, Any]) -> dict[str, Any]:
    props = normalize_environment(environment)
    aqe = props.get("spark.sql.adaptive.enabled", "true").lower()
    skew_enabled = props.get("spark.sql.adaptive.skewJoin.enabled", "true").lower()
    skew_factor = parse_quantity(props.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5"))
    skew_threshold = parse_quantity(
        props.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m"),
        "bytes",
    )
    shuffle_partitions = parse_quantity(props.get("spark.sql.shuffle.partitions"))
    broadcast_threshold = parse_quantity(
        props.get("spark.sql.autoBroadcastJoinThreshold", "10m"),
        "bytes",
    )
    dynamic_enabled = props.get("spark.dynamicAllocation.enabled", "false").lower()
    min_executors = parse_quantity(props.get("spark.dynamicAllocation.minExecutors", "0"))
    idle_timeout = parse_quantity(props.get("spark.dynamicAllocation.executorIdleTimeout", "60"))

    inferred_partitions = None
    for item in plan.get("exchangeInfo") or []:
        if item.get("partitions"):
            inferred_partitions = max(inferred_partitions or 0, item["partitions"])

    return {
        "aqeEnabled": aqe != "false",
        "skewJoinEnabled": skew_enabled != "false",
        "skewPartitionFactor": skew_factor,
        "skewPartitionThresholdBytes": skew_threshold,
        "shufflePartitions": shuffle_partitions or inferred_partitions,
        "autoBroadcastJoinThresholdBytes": broadcast_threshold,
        "dynamicAllocationEnabled": dynamic_enabled == "true",
        "minExecutors": min_executors,
        "executorIdleTimeoutSec": idle_timeout,
        "eventLogEnabled": props.get("spark.eventLog.enabled"),
        "eventLogDir": props.get("spark.eventLog.dir"),
        "props": props,
    }


def add_finding(findings: list[dict[str, Any]], severity: str, finding_type: str,
                title: str, evidence: dict[str, Any], confidence: str = "high") -> None:
    findings.append({
        "severity": severity,
        "type": finding_type,
        "title": title,
        "evidence": evidence,
        "confidence": confidence,
    })


def build_recommendations(findings: list[dict[str, Any]], plan: dict[str, Any],
                          config: dict[str, Any]) -> list[dict[str, Any]]:
    recommendations = []
    types = {f["type"] for f in findings}

    if "data_skew" in types:
        recommendations.append({
            "id": "broadcast_or_salt",
            "title": "优先广播小表，或对热点 key 做 salting",
            "reason": "存在单 task 热点，shuffle join 会把热点 key 压到一个分区。",
            "expectedEffect": "消除 reduce 侧热点和 spill，缩短 stage 墙钟。",
            "risk": "广播需要 executor 内存足够；salting 会增加数据量和 SQL 复杂度。",
        })
        recommendations.append({
            "id": "pre_aggregate",
            "title": "能预聚合就先预聚合，再 join",
            "reason": "如果业务允许，先把事实表按 join key 聚合，可把热点 key 压缩成一行。",
            "expectedEffect": "join 输入显著变小，热点消失。",
            "risk": "只适用于 count/sum 等可合并聚合，且维度表 key 唯一时要调整计数逻辑。",
        })
    if "cold_start" in types or "scheduling" in types:
        recommendations.append({
            "id": "warm_executors",
            "title": "保留热 executor，减少动态分配冷启动",
            "reason": "stage 墙钟远大于 task 运行时间，task 启动延迟高。",
            "expectedEffect": "减少数秒级容器申请和类加载开销。",
            "risk": "常驻 executor 会占用队列资源，需要按 SLA 权衡。",
        })
    if plan.get("overPartitioned"):
        recommendations.append({
            "id": "reduce_shuffle_partitions",
            "title": "降低 shuffle 分区数，避免大量空 block",
            "reason": "分区数远大于数据量，产生大量小 block 和远程请求。",
            "expectedEffect": "减少 block 调度和 fetch 开销。",
            "risk": "分区过少会降低大 shuffle 的并行度，建议结合 AQE 和实际数据量。",
        })
    if plan.get("sortMergeJoin") and (config.get("autoBroadcastJoinThresholdBytes") or 0) <= 10 * 1024 * 1024:
        recommendations.append({
            "id": "evaluate_broadcast",
            "title": "评估把小表广播掉",
            "reason": "当前是 SortMergeJoin，且广播阈值较低；如果小表能放进内存，可避免 shuffle join。",
            "expectedEffect": "消除 join 侧 shuffle 和热点。",
            "risk": "广播过大可能导致 executor OOM。",
        })
    if "aqe_threshold_miss" in types:
        recommendations.append({
            "id": "aqe_tuning",
            "title": "不要默认 AQE 会拆分这个热点",
            "reason": "热点分区压缩后大小低于 AQE 阈值，skew join 不会触发。",
            "expectedEffect": "明确改用广播/预聚合/salting，而不是只打开 AQE。",
            "risk": "强行降低阈值可能误伤其他分区，需要测试。",
        })
    if "spill" in types and "data_skew" not in types:
        recommendations.append({
            "id": "memory_or_plan",
            "title": "检查内存和聚合/排序状态",
            "reason": "存在 spill，但没有明显单 key 热点。",
            "expectedEffect": "减少磁盘 spill 和 GC。",
            "risk": "单纯加内存可能掩盖计划问题。",
        })
    return recommendations


def analyze_execution(context: dict[str, Any],
                      log_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    sql = context.get("sql") or {}
    stages_raw = context.get("stages") or []
    task_lists = context.get("task_lists") or {}
    environment = context.get("environment") or {}

    stages = []
    for stage in stages_raw:
        stage_id = stage.get("stageId")
        tasks = task_lists.get(stage_id)
        if tasks is None:
            tasks = task_lists.get(str(stage_id), [])
        stages.append(analyze_stage(stage, tasks))
    stages.sort(key=lambda s: (s.get("stageId") or 0, s.get("attemptId") or 0))

    plan = analyze_plan(sql, log_summary)
    config = analyze_config(environment, plan)

    findings: list[dict[str, Any]] = []

    for stage in stages:
        if stage["isSkewed"]:
            hot = stage["hotTask"] or {}
            add_finding(
                findings,
                "HIGH",
                "data_skew",
                f"Stage {stage['stageId']} 存在数据倾斜：task {hot.get('taskId')} 处理 {hot.get('recordsRead', 0):,.0f} 条记录",
                {
                    "stageId": stage["stageId"],
                    "taskId": hot.get("taskId"),
                    "records": hot.get("recordsRead"),
                    "durationMs": hot.get("duration"),
                    "medianRecords": stage["records"].get("p50"),
                    "medianDurationMs": stage["duration"].get("p50"),
                    "recordsRatio": stage["ratioRecords"],
                    "durationRatio": stage["ratioDuration"],
                    "maxShare": stage["maxShare"],
                    "memorySpilled": stage["memorySpilled"],
                    "diskSpilled": stage["diskSpilled"],
                },
            )

    for stage in stages:
        if stage["schedulingSuspected"]:
            add_finding(
                findings,
                "MEDIUM",
                "scheduling",
                f"Stage {stage['stageId']} 墙钟 {stage['wallMs']/1000:.2f}s，但最长 task 只有 {stage['duration'].get('max', 0)/1000:.2f}s",
                {
                    "stageId": stage["stageId"],
                    "wallMs": stage["wallMs"],
                    "maxTaskDurationMs": stage["duration"].get("max"),
                    "maxLaunchDelayMs": stage["maxLaunchDelay"],
                    "schedulingGapMs": stage["schedulingGap"],
                },
            )
        if stage["coldDeserializeTasks"]:
            task = stage["coldDeserializeTasks"][0]
            add_finding(
                findings,
                "MEDIUM",
                "cold_start",
                f"Stage {stage['stageId']} 的慢 task 里存在冷 executor：反序列化 {task['deserialize']:.0f}ms",
                {
                    "stageId": stage["stageId"],
                    "taskId": task.get("taskId"),
                    "durationMs": task.get("duration"),
                    "deserializeMs": task.get("deserialize"),
                    "records": task.get("recordsRead"),
                },
            )

    total_memory_spill = sum(s["memorySpilled"] for s in stages)
    total_disk_spill = sum(s["diskSpilled"] for s in stages)
    if total_memory_spill or total_disk_spill:
        add_finding(
            findings,
            "MEDIUM" if total_memory_spill + total_disk_spill < 1024**3 else "HIGH",
            "spill",
            "执行过程中发生 spill",
            {
                "memoryBytesSpilled": total_memory_spill,
                "diskBytesSpilled": total_disk_spill,
                "stages": [s["stageId"] for s in stages if s["hasSpill"]],
            },
        )

    if plan["joinInflation"]:
        add_finding(
            findings,
            "HIGH",
            "join_inflation",
            "Join 输出行数明显大于扫描输入行数，存在膨胀风险",
            {
                "maxScanRows": plan["maxScanRows"],
                "maxJoinRows": plan["maxJoinRows"],
                "ratio": (plan["maxJoinRows"] / plan["maxScanRows"]) if plan["maxScanRows"] else None,
            },
            confidence="medium",
        )

    if plan["overPartitioned"]:
        worst = max(
            (item for item in plan["exchangeInfo"] if item.get("partitions")),
            key=lambda item: item["partitions"],
        )
        add_finding(
            findings,
            "LOW",
            "over_partitioning",
            f"Shuffle 分区数 {worst['partitions']:.0f} 对当前数据量过大",
            {
                "partitions": worst["partitions"],
                "shuffleBytes": worst["shuffleBytes"],
                "bytesPerPartition": worst["bytesPerPartition"],
                "blocks": worst["blocks"],
            },
        )

    has_skew = any(f["type"] == "data_skew" for f in findings)
    if plan["distinct"] and not has_skew:
        add_finding(
            findings,
            "INFO",
            "distinct_skew_resistant",
            "count(DISTINCT) 采用两阶段去重，热点 key 被 distinct 字段摊开",
            {
                "distinct": True,
                "partialAggregation": plan["partialAggregation"],
                "maxRecordsRatio": max((s["ratioRecords"] or 0) for s in stages) if stages else None,
            },
        )
    if plan["partialAggregation"] and not has_skew:
        add_finding(
            findings,
            "INFO",
            "partial_aggregation_hides_skew",
            "计划包含 map 端预聚合，可能掩盖了源数据的 key 倾斜",
            {
                "partialAggregation": True,
                "maxRecordsRatio": max((s["ratioRecords"] or 0) for s in stages) if stages else None,
            },
        )

    if has_skew:
        hot_bytes = None
        median_bytes = None
        hot_stage = next((s for s in stages if s["isSkewed"]), None)
        if hot_stage:
            hot_bytes = (hot_stage["hotTask"] or {}).get("readBytes")
            median_bytes = hot_stage["readBytes"].get("p50")
        if hot_bytes and median_bytes and config.get("skewPartitionFactor") and config.get("skewPartitionThresholdBytes"):
            ratio = hot_bytes / median_bytes if median_bytes else None
            if ratio is not None and (
                ratio < config["skewPartitionFactor"]
                or hot_bytes < config["skewPartitionThresholdBytes"]
            ):
                add_finding(
                    findings,
                    "MEDIUM",
                    "aqe_threshold_miss",
                    "AQE skew join 阈值可能无法识别这个热点",
                    {
                        "hotPartitionBytes": hot_bytes,
                        "medianPartitionBytes": median_bytes,
                        "sizeRatio": ratio,
                        "skewPartitionFactor": config["skewPartitionFactor"],
                        "skewPartitionThresholdBytes": config["skewPartitionThresholdBytes"],
                    },
                )

    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "INFO": 3}
    findings.sort(key=lambda f: (severity_order.get(f["severity"], 9), f["type"]))
    recommendations = build_recommendations(findings, plan, config)

    return {
        "appId": context.get("app_id"),
        "execution": {
            "id": sql.get("id"),
            "status": sql.get("status"),
            "description": sql.get("description"),
            "durationMs": first_number(sql.get("duration")),
            "submissionTime": sql.get("submissionTime"),
            "jobs": sql.get("successJobIds") or [],
        },
        "stages": stages,
        "plan": {
            "tableNames": plan["tableNames"],
            "sortMergeJoin": plan["sortMergeJoin"],
            "broadcastJoin": plan["broadcastJoin"],
            "distinct": plan["distinct"],
            "partialAggregation": plan["partialAggregation"],
            "joinInflation": plan["joinInflation"],
            "overPartitioned": plan["overPartitioned"],
            "maxScanRows": plan["maxScanRows"],
            "maxJoinRows": plan["maxJoinRows"],
            "exchangeInfo": plan["exchangeInfo"],
            "planText": plan["planText"],
        },
        "config": config,
        "findings": findings,
        "recommendations": recommendations,
        "limits": [
            "看不到真实数据内容，热点 key 的具体值需要单独查询。",
            "YARN 队列排队和容器分配原因需要 RM/Kyuubi 日志确认。",
            "UDF/自定义代码问题需要代码和 executor 日志。",
        ],
    }
