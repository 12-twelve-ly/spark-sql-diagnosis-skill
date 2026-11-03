"""Spark SQL 执行指标分析与归因。

输入是 spark_ui.SparkClient.fetch_context 的结构，输出结构化 findings。
v2 在倾斜之外，增加并行度上限、队列资源不足、单 task 过重、
shuffle fetch、map 端倾斜、小文件、重试等慢因。
"""
from __future__ import annotations

import re
import statistics
from typing import Any

from spark_ui import epoch_ms

UNIT_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s*(GiB|MiB|KiB|GB|MB|KB|B|ms|s|us|m)\b", re.I)
NUMBER_RE = re.compile(r"([\d,]+(?:\.\d+)?)")
STAGE_IN_METRIC_RE = re.compile(r"stage\s+(\d+)\.(\d+)", re.I)

BYTE_UNITS = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3,
              "kb": 1000, "mb": 1000**2, "gb": 1000**3, "m": 1024**2}
TIME_UNITS = {"us": 0.001, "ms": 1, "s": 1000}

# 可调阈值：全部是启发式，报告里会标注为“默认阈值”
PARALLELISM_MIN_TASKS = 50
LAUNCH_SPREAD_MS = 2000
HEAVY_INPUT_RECORDS = 10_000_000
HEAVY_INPUT_BYTES = 512 * 1024**2
FETCH_WAIT_RATIO = 0.20
SMALL_FILE_COUNT = 100
SMALL_FILE_SIZE = 64 * 1024**2
MANY_JOBS = 20
MANY_STAGES = 50
SCAN_PRUNING_ROWS = 50_000_000


def parse_quantity(value: Any, kind: str = "auto") -> float | None:
    """解析 Spark UI 里的数值，支持 20,000,000 / 3.8 MiB / 417 ms / 3.2 s / 400m。"""
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
        "fetchWait": first_number(read.get("fetchWaitTime")),
        "remoteReqsDuration": first_number(read.get("remoteReqsDuration")),
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
    input_records_stats = stats([t["inputRecords"] for t in normalized])
    input_bytes_stats = stats([t["inputBytes"] for t in normalized])
    fetch_wait_stats = stats([t["fetchWait"] for t in normalized])
    remote_reqs_stats = stats([t["remoteReqsDuration"] for t in normalized])

    total_records = sum(t["recordsRead"] for t in normalized)
    total_duration = sum(t["duration"] for t in normalized)
    total_fetch_wait = sum(t["fetchWait"] for t in normalized)
    total_remote_reqs = sum(t["remoteReqsDuration"] for t in normalized)
    total_input_records = sum(t["inputRecords"] for t in normalized)
    total_input_bytes = sum(t["inputBytes"] for t in normalized)

    max_records = max((t["recordsRead"] for t in normalized), default=0.0)
    max_duration = max((t["duration"] for t in normalized), default=0.0)
    median_records = records_stats.get("p50") or 0.0
    median_duration = duration_stats.get("p50") or 0.0
    ratio_records = (max_records / median_records) if median_records else None
    ratio_duration = (max_duration / median_duration) if median_duration else None
    max_share = (max_records / total_records) if total_records else None
    balanced = bool(
        ratio_records is not None and ratio_duration is not None
        and ratio_records < 3 and ratio_duration < 3
    )

    reliable_distribution = len(normalized) >= 4
    is_skewed = bool(
        reliable_distribution and not balanced
        and ratio_records is not None and ratio_duration is not None and max_share is not None
        and ratio_records >= 5 and ratio_duration >= 3 and max_share >= 0.2
    )

    hot_task = max(normalized, key=lambda t: (t["recordsRead"], t["duration"])) if normalized else None

    slow_threshold = median_duration * 2 if median_duration else 0
    slow_tasks = [t for t in normalized if slow_threshold and t["duration"] >= slow_threshold]
    cold_deser_tasks = [
        t for t in slow_tasks
        if t["deserialize"] >= 300 and t["duration"] > 0
        and t["deserialize"] / t["duration"] >= 0.3
        and (median_records == 0 or t["recordsRead"] <= median_records * 1.5)
    ]
    max_launch_delay = launch_stats.get("max")
    launch_values = [t["launchDelay"] for t in normalized if t["launchDelay"] is not None]
    launch_p10 = percentile(launch_values, 0.1)
    launch_spread = (
        launch_stats.get("p90") - launch_p10
        if launch_stats.get("p90") is not None and launch_p10 is not None else None
    )
    delayed_tasks = sum(1 for t in normalized if (t["launchDelay"] or 0) >= 1000)

    scheduling_gap = (wall_ms - max_duration) if wall_ms and max_duration else 0.0
    scheduling_suspected = bool(
        wall_ms > 0 and scheduling_gap > wall_ms * 0.5
        and ((max_launch_delay is not None and max_launch_delay >= 1000)
             or (max_duration > 0 and scheduling_gap >= 3000))
    )

    memory_spilled = first_number(stage.get("memoryBytesSpilled"))
    disk_spilled = first_number(stage.get("diskBytesSpilled"))
    input_records = first_number(stage.get("inputRecords"))
    shuffle_read_records = first_number(stage.get("shuffleReadRecords"))
    is_map_stage = input_records > 0 and shuffle_read_records == 0

    input_median = input_records_stats.get("p50") or 0
    max_input_records = input_records_stats.get("max") or 0
    input_ratio = (max_input_records / input_median) if input_median else None
    input_share = (max_input_records / total_input_records) if total_input_records else None
    map_skew = bool(
        is_map_stage and len(normalized) >= 2 and input_ratio is not None
        and input_ratio >= 3 and (input_share or 0) >= 0.6
    )

    median_input_records = input_records_stats.get("p50") or 0
    median_input_bytes = input_bytes_stats.get("p50") or 0
    task_too_heavy = bool(
        balanced and not is_skewed and not map_skew
        and (median_input_records > HEAVY_INPUT_RECORDS
             or median_input_bytes > HEAVY_INPUT_BYTES)
    )

    fetch_ratio = (total_fetch_wait / total_duration) if total_duration else 0.0
    remote_reqs_ratio = (total_remote_reqs / total_duration) if total_duration else 0.0
    # 单 task stage 的 fetch 比例没有统计意义，先不报，避免把 1 个 task 的
    # 小 stage 误判成 fetch 慢。
    fetch_slow = bool(
        len(normalized) >= 4
        and (fetch_ratio >= FETCH_WAIT_RATIO or remote_reqs_ratio >= FETCH_WAIT_RATIO)
    )

    executor_ids = sorted({str(t["executorId"]) for t in normalized if t.get("executorId")})

    return {
        "stageId": stage_id,
        "attemptId": attempt,
        "status": stage.get("status"),
        "numTasks": stage.get("numTasks", len(normalized)),
        "numFailedTasks": first_number(stage.get("numFailedTasks")),
        "numKilledTasks": first_number(stage.get("numKilledTasks")),
        "wallMs": wall_ms,
        "executorRunTimeMs": first_number(stage.get("executorRunTime")),
        "jvmGcTimeMs": first_number(stage.get("jvmGcTime")),
        "inputBytes": first_number(stage.get("inputBytes")),
        "inputRecords": input_records,
        "shuffleReadBytes": first_number(stage.get("shuffleReadBytes")),
        "shuffleReadRecords": shuffle_read_records,
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
        "inputRecordsStats": input_records_stats,
        "inputBytesStats": input_bytes_stats,
        "fetchWait": fetch_wait_stats,
        "remoteReqsDuration": remote_reqs_stats,
        "totalRecords": total_records,
        "totalInputRecords": total_input_records,
        "totalInputBytes": total_input_bytes,
        "maxRecords": max_records,
        "maxShare": max_share,
        "ratioRecords": ratio_records,
        "ratioDuration": ratio_duration,
        "balanced": balanced,
        "reliableDistribution": reliable_distribution,
        "isSkewed": is_skewed,
        "hotTask": hot_task,
        "slowTasks": slow_tasks,
        "coldDeserializeTasks": cold_deser_tasks,
        "maxLaunchDelay": max_launch_delay,
        "launchSpread": launch_spread,
        "delayedTasks": delayed_tasks,
        "schedulingGap": scheduling_gap,
        "schedulingSuspected": scheduling_suspected,
        "isMapStage": is_map_stage,
        "inputRatio": input_ratio,
        "inputShare": input_share,
        "mapSkew": map_skew,
        "taskTooHeavy": task_too_heavy,
        "fetchRatio": fetch_ratio,
        "remoteReqsRatio": remote_reqs_ratio,
        "fetchSlow": fetch_slow,
        "executorIds": executor_ids,
        "executorCount": len(executor_ids),
        "hasSpill": memory_spilled > 0 or disk_spilled > 0,
        "tasks": normalized,
    }


def normalize_node(node: dict[str, Any]) -> dict[str, Any]:
    metrics: dict[str, dict[str, Any]] = {}
    stage_ids: set[int] = set()
    for metric in node.get("metrics") or []:
        name = metric.get("name")
        if not name:
            continue
        raw = metric.get("value")
        metrics[name] = {"raw": raw, "value": parse_quantity(raw, metric_kind(name))}
        if raw:
            stage_ids.update(int(sid) for sid, _ in STAGE_IN_METRIC_RE.findall(str(raw)))
    return {
        "nodeId": node.get("nodeId"),
        "nodeName": node.get("nodeName"),
        "metrics": metrics,
        "stageIds": sorted(stage_ids),
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
        if any(word in str(n.get("nodeName", "")).lower() for word in ("join", "cartesianproduct"))
    ]
    aggregates = [n for n in nodes if "aggregate" in str(n.get("nodeName", "")).lower()]
    window_nodes = [n for n in nodes if "window" in str(n.get("nodeName", "")).lower()]
    write_nodes = [
        n for n in nodes
        if any(word in str(n.get("nodeName", "")).lower()
               for word in ("insertintohadoopfsrelation", "fileformatwriter", "insertinto"))
    ]

    scan_rows = [node_metric(n, "number of output rows") or 0 for n in scans]
    join_rows = [node_metric(n, "number of output rows") or 0 for n in joins]
    max_scan_rows = max(scan_rows) if scan_rows else 0
    max_join_rows = max(join_rows) if join_rows else 0

    exchange_info = []
    stage_to_nodes: dict[int, set[str]] = {}
    for node in nodes:
        for sid in node.get("stageIds") or []:
            stage_to_nodes.setdefault(sid, set()).add(str(node.get("nodeName")))

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

    scan_layout = []
    for node in scans:
        files = node_metric(node, "number of files read")
        size = node_metric(node, "size of files read")
        rows = node_metric(node, "number of output rows")
        scan_ms = node_metric(node, "scan time")
        avg_file = (size / files) if files and size else None
        scan_layout.append({
            "table": node.get("nodeName"),
            "files": files,
            "size": size,
            "rows": rows,
            "scanMs": scan_ms,
            "avgFileSize": avg_file,
            "smallFiles": bool(files and files >= SMALL_FILE_COUNT and avg_file and avg_file < SMALL_FILE_SIZE),
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
    distinct_two_phase = bool(re.search(r"partial_count\s*\(\s*distinct", lowered))
    partial_agg = "partial_" in lowered
    sort_merge_join = any("sortmergejoin" in str(n.get("nodeName", "")).lower() for n in joins)
    broadcast_join = any("broadcast" in str(n.get("nodeName", "")).lower() for n in joins)

    table_names = sorted(set(
        match.group(1) for match in re.finditer(r"scan\s+parquet\s+(\S+)", plan_text, re.I)
    ))
    exchange_keys = sorted(set(re.findall(r"hashpartitioning\(([^)]*)\)", plan_text, re.I)))
    small_files = any(item["smallFiles"] for item in scan_layout)
    predicate_pruning = bool(
        max_scan_rows >= SCAN_PRUNING_ROWS
        and "sample" not in lowered
        and "partitionfilters: []" in lowered
        and "pushedfilters: []" in lowered
    )
    write_plan = bool(write_nodes) or "insertintohadoopfsrelation" in lowered

    return {
        "nodes": nodes,
        "scans": scans,
        "exchanges": exchanges,
        "joins": joins,
        "aggregates": aggregates,
        "windowNodes": window_nodes,
        "writeNodes": write_nodes,
        "scanRows": scan_rows,
        "joinRows": join_rows,
        "maxScanRows": max_scan_rows,
        "maxJoinRows": max_join_rows,
        "exchangeInfo": exchange_info,
        "exchangeKeys": exchange_keys,
        "scanLayout": scan_layout,
        "stageToNodes": {sid: sorted(names) for sid, names in stage_to_nodes.items()},
        "overPartitioned": over_partitioned,
        "joinInflation": join_inflation,
        "distinct": distinct,
        "distinctTwoPhase": distinct_two_phase,
        "partialAggregation": partial_agg,
        "sortMergeJoin": sort_merge_join,
        "broadcastJoin": broadcast_join,
        "smallFiles": small_files,
        "predicatePruning": predicate_pruning,
        "writePlan": write_plan,
        "tableNames": table_names,
        "planText": plan_text,
    }


def analyze_config(environment: dict[str, Any] | None, plan: dict[str, Any]) -> dict[str, Any]:
    props = normalize_environment(environment)
    aqe = props.get("spark.sql.adaptive.enabled", "true").lower()
    skew_enabled = props.get("spark.sql.adaptive.skewJoin.enabled", "true").lower()
    skew_factor = parse_quantity(props.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5"))
    skew_threshold = parse_quantity(
        props.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m"), "bytes"
    )
    shuffle_partitions = parse_quantity(props.get("spark.sql.shuffle.partitions"))
    broadcast_threshold = parse_quantity(
        props.get("spark.sql.autoBroadcastJoinThreshold", "10m"), "bytes"
    )
    dynamic_enabled = props.get("spark.dynamicAllocation.enabled", "false").lower()
    min_executors = parse_quantity(props.get("spark.dynamicAllocation.minExecutors", "0"))
    max_executors = parse_quantity(
        props.get("spark.dynamicAllocation.maxExecutors") or props.get("spark.executor.instances")
    )
    executor_cores = parse_quantity(props.get("spark.executor.cores"))
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
        "maxExecutors": max_executors,
        "executorCores": executor_cores,
        "executorIdleTimeoutSec": idle_timeout,
        "eventLogEnabled": props.get("spark.eventLog.enabled"),
        "eventLogDir": props.get("spark.eventLog.dir"),
        "props": props,
    }


def analyze_executor_info(executors: list[dict[str, Any]], stages: list[dict[str, Any]],
                          config: dict[str, Any]) -> dict[str, Any]:
    active = [e for e in executors or [] if str(e.get("id")) != "driver"]
    per_executor: dict[str, dict[str, Any]] = {}
    for stage in stages:
        for task in stage.get("tasks") or []:
            eid = str(task.get("executorId") or "")
            if not eid:
                continue
            item = per_executor.setdefault(eid, {
                "executorId": eid, "tasks": 0, "durationMs": 0.0,
                "gcMs": 0.0, "host": task.get("host"),
            })
            item["tasks"] += 1
            item["durationMs"] += task.get("duration") or 0
            item["gcMs"] += task.get("gc") or 0
    task_executor_count = len(per_executor)
    active_count = len(active)
    max_executors = config.get("maxExecutors")
    observed = max(active_count, task_executor_count)
    at_max = bool(max_executors and observed >= max_executors * 0.8)
    return {
        "activeCount": active_count,
        "taskExecutorCount": task_executor_count,
        "observedCount": observed,
        "maxExecutors": max_executors,
        "executorCores": config.get("executorCores"),
        "atMax": at_max,
        "perExecutor": sorted(per_executor.values(), key=lambda x: x["durationMs"], reverse=True),
    }


def analyze_jobs_stages(context: dict[str, Any], stages: list[dict[str, Any]],
                        attempts: list[dict[str, Any]]) -> dict[str, Any]:
    jobs = context.get("jobs") or []
    stage_ids = {s.get("stageId") for s in attempts}
    return {
        "jobCount": len(jobs),
        "stageCount": len(stage_ids),
        "taskCount": sum(s.get("numTasks") or s.get("taskCount") or 0 for s in stages),
        "attemptCount": len(attempts),
    }


def stage_tasks(task_lists: dict[Any, list[dict[str, Any]]], stage_id: int,
                attempt: int) -> list[dict[str, Any]]:
    for key in ((stage_id, attempt), f"{stage_id}/{attempt}", f"{stage_id}.{attempt}",
                stage_id, str(stage_id)):
        if key in task_lists:
            return task_lists[key]
    return []


def add_finding(findings: list[dict[str, Any]], severity: str, finding_type: str,
                title: str, evidence: dict[str, Any], confidence: str = "high") -> None:
    findings.append({
        "severity": severity,
        "type": finding_type,
        "title": title,
        "evidence": evidence,
        "confidence": confidence,
    })


def stage_cause(stage: dict[str, Any], executor_info: dict[str, Any]) -> str | None:
    """给一个 stage 选择一个主要慢因，避免同时报五条。"""
    if stage["isSkewed"]:
        return "data_skew"
    if stage["mapSkew"]:
        return "map_side_skew"
    if stage["taskTooHeavy"]:
        return "task_too_heavy"
    if stage["fetchSlow"]:
        return "shuffle_fetch_slow"
    if (
        stage["taskCount"] >= PARALLELISM_MIN_TASKS
        and stage["balanced"]
        and (stage.get("launchSpread") or 0) >= LAUNCH_SPREAD_MS
    ):
        if executor_info.get("atMax"):
            return "parallelism_cap"
        return "resource_shortage"
    if stage["schedulingSuspected"]:
        return "scheduling"
    return None


def build_recommendations(findings: list[dict[str, Any]], plan: dict[str, Any],
                          config: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    types = {f["type"] for f in findings}

    def add(rid, title, reason, effect, risk, constraints):
        rows.append({
            "id": rid, "title": title, "reason": reason,
            "expectedEffect": effect, "risk": risk, "constraints": constraints,
        })

    if "data_skew" in types:
        add("broadcast_or_salt", "优先广播小表，或对热点 key 做 salting",
            "存在单 task 热点，shuffle join 会把热点 key 压到一个分区。",
            "消除 reduce 侧热点和 spill，缩短 stage 墙钟。",
            "广播需要 executor 内存足够；salting 需要膨胀另一侧，增加数据量。",
            "先确认 join 类型；left join 只能广播右表，right join 只能广播左表，full join 不适用。")
        add("pre_aggregate", "能预聚合就先预聚合，再 join",
            "如果业务允许，先把事实表按 join key 聚合，可把热点 key 压缩成一行。",
            "join 输入显著变小，热点消失。",
            "只适用于 count/sum 等可合并聚合，且维度表 key 唯一时要调整计数逻辑。",
            "聚合会改变明细粒度，需确认下游只使用聚合结果。")
    if "map_side_skew" in types:
        add("fix_map_input", "处理 map 端输入不均",
            "map 阶段输入记录/字节明显不均，通常是不可切分文件、大量小文件或异常宽记录。",
            "让 map 阶段 task 负载均衡。",
            "合并小文件或改格式可能影响上游写入链路。",
            "先确认文件格式是否可切分、是否存在单个超大文件。")
    if "task_too_heavy" in types:
        add("reduce_task_payload", "降低单 task 数据量或提高并行度",
            "所有 task 都慢且均衡，单 task 输入偏大。",
            "缩短单 task 运行时间。",
            "分区过多会增加调度和文件开销。",
            "需要同时看输入文件布局和业务逻辑复杂度。")
    if "shuffle_fetch_slow" in types:
        add("shuffle_fetch", "检查 shuffle fetch 和上游 task 数",
            "fetchWait/remote reqs 占 task 时间比例高。",
            "减少 fetch 等待和 stage 重试。",
            "只调大超时可能掩盖真实网络或磁盘问题。",
            "结合上游 map 数、shuffle 大小和网络/磁盘指标判断。")
    if "parallelism_cap" in types:
        add("raise_max_executors", "提高 executor 上限或降低单 stage task 数",
            "task 数很多但执行均衡，launch 间隔大，executor 数接近上限。",
            "提高并行度，缩短 stage 墙钟。",
            "受队列配额和集群资源限制。",
            "先确认队列是否有足够资源，避免只改参数不扩容。")
    if "resource_shortage" in types:
        add("queue_resources", "处理队列/集群资源不足",
            "task 均衡但 launch 间隔大，executor 数低于上限或申请不到资源。",
            "提高任务并行度。",
            "需要跨团队协调资源。",
            "可考虑错峰、提高优先级、申请扩容或使用弹性资源。")
    if "stage_retry" in types:
        add("stage_retry", "排查 stage 失败重试",
            "存在失败 attempt、失败 task 或重试。",
            "减少重复计算和整体耗时。",
            "只调大超时可能掩盖数据或网络问题。",
            "先看 fetch failure、OOM、节点异常和 shuffle 损坏。")
    if "small_files_scan" in types:
        add("compact_small_files", "合并小文件或调整写入分区",
            "扫描文件数多、平均文件小，读放大和 task 开销高。",
            "减少文件打开和 task 调度开销。",
            "合并需要额外存储和调度窗口。",
            "先确认上游是否持续产生小文件。")
    if "predicate_pruning" in types:
        add("partition_pruning", "检查分区裁剪和谓词下推",
            "扫描行数大且没有分区/谓词过滤。",
            "减少扫描数据量。",
            "需要业务分区字段和可下推的过滤条件。",
            "确认 SQL 是否真的需要全表扫描。")
    if "window_skew_risk" in types:
        add("two_stage_window", "窗口 TopN 考虑两阶段 ROW_NUMBER",
            "窗口分区 key 可能是热点。",
            "把热点分区的 TopN 拆到多个 task。",
            "两阶段会多一次聚合，SQL 复杂度上升。",
            "先确认业务只取 TopN，且排序字段稳定。")
    if "distinct_skew_risk" in types:
        add("distinct_rewrite", "检查 count(distinct) 计划，必要时改写",
            "distinct 没有把去重字段放进第一层 shuffle key，可能出现热点。",
            "降低单 task 去重压力。",
            "改写会影响 null 和重复值语义。",
            "优先看计划是否已经是两阶段去重；已是两阶段时不要重复改写。")
    if "dynamic_partition_write" in types:
        add("dynamic_partition_write", "检查动态分区写入",
            "写阶段 task 多、输出文件小，可能存在 n×m 小文件。",
            "减少写阶段文件操作和倾斜。",
            "调整分区策略可能影响下游读取。",
            "确认分区键分布和写入并发。")
    if "aqe_threshold_miss" in types:
        add("aqe_tuning", "不要默认 AQE 会拆分这个热点",
            "热点分区压缩后大小低于 AQE 阈值，skew join 不会触发。",
            "改用广播/预聚合/salting，而不是只打开 AQE。",
            "强行降低阈值可能误伤其他分区。",
            "需要实际对比 Final Plan 和 task 分布。")
    if "spill" in types and "data_skew" not in types:
        add("memory_or_plan", "检查内存和聚合/排序状态",
            "存在 spill，但没有明显单 key 热点。",
            "减少磁盘 spill 和 GC。",
            "单纯加内存可能掩盖计划问题。",
            "先看 peak memory、spill 所在算子和 GC 比例。")
    return rows


def analyze_execution(context: dict[str, Any],
                      log_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    sql = context.get("sql") or {}
    attempts_raw = context.get("stage_attempts") or context.get("stages") or []
    task_lists = context.get("task_lists") or {}
    environment = context.get("environment") or {}

    analyzed_attempts = []
    for stage in attempts_raw:
        stage_id = stage.get("stageId")
        attempt = stage.get("attemptId", 0)
        tasks = stage_tasks(task_lists, stage_id, attempt)
        analyzed_attempts.append(analyze_stage(stage, tasks))

    selected: dict[Any, dict[str, Any]] = {}
    for stage in analyzed_attempts:
        sid = stage.get("stageId")
        rank = (stage.get("status") == "COMPLETE", stage.get("attemptId") or 0)
        current = selected.get(sid)
        if current is None:
            selected[sid] = stage
        else:
            current_rank = (current.get("status") == "COMPLETE", current.get("attemptId") or 0)
            if rank > current_rank:
                selected[sid] = stage
    stages = sorted(selected.values(), key=lambda s: (s.get("stageId") or 0, s.get("attemptId") or 0))

    plan = analyze_plan(sql, log_summary)
    config = analyze_config(environment, plan)
    executor_info = analyze_executor_info(context.get("executors") or [], stages, config)
    jobs_info = analyze_jobs_stages(context, stages, analyzed_attempts)

    findings: list[dict[str, Any]] = []
    specific_causes = {}
    for stage in stages:
        cause = stage_cause(stage, executor_info)
        if cause:
            specific_causes[stage["stageId"]] = cause

    for stage in stages:
        cause = specific_causes.get(stage["stageId"])
        if cause == "data_skew":
            hot = stage["hotTask"] or {}
            add_finding(findings, "HIGH", "data_skew",
                        f"Stage {stage['stageId']} 存在数据倾斜：task {hot.get('taskId')} 处理 {hot.get('recordsRead', 0):,.0f} 条记录",
                        {
                            "stageId": stage["stageId"], "taskId": hot.get("taskId"),
                            "records": hot.get("recordsRead"), "durationMs": hot.get("duration"),
                            "medianRecords": stage["records"].get("p50"),
                            "medianDurationMs": stage["duration"].get("p50"),
                            "recordsRatio": stage["ratioRecords"], "durationRatio": stage["ratioDuration"],
                            "maxShare": stage["maxShare"], "memorySpilled": stage["memorySpilled"],
                            "diskSpilled": stage["diskSpilled"],
                        })
        elif cause == "map_side_skew":
            add_finding(findings, "HIGH", "map_side_skew",
                        f"Stage {stage['stageId']} 存在 map 端输入倾斜",
                        {
                            "stageId": stage["stageId"], "inputRatio": stage["inputRatio"],
                            "inputShare": stage["inputShare"],
                            "maxInputRecords": stage["inputRecordsStats"].get("max"),
                            "medianInputRecords": stage["inputRecordsStats"].get("p50"),
                            "taskCount": stage["taskCount"],
                        })
        elif cause == "task_too_heavy":
            add_finding(findings, "MEDIUM", "task_too_heavy",
                        f"Stage {stage['stageId']} 所有 task 偏重且均衡",
                        {
                            "stageId": stage["stageId"],
                            "medianInputRecords": stage["inputRecordsStats"].get("p50"),
                            "medianInputBytes": stage["inputBytesStats"].get("p50"),
                            "taskCount": stage["taskCount"],
                        })
        elif cause == "shuffle_fetch_slow":
            add_finding(findings, "MEDIUM", "shuffle_fetch_slow",
                        f"Stage {stage['stageId']} 的 shuffle fetch 占比偏高",
                        {
                            "stageId": stage["stageId"], "fetchRatio": stage["fetchRatio"],
                            "remoteReqsRatio": stage["remoteReqsRatio"],
                            "fetchWaitMs": stage["fetchWait"].get("max"),
                            "upstreamTasks": stage["shuffleReadRecords"],
                        })
        elif cause == "parallelism_cap":
            add_finding(findings, "MEDIUM", "parallelism_cap",
                        f"Stage {stage['stageId']} 可能被 executor 上限限制并行度",
                        {
                            "stageId": stage["stageId"], "taskCount": stage["taskCount"],
                            "launchSpreadMs": stage["launchSpread"],
                            "maxLaunchDelayMs": stage["maxLaunchDelay"],
                            "executorCount": executor_info.get("observedCount"),
                            "maxExecutors": executor_info.get("maxExecutors"),
                        })
        elif cause == "resource_shortage":
            add_finding(findings, "MEDIUM", "resource_shortage",
                        f"Stage {stage['stageId']} 可能受队列/集群资源限制",
                        {
                            "stageId": stage["stageId"], "taskCount": stage["taskCount"],
                            "launchSpreadMs": stage["launchSpread"],
                            "maxLaunchDelayMs": stage["maxLaunchDelay"],
                            "executorCount": executor_info.get("observedCount"),
                            "maxExecutors": executor_info.get("maxExecutors"),
                        })
        elif cause == "scheduling":
            add_finding(findings, "MEDIUM", "scheduling",
                        f"Stage {stage['stageId']} 墙钟 {stage['wallMs']/1000:.2f}s，但最长 task 只有 {stage['duration'].get('max', 0)/1000:.2f}s",
                        {
                            "stageId": stage["stageId"], "wallMs": stage["wallMs"],
                            "maxTaskDurationMs": stage["duration"].get("max"),
                            "maxLaunchDelayMs": stage["maxLaunchDelay"],
                            "schedulingGapMs": stage["schedulingGap"],
                        })
        if stage["coldDeserializeTasks"]:
            task = stage["coldDeserializeTasks"][0]
            add_finding(findings, "MEDIUM", "cold_start",
                        f"Stage {stage['stageId']} 的慢 task 里存在冷 executor：反序列化 {task['deserialize']:.0f}ms",
                        {
                            "stageId": stage["stageId"], "taskId": task.get("taskId"),
                            "durationMs": task.get("duration"), "deserializeMs": task.get("deserialize"),
                            "records": task.get("recordsRead"),
                        })

    total_memory_spill = sum(s["memorySpilled"] for s in stages)
    total_disk_spill = sum(s["diskSpilled"] for s in stages)
    if total_memory_spill or total_disk_spill:
        add_finding(findings,
                    "MEDIUM" if total_memory_spill + total_disk_spill < 1024**3 else "HIGH",
                    "spill", "执行过程中发生 spill",
                    {"memoryBytesSpilled": total_memory_spill,
                     "diskBytesSpilled": total_disk_spill,
                     "stages": [s["stageId"] for s in stages if s["hasSpill"]]})

    retried = [
        s for s in analyzed_attempts
        if s.get("status") in ("FAILED", "KILLED")
        or (s.get("numFailedTasks") or 0) > 0
        or s.get("attemptId", 0) > 0
    ]
    if retried:
        add_finding(findings, "MEDIUM", "stage_retry", "存在 stage 失败或重试",
                    {"stages": [{"stageId": s["stageId"], "attemptId": s["attemptId"],
                                 "status": s.get("status"), "failedTasks": s.get("numFailedTasks")}
                                for s in retried[:10]]})

    if jobs_info["jobCount"] > MANY_JOBS or jobs_info["stageCount"] > MANY_STAGES:
        add_finding(findings, "LOW", "too_many_jobs_stages", "Job/Stage 数量偏多",
                    jobs_info, confidence="medium")

    if plan["joinInflation"]:
        add_finding(findings, "HIGH", "join_inflation", "Join 输出行数明显大于扫描输入行数，存在膨胀风险",
                    {"maxScanRows": plan["maxScanRows"], "maxJoinRows": plan["maxJoinRows"],
                     "ratio": (plan["maxJoinRows"] / plan["maxScanRows"]) if plan["maxScanRows"] else None},
                    confidence="medium")
    if plan["overPartitioned"]:
        worst = max((item for item in plan["exchangeInfo"] if item.get("partitions")),
                    key=lambda item: item["partitions"])
        add_finding(findings, "LOW", "over_partitioning",
                    f"Shuffle 分区数 {worst['partitions']:.0f} 对当前数据量过大",
                    {"partitions": worst["partitions"], "shuffleBytes": worst["shuffleBytes"],
                     "bytesPerPartition": worst["bytesPerPartition"], "blocks": worst["blocks"]})
    if plan["smallFiles"]:
        worst = max((item for item in plan["scanLayout"] if item.get("smallFiles")),
                    key=lambda item: item.get("files") or 0)
        add_finding(findings, "MEDIUM", "small_files_scan", "扫描文件数多且平均文件较小",
                    {"files": worst["files"], "avgFileSize": worst["avgFileSize"],
                     "table": worst["table"]}, confidence="medium")
    if plan["predicatePruning"]:
        add_finding(findings, "LOW", "predicate_pruning", "大表扫描没有分区/谓词过滤",
                    {"maxScanRows": plan["maxScanRows"]}, confidence="medium")

    has_skew = any(f["type"] == "data_skew" for f in findings)
    if plan["distinct"]:
        if plan["distinctTwoPhase"] and not has_skew:
            add_finding(findings, "INFO", "distinct_skew_resistant",
                        "count(DISTINCT) 采用两阶段去重，热点 key 被 distinct 字段摊开",
                        {"distinct": True, "partialAggregation": plan["partialAggregation"],
                         "maxRecordsRatio": max((s["ratioRecords"] or 0) for s in stages) if stages else None})
        elif not plan["distinctTwoPhase"]:
            add_finding(findings, "MEDIUM", "distinct_skew_risk",
                        "count(DISTINCT) 未看到两阶段去重，存在热点风险",
                        {"distinct": True, "distinctTwoPhase": False})
    if plan["partialAggregation"] and not has_skew:
        add_finding(findings, "INFO", "partial_aggregation_hides_skew",
                    "计划包含 map 端预聚合，可能掩盖了源数据的 key 倾斜",
                    {"partialAggregation": True,
                     "maxRecordsRatio": max((s["ratioRecords"] or 0) for s in stages) if stages else None})
    if plan["windowNodes"] and has_skew:
        add_finding(findings, "MEDIUM", "window_skew_risk",
                    "窗口函数的分区 key 可能落在倾斜 stage 上",
                    {"windowNodes": [n.get("nodeName") for n in plan["windowNodes"]],
                     "stageToNodes": plan["stageToNodes"]}, confidence="medium")
    if plan["writePlan"]:
        write_stage = next((s for s in stages if s["shuffleReadRecords"] == 0 and s["taskCount"] > 0), None)
        if write_stage and write_stage["taskCount"] >= 50 and (write_stage["shuffleWriteBytes"] or 0) < 64 * 1024**2:
            add_finding(findings, "LOW", "dynamic_partition_write",
                        "写阶段 task 多、输出偏小，可能存在动态分区小文件",
                        {"stageId": write_stage["stageId"], "taskCount": write_stage["taskCount"],
                         "shuffleWriteBytes": write_stage["shuffleWriteBytes"]}, confidence="low")

    if has_skew:
        hot_stage = next((s for s in stages if s["isSkewed"]), None)
        if hot_stage:
            hot_bytes = (hot_stage["hotTask"] or {}).get("readBytes")
            median_bytes = hot_stage["readBytes"].get("p50")
            if hot_bytes and median_bytes and config.get("skewPartitionFactor") and config.get("skewPartitionThresholdBytes"):
                ratio = hot_bytes / median_bytes if median_bytes else None
                if ratio is not None and (ratio < config["skewPartitionFactor"]
                                          or hot_bytes < config["skewPartitionThresholdBytes"]):
                    add_finding(findings, "MEDIUM", "aqe_threshold_miss",
                                "AQE skew join 阈值可能无法识别这个热点",
                                {"hotPartitionBytes": hot_bytes, "medianPartitionBytes": median_bytes,
                                 "sizeRatio": ratio, "skewPartitionFactor": config["skewPartitionFactor"],
                                 "skewPartitionThresholdBytes": config["skewPartitionThresholdBytes"]})

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
        "attempts": analyzed_attempts,
        "plan": {
            "tableNames": plan["tableNames"],
            "sortMergeJoin": plan["sortMergeJoin"],
            "broadcastJoin": plan["broadcastJoin"],
            "distinct": plan["distinct"],
            "distinctTwoPhase": plan["distinctTwoPhase"],
            "partialAggregation": plan["partialAggregation"],
            "joinInflation": plan["joinInflation"],
            "overPartitioned": plan["overPartitioned"],
            "maxScanRows": plan["maxScanRows"],
            "maxJoinRows": plan["maxJoinRows"],
            "exchangeInfo": plan["exchangeInfo"],
            "exchangeKeys": plan["exchangeKeys"],
            "scanLayout": plan["scanLayout"],
            "stageToNodes": plan["stageToNodes"],
            "smallFiles": plan["smallFiles"],
            "predicatePruning": plan["predicatePruning"],
            "windowNodes": [n.get("nodeName") for n in plan["windowNodes"]],
            "writePlan": plan["writePlan"],
            "planText": plan["planText"],
        },
        "config": config,
        "executors": executor_info,
        "jobsStages": jobs_info,
        "findings": findings,
        "recommendations": recommendations,
        "limits": [
            "看不到真实数据内容，热点 key 的具体值需要单独查询。",
            "YARN 队列排队和容器分配原因需要 RM/Kyuubi 日志确认。",
            "UDF/自定义代码问题需要代码和 executor 日志。",
        ],
    }
