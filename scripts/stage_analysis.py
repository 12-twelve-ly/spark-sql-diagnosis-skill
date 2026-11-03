"""Task / Stage 指标归一化与倾斜、调度、fetch 等分析。"""
from __future__ import annotations

from typing import Any

from spark_ui import epoch_ms
from metrics import (
    FETCH_WAIT_RATIO,
    HEAVY_INPUT_BYTES,
    HEAVY_INPUT_RECORDS,
    first_number,
    percentile,
    stats,
)

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
