"""Spark SQL 执行指标分析与归因（门面模块）。

v2 的实现在 metrics / stage_analysis / plan_analysis / runtime_analysis / findings。
本模块保留 analyze_execution 主流程，并 re-export 旧的模块级符号。
"""
from __future__ import annotations

import re
import statistics
from typing import Any

from spark_ui import epoch_ms
from metrics import (
    BYTE_UNITS,
    FETCH_WAIT_RATIO,
    HEAVY_INPUT_BYTES,
    HEAVY_INPUT_RECORDS,
    LAUNCH_SPREAD_MS,
    MANY_JOBS,
    MANY_STAGES,
    NUMBER_RE,
    PARALLELISM_MIN_TASKS,
    SCAN_PRUNING_ROWS,
    SMALL_FILE_COUNT,
    SMALL_FILE_SIZE,
    STAGE_IN_METRIC_RE,
    TIME_UNITS,
    UNIT_RE,
    first_number,
    metric_kind,
    metric_raw,
    metric_value,
    normalize_environment,
    parse_quantity,
    percentile,
    stats,
)
from stage_analysis import analyze_stage, normalize_task
from plan_analysis import analyze_plan, node_metric, normalize_node
from runtime_analysis import (
    analyze_config,
    analyze_executor_info,
    analyze_jobs_stages,
    stage_tasks,
)
from findings import add_finding, build_recommendations, stage_cause

__all__ = [
    "UNIT_RE", "NUMBER_RE", "STAGE_IN_METRIC_RE", "BYTE_UNITS", "TIME_UNITS",
    "PARALLELISM_MIN_TASKS", "LAUNCH_SPREAD_MS", "HEAVY_INPUT_RECORDS",
    "HEAVY_INPUT_BYTES", "FETCH_WAIT_RATIO", "SMALL_FILE_COUNT", "SMALL_FILE_SIZE",
    "MANY_JOBS", "MANY_STAGES", "SCAN_PRUNING_ROWS",
    "parse_quantity", "first_number", "metric_kind", "metric_raw", "metric_value",
    "normalize_environment", "percentile", "stats",
    "normalize_task", "analyze_stage", "normalize_node", "node_metric", "analyze_plan",
    "analyze_config", "analyze_executor_info", "analyze_jobs_stages", "stage_tasks",
    "add_finding", "stage_cause", "build_recommendations", "analyze_execution",
    "epoch_ms",
]

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
