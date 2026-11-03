"""运行环境、executor、job/stage 统计。"""
from __future__ import annotations

from typing import Any

from metrics import normalize_environment, parse_quantity

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
