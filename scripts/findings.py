"""Finding 构建与带约束的解法建议。"""
from __future__ import annotations

from typing import Any

from metrics import LAUNCH_SPREAD_MS, PARALLELISM_MIN_TASKS

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
