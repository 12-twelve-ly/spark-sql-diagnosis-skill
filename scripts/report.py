"""把分析结果渲染为中文 Markdown 报告。"""
from __future__ import annotations

from typing import Any


SEVERITY_LABEL = {
    "HIGH": "高",
    "MEDIUM": "中",
    "LOW": "低",
    "INFO": "提示",
}


def fmt_bytes(value: Any) -> str:
    if value is None:
        return "—"
    number = float(value)
    for unit, scale in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024), ("B", 1)):
        if number >= scale or unit == "B":
            return f"{number / scale:.2f} {unit}"
    return f"{number:.0f} B"


def fmt_ms(value: Any) -> str:
    if value is None:
        return "—"
    number = float(value)
    if number >= 1000:
        return f"{number / 1000:.2f}s"
    return f"{number:.0f}ms"


def fmt_num(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return str(value)


def fmt_ratio(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value):.2f}x"


def format_evidence(evidence: dict[str, Any]) -> str:
    keys = [
        ("taskId", "task"),
        ("records", "records"),
        ("medianRecords", "median records"),
        ("durationMs", "duration"),
        ("deserializeMs", "deserialize"),
        ("recordsRatio", "records ratio"),
        ("durationRatio", "duration ratio"),
        ("maxShare", "max share"),
        ("wallMs", "wall"),
        ("maxLaunchDelayMs", "launch delay"),
        ("memoryBytesSpilled", "memory spill"),
        ("diskBytesSpilled", "disk spill"),
        ("partitions", "partitions"),
        ("hotPartitionBytes", "hot partition bytes"),
        ("sizeRatio", "size ratio"),
        ("maxScanRows", "scan rows"),
        ("maxJoinRows", "join rows"),
        ("maxRecordsRatio", "max records ratio"),
        ("distinct", "distinct"),
        ("partialAggregation", "partial agg"),
    ]
    parts = []
    for key, label in keys:
        if key not in evidence or evidence[key] is None:
            continue
        value = evidence[key]
        if key.endswith("Bytes") or key.endswith("Spilled"):
            rendered = fmt_bytes(value)
        elif key.endswith("Ms"):
            rendered = fmt_ms(value)
        elif key in ("records", "medianRecords", "maxScanRows", "maxJoinRows", "partitions"):
            rendered = fmt_num(value)
        elif key in ("recordsRatio", "durationRatio", "maxShare", "sizeRatio", "maxRecordsRatio"):
            rendered = fmt_ratio(value)
        elif key in ("distinct", "partialAggregation"):
            rendered = str(bool(value)).lower()
        else:
            rendered = str(value)
        parts.append(f"{label}={rendered}")
    return "；".join(parts)


def render_markdown(result: dict[str, Any], *,
                    source: str = "Spark UI REST",
                    log_summary: dict[str, Any] | None = None) -> str:
    execution = result.get("execution") or {}
    stages = result.get("stages") or []
    findings = result.get("findings") or []
    plan = result.get("plan") or {}
    config = result.get("config") or {}
    recommendations = result.get("recommendations") or []

    lines = [
        "# Spark SQL 诊断报告",
        "",
        f"- Application：`{result.get('appId') or '—'}`",
        f"- SQL Execution：`{execution.get('id') or '—'}`",
        f"- 状态：{execution.get('status') or '—'}",
        f"- 耗时：{fmt_ms(execution.get('durationMs'))}",
        f"- 提交时间：{execution.get('submissionTime') or '—'}",
        f"- 证据来源：{source}",
        "",
    ]

    if execution.get("description"):
        lines.extend([
            "## SQL",
            "",
            "```sql",
            str(execution["description"]).strip(),
            "```",
            "",
        ])

    lines.extend(["## 结论先行", ""])
    if findings:
        lines.append("| 严重度 | 类型 | 结论 | 证据 |")
        lines.append("|---|---|---|---|")
        for finding in findings:
            lines.append(
                "| {severity} | {type} | {title} | {evidence} |".format(
                    severity=SEVERITY_LABEL.get(finding.get("severity"), finding.get("severity")),
                    type=finding.get("type"),
                    title=str(finding.get("title", "")).replace("|", "\\|"),
                    evidence=format_evidence(finding.get("evidence") or {}).replace("|", "\\|"),
                )
            )
    else:
        lines.append("未发现明显的数据倾斜、spill 或调度异常。")
    lines.append("")

    lines.extend(["## 执行时间线", ""])
    if stages:
        lines.append("| Stage | tasks | 墙钟 | task 运行 | 启动延迟(max) | 输入 | shuffle 读/写 | spill |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for stage in stages:
            lines.append(
                "| {sid} | {tasks} | {wall} | {run} | {launch} | {input} | 读 {read}；写 {write} | {spill} |".format(
                    sid=stage.get("stageId"),
                    tasks=stage.get("taskCount") or stage.get("numTasks") or "—",
                    wall=fmt_ms(stage.get("wallMs")),
                    run=fmt_ms(stage.get("executorRunTimeMs")),
                    launch=fmt_ms(stage.get("maxLaunchDelay")),
                    input=f"{fmt_num(stage.get('inputRecords'))} 行 / {fmt_bytes(stage.get('inputBytes'))}",
                    read=f"{fmt_num(stage.get('shuffleReadRecords'))} 行 / {fmt_bytes(stage.get('shuffleReadBytes'))}",
                    write=f"{fmt_num(stage.get('shuffleWriteRecords'))} 行 / {fmt_bytes(stage.get('shuffleWriteBytes'))}",
                    spill=f"{fmt_bytes(stage.get('memorySpilled'))} / {fmt_bytes(stage.get('diskSpilled'))}",
                )
            )
    else:
        lines.append("没有拉到 stage 数据。")
    lines.append("")

    lines.extend(["## 倾斜与长尾", ""])
    skew_stages = [s for s in stages if s.get("taskCount")]
    if not skew_stages:
        lines.append("没有 task 级数据，无法做分布判断。")
        lines.append("")
    for stage in skew_stages:
        stage_id = stage.get("stageId")
        records = stage.get("records") or {}
        duration = stage.get("duration") or {}
        lines.append(f"### Stage {stage_id}（{stage.get('taskCount')} tasks）")
        lines.append(
            f"- records：中位数 {fmt_num(records.get('p50'))}，最大 {fmt_num(records.get('max'))}，"
            f"max/median {fmt_ratio(stage.get('ratioRecords'))}"
        )
        lines.append(
            f"- duration：中位数 {fmt_ms(duration.get('p50'))}，最大 {fmt_ms(duration.get('max'))}，"
            f"max/median {fmt_ratio(stage.get('ratioDuration'))}"
        )
        lines.append(
            f"- 最大 task 记录占比：{fmt_ratio(stage.get('maxShare'))}；"
            f"启动延迟最大：{fmt_ms(stage.get('maxLaunchDelay'))}"
        )
        if stage.get("isSkewed"):
            lines.append("- 判定：**存在数据倾斜**")
        elif stage.get("reliableDistribution"):
            lines.append("- 判定：未达到倾斜阈值")
        else:
            lines.append("- 判定：task 数少于 4，分布判断不可靠")
        if stage.get("coldDeserializeTasks"):
            task = stage["coldDeserializeTasks"][0]
            lines.append(
                f"- 冷启动干扰：task {task.get('taskId')} 耗时 {fmt_ms(task.get('duration'))}，"
                f"其中反序列化 {fmt_ms(task.get('deserialize'))}，记录数 {fmt_num(task.get('recordsRead'))}"
            )
        tasks = sorted(stage.get("tasks") or [], key=lambda t: t.get("duration") or 0, reverse=True)
        if tasks:
            lines.append("")
            lines.append("| task | partition | executor | host | duration | records | shuffle read | deserialize | spill |")
            lines.append("|---|---|---|---|---|---|---|---|---|")
            for task in tasks[:5]:
                spill = (task.get("memorySpilled") or 0) + (task.get("diskSpilled") or 0)
                lines.append(
                    "| {task} | {partition} | {exec} | {host} | {dur} | {records} | {read} | {deser} | {spill} |".format(
                        task=task.get("taskId"),
                        partition=task.get("index"),
                        exec=task.get("executorId") or "—",
                        host=task.get("host") or "—",
                        dur=fmt_ms(task.get("duration")),
                        records=fmt_num(task.get("recordsRead")),
                        read=fmt_bytes(task.get("readBytes")),
                        deser=fmt_ms(task.get("deserialize")),
                        spill=fmt_bytes(spill),
                    )
                )
        lines.append("")

    lines.extend(["## SQL 计划", ""])
    if plan.get("tableNames"):
        lines.append(f"- 表：{', '.join(f'`{t}`' for t in plan['tableNames'])}")
    lines.append(
        f"- Join 类型：{'SortMergeJoin' if plan.get('sortMergeJoin') else ('BroadcastJoin' if plan.get('broadcastJoin') else '无/未知')}"
    )
    lines.append(f"- distinct：{plan.get('distinct')}；map 端预聚合：{plan.get('partialAggregation')}")
    lines.append(
        f"- scan 输出行数（最大）：{fmt_num(plan.get('maxScanRows'))}；"
        f"join 输出行数（最大）：{fmt_num(plan.get('maxJoinRows'))}"
    )
    for item in plan.get("exchangeInfo") or []:
        lines.append(
            f"- Exchange：{fmt_num(item.get('partitions'))} 分区，"
            f"shuffle {fmt_bytes(item.get('shuffleBytes'))}，"
            f"每分区 {fmt_bytes(item.get('bytesPerPartition'))}"
        )
    if plan.get("planText"):
        lines.extend(["", "```text", plan["planText"].strip()[:6000], "```"])
    lines.append("")

    lines.extend(["## 关键配置", ""])
    lines.append("| 配置 | 值 |")
    lines.append("|---|---|")
    config_rows = [
        ("AQE", config.get("aqeEnabled")),
        ("AQE skew join", config.get("skewJoinEnabled")),
        ("skewedPartitionFactor", config.get("skewPartitionFactor")),
        ("skewedPartitionThreshold", fmt_bytes(config.get("skewPartitionThresholdBytes"))),
        ("shuffle partitions", config.get("shufflePartitions")),
        ("auto broadcast threshold", fmt_bytes(config.get("autoBroadcastJoinThresholdBytes"))),
        ("dynamic allocation", config.get("dynamicAllocationEnabled")),
        ("minExecutors", config.get("minExecutors")),
        ("executor idle timeout", config.get("executorIdleTimeoutSec")),
        ("event log", config.get("eventLogEnabled")),
    ]
    for key, value in config_rows:
        lines.append(f"| {key} | {value if value is not None else '—'} |")
    lines.append("")

    if recommendations:
        lines.extend(["## 建议", ""])
        for index, item in enumerate(recommendations, start=1):
            lines.append(f"{index}. **{item.get('title')}**：{item.get('reason')}")
            lines.append(f"   - 预期：{item.get('expectedEffect')}")
            lines.append(f"   - 风险：{item.get('risk')}")
        lines.append("")

    if log_summary:
        lines.extend(["## 运行日志摘要", ""])
        if log_summary.get("operation_id"):
            lines.append(f"- operation：`{log_summary['operation_id']}`")
        if log_summary.get("query_duration"):
            lines.append(f"- 日志耗时：{log_summary['query_duration']}")
        if log_summary.get("stages"):
            lines.append("| Stage | 日志墙钟 |")
            lines.append("|---|---|")
            for stage in log_summary["stages"]:
                lines.append(f"| {stage.get('stageId')} | {fmt_ms(stage.get('tookMs'))} |")
        lines.append("")

    lines.extend(["## 局限", ""])
    for limit in result.get("limits") or []:
        lines.append(f"- {limit}")
    lines.extend([
        "",
        "---",
        "本报告由 `spark-sql-diagnosis` 生成。指标来自 Spark REST；未读取数据内容。",
        "",
    ])
    return "\n".join(lines)
