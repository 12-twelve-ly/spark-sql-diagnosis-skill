"""Spark SQL 诊断 CLI。

用法示例：
    python scripts/spark_diagnose.py --url http://host:8088/proxy/application_x --exec-id 94 \
        --sql-file query.sql --log run.log --out report.md --json report.json

只读：不执行 SQL，不修改集群。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from analyze import analyze_execution
from report import render_markdown
from spark_ui import SparkClient, SparkUIError, resolve_endpoint

STAGE_START_RE = re.compile(r"Stage (\d+)\.(\d+) started with (\d+) tasks")
STAGE_FINISH_RE = re.compile(r"Finished stage: Stage\((\d+), (\d+)\).*?Took: (\d+) msec")
STATS_RE = re.compile(
    r"(task runtime|shuffle bytes written|remote bytes read):"
    r"\(count:\s*(\d+),\s*mean:\s*([\d.]+).*?max:\s*([\d.]+).*?min:\s*([\d.]+)\)"
)
TIMESTAMP_RE = re.compile(r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def read_text(path: str | None) -> str | None:
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8", errors="replace")


def parse_log(path: str) -> dict[str, Any]:
    """解析 Kyuubi/Spark Driver 日志中的关键信息。"""
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    result: dict[str, Any] = {
        "path": path,
        "app_id": None,
        "ui_urls": [],
        "operation_id": None,
        "query_duration": None,
        "stages": [],
        "jobs": [],
        "final_plan": None,
        "explain": None,
    }

    match = re.search(r"application ID:\s*(application_\d+_\d+)", text)
    if match:
        result["app_id"] = match.group(1)

    match = re.search(r"application web UI:\s*(.+)", text)
    if match:
        urls = [u.strip().rstrip("、,，") for u in re.split(r"[,，、]", match.group(1)) if u.strip()]
        result["ui_urls"] = urls

    match = re.search(r"operation=([\w-]{8,})", text)
    if match:
        result["operation_id"] = match.group(1)

    durations = re.findall(r"time taken:\s*([\d.]+)\s*seconds", text)
    if durations:
        result["query_duration"] = f"{durations[-1]}s"

    stage_map: dict[int, dict[str, Any]] = {}
    current_stage: int | None = None
    lines = text.splitlines()
    for line in lines:
        start = STAGE_START_RE.search(line)
        if start:
            stage_id = int(start.group(1))
            current_stage = stage_id
            stage_map.setdefault(stage_id, {
                "stageId": stage_id,
                "attempt": int(start.group(2)),
                "numTasks": int(start.group(3)),
                "tookMs": None,
                "stats": {},
            })
            continue
        finish = STAGE_FINISH_RE.search(line)
        if finish:
            stage_id = int(finish.group(1))
            current_stage = stage_id
            stage = stage_map.setdefault(stage_id, {
                "stageId": stage_id,
                "attempt": int(finish.group(2)),
                "numTasks": None,
                "tookMs": None,
                "stats": {},
            })
            stage["attempt"] = int(finish.group(2))
            stage["tookMs"] = int(finish.group(3))
            continue
        stats = STATS_RE.search(line)
        if stats and current_stage is not None:
            name = stats.group(1)
            stage_map.setdefault(current_stage, {
                "stageId": current_stage,
                "attempt": 0,
                "numTasks": None,
                "tookMs": None,
                "stats": {},
            })["stats"][name] = {
                "count": int(stats.group(2)),
                "mean": float(stats.group(3)),
                "max": float(stats.group(4)),
                "min": float(stats.group(5)),
            }
            continue
        job = re.search(r"Job (\d+) finished:.*?took ([\d.]+) s", line)
        if job:
            result["jobs"].append({"jobId": int(job.group(1)), "tookSec": float(job.group(2))})

    result["stages"] = sorted(stage_map.values(), key=lambda s: s["stageId"])

    # 捕获 AQE Final Plan 或 EXPLAIN 物理计划
    result["final_plan"] = _capture_after(lines, "Final plan:")
    result["explain"] = _capture_after(lines, "== Physical Plan ==")
    return result


def _capture_after(lines: list[str], marker: str) -> str | None:
    for index, line in enumerate(lines):
        if marker in line:
            captured: list[str] = []
            for next_line in lines[index + 1:]:
                if TIMESTAMP_RE.match(next_line):
                    break
                captured.append(next_line)
            plan = "\n".join(captured).strip()
            if plan:
                return plan
    return None


def log_only_markdown(log: dict[str, Any], sql_text: str | None) -> str:
    lines = [
        "# Spark SQL 诊断报告（仅日志）",
        "",
        f"- Application：`{log.get('app_id') or '—'}`",
        f"- Operation：`{log.get('operation_id') or '—'}`",
        f"- 日志耗时：{log.get('query_duration') or '—'}",
        "- 证据来源：Kyuubi/Driver 日志（没有可访问的 UI）",
        "",
    ]
    if sql_text:
        lines.extend(["## SQL", "", "```sql", sql_text.strip(), "```", ""])
    lines.extend(["## Stage 时间线", ""])
    lines.append("| Stage | tasks | 日志墙钟 | task runtime max | shuffle write max |")
    lines.append("|---|---|---|---|---|")
    for stage in log.get("stages") or []:
        runtime = (stage.get("stats") or {}).get("task runtime") or {}
        shuffle = (stage.get("stats") or {}).get("shuffle bytes written") or {}
        lines.append(
            f"| {stage.get('stageId')} | {stage.get('numTasks') or '—'} | "
            f"{stage.get('tookMs') or '—'} ms | {runtime.get('max', '—')} ms | "
            f"{shuffle.get('max', '—')} B |"
        )
    lines.extend(["", "## 判断", ""])
    findings = []
    for stage in log.get("stages") or []:
        runtime = (stage.get("stats") or {}).get("task runtime") or {}
        shuffle = (stage.get("stats") or {}).get("shuffle bytes written") or {}
        took = stage.get("tookMs")
        if took and runtime.get("max") and took > runtime["max"] * 2:
            findings.append(
                f"- Stage {stage['stageId']} 墙钟 {took}ms，但最长 task 约 {runtime['max']:.0f}ms，"
                "大概率是调度/冷启动，而不是数据倾斜。"
            )
        if shuffle.get("min") and shuffle.get("max") and shuffle["max"] > shuffle["min"] * 3:
            findings.append(
                f"- Stage {stage['stageId']} 的 shuffle write 分布 max/min = "
                f"{shuffle['max'] / shuffle['min']:.1f}x，可能存在倾斜，需要 task 级数据确认。"
            )
        if runtime.get("mean") and runtime.get("max") and runtime["max"] > runtime["mean"] * 3:
            findings.append(
                f"- Stage {stage['stageId']} task runtime max/mean = "
                f"{runtime['max'] / runtime['mean']:.1f}x，存在长尾，需区分倾斜和冷启动。"
            )
    lines.extend(findings or ["- 仅凭日志无法判断 task 级倾斜；建议提供 UI 地址或 event log。"])
    lines.extend([
        "",
        "## 局限",
        "",
        "- 普通 Driver 日志只有 stage 级分位数，没有逐 task 的 partition/executor/records。",
        "- 热点 key 的具体值、spill/GC 明细需要 UI 或 event log。",
        "",
    ])
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark SQL 只读诊断")
    parser.add_argument("--url", help="Spark UI / History Server / YARN proxy 地址")
    parser.add_argument("--app-id", help="application id，例如 application_123_456")
    parser.add_argument("--exec-id", help="SQL Execution ID")
    parser.add_argument("--exec-time", help="SQL 提交时间，例如 2026-11-02T08:06:50Z")
    parser.add_argument("--sql", help="SQL 文本")
    parser.add_argument("--sql-file", help="SQL 文件")
    parser.add_argument("--explain", help="EXPLAIN FORMATTED 文件")
    parser.add_argument("--log", help="Kyuubi/Spark Driver 日志文件")
    parser.add_argument("--out", default="report.md", help="Markdown 输出路径")
    parser.add_argument("--json", dest="json_out", help="结构化 JSON 输出路径")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--insecure", action="store_true", help="跳过 TLS 校验")
    parser.add_argument("--cookie", help="Cookie 头，注意不要提交到仓库")
    parser.add_argument("--header", action="append", default=[], help="自定义请求头，可重复")
    parser.add_argument("--user", help="Basic Auth，格式 user:password")
    parser.add_argument("--kerberos", action="store_true", help="用 curl --negotiate 访问")
    parser.add_argument("--max-tasks", type=int, default=20000, help="每个 stage 最多抓多少 task")
    return parser.parse_args(argv)


def headers_from_args(values: list[str]) -> dict[str, str]:
    headers = {}
    for item in values:
        if ":" in item:
            key, value = item.split(":", 1)
            headers[key.strip()] = value.strip()
    return headers


def main(argv: list[str] | None = None) -> int:
    # Windows 控制台默认 GBK，强制 UTF-8，避免中文输出乱码
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except (AttributeError, OSError):
                pass

    args = parse_args(argv)
    sql_text = args.sql or read_text(args.sql_file)
    explain = read_text(args.explain)
    log_summary = parse_log(args.log) if args.log else None

    url = args.url
    app_id = args.app_id
    if log_summary:
        app_id = app_id or log_summary.get("app_id")
        if not url and log_summary.get("ui_urls"):
            url = log_summary["ui_urls"][0]
        if not log_summary.get("final_plan") and explain:
            log_summary["final_plan"] = explain

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not url:
        markdown = log_only_markdown(log_summary or {}, sql_text)
        output_path.write_text(markdown, encoding="utf-8")
        print(f"仅日志模式：已写入 {output_path}")
        if log_summary:
            print(f"application: {log_summary.get('app_id')}, operation: {log_summary.get('operation_id')}")
        return 0

    try:
        endpoint = resolve_endpoint(url, app_id=app_id)
        client = SparkClient(
            endpoint.api_base,
            app_id=endpoint.app_id or app_id,
            timeout=args.timeout,
            insecure=args.insecure,
            cookie=args.cookie,
            headers=headers_from_args(args.header),
            user=args.user,
            kerberos=args.kerberos,
        )
        execution = client.find_execution(
            sql_text=sql_text,
            exec_time=args.exec_time,
            exec_id=args.exec_id,
        )
        context = client.fetch_context(execution.get("id"), max_tasks=args.max_tasks)
        result = analyze_execution(context, log_summary=log_summary)
    except SparkUIError as exc:
        if log_summary:
            markdown = log_only_markdown(log_summary, sql_text)
            output_path.write_text(markdown, encoding="utf-8")
            print(f"UI 访问失败（{exc}），已降级为仅日志报告：{output_path}", file=sys.stderr)
            return 0
        print(f"诊断失败：{exc}", file=sys.stderr)
        return 2

    markdown = render_markdown(
        result,
        source="Spark UI REST" + (" + 运行日志" if log_summary else ""),
        log_summary=log_summary,
    )
    output_path.write_text(markdown, encoding="utf-8")

    if args.json_out:
        json_path = Path(args.json_out)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    findings = result.get("findings") or []
    print(f"已写入 {output_path}")
    if args.json_out:
        print(f"已写入 {args.json_out}")
    print(f"application={result.get('appId')} execution={result['execution'].get('id')} "
          f"duration={result['execution'].get('durationMs')}ms findings={len(findings)}")
    for finding in findings[:5]:
        print(f"  [{finding['severity']}] {finding['type']}: {finding['title']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
