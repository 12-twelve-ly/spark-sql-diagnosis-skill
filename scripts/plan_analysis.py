"""SQL 计划节点解析、Exchange/Scan/Join/Aggregate 分析。"""
from __future__ import annotations

import re
from typing import Any

from metrics import (
    SCAN_PRUNING_ROWS,
    SMALL_FILE_COUNT,
    SMALL_FILE_SIZE,
    STAGE_IN_METRIC_RE,
    metric_kind,
    parse_quantity,
)

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
