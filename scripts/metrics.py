"""Spark UI 指标解析工具：数值、单位、分位数、环境变量。"""
from __future__ import annotations

import re
import statistics
from typing import Any

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
