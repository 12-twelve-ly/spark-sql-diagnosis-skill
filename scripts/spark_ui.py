"""Spark UI REST 客户端。

只读访问 Spark UI / History Server / YARN proxy 的 /api/v1 接口，
负责 URL 解析、应用与 SQL 执行定位、分页抓取。
"""
from __future__ import annotations

import base64
import json
import re
import ssl
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

APP_ID_RE = re.compile(r"application_\d+_\d+")
DEFAULT_QUANTILES = "0,0.25,0.5,0.75,0.9,0.95,0.99"


class SparkUIError(RuntimeError):
    """Spark UI 访问或数据解析失败。"""


@dataclass
class UIEndpoint:
    api_base: str
    app_id: str | None
    source_url: str


def extract_app_id(text: str | None) -> str | None:
    if not text:
        return None
    match = APP_ID_RE.search(text)
    return match.group(0) if match else None


def resolve_endpoint(url: str, app_id: str | None = None) -> UIEndpoint:
    """把用户粘贴的 UI 地址转换为 API base。

    支持：
    - http://host:4040
    - http://host:18080/history/application_xxx/
    - http://host:8088/proxy/application_xxx
    - 已经指向 /api/v1 的地址
    """
    raw = url.strip()
    if "://" not in raw:
        raw = "http://" + raw
    parsed = urlparse(raw)
    path = parsed.path.rstrip("/")
    found = extract_app_id(path)
    app_id = app_id or found

    if path.endswith("/api/v1"):
        api_base = raw.rstrip("/")
    elif "/proxy/" in path:
        match = APP_ID_RE.search(path)
        if match:
            prefix = path[: match.end()]
        else:
            prefix = path
            if app_id:
                prefix = prefix.rstrip("/") + "/" + app_id
        api_base = f"{parsed.scheme}://{parsed.netloc}{prefix}/api/v1"
    elif "/history/" in path:
        prefix = path.split("/history/")[0]
        api_base = f"{parsed.scheme}://{parsed.netloc}{prefix}/api/v1"
    else:
        # 运行中的 Spark UI，API 在根路径下，忽略 /jobs、/sql 等子页面
        api_base = f"{parsed.scheme}://{parsed.netloc}/api/v1"

    return UIEndpoint(api_base=api_base, app_id=app_id, source_url=raw)


def parse_time(value: Any) -> datetime | None:
    """解析 Spark REST / 日志中的时间。

    Spark REST 通常是 2026-11-02T08:06:50.516GMT，
    日志通常是 2026-11-02 16:06:50.434（本地时区）。
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    text = str(value).strip()
    formats = (
        "%Y-%m-%dT%H:%M:%S.%fGMT",
        "%Y-%m-%dT%H:%M:%SGMT",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            if fmt.endswith(("GMT", "Z")):
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(text.replace("GMT", "+00:00").replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def epoch_ms(value: Any) -> float | None:
    dt = parse_time(value)
    return dt.timestamp() * 1000 if dt else None


def normalize_sql(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip().casefold()


class SparkClient:
    """只读 Spark REST 客户端。"""

    def __init__(
        self,
        api_base: str,
        *,
        app_id: str | None = None,
        timeout: float = 30.0,
        insecure: bool = False,
        cookie: str | None = None,
        headers: dict[str, str] | None = None,
        user: str | None = None,
        kerberos: bool = False,
    ) -> None:
        self.api_base = api_base.rstrip("/")
        self.app_id = app_id
        self.timeout = timeout
        self.insecure = insecure
        self.cookie = cookie
        self.user = user
        self.kerberos = kerberos
        self.headers = {"Accept": "application/json"}
        if headers:
            self.headers.update(headers)
        if user and ":" in user:
            token = base64.b64encode(user.encode("utf-8")).decode("ascii")
            self.headers["Authorization"] = f"Basic {token}"
        if cookie:
            self.headers["Cookie"] = cookie

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self.api_base + path

    def _curl(self, url: str) -> bytes:
        cmd = ["curl", "-sS", "-L", "--max-time", str(int(self.timeout))]
        if self.insecure:
            cmd.append("-k")
        if self.kerberos:
            cmd.extend(["--negotiate", "-u", ":"])
        if self.cookie:
            cmd.extend(["-H", f"Cookie: {self.cookie}"])
        if self.user and ":" in self.user:
            cmd.extend(["-u", self.user])
        for key, value in self.headers.items():
            if key.lower() in ("authorization", "cookie"):
                continue
            cmd.extend(["-H", f"{key}: {value}"])
        result = subprocess.run(cmd + [url], capture_output=True)
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace")[:300]
            raise SparkUIError(f"curl 访问失败：{stderr}")
        return result.stdout

    def _get_bytes(self, path: str) -> bytes:
        url = self._url(path)
        if self.kerberos:
            return self._curl(url)
        request = Request(url, headers=self.headers)
        context = None
        if self.insecure:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        try:
            with urlopen(request, timeout=self.timeout, context=context) as response:
                return response.read()
        except HTTPError as exc:
            raise SparkUIError(f"HTTP {exc.code}：{url}") from exc
        except URLError as exc:
            raise SparkUIError(f"无法访问 {url}：{exc.reason}") from exc

    def get_json(self, path: str, *, allow_404: bool = False) -> Any:
        try:
            raw = self._get_bytes(path)
        except SparkUIError as exc:
            if allow_404 and "HTTP 404" in str(exc):
                return None
            raise
        if not raw.strip():
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SparkUIError(f"接口返回不是 JSON：{self.api_base}{path}") from exc

    def get_all_pages(
        self,
        path: str,
        *,
        limit: int = 1000,
        max_items: int = 20000,
    ) -> list[dict[str, Any]]:
        """按 offset/length 分页读取列表接口。"""
        items: list[dict[str, Any]] = []
        seen: set[Any] = set()
        offset = 0
        while len(items) < max_items:
            sep = "&" if "?" in path else "?"
            page = self.get_json(f"{path}{sep}offset={offset}&length={limit}", allow_404=True)
            if not page:
                break
            if isinstance(page, dict):
                page = page.get("tasks") or page.get("elements") or []
            if not isinstance(page, list) or not page:
                break
            before = len(items)
            for item in page:
                key = item.get("id", item.get("taskId", item.get("index")))
                if key is not None and key in seen:
                    continue
                if key is not None:
                    seen.add(key)
                items.append(item)
                if len(items) >= max_items:
                    break
            if len(items) == before or len(page) < limit:
                break
            offset += limit
        return items

    def resolve_app_id(self) -> str:
        if self.app_id:
            return self.app_id
        apps = self.get_json("/applications", allow_404=True)
        if not apps:
            raise SparkUIError("无法从 /api/v1/applications 获取 application id，请显式传 --app-id")
        if isinstance(apps, dict):
            apps = [apps]
        if len(apps) == 1:
            self.app_id = apps[0]["id"]
            return self.app_id
        # 多个应用时优先选择仍在运行、最近启动的那个
        running = [a for a in apps if not (a.get("attempts") or [{}])[0].get("completed", True)]
        pool = running or apps
        pool = sorted(pool, key=lambda a: (a.get("attempts") or [{}])[0].get("startTimeEpoch", 0), reverse=True)
        self.app_id = pool[0]["id"]
        return self.app_id

    def list_sql(self, *, limit: int = 100, max_items: int = 2000) -> list[dict[str, Any]]:
        app_id = self.resolve_app_id()
        return self.get_all_pages(
            f"/applications/{app_id}/sql",
            limit=limit,
            max_items=max_items,
        )

    def find_execution(
        self,
        *,
        sql_text: str | None = None,
        exec_time: str | None = None,
        exec_id: int | str | None = None,
    ) -> dict[str, Any]:
        app_id = self.resolve_app_id()
        if exec_id is not None:
            execution = self.get_json(f"/applications/{app_id}/sql/{exec_id}")
            if not execution:
                raise SparkUIError(f"找不到 SQL Execution {exec_id}")
            return execution

        executions = self.list_sql()
        if not executions:
            raise SparkUIError("SQL 列表为空，无法定位执行记录")

        target_time = parse_time(exec_time) if exec_time else None
        candidates = executions
        if sql_text:
            needle = normalize_sql(sql_text)[:160]
            exact = [q for q in executions if needle and needle in normalize_sql(q.get("description"))]
            if exact:
                candidates = exact
            else:
                words = [w for w in re.split(r"\W+", normalize_sql(sql_text)) if len(w) >= 4][:8]
                scored = []
                for q in executions:
                    desc = normalize_sql(q.get("description"))
                    score = sum(1 for w in words if w in desc)
                    if score:
                        scored.append((score, q))
                if scored:
                    best = max(score for score, _ in scored)
                    candidates = [q for score, q in scored if score == best]

        if target_time:
            def delta(q: dict[str, Any]) -> float:
                submitted = parse_time(q.get("submissionTime"))
                return abs((submitted - target_time).total_seconds()) if submitted else float("inf")

            candidates = sorted(candidates, key=delta)
            nearest = delta(candidates[0]) if candidates else float("inf")
            # 日志时区可能不同，15 分钟内优先；否则仍然按最近时间返回
            if nearest <= 900:
                candidates = [q for q in candidates if delta(q) <= max(nearest + 60, 900)]

        completed = [q for q in candidates if q.get("status") == "COMPLETED"]
        pool = completed or candidates
        if not pool:
            raise SparkUIError("没有找到匹配的 SQL Execution，请提供 --exec-id 或更精确的提交时间")
        pool = sorted(pool, key=lambda q: int(q.get("id", 0)), reverse=True)
        chosen = pool[0]
        chosen["_candidates"] = [q.get("id") for q in pool[:10]]
        return chosen

    def fetch_context(self, exec_id: int | str, *, max_tasks: int = 20000) -> dict[str, Any]:
        app_id = self.resolve_app_id()
        sql = self.get_json(f"/applications/{app_id}/sql/{exec_id}")
        if not sql:
            raise SparkUIError(f"找不到 SQL Execution {exec_id}")

        job_ids = sorted(
            set(
                (sql.get("successJobIds") or [])
                + (sql.get("failedJobIds") or [])
                + (sql.get("runningJobIds") or [])
            )
        )
        jobs = []
        for job_id in job_ids:
            job = self.get_json(f"/applications/{app_id}/jobs/{job_id}", allow_404=True)
            if job:
                jobs.append(job)

        stage_ids: list[int] = []
        for job in jobs:
            stage_ids.extend(job.get("stageIds") or [])
        stage_ids = sorted(set(stage_ids))

        all_stages = self.get_json(f"/applications/{app_id}/stages", allow_404=True) or []
        stage_attempts = [s for s in all_stages if s.get("stageId") in stage_ids]
        for stage_id in stage_ids:
            if not any(s.get("stageId") == stage_id for s in stage_attempts):
                detail = self.get_json(f"/applications/{app_id}/stages/{stage_id}/0", allow_404=True)
                if detail:
                    stage_attempts.append(detail)

        # 同一 stageId 可能有多 attempt；全部保留，方便识别失败重试
        deduped: dict[tuple[Any, Any], dict[str, Any]] = {}
        for stage in stage_attempts:
            key = (stage.get("stageId"), stage.get("attemptId", 0))
            deduped[key] = stage
        stage_attempts = sorted(
            deduped.values(),
            key=lambda s: (s.get("stageId") or 0, s.get("attemptId") or 0),
        )

        task_lists: dict[Any, list[dict[str, Any]]] = {}
        summaries: dict[Any, dict[str, Any]] = {}
        for stage in stage_attempts:
            stage_id = stage.get("stageId")
            attempt = stage.get("attemptId", 0)
            summary = self.get_json(
                f"/applications/{app_id}/stages/{stage_id}/{attempt}/taskSummary?quantiles={DEFAULT_QUANTILES}",
                allow_404=True,
            )
            if summary:
                summaries[(stage_id, attempt)] = summary
                summaries[f"{stage_id}/{attempt}"] = summary
            tasks = self.get_all_pages(
                f"/applications/{app_id}/stages/{stage_id}/{attempt}/taskList",
                limit=1000,
                max_items=max_tasks,
            )
            task_lists[(stage_id, attempt)] = tasks
            task_lists[f"{stage_id}/{attempt}"] = tasks

        selected: dict[Any, dict[str, Any]] = {}
        for stage in stage_attempts:
            stage_id = stage.get("stageId")
            rank = (stage.get("status") == "COMPLETE", stage.get("attemptId", 0))
            current = selected.get(stage_id)
            if current is None:
                selected[stage_id] = stage
            else:
                current_rank = (current.get("status") == "COMPLETE", current.get("attemptId", 0))
                if rank > current_rank:
                    selected[stage_id] = stage
        stages = sorted(selected.values(), key=lambda s: s.get("stageId") or 0)

        # 兼容旧调用：stageId 直接取选中 attempt 的 task list
        for stage in stages:
            stage_id = stage.get("stageId")
            attempt = stage.get("attemptId", 0)
            task_lists.setdefault(stage_id, task_lists.get(f"{stage_id}/{attempt}", []))
            if f"{stage_id}/{attempt}" in summaries:
                summaries.setdefault(stage_id, summaries[f"{stage_id}/{attempt}"])

        environment = self.get_json(f"/applications/{app_id}/environment", allow_404=True) or {}
        executors = self.get_json(f"/applications/{app_id}/executors", allow_404=True) or []
        return {
            "app_id": app_id,
            "sql": sql,
            "jobs": jobs,
            "stages": stages,
            "stage_attempts": stage_attempts,
            "task_lists": task_lists,
            "summaries": summaries,
            "environment": environment,
            "executors": executors,
        }
