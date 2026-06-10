#!/usr/bin/env python3
"""Incremental Bilibili crawler for dated keyword searches and virtual-UP videos."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import re
import sqlite3
import sys
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


API_BASE = "https://api.bilibili.com"
DEFAULT_TIMEZONE = "Asia/Shanghai"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36"
)

# These are the current subchannels declared by Bilibili's official /v/virtual page.
VIRTUAL_CATEGORIES = {
    "game": 4,
    "music": 3,
    "douga": 1,
    # The official page declares other=0, but its public listing endpoint currently
    # returns no data or -400. It remains opt-in so future runs can detect recovery.
    "other": 0,
}

TARGET_TAGS = {"虚拟UP主", "虚拟主播"}
WBI_MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
]

RISK_CONTROL_CODES = {-352, -412, 412}
TRANSIENT_HTTP_CODES = {429, 500, 502, 503, 504}


class BilibiliAPIError(RuntimeError):
    def __init__(self, code: int, message: str, url: str) -> None:
        self.code = code
        self.message = message
        self.url = url
        super().__init__(f"Bilibili API code={code}, message={message!r}, url={url}")


class RiskControlError(BilibiliAPIError):
    """Bilibili explicitly rejected the request under its risk-control policy."""


class EndpointCircuitOpen(RuntimeError):
    def __init__(self, endpoint: str, retry_after_seconds: int, reason: str) -> None:
        self.endpoint = endpoint
        self.retry_after_seconds = retry_after_seconds
        self.reason = reason
        super().__init__(
            f"endpoint circuit open: {endpoint}; retry after about "
            f"{retry_after_seconds}s; reason={reason}"
        )


def is_risk_control_code(code: int) -> bool:
    return code in RISK_CONTROL_CODES


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def local_datetime(timestamp: Any, timezone_name: str = DEFAULT_TIMEZONE) -> str:
    try:
        return datetime.fromtimestamp(
            int(timestamp), get_timezone(timezone_name)
        ).isoformat(timespec="seconds")
    except (TypeError, ValueError, OSError, argparse.ArgumentTypeError):
        return ""


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def unique_strings(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        clean = re.sub(r"\s+", " ", str(value)).strip(" \t\r\n,，;；")
        if clean and clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


class BilibiliClient:
    def __init__(
        self,
        cookie: str = "",
        sleep_seconds: float = 1.2,
        timeout: int = 25,
        retries: int = 4,
        jitter_seconds: float = 0.5,
        max_backoff_seconds: float = 30.0,
        wbi_cache_seconds: int = 21600,
        profile_circuit_seconds: int = 3600,
    ) -> None:
        self.cookie = cookie.strip()
        self.sleep_seconds = max(0.0, sleep_seconds)
        self.timeout = timeout
        self.retries = retries
        self.jitter_seconds = max(0.0, jitter_seconds)
        self.max_backoff_seconds = max(1.0, max_backoff_seconds)
        self.wbi_cache_seconds = max(60, wbi_cache_seconds)
        self.profile_circuit_seconds = max(60, profile_circuit_seconds)
        self._last_request = 0.0
        self._wbi_keys: tuple[str, str, float] | None = None
        self._adaptive_delay = 0.0
        self._circuits: dict[str, tuple[float, str]] = {}
        self.stats: dict[str, int] = defaultdict(int)

    @staticmethod
    def endpoint_name(path: str) -> str:
        if "/x/space/wbi/acc/info" in path or "/x/web-interface/card" in path:
            return "profile"
        if "/search/" in path:
            return "search"
        if "/x/web-interface/view" in path:
            return "video_view"
        if "/x/tag/archive/tags" in path:
            return "video_tags"
        if "/x/relation/stat" in path:
            return "relation"
        if "/x/web-interface/nav" in path:
            return "nav"
        return "other"

    def credential_summary(self) -> dict[str, bool]:
        names = {
            part.split("=", 1)[0].strip().lower()
            for part in self.cookie.split(";")
            if "=" in part
        }
        return {
            "cookie_present": bool(self.cookie),
            "sessdata": "sessdata" in names,
            "bili_jct": "bili_jct" in names,
            "buvid3": "buvid3" in names,
            "buvid4": "buvid4" in names,
            "dedeuserid": "dedeuserid" in names,
        }

    def open_circuit(self, endpoint: str, seconds: int, reason: str) -> None:
        self._circuits[endpoint] = (time.monotonic() + max(1, seconds), reason)
        self.stats[f"circuit_opened_{endpoint}"] += 1

    def circuit_status(self, endpoint: str) -> tuple[bool, int, str]:
        circuit = self._circuits.get(endpoint)
        if not circuit:
            return False, 0, ""
        until, reason = circuit
        remaining = int(max(0.0, until - time.monotonic()))
        if remaining <= 0:
            self._circuits.pop(endpoint, None)
            return False, 0, ""
        return True, remaining, reason

    def request_stats(self) -> dict[str, int]:
        return dict(sorted(self.stats.items()))

    def _raise_if_circuit_open(self, endpoint: str) -> None:
        circuit_open, remaining, reason = self.circuit_status(endpoint)
        if circuit_open:
            self.stats[f"circuit_skips_{endpoint}"] += 1
            raise EndpointCircuitOpen(endpoint, remaining, reason)

    def _pace(self) -> None:
        elapsed = time.monotonic() - self._last_request
        jitter = random.uniform(0.0, min(self.jitter_seconds, self.sleep_seconds or self.jitter_seconds))
        target = self.sleep_seconds + self._adaptive_delay + jitter
        if elapsed < target:
            time.sleep(target - elapsed)

    @staticmethod
    def _retry_after_seconds(error: HTTPError) -> float | None:
        value = error.headers.get("Retry-After") if error.headers else None
        if value and value.isdigit():
            return float(value)
        return None

    def _backoff(self, attempt: int, retry_after: float | None = None) -> None:
        delay = retry_after if retry_after is not None else 1.5 * (2 ** (attempt - 1))
        delay = min(self.max_backoff_seconds, delay + random.uniform(0.0, 0.75))
        self._adaptive_delay = min(5.0, self._adaptive_delay + 0.5)
        self.stats["backoff_count"] += 1
        time.sleep(delay)

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        endpoint = self.endpoint_name(path)
        self._raise_if_circuit_open(endpoint)
        query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        url = path if path.startswith("http") else f"{API_BASE}{path}"
        if query:
            url = f"{url}{'&' if '?' in url else '?'}{query}"

        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                self._pace()
                self.stats["request_attempts"] += 1
                self.stats[f"request_attempts_{endpoint}"] += 1
                headers = {
                    "User-Agent": USER_AGENT,
                    "Referer": "https://www.bilibili.com/v/virtual/",
                    "Origin": "https://www.bilibili.com",
                    "Accept": "application/json, text/plain, */*",
                }
                if self.cookie:
                    headers["Cookie"] = self.cookie
                request = Request(url, headers=headers)
                with urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                self._last_request = time.monotonic()
                code = payload.get("code", 0)
                nav_has_wbi_keys = (
                    path.endswith("/x/web-interface/nav")
                    and bool(((payload.get("data") or {}).get("wbi_img") or {}).get("img_url"))
                )
                if code != 0 and not nav_has_wbi_keys:
                    message = str(payload.get("message") or "")
                    if is_risk_control_code(int(code)):
                        self.stats["risk_control_events"] += 1
                        self.stats[f"risk_control_events_{endpoint}"] += 1
                        raise RiskControlError(int(code), message, url)
                    self.stats["api_errors"] += 1
                    raise BilibiliAPIError(int(code), message, url)
                self.stats["successful_requests"] += 1
                self.stats[f"successful_requests_{endpoint}"] += 1
                self._adaptive_delay = max(0.0, self._adaptive_delay * 0.75 - 0.05)
                return payload
            except RiskControlError:
                self._last_request = time.monotonic()
                raise
            except BilibiliAPIError:
                self._last_request = time.monotonic()
                raise
            except HTTPError as exc:
                last_error = exc
                self._last_request = time.monotonic()
                self.stats["http_errors"] += 1
                if is_risk_control_code(exc.code):
                    self.stats["risk_control_events"] += 1
                    self.stats[f"risk_control_events_{endpoint}"] += 1
                    raise RiskControlError(exc.code, str(exc.reason), url) from exc
                if exc.code not in TRANSIENT_HTTP_CODES or attempt >= self.retries:
                    raise RuntimeError(f"non-retryable HTTP {exc.code}: {url}") from exc
                self.stats["retry_attempts"] += 1
                self._backoff(attempt, self._retry_after_seconds(exc))
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                self._last_request = time.monotonic()
                self.stats["transient_errors"] += 1
                if attempt >= self.retries:
                    break
                self.stats["retry_attempts"] += 1
                self._backoff(attempt)
        raise RuntimeError(f"request failed after {self.retries} attempts: {last_error}")

    def get_wbi_keys(self) -> tuple[str, str]:
        if self._wbi_keys and time.time() - self._wbi_keys[2] < self.wbi_cache_seconds:
            return self._wbi_keys[0], self._wbi_keys[1]
        data = self.get_json("/x/web-interface/nav").get("data") or {}
        wbi = data.get("wbi_img") or {}
        img_key = (wbi.get("img_url") or "").rsplit("/", 1)[-1].split(".", 1)[0]
        sub_key = (wbi.get("sub_url") or "").rsplit("/", 1)[-1].split(".", 1)[0]
        if not img_key or not sub_key:
            raise RuntimeError("Bilibili nav response did not contain WBI keys")
        self._wbi_keys = (img_key, sub_key, time.time())
        return img_key, sub_key

    def signed_params(self, params: dict[str, Any]) -> dict[str, Any]:
        img_key, sub_key = self.get_wbi_keys()
        raw = img_key + sub_key
        mixin_key = "".join(raw[index] for index in WBI_MIXIN_KEY_ENC_TAB)[:32]
        values = dict(params)
        values["wts"] = int(time.time())
        cleaned = {
            str(key): re.sub(r"[!'()*]", "", str(value))
            for key, value in sorted(values.items())
            if value is not None
        }
        query = urlencode(cleaned)
        cleaned["w_rid"] = hashlib.md5((query + mixin_key).encode()).hexdigest()
        return cleaned

    def get_wbi_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        self._raise_if_circuit_open(self.endpoint_name(path))
        return self.get_json(path, self.signed_params(params))


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS discoveries (
                bvid TEXT NOT NULL,
                source TEXT NOT NULL,
                source_key TEXT NOT NULL,
                discovered_at TEXT NOT NULL,
                PRIMARY KEY (bvid, source, source_key)
            );

            CREATE TABLE IF NOT EXISTS creators (
                mid INTEGER PRIMARY KEY,
                name TEXT,
                follower_count INTEGER,
                bio TEXT,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS videos (
                bvid TEXT PRIMARY KEY,
                aid INTEGER,
                title TEXT,
                pubdate_ts INTEGER,
                pubdate TEXT,
                ctime_ts INTEGER,
                duration_seconds INTEGER,
                tid INTEGER,
                tid_v2 INTEGER,
                category_name TEXT,
                view_count INTEGER,
                danmaku_count INTEGER,
                comment_count INTEGER,
                favorite_count INTEGER,
                coin_count INTEGER,
                share_count INTEGER,
                like_count INTEGER,
                description TEXT,
                owner_mid INTEGER,
                owner_name TEXT,
                creator_uids_json TEXT,
                creators_json TEXT,
                first_creator_mid INTEGER,
                first_creator_name TEXT,
                first_creator_follower_count INTEGER,
                first_creator_bio TEXT,
                tags_json TEXT,
                tags_text TEXT,
                target_tag_match INTEGER,
                is_cooperation INTEGER,
                ai_suspected INTEGER,
                ai_score INTEGER,
                ai_reasons_json TEXT,
                music_candidates_json TEXT,
                music_evidence_json TEXT,
                argue_message TEXT,
                cover_url TEXT,
                video_url TEXT,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage TEXT NOT NULL,
                item_key TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS search_windows (
                keyword TEXT NOT NULL,
                window_begin_ts INTEGER NOT NULL,
                window_end_ts INTEGER NOT NULL,
                timezone_name TEXT NOT NULL,
                depth INTEGER NOT NULL,
                status TEXT NOT NULL,
                num_results INTEGER,
                num_pages INTEGER,
                pages_fetched INTEGER NOT NULL DEFAULT 0,
                hits_seen INTEGER NOT NULL DEFAULT 0,
                truncated INTEGER NOT NULL DEFAULT 0,
                message TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (keyword, window_begin_ts, window_end_ts)
            );

            CREATE TABLE IF NOT EXISTS search_hits (
                keyword TEXT NOT NULL,
                bvid TEXT NOT NULL,
                window_begin_ts INTEGER NOT NULL,
                window_end_ts INTEGER NOT NULL,
                result_page INTEGER NOT NULL,
                result_rank INTEGER NOT NULL,
                result_pubdate_ts INTEGER,
                discovered_at TEXT NOT NULL,
                PRIMARY KEY (keyword, bvid, window_begin_ts, window_end_ts)
            );

            CREATE TABLE IF NOT EXISTS tag_filter_rejections (
                bvid TEXT NOT NULL,
                keyword TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                match_mode TEXT NOT NULL,
                rejected_at TEXT NOT NULL,
                PRIMARY KEY (bvid, keyword)
            );

            CREATE TABLE IF NOT EXISTS request_run_stats (
                run_id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                credential_summary_json TEXT NOT NULL,
                request_stats_json TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def add_discovery(self, bvid: str, source: str, source_key: str) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO discoveries VALUES (?, ?, ?, ?)",
            (bvid, source, source_key, utc_now()),
        )

    def add_search_hit(
        self,
        keyword: str,
        bvid: str,
        begin_ts: int,
        end_ts: int,
        page: int,
        rank: int,
        pubdate_ts: Any,
    ) -> None:
        self.add_discovery(bvid, "keyword_search", keyword)
        self.connection.execute(
            """
            INSERT INTO search_hits(
                keyword, bvid, window_begin_ts, window_end_ts,
                result_page, result_rank, result_pubdate_ts, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(keyword, bvid, window_begin_ts, window_end_ts) DO UPDATE SET
                result_page=excluded.result_page,
                result_rank=excluded.result_rank,
                result_pubdate_ts=excluded.result_pubdate_ts,
                discovered_at=excluded.discovered_at
            """,
            (keyword, bvid, begin_ts, end_ts, page, rank, pubdate_ts, utc_now()),
        )

    def get_search_window(self, keyword: str, begin_ts: int, end_ts: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT * FROM search_windows
            WHERE keyword = ? AND window_begin_ts = ? AND window_end_ts = ?
            """,
            (keyword, begin_ts, end_ts),
        ).fetchone()

    def upsert_search_window(
        self,
        keyword: str,
        begin_ts: int,
        end_ts: int,
        timezone_name: str,
        depth: int,
        status: str,
        num_results: int | None = None,
        num_pages: int | None = None,
        pages_fetched: int = 0,
        hits_seen: int = 0,
        truncated: int = 0,
        message: str = "",
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO search_windows(
                keyword, window_begin_ts, window_end_ts, timezone_name, depth,
                status, num_results, num_pages, pages_fetched, hits_seen,
                truncated, message, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(keyword, window_begin_ts, window_end_ts) DO UPDATE SET
                timezone_name=excluded.timezone_name,
                depth=excluded.depth,
                status=excluded.status,
                num_results=excluded.num_results,
                num_pages=excluded.num_pages,
                pages_fetched=excluded.pages_fetched,
                hits_seen=excluded.hits_seen,
                truncated=excluded.truncated,
                message=excluded.message,
                updated_at=excluded.updated_at
            """,
            (
                keyword, begin_ts, end_ts, timezone_name, depth, status,
                num_results, num_pages, pages_fetched, hits_seen, truncated,
                message[:1000], utc_now(),
            ),
        )
        self.connection.commit()

    def search_keywords_for_bvid(self, bvid: str) -> list[str]:
        return [
            row[0] for row in self.connection.execute(
                "SELECT DISTINCT keyword FROM search_hits WHERE bvid = ? ORDER BY keyword",
                (bvid,),
            )
        ]

    def reject_keyword_hit(
        self,
        bvid: str,
        keyword: str,
        tags: list[str],
        match_mode: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO tag_filter_rejections(
                bvid, keyword, tags_json, match_mode, rejected_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(bvid, keyword) DO UPDATE SET
                tags_json=excluded.tags_json,
                match_mode=excluded.match_mode,
                rejected_at=excluded.rejected_at
            """,
            (bvid, keyword, json_text(tags), match_mode, utc_now()),
        )
        self.connection.execute(
            "DELETE FROM search_hits WHERE bvid = ? AND keyword = ?",
            (bvid, keyword),
        )
        self.connection.execute(
            """
            DELETE FROM discoveries
            WHERE bvid = ? AND source = 'keyword_search' AND source_key = ?
            """,
            (bvid, keyword),
        )

    def delete_video(self, bvid: str) -> None:
        self.connection.execute("DELETE FROM videos WHERE bvid = ?", (bvid,))

    def restore_accepted_keyword(self, bvid: str, keyword: str) -> None:
        self.connection.execute(
            "DELETE FROM tag_filter_rejections WHERE bvid = ? AND keyword = ?",
            (bvid, keyword),
        )

    def save_request_run_stats(
        self,
        run_id: str,
        command: str,
        started_at: str,
        client: BilibiliClient,
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO request_run_stats(
                run_id, command, started_at, finished_at,
                credential_summary_json, request_stats_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                command,
                started_at,
                utc_now(),
                json_text(client.credential_summary()),
                json_text(client.request_stats()),
            ),
        )
        self.connection.commit()

    def add_error(self, stage: str, item_key: str, error: Exception | str) -> None:
        self.connection.execute(
            "INSERT INTO errors(stage, item_key, message, created_at) VALUES (?, ?, ?, ?)",
            (stage, item_key, str(error)[:2000], utc_now()),
        )
        self.connection.commit()

    def commit(self) -> None:
        self.connection.commit()

    def pending_bvids(self, refresh: bool = False) -> list[str]:
        if refresh:
            query = "SELECT DISTINCT bvid FROM discoveries ORDER BY bvid"
        else:
            query = """
                SELECT DISTINCT d.bvid
                FROM discoveries d LEFT JOIN videos v USING (bvid)
                WHERE v.bvid IS NULL
                ORDER BY d.bvid
            """
        return [row[0] for row in self.connection.execute(query)]

    def get_creator(self, mid: int, max_age_hours: float) -> sqlite3.Row | None:
        row = self.connection.execute("SELECT * FROM creators WHERE mid = ?", (mid,)).fetchone()
        if not row:
            return None
        try:
            fetched = datetime.fromisoformat(row["fetched_at"])
            age = datetime.now(timezone.utc) - fetched.astimezone(timezone.utc)
            if age.total_seconds() <= max_age_hours * 3600:
                return row
        except (TypeError, ValueError):
            pass
        return None

    def upsert_creator(self, creator: dict[str, Any]) -> None:
        self.connection.execute(
            """
            INSERT INTO creators(mid, name, follower_count, bio, fetched_at)
            VALUES (:mid, :name, :follower_count, :bio, :fetched_at)
            ON CONFLICT(mid) DO UPDATE SET
                name=excluded.name,
                follower_count=excluded.follower_count,
                bio=excluded.bio,
                fetched_at=excluded.fetched_at
            """,
            creator,
        )

    def upsert_video(self, row: dict[str, Any]) -> None:
        columns = list(row)
        placeholders = ",".join(f":{column}" for column in columns)
        updates = ",".join(f"{column}=excluded.{column}" for column in columns if column != "bvid")
        self.connection.execute(
            f"INSERT INTO videos ({','.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT(bvid) DO UPDATE SET {updates}",
            row,
        )


AI_RULES: list[tuple[re.Pattern[str], int, str]] = [
    (re.compile(r"含\s*AI\s*生成内容", re.I), 5, "平台或文本明确标注含AI生成内容"),
    (re.compile(r"\bAIGC\b|生成式\s*AI|人工智能生成", re.I), 4, "文本包含AIGC/生成式AI声明"),
    (re.compile(r"AI\s*(?:翻唱|歌声|声线|配音|绘画|动画|视频|生成|作曲|音乐)", re.I), 3, "文本说明AI参与内容制作"),
    (re.compile(r"(?:Suno|Stable\s*Diffusion|Midjourney|NovelAI)", re.I), 2, "文本包含常见生成式AI工具名"),
    (re.compile(r"(?:^|[【\[（(])\s*AI(?:[^A-Za-z]|$)", re.I), 1, "标题或标签以AI标记开头"),
]


def infer_ai(title: str, description: str, tags: list[str], argue_message: str) -> tuple[int, int, list[str]]:
    fields = {
        "平台声明": argue_message,
        "标题": title,
        "简介": description,
        "标签": " ".join(tags),
    }
    score = 0
    reasons: list[str] = []
    for field_name, text in fields.items():
        if not text:
            continue
        for pattern, weight, reason in AI_RULES:
            if pattern.search(text):
                score += weight
                reasons.append(f"{field_name}: {reason}")
    reasons = unique_strings(reasons)
    return int(score >= 2), score, reasons


MUSIC_LINE_PATTERN = re.compile(
    r"(?im)^\s*(原曲|原唱|歌曲|曲名|歌名|BGM|背景音乐|使用音乐|音乐|Music|Song)\s*[:：]\s*(.{1,160})$"
)
MUSIC_QUOTE_PATTERN = re.compile(
    r"(?:原曲|歌曲|曲名|歌名|BGM|背景音乐|使用音乐|音乐)[^\n《]{0,15}《([^》]{1,80})》",
    re.I,
)


def infer_music(title: str, description: str, tags: list[str]) -> tuple[list[str], list[str]]:
    combined = f"{title}\n{description}"
    candidates: list[str] = []
    evidence: list[str] = []
    for match in MUSIC_LINE_PATTERN.finditer(combined):
        label, value = match.group(1), match.group(2)
        candidates.append(value)
        evidence.append(f"{label}: {value}")
    for match in MUSIC_QUOTE_PATTERN.finditer(combined):
        candidates.append(match.group(1))
        evidence.append(f"音乐关键词附近书名号: 《{match.group(1)}》")

    music_tags = [
        tag for tag in tags
        if re.search(r"歌曲|音乐|翻唱|原创曲|VOCALOID|Synthesizer|BGM|歌回", tag, re.I)
    ]
    evidence.extend(f"音乐相关标签: {tag}" for tag in music_tags)
    return unique_strings(candidates), unique_strings(evidence)


def normalize_tag_text(value: str) -> str:
    return re.sub(r"\s+", "", value).casefold()


def keyword_matches_tags(keyword: str, tags: list[str], mode: str = "exact") -> bool:
    normalized_keyword = normalize_tag_text(keyword)
    if not normalized_keyword:
        return False
    normalized_tags = [normalize_tag_text(tag) for tag in tags]
    if mode == "exact":
        return normalized_keyword in normalized_tags
    if mode == "substring":
        return any(normalized_keyword in tag for tag in normalized_tags)
    raise ValueError(f"unknown tag match mode: {mode}")


def keyword_matches_description(keyword: str, description: str) -> bool:
    normalized_keyword = normalize_tag_text(keyword)
    normalized_description = normalize_tag_text(description)
    return bool(normalized_keyword and normalized_keyword in normalized_description)


def parse_creators(view: dict[str, Any]) -> list[dict[str, Any]]:
    staff = view.get("staff") or []
    if staff:
        return [
            {
                "mid": item.get("mid"),
                "name": item.get("name") or "",
                "role": item.get("title") or "联合投稿",
            }
            for item in staff
            if item.get("mid") is not None
        ]
    owner = view.get("owner") or {}
    if owner.get("mid") is None:
        return []
    return [{"mid": owner.get("mid"), "name": owner.get("name") or "", "role": "UP主"}]


def fetch_creator(
    client: BilibiliClient,
    store: Store,
    mid: int,
    fallback_name: str,
    cache_hours: float,
) -> dict[str, Any]:
    cached = store.get_creator(mid, cache_hours)
    profile_circuit_open, _remaining, _reason = client.circuit_status("profile")
    if cached and (cached["bio"] or profile_circuit_open):
        return dict(cached)

    follower: int | None = cached["follower_count"] if cached else None
    bio = cached["bio"] if cached else ""
    name = (cached["name"] if cached else "") or fallback_name
    errors: list[str] = []
    if follower is None:
        try:
            relation = client.get_json("/x/relation/stat", {"vmid": mid}).get("data") or {}
            follower = relation.get("follower")
        except RiskControlError:
            raise
        except Exception as exc:  # noqa: BLE001
            errors.append(f"follower: {exc}")

    profile_circuit_open, _remaining, _reason = client.circuit_status("profile")
    if not bio and not profile_circuit_open:
        try:
            profile = client.get_wbi_json("/x/space/wbi/acc/info", {"mid": mid}).get("data") or {}
            name = profile.get("name") or name
            bio = profile.get("sign") or ""
        except RiskControlError as exc:
            client.open_circuit("profile", client.profile_circuit_seconds, str(exc))
            errors.append(f"profile risk-control circuit opened: {exc}")
        except EndpointCircuitOpen:
            pass
        except Exception as exc:  # noqa: BLE001
            errors.append(f"profile: {exc}")

    creator = {
        "mid": mid,
        "name": name,
        "follower_count": follower,
        "bio": bio,
        "fetched_at": utc_now(),
    }
    store.upsert_creator(creator)
    if errors:
        store.add_error("creator_partial", str(mid), " | ".join(errors))
    return creator


def discover_channel(
    client: BilibiliClient,
    store: Store,
    categories: list[str],
    page_size: int,
    max_pages: int | None,
) -> int:
    discovered = 0
    for category in categories:
        rid = VIRTUAL_CATEGORIES[category]
        page = 1
        while max_pages is None or page <= max_pages:
            try:
                payload = client.get_json(
                    "/x/web-interface/landing/page/dynamic/region",
                    {"business": "vup", "rid": rid, "pn": page, "ps": page_size},
                )
            except RiskControlError:
                raise
            except Exception as exc:  # noqa: BLE001
                store.add_error("discover_channel", category, exc)
                break
            data = payload.get("data") or {}
            archives = data.get("archives") or []
            if not archives:
                break
            for archive in archives:
                bvid = archive.get("bvid")
                if bvid:
                    store.add_discovery(bvid, "virtual_channel", category)
                    discovered += 1
            store.commit()
            page_info = data.get("page") or {}
            count = int(page_info.get("count") or 0)
            if len(archives) < page_size or (count and page * page_size >= count):
                break
            page += 1
    return discovered


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def detect_timezone_name() -> str:
    configured = os.environ.get("TZ", "").strip()
    if configured:
        return configured
    try:
        target = Path("/etc/localtime").resolve()
        marker = "/zoneinfo/"
        if marker in str(target):
            return str(target).split(marker, 1)[1]
    except OSError:
        pass
    return DEFAULT_TIMEZONE


def get_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise argparse.ArgumentTypeError(f"unknown timezone: {name}") from exc


def date_time_windows(
    start: date,
    end: date,
    timezone_name: str,
    window_days: int = 1,
) -> list[tuple[int, int]]:
    if start > end:
        raise ValueError("start date must be on or before end date")
    if window_days < 1:
        raise ValueError("window_days must be at least 1")
    zone = get_timezone(timezone_name)
    windows: list[tuple[int, int]] = []
    current = start
    while current <= end:
        next_date = min(current + timedelta(days=window_days), end + timedelta(days=1))
        begin = datetime.combine(current, datetime_time.min, tzinfo=zone)
        next_begin = datetime.combine(next_date, datetime_time.min, tzinfo=zone)
        windows.append((int(begin.timestamp()), int(next_begin.timestamp()) - 1))
        current = next_date
    return windows


def daily_time_windows(start: date, end: date, timezone_name: str) -> list[tuple[int, int]]:
    return date_time_windows(start, end, timezone_name, window_days=1)


def split_time_window(begin_ts: int, end_ts: int) -> tuple[tuple[int, int], tuple[int, int]]:
    if begin_ts >= end_ts:
        raise ValueError("cannot split a one-second window")
    midpoint = (begin_ts + end_ts) // 2
    return (begin_ts, midpoint), (midpoint + 1, end_ts)


def load_keywords(raw: str, keyword_file: Path | None) -> list[str]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if keyword_file:
        for line in keyword_file.read_text(encoding="utf-8-sig").splitlines():
            clean = line.strip()
            if clean and not clean.startswith("#"):
                values.append(clean)
    return unique_strings(values)


def search_page(
    client: BilibiliClient,
    keyword: str,
    begin_ts: int,
    end_ts: int,
    page: int,
) -> dict[str, Any]:
    payload = client.get_wbi_json(
        "/x/web-interface/wbi/search/type",
        {
            "search_type": "video",
            "keyword": keyword,
            "order": "pubdate",
            "page": page,
            "pubtime_begin_s": begin_ts,
            "pubtime_end_s": end_ts,
        },
    )
    return payload.get("data") or {}


def store_search_results(
    store: Store,
    keyword: str,
    begin_ts: int,
    end_ts: int,
    page: int,
    results: list[dict[str, Any]],
) -> int:
    stored = 0
    for rank, result in enumerate(results, start=1):
        bvid = result.get("bvid")
        if not bvid:
            continue
        store.add_search_hit(
            keyword, bvid, begin_ts, end_ts, page, rank, result.get("pubdate")
        )
        stored += 1
    store.commit()
    return stored


def discover_search_window(
    client: BilibiliClient,
    store: Store,
    keyword: str,
    begin_ts: int,
    end_ts: int,
    timezone_name: str,
    max_pages: int,
    min_window_seconds: int,
    rediscover: bool,
    depth: int = 0,
) -> tuple[int, int]:
    previous = store.get_search_window(keyword, begin_ts, end_ts)
    if previous and not rediscover:
        if previous["status"] in {"complete", "truncated"}:
            return 0, int(previous["truncated"] or 0)
        if previous["status"] == "split":
            left, right = split_time_window(begin_ts, end_ts)
            left_count, left_gaps = discover_search_window(
                client, store, keyword, *left, timezone_name, max_pages,
                min_window_seconds, rediscover, depth + 1
            )
            right_count, right_gaps = discover_search_window(
                client, store, keyword, *right, timezone_name, max_pages,
                min_window_seconds, rediscover, depth + 1
            )
            return left_count + right_count, left_gaps + right_gaps

    try:
        first_data = search_page(client, keyword, begin_ts, end_ts, 1)
    except RiskControlError as exc:
        store.upsert_search_window(
            keyword, begin_ts, end_ts, timezone_name, depth, "failed",
            truncated=1, message=f"risk control: {exc}",
        )
        store.add_error("risk_control_search", f"{keyword}:{begin_ts}-{end_ts}", exc)
        raise
    except Exception as exc:  # noqa: BLE001
        store.upsert_search_window(
            keyword, begin_ts, end_ts, timezone_name, depth, "failed",
            message=str(exc),
        )
        store.add_error("discover_search_window", f"{keyword}:{begin_ts}-{end_ts}", exc)
        return 0, 1

    num_results = int(first_data.get("numResults") or 0)
    num_pages = int(first_data.get("numPages") or 0)
    window_seconds = end_ts - begin_ts + 1
    if num_pages > max_pages and window_seconds > min_window_seconds:
        store.upsert_search_window(
            keyword, begin_ts, end_ts, timezone_name, depth, "split",
            num_results=num_results, num_pages=num_pages,
            message=f"split because num_pages={num_pages} exceeded max_pages={max_pages}",
        )
        left, right = split_time_window(begin_ts, end_ts)
        left_count, left_gaps = discover_search_window(
            client, store, keyword, *left, timezone_name, max_pages,
            min_window_seconds, rediscover, depth + 1
        )
        right_count, right_gaps = discover_search_window(
            client, store, keyword, *right, timezone_name, max_pages,
            min_window_seconds, rediscover, depth + 1
        )
        return left_count + right_count, left_gaps + right_gaps

    pages_to_fetch = min(num_pages, max_pages)
    hits_seen = store_search_results(
        store, keyword, begin_ts, end_ts, 1, first_data.get("result") or []
    )
    pages_fetched = 1 if num_pages else 0
    status = "complete"
    message = ""
    try:
        for page in range(2, pages_to_fetch + 1):
            data = search_page(client, keyword, begin_ts, end_ts, page)
            hits_seen += store_search_results(
                store, keyword, begin_ts, end_ts, page, data.get("result") or []
            )
            pages_fetched = page
    except RiskControlError as exc:
        store.upsert_search_window(
            keyword, begin_ts, end_ts, timezone_name, depth, "failed",
            num_results=num_results, num_pages=num_pages,
            pages_fetched=pages_fetched, hits_seen=hits_seen, truncated=1,
            message=f"risk control: {exc}",
        )
        store.add_error(
            "risk_control_search", f"{keyword}:{begin_ts}-{end_ts}:page={pages_fetched + 1}", exc
        )
        raise
    except Exception as exc:  # noqa: BLE001
        status = "failed"
        message = str(exc)
        store.add_error(
            "discover_search_page", f"{keyword}:{begin_ts}-{end_ts}:page={pages_fetched + 1}", exc
        )

    truncated = int(num_pages > max_pages)
    if status == "complete" and truncated:
        status = "truncated"
        message = (
            f"minimum window still had {num_pages} pages; only {max_pages} were fetched"
        )
    store.upsert_search_window(
        keyword, begin_ts, end_ts, timezone_name, depth, status,
        num_results=num_results, num_pages=num_pages, pages_fetched=pages_fetched,
        hits_seen=hits_seen, truncated=truncated or int(status == "failed"), message=message,
    )
    return hits_seen, int(truncated or status == "failed")


def discover_dated_search(
    client: BilibiliClient,
    store: Store,
    keywords: list[str],
    start: date,
    end: date,
    timezone_name: str,
    max_pages: int,
    min_window_seconds: int,
    rediscover: bool,
    initial_window_days: int = 1,
) -> tuple[int, int]:
    discovered = 0
    coverage_gaps = 0
    windows = date_time_windows(start, end, timezone_name, initial_window_days)
    for keyword in keywords:
        for begin_ts, end_ts in windows:
            count, gaps = discover_search_window(
                client, store, keyword, begin_ts, end_ts, timezone_name,
                max_pages, min_window_seconds, rediscover,
            )
            discovered += count
            coverage_gaps += gaps
    return discovered, coverage_gaps


def discover_bvid_file(store: Store, path: Path) -> int:
    count = 0
    for match in re.finditer(r"BV[0-9A-Za-z]{10}", path.read_text(encoding="utf-8")):
        store.add_discovery(match.group(0), "bvid_file", path.name)
        count += 1
    store.commit()
    return count


def enrich_video(
    client: BilibiliClient,
    store: Store,
    bvid: str,
    creator_cache_hours: float,
) -> tuple[list[str], str]:
    view = client.get_json("/x/web-interface/view", {"bvid": bvid}).get("data") or {}
    tag_data = client.get_json("/x/tag/archive/tags", {"bvid": bvid}).get("data") or []
    tags = [item.get("tag_name") for item in tag_data if item.get("tag_name")]
    creators = parse_creators(view)
    first_creator = creators[0] if creators else {}
    first_profile: dict[str, Any] = {}
    if first_creator.get("mid") is not None:
        first_profile = fetch_creator(
            client,
            store,
            int(first_creator["mid"]),
            first_creator.get("name") or "",
            creator_cache_hours,
        )

    title = view.get("title") or ""
    description = view.get("desc") or ""
    argue_message = (view.get("argue_info") or {}).get("argue_msg") or ""
    ai_suspected, ai_score, ai_reasons = infer_ai(title, description, tags, argue_message)
    music_candidates, music_evidence = infer_music(title, description, tags)
    stat = view.get("stat") or {}
    owner = view.get("owner") or {}

    row = {
        "bvid": bvid,
        "aid": view.get("aid"),
        "title": title,
        "pubdate_ts": view.get("pubdate"),
        "pubdate": local_datetime(view.get("pubdate")),
        "ctime_ts": view.get("ctime"),
        "duration_seconds": view.get("duration"),
        "tid": view.get("tid"),
        "tid_v2": view.get("tid_v2"),
        "category_name": view.get("tname_v2") or view.get("tname") or "",
        "view_count": stat.get("view"),
        "danmaku_count": stat.get("danmaku"),
        "comment_count": stat.get("reply"),
        "favorite_count": stat.get("favorite"),
        "coin_count": stat.get("coin"),
        "share_count": stat.get("share"),
        "like_count": stat.get("like"),
        "description": description,
        "owner_mid": owner.get("mid"),
        "owner_name": owner.get("name") or "",
        "creator_uids_json": json_text([item.get("mid") for item in creators]),
        "creators_json": json_text(creators),
        "first_creator_mid": first_creator.get("mid"),
        "first_creator_name": first_profile.get("name") or first_creator.get("name") or "",
        "first_creator_follower_count": first_profile.get("follower_count"),
        "first_creator_bio": first_profile.get("bio") or "",
        "tags_json": json_text(tags),
        "tags_text": "|".join(tags),
        "target_tag_match": int(bool(TARGET_TAGS.intersection(tags))),
        "is_cooperation": int(bool((view.get("rights") or {}).get("is_cooperation") or len(creators) > 1)),
        "ai_suspected": ai_suspected,
        "ai_score": ai_score,
        "ai_reasons_json": json_text(ai_reasons),
        "music_candidates_json": json_text(music_candidates),
        "music_evidence_json": json_text(music_evidence),
        "argue_message": argue_message,
        "cover_url": view.get("pic") or "",
        "video_url": f"https://www.bilibili.com/video/{bvid}",
        "fetched_at": utc_now(),
    }
    store.upsert_video(row)
    store.commit()
    return tags, description


def export_csv(store: Store, output: Path, only_target_tags: bool = False) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    where = "WHERE target_tag_match = 1" if only_target_tags else ""
    query = f"""
        SELECT v.*,
               COALESCE((
                   SELECT group_concat(keyword, '|') FROM (
                       SELECT DISTINCT keyword
                       FROM search_hits h WHERE h.bvid = v.bvid
                       ORDER BY keyword
                   )
               ), '') AS matched_keywords,
               (SELECT count(DISTINCT keyword) FROM search_hits h WHERE h.bvid = v.bvid)
                   AS matched_keyword_count,
               (SELECT count(*) FROM search_hits h WHERE h.bvid = v.bvid)
                   AS search_window_hit_count,
               COALESCE((
                   SELECT group_concat(source || ':' || source_key, '|')
                   FROM discoveries d WHERE d.bvid = v.bvid
               ), '') AS discovery_sources
        FROM videos v {where}
        ORDER BY pubdate_ts DESC, bvid
    """
    rows = store.connection.execute(query).fetchall()
    if not rows:
        output.write_text("", encoding="utf-8-sig")
        return 0
    with output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)
    return len(rows)


def export_coverage_csv(store: Store, output: Path) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = store.connection.execute(
        """
        SELECT * FROM search_windows
        ORDER BY keyword, window_begin_ts, window_end_ts
        """
    ).fetchall()
    fieldnames = [
        "keyword", "window_begin_ts", "window_end_ts", "window_begin",
        "window_end", "timezone_name", "depth", "status", "num_results",
        "num_pages", "pages_fetched", "hits_seen", "truncated", "message",
        "updated_at",
    ]
    with output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for source in rows:
            row = dict(source)
            try:
                zone = get_timezone(row["timezone_name"])
                row["window_begin"] = datetime.fromtimestamp(
                    row["window_begin_ts"], zone
                ).isoformat(timespec="seconds")
                row["window_end"] = datetime.fromtimestamp(
                    row["window_end_ts"], zone
                ).isoformat(timespec="seconds")
            except (ValueError, argparse.ArgumentTypeError):
                row["window_begin"] = ""
                row["window_end"] = ""
            writer.writerow({key: row.get(key) for key in fieldnames})
    return len(rows)


def default_coverage_path(video_output: Path) -> Path:
    return video_output.with_name(f"{video_output.stem}_coverage.csv")


def apply_keyword_tag_filter(
    store: Store,
    bvid: str,
    tags: list[str],
    description: str,
    match_mode: str,
) -> tuple[bool, list[str]]:
    search_keywords = store.search_keywords_for_bvid(bvid)
    if not search_keywords:
        return True, []
    rejected_keywords: list[str] = []
    for keyword in search_keywords:
        if (
            keyword_matches_tags(keyword, tags, match_mode)
            or keyword_matches_description(keyword, description)
        ):
            store.restore_accepted_keyword(bvid, keyword)
        else:
            store.reject_keyword_hit(bvid, keyword, tags, match_mode)
            rejected_keywords.append(keyword)
    kept = len(rejected_keywords) < len(search_keywords)
    if not kept:
        store.delete_video(bvid)
    store.commit()
    return kept, rejected_keywords


def filter_stored_search_videos(store: Store, match_mode: str) -> int:
    rows = store.connection.execute(
        """
        SELECT DISTINCT v.bvid, v.tags_json, v.description
        FROM videos v JOIN search_hits h ON h.bvid = v.bvid
        ORDER BY v.bvid
        """
    ).fetchall()
    deleted = 0
    for row in rows:
        try:
            tags = json.loads(row["tags_json"] or "[]")
        except json.JSONDecodeError:
            tags = []
        kept, _rejected = apply_keyword_tag_filter(
            store, row["bvid"], [str(tag) for tag in tags],
            row["description"] or "", match_mode
        )
        if not kept:
            deleted += 1
    return deleted


def enrich_pending_videos(
    client: BilibiliClient,
    store: Store,
    refresh: bool,
    max_videos: int | None,
    creator_cache_hours: float,
    require_keyword_tag: bool = False,
    tag_match_mode: str = "exact",
) -> tuple[int, int]:
    deleted = filter_stored_search_videos(store, tag_match_mode) if require_keyword_tag else 0
    pending = store.pending_bvids(refresh)
    if max_videos is not None:
        pending = pending[: max(0, max_videos)]
    print(f"videos to enrich: {len(pending)}")
    successful = 0
    for index, bvid in enumerate(pending, start=1):
        try:
            tags, description = enrich_video(client, store, bvid, creator_cache_hours)
            rejected_keywords: list[str] = []
            if require_keyword_tag:
                kept, rejected_keywords = apply_keyword_tag_filter(
                    store, bvid, tags, description, tag_match_mode
                )
                if not kept:
                    deleted += 1
                    print(
                        f"[{index}/{len(pending)}] deleted {bvid}: no keyword in tags or description; "
                        f"keywords={json_text(rejected_keywords)} tags={json_text(tags)}"
                    )
                    continue
            successful += 1
            if rejected_keywords:
                print(
                    f"[{index}/{len(pending)}] ok {bvid}; removed keyword hits: "
                    f"{json_text(rejected_keywords)}"
                )
            else:
                print(f"[{index}/{len(pending)}] ok {bvid}")
        except RiskControlError as exc:
            store.add_error("risk_control_core", bvid, exc)
            print(
                f"[{index}/{len(pending)}] stopped on risk control at {bvid}: {exc}. "
                "Committed rows are safe; wait before resuming.",
                file=sys.stderr,
            )
            break
        except KeyboardInterrupt:
            print("interrupted; committed rows are safe", file=sys.stderr)
            break
        except Exception as exc:  # noqa: BLE001
            store.add_error("enrich_video", bvid, exc)
            print(f"[{index}/{len(pending)}] failed {bvid}: {exc}", file=sys.stderr)
    return successful, deleted


def read_cookie(args: argparse.Namespace) -> str:
    if args.cookie_file:
        return args.cookie_file.read_text(encoding="utf-8").strip()
    return args.cookie or os.environ.get("BILI_COOKIE", "")


def parse_categories(raw: str) -> list[str]:
    values = [value.strip() for value in raw.split(",") if value.strip()]
    unknown = [value for value in values if value not in VIRTUAL_CATEGORIES]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown categories: {', '.join(unknown)}")
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/bilibili_videos.sqlite3"))
    parser.add_argument("--cookie", default="", help="Raw Bilibili Cookie header; prefer BILI_COOKIE")
    parser.add_argument("--cookie-file", type=Path, help="Text file containing the Cookie header")
    parser.add_argument("--sleep", type=float, default=1.2, help="Minimum seconds between requests")
    parser.add_argument("--timeout", type=int, default=25)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--jitter", type=float, default=0.5, help="Random pacing jitter in seconds")
    parser.add_argument("--max-backoff", type=float, default=30.0)
    parser.add_argument("--wbi-cache-hours", type=float, default=6.0)
    parser.add_argument(
        "--profile-circuit-minutes", type=int, default=60,
        help="Disable the optional profile endpoint after its first risk-control response",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    crawl = subparsers.add_parser("crawl", help="Discover, enrich, and export videos")
    crawl.add_argument("--categories", default="game,music,douga")
    crawl.add_argument("--page-size", type=int, default=20)
    crawl.add_argument("--max-pages", type=int, help="Maximum pages per virtual subchannel")
    crawl.add_argument("--bvid-file", type=Path)
    crawl.add_argument("--max-videos", type=int, help="Maximum videos to enrich in this run")
    crawl.add_argument("--refresh", action="store_true", help="Refresh already enriched videos")
    crawl.add_argument("--creator-cache-hours", type=float, default=24.0)
    crawl.add_argument("--out", type=Path, default=Path("output/bilibili_virtual_videos.csv"))
    crawl.add_argument("--only-target-tags", action="store_true")

    search = subparsers.add_parser(
        "search", help="Crawl keyword results using daily and adaptive time windows"
    )
    search.add_argument("--keywords", default="", help="Comma-separated keywords")
    search.add_argument("--keyword-file", type=Path, help="UTF-8 file with one keyword per line")
    search.add_argument("--start-date", type=parse_iso_date, required=True)
    search.add_argument("--end-date", type=parse_iso_date, required=True)
    search.add_argument(
        "--timezone", default=DEFAULT_TIMEZONE,
        help="Date boundary timezone (default: Asia/Shanghai, Beijing time)",
    )
    search.add_argument(
        "--max-search-pages", type=int, default=24,
        help="Split a time window when the API reports more pages than this",
    )
    search.add_argument(
        "--min-window-seconds", type=int, default=60,
        help="Smallest adaptive search window; a remaining overflow is reported",
    )
    search.add_argument(
        "--initial-window-days", type=int, default=1,
        help="Start with N-day windows and split adaptively when page limits are exceeded",
    )
    search.add_argument("--rediscover", action="store_true", help="Repeat completed search windows")
    search.add_argument("--discover-only", action="store_true")
    search.add_argument("--max-videos", type=int, help="Maximum videos to enrich in this run")
    search.add_argument("--refresh", action="store_true", help="Refresh already enriched videos")
    search.add_argument("--creator-cache-hours", type=float, default=24.0)
    search.add_argument(
        "--require-keyword-tag", action="store_true",
        help="Delete search videos when no matched keyword appears in tags or description",
    )
    search.add_argument(
        "--tag-match-mode", choices=("exact", "substring"), default="exact",
        help="How a search keyword is compared with individual tags",
    )
    search.add_argument("--out", type=Path, default=Path("output/bilibili_keyword_videos.csv"))
    search.add_argument("--coverage-out", type=Path)

    export = subparsers.add_parser("export", help="Export the existing SQLite database")
    export.add_argument("--out", type=Path, default=Path("output/bilibili_videos.csv"))
    export.add_argument("--coverage-out", type=Path)
    export.add_argument("--only-target-tags", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    store = Store(args.db)
    if args.command == "export":
        count = export_csv(store, args.out, args.only_target_tags)
        coverage_out = args.coverage_out or default_coverage_path(args.out)
        coverage_count = export_coverage_csv(store, coverage_out)
        print(f"exported {count} videos to {args.out}")
        print(f"exported {coverage_count} search windows to {coverage_out}")
        return 0

    run_id = str(uuid.uuid4())
    run_started_at = utc_now()
    client = BilibiliClient(
        read_cookie(args), args.sleep, args.timeout, args.retries,
        jitter_seconds=args.jitter,
        max_backoff_seconds=args.max_backoff,
        wbi_cache_seconds=int(args.wbi_cache_hours * 3600),
        profile_circuit_seconds=args.profile_circuit_minutes * 60,
    )
    credential = client.credential_summary()
    print(
        "credential status: "
        + ", ".join(f"{key}={'yes' if value else 'no'}" for key, value in credential.items())
    )
    if args.command == "search":
        keywords = load_keywords(args.keywords, args.keyword_file)
        if not keywords:
            parser.error("search requires --keywords and/or --keyword-file")
        if args.start_date > args.end_date:
            parser.error("--start-date must be on or before --end-date")
        if args.max_search_pages < 1:
            parser.error("--max-search-pages must be at least 1")
        if args.min_window_seconds < 1:
            parser.error("--min-window-seconds must be at least 1")
        if args.initial_window_days < 1:
            parser.error("--initial-window-days must be at least 1")
        try:
            get_timezone(args.timezone)
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))

        risk_stopped = False
        try:
            found, coverage_gaps = discover_dated_search(
                client, store, keywords, args.start_date, args.end_date,
                args.timezone, args.max_search_pages, args.min_window_seconds,
                args.rediscover, args.initial_window_days,
            )
        except RiskControlError as exc:
            found, coverage_gaps, risk_stopped = 0, 1, True
            print(
                f"search stopped by Bilibili risk control: {exc}. "
                "Wait before rerunning; completed windows remain saved.",
                file=sys.stderr,
            )
        print(f"search hits seen this run: {found}; unresolved coverage windows: {coverage_gaps}")
        successful = 0
        deleted = 0
        if not args.discover_only and not risk_stopped:
            successful, deleted = enrich_pending_videos(
                client, store, args.refresh, args.max_videos, args.creator_cache_hours,
                args.require_keyword_tag, args.tag_match_mode,
            )
        exported = export_csv(store, args.out)
        coverage_out = args.coverage_out or default_coverage_path(args.out)
        coverage_count = export_coverage_csv(store, coverage_out)
        print(
            f"enriched successfully: {successful}; tag-filter deleted: {deleted}; "
            f"exported videos: {exported}; "
            f"coverage rows: {coverage_count}"
        )
        print(f"video output: {args.out}")
        print(f"coverage output: {coverage_out}")
        store.save_request_run_stats(run_id, args.command, run_started_at, client)
        print(f"request stats: {json_text(client.request_stats())}")
        return 0

    categories = parse_categories(args.categories)
    try:
        found = discover_channel(client, store, categories, args.page_size, args.max_pages)
    except RiskControlError as exc:
        found = 0
        print(
            f"channel discovery stopped by Bilibili risk control: {exc}",
            file=sys.stderr,
        )
    print(f"channel discoveries seen this run: {found}")

    if args.bvid_file:
        found_file = discover_bvid_file(store, args.bvid_file)
        print(f"BV identifiers read from file: {found_file}")

    successful, _deleted = enrich_pending_videos(
        client, store, args.refresh, args.max_videos, args.creator_cache_hours
    )

    exported = export_csv(store, args.out, args.only_target_tags)
    print(f"enriched successfully: {successful}; exported rows: {exported}; output: {args.out}")
    store.save_request_run_stats(run_id, args.command, run_started_at, client)
    print(f"request stats: {json_text(client.request_stats())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
