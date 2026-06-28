from __future__ import annotations

import asyncio
import json
import hashlib
import os
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import httpx

from config.settings import settings

RequestProgress = Callable[[str], Awaitable[None]]


def parse_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)
    except ValueError:
        return None


class PolymarketClient:
    """Read-only client for Gamma fixed-time events."""

    # Gamma currently caps event responses at 100 rows even when a larger
    # limit is requested. Using that real cap is important because a shorter
    # response is also how we detect the final page.
    GAMMA_EVENTS_PAGE_LIMIT = 100
    MEMORY_CACHE_MAX_ENTRIES = 128

    def __init__(self, timeout: float = 20, cache_dir: Path | None = None) -> None:
        connection_limit = max(32, settings.history_request_concurrency * 2)
        try:
            import h2  # noqa: F401
            http2_enabled = True
        except ImportError:
            http2_enabled = False
        self.http = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            trust_env=True,
            http2=http2_enabled,
            limits=httpx.Limits(
                max_connections=connection_limit,
                max_keepalive_connections=min(connection_limit, settings.history_request_concurrency),
            ),
        )
        self.cache_dir = cache_dir
        self._memory_cache: dict[str, tuple[float, Any]] = {}
        if cache_dir is not None:
            cache_dir.mkdir(parents=True, exist_ok=True)

    async def close(self) -> None:
        await self.http.aclose()

    async def get_closed_events(
        self,
        series_id: str,
        start_time: datetime,
        end_time: datetime,
        request_progress: RequestProgress | None = None,
        semaphore: asyncio.Semaphore | None = None,
    ) -> list[dict]:
        page_size = min(settings.history_page_size, self.GAMMA_EVENTS_PAGE_LIMIT)
        chunks = []
        chunk_start = start_time
        while chunk_start < end_time:
            chunk_end = min(
                chunk_start + timedelta(days=settings.history_chunk_days), end_time
            )
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end

        async def fetch_chunk(chunk_start: datetime, chunk_end: datetime) -> list[dict]:
            rows = []
            chunk_complete = False
            for offset in range(0, settings.history_max_events_per_series, page_size):
                await self._notify(request_progress, "scheduled")
                try:
                    async def request() -> Any:
                        return await self._get_json(
                            f"{settings.gamma_api_url}/events",
                            {
                                "series_id": series_id,
                                "closed": "true",
                                "order": "endDate",
                                "ascending": "true",
                                "end_date_min": chunk_start.isoformat(),
                                "end_date_max": chunk_end.isoformat(),
                                "limit": page_size,
                                "offset": offset,
                            },
                            settings.market_cache_ttl_seconds,
                        )
                    if semaphore is None:
                        payload = await request()
                    else:
                        async with semaphore:
                            payload = await request()
                finally:
                    await self._notify(request_progress, "completed")
                page = payload if isinstance(payload, list) else []
                rows.extend(page)
                if len(page) < page_size:
                    chunk_complete = True
                    break
            if not chunk_complete:
                raise ValueError(
                    "A historical time chunk contains too many events; reduce HISTORY_CHUNK_DAYS"
                )
            return rows

        pages = await asyncio.gather(*(fetch_chunk(start, end) for start, end in chunks))
        result: dict[str, dict] = {}
        for event in (event for page in pages for event in page):
            event_id = str(event.get("id") or event.get("slug") or "")
            if event_id:
                result[event_id] = event
        return sorted(result.values(), key=lambda item: str(item.get("endDate") or ""))

    @staticmethod
    async def _notify(callback: RequestProgress | None, event: str) -> None:
        if callback is not None:
            await callback(event)

    async def _get_json(self, url: str, params: dict[str, Any], ttl: int) -> Any:
        if self.cache_dir is None or ttl <= 0:
            response = await self.http.get(url, params=params)
            response.raise_for_status()
            return response.json()

        identity = json.dumps([url, params], sort_keys=True, separators=(",", ":"))
        key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        now = time.time()
        memory = self._memory_cache.get(key)
        if memory and now - memory[0] <= ttl:
            return memory[1]

        path = self.cache_dir / f"{key}.json"
        cached = await self._read_cache(path)
        if cached and now - cached[0] <= ttl:
            self._remember(key, cached)
            return cached[1]

        try:
            response = await self.http.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            if cached:
                self._remember(key, cached)
                return cached[1]
            raise

        self._remember(key, (now, payload))
        await self._write_cache(path, payload)
        return payload

    def _remember(self, key: str, value: tuple[float, Any]) -> None:
        self._memory_cache.pop(key, None)
        self._memory_cache[key] = value
        while len(self._memory_cache) > self.MEMORY_CACHE_MAX_ENTRIES:
            self._memory_cache.pop(next(iter(self._memory_cache)))

    @staticmethod
    async def _read_cache(path: Path) -> tuple[float, Any] | None:
        def read() -> tuple[float, Any] | None:
            try:
                return path.stat().st_mtime, json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return None
        return await asyncio.to_thread(read)

    @staticmethod
    async def _write_cache(path: Path, payload: Any) -> None:
        def write() -> None:
            temporary = path.with_suffix(f".{os.getpid()}.{id(payload)}.tmp")
            try:
                temporary.write_text(
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    encoding="utf-8",
                )
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
        await asyncio.to_thread(write)
