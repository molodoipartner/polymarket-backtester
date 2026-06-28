from __future__ import annotations

import asyncio
import inspect
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from backend.data.polymarket_client import PolymarketClient, parse_datetime, parse_list
from backend.fixed_time import SERIES_IDS, TIMEFRAME_SECONDS, parse_period, validate_selection
from backend.storage import FileStore
from config.settings import settings

ProgressCallback = Callable[[dict], None | Awaitable[None]]
logger = logging.getLogger(__name__)


class APIRequestProgress:
    def __init__(self, report: ProgressCallback | None) -> None:
        self.report = report
        self.total = 0
        self.completed = 0
        self._last_report = 0.0
        self._lock = asyncio.Lock()

    async def add_total(self, count: int) -> None:
        if count <= 0:
            return
        async with self._lock:
            self.total += count
            await self._maybe_report_locked(force=True)

    async def update(self, event: str) -> None:
        async with self._lock:
            if event == "scheduled":
                self.total += 1
            elif event == "completed":
                self.completed += 1
            await self._maybe_report_locked()

    async def finish(self) -> None:
        if self.report:
            await DataCollector._report(self.report, {
                "stage": "download",
                "message": f"API requests completed: {self.completed} of {self.total}",
                "api_requests_completed": self.completed,
                "api_requests_total": self.total,
            })

    async def _maybe_report_locked(self, force: bool = False) -> None:
        now = time.monotonic()
        if self.report and (
            force or self.completed == self.total or now - self._last_report >= 0.25
        ):
            self._last_report = now
            await DataCollector._report(self.report, {
                "stage": "download",
                "message": f"API requests completed: {self.completed} of {self.total}",
                "api_requests_completed": self.completed,
                "api_requests_total": self.total,
            })


class DataCollector:
    def __init__(self, store: FileStore, client: PolymarketClient | None = None) -> None:
        self.store = store
        self.client = client or PolymarketClient(
            cache_dir=store.root / "cache" / "polymarket"
        )
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        await self.client.close()

    async def collect(
        self,
        asset_symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        progress: ProgressCallback | None = None,
    ) -> dict[str, int]:
        assets = validate_selection(asset_symbols, timeframe)
        start_time, end_time = parse_period(start_date, end_date)
        await self._report(progress, {
            "stage": "download", "message": "Loading historical data from the Polymarket API..."
        })
        async with self._lock:
            semaphore = asyncio.Semaphore(settings.history_request_concurrency)
            request_progress = APIRequestProgress(progress)
            missing = self.store.missing_fixed_coverage(assets, timeframe, start_time, end_time)
            missing_jobs = [
                (asset, missing_start, missing_end)
                for asset, ranges in missing.items()
                for missing_start, missing_end in ranges
            ]
            cached_assets = [asset for asset, ranges in missing.items() if not ranges]
            if cached_assets:
                await self._report(progress, {
                    "stage": "download",
                    "message": (
                        "Using local cache for: "
                        + ", ".join(cached_assets)
                        + ". Only missing date ranges will be downloaded."
                    ),
                })
            if missing_jobs:
                await self._report(progress, {
                    "stage": "download",
                    "message": f"Downloading {len(missing_jobs)} missing historical range(s).",
                })
            else:
                await self._report(progress, {
                    "stage": "download",
                    "message": "The requested period is already fully cached locally.",
                    "api_requests_completed": request_progress.completed,
                    "api_requests_total": request_progress.total,
                })

            series_results = await asyncio.gather(
                *(
                    self._collect_series(
                        asset, timeframe, missing_start, missing_end,
                        semaphore, request_progress,
                    )
                    for asset, missing_start, missing_end in missing_jobs
                )
            ) if missing_jobs else []
            await request_progress.finish()
            events = [event for result in series_results for event in result]
            newly_saved = self.store.save_fixed_events(events)
            for asset, missing_start, missing_end in missing_jobs:
                self.store.mark_fixed_coverage(asset, timeframe, missing_start, missing_end)
            requested_events = self.store.list_fixed_events(assets, timeframe, start_time, end_time)
            logger.info(
                "Saved %d fixed-time events for %s %s; %d events available in requested period",
                len(events), assets, timeframe, len(requested_events),
            )
            await self._report(progress, {
                "stage": "validation",
                "message": (
                    "Event parsing and validation completed. "
                    f"{len(requested_events)} local events are ready for the requested period."
                ),
                "saved_events": len(requested_events),
            })
            return {
                "events": len(requested_events),
                "new_events": newly_saved,
                "assets": len(assets),
            }

    async def _collect_series(
        self,
        asset: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        semaphore: asyncio.Semaphore,
        request_progress: APIRequestProgress,
    ) -> list[dict]:
        raw_events = await self.client.get_closed_events(
            SERIES_IDS[asset][timeframe], start_time, end_time,
            request_progress=request_progress.update, semaphore=semaphore,
        )
        existing = {
            event["id"]: event
            for event in self.store.list_fixed_events([asset], timeframe, start_time, end_time)
        }

        async def normalize(raw: dict) -> dict | None:
            event_id = str(raw.get("id") or "")
            if event_id in existing and {
                "expiration_time", "entry_time", "data_quality"
            } <= existing[event_id].keys() and existing[event_id].get(
                "data_quality", {}
            ).get("quote_source") == "gamma_resolution_only":
                return existing[event_id]
            market = (raw.get("markets") or [None])[0]
            if not market:
                return None
            raw_start = self._event_start(raw, market)
            raw_end = self._scheduled_end(raw, market)
            effective_end = raw_end or (
                raw_start + timedelta(seconds=TIMEFRAME_SECONDS[timeframe]) if raw_start else None
            )
            if (
                not raw_start or not effective_end
                or raw_start < start_time or effective_end > end_time
            ):
                return None
            token_ids = [str(value) for value in parse_list(market.get("clobTokenIds"))]
            if len(token_ids) < 2:
                return None
            condition_id = self._condition_id(raw, market)
            if not condition_id:
                return None
            try:
                return self._event(raw, market, asset, timeframe, condition_id)
            except (TypeError, ValueError) as error:
                return None

        normalized = await asyncio.gather(*(normalize(raw) for raw in raw_events))
        return [event for event in normalized if event is not None]

    @staticmethod
    def _event(
        raw: dict,
        market: dict,
        asset: str,
        timeframe: str,
        condition_id: str | None = None,
    ) -> dict | None:
        start = DataCollector._event_start(raw, market)
        scheduled_end = DataCollector._scheduled_end(raw, market)
        outcomes = [str(value).lower() for value in parse_list(market.get("outcomes"))]
        outcome_prices = [float(value) for value in parse_list(market.get("outcomePrices"))]
        if not start or len(outcomes) != len(outcome_prices):
            return None
        expected_end = start + timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
        if scheduled_end and abs((scheduled_end - expected_end).total_seconds()) > 1:
            return None
        resolved_index = next((i for i, price in enumerate(outcome_prices) if price >= 0.99), None)
        if resolved_index is None or outcomes[resolved_index] not in {"up", "down"}:
            return None
        return {
            "id": str(raw.get("id") or market.get("id")),
            "market_id": str(market.get("id")),
            "condition_id": str(condition_id or ""),
            "question": str(raw.get("title") or market.get("question")),
            "asset": asset,
            "timeframe": timeframe,
            "start_time": start.astimezone(timezone.utc).isoformat(),
            "end_time": expected_end.astimezone(timezone.utc).isoformat(),
            "entry_time": start.astimezone(timezone.utc).isoformat(),
            "expiration_time": expected_end.astimezone(timezone.utc).isoformat(),
            "resolved_outcome": outcomes[resolved_index],
            "source": "Polymarket Gamma closed events",
            "data_quality": {
                "is_synthetic": False,
                "quote_source": "gamma_resolution_only",
                "execution_price": "user-defined assumed ask price",
                "gamma_start_field": "startTime/eventStartTime",
                "gamma_end_field": "endDate",
            },
            "closed_time": raw.get("closedTime") or market.get("closedTime"),
        }

    @staticmethod
    def _condition_id(raw: dict, market: dict) -> str | None:
        return next(
            (
                str(value) for value in (
                    market.get("conditionId"),
                    market.get("condition_id"),
                    market.get("conditionID"),
                    raw.get("conditionId"),
                    raw.get("condition_id"),
                    raw.get("conditionID"),
                )
                if value
            ),
            None,
        )

    @staticmethod
    def _event_start(raw: dict, market: dict) -> datetime | None:
        return next(
            (
                parsed for parsed in (
                    parse_datetime(raw.get("start_time")),
                    parse_datetime(raw.get("startTime")),
                    parse_datetime(market.get("eventStartTime")),
                    parse_datetime(market.get("start_time")),
                    parse_datetime(market.get("startDate")),
                    parse_datetime(raw.get("start_date")),
                    parse_datetime(raw.get("startDate")),
                )
                if parsed is not None
            ),
            None,
        )

    @staticmethod
    def _scheduled_end(raw: dict, market: dict) -> datetime | None:
        return next(
            (
                parsed for parsed in (
                    parse_datetime(raw.get("expiration_time")),
                    parse_datetime(raw.get("end_date")),
                    parse_datetime(raw.get("endDate")),
                    parse_datetime(market.get("end_date")),
                    parse_datetime(market.get("endDate")),
                    parse_datetime(market.get("umaEndDate")),
                )
                if parsed is not None
            ),
            None,
        )

    @staticmethod
    async def _report(progress: ProgressCallback | None, payload: dict) -> None:
        if progress is None:
            return
        result = progress(payload)
        if inspect.isawaitable(result):
            await result
