from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK

from backend.data.polymarket_client import parse_list
from backend.storage.file_store import utc_iso
from config.settings import settings

POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_CRYPTO_WS_URL = "wss://ws-live-data.polymarket.com"
POLYMARKET_CLOB_URL = "https://clob.polymarket.com"
POLYMARKET_TOP_BID = 0.99
POLYMARKET_PRICE_MIDPOINT = 0.5
POLYMARKET_PRICE_EPSILON = 1e-9
RECONNECT_STABLE_RESET_SECONDS = 60
RECONNECT_MAX_DELAY_SECONDS = 10
REST_QUOTE_POLL_SECONDS = 1.0
WS_STALE_POLL_AFTER_SECONDS = 2.0
CRYPTO_STALE_AFTER_SECONDS = 3.0
CRYPTO_FAST_RECONNECT_DELAY_SECONDS = 0.1
TRANSIENT_DISCONNECT_GRACE_SECONDS = 3.0
NEXT_CONTRACT_PRECONNECT_SECONDS = 10.0
PRE_WINDOW_HISTORY_SECONDS = 10.0
PRELOADED_HISTORY_PRIORITY_SECONDS = 0.05
LIVE_STREAM_MIN_INTERVAL_SECONDS = 0.25
HISTORY_CAPTURE_INTERVAL_SECONDS = 0.5
LIVE_SNAPSHOT_HISTORY_LIMIT = 180
SAVED_RECORD_METADATA_FALLBACK_MAX_BYTES = 2 * 1024 * 1024
SOURCE_TIMELINE_RETENTION_SECONDS = 15.0
SAMPLE_WRITE_MIN_INTERVAL_SECONDS = 0.5
SAMPLE_WRITE_QUEUE_SIZE = 5000
SAMPLE_WRITE_DRAIN_TIMEOUT_SECONDS = 2.0
PARTIAL_CONTRACT_GRACE_SECONDS = 10
MAX_ACTIVE_LIVE_MONITORS_PER_USER = 5
TARGET_PRICE_DECIMALS_BY_ASSET = {
    "btc": 2,
    "eth": 2,
    "sol": 2,
    "xrp": 5,
    "doge": 6,
    "bnb": 4,
    "hype": 4,
}

logger = logging.getLogger(__name__)
_SAVED_RECORD_PREVIEW_CACHE: dict[Path, dict[str, Any]] = {}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


LIVE_HISTORY_MEMORY_LIMIT = _env_int("LIVE_MONITOR_HISTORY_MEMORY_LIMIT", 7200)
LIVE_DISCONNECT_PERIOD_LIMIT = _env_int("LIVE_MONITOR_DISCONNECT_PERIOD_LIMIT", 500)
LIVE_SUBSCRIBER_QUEUE_SIZE = _env_int("LIVE_MONITOR_SUBSCRIBER_QUEUE_SIZE", 50, 1)
SAVED_RECORD_PREVIEW_CACHE_LIMIT = _env_int(
    "LIVE_MONITOR_SAVED_PREVIEW_CACHE_LIMIT",
    32,
    1,
)


@dataclass
class LiveQuote:
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    tick_size: str | None = None
    updated_at: float | None = None
    event_at: float | None = None
    event_count: int = 0
    price_change_count: int = 0


@dataclass(frozen=True)
class LiveToken:
    token_id: str
    market_question: str
    outcome: str
    condition_id: str | None = None


@dataclass
class ResolvedPolymarketEvent:
    url: str
    slug: str
    title: str
    markets: list[dict]
    tokens: list[LiveToken]
    crypto_symbol: str
    window_size_seconds: int | None = None
    window_start_ts: int | None = None
    window_end_ts: int | None = None
    event_title: str | None = None


@dataclass
class PreloadedPolymarketOrderBook:
    event: ResolvedPolymarketEvent
    state: LiveMonitorState
    orderbook: PolymarketLiveOrderBook
    task: asyncio.Task
    quote_timeline: dict[str, list[LiveQuote]] = field(default_factory=dict)
    history: list[dict] = field(default_factory=list)
    target_price: float | None = None
    target_price_ts: float | None = None
    history_task: asyncio.Task | None = None
    history_stopped: bool = False
    history_truncated_count: int = 0
    history_truncated_before_ts: float | None = None


@dataclass
class LiveMonitorState:
    quotes: dict[str, LiveQuote] = field(default_factory=dict)
    crypto_price: float | None = None
    crypto_updated_at: float | None = None
    target_price: float | None = None
    quote_status: str = "starting"
    crypto_status: str = "starting"
    last_error: str | None = None


class EmptyOrderBookError(RuntimeError):
    pass


class TransientWebSocketDisconnect(ConnectionError):
    pass


def _disconnect_period(
    start_ts: float,
    end_ts: float,
    duration: float,
    reason: str | None,
) -> dict:
    period = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "duration_seconds": round(duration, 3),
    }
    if reason:
        period["reason"] = reason
    return period


def _is_recordable_disconnect(duration: float) -> bool:
    return duration >= TRANSIENT_DISCONNECT_GRACE_SECONDS


def _is_fast_crypto_reconnect(reason: str | None) -> bool:
    return reason == "Crypto price WebSocket stream ended"


def _append_limited(items: list, item: Any, limit: int) -> None:
    items.append(item)
    if limit > 0 and len(items) > limit:
        del items[:len(items) - limit]


class PolymarketLiveOrderBook:
    def __init__(
        self,
        token_ids: list[str],
        state: LiveMonitorState,
        on_update,
    ) -> None:
        self.token_ids = list(dict.fromkeys(token_ids))
        self.state = state
        self.on_update = on_update
        self._closed = False
        self._ws = None
        self._pending_snapshot_tokens = set(self.token_ids)
        self._books = {
            token_id: {"bids": {}, "asks": {}}
            for token_id in self.token_ids
        }
        self._tick_sizes = {token_id: "0.01" for token_id in self.token_ids}
        self._last_message_at: float | None = None
        self._connected_at: float | None = None
        self._ever_connected = False
        self._disconnected_since: float | None = None
        self._disconnected_wall_since: float | None = None
        self._disconnected_reason: str | None = None
        self.disconnect_count = 0
        self.disconnected_seconds = 0.0
        self.disconnect_periods: list[dict] = []

    async def run(self) -> None:
        retry = 0
        while not self._closed:
            try:
                await self._connect_and_listen()
                retry = 0
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if self._closed:
                    break
                self._mark_disconnected(reason=_error_message(error))
                self.state.quote_status = "reconnecting"
                self.state.last_error = _error_message(error)
                logger.warning("Polymarket WS reconnecting: %s", self.state.last_error)
                await self.on_update("status", {"source": "polymarket"})
                stable_for = self._connection_age()
                retry = _next_reconnect_attempt(retry, stable_for)
                await asyncio.sleep(_reconnect_delay(retry))

    async def _connect_and_listen(self) -> None:
        async with websockets.connect(
            POLYMARKET_WS_URL,
            ping_interval=None,
            ping_timeout=20,
            open_timeout=10,
            close_timeout=10,
        ) as ws:
            self._ws = ws
            self._pending_snapshot_tokens = set(self.token_ids)
            self._last_message_at = time.monotonic()
            self._connected_at = time.monotonic()
            self._mark_connected()
            self.state.quote_status = "connected"
            self.state.last_error = None
            await ws.send(json.dumps({
                "assets_ids": self.token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            }))
            await self.on_update("status", {"source": "polymarket"})
            await self.on_update(
                "log",
                {
                    "level": "info",
                    "source": "polymarket",
                    "message": f"Connected to Polymarket CLOB for {len(self.token_ids)} token(s).",
                },
            )
            listener = asyncio.create_task(self._listen(ws))
            heartbeat = asyncio.create_task(self._heartbeat(ws))
            disconnect_reason = "Polymarket WebSocket stream ended"
            try:
                done, _pending = await asyncio.wait(
                    (listener, heartbeat),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    task.result()
                raise TransientWebSocketDisconnect(
                    disconnect_reason
                )
            except Exception as error:
                disconnect_reason = _error_message(error)
                raise
            finally:
                if not self._closed:
                    self._mark_disconnected(reason=disconnect_reason)
                    self.state.quote_status = "reconnecting"
                    await self.on_update(
                        "log",
                        {
                            "level": "warning",
                            "source": "polymarket",
                            "message": "Polymarket WebSocket listener stopped; reconnecting if tracking is still active.",
                        },
                    )
                for task in (listener, heartbeat):
                    task.cancel()
                await asyncio.gather(listener, heartbeat, return_exceptions=True)
                self._ws = None

    async def _listen(self, ws) -> None:
        async for raw in ws:
            self._last_message_at = time.monotonic()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            if raw.strip().upper() == "PONG":
                continue
            try:
                payload = json.loads(raw)
            except (TypeError, json.JSONDecodeError) as error:
                raise RuntimeError(
                    f"invalid Polymarket WS message: {error}"
                ) from error
            messages = payload if isinstance(payload, list) else [payload]
            for message in messages:
                if isinstance(message, dict):
                    await self._apply_message(message)

    async def _heartbeat(self, ws) -> None:
        while True:
            await ws.send("PING")
            await asyncio.sleep(10)

    async def _apply_message(self, message: dict) -> None:
        event_type = message.get("event_type")
        now = time.time()

        if event_type == "book" or (
            not event_type and ("bids" in message or "asks" in message)
        ):
            token_id = str(message.get("asset_id", ""))
            if token_id not in self.state.quotes:
                return
            bids = message.get("bids") or []
            asks = message.get("asks") or []
            self._books[token_id] = {
                "bids": {
                    Decimal(str(level["price"])): Decimal(str(level["size"]))
                    for level in bids
                    if Decimal(str(level.get("size", "0"))) > 0
                },
                "asks": {
                    Decimal(str(level["price"])): Decimal(str(level["size"]))
                    for level in asks
                    if Decimal(str(level.get("size", "0"))) > 0
                },
            }
            self._tick_sizes[token_id] = self._infer_tick_size(token_id)
            best_bid = max(self._books[token_id]["bids"], default=None)
            best_ask = min(self._books[token_id]["asks"], default=None)
            self._pending_snapshot_tokens.discard(token_id)
            if not self._pending_snapshot_tokens:
                self.state.quote_status = "ready"
            await self._store_quote(
                token_id,
                float(best_bid) if best_bid is not None else None,
                float(best_ask) if best_ask is not None else None,
                now,
                bid_size=(
                    float(self._books[token_id]["bids"][best_bid])
                    if best_bid is not None
                    else None
                ),
                ask_size=(
                    float(self._books[token_id]["asks"][best_ask])
                    if best_ask is not None
                    else None
                ),
            )
            return

        if event_type == "price_change":
            for change in message.get("price_changes") or []:
                token_id = str(change.get("asset_id", ""))
                if (
                    token_id not in self.state.quotes
                    or token_id in self._pending_snapshot_tokens
                ):
                    continue
                side = str(change.get("side", "")).lower()
                price = change.get("price")
                size = change.get("size")
                if (
                    side in {"buy", "sell"}
                    and price not in (None, "")
                    and size not in (None, "")
                ):
                    book_side = "bids" if side == "buy" else "asks"
                    level_price = Decimal(str(price))
                    level_size = Decimal(str(size))
                    if level_size > 0:
                        self._books[token_id][book_side][level_price] = level_size
                    else:
                        self._books[token_id][book_side].pop(level_price, None)
                book_best_bid = max(self._books[token_id]["bids"], default=None)
                book_best_ask = min(self._books[token_id]["asks"], default=None)
                message_best_bid = _polymarket_bid(change.get("best_bid"))
                message_best_ask = _polymarket_ask(change.get("best_ask"))
                best_bid = (
                    Decimal(str(message_best_bid))
                    if message_best_bid is not None
                    else book_best_bid
                )
                best_ask = (
                    Decimal(str(message_best_ask))
                    if message_best_ask is not None
                    else book_best_ask
                )
                await self._store_quote(
                    token_id,
                    float(best_bid) if best_bid is not None else None,
                    float(best_ask) if best_ask is not None else None,
                    now,
                    bid_size=self._polymarket_level_size(
                        token_id,
                        "bids",
                        float(best_bid) if best_bid is not None else None,
                    ),
                    ask_size=self._polymarket_level_size(
                        token_id,
                        "asks",
                        float(best_ask) if best_ask is not None else None,
                    ),
                )
            return

        if event_type == "best_bid_ask":
            token_id = str(message.get("asset_id", ""))
            if (
                token_id not in self.state.quotes
                or token_id in self._pending_snapshot_tokens
            ):
                return
            await self._store_quote(
                token_id,
                _polymarket_bid(message.get("best_bid")),
                _polymarket_ask(message.get("best_ask")),
                now,
                bid_size=self._polymarket_level_size(
                    token_id, "bids", _polymarket_bid(message.get("best_bid"))
                ),
                ask_size=self._polymarket_level_size(
                    token_id, "asks", _polymarket_ask(message.get("best_ask"))
                ),
            )
            return

        if event_type == "tick_size_change":
            token_id = str(message.get("asset_id", ""))
            new_tick_size = str(message.get("new_tick_size", "")).strip()
            if token_id not in self.state.quotes or not new_tick_size:
                return
            self._tick_sizes[token_id] = new_tick_size
            quote = self.state.quotes[token_id]
            quote.tick_size = new_tick_size
            quote.event_at = now
            quote.event_count += 1
            await self.on_update("quote", {"token_id": token_id})

    async def _store_quote(
        self,
        token_id: str,
        bid: float | None,
        ask: float | None,
        now: float,
        bid_size: float | None = None,
        ask_size: float | None = None,
    ) -> None:
        bid, ask, bid_size, ask_size = _normalize_polymarket_quote(
            bid,
            ask,
            bid_size,
            ask_size,
        )
        previous = self.state.quotes[token_id]
        if bid is not None and ask is not None and bid > ask:
            await self.on_update(
                "log",
                {
                    "level": "warning",
                    "source": "polymarket",
                    "message": (
                        "Recorded crossed Polymarket order book for "
                        f"{token_id}: bid={bid}, ask={ask}."
                    ),
                },
            )
        price_changed = previous.bid != bid or previous.ask != ask
        self.state.quotes[token_id] = LiveQuote(
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            tick_size=self._tick_sizes.get(token_id),
            updated_at=now if price_changed else previous.updated_at,
            event_at=now,
            event_count=previous.event_count + 1,
            price_change_count=previous.price_change_count + int(price_changed),
        )
        await self.on_update("quote", {"token_id": token_id})

    def _polymarket_level_size(
        self,
        token_id: str,
        side: str,
        price: float | None,
    ) -> float | None:
        if price is None:
            return None
        value = self._books[token_id][side].get(Decimal(str(price)))
        return float(value) if value is not None else None

    def _infer_tick_size(self, token_id: str) -> str:
        book = self._books[token_id]
        prices = (max(book["bids"], default=None), min(book["asks"], default=None))
        if any(
            price is not None
            and (price < Decimal("0.04") or price > Decimal("0.96"))
            for price in prices
        ):
            return "0.001"
        return "0.01"

    def _connection_age(self) -> float:
        if self._connected_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._connected_at)

    def _mark_connected(self) -> None:
        if self._disconnected_since is not None:
            now = time.monotonic()
            wall_now = time.time()
            duration = now - self._disconnected_since
            if _is_recordable_disconnect(duration):
                self.disconnect_count += 1
                self.disconnected_seconds += duration
                _append_limited(
                    self.disconnect_periods,
                    _disconnect_period(
                        self._disconnected_wall_since
                        if self._disconnected_wall_since is not None
                        else wall_now,
                        wall_now,
                        duration,
                        self._disconnected_reason,
                    ),
                    LIVE_DISCONNECT_PERIOD_LIMIT,
                )
            self._disconnected_since = None
            self._disconnected_wall_since = None
            self._disconnected_reason = None
        self._ever_connected = True

    def _mark_disconnected(self, reason: str | None = None) -> None:
        if self._disconnected_since is not None:
            if reason and not self._disconnected_reason:
                self._disconnected_reason = reason
            return
        if (
            self._closed
            or not self._ever_connected
        ):
            return
        self._disconnected_since = time.monotonic()
        self._disconnected_wall_since = time.time()
        self._disconnected_reason = reason

    def connection_stats(self) -> dict:
        disconnected_seconds = self.disconnected_seconds
        disconnect_count = self.disconnect_count
        periods = list(self.disconnect_periods)
        if self._disconnected_since is not None:
            now = time.monotonic()
            wall_now = time.time()
            duration = now - self._disconnected_since
            if _is_recordable_disconnect(duration):
                disconnect_count += 1
                disconnected_seconds += duration
                periods.append(_disconnect_period(
                    self._disconnected_wall_since
                    if self._disconnected_wall_since is not None
                    else wall_now,
                    wall_now,
                    duration,
                    self._disconnected_reason,
                ))
        return {
            "disconnect_count": disconnect_count,
            "disconnected_seconds": round(disconnected_seconds, 3),
            "disconnect_periods": periods,
        }

    def attach(self, state: LiveMonitorState, on_update) -> None:
        self.state = state
        self.on_update = on_update

    def reset_tracking_stats(self) -> None:
        connected = self._ws is not None and not self._closed
        self.disconnect_count = 0
        self.disconnected_seconds = 0.0
        self._disconnected_since = None if connected else time.monotonic()
        self._disconnected_wall_since = None if connected else time.time()
        self._disconnected_reason = None
        self._ever_connected = connected
        self.disconnect_periods = []

    def health(self) -> dict:
        connection_stats = self.connection_stats()
        return {
            "connected": self._ws is not None and not self._closed,
            "connected_for": self._connection_age(),
            **connection_stats,
            "last_message_age": (
                time.monotonic() - self._last_message_at
                if self._last_message_at is not None
                else None
            ),
            "pending_snapshots": len(self._pending_snapshot_tokens),
        }

    async def close(self) -> None:
        self._closed = True
        self.state.quote_status = "stopped"
        if self._ws is not None:
            await self._ws.close()


class PolymarketCryptoPriceFeed:
    def __init__(self, symbol: str, state: LiveMonitorState, on_update) -> None:
        self.symbol = normalize_crypto_symbol(symbol)
        self.state = state
        self.on_update = on_update
        self._closed = False
        self._ws = None
        self._connected_at: float | None = None
        self._last_message_at: float | None = None
        self._last_price_at: float | None = None
        self._ever_connected = False
        self._disconnected_since: float | None = None
        self._disconnected_wall_since: float | None = None
        self._disconnected_reason: str | None = None
        self.disconnect_count = 0
        self.disconnected_seconds = 0.0
        self.disconnect_periods: list[dict] = []

    async def run(self) -> None:
        retry = 0
        while not self._closed:
            try:
                await self._connect_and_listen()
                retry = 0
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if self._closed:
                    break
                self._mark_disconnected(reason=_error_message(error))
                self.state.crypto_status = "reconnecting"
                self.state.last_error = _error_message(error)
                logger.warning("Crypto WS reconnecting: %s", self.state.last_error)
                await self.on_update(
                    "log",
                    {
                        "level": "warning",
                        "source": "crypto",
                        "message": f"Crypto price feed reconnecting: {self.state.last_error}",
                    },
                )
                await self.on_update("status", {"source": "crypto"})
                stable_for = self._connection_age()
                retry = _next_reconnect_attempt(retry, stable_for)
                delay = (
                    CRYPTO_FAST_RECONNECT_DELAY_SECONDS
                    if _is_fast_crypto_reconnect(self.state.last_error)
                    else _reconnect_delay(retry)
                )
                await asyncio.sleep(delay)

    async def _connect_and_listen(self) -> None:
        async with websockets.connect(
            POLYMARKET_CRYPTO_WS_URL,
            ping_interval=20,
            ping_timeout=20,
            open_timeout=10,
            close_timeout=10,
        ) as ws:
            self._ws = ws
            self._connected_at = time.monotonic()
            self._last_message_at = time.monotonic()
            self._last_price_at = time.monotonic()
            self._mark_connected()
            self.state.crypto_status = "connected"
            self.state.last_error = None
            await ws.send(json.dumps({
                "action": "subscribe",
                "subscriptions": [{
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": "",
                }],
            }))
            await self.on_update("status", {"source": "crypto"})
            await self.on_update(
                "log",
                {
                    "level": "info",
                    "source": "crypto",
                    "message": f"Connected to crypto price feed: {self.symbol}.",
                },
            )
            disconnect_reason = "Crypto price WebSocket stream ended"
            try:
                await self._listen(ws)
                raise TransientWebSocketDisconnect(
                    disconnect_reason
                )
            except Exception as error:
                disconnect_reason = _error_message(error)
                raise
            finally:
                if not self._closed:
                    self._mark_stream_disconnected(disconnect_reason)
                self._ws = None

    async def _listen(self, ws) -> None:
        async for raw in ws:
            self._last_message_at = time.monotonic()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            if not raw or not raw.strip():
                continue
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            payload = message.get("payload") or {}
            if str(payload.get("symbol", "")).lower() != self.symbol:
                continue
            price_point = _crypto_price_point_from_payload(payload)
            if price_point is None:
                continue
            price, price_ts = price_point
            self._last_price_at = time.monotonic()
            self.state.crypto_price = price
            self.state.crypto_updated_at = price_ts or time.time()
            self.state.crypto_status = "ready"
            self.state.last_error = None
            await self.on_update(
                "crypto",
                {"symbol": self.symbol, "price_ts": self.state.crypto_updated_at},
            )

    def _connection_age(self) -> float:
        if self._connected_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._connected_at)

    def _mark_connected(self) -> None:
        if self._disconnected_since is not None:
            now = time.monotonic()
            wall_now = time.time()
            duration = now - self._disconnected_since
            if _is_recordable_disconnect(duration):
                self.disconnect_count += 1
                self.disconnected_seconds += duration
                _append_limited(
                    self.disconnect_periods,
                    _disconnect_period(
                        self._disconnected_wall_since
                        if self._disconnected_wall_since is not None
                        else wall_now,
                        wall_now,
                        duration,
                        self._disconnected_reason,
                    ),
                    LIVE_DISCONNECT_PERIOD_LIMIT,
                )
            self._disconnected_since = None
            self._disconnected_wall_since = None
            self._disconnected_reason = None
        self._ever_connected = True

    def _mark_stream_disconnected(self, reason: str | None) -> None:
        if reason and reason.startswith("Crypto price feed stale"):
            self._mark_disconnected(
                since=(
                    self._last_price_at
                    if self._last_price_at is not None
                    else self._connected_at
                ),
                wall_since=self.state.crypto_updated_at,
                reason=reason,
            )
            return
        self._mark_disconnected(reason=reason)

    def _mark_disconnected(
        self,
        since: float | None = None,
        wall_since: float | None = None,
        reason: str | None = None,
    ) -> None:
        if self._disconnected_since is not None:
            if reason and not self._disconnected_reason:
                self._disconnected_reason = reason
            return
        if (
            self._closed
            or not self._ever_connected
        ):
            return
        self._disconnected_since = since if since is not None else time.monotonic()
        self._disconnected_wall_since = (
            wall_since if wall_since is not None else time.time()
        )
        self._disconnected_reason = reason

    def connection_stats(self) -> dict:
        disconnected_seconds = self.disconnected_seconds
        disconnect_count = self.disconnect_count
        periods = list(self.disconnect_periods)
        if self._disconnected_since is not None:
            now = time.monotonic()
            wall_now = time.time()
            duration = now - self._disconnected_since
            if _is_recordable_disconnect(duration):
                disconnect_count += 1
                disconnected_seconds += duration
                periods.append(_disconnect_period(
                    self._disconnected_wall_since
                    if self._disconnected_wall_since is not None
                    else wall_now,
                    wall_now,
                    duration,
                    self._disconnected_reason,
                ))
        return {
            "disconnect_count": disconnect_count,
            "disconnected_seconds": round(disconnected_seconds, 3),
            "disconnect_periods": periods,
        }

    def reset_tracking_stats(self) -> None:
        connected = self._ws is not None and not self._closed
        self.disconnect_count = 0
        self.disconnected_seconds = 0.0
        self._disconnected_since = None if connected else time.monotonic()
        self._disconnected_wall_since = None if connected else time.time()
        self._disconnected_reason = None
        self._ever_connected = connected
        self.disconnect_periods = []

    def health(self) -> dict:
        connection_stats = self.connection_stats()
        return {
            "connected": self._ws is not None and not self._closed,
            "connected_for": self._connection_age(),
            **connection_stats,
            "last_message_age": (
                time.monotonic() - self._last_message_at
                if self._last_message_at is not None
                else None
            ),
            "last_price_age": (
                time.monotonic() - self._last_price_at
                if self._last_price_at is not None
                else None
            ),
        }

    async def close(self) -> None:
        self._closed = True
        self.state.crypto_status = "stopped"
        if self._ws is not None:
            await self._ws.close()


class LiveMonitorSession:
    def __init__(
        self,
        session_id: str,
        user_id: str,
        event: ResolvedPolymarketEvent,
        data_root: Path,
    ) -> None:
        self.id = session_id
        self.user_id = user_id
        self.event = event
        self.started_at_ts = time.time()
        self.created_at = utc_iso()
        self.data_root = data_root
        self.samples_path = data_root / "samples" / f"{session_id}.jsonl"
        self.meta_path = data_root / "sessions" / f"{session_id}.json"
        self.saved_dir = _saved_records_dir(data_root, user_id)
        self.tokens_by_id = {token.token_id: token for token in event.tokens}
        self.primary_token_id = event.tokens[0].token_id
        self.current_record_id = uuid.uuid4().hex
        self.state = LiveMonitorState(
            quotes={token.token_id: LiveQuote() for token in event.tokens}
        )
        self.polymarket = PolymarketLiveOrderBook(
            list(self.state.quotes),
            self.state,
            self._handle_update,
        )
        self.crypto = PolymarketCryptoPriceFeed(
            event.crypto_symbol,
            self.state,
            self._handle_update,
        )
        self._tasks: list[asyncio.Task] = []
        self._sample_writer_task: asyncio.Task | None = None
        self._sample_queue: asyncio.Queue[dict] = asyncio.Queue(
            maxsize=SAMPLE_WRITE_QUEUE_SIZE
        )
        self._subscribers: set[asyncio.Queue] = set()
        self._write_lock = asyncio.Lock()
        self._history: list[dict] = []
        self._history_truncated_count = 0
        self._history_truncated_before_ts: float | None = None
        self._quote_timeline: dict[str, list[LiveQuote]] = {
            token.token_id: [] for token in event.tokens
        }
        self._crypto_timeline: list[tuple[float, float]] = []
        self._target_price_ts: float | None = None
        self._logs: list[dict] = []
        self._saved = False
        self._last_history_sample_ts: float | None = None
        self._next_history_sample_ts: float | None = None
        self._stopped = False
        self._tick_count = 0
        self._last_tick_at: float | None = None
        self._last_rest_poll_at: float | None = None
        self._rest_poll_count = 0
        self._last_rest_error_at = 0.0
        self._last_live_update_publish_at = 0.0
        self._pending_live_update_count = 0
        self._last_sample_enqueue_at = 0.0
        self._sample_write_count = 0
        self._sample_drop_count = 0
        self._last_sample_write_error_at = 0.0
        self._last_sample_write_error: str | None = None
        self._preloaded_next: PreloadedPolymarketOrderBook | None = None

    @property
    def is_finished(self) -> bool:
        return self._stopped or self.state.quote_status in {"ended", "stopped"}

    async def start(self) -> None:
        self._write_meta()
        self._reset_history_schedule(time.time())
        self._sample_writer_task = asyncio.create_task(self._sample_writer_loop())
        self._tasks = [
            asyncio.create_task(self.polymarket.run()),
            asyncio.create_task(self.crypto.run()),
            asyncio.create_task(self._sampler_loop()),
            asyncio.create_task(self._rest_quote_poll_loop()),
        ]
        if self.event.window_end_ts:
            self._tasks.append(asyncio.create_task(self._window_rollover_loop()))
        await self._log(
            "info",
            "session",
            f"Started live monitor for {self.event.slug}.",
        )
        await self._publish("snapshot", self.snapshot())

    async def close(self) -> None:
        self._stopped = True
        saved_record = await self.save_finished_record(status="stopped")
        preloaded = self._preloaded_next
        self._preloaded_next = None
        close_tasks = [self.polymarket.close(), self.crypto.close()]
        if preloaded is not None:
            close_tasks.append(preloaded.orderbook.close())
        await asyncio.gather(*close_tasks, return_exceptions=True)
        await self._cancel_runtime_tasks()
        if self._sample_writer_task:
            try:
                await asyncio.wait_for(
                    self._sample_queue.join(),
                    timeout=SAMPLE_WRITE_DRAIN_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Timed out while flushing %s live monitor sample(s)",
                    self._sample_queue.qsize(),
                )
            self._sample_writer_task.cancel()
            await asyncio.gather(self._sample_writer_task, return_exceptions=True)
            self._sample_writer_task = None
        payload = self.snapshot()
        payload["saved_record"] = saved_record
        await self._publish("status", payload)

    def _prune_tasks(self) -> None:
        self._tasks = [task for task in self._tasks if not task.done()]

    async def _cancel_runtime_tasks(
        self,
        exclude: asyncio.Task | None = None,
    ) -> None:
        tasks = [
            task
            for task in self._tasks
            if task is not exclude and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks = [
            task
            for task in self._tasks
            if task is exclude and not task.done()
        ]

    async def _sampler_loop(self) -> None:
        while not self._stopped:
            try:
                if self._next_history_sample_ts is None:
                    self._reset_history_schedule(time.time())
                sample_ts = self._next_history_sample_ts or _ceil_history_sample_time(
                    time.time()
                )
                await asyncio.sleep(max(0, sample_ts - time.time()))
                captured_at = time.time()
                self._tick_count += 1
                self._last_tick_at = captured_at
                self._capture_history_row(
                    timestamp=sample_ts,
                    captured_at=captured_at,
                )
                self._next_history_sample_ts = round(
                    sample_ts + HISTORY_CAPTURE_INTERVAL_SECONDS,
                    3,
                )
                await self._publish("tick", self.snapshot())
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.state.last_error = _error_message(error)
                logger.exception("Live monitor sampler iteration failed")
                await self._log(
                    "error",
                    "sampler",
                    f"Sampler iteration failed: {self.state.last_error}",
                )

    async def _rest_quote_poll_loop(self) -> None:
        while not self._stopped:
            try:
                await asyncio.sleep(REST_QUOTE_POLL_SECONDS)
                health = self.polymarket.health()
                last_message_age = health.get("last_message_age")
                should_poll = (
                    not health.get("connected")
                    or health.get("pending_snapshots", 0) > 0
                    or last_message_age is None
                    or last_message_age >= WS_STALE_POLL_AFTER_SECONDS
                )
                if should_poll:
                    await self._poll_rest_quotes()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.state.last_error = _error_message(error)
                logger.exception("Live monitor REST poll iteration failed")
                await self._log(
                    "error",
                    "rest",
                    f"REST quote poll iteration failed: {self.state.last_error}",
                )

    async def _poll_rest_quotes(self) -> None:
        token_ids = list(self.state.quotes)
        if not token_ids:
            return
        orderbook = self.polymarket
        try:
            async with httpx.AsyncClient(timeout=5, follow_redirects=True, trust_env=True) as client:
                for token_id in token_ids:
                    bid, ask, bid_size, ask_size = await _fetch_clob_book(client, token_id)
                    if self.polymarket is not orderbook or token_id not in self.state.quotes:
                        return
                    bid, ask, bid_size, ask_size = _normalize_polymarket_quote(
                        bid,
                        ask,
                        bid_size,
                        ask_size,
                    )
                    if bid is None and ask is None:
                        continue
                    if bid is not None and ask is not None and bid > ask:
                        now = time.time()
                        if now - self._last_rest_error_at >= 10:
                            self._last_rest_error_at = now
                            await self._log(
                                "warning",
                                "rest",
                                (
                                    "Recorded crossed REST book for "
                                    f"{token_id}: bid={bid}, ask={ask}"
                                ),
                            )
                    await orderbook._store_quote(
                        token_id,
                        bid,
                        ask,
                        time.time(),
                        bid_size=bid_size,
                        ask_size=ask_size,
                    )
            self._last_rest_poll_at = time.time()
            self._rest_poll_count += 1
        except Exception as error:
            now = time.time()
            if now - self._last_rest_error_at >= 10:
                self._last_rest_error_at = now
                self.state.last_error = _error_message(error)
                logger.warning("REST quote fallback failed: %s", self.state.last_error)
                await self._log(
                    "warning",
                    "rest",
                    f"REST quote fallback failed: {self.state.last_error}",
                )

    async def _window_rollover_loop(self) -> None:
        while not self._stopped and self.event.window_end_ts:
            next_url = self._next_window_url()
            if next_url:
                preload_delay = max(
                    0,
                    self.event.window_end_ts
                    - NEXT_CONTRACT_PRECONNECT_SECONDS
                    - time.time(),
                )
                await asyncio.sleep(preload_delay)
                if self._stopped:
                    return
                if time.time() < self.event.window_end_ts:
                    await self._preload_next_event(next_url)
            switch_at = float(self.event.window_end_ts)
            if self._preloaded_next is not None:
                switch_at += PRELOADED_HISTORY_PRIORITY_SECONDS
            delay = max(0, switch_at - time.time())
            await asyncio.sleep(delay)
            if self._stopped:
                return
            rollover_at = float(self.event.window_end_ts)
            if not next_url:
                saved = await self.save_finished_record(status="ended")
                await self._publish("saved", {"record": saved, **self.snapshot()})
                await self._log(
                    "info",
                    "session",
                    "Window ended and no next timestamp could be inferred.",
                )
                self.state.quote_status = "ended"
                await self._publish("ended", self.snapshot())
                self._stopped = True
                await asyncio.gather(
                    self.polymarket.close(),
                    self.crypto.close(),
                    return_exceptions=True,
                )
                await self._cancel_runtime_tasks(exclude=asyncio.current_task())
                return
            preloaded = self._take_preloaded_next(next_url)
            try:
                next_event = (
                    preloaded.event
                    if preloaded is not None
                    else await resolve_polymarket_event(
                        next_url,
                        self.event.crypto_symbol,
                        normalize_live_slug=False,
                    )
                )
            except Exception as error:
                self.state.last_error = _error_message(error)
                logger.exception("Failed to resolve next Polymarket window")
                await self._log(
                    "error",
                    "session",
                    f"Could not resolve next contract: {self.state.last_error}",
                )
                await self._publish("status", self.snapshot())
                await asyncio.sleep(3)
                continue
            saved = self._prepare_finished_record(status="ended")
            self._saved = True
            await self._switch_event(
                next_event,
                target_price=None,
                target_price_is_authoritative=False,
                started_at_ts=next_event.window_start_ts or rollover_at,
                preloaded=preloaded,
            )
            self._tasks.append(
                asyncio.create_task(
                    self._save_prepared_finished_record(saved, publish_saved=True)
                )
            )
            self._prune_tasks()

    def add_subscriber(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=LIVE_SUBSCRIBER_QUEUE_SIZE)
        self._subscribers.add(queue)
        queue.put_nowait({"event": "snapshot", "data": self.snapshot()})
        return queue

    def remove_subscriber(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    async def _preload_next_event(self, next_url: str) -> None:
        if self._preloaded_next is not None:
            return
        try:
            event = await resolve_polymarket_event(
                next_url,
                self.event.crypto_symbol,
                normalize_live_slug=False,
            )
        except Exception as error:
            self.state.last_error = _error_message(error)
            await self._log(
                "warning",
                "session",
                f"Could not preload next contract: {self.state.last_error}",
            )
            return
        if self._stopped:
            return
        preload_state = LiveMonitorState(
            quotes={token.token_id: LiveQuote() for token in event.tokens}
        )
        preload_quote_timeline: dict[str, list[LiveQuote]] = {
            token.token_id: [] for token in event.tokens
        }

        async def handle_preload_update(update_type: str, detail: dict) -> None:
            if update_type == "quote":
                token_id = str(detail.get("token_id", ""))
                self._remember_quote_update_from_state(
                    preload_quote_timeline,
                    preload_state,
                    token_id,
                )
            elif update_type == "log":
                return

        orderbook = PolymarketLiveOrderBook(
            list(preload_state.quotes),
            preload_state,
            handle_preload_update,
        )
        self._prune_tasks()
        task = asyncio.create_task(orderbook.run())
        self._tasks.append(task)
        self._preloaded_next = PreloadedPolymarketOrderBook(
            event=event,
            state=preload_state,
            orderbook=orderbook,
            task=task,
            quote_timeline=preload_quote_timeline,
        )
        self._preloaded_next.history_task = asyncio.create_task(
            self._preloaded_history_loop(self._preloaded_next)
        )
        self._tasks.append(self._preloaded_next.history_task)
        await self._log(
            "info",
            "session",
            f"Preconnected next contract: {event.slug}.",
        )

    def _take_preloaded_next(
        self,
        next_url: str,
    ) -> PreloadedPolymarketOrderBook | None:
        preloaded = self._preloaded_next
        self._preloaded_next = None
        if preloaded is None:
            return None
        expected_slug = polymarket_slug_from_url(next_url)
        if preloaded.event.slug == expected_slug:
            return preloaded
        preloaded.history_stopped = True
        preloaded.task.cancel()
        if preloaded.history_task is not None:
            preloaded.history_task.cancel()
        return None

    async def _preloaded_history_loop(
        self,
        preloaded: PreloadedPolymarketOrderBook,
    ) -> None:
        start_ts = self._history_window_start_ts(preloaded.event)
        now = time.time()
        sample_ts = (
            start_ts
            if start_ts is not None and now <= start_ts
            else _ceil_history_sample_time(now)
        )
        while not self._stopped and not preloaded.history_stopped:
            if (
                preloaded.event.window_end_ts is not None
                and sample_ts > preloaded.event.window_end_ts
            ):
                return
            await asyncio.sleep(max(0, sample_ts - time.time()))
            if self._stopped or preloaded.history_stopped:
                return
            captured_at = time.time()
            self._capture_preloaded_history_row(
                preloaded,
                timestamp=sample_ts,
                captured_at=captured_at,
            )
            sample_ts = round(
                sample_ts + HISTORY_CAPTURE_INTERVAL_SECONDS,
                3,
            )

    async def _log(self, level: str, source: str, message: str) -> None:
        entry = {
            "t": int(time.time()),
            "time": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "source": source,
            "message": message,
        }
        self._logs.append(entry)
        self._logs = self._logs[-200:]
        if level == "error":
            logger.error("[%s] %s", source, message)
        elif level == "warning":
            logger.warning("[%s] %s", source, message)
        else:
            logger.info("[%s] %s", source, message)
        await self._publish("log", {"log": entry, **self.snapshot()})

    def _next_window_url(self) -> str | None:
        if self.event.window_start_ts is None or self.event.window_size_seconds is None:
            return None
        next_start = self.event.window_start_ts + self.event.window_size_seconds
        next_slug = _replace_slug_timestamp(self.event.slug, next_start)
        if not next_slug:
            return None
        return f"https://polymarket.com/event/{next_slug}"

    async def _switch_event(
        self,
        event: ResolvedPolymarketEvent,
        target_price: float | None = None,
        target_price_is_authoritative: bool = False,
        started_at_ts: float | None = None,
        preloaded: PreloadedPolymarketOrderBook | None = None,
    ) -> None:
        previous_orderbook = self.polymarket
        self.event = event
        self.started_at_ts = started_at_ts or time.time()
        self.tokens_by_id = {token.token_id: token for token in event.tokens}
        self.primary_token_id = event.tokens[0].token_id
        self.current_record_id = uuid.uuid4().hex
        self.state.target_price = (
            self._price_to_beat(target_price) if target_price is not None else None
        )
        self._target_price_ts = None
        if target_price is not None:
            self._target_price_ts = started_at_ts or event.window_start_ts or time.time()
        self._history = []
        self._history_truncated_count = 0
        self._history_truncated_before_ts = None
        self._saved = False
        self._last_history_sample_ts = None
        self._next_history_sample_ts = None
        self._quote_timeline = {token.token_id: [] for token in event.tokens}
        self.crypto.reset_tracking_stats()
        self._close_orderbook_soon(previous_orderbook)
        if preloaded is not None:
            self.state.quotes = preloaded.state.quotes
            self.state.quote_status = preloaded.state.quote_status
            self._quote_timeline = {
                token.token_id: list(preloaded.quote_timeline.get(token.token_id, []))
                for token in event.tokens
            }
            if target_price is None and preloaded.target_price is not None:
                self.state.target_price = preloaded.target_price
                self._target_price_ts = preloaded.target_price_ts
            if preloaded.history:
                self._history = self._contract_window_history(
                    preloaded.history,
                    event,
                )
                self._history_truncated_count = preloaded.history_truncated_count
                self._history_truncated_before_ts = (
                    preloaded.history_truncated_before_ts
                )
                self._last_history_sample_ts = _optional_float(
                    self._history[-1].get("t")
                    if self._history
                    else None
                )
            if preloaded.state.last_error:
                self.state.last_error = preloaded.state.last_error
            self.polymarket = preloaded.orderbook
            self.polymarket.reset_tracking_stats()
            self.polymarket.attach(self.state, self._handle_update)
            for token_id in self.state.quotes:
                self._remember_quote_update(token_id)
            preloaded.history_stopped = True
            if preloaded.history_task is not None:
                preloaded.history_task.cancel()
        else:
            self.state.quotes = {token.token_id: LiveQuote() for token in event.tokens}
            self.state.quote_status = "starting"
            self.polymarket = PolymarketLiveOrderBook(
                list(self.state.quotes),
                self.state,
                self._handle_update,
            )
            self._prune_tasks()
            self._tasks.append(asyncio.create_task(self.polymarket.run()))
        if self.state.target_price is not None:
            self._refresh_history_targets()
        captured_at = time.time()
        start_sample_ts = _floor_history_sample_time(captured_at)
        self._capture_history_row(
            timestamp=start_sample_ts,
            captured_at=captured_at,
        )
        self._next_history_sample_ts = round(
            start_sample_ts + HISTORY_CAPTURE_INTERVAL_SECONDS,
            3,
        )
        self._write_meta()
        await self._log("info", "session", f"Switched to next contract: {event.slug}.")
        await self._publish("rolled", self.snapshot())

    async def _handle_update(self, update_type: str, detail: dict) -> None:
        if update_type == "log":
            await self._log(
                str(detail.get("level", "info")),
                str(detail.get("source", "system")),
                str(detail.get("message", "")),
            )
            return
        target_locked = False
        if update_type == "quote":
            token_id = str(detail.get("token_id", ""))
            self._remember_quote_update(token_id)
        elif update_type == "crypto":
            self._remember_crypto_update()
            target_locked = self._maybe_lock_target_price(self.state.crypto_updated_at)
            if target_locked:
                self._refresh_history_targets()
        if update_type in {"quote", "crypto"}:
            self._pending_live_update_count += 1
            now = time.time()
            should_publish = target_locked or (
                now - self._last_live_update_publish_at
                >= LIVE_STREAM_MIN_INTERVAL_SECONDS
            )
            should_sample = target_locked or (
                now - self._last_sample_enqueue_at
                >= SAMPLE_WRITE_MIN_INTERVAL_SECONDS
            )
            if not should_publish and not should_sample:
                return
            payload = self.snapshot()
            payload["update"] = {
                "type": update_type,
                **detail,
                "coalesced_count": self._pending_live_update_count,
            }
            if should_sample:
                self._last_sample_enqueue_at = now
                self._queue_sample(update_type, detail, payload)
            if should_publish:
                self._last_live_update_publish_at = now
                self._pending_live_update_count = 0
                await self._publish(update_type, payload)
            return
        payload = self.snapshot()
        payload["update"] = {"type": update_type, **detail}
        self._queue_sample(update_type, detail, payload)
        await self._publish(update_type, payload)

    async def _publish(self, event: str, data: dict) -> None:
        message = {"event": event, "data": data}
        stale = []
        for queue in self._subscribers:
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                stale.append(queue)
        for queue in stale:
            self._subscribers.discard(queue)

    def _queue_sample(self, update_type: str, detail: dict, payload: dict) -> None:
        row = {
            "id": uuid.uuid4().hex,
            "session_id": self.id,
            "created_at": utc_iso(),
            "update_type": update_type,
            "detail": detail,
            "crypto_price": self.state.crypto_price,
            "quotes": payload["quotes"],
        }
        try:
            self._sample_queue.put_nowait(row)
        except asyncio.QueueFull:
            self._sample_drop_count += 1
            self._last_sample_write_error = (
                f"Sample writer queue is full; dropped {self._sample_drop_count} row(s)."
            )

    async def _sample_writer_loop(self) -> None:
        while True:
            if self._stopped and self._sample_queue.empty():
                return
            try:
                row = await asyncio.wait_for(self._sample_queue.get(), timeout=0.25)
            except asyncio.TimeoutError:
                continue
            try:
                async with self._write_lock:
                    await asyncio.to_thread(_append_jsonl, self.samples_path, row)
                self._sample_write_count += 1
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._last_sample_write_error = _error_message(error)
                now = time.time()
                if now - self._last_sample_write_error_at >= 10:
                    self._last_sample_write_error_at = now
                    logger.exception("Live monitor sample writer failed")
                    await self._log(
                        "error",
                        "storage",
                        f"Sample writer failed: {self._last_sample_write_error}",
                    )
            finally:
                self._sample_queue.task_done()

    def _reset_history_schedule(self, anchor_ts: float | None = None) -> None:
        anchor = time.time() if anchor_ts is None else anchor_ts
        window_start = self._history_window_start_ts(self.event)
        if window_start is not None and anchor <= window_start:
            self._next_history_sample_ts = window_start
            return
        self._next_history_sample_ts = _ceil_history_sample_time(anchor)

    def _remember_quote_update(self, token_id: str) -> None:
        self._remember_quote_update_from_state(
            self._quote_timeline,
            self.state,
            token_id,
        )
        self._trim_quote_timeline(token_id)

    def _remember_quote_update_from_state(
        self,
        timelines: dict[str, list[LiveQuote]],
        state: LiveMonitorState,
        token_id: str,
    ) -> None:
        quote = state.quotes.get(token_id)
        if quote is None:
            return
        event_at = quote.event_at if quote.event_at is not None else time.time()
        snapshot = LiveQuote(
            bid=quote.bid,
            ask=quote.ask,
            bid_size=quote.bid_size,
            ask_size=quote.ask_size,
            tick_size=quote.tick_size,
            updated_at=quote.updated_at,
            event_at=event_at,
            event_count=quote.event_count,
            price_change_count=quote.price_change_count,
        )
        timeline = timelines.setdefault(token_id, [])
        if timeline and timeline[-1].event_at == event_at:
            timeline[-1] = snapshot
        else:
            timeline.append(snapshot)

    def _remember_crypto_update(self) -> None:
        if self.state.crypto_price is None:
            return
        updated_at = self.state.crypto_updated_at or time.time()
        point = (updated_at, self.state.crypto_price)
        for index, (existing_at, _price) in enumerate(self._crypto_timeline):
            if existing_at == updated_at:
                self._crypto_timeline[index] = point
                break
        else:
            self._crypto_timeline.append(point)
            if (
                len(self._crypto_timeline) > 1
                and updated_at < self._crypto_timeline[-2][0]
            ):
                self._crypto_timeline.sort(key=lambda item: item[0])
        self._trim_crypto_timeline()

    def _trim_quote_timeline(self, token_id: str) -> None:
        if self._last_history_sample_ts is None:
            return
        cutoff = self._last_history_sample_ts - SOURCE_TIMELINE_RETENTION_SECONDS
        timeline = self._quote_timeline.get(token_id) or []
        while len(timeline) > 1 and (timeline[1].event_at or 0) < cutoff:
            timeline.pop(0)

    def _trim_crypto_timeline(self) -> None:
        if self._last_history_sample_ts is None:
            return
        cutoff = self._last_history_sample_ts - SOURCE_TIMELINE_RETENTION_SECONDS
        while len(self._crypto_timeline) > 1 and self._crypto_timeline[1][0] < cutoff:
            self._crypto_timeline.pop(0)

    def _quote_at(self, token_id: str, sample_ts: float) -> LiveQuote | None:
        return self._quote_at_from(
            self._quote_timeline,
            self.state,
            token_id,
            sample_ts,
        )

    def _quote_at_from(
        self,
        timelines: dict[str, list[LiveQuote]],
        state: LiveMonitorState,
        token_id: str,
        sample_ts: float,
    ) -> LiveQuote | None:
        timeline = timelines.get(token_id) or []
        for quote in reversed(timeline):
            if quote.event_at is not None and quote.event_at <= sample_ts:
                return quote
        quote = state.quotes.get(token_id)
        if quote is None:
            return None
        if quote.event_at is None or quote.event_at <= sample_ts:
            return quote
        return None

    def _crypto_price_at(self, sample_ts: float) -> tuple[float | None, float | None]:
        for updated_at, price in reversed(self._crypto_timeline):
            if updated_at <= sample_ts:
                return price, updated_at
        if self.state.crypto_price is None:
            return None, None
        updated_at = self.state.crypto_updated_at
        if updated_at is None or updated_at <= sample_ts:
            return self.state.crypto_price, updated_at
        return None, None

    def _target_lock_after_ts(self) -> float | None:
        if self.event.window_start_ts is None:
            return self.started_at_ts
        window_start = float(self.event.window_start_ts)
        if self.started_at_ts <= window_start:
            return window_start
        return self.started_at_ts

    def _target_price_candidate(
        self,
        timestamp: float,
        lock_after: float | None,
    ) -> tuple[float, float] | None:
        max_ts = timestamp + 1e-6
        for updated_at, price in self._crypto_timeline:
            if lock_after is not None and updated_at + 1e-6 < lock_after:
                continue
            if updated_at <= max_ts:
                return price, updated_at
        if self._crypto_timeline:
            return None
        if self.state.crypto_price is None:
            return None
        updated_at = self.state.crypto_updated_at or timestamp
        if lock_after is not None and updated_at + 1e-6 < lock_after:
            return None
        if updated_at > max_ts:
            return None
        return self.state.crypto_price, updated_at

    def _maybe_lock_target_price(self, timestamp: float | None = None) -> bool:
        if self.state.target_price is not None:
            return False
        lock_after = self._target_lock_after_ts()
        timestamp = time.time() if timestamp is None else timestamp
        if lock_after is not None and timestamp < lock_after:
            return False
        candidate = self._target_price_candidate(timestamp, lock_after)
        if candidate is None:
            return False
        price, price_ts = candidate
        self.state.target_price = self._price_to_beat(price)
        self._target_price_ts = price_ts
        return True

    def _maybe_lock_preloaded_target_price(
        self,
        preloaded: PreloadedPolymarketOrderBook,
        timestamp: float,
    ) -> bool:
        if preloaded.target_price is not None:
            return False
        lock_after = (
            float(preloaded.event.window_start_ts)
            if preloaded.event.window_start_ts is not None
            else timestamp
        )
        if timestamp < lock_after:
            return False
        candidate = self._target_price_candidate(timestamp, lock_after)
        if candidate is None:
            return False
        price, price_ts = candidate
        preloaded.target_price = _price_to_beat(price, preloaded.event.crypto_symbol)
        preloaded.target_price_ts = price_ts
        return True

    def _refresh_history_targets(self) -> None:
        self._refresh_history_targets_for(
            self._history,
            self.event,
            self.state.target_price,
            self._target_price_ts,
        )

    def _refresh_preloaded_history_targets(
        self,
        preloaded: PreloadedPolymarketOrderBook,
    ) -> None:
        self._refresh_history_targets_for(
            preloaded.history,
            preloaded.event,
            preloaded.target_price,
            preloaded.target_price_ts,
        )

    def _refresh_history_targets_for(
        self,
        history: list[dict],
        event: ResolvedPolymarketEvent,
        target: float | None,
        target_price_ts: float | None,
    ) -> None:
        window_start = _optional_float(event.window_start_ts)
        for row in history:
            timestamp = _optional_float(row.get("t"))
            if timestamp is None:
                continue
            crypto_price = _optional_float(row.get("crypto_price"))
            if (
                target is None
                or crypto_price is None
                or (window_start is not None and timestamp + 1e-9 < window_start)
            ):
                row["target_price"] = None
                row["target_price_ts"] = None
                row["deviation_pct"] = None
                continue
            row["target_price"] = target
            row["target_price_ts"] = (
                round(target_price_ts, 3)
                if target_price_ts is not None
                else None
            )
            row["deviation_pct"] = (
                round((crypto_price - target) / target * 100, 4)
                if target
                else None
            )

    def _price_to_beat(self, price: float | None) -> float | None:
        return _price_to_beat(price, self.event.crypto_symbol)

    def _append_history_row(
        self,
        timestamp: float,
        quote: LiveQuote,
        crypto_price: float,
        captured_at: float,
        crypto_updated_at: float | None,
    ) -> None:
        if self._last_history_sample_ts == timestamp and self._history:
            existing_captured_at = _optional_float(self._history[-1].get("captured_at"))
            if existing_captured_at is None or captured_at <= existing_captured_at:
                self._history[-1] = self._history_row(
                    timestamp,
                    quote,
                    crypto_price,
                    captured_at=captured_at,
                    crypto_updated_at=crypto_updated_at,
                )
            return
        if (
            self._last_history_sample_ts is not None
            and timestamp < self._last_history_sample_ts
        ):
            return
        self._last_history_sample_ts = timestamp
        self._history.append(
            self._history_row(
                timestamp,
                quote,
                crypto_price,
                captured_at=captured_at,
                crypto_updated_at=crypto_updated_at,
            )
        )
        self._trim_history_memory()
        self._trim_quote_timeline(self.primary_token_id)
        self._trim_crypto_timeline()

    def _trim_history_memory(self) -> None:
        if LIVE_HISTORY_MEMORY_LIMIT <= 0:
            return
        overflow = len(self._history) - LIVE_HISTORY_MEMORY_LIMIT
        if overflow <= 0:
            return
        removed = self._history[:overflow]
        last_removed = removed[-1] if removed else None
        self._history_truncated_count += overflow
        self._history_truncated_before_ts = _optional_float(
            last_removed.get("t") if last_removed else None
        )
        del self._history[:overflow]

    def _append_preloaded_history_row(
        self,
        preloaded: PreloadedPolymarketOrderBook,
        timestamp: float,
        quote: LiveQuote,
        crypto_price: float,
        captured_at: float,
        crypto_updated_at: float | None,
    ) -> None:
        if preloaded.history and preloaded.history[-1].get("t") == timestamp:
            existing_captured_at = _optional_float(
                preloaded.history[-1].get("captured_at")
            )
            if existing_captured_at is None or captured_at <= existing_captured_at:
                preloaded.history[-1] = self._history_row_for(
                    preloaded.event,
                    preloaded.target_price,
                    preloaded.target_price_ts,
                    timestamp,
                    quote,
                    crypto_price,
                    captured_at=captured_at,
                    crypto_updated_at=crypto_updated_at,
                )
            return
        if preloaded.history and timestamp < float(preloaded.history[-1].get("t", 0)):
            return
        preloaded.history.append(
            self._history_row_for(
                preloaded.event,
                preloaded.target_price,
                preloaded.target_price_ts,
                timestamp,
                quote,
                crypto_price,
                captured_at=captured_at,
                crypto_updated_at=crypto_updated_at,
            )
        )
        self._trim_preloaded_history_memory(preloaded)

    def _trim_preloaded_history_memory(
        self,
        preloaded: PreloadedPolymarketOrderBook,
    ) -> None:
        if LIVE_HISTORY_MEMORY_LIMIT <= 0:
            return
        overflow = len(preloaded.history) - LIVE_HISTORY_MEMORY_LIMIT
        if overflow <= 0:
            return
        removed = preloaded.history[:overflow]
        last_removed = removed[-1] if removed else None
        preloaded.history_truncated_count += overflow
        preloaded.history_truncated_before_ts = _optional_float(
            last_removed.get("t") if last_removed else None
        )
        del preloaded.history[:overflow]

    def _capture_preloaded_history_row(
        self,
        preloaded: PreloadedPolymarketOrderBook,
        timestamp: float,
        captured_at: float | None = None,
    ) -> None:
        captured_at = time.time() if captured_at is None else captured_at
        sample_ts = round(timestamp, 3)
        if not self._is_history_sample_in_window(sample_ts, preloaded.event):
            return
        if self._is_connection_gap_sample(sample_ts):
            return
        if self._maybe_lock_preloaded_target_price(preloaded, sample_ts):
            self._refresh_preloaded_history_targets(preloaded)
        quote = self._quote_at_from(
            preloaded.quote_timeline,
            preloaded.state,
            preloaded.event.tokens[0].token_id,
            sample_ts,
        )
        crypto_price, crypto_updated_at = self._crypto_price_at(sample_ts)
        if (
            quote is None
            or crypto_price is None
            or (quote.bid is None and quote.ask is None)
        ):
            return
        self._append_preloaded_history_row(
            preloaded,
            sample_ts,
            quote,
            crypto_price,
            captured_at=captured_at,
            crypto_updated_at=crypto_updated_at,
        )

    @staticmethod
    def _history_window_start_ts(event: ResolvedPolymarketEvent) -> float | None:
        if event.window_start_ts is None:
            return None
        return round(float(event.window_start_ts) - PRE_WINDOW_HISTORY_SECONDS, 3)

    @staticmethod
    def _history_window_end_ts(event: ResolvedPolymarketEvent) -> float | None:
        if event.window_end_ts is None:
            return None
        return round(float(event.window_end_ts), 3)

    def _is_history_sample_in_window(
        self,
        timestamp: float,
        event: ResolvedPolymarketEvent | None = None,
    ) -> bool:
        event = event or self.event
        window_start = self._history_window_start_ts(event)
        if window_start is not None and timestamp + 1e-9 < window_start:
            return False
        window_end = self._history_window_end_ts(event)
        if window_end is not None and timestamp > window_end + 1e-9:
            return False
        return True

    def _contract_window_history(
        self,
        history: list[dict],
        event: ResolvedPolymarketEvent,
    ) -> list[dict]:
        window_start = self._history_window_start_ts(event)
        if window_start is None:
            return list(history)
        window_end = self._history_window_end_ts(event)
        filtered = []
        for row in history:
            timestamp = _optional_float(row.get("t"))
            if timestamp is None or timestamp < window_start:
                continue
            if window_end is not None and timestamp > window_end:
                continue
            filtered.append(row)
        return filtered

    def _complete_history_grid(self, history: list[dict]) -> list[dict]:
        window_start = self._history_window_start_ts(self.event)
        window_end = self._history_window_end_ts(self.event)
        if window_start is None or window_end is None or window_end < window_start:
            return history
        rows_by_ts = {
            round(timestamp, 3): row
            for row in history
            if (timestamp := _optional_float(row.get("t"))) is not None
        }
        if not rows_by_ts:
            return []
        first_ts = min(rows_by_ts)
        last_ts = max(rows_by_ts)
        interval = HISTORY_CAPTURE_INTERVAL_SECONDS
        grid_start = (
            window_start
            if first_ts <= window_start + interval + 1e-9
            else first_ts
        )
        grid_end = (
            window_end
            if last_ts >= window_end - interval - 1e-9
            else last_ts
        )
        sorted_rows = [
            rows_by_ts[timestamp]
            for timestamp in sorted(rows_by_ts)
        ]
        completed = []
        previous: dict | None = None
        for timestamp in _history_sample_timestamps(grid_start, grid_end):
            row = rows_by_ts.get(timestamp)
            if row is None:
                row = self._filled_history_row(timestamp, previous, sorted_rows)
            if row is None:
                continue
            completed.append(row)
            previous = row
        self._refresh_history_targets_for(
            completed,
            self.event,
            self.state.target_price,
            self._target_price_ts,
        )
        return completed

    def _filled_history_row(
        self,
        timestamp: float,
        previous: dict | None,
        sorted_rows: list[dict],
    ) -> dict | None:
        source = previous
        if source is None:
            source = next(
                (
                    row
                    for row in sorted_rows
                    if (_optional_float(row.get("t")) or 0) > timestamp
                ),
                None,
            )
        if source is None:
            return None
        row = dict(source)
        captured_at = _optional_float(row.get("captured_at"))
        if captured_at is None:
            captured_at = timestamp
        captured_at = max(captured_at, timestamp)
        row["t"] = timestamp
        row["time"] = _format_history_time(timestamp)
        row["captured_at"] = round(captured_at, 3)
        row["captured_time"] = _format_history_time(captured_at)
        row["sample_delay_ms"] = round(max(0, captured_at - timestamp) * 1000, 1)
        crypto_updated_at = _optional_float(row.get("crypto_updated_at"))
        row["crypto_age_seconds"] = (
            round(max(0, timestamp - crypto_updated_at), 3)
            if crypto_updated_at is not None
            else None
        )
        quote_event_at = _optional_float(row.get("quote_event_at"))
        row["quote_age_seconds"] = (
            round(max(0, timestamp - quote_event_at), 3)
            if quote_event_at is not None
            else None
        )
        row["seconds_left"] = (
            round(max(0, self.event.window_end_ts - timestamp), 3)
            if self.event.window_end_ts
            else None
        )
        return row

    def _capture_history_row(
        self,
        timestamp: float | None = None,
        captured_at: float | None = None,
    ) -> None:
        captured_at = time.time() if captured_at is None else captured_at
        sample_ts = (
            _floor_history_sample_time(captured_at)
            if timestamp is None
            else round(timestamp, 3)
        )
        if not self._is_history_sample_in_window(sample_ts):
            return
        if self._is_connection_gap_sample(sample_ts):
            return
        if self._maybe_lock_target_price(sample_ts):
            self._refresh_history_targets()
        quote = self._quote_at(self.primary_token_id, sample_ts)
        crypto_price, crypto_updated_at = self._crypto_price_at(sample_ts)
        if (
            quote is None
            or crypto_price is None
            or (quote.bid is None and quote.ask is None)
        ):
            return
        self._append_history_row(
            sample_ts,
            quote,
            crypto_price,
            captured_at=captured_at,
            crypto_updated_at=crypto_updated_at,
        )

    def _fresh_crypto_price(self, now: float | None = None) -> float | None:
        if self.state.crypto_price is None:
            return None
        if self.state.crypto_updated_at is None:
            return self.state.crypto_price
        now = time.time() if now is None else now
        if now - self.state.crypto_updated_at > CRYPTO_STALE_AFTER_SECONDS:
            return None
        return self.state.crypto_price

    def _current_connection_gap_periods(
        self,
        connection_stats: dict | None = None,
        crypto_connection_stats: dict | None = None,
    ) -> list[dict]:
        connection_stats = connection_stats or self.polymarket.connection_stats()
        crypto_connection_stats = (
            crypto_connection_stats or self.crypto.connection_stats()
        )
        return (
            self._connection_gap_periods("polymarket", connection_stats)
            + self._connection_gap_periods("crypto", crypto_connection_stats)
        )

    def _is_connection_gap_sample(
        self,
        timestamp: float,
        periods: list[dict] | None = None,
    ) -> bool:
        for period in periods or self._current_connection_gap_periods():
            start_ts = _optional_float(period.get("start_ts"))
            end_ts = _optional_float(period.get("end_ts"))
            if start_ts is None or end_ts is None:
                continue
            if start_ts <= timestamp < end_ts:
                return True
        return False

    def _history_without_connection_gaps(
        self,
        history: list[dict],
        periods: list[dict],
    ) -> list[dict]:
        filtered = []
        for row in history:
            timestamp = _optional_float(row.get("t"))
            if timestamp is None or not self._is_connection_gap_sample(
                timestamp,
                periods,
            ):
                filtered.append(row)
        return filtered

    def _history_row(
        self,
        timestamp: float,
        quote: LiveQuote,
        crypto_price: float,
        captured_at: float | None = None,
        crypto_updated_at: float | None = None,
    ) -> dict:
        return self._history_row_for(
            self.event,
            self.state.target_price,
            self._target_price_ts,
            timestamp,
            quote,
            crypto_price,
            captured_at=captured_at,
            crypto_updated_at=crypto_updated_at,
        )

    def _history_row_for(
        self,
        event: ResolvedPolymarketEvent,
        target: float | None,
        target_price_ts: float | None,
        timestamp: float,
        quote: LiveQuote,
        crypto_price: float,
        captured_at: float | None = None,
        crypto_updated_at: float | None = None,
    ) -> dict:
        yes_bid = round(quote.bid * 100, 4) if quote.bid is not None else None
        yes_ask = round(quote.ask * 100, 4) if quote.ask is not None else None
        no_bid = round(100 - yes_ask, 4) if yes_ask is not None else None
        no_ask = round(100 - yes_bid, 4) if yes_bid is not None else None
        captured_at = timestamp if captured_at is None else captured_at
        quote_event_at = quote.event_at
        quote_updated_at = quote.updated_at
        deviation = (
            round((crypto_price - target) / target * 100, 4)
            if target
            else None
        )
        return {
            "t": timestamp,
            "time": _format_history_time(timestamp),
            "captured_at": round(captured_at, 3),
            "captured_time": _format_history_time(captured_at),
            "sample_delay_ms": round(max(0, captured_at - timestamp) * 1000, 1),
            "crypto_price": crypto_price,
            "crypto_updated_at": (
                round(crypto_updated_at, 3)
                if crypto_updated_at is not None
                else None
            ),
            "crypto_age_seconds": (
                round(max(0, timestamp - crypto_updated_at), 3)
                if crypto_updated_at is not None
                else None
            ),
            "target_price": target,
            "target_price_ts": (
                round(target_price_ts, 3)
                if target_price_ts is not None
                else None
            ),
            "deviation_pct": deviation,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "quote_event_at": (
                round(quote_event_at, 3)
                if quote_event_at is not None
                else None
            ),
            "quote_updated_at": (
                round(quote_updated_at, 3)
                if quote_updated_at is not None
                else None
            ),
            "quote_age_seconds": (
                round(max(0, timestamp - quote_event_at), 3)
                if quote_event_at is not None
                else None
            ),
            "no_bid": no_bid,
            "no_ask": no_ask,
            "yes_mid": (
                round((yes_bid + yes_ask) / 2, 4)
                if yes_bid is not None and yes_ask is not None
                else None
            ),
            "spread": (
                round(yes_ask - yes_bid, 4)
                if yes_bid is not None and yes_ask is not None
                else None
            ),
            "seconds_left": (
                round(max(0, event.window_end_ts - timestamp), 3)
                if event.window_end_ts
                else None
            ),
        }

    def _prepare_finished_record(self, status: str) -> dict:
        final_sample_ts = (
            float(self.event.window_end_ts)
            if status == "ended" and self.event.window_end_ts is not None
            else None
        )
        self._capture_history_row(timestamp=final_sample_ts)
        return self.finished_record(status=status)

    async def _write_finished_record(self, record: dict) -> None:
        async with self._write_lock:
            await asyncio.to_thread(_write_saved_record, self.saved_dir, record)
        await self._log(
            "info",
            "storage",
            f"Saved contract {record['slug']} with {len(record['history'])} history row(s).",
        )

    async def _save_prepared_finished_record(
        self,
        record: dict,
        publish_saved: bool = False,
    ) -> None:
        try:
            await self._write_finished_record(record)
            if publish_saved:
                await self._publish("saved", {"record": record, **self.snapshot()})
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.state.last_error = _error_message(error)
            logger.exception("Failed to save prepared live monitor record")
            await self._log(
                "error",
                "storage",
                f"Failed to save contract {record.get('slug', 'unknown')}: {self.state.last_error}",
            )

    def _close_orderbook_soon(self, orderbook: PolymarketLiveOrderBook) -> None:
        async def ignore_update(_update_type: str, _detail: dict) -> None:
            return None

        orderbook.attach(orderbook.state, ignore_update)
        self._prune_tasks()
        self._tasks.append(asyncio.create_task(orderbook.close()))

    async def save_finished_record(self, status: str) -> dict:
        if self._saved:
            return self.finished_record(status=status)
        record = self._prepare_finished_record(status=status)
        await self._write_finished_record(record)
        self._saved = True
        return record

    def finished_record(self, status: str) -> dict:
        connection_stats = self.polymarket.connection_stats()
        crypto_connection_stats = self.crypto.connection_stats()
        connection_gap_periods = self._current_connection_gap_periods(
            connection_stats,
            crypto_connection_stats,
        )
        self._refresh_history_targets()
        history = self._complete_history_grid(
            self._contract_window_history(list(self._history), self.event),
        )
        history = self._history_without_connection_gaps(history, connection_gap_periods)
        target = self.state.target_price
        final_crypto = (
            history[-1]["crypto_price"]
            if history
            else self.state.crypto_price
        )
        first_history_ts = _optional_float(history[0].get("t")) if history else None
        last_history_ts = _optional_float(history[-1].get("t")) if history else None
        winning_outcome = _winning_outcome(target, final_crypto)
        quality = self._contract_quality(connection_stats, crypto_connection_stats)
        primary = self.tokens_by_id[self.primary_token_id]
        polymarket_disconnect_periods = self._connection_gap_periods(
            "polymarket",
            connection_stats,
        )
        crypto_disconnect_periods = self._connection_gap_periods(
            "crypto",
            crypto_connection_stats,
        )
        return {
            "id": self.current_record_id,
            "session_id": self.id,
            "user_id": self.user_id,
            "status": status,
            "created_at": self.created_at,
            "saved_at": utc_iso(),
            "url": self.event.url,
            "slug": self.event.slug,
            "question": primary.market_question,
            "event_title": self.event.event_title or self.event.title,
            "token_id": self.primary_token_id,
            "target_price": target,
            "target_price_ts": (
                round(self._target_price_ts, 3)
                if self._target_price_ts is not None
                else None
            ),
            "target_price_time": (
                _format_history_time(self._target_price_ts)
                if self._target_price_ts is not None
                else None
            ),
            "final_crypto_price": final_crypto,
            "winning_outcome": winning_outcome,
            "condition_id": primary.condition_id,
            "crypto_symbol": self.event.crypto_symbol,
            "window_start_ts": self.event.window_start_ts,
            "window_end_ts": self.event.window_end_ts,
            "tracking_started_at_ts": int(self.started_at_ts),
            "tracking_started_after_seconds": quality["started_after_seconds"],
            "tracking_ended_before_seconds": quality["ended_before_seconds"],
            "polymarket_disconnect_count": connection_stats["disconnect_count"],
            "polymarket_disconnected_seconds": connection_stats["disconnected_seconds"],
            "crypto_disconnect_count": crypto_connection_stats["disconnect_count"],
            "crypto_disconnected_seconds": crypto_connection_stats["disconnected_seconds"],
            "polymarket_disconnect_periods": polymarket_disconnect_periods,
            "crypto_disconnect_periods": crypto_disconnect_periods,
            "connection_gap_periods": connection_gap_periods,
            "unsuitable_contract": quality["unsuitable_contract"],
            "unsuitable_reason": quality["unsuitable_reason"],
            "partial_contract": quality["partial_contract"],
            "partial_reason": quality["partial_reason"],
            "history_count": len(history),
            "first_history_ts": first_history_ts,
            "last_history_ts": last_history_ts,
            "history_truncated_count": self._history_truncated_count,
            "history_truncated_before_ts": self._history_truncated_before_ts,
            "history_retention_limit": LIVE_HISTORY_MEMORY_LIMIT,
            "history": history,
        }

    def _contract_quality(
        self,
        connection_stats: dict,
        crypto_connection_stats: dict,
    ) -> dict:
        disconnect_count = connection_stats["disconnect_count"]
        crypto_disconnect_count = crypto_connection_stats["disconnect_count"]
        if self.event.window_start_ts is None:
            ended_before = self._tracking_ended_before_seconds()
            partial = disconnect_count > 0 or crypto_disconnect_count > 0
            return {
                "unsuitable_contract": ended_before is not None,
                "unsuitable_reason": _unsuitable_reason(None, ended_before),
                "partial_contract": partial,
                "started_after_seconds": None,
                "ended_before_seconds": ended_before,
                "partial_reason": _partial_reason(
                    disconnect_count,
                    crypto_disconnect_count,
                ),
            }
        started_after = max(0, int(self.started_at_ts - self.event.window_start_ts))
        if started_after <= PARTIAL_CONTRACT_GRACE_SECONDS:
            started_after = 0
        ended_before = self._tracking_ended_before_seconds()
        unsuitable = bool(started_after or ended_before is not None)
        partial = disconnect_count > 0 or crypto_disconnect_count > 0
        return {
            "unsuitable_contract": unsuitable,
            "unsuitable_reason": _unsuitable_reason(
                started_after or None,
                ended_before,
            ),
            "partial_contract": partial,
            "started_after_seconds": started_after or None,
            "ended_before_seconds": ended_before,
            "partial_reason": _partial_reason(
                disconnect_count,
                crypto_disconnect_count,
            ),
        }

    def _connection_gap_periods(self, source: str, stats: dict) -> list[dict]:
        periods = []
        window_start = self.event.window_start_ts
        window_end = self.event.window_end_ts
        for period in stats.get("disconnect_periods") or []:
            raw_start_ts = _optional_float(period.get("start_ts"))
            raw_end_ts = _optional_float(period.get("end_ts"))
            if raw_start_ts is None or raw_end_ts is None:
                continue
            start_ts = raw_start_ts
            end_ts = raw_end_ts
            if window_start is not None:
                start_ts = max(start_ts, float(window_start))
            if window_end is not None:
                end_ts = min(end_ts, float(window_end))
            if end_ts <= start_ts:
                continue
            item = {
                "source": source,
                "start_ts": round(start_ts, 3),
                "end_ts": round(end_ts, 3),
                "start_time": _iso_from_ts(start_ts),
                "end_time": _iso_from_ts(end_ts),
                "duration_seconds": round(max(0, end_ts - start_ts), 3),
                "raw_start_ts": round(raw_start_ts, 3),
                "raw_end_ts": round(raw_end_ts, 3),
                "raw_duration_seconds": round(max(0, raw_end_ts - raw_start_ts), 3),
            }
            reason = str(period.get("reason") or "").strip()
            if reason:
                item["reason"] = reason
            if window_start is not None:
                item["start_second"] = round(max(0, start_ts - window_start), 3)
                item["end_second"] = round(max(0, end_ts - window_start), 3)
            periods.append(item)
        return periods

    def _tracking_ended_before_seconds(self) -> int | None:
        if self.event.window_end_ts is None:
            return None
        ended_before = int(self.event.window_end_ts - time.time())
        if ended_before <= PARTIAL_CONTRACT_GRACE_SECONDS:
            return None
        return ended_before

    def _write_meta(self) -> None:
        _write_json(
            self.meta_path,
            {
                "id": self.id,
                "user_id": self.user_id,
                "created_at": self.created_at,
                "event": self.event_payload(),
                "samples_path": str(self.samples_path),
                "saved_dir": str(self.saved_dir),
            },
        )

    def event_payload(self) -> dict:
        return {
            "url": self.event.url,
            "slug": self.event.slug,
            "title": self.event.title,
            "event_title": self.event.event_title,
            "crypto_symbol": self.event.crypto_symbol,
            "window_size_seconds": self.event.window_size_seconds,
            "window_start_ts": self.event.window_start_ts,
            "window_end_ts": self.event.window_end_ts,
            "markets": self.event.markets,
            "tokens": [
                {
                    "token_id": token.token_id,
                    "market_question": token.market_question,
                    "outcome": token.outcome,
                    "condition_id": token.condition_id,
                }
                for token in self.event.tokens
            ],
        }

    def snapshot(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "server_time": time.time(),
            "tick_count": self._tick_count,
            "last_tick_at": self._last_tick_at,
            "event": self.event_payload(),
            "status": {
                "quotes": self.state.quote_status,
                "crypto": self.state.crypto_status,
                "last_error": self.state.last_error,
            },
            "crypto": {
                "symbol": self.event.crypto_symbol,
                "price": self.state.crypto_price,
                "target_price": self.state.target_price,
                "target_price_ts": self._target_price_ts,
                "updated_at": self.state.crypto_updated_at,
            },
            "quotes": [
                {
                    "token_id": token_id,
                    "market_question": self.tokens_by_id[token_id].market_question,
                    "outcome": self.tokens_by_id[token_id].outcome,
                    "condition_id": self.tokens_by_id[token_id].condition_id,
                    "bid": quote.bid,
                    "ask": quote.ask,
                    "bid_size": quote.bid_size,
                    "ask_size": quote.ask_size,
                    "tick_size": quote.tick_size,
                    "updated_at": quote.updated_at,
                    "event_at": quote.event_at,
                    "event_count": quote.event_count,
                    "price_change_count": quote.price_change_count,
                }
                for token_id, quote in self.state.quotes.items()
            ],
            "history": list(self._history[-LIVE_SNAPSHOT_HISTORY_LIMIT:]),
            "history_truncated_count": self._history_truncated_count,
            "history_truncated_before_ts": self._history_truncated_before_ts,
            "history_retention_limit": LIVE_HISTORY_MEMORY_LIMIT,
            "samples_path": str(self.samples_path),
            "saved_path": str(self.saved_dir),
            "logs": list(self._logs[-100:]),
            "health": {
                "polymarket": self.polymarket.health(),
                "crypto": self.crypto.health(),
                "rest": {
                    "last_poll_at": self._last_rest_poll_at,
                    "poll_count": self._rest_poll_count,
                },
                "storage": {
                    "sample_queue_size": self._sample_queue.qsize(),
                    "sample_write_count": self._sample_write_count,
                    "sample_drop_count": self._sample_drop_count,
                    "last_error": self._last_sample_write_error,
                },
                "stream": {
                    "pending_live_update_count": self._pending_live_update_count,
                    "min_interval_seconds": LIVE_STREAM_MIN_INTERVAL_SECONDS,
                },
            },
        }


class LiveMonitorManager:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self.sessions: dict[str, LiveMonitorSession] = {}
        self.active_by_user: dict[str, list[str]] = {}
        self.data_root.mkdir(parents=True, exist_ok=True)
        (self.data_root / "sessions").mkdir(exist_ok=True)
        (self.data_root / "samples").mkdir(exist_ok=True)

    async def start(
        self,
        user_id: str,
        event_url: str,
        crypto_symbol: str | None = None,
    ) -> LiveMonitorSession:
        if len(self.active_sessions(user_id)) >= MAX_ACTIVE_LIVE_MONITORS_PER_USER:
            raise ValueError(
                f"Maximum {MAX_ACTIVE_LIVE_MONITORS_PER_USER} active live monitors reached"
            )
        event = await resolve_polymarket_event(event_url, crypto_symbol)
        session = LiveMonitorSession(uuid.uuid4().hex, user_id, event, self.data_root)
        self.sessions[session.id] = session
        self.active_by_user.setdefault(user_id, []).append(session.id)
        try:
            await session.start()
        except Exception:
            self.sessions.pop(session.id, None)
            session_ids = self.active_by_user.get(user_id) or []
            kept_ids = [active_id for active_id in session_ids if active_id != session.id]
            if kept_ids:
                self.active_by_user[user_id] = kept_ids
            else:
                self.active_by_user.pop(user_id, None)
            await session.close()
            raise
        return session

    def get(self, session_id: str) -> LiveMonitorSession | None:
        return self.sessions.get(session_id)

    def active_session(self, user_id: str) -> LiveMonitorSession | None:
        sessions = self.active_sessions(user_id)
        return sessions[0] if sessions else None

    def active_sessions(self, user_id: str) -> list[LiveMonitorSession]:
        session_ids = self.active_by_user.get(user_id) or []
        active: list[LiveMonitorSession] = []
        kept_ids: list[str] = []
        for session_id in session_ids:
            session = self.sessions.get(session_id)
            if session is None or session.user_id != user_id or session.is_finished:
                self.sessions.pop(session_id, None)
                continue
            active.append(session)
            kept_ids.append(session_id)
        if kept_ids:
            self.active_by_user[user_id] = kept_ids
        else:
            self.active_by_user.pop(user_id, None)
        return active

    async def stop(self, session_id: str) -> bool:
        session = self.sessions.pop(session_id, None)
        if not session:
            return False
        session_ids = self.active_by_user.get(session.user_id) or []
        kept_ids = [active_id for active_id in session_ids if active_id != session_id]
        if kept_ids:
            self.active_by_user[session.user_id] = kept_ids
        else:
            self.active_by_user.pop(session.user_id, None)
        await session.close()
        return True

    def list_saved(self, user_id: str, category: str | None = None) -> list[dict]:
        records = _read_saved_records(_saved_records_dir(self.data_root, user_id))
        normalized_category = _normalize_saved_category(category)
        if normalized_category:
            records = [
                record
                for record in records
                if _record_saved_category(record)["id"] == normalized_category
            ]
        return [_with_saved_category(record) for record in records]

    def list_saved_page(
        self,
        user_id: str,
        category: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        record_paths = _read_saved_record_preview_paths(
            _saved_records_dir(self.data_root, user_id),
            include_history_stats=False,
        )
        normalized_category = _normalize_saved_category(category)
        if normalized_category:
            record_paths = [
                (path, record)
                for path, record in record_paths
                if _record_saved_category(record)["id"] == normalized_category
            ]
        newest_first = list(reversed(record_paths))
        page = max(1, int(page))
        page_size = min(100, max(1, int(page_size)))
        total = len(newest_first)
        total_pages = max(1, (total + page_size - 1) // page_size)
        current_page = min(page, total_pages)
        start = (current_page - 1) * page_size
        end = start + page_size
        return {
            "records": [
                _saved_list_record(record)
                for _path, record in newest_first[start:end]
            ],
            "page": current_page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_previous": current_page > 1,
            "has_next": end < total,
        }

    def saved_categories(self, user_id: str) -> list[dict]:
        counts: dict[str, dict] = {}
        for record in _read_saved_record_previews(
            _saved_records_dir(self.data_root, user_id),
            include_history_stats=False,
        ):
            category = _record_saved_category(record)
            bucket = counts.setdefault(
                category["id"],
                {**category, "count": 0},
            )
            bucket["count"] += 1
        return sorted(counts.values(), key=lambda item: (item["label"], item["id"]))

    def get_saved(self, user_id: str, record_id: str) -> dict | None:
        path = _find_saved_record_path(self.data_root, user_id, record_id)
        if path is None:
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return _with_saved_category(payload) if isinstance(payload, dict) else None

    def delete_saved(self, user_id: str, record_id: str) -> bool:
        path = _find_saved_record_path(self.data_root, user_id, record_id)
        if path is None:
            return False
        path.unlink()
        return True

    def delete_saved_category(self, user_id: str, category: str) -> int:
        normalized_category = _normalize_saved_category(category)
        if not normalized_category:
            return 0
        deleted = 0
        directory = _saved_records_dir(self.data_root, user_id)
        for path, record in _read_saved_record_preview_paths(
            directory,
            include_history_stats=False,
        ):
            if _record_saved_category(record)["id"] != normalized_category:
                continue
            path.unlink()
            deleted += 1
        return deleted

    async def close(self) -> None:
        sessions = list(self.sessions)
        for session_id in sessions:
            await self.stop(session_id)


async def resolve_polymarket_event(
    event_url: str,
    crypto_symbol: str | None = None,
    normalize_live_slug: bool = True,
) -> ResolvedPolymarketEvent:
    original_slug = polymarket_slug_from_url(event_url)
    slug = _current_live_slug(original_slug) if normalize_live_slug else original_slug
    async with httpx.AsyncClient(
        timeout=15,
        follow_redirects=True,
        trust_env=True,
    ) as client:
        response = await client.get(
            f"{settings.gamma_api_url}/events",
            params={"slug": slug},
        )
        response.raise_for_status()
        payload = response.json()
        if (not isinstance(payload, list) or not payload) and slug != original_slug:
            next_slug = _next_slug(slug)
            if next_slug:
                response = await client.get(
                    f"{settings.gamma_api_url}/events",
                    params={"slug": next_slug},
                )
                response.raise_for_status()
                next_payload = response.json()
                if isinstance(next_payload, list) and next_payload:
                    slug = next_slug
                    payload = next_payload
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"Polymarket event not found: {slug}")
    event = payload[0]
    markets = event.get("markets") or []
    tokens = _extract_live_tokens(markets)
    if not tokens:
        raise ValueError("No CLOB token ids found in this Polymarket event")
    title = str(event.get("title") or event.get("question") or slug)
    window_size = _window_size_from_slug(slug)
    window_start = _window_start_from_slug(slug)
    window_end = (
        window_start + window_size
        if window_start is not None and window_size is not None
        else None
    )
    symbol = normalize_crypto_symbol(
        crypto_symbol or infer_crypto_symbol(f"{slug} {title}")
    )
    return ResolvedPolymarketEvent(
        url=event_url,
        slug=slug,
        title=title,
        markets=[
            {
                "question": str(market.get("question") or title),
                "condition_id": market.get("conditionId"),
                "clob_token_ids": [token.token_id for token in _market_tokens(market)],
            }
            for market in markets
            if _market_tokens(market)
        ],
        tokens=tokens,
        crypto_symbol=symbol,
        window_size_seconds=window_size,
        window_start_ts=window_start,
        window_end_ts=window_end,
        event_title=_format_event_window(window_start, window_size),
    )


def polymarket_slug_from_url(event_url: str) -> str:
    value = event_url.strip()
    if not value:
        raise ValueError("Event URL is required")
    parsed = urlparse(value)
    if parsed.netloc and "polymarket.com" not in parsed.netloc:
        raise ValueError("Only polymarket.com event links are supported")
    if parsed.netloc:
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "event":
            return parts[1]
        raise ValueError("Paste a Polymarket event URL like /event/<slug>")
    return value.strip("/")


def infer_crypto_symbol(text: str) -> str:
    normalized = text.lower()
    aliases = [
        ("bitcoin", "btc/usd"),
        ("btc", "btc/usd"),
        ("ethereum", "eth/usd"),
        ("ether", "eth/usd"),
        ("eth", "eth/usd"),
        ("solana", "sol/usd"),
        ("sol", "sol/usd"),
        ("xrp", "xrp/usd"),
        ("dogecoin", "doge/usd"),
        ("doge", "doge/usd"),
        ("binance coin", "bnb/usd"),
        ("binance", "bnb/usd"),
        ("bnb", "bnb/usd"),
        ("hyperliquid", "hype/usd"),
        ("hype", "hype/usd"),
    ]
    for needle, symbol in aliases:
        if re.search(rf"(^|[^a-z0-9]){re.escape(needle)}([^a-z0-9]|$)", normalized):
            return symbol
    return "btc/usd"


def normalize_crypto_symbol(symbol: str) -> str:
    value = symbol.strip().lower()
    if not value:
        return "btc/usd"
    if "/" not in value:
        value = f"{value}/usd"
    base, quote = value.split("/", 1)
    base = re.sub(r"[^a-z0-9]", "", base)
    quote = re.sub(r"[^a-z0-9]", "", quote) or "usd"
    if not base:
        base = "btc"
    return f"{base}/{quote}"


def _window_size_from_slug(slug: str) -> int | None:
    match = re.search(r"(?:^|-)(\d+)m(?:-|$)", slug)
    if match:
        return int(match.group(1)) * 60
    return None


def _window_start_from_slug(slug: str) -> int | None:
    parts = [part for part in slug.strip("/").split("-") if part.isdigit()]
    if not parts:
        return None
    timestamp = int(parts[-1])
    if timestamp < 1_000_000_000:
        return None
    return timestamp


def _format_event_window(
    start_ts: int | None,
    window_size_seconds: int | None,
) -> str | None:
    if start_ts is None or window_size_seconds is None:
        return None
    start = datetime.fromtimestamp(start_ts)
    end = start + timedelta(seconds=window_size_seconds)
    return f"{start:%H:%M:%S} - {end:%H:%M:%S}"


def _format_history_time(timestamp: float) -> str:
    parsed = datetime.fromtimestamp(timestamp)
    if abs(timestamp - int(timestamp)) < 0.001:
        return parsed.strftime("%H:%M:%S")
    return parsed.strftime("%H:%M:%S.%f")[:-3]


def _price_to_beat(price: float | None, crypto_symbol: str | None = None) -> float | None:
    if price is None:
        return None
    decimals = _target_price_decimals(crypto_symbol)
    quantum = Decimal("1").scaleb(-decimals)
    return float(Decimal(str(price)).quantize(quantum, rounding=ROUND_HALF_UP))


def _target_price_decimals(crypto_symbol: str | None) -> int:
    if not crypto_symbol:
        return 2
    base = normalize_crypto_symbol(crypto_symbol).split("/", 1)[0]
    return TARGET_PRICE_DECIMALS_BY_ASSET.get(base, 2)


def _ceil_history_sample_time(timestamp: float) -> float:
    interval = HISTORY_CAPTURE_INTERVAL_SECONDS
    return round(math.ceil((timestamp - 1e-9) / interval) * interval, 3)


def _floor_history_sample_time(timestamp: float) -> float:
    interval = HISTORY_CAPTURE_INTERVAL_SECONDS
    return round(math.floor((timestamp + 1e-9) / interval) * interval, 3)


def _history_sample_timestamps(start: float, end: float) -> list[float]:
    if end < start:
        return []
    interval = HISTORY_CAPTURE_INTERVAL_SECONDS
    count = int(round((end - start) / interval)) + 1
    return [round(start + index * interval, 3) for index in range(count)]


def _iso_from_ts(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def _winning_outcome(
    target_price: float | None,
    final_crypto_price: float | None,
) -> str | None:
    if target_price is None or final_crypto_price is None:
        return None
    if final_crypto_price > target_price:
        return "Up"
    if final_crypto_price < target_price:
        return "Down"
    return "Flat"


def _extract_live_tokens(markets: list[dict]) -> list[LiveToken]:
    tokens: list[LiveToken] = []
    seen: set[str] = set()
    for market in markets:
        for token in _market_tokens(market):
            if token.token_id in seen:
                continue
            seen.add(token.token_id)
            tokens.append(token)
    return tokens


def _market_tokens(market: dict) -> list[LiveToken]:
    token_ids = [str(value) for value in parse_list(market.get("clobTokenIds"))]
    outcomes = [str(value) for value in parse_list(market.get("outcomes"))]
    question = str(market.get("question") or "Market")
    condition_id = (
        str(market.get("conditionId"))
        if market.get("conditionId") is not None
        else None
    )
    result = []
    for index, token_id in enumerate(token_ids):
        if not token_id:
            continue
        outcome = outcomes[index] if index < len(outcomes) else f"Outcome {index + 1}"
        result.append(LiveToken(token_id, question, outcome, condition_id))
    return result


def _crypto_price_point_from_payload(payload: dict) -> tuple[float, float | None] | None:
    source: Any = payload
    data = payload.get("data")
    if isinstance(data, list) and data:
        source = data[-1]
    elif isinstance(data, dict):
        source = data
    if not isinstance(source, dict):
        source = {"value": source}

    price = _float_from_keys(source, ("value", "price", "last_price"))
    if price is None and source is not payload:
        price = _float_from_keys(payload, ("value", "price", "last_price"))
    if price is None:
        return None

    timestamp = _timestamp_from_mapping(source)
    if timestamp is None and source is not payload:
        timestamp = _timestamp_from_mapping(payload)
    return price, timestamp


def _crypto_price_from_payload(payload: dict) -> float | None:
    point = _crypto_price_point_from_payload(payload)
    return point[0] if point is not None else None


def _float_from_keys(payload: dict, keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _timestamp_from_mapping(payload: dict) -> float | None:
    return _timestamp_from_value(
        payload.get("timestamp")
        or payload.get("ts")
        or payload.get("time")
        or payload.get("updated_at")
        or payload.get("created_at")
        or payload.get("published_at")
    )


def _timestamp_from_value(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        if not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    if parsed > 100_000_000_000_000:
        return parsed / 1_000_000_000
    if parsed > 100_000_000_000:
        return parsed / 1_000
    return parsed


async def _fetch_clob_book(
    client: httpx.AsyncClient,
    token_id: str,
) -> tuple[float | None, float | None, float | None, float | None]:
    response = await client.get(
        f"{POLYMARKET_CLOB_URL}/book",
        params={"token_id": token_id},
    )
    if response.status_code in {400, 404, 422}:
        return None, None, None, None
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return None, None, None, None
    bids = _levels_to_decimals(payload.get("bids") or [])
    asks = _levels_to_decimals(payload.get("asks") or [])
    best_bid = max(bids, default=None)
    best_ask = min(asks, default=None)
    return (
        float(best_bid) if best_bid is not None else None,
        float(best_ask) if best_ask is not None else None,
        float(bids[best_bid]) if best_bid is not None else None,
        float(asks[best_ask]) if best_ask is not None else None,
    )


def _levels_to_decimals(levels: list[dict]) -> dict[Decimal, Decimal]:
    result: dict[Decimal, Decimal] = {}
    for level in levels:
        try:
            price = Decimal(str(level["price"]))
            size = Decimal(str(level["size"]))
        except (KeyError, TypeError, ValueError):
            continue
        if size > 0:
            result[price] = size
    return result


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _normalize_polymarket_quote(
    bid: float | None,
    ask: float | None,
    bid_size: float | None = None,
    ask_size: float | None = None,
) -> tuple[float | None, float | None, float | None, float | None]:
    if bid is not None and ask is not None and bid > ask:
        if (
            bid > POLYMARKET_PRICE_MIDPOINT + POLYMARKET_PRICE_EPSILON
            and ask > POLYMARKET_PRICE_MIDPOINT + POLYMARKET_PRICE_EPSILON
        ):
            return bid, None, bid_size, None
        if (
            bid < POLYMARKET_PRICE_MIDPOINT - POLYMARKET_PRICE_EPSILON
            and ask < POLYMARKET_PRICE_MIDPOINT - POLYMARKET_PRICE_EPSILON
        ):
            return None, ask, None, ask_size
    if bid is not None and (
        POLYMARKET_TOP_BID - POLYMARKET_PRICE_EPSILON
        <= bid
        <= 1 + POLYMARKET_PRICE_EPSILON
    ):
        return bid, None, bid_size, None
    return bid, ask, bid_size, ask_size


def _polymarket_bid(value: Any) -> float | None:
    price = _optional_float(value)
    if price is None or price <= 0:
        return None
    return price


def _polymarket_ask(value: Any) -> float | None:
    price = _optional_float(value)
    if price is None or price >= 1:
        return None
    return price


def _reconnect_delay(attempt: int) -> float:
    base = min(
        0.5 * (2 ** min(max(0, attempt - 1), 10)),
        RECONNECT_MAX_DELAY_SECONDS,
    )
    return base + random.uniform(0, 0.25)


def _next_reconnect_attempt(current_attempt: int, stable_for: float) -> int:
    if stable_for >= RECONNECT_STABLE_RESET_SECONDS:
        return 1
    return current_attempt + 1


def _error_message(error: BaseException) -> str:
    if isinstance(error, ConnectionClosed):
        if error.rcvd is None and error.sent is None:
            return "transport ended without a WebSocket close frame"
        if isinstance(error, ConnectionClosedOK):
            return "server closed the WebSocket normally"
    return str(error) or error.__class__.__name__


def _append_jsonl(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


def _safe_file_id(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "", str(value))
    if not safe:
        raise ValueError("Invalid file id")
    return safe


def _saved_records_dir(data_root: Path, user_id: str) -> Path:
    safe_user_id = re.sub(r"[^A-Za-z0-9_-]", "", user_id)
    if not safe_user_id:
        raise ValueError("Invalid user id")
    return data_root / "users" / safe_user_id / "saved_contracts"


def _saved_record_path(data_root: Path, user_id: str, record_id: str) -> Path:
    return _saved_records_dir(data_root, user_id) / f"{_safe_file_id(record_id)}.json"


def _find_saved_record_path(
    data_root: Path,
    user_id: str,
    record_id: str,
) -> Path | None:
    directory = _saved_records_dir(data_root, user_id)
    safe_record_id = _safe_file_id(record_id)
    legacy_path = directory / f"{safe_record_id}.json"
    if legacy_path.exists():
        return legacy_path
    matches = sorted(directory.glob(f"*__{safe_record_id}.json"))
    if matches:
        return matches[-1]
    for path in directory.glob("*.json"):
        try:
            payload = _read_saved_record_metadata(path)
        except (OSError, ValueError):
            continue
        if isinstance(payload, dict) and str(payload.get("id") or "") == record_id:
            return path
    return None


def _read_saved_records(directory: Path) -> list[dict]:
    if not directory.exists():
        return []
    records = []
    for path in directory.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return sorted(records, key=lambda item: str(item.get("saved_at", "")))


def _read_saved_record_previews(
    directory: Path,
    include_history_stats: bool = True,
) -> list[dict]:
    return [
        record
        for _path, record in _read_saved_record_preview_paths(
            directory,
            include_history_stats=include_history_stats,
        )
    ]


def _read_saved_record_preview_paths(
    directory: Path,
    include_history_stats: bool = True,
) -> list[tuple[Path, dict]]:
    if not directory.exists():
        return []
    if not include_history_stats:
        return _read_saved_record_metadata_preview_paths(directory)
    records: list[tuple[Path, dict]] = []
    for path in directory.glob("*.json"):
        record = _read_saved_record_preview(
            path,
            include_history_stats=include_history_stats,
        )
        if record is not None:
            records.append((path, record))
    return sorted(records, key=lambda item: str(item[1].get("saved_at", "")))


def _read_saved_record_preview(
    path: Path,
    include_history_stats: bool = True,
) -> dict | None:
    metadata = _read_saved_record_metadata(path)
    if not isinstance(metadata, dict):
        return None
    preview = dict(metadata)
    if include_history_stats:
        preview = _saved_record_with_history_stats(path, preview)
    if "id" not in preview:
        preview["id"] = _saved_record_id_from_filename(path)
    return preview


def _read_saved_record_metadata_preview_paths(directory: Path) -> list[tuple[Path, dict]]:
    signature = _saved_record_directory_signature(directory)
    cached = _SAVED_RECORD_PREVIEW_CACHE.get(directory)
    if cached and cached.get("signature") == signature:
        return [
            (path, dict(record))
            for path, record in cached["records"]
        ]

    records: list[tuple[Path, dict]] = []
    for path, _mtime_ns, _size in signature:
        record = _read_saved_record_preview(path, include_history_stats=False)
        if record is not None:
            records.append((path, record))
    records.sort(key=lambda item: str(item[1].get("saved_at", "")))
    _SAVED_RECORD_PREVIEW_CACHE[directory] = {
        "signature": signature,
        "records": [(path, dict(record)) for path, record in records],
    }
    while len(_SAVED_RECORD_PREVIEW_CACHE) > SAVED_RECORD_PREVIEW_CACHE_LIMIT:
        _SAVED_RECORD_PREVIEW_CACHE.pop(next(iter(_SAVED_RECORD_PREVIEW_CACHE)))
    return records


def _saved_record_directory_signature(directory: Path) -> tuple[tuple[Path, int, int], ...]:
    if not directory.exists():
        return ()
    result = []
    for path in directory.glob("*.json"):
        try:
            stat = path.stat()
        except OSError:
            continue
        result.append((path, stat.st_mtime_ns, stat.st_size))
    return tuple(sorted(result, key=lambda item: item[0].name))


def _saved_record_with_history_stats(path: Path, record: dict) -> dict:
    history_stats = _saved_record_history_stats(path, record)
    return {
        **record,
        **{
            key: value
            for key, value in history_stats.items()
            if value is not None or key == "history_count"
        },
    }


def _read_saved_record_metadata(path: Path) -> dict | None:
    if not path.exists():
        return None
    prefix_lines: list[str] = []
    found_history = False
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if '"history"' in line:
                    found_history = True
                    before_history = line.split('"history"', 1)[0].rstrip()
                    if before_history and before_history not in {"{", ","}:
                        prefix_lines.append(before_history)
                    break
                prefix_lines.append(line)
    except OSError:
        return None

    if found_history:
        prefix = "".join(prefix_lines).rstrip()
        if prefix.endswith(","):
            prefix = prefix[:-1].rstrip()
        try:
            payload = json.loads(f"{prefix}\n}}")
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            payload.pop("history", None)
            return payload

    try:
        if path.stat().st_size > SAVED_RECORD_METADATA_FALLBACK_MAX_BYTES:
            return _minimal_saved_record_from_filename(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    payload.pop("history", None)
    return payload


def _saved_record_history_stats(
    path: Path,
    metadata: dict | None = None,
) -> dict[str, float | int | None]:
    fallback_metadata = metadata or {}
    stats = {
        "history_count": _optional_int(fallback_metadata.get("history_count")) or 0,
        "first_history_ts": _optional_float(fallback_metadata.get("first_history_ts")),
        "last_history_ts": _optional_float(fallback_metadata.get("last_history_ts")),
    }
    if stats["history_count"] or stats["first_history_ts"] is not None:
        return stats

    in_history = False
    found_history = False
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not in_history:
                    if '"history"' not in line:
                        continue
                    in_history = True
                    found_history = True
                    if "[" not in line:
                        continue
                if in_history and line.lstrip().startswith("]"):
                    break
                match = re.search(r'"t"\s*:\s*(-?\d+(?:\.\d+)?)', line)
                if not match:
                    continue
                try:
                    timestamp = float(match.group(1))
                except ValueError:
                    continue
                if stats["first_history_ts"] is None:
                    stats["first_history_ts"] = timestamp
                stats["last_history_ts"] = timestamp
                stats["history_count"] += 1
    except OSError:
        return stats

    if found_history and stats["history_count"]:
        return stats

    try:
        if path.stat().st_size > SAVED_RECORD_METADATA_FALLBACK_MAX_BYTES:
            return stats
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return stats
    history = payload.get("history") if isinstance(payload, dict) else None
    if not isinstance(history, list):
        return stats
    timestamps = [
        timestamp
        for timestamp in (
            _optional_float(row.get("t")) if isinstance(row, dict) else None
            for row in history
        )
        if timestamp is not None
    ]
    stats["history_count"] = len(history)
    stats["first_history_ts"] = timestamps[0] if timestamps else None
    stats["last_history_ts"] = timestamps[-1] if timestamps else None
    return stats


def _saved_record_id_from_filename(path: Path) -> str:
    stem = path.stem
    if "__" in stem:
        return stem.rsplit("__", 1)[-1]
    return stem


def _minimal_saved_record_from_filename(path: Path) -> dict:
    return {
        "id": _saved_record_id_from_filename(path),
        "saved_at": "",
        "question": path.stem,
    }


def _write_saved_record(directory: Path, record: dict) -> None:
    _write_json(directory / _saved_record_filename(record), record)


def _saved_record_filename(record: dict) -> str:
    record_id = _safe_file_id(str(record.get("id", "")))
    timestamp = _saved_record_filename_timestamp(record)
    slug = _safe_filename_part(str(record.get("slug") or "contract"), max_length=120)
    return f"{timestamp}__{slug}__{record_id}.json"


def _saved_record_filename_timestamp(record: dict) -> str:
    timestamp = _optional_float(record.get("window_start_ts"))
    if timestamp is None:
        saved_at = str(record.get("saved_at") or "")
        try:
            timestamp = datetime.fromisoformat(
                saved_at.replace("Z", "+00:00")
            ).timestamp()
        except ValueError:
            timestamp = time.time()
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y%m%d-%H%M%SZ")


def _safe_filename_part(value: str, max_length: int = 120) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]+", "-", value.strip().lower())
    safe = safe.strip("-_")
    if not safe:
        safe = "contract"
    return safe[:max_length].strip("-_") or "contract"


def _normalize_saved_category(category: str | None) -> str | None:
    if category is None:
        return None
    value = category.strip().lower()
    if not value or value == "all":
        return None
    return normalize_crypto_symbol(value).split("/", 1)[0]


def _record_saved_category(record: dict) -> dict:
    symbol = str(record.get("crypto_symbol") or "").strip()
    if not symbol:
        symbol = infer_crypto_symbol(
            " ".join(
                str(record.get(key) or "")
                for key in ("question", "slug", "url", "event_title")
            )
        )
    normalized = normalize_crypto_symbol(symbol)
    base = normalized.split("/", 1)[0]
    labels = {
        "btc": "Bitcoin",
        "eth": "Ethereum",
        "sol": "Solana",
        "xrp": "XRP",
        "doge": "Dogecoin",
        "bnb": "BNB",
        "hype": "Hyperliquid",
    }
    return {
        "id": base,
        "symbol": normalized,
        "label": labels.get(base, base.upper()),
    }


def _with_saved_category(record: dict) -> dict:
    return {
        **record,
        **_record_partial_fields(record),
        **_record_connection_fields(record),
        "category": _record_saved_category(record),
        "summary": _record_saved_summary(record),
    }


def _saved_list_record(record: dict) -> dict:
    full_record = {
        **record,
        **_record_partial_fields(record),
        **_record_connection_fields(record),
        "category": _record_saved_category(record),
    }
    history = full_record.pop("history", [])
    if "history_count" not in full_record and isinstance(history, list) and history:
        full_record["history_count"] = len(history) if isinstance(history, list) else 0
    return full_record


def _record_connection_fields(record: dict) -> dict:
    polymarket_periods = _record_gap_periods(record, "polymarket_disconnect_periods")
    crypto_periods = _record_gap_periods(record, "crypto_disconnect_periods")
    all_periods = _record_gap_periods(record, "connection_gap_periods")
    if not all_periods:
        all_periods = polymarket_periods + crypto_periods
    return {
        "polymarket_disconnect_count": _optional_int(
            record.get("polymarket_disconnect_count")
        ) or 0,
        "polymarket_disconnected_seconds": _optional_float(
            record.get("polymarket_disconnected_seconds")
        ) or 0,
        "crypto_disconnect_count": _optional_int(
            record.get("crypto_disconnect_count")
        ) or 0,
        "crypto_disconnected_seconds": _optional_float(
            record.get("crypto_disconnected_seconds")
        ) or 0,
        "polymarket_disconnect_periods": polymarket_periods,
        "crypto_disconnect_periods": crypto_periods,
        "connection_gap_periods": all_periods,
    }


def _record_gap_periods(record: dict, key: str) -> list[dict]:
    periods = record.get(key)
    return [
        period
        for period in periods
        if isinstance(period, dict)
    ] if isinstance(periods, list) else []


def _record_partial_fields(record: dict) -> dict:
    if "partial_contract" in record and "unsuitable_contract" in record:
        return {}
    disconnect_count = _optional_int(record.get("polymarket_disconnect_count")) or 0
    crypto_disconnect_count = _optional_int(record.get("crypto_disconnect_count")) or 0
    window_start = _optional_int(record.get("window_start_ts"))
    window_end = _optional_int(record.get("window_end_ts"))
    first_history_ts = _first_history_ts(record)
    last_history_ts = _last_history_ts(record)
    ended_before = (
        max(0, window_end - last_history_ts)
        if window_end is not None and last_history_ts is not None
        else None
    )
    if (
        ended_before is not None
        and ended_before <= PARTIAL_CONTRACT_GRACE_SECONDS
    ):
        ended_before = None
    if window_start is None or first_history_ts is None:
        unsuitable = ended_before is not None
        partial = disconnect_count > 0 or crypto_disconnect_count > 0
        return {
            "unsuitable_contract": unsuitable,
            "unsuitable_reason": _unsuitable_reason(None, ended_before),
            "partial_contract": partial,
            "tracking_started_after_seconds": None,
            "tracking_ended_before_seconds": ended_before,
            "partial_reason": _partial_reason(
                disconnect_count,
                crypto_disconnect_count,
            ),
        }
    started_after = max(0, first_history_ts - window_start)
    if started_after <= PARTIAL_CONTRACT_GRACE_SECONDS:
        started_after = 0
    unsuitable = bool(started_after or ended_before is not None)
    partial = disconnect_count > 0 or crypto_disconnect_count > 0
    return {
        "unsuitable_contract": unsuitable,
        "unsuitable_reason": _unsuitable_reason(
            started_after or None,
            ended_before,
        ),
        "partial_contract": partial,
        "tracking_started_after_seconds": started_after or None,
        "tracking_ended_before_seconds": ended_before,
        "partial_reason": _partial_reason(
            disconnect_count,
            crypto_disconnect_count,
        ),
    }


def _unsuitable_reason(
    started_after_seconds: int | None,
    ended_before_seconds: int | None,
) -> str | None:
    reasons = []
    if started_after_seconds:
        reasons.append(
            "Tracking started after the contract window opened; target price is the first observed monitor price."
        )
    if ended_before_seconds is not None:
        reasons.append("Tracking stopped before the contract window ended.")
    return " ".join(reasons) or None


def _partial_reason(
    disconnect_count: int = 0,
    crypto_disconnect_count: int = 0,
) -> str | None:
    reasons = []
    if disconnect_count > 0:
        reasons.append("Polymarket connection was interrupted during tracking.")
    if crypto_disconnect_count > 0:
        reasons.append("Crypto price feed was interrupted during tracking.")
    return " ".join(reasons) or None


def _record_saved_summary(record: dict) -> dict:
    history = [
        row
        for row in record.get("history") or []
        if isinstance(row, dict) and _optional_float(row.get("t")) is not None
    ]
    first_ts = _optional_float(history[0].get("t")) if history else None
    last_ts = _optional_float(history[-1].get("t")) if history else None
    window_start = _optional_int(record.get("window_start_ts"))
    window_end = _optional_int(record.get("window_end_ts"))
    duration = (
        max(0, last_ts - first_ts)
        if first_ts is not None and last_ts is not None
        else None
    )
    window_duration = (
        max(0, window_end - window_start)
        if window_start is not None and window_end is not None
        else None
    )
    coverage_pct = (
        round(duration / window_duration * 100, 2)
        if duration is not None and window_duration
        else None
    )
    spreads = _history_numbers(history, "spread")
    crypto_prices = _history_numbers(history, "crypto_price")
    yes_bids = _history_numbers(history, "yes_bid")
    yes_asks = _history_numbers(history, "yes_ask")
    deviations = _history_numbers(history, "deviation_pct")
    return {
        "first_history_ts": first_ts,
        "last_history_ts": last_ts,
        "duration_seconds": duration,
        "window_duration_seconds": window_duration,
        "coverage_pct": coverage_pct,
        "average_spread": _average(spreads),
        "min_spread": min(spreads) if spreads else None,
        "max_spread": max(spreads) if spreads else None,
        "min_crypto_price": min(crypto_prices) if crypto_prices else None,
        "max_crypto_price": max(crypto_prices) if crypto_prices else None,
        "min_yes_bid": min(yes_bids) if yes_bids else None,
        "max_yes_bid": max(yes_bids) if yes_bids else None,
        "min_yes_ask": min(yes_asks) if yes_asks else None,
        "max_yes_ask": max(yes_asks) if yes_asks else None,
        "start_deviation_pct": deviations[0] if deviations else None,
        "final_deviation_pct": deviations[-1] if deviations else None,
    }


def _history_numbers(history: list[dict], key: str) -> list[float]:
    values = []
    for row in history:
        value = row.get(key)
        if isinstance(value, int | float):
            values.append(float(value))
    return values


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _first_history_ts(record: dict) -> float | None:
    value = _optional_float(record.get("first_history_ts"))
    if value is not None:
        return value
    for row in record.get("history") or []:
        if isinstance(row, dict):
            value = _optional_float(row.get("t"))
            if value is not None:
                return value
    return None


def _last_history_ts(record: dict) -> float | None:
    value = _optional_float(record.get("last_history_ts"))
    if value is not None:
        return value
    for row in reversed(record.get("history") or []):
        if isinstance(row, dict):
            value = _optional_float(row.get("t"))
            if value is not None:
                return value
    return None


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _replace_slug_timestamp(slug: str, timestamp: int) -> str | None:
    parts = slug.strip("/").split("-")
    for index in range(len(parts) - 1, -1, -1):
        if parts[index].isdigit() and int(parts[index]) >= 1_000_000_000:
            parts[index] = str(timestamp)
            return "-".join(parts)
    return None


def _current_live_slug(slug: str) -> str:
    window_size = _window_size_from_slug(slug)
    window_start = _window_start_from_slug(slug)
    if window_size is None or window_start is None:
        return slug
    now = int(time.time())
    if window_start <= now < window_start + window_size:
        return slug
    current_start = (now // window_size) * window_size
    return _replace_slug_timestamp(slug, current_start) or slug


def _next_slug(slug: str) -> str | None:
    window_size = _window_size_from_slug(slug)
    window_start = _window_start_from_slug(slug)
    if window_size is None or window_start is None:
        return None
    return _replace_slug_timestamp(slug, window_start + window_size)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
