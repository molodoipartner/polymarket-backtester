from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest

import backend.live_monitor as live_monitor
from backend.live_monitor import (
    LiveMonitorManager,
    LiveMonitorSession,
    LiveMonitorState,
    LiveQuote,
    PreloadedPolymarketOrderBook,
    PolymarketCryptoPriceFeed,
    PolymarketLiveOrderBook,
    ResolvedPolymarketEvent,
    LiveToken,
    _fetch_clob_book,
    _market_tokens,
    _price_to_beat,
    _record_partial_fields,
    _replace_slug_timestamp,
    _window_size_from_slug,
    _window_start_from_slug,
    infer_crypto_symbol,
    normalize_crypto_symbol,
    polymarket_slug_from_url,
)


def test_polymarket_slug_from_event_url():
    assert (
        polymarket_slug_from_url(
            "https://polymarket.com/event/bitcoin-up-or-down-june-28-12pm-et"
        )
        == "bitcoin-up-or-down-june-28-12pm-et"
    )
    assert polymarket_slug_from_url("ethereum-up-or-down") == "ethereum-up-or-down"


def test_crypto_symbol_inference_and_normalization():
    assert infer_crypto_symbol("bitcoin up or down") == "btc/usd"
    assert infer_crypto_symbol("Will Ethereum go up?") == "eth/usd"
    assert infer_crypto_symbol("bnb updown 5m") == "bnb/usd"
    assert infer_crypto_symbol("Will Binance Coin go up?") == "bnb/usd"
    assert infer_crypto_symbol("hype updown 5m") == "hype/usd"
    assert infer_crypto_symbol("Will Hyperliquid go up?") == "hype/usd"
    assert normalize_crypto_symbol("ETH") == "eth/usd"
    assert normalize_crypto_symbol(" sol/usd ") == "sol/usd"
    assert normalize_crypto_symbol("BNB") == "bnb/usd"
    assert normalize_crypto_symbol(" hype ") == "hype/usd"


def test_price_to_beat_uses_asset_specific_precision():
    assert _price_to_beat(63250.125, "btc/usd") == 63250.13
    assert _price_to_beat(101.005, "eth/usd") == 101.01
    assert _price_to_beat(1.04284, "xrp/usd") == 1.04284
    assert _price_to_beat(0.0725164, "doge/usd") == 0.072516
    assert _price_to_beat(546.5469, "bnb/usd") == 546.5469
    assert _price_to_beat(65.6423, "hype/usd") == 65.6423


def test_window_metadata_from_updown_slug():
    slug = "btc-updown-5m-1782639600"
    assert _window_size_from_slug(slug) == 300
    assert _window_start_from_slug(slug) == 1782639600
    assert _replace_slug_timestamp(slug, 1782639900) == "btc-updown-5m-1782639900"


def test_market_tokens_decode_string_lists():
    tokens = _market_tokens(
        {
            "question": "Bitcoin up?",
            "conditionId": "condition-1",
            "clobTokenIds": '["yes-token","no-token"]',
            "outcomes": '["Up","Down"]',
        }
    )
    assert [token.token_id for token in tokens] == ["yes-token", "no-token"]
    assert [token.outcome for token in tokens] == ["Up", "Down"]
    assert tokens[0].market_question == "Bitcoin up?"


@pytest.mark.asyncio
async def test_polymarket_orderbook_updates_quote_from_book_and_price_change():
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    state = LiveMonitorState(quotes={"token-1": LiveQuote()})
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)
    await client._apply_message(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "bids": [{"price": "0.41", "size": "7"}],
            "asks": [{"price": "0.44", "size": "9"}],
        }
    )
    assert state.quotes["token-1"].bid == 0.41
    assert state.quotes["token-1"].ask == 0.44
    assert state.quotes["token-1"].bid_size == 7
    assert state.quotes["token-1"].ask_size == 9

    await client._apply_message(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "token-1",
                    "side": "buy",
                    "price": "0.42",
                    "size": "3",
                    "best_bid": "0.42",
                    "best_ask": "0.44",
                }
            ],
        }
    )
    assert state.quotes["token-1"].bid == 0.42
    assert state.quotes["token-1"].ask == 0.44
    assert state.quotes["token-1"].price_change_count == 2
    assert updates[-1] == ("quote", {"token_id": "token-1"})


@pytest.mark.asyncio
async def test_price_change_can_reuse_previous_side_when_best_price_missing():
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState(quotes={"token-1": LiveQuote()})
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)
    await client._apply_message(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "bids": [{"price": "0.41", "size": "7"}],
            "asks": [{"price": "0.44", "size": "9"}],
        }
    )
    await client._apply_message(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "token-1",
                    "side": "buy",
                    "price": "0.42",
                    "size": "3",
                    "best_bid": "0.42",
                }
            ],
        }
    )
    assert state.quotes["token-1"].bid == 0.42
    assert state.quotes["token-1"].ask == 0.44


@pytest.mark.asyncio
async def test_price_change_prefers_exchange_best_bid_ask_over_local_book():
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState(quotes={"token-1": LiveQuote()})
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)
    await client._apply_message(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "bids": [{"price": "0.41", "size": "7"}],
            "asks": [{"price": "0.44", "size": "9"}],
        }
    )
    await client._apply_message(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "token-1",
                    "side": "buy",
                    "price": "0.40",
                    "size": "3",
                    "best_bid": "0.39",
                    "best_ask": "0.43",
                }
            ],
        }
    )

    assert state.quotes["token-1"].bid == 0.39
    assert state.quotes["token-1"].ask == 0.43


@pytest.mark.asyncio
async def test_top_bid_polymarket_quote_clears_crossed_ask_without_disconnect():
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    state = LiveMonitorState(quotes={"token-1": LiveQuote()})
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)
    await client._apply_message(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "bids": [{"price": "0.41", "size": "7"}],
            "asks": [{"price": "0.44", "size": "9"}],
        }
    )
    updates.clear()

    await client._apply_message(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "token-1",
                    "side": "buy",
                    "price": "0.99",
                    "size": "3",
                    "best_bid": "0.99",
                    "best_ask": "0.98",
                }
            ],
        }
    )

    assert state.quotes["token-1"].bid == 0.99
    assert state.quotes["token-1"].ask is None
    assert state.quotes["token-1"].ask_size is None
    assert state.quotes["token-1"].event_count == 2
    assert client.disconnect_count == 0
    assert updates == [("quote", {"token_id": "token-1"})]


@pytest.mark.asyncio
async def test_high_half_crossed_polymarket_quote_clears_ask_without_disconnect():
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    state = LiveMonitorState(quotes={"token-1": LiveQuote()})
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)

    await client._store_quote(
        "token-1",
        0.70,
        0.69,
        123.0,
        bid_size=3.0,
        ask_size=4.0,
    )

    assert state.quotes["token-1"].bid == 0.70
    assert state.quotes["token-1"].ask is None
    assert state.quotes["token-1"].bid_size == 3.0
    assert state.quotes["token-1"].ask_size is None
    assert state.quotes["token-1"].updated_at == 123.0
    assert client.disconnect_count == 0
    assert updates == [("quote", {"token_id": "token-1"})]


@pytest.mark.asyncio
async def test_low_half_crossed_polymarket_quote_clears_bid_without_disconnect():
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    state = LiveMonitorState(quotes={"no-token": LiveQuote()})
    client = PolymarketLiveOrderBook(["no-token"], state, on_update)

    await client._store_quote(
        "no-token",
        0.31,
        0.30,
        123.0,
        bid_size=3.0,
        ask_size=4.0,
    )

    assert state.quotes["no-token"].bid is None
    assert state.quotes["no-token"].ask == 0.30
    assert state.quotes["no-token"].bid_size is None
    assert state.quotes["no-token"].ask_size == 4.0
    assert state.quotes["no-token"].updated_at == 123.0
    assert client.disconnect_count == 0
    assert updates == [("quote", {"token_id": "no-token"})]


@pytest.mark.asyncio
async def test_midpoint_crossed_polymarket_quote_is_recorded():
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    state = LiveMonitorState(quotes={"token-1": LiveQuote()})
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)

    await client._store_quote("token-1", 0.60, 0.40, 123.0)

    assert state.quotes["token-1"].bid == 0.60
    assert state.quotes["token-1"].ask == 0.40
    assert state.quotes["token-1"].updated_at == 123.0
    assert updates == [
        (
            "log",
            {
                "level": "warning",
                "source": "polymarket",
                "message": (
                    "Recorded crossed Polymarket order book for "
                    "token-1: bid=0.6, ask=0.4."
                ),
            },
        ),
        ("quote", {"token_id": "token-1"}),
    ]


@pytest.mark.asyncio
async def test_rest_poll_clears_crossed_ask_for_top_bid(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)

    class AsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    async def fetch_book_snapshot(client, token_id):
        assert token_id == "token-1"
        return (
            {Decimal("0.99"): Decimal("3.0")},
            {Decimal("0.98"): Decimal("2.0")},
        )

    monkeypatch.setattr(live_monitor.httpx, "AsyncClient", AsyncClient)
    monkeypatch.setattr(live_monitor, "_fetch_clob_book_snapshot", fetch_book_snapshot)

    await session._poll_rest_quotes()

    assert session.state.quotes["token-1"].bid == 0.99
    assert session.state.quotes["token-1"].ask is None
    assert session.state.quotes["token-1"].ask_size is None
    assert session._logs == []


@pytest.mark.asyncio
async def test_rest_poll_refreshes_local_book_for_next_ws_delta(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)

    await session.polymarket._apply_message(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "bids": [{"price": "0.40", "size": "4"}],
            "asks": [{"price": "0.45", "size": "5"}],
        }
    )

    class AsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    async def fetch_book_snapshot(client, token_id):
        assert token_id == "token-1"
        return (
            {
                Decimal("0.50"): Decimal("3.0"),
                Decimal("0.49"): Decimal("8.0"),
            },
            {Decimal("0.55"): Decimal("2.0")},
        )

    monkeypatch.setattr(live_monitor.httpx, "AsyncClient", AsyncClient)
    monkeypatch.setattr(live_monitor, "_fetch_clob_book_snapshot", fetch_book_snapshot)

    await session._poll_rest_quotes()
    assert session.state.quotes["token-1"].bid == 0.50
    assert session.state.quotes["token-1"].ask == 0.55

    await session.polymarket._apply_message(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "token-1",
                    "side": "buy",
                    "price": "0.50",
                    "size": "0",
                }
            ],
        }
    )

    assert session.state.quotes["token-1"].bid == 0.49
    assert session.state.quotes["token-1"].bid_size == 8.0
    assert session.state.quotes["token-1"].ask == 0.55


@pytest.mark.asyncio
async def test_rest_poll_timeout_sets_cooldown(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1000.0)

    class AsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    async def fetch_book_snapshot(_client, _token_id):
        raise live_monitor.httpx.ConnectTimeout("connect timed out")

    monkeypatch.setattr(live_monitor.httpx, "AsyncClient", AsyncClient)
    monkeypatch.setattr(live_monitor, "_fetch_clob_book_snapshot", fetch_book_snapshot)

    await session._poll_rest_quotes()

    assert session._rest_backoff_until == 115.0
    assert session.state.last_error == "connect timed out"
    assert session._logs[-1]["message"] == "REST quote fallback failed: connect timed out"


@pytest.mark.asyncio
async def test_finished_record_is_saved_per_user(tmp_path):
    manager = LiveMonitorManager(tmp_path)
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
        event_title="10:00:00 - 10:05:00",
    )
    session = manager.sessions["session-1"] = LiveMonitorSession(
        "session-1",
        "user-1",
        event,
        tmp_path,
    )
    session.state.target_price = 100.0
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    await session.save_finished_record(status="ended")

    saved = manager.list_saved("user-1")
    assert len(saved) == 1
    assert saved[0]["question"] == "Bitcoin Up or Down"
    assert saved[0]["target_price"] == 100.0
    assert saved[0]["winning_outcome"] == "Up"
    assert saved[0]["history"][0]["yes_bid"] == 40.0
    saved_files = list((tmp_path / "users" / "user-1" / "saved_contracts").glob("*.json"))
    assert len(saved_files) == 1
    assert "btc-updown-5m-1782639600" in saved_files[0].name
    assert saved_files[0].name.startswith("20260628-094000Z__")
    assert manager.get_saved("user-1", saved[0]["id"])["slug"] == "btc-updown-5m-1782639600"
    assert manager.delete_saved("user-1", saved[0]["id"]) is True
    assert not saved_files[0].exists()


def test_stopped_before_window_end_marks_record_unsuitable(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
        event_title="10:00:00 - 10:05:00",
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 1782639602
    session.state.target_price = 100.0
    session._target_price_ts = 1782639600
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639700)

    record = session.finished_record(status="stopped")

    assert record["unsuitable_contract"] is True
    assert record["partial_contract"] is False
    assert record["tracking_started_after_seconds"] is None
    assert record["tracking_ended_before_seconds"] == 200
    assert record["unsuitable_reason"] == "Tracking stopped before the contract window ended."


def test_started_more_than_five_seconds_after_window_marks_record_unsuitable(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
        event_title="10:00:00 - 10:05:00",
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 1782639605.6
    session.state.target_price = 100.0
    session._target_price_ts = 1782639600
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session._history.append({
        "t": 1782639605.6,
        "crypto_price": 101.0,
        "target_price": 100.0,
        "yes_bid": 40.0,
        "yes_ask": 45.0,
    })
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639901)

    record = session.finished_record(status="ended")

    assert record["unsuitable_contract"] is True
    assert record["tracking_started_after_seconds"] == 5.6
    assert "Tracking started after" in record["unsuitable_reason"]


def test_manager_tracks_active_sessions_per_user(tmp_path):
    manager = LiveMonitorManager(tmp_path)
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
    )
    session_1 = manager.sessions["session-1"] = LiveMonitorSession(
        "session-1",
        "user-1",
        event,
        tmp_path,
    )
    session_2 = manager.sessions["session-2"] = LiveMonitorSession(
        "session-2",
        "user-1",
        event,
        tmp_path,
    )
    manager.active_by_user["user-1"] = ["session-1", "session-2"]

    assert manager.active_sessions("user-1") == [session_1, session_2]
    assert manager.active_session("user-1") is session_1

    session_1._stopped = True
    assert manager.active_sessions("user-1") == [session_2]
    assert manager.active_by_user["user-1"] == ["session-2"]
    assert "session-1" not in manager.sessions


@pytest.mark.asyncio
async def test_manager_stop_clears_active_session(tmp_path, monkeypatch):
    manager = LiveMonitorManager(tmp_path)
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
    )
    session = manager.sessions["session-1"] = LiveMonitorSession(
        "session-1",
        "user-1",
        event,
        tmp_path,
    )
    second_session = manager.sessions["session-2"] = LiveMonitorSession(
        "session-2",
        "user-1",
        event,
        tmp_path,
    )
    manager.active_by_user["user-1"] = ["session-1", "session-2"]

    async def close_without_io():
        session._stopped = True

    monkeypatch.setattr(session, "close", close_without_io)

    assert await manager.stop("session-1") is True
    assert manager.active_session("user-1") is second_session
    assert "session-1" not in manager.sessions
    assert manager.active_by_user["user-1"] == ["session-2"]


@pytest.mark.asyncio
async def test_closing_previous_orderbook_does_not_finish_live_session(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    manager = LiveMonitorManager(tmp_path)
    session = manager.sessions["session-1"] = LiveMonitorSession(
        "session-1",
        "user-1",
        event,
        tmp_path,
    )
    manager.active_by_user["user-1"] = ["session-1"]
    session.state.quote_status = "ready"

    session._close_orderbook_soon(session.polymarket)
    await asyncio.gather(*session._tasks, return_exceptions=True)
    session._prune_tasks()

    assert session.state.quote_status == "ready"
    assert manager.active_sessions("user-1") == [session]


@pytest.mark.asyncio
async def test_manager_rejects_more_than_five_active_sessions(tmp_path):
    manager = LiveMonitorManager(tmp_path)
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
    )
    session_ids = []
    for index in range(live_monitor.MAX_ACTIVE_LIVE_MONITORS_PER_USER):
        session_id = f"session-{index}"
        session_ids.append(session_id)
        manager.sessions[session_id] = LiveMonitorSession(
            session_id,
            "user-1",
            event,
            tmp_path,
        )
    manager.active_by_user["user-1"] = session_ids

    with pytest.raises(ValueError, match="Maximum 5 active live monitors"):
        await manager.start("user-1", event.url)


def test_polymarket_disconnect_metrics_mark_record_partial(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
        event_title="10:00:00 - 10:05:00",
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 1782639600
    session.state.target_price = 100.0
    session._target_price_ts = 1782639600
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session.polymarket.disconnect_count = 2
    session.polymarket.disconnected_seconds = 35.25
    session.polymarket.disconnect_periods = [{
        "start_ts": 1782639660.0,
        "end_ts": 1782639695.25,
        "duration_seconds": 35.25,
        "reason": "transport ended without a WebSocket close frame",
    }]
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639901)

    record = session.finished_record(status="ended")

    assert record["partial_contract"] is True
    assert record["unsuitable_contract"] is False
    assert record["polymarket_disconnect_count"] == 2
    assert record["polymarket_disconnected_seconds"] == 35.25
    assert record["connection_gap_periods"][0]["source"] == "polymarket"
    assert record["connection_gap_periods"][0]["start_second"] == 60.0
    assert record["connection_gap_periods"][0]["end_second"] == 95.25
    assert (
        record["connection_gap_periods"][0]["reason"]
        == "transport ended without a WebSocket close frame"
    )
    assert (
        record["partial_reason"]
        == "Polymarket connection was interrupted during tracking."
    )


def test_history_is_not_recorded_inside_active_connection_gap(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.state.target_price = 100.0
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session.polymarket.disconnect_count = 1
    session.polymarket.disconnect_periods = [{
        "start_ts": 1782639874.6,
        "end_ts": 1782639876.6,
        "duration_seconds": 2.0,
    }]

    session._capture_history_row(timestamp=1782639875.0)
    session._capture_history_row(timestamp=1782639877.0)

    assert [row["t"] for row in session._history] == [1782639877.0]


def test_finished_record_filters_history_inside_connection_gap(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.state.target_price = 100.0
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session._history = [
        {"t": 1782639874.5, "crypto_price": 101.0},
        {"t": 1782639875.0, "crypto_price": 101.0},
        {"t": 1782639876.5, "crypto_price": 101.0},
        {"t": 1782639877.0, "crypto_price": 101.0},
    ]
    session.polymarket.disconnect_count = 1
    session.polymarket.disconnect_periods = [{
        "start_ts": 1782639874.6,
        "end_ts": 1782639876.6,
        "duration_seconds": 2.0,
    }]

    record = session.finished_record(status="ended")

    assert record["connection_gap_periods"][0]["start_second"] == 274.6
    assert record["connection_gap_periods"][0]["end_second"] == 276.6
    assert [row["t"] for row in record["history"]] == [
        1782639874.5,
        1782639877.0,
    ]


def test_live_snapshot_limits_history_payload(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=None,
        window_end_ts=None,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session._history = [
        {"t": index, "crypto_price": 100 + index}
        for index in range(live_monitor.LIVE_SNAPSHOT_HISTORY_LIMIT + 20)
    ]

    snapshot = session.snapshot()

    assert len(snapshot["history"]) == live_monitor.LIVE_SNAPSHOT_HISTORY_LIMIT
    assert snapshot["history"][0]["t"] == 20
    assert session.finished_record(status="ended")["history"][0]["t"] == 0


def test_live_snapshot_can_use_smaller_stream_history_limit(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=None,
        window_end_ts=None,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session._history = [
        {"t": index, "crypto_price": 100 + index}
        for index in range(10)
    ]

    snapshot = session.snapshot(history_limit=3)

    assert [row["t"] for row in snapshot["history"]] == [7, 8, 9]


def test_live_stream_queue_keeps_latest_message():
    queue = asyncio.Queue(maxsize=2)

    LiveMonitorSession._put_latest(queue, {"event": "tick", "data": {"index": 1}})
    LiveMonitorSession._put_latest(queue, {"event": "tick", "data": {"index": 2}})
    LiveMonitorSession._put_latest(queue, {"event": "tick", "data": {"index": 3}})

    messages = [queue.get_nowait()]

    assert [message["data"]["index"] for message in messages] == [3]


def test_live_stream_queue_preserves_important_events():
    queue = asyncio.Queue(maxsize=3)

    LiveMonitorSession._put_latest(queue, {"event": "saved", "data": {"id": "record-1"}})
    LiveMonitorSession._put_latest(queue, {"event": "tick", "data": {"index": 1}})
    LiveMonitorSession._put_latest(queue, {"event": "quote", "data": {"index": 2}})

    messages = [queue.get_nowait(), queue.get_nowait()]

    assert [message["event"] for message in messages] == ["saved", "quote"]
    assert messages[1]["data"]["index"] == 2


@pytest.mark.asyncio
async def test_quote_updates_respect_live_stream_throttle(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=None,
        window_end_ts=None,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session._history = [{"t": 999.5, "crypto_price": 100.0}]
    queue = session.add_subscriber()
    queue.get_nowait()
    clock = {"now": 1000.0}
    monkeypatch.setattr(live_monitor.time, "time", lambda: clock["now"])
    monkeypatch.setattr(live_monitor, "LIVE_STREAM_MIN_INTERVAL_SECONDS", 10.0)
    session.state.quotes["token-1"] = LiveQuote(bid=0.41, ask=0.44)

    await session._handle_update("quote", {"token_id": "token-1"})
    first = queue.get_nowait()
    clock["now"] = 1000.1
    session.state.quotes["token-1"] = LiveQuote(bid=0.42, ask=0.43)
    await session._handle_update("quote", {"token_id": "token-1"})

    assert first["event"] == "quote"
    assert first["data"]["history"] == [{"t": 999.5, "crypto_price": 100.0}]
    assert queue.empty()
    assert session._pending_live_update_count == 1

    clock["now"] = 1010.0
    await session._handle_update("quote", {"token_id": "token-1"})
    second = queue.get_nowait()

    assert second["event"] == "quote"
    assert second["data"]["update"]["coalesced_count"] == 2
    assert second["data"]["quotes"][0]["bid"] == 0.42
    assert second["data"]["quotes"][0]["ask"] == 0.43


@pytest.mark.asyncio
async def test_saved_stream_event_omits_full_history(tmp_path, monkeypatch):
    monkeypatch.setattr(live_monitor, "LIVE_STREAM_HISTORY_LIMIT", 2)
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=None,
        window_end_ts=None,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session._history = [
        {"t": index, "crypto_price": 100 + index}
        for index in range(5)
    ]
    queue = session.add_subscriber()
    queue.get_nowait()

    await session._publish_saved_record({
        "id": "record-1",
        "slug": "btc-updown-5m-1782639600",
        "history": [{"t": index} for index in range(100)],
    })

    message = queue.get_nowait()

    assert message["event"] == "saved"
    assert "history" not in message["data"]["record"]
    assert [row["t"] for row in message["data"]["history"]] == [3, 4]


def test_crypto_disconnect_metrics_mark_record_partial(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
        event_title="10:00:00 - 10:05:00",
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 1782639600
    session.state.target_price = 100.0
    session._target_price_ts = 1782639600
    session.state.crypto_price = 101.0
    session.state.crypto_updated_at = 1782639899
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session.crypto.disconnect_count = 1
    session.crypto.disconnected_seconds = 35.5
    session.crypto.disconnect_periods = [{
        "start_ts": 1782639700.0,
        "end_ts": 1782639735.5,
        "duration_seconds": 35.5,
        "reason": "Crypto price feed stale for 45.0s",
    }]
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639901)

    record = session.finished_record(status="ended")

    assert record["partial_contract"] is True
    assert record["unsuitable_contract"] is False
    assert record["crypto_disconnect_count"] == 1
    assert record["crypto_disconnected_seconds"] == 35.5
    assert record["connection_gap_periods"][0]["source"] == "crypto"
    assert record["connection_gap_periods"][0]["start_second"] == 100.0
    assert record["connection_gap_periods"][0]["end_second"] == 135.5
    assert record["connection_gap_periods"][0]["reason"] == "Crypto price feed stale for 45.0s"
    assert "Crypto price feed was interrupted" in record["partial_reason"]


def test_crypto_connection_stats_preserve_disconnect_reason(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._ever_connected = True

    feed._mark_disconnected(
        since=10.0,
        wall_since=1000.0,
        reason="Crypto price feed stale for 45.0s",
    )
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: 45.5)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1035.5)
    feed._mark_connected()

    assert feed.disconnect_count == 1
    assert feed.disconnect_periods == [{
        "start_ts": 1000.0,
        "end_ts": 1035.5,
        "duration_seconds": 35.5,
        "reason": "Crypto price feed stale for 45.0s",
    }]


def test_short_crypto_stream_reconnect_is_ignored(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._ever_connected = True
    clock = {"monotonic": 100.0, "time": 1000.0}
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(live_monitor.time, "time", lambda: clock["time"])

    feed._mark_stream_disconnected("Crypto price WebSocket stream ended")
    clock["monotonic"] = 102.2
    clock["time"] = 1002.2
    assert feed.connection_stats() == {
        "disconnect_count": 0,
        "disconnected_seconds": 0.0,
        "disconnect_periods": [],
    }

    feed._mark_connected()

    assert feed.disconnect_count == 0
    assert feed.disconnected_seconds == 0.0
    assert feed.disconnect_periods == []


def test_long_crypto_stream_reconnect_is_recorded(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._ever_connected = True
    clock = {"monotonic": 100.0, "time": 1000.0}
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(live_monitor.time, "time", lambda: clock["time"])

    feed._mark_stream_disconnected("Crypto price WebSocket stream ended")
    clock["monotonic"] = 131.0
    clock["time"] = 1031.0
    feed._mark_connected()

    assert feed.disconnect_count == 1
    assert feed.disconnected_seconds == 31.0
    assert feed.disconnect_periods == [{
        "start_ts": 1000.0,
        "end_ts": 1031.0,
        "duration_seconds": 31.0,
        "reason": "Crypto price WebSocket stream ended",
    }]


def test_crypto_ping_timeout_reconnect_is_recorded(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._ever_connected = True
    clock = {"monotonic": 100.0, "time": 1000.0}
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(live_monitor.time, "time", lambda: clock["time"])

    feed._mark_stream_disconnected("Crypto WebSocket ping timeout after 5.0s")
    clock["monotonic"] = 102.0
    clock["time"] = 1002.0
    feed._mark_connected()

    assert feed.disconnect_count == 1
    assert feed.disconnected_seconds == 2.0
    assert feed.disconnect_periods == [{
        "start_ts": 1000.0,
        "end_ts": 1002.0,
        "duration_seconds": 2.0,
        "reason": "Crypto WebSocket ping timeout after 5.0s",
    }]


def test_crypto_stream_end_does_not_backdate_to_last_price(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState(crypto_updated_at=1000.0)
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._ever_connected = True
    feed._connected_at = 90.0
    feed._last_price_at = 100.0
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: 102.0)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1002.0)

    feed._mark_stream_disconnected("Crypto price WebSocket stream ended")

    assert feed._disconnected_since == 102.0
    assert feed._disconnected_wall_since == 1002.0


def test_crypto_stale_disconnect_keeps_last_price_start(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState(crypto_updated_at=1000.0)
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._ever_connected = True
    feed._connected_at = 90.0
    feed._last_price_at = 100.0
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: 162.0)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1062.0)

    feed._mark_stream_disconnected("Crypto price feed stale for 62.0s")

    assert feed._disconnected_since == 100.0
    assert feed._disconnected_wall_since == 1000.0


@pytest.mark.asyncio
async def test_crypto_stale_watchdog_reconnects_silent_feed(monkeypatch):
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    class WebSocket:
        def __init__(self):
            self.closed = False
            self.sent = []

        async def send(self, payload):
            self.sent.append(payload)

        async def close(self):
            self.closed = True

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._connected_at = 100.0
    feed._last_price_at = 100.0
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: 146.0)
    ws = WebSocket()

    with pytest.raises(live_monitor.TransientWebSocketDisconnect):
        await feed._stale_watchdog(ws)

    assert ws.closed is True
    assert len(ws.sent) == 1
    assert state.crypto_status == "reconnecting"
    assert state.last_error == "Crypto price feed stale for 46.0s"
    assert ("status", {"source": "crypto"}) in updates


@pytest.mark.asyncio
async def test_crypto_stale_watchdog_resubscribes_and_reconnects_at_10s(monkeypatch):
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    class WebSocket:
        def __init__(self):
            self.closed = False
            self.sent = []

        async def send(self, payload):
            self.sent.append(payload)

        async def close(self):
            self.closed = True

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._connected_at = 100.0
    feed._last_price_at = 100.0
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: 110.0)
    ws = WebSocket()

    with pytest.raises(live_monitor.TransientWebSocketDisconnect):
        await feed._stale_watchdog(ws)

    assert ws.closed is True
    assert len(ws.sent) == 1
    assert state.crypto_status == "reconnecting"
    assert state.last_error == "Crypto price feed stale for 10.0s"
    assert ("status", {"source": "crypto"}) in updates


@pytest.mark.asyncio
async def test_crypto_heartbeat_reconnects_on_missing_pong(monkeypatch):
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    class WebSocket:
        def __init__(self):
            self.closed = False

        async def ping(self):
            return asyncio.Future()

        async def close(self):
            self.closed = True

    async def immediate_sleep(_delay):
        return None

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    monkeypatch.setattr(live_monitor.asyncio, "sleep", immediate_sleep)
    monkeypatch.setattr(live_monitor, "CRYPTO_HEARTBEAT_TIMEOUT_SECONDS", 0.001)
    ws = WebSocket()

    with pytest.raises(live_monitor.TransientWebSocketDisconnect):
        await feed._heartbeat(ws)

    assert ws.closed is True
    assert state.crypto_status == "reconnecting"
    assert state.last_error == "Crypto WebSocket ping timeout after 0.0s"
    assert ("status", {"source": "crypto"}) in updates


@pytest.mark.asyncio
async def test_crypto_listener_ignores_stale_provider_timestamp(monkeypatch):
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    class WebSocket:
        def __init__(self):
            self.messages = iter([
                json.dumps({
                    "payload": {
                        "symbol": "eth/usd",
                        "value": "123.45",
                        "timestamp": 1000,
                    },
                })
            ])

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.messages)
            except StopIteration:
                raise StopAsyncIteration

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1025.0)

    await feed._listen(WebSocket())

    assert state.crypto_price is None
    assert state.crypto_status == "stale"
    assert state.last_error == "Crypto price provider timestamp is stale (25.0s)"
    assert updates[-1] == ("status", {"source": "crypto"})


def test_short_polymarket_transport_reconnect_is_ignored(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState()
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)
    client._ever_connected = True
    clock = {"monotonic": 200.0, "time": 2000.0}
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(live_monitor.time, "time", lambda: clock["time"])

    client._mark_disconnected("transport ended without a WebSocket close frame")
    clock["monotonic"] = 229.0
    clock["time"] = 2029.0
    client._mark_connected()

    assert client.disconnect_count == 0
    assert client.disconnected_seconds == 0.0
    assert client.disconnect_periods == []


def test_long_polymarket_transport_reconnect_is_recorded(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    state = LiveMonitorState()
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)
    client._ever_connected = True
    clock = {"monotonic": 200.0, "time": 2000.0}
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(live_monitor.time, "time", lambda: clock["time"])

    client._mark_disconnected("transport ended without a WebSocket close frame")
    clock["monotonic"] = 231.0
    clock["time"] = 2031.0
    client._mark_connected()

    assert client.disconnect_count == 1
    assert client.disconnected_seconds == 31.0
    assert client.disconnect_periods == [{
        "start_ts": 2000.0,
        "end_ts": 2031.0,
        "duration_seconds": 31.0,
        "reason": "transport ended without a WebSocket close frame",
    }]


@pytest.mark.asyncio
async def test_polymarket_stale_watchdog_reconnects_silent_quote_feed(monkeypatch):
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    class WebSocket:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    state = LiveMonitorState(quotes={"token-1": LiveQuote()})
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)
    client._connected_at = 100.0
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: 110.0)
    ws = WebSocket()

    with pytest.raises(live_monitor.TransientWebSocketDisconnect):
        await client._stale_watchdog(ws)

    assert ws.closed is True
    assert state.quote_status == "reconnecting"
    assert state.last_error == "Polymarket CLOB quote feed stale for 10.0s"
    assert ("status", {"source": "polymarket"}) in updates


@pytest.mark.asyncio
async def test_polymarket_quote_receipt_refreshes_stale_watchdog_clock(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    clock = {"monotonic": 100.0}
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: clock["monotonic"])
    state = LiveMonitorState(quotes={"token-1": LiveQuote(bid=0.41, ask=0.44)})
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)
    client._connected_at = 90.0
    client._last_quote_at = 90.0

    await client._store_quote("token-1", 0.41, 0.44, 2000.0)

    assert client.health()["last_quote_age"] == 0.0
    assert state.quotes["token-1"].price_change_count == 0


@pytest.mark.asyncio
async def test_polymarket_listener_ignores_control_and_invalid_text_frames():
    async def on_update(_kind, _detail):
        return None

    class WebSocket:
        def __init__(self):
            self.messages = iter(["PING", "not json", "PONG"])

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.messages)
            except StopIteration:
                raise StopAsyncIteration

    state = LiveMonitorState()
    client = PolymarketLiveOrderBook(["token-1"], state, on_update)

    await client._listen(WebSocket())


def test_disconnect_period_is_clipped_to_contract_window(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/eth-updown-5m-1782658800",
        slug="eth-updown-5m-1782658800",
        title="Ethereum Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Ethereum Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="eth/usd",
        window_size_seconds=300,
        window_start_ts=1782658800,
        window_end_ts=1782659100,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 1782658800
    session.state.target_price = 1579.0
    session.state.crypto_price = 1580.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.49, ask=0.51)
    session.crypto.disconnect_count = 1
    session.crypto.disconnected_seconds = 5.262
    session.crypto.disconnect_periods = [{
        "start_ts": 1782658798.498,
        "end_ts": 1782658803.76,
        "duration_seconds": 5.262,
    }]
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782659101)

    record = session.finished_record(status="ended")
    gap = record["connection_gap_periods"][0]

    assert gap["start_ts"] == 1782658800.0
    assert gap["end_ts"] == 1782658803.76
    assert gap["start_second"] == 0
    assert gap["end_second"] == 3.76
    assert gap["duration_seconds"] == 3.76
    assert gap["raw_start_ts"] == 1782658798.498
    assert gap["raw_duration_seconds"] == 5.262


def test_stale_crypto_price_age_is_recorded_only_for_real_samples(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.state.target_price = 100.0
    session.state.crypto_price = 101.0
    session.state.crypto_updated_at = 1782639600.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)

    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639601.0)
    session._capture_history_row()
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639604.5)
    session._capture_history_row()

    history = session.finished_record(status="ended")["history"]
    assert len(history) == 2
    assert history[0]["t"] == 1782639601.0
    assert history[0]["crypto_age_seconds"] == 1.0
    assert history[-1]["t"] == 1782639604.5
    assert history[-1]["crypto_age_seconds"] == 4.5


def test_crypto_stale_does_not_trigger_fast_reconnect():
    assert not live_monitor._is_fast_crypto_reconnect(
        "Crypto price feed stale for 3.2s"
    )
    assert live_monitor._is_fast_crypto_reconnect(
        "Crypto price WebSocket stream ended"
    )
    assert not live_monitor._is_fast_crypto_reconnect("temporary DNS failure")


def test_crypto_price_payload_reads_nested_timestamp():
    point = live_monitor._crypto_price_point_from_payload({
        "symbol": "eth/usd",
        "timestamp": 1_000_000_001_000,
        "data": [{"value": "99.005", "timestamp": 1_000_000_000_250}],
    })

    assert point == (99.005, 1_000_000_000.25)


@pytest.mark.asyncio
async def test_crypto_listener_reads_nested_symbol_payloads(monkeypatch):
    updates = []

    async def on_update(kind, detail):
        updates.append((kind, detail))

    class WebSocket:
        def __init__(self):
            self.messages = iter([
                json.dumps({
                    "payload": {
                        "topic": "crypto_prices_chainlink",
                        "data": [
                            {"symbol": "btc/usd", "value": "45000", "timestamp": 1000},
                            {"symbol": "ETH", "value": "123.45", "timestamp": 1001},
                        ],
                    },
                })
            ])

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.messages)
            except StopIteration:
                raise StopAsyncIteration

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1001.5)

    await feed._listen(WebSocket())

    assert state.crypto_price == 123.45
    assert state.crypto_updated_at == 1001
    assert state.crypto_status == "ready"
    assert updates == [("crypto", {"symbol": "eth/usd", "price_ts": 1001})]


@pytest.mark.asyncio
async def test_gamma_event_payload_retries_after_rate_limit(monkeypatch):
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)

    class Response:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self.payload = payload
            self.headers = {"retry-after": "0.25"} if status_code == 429 else {}

        def raise_for_status(self):
            if self.status_code >= 400:
                request = httpx.Request(
                    "GET",
                    "https://gamma-api.polymarket.com/events",
                )
                response = httpx.Response(self.status_code, request=request)
                raise httpx.HTTPStatusError(
                    "rate limited",
                    request=request,
                    response=response,
                )

        def json(self):
            return self.payload

    class Client:
        def __init__(self):
            self.calls = 0

        async def get(self, _url, params):
            self.calls += 1
            if self.calls == 1:
                return Response(429, {"error": "slow down"})
            return Response(200, [{"slug": params["slug"]}])

    monkeypatch.setattr(live_monitor.asyncio, "sleep", sleep)

    client = Client()
    payload = await live_monitor._fetch_gamma_event_payload(client, "eth-updown")

    assert payload == [{"slug": "eth-updown"}]
    assert client.calls == 2
    assert sleeps == [0.25]


def test_target_price_locks_on_first_websocket_price_after_window_start(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/eth-updown-5m-1000",
        slug="eth-updown-5m-1000",
        title="Ethereum Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Ethereum Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="eth/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 999.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session.state.crypto_price = 99.005
    session.state.crypto_updated_at = 999.8
    session._remember_crypto_update()

    session._capture_history_row(timestamp=1000.0, captured_at=1000.0)

    assert session.state.target_price is None
    assert [row["t"] for row in session._history] == [1000.0]
    assert session._history[0]["target_price"] is None

    session.state.crypto_price = 101.005
    session.state.crypto_updated_at = 1000.2
    session._remember_crypto_update()
    session._capture_history_row(timestamp=1000.5, captured_at=1000.5)

    assert [row["t"] for row in session._history] == [1000.0, 1000.5]
    assert session.state.target_price == 101.01
    assert session._history[0]["target_price"] == 101.01
    assert session._history[0]["target_price_ts"] == 1000.2
    assert session._history[1]["target_price"] == 101.01
    assert session._history[1]["target_price_ts"] == 1000.2


def test_saved_contract_marks_late_target_price_as_anomaly():
    record = live_monitor._with_saved_category({
        "id": "late-target",
        "slug": "sol-updown-5m-1000",
        "question": "Solana Up or Down",
        "crypto_symbol": "sol/usd",
        "window_start_ts": 1000.0,
        "window_end_ts": 1300.0,
        "target_price": 81.10,
        "target_price_ts": 1080.0,
        "final_crypto_price": 81.50,
        "winning_outcome": "Up",
        "history_count": 0,
        "history": [],
    })

    assert record["target_price_anomaly"] is True
    assert record["unsuitable_contract"] is True
    assert record["quality_anomalies"][0]["type"] == "target_locked_late"
    assert "Target price locked" in record["unsuitable_reason"]


def test_saved_contract_accepts_start_history_target_even_if_record_timestamp_is_late():
    record = live_monitor._with_saved_category({
        "id": "history-start-target",
        "slug": "sol-updown-5m-1000",
        "question": "Solana Up or Down",
        "crypto_symbol": "sol/usd",
        "window_start_ts": 1000.0,
        "window_end_ts": 1300.0,
        "target_price": 82.55,
        "target_price_ts": 1003.0,
        "final_crypto_price": 82.40,
        "winning_outcome": "Down",
        "unsuitable_contract": False,
        "history_count": 2,
        "history": [
            {
                "t": 1000.0,
                "time": "00:16:40",
                "crypto_price": 82.553057,
                "target_price": 82.55,
                "target_price_ts": 1003.0,
            },
            {
                "t": 1000.5,
                "crypto_price": 82.553057,
                "target_price": 82.55,
                "target_price_ts": 1003.0,
            },
        ],
    })

    assert record.get("target_price_anomaly") is not True
    assert not any(
        item.get("type") == "target_locked_late"
        for item in record.get("quality_anomalies") or []
    )


def test_saved_contract_accepts_target_matching_start_crypto_rounding():
    record = live_monitor._with_saved_category({
        "id": "rounded-start-target",
        "slug": "sol-updown-5m-1000",
        "question": "Solana Up or Down",
        "crypto_symbol": "sol/usd",
        "window_start_ts": 1000.0,
        "window_end_ts": 1300.0,
        "target_price": 81.92,
        "target_price_ts": 1000.0,
        "final_crypto_price": 82.40,
        "winning_outcome": "Up",
        "history_count": 1,
        "history": [
            {
                "t": 1000.0,
                "crypto_price": 81.91605096982137,
                "crypto_updated_at": 1000.0,
                "target_price": 81.92,
                "target_price_ts": 1000.0,
            },
        ],
    })

    assert record.get("target_price_anomaly") is not True


def test_saved_contract_marks_half_second_late_target_price_as_anomaly():
    record = live_monitor._with_saved_category({
        "id": "half-second-late-target",
        "slug": "sol-updown-5m-1000",
        "question": "Solana Up or Down",
        "crypto_symbol": "sol/usd",
        "window_start_ts": 1000.0,
        "window_end_ts": 1300.0,
        "target_price": 81.10,
        "target_price_ts": 1000.5,
        "final_crypto_price": 81.50,
        "winning_outcome": "Up",
        "history_count": 0,
        "history": [],
    })

    assert record["target_price_anomaly"] is True
    assert record["unsuitable_contract"] is True
    assert record["quality_anomalies"][0]["delay_seconds"] == 0.5


def test_saved_contract_marks_missing_target_timestamp_as_anomaly():
    record = live_monitor._with_saved_category({
        "id": "missing-target-ts",
        "slug": "sol-updown-5m-1000",
        "question": "Solana Up or Down",
        "crypto_symbol": "sol/usd",
        "window_start_ts": 1000.0,
        "window_end_ts": 1300.0,
        "target_price": 81.10,
        "final_crypto_price": 81.50,
        "winning_outcome": "Up",
        "history_count": 0,
        "history": [],
    })

    assert record["target_price_anomaly"] is True
    assert record["quality_anomalies"][0]["type"] == "target_timestamp_missing"


def test_target_price_uses_dogecoin_precision(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/doge-updown-5m-1000",
        slug="doge-updown-5m-1000",
        title="Dogecoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Dogecoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="doge/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 999.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session.state.crypto_price = 0.0725164
    session.state.crypto_updated_at = 1000.2
    session._remember_crypto_update()

    session._capture_history_row(timestamp=1000.5, captured_at=1000.5)

    assert session.state.target_price == 0.072516
    assert session._history[0]["target_price"] == 0.072516


def test_finished_record_falls_back_to_first_history_price_for_missing_target(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/doge-updown-5m-1000",
        slug="doge-updown-5m-1000",
        title="Dogecoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Dogecoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="doge/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 999.0
    quote = LiveQuote(bid=0.4, ask=0.45, event_at=1000.0, updated_at=1000.0)
    session._history = [
        session._history_row(
            1000.0,
            quote,
            0.0741234,
            captured_at=1000.0,
            crypto_updated_at=990.0,
        ),
        session._history_row(
            1300.0,
            quote,
            0.075,
            captured_at=1300.0,
            crypto_updated_at=1299.0,
        ),
    ]

    record = session.finished_record(status="ended")

    assert record["target_price"] == 0.074123
    assert record["target_price_ts"] == 1000.0
    assert record["winning_outcome"] == "Up"
    assert record["history"][0]["target_price"] == 0.074123


def test_saved_record_with_missing_target_is_repaired_from_history():
    record = live_monitor._with_saved_category({
        "id": "record-1",
        "slug": "doge-updown-5m-1000",
        "question": "Dogecoin Up or Down",
        "crypto_symbol": "doge/usd",
        "window_start_ts": 1000,
        "window_end_ts": 1300,
        "target_price": None,
        "winning_outcome": None,
        "history": [
            {"t": 999.5, "crypto_price": 0.073},
            {"t": 1000.0, "crypto_price": 0.0741234},
            {"t": 1300.0, "crypto_price": 0.075},
        ],
    })

    assert record["target_price"] == 0.074123
    assert record["target_price_ts"] == 1000.0
    assert record["winning_outcome"] == "Up"
    assert record["history"][0].get("target_price") is None
    assert record["history"][1]["target_price"] == 0.074123
    assert record["summary"]["final_deviation_pct"] is not None


def test_late_history_capture_records_current_sample_only(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/eth-updown-5m-1000",
        slug="eth-updown-5m-1000",
        title="Ethereum Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Ethereum Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="eth/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 999.0
    session.state.crypto_price = 101.005
    session.state.crypto_updated_at = 1000.0
    session._remember_crypto_update()
    session.state.quotes["token-1"] = LiveQuote(
        bid=0.4,
        ask=0.45,
        updated_at=999.7,
        event_at=999.7,
        event_count=1,
        price_change_count=1,
    )
    session._remember_quote_update("token-1")

    captured_at = 1001.502
    session._capture_history_row(captured_at=captured_at)

    assert [row["t"] for row in session._history] == [1001.5]
    assert session._history[0]["target_price"] == 101.01
    assert session._history[0]["target_price_ts"] == 1000.0
    assert session._history[0]["crypto_updated_at"] == 1000.0
    assert session._history[0]["quote_event_at"] == 999.7
    assert session._history[0]["captured_at"] == 1001.502


@pytest.mark.parametrize(
    ("scheduled_ts", "captured_at", "expected_ts", "expected_skipped"),
    [
        (1000.0, 1000.249, 1000.0, 0),
        (1000.0, 1000.501, 1000.5, 1),
        (1000.0, 1002.219, 1002.0, 4),
    ],
)
def test_late_sampler_coalesces_missed_intervals(
    scheduled_ts,
    captured_at,
    expected_ts,
    expected_skipped,
):
    sample_ts, skipped = live_monitor._coalesce_late_history_sample(
        scheduled_ts,
        captured_at,
    )

    assert sample_ts == expected_ts
    assert skipped == expected_skipped
    assert sample_ts + live_monitor.HISTORY_CAPTURE_INTERVAL_SECONDS > captured_at


def test_history_does_not_use_future_source_points(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/eth-updown-5m-1000",
        slug="eth-updown-5m-1000",
        title="Ethereum Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Ethereum Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="eth/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.started_at_ts = 999.0
    session.state.crypto_price = 101.005
    session.state.crypto_updated_at = 1001.5
    session._remember_crypto_update()
    session.state.quotes["token-1"] = LiveQuote(
        bid=0.4,
        ask=0.45,
        updated_at=1001.5,
        event_at=1001.5,
        event_count=1,
        price_change_count=1,
    )
    session._remember_quote_update("token-1")

    captured_at = 1001.502
    session._capture_history_row(timestamp=1001.0, captured_at=captured_at)

    assert session._history == []

    session._capture_history_row(timestamp=1001.5, captured_at=captured_at)

    assert [row["t"] for row in session._history] == [1001.5]
    assert session._history[0]["crypto_updated_at"] == 1001.5
    assert session._history[0]["quote_event_at"] == 1001.5


@pytest.mark.asyncio
async def test_taking_preloaded_contract_does_not_stop_history_capture(tmp_path):
    current_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/doge-updown-5m-1000",
        slug="doge-updown-5m-1000",
        title="Dogecoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="current-token",
                market_question="Current",
                outcome="Up",
            )
        ],
        crypto_symbol="doge/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    next_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/doge-updown-5m-1300",
        slug="doge-updown-5m-1300",
        title="Dogecoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="next-token",
                market_question="Next",
                outcome="Up",
            )
        ],
        crypto_symbol="doge/usd",
        window_size_seconds=300,
        window_start_ts=1300,
        window_end_ts=1600,
    )
    session = LiveMonitorSession("session-1", "user-1", current_event, tmp_path)

    async def on_preload_update(_kind, _detail):
        return None

    preload_state = LiveMonitorState(quotes={"next-token": LiveQuote()})
    orderbook = PolymarketLiveOrderBook(
        ["next-token"],
        preload_state,
        on_preload_update,
    )
    task = asyncio.create_task(asyncio.sleep(60))
    history_task = asyncio.create_task(asyncio.sleep(60))
    session._preloaded_next = PreloadedPolymarketOrderBook(
        event=next_event,
        state=preload_state,
        orderbook=orderbook,
        task=task,
        history_task=history_task,
    )

    try:
        preloaded = session._take_preloaded_next(
            "https://polymarket.com/event/doge-updown-5m-1300"
        )

        assert preloaded is not None
        assert preloaded.history_stopped is False
        assert preloaded.history_task is history_task
        assert history_task.cancelled() is False
    finally:
        task.cancel()
        history_task.cancel()
        await asyncio.gather(task, history_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_preloaded_switch_preserves_rollover_target_price(tmp_path):
    current_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="current-token",
                market_question="Current",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    next_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639900",
        slug="btc-updown-5m-1782639900",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="next-token",
                market_question="Next",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639900,
        window_end_ts=1782640200,
    )
    session = LiveMonitorSession("session-1", "user-1", current_event, tmp_path)
    session.state.crypto_price = 125.0
    session.state.target_price = 100.0
    session.state.quotes["current-token"] = LiveQuote(bid=0.4, ask=0.45)
    preload_state = LiveMonitorState(
        quotes={"next-token": LiveQuote(bid=0.51, ask=0.52)}
    )

    async def on_preload_update(_kind, _detail):
        return None

    orderbook = PolymarketLiveOrderBook(
        ["next-token"],
        preload_state,
        on_preload_update,
    )
    task = asyncio.create_task(asyncio.sleep(60))
    preloaded = PreloadedPolymarketOrderBook(
        event=next_event,
        state=preload_state,
        orderbook=orderbook,
        task=task,
    )
    original_time = live_monitor.time.time
    try:
        live_monitor.time.time = lambda: 1782639900.01
        await session._switch_event(
            next_event,
            target_price=123.45,
            started_at_ts=1782639900.0,
            preloaded=preloaded,
        )
    finally:
        live_monitor.time.time = original_time
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert session.event.slug == "btc-updown-5m-1782639900"
    assert session.state.target_price == 123.45
    assert session.polymarket is orderbook
    assert session.state.quotes["next-token"].bid == 0.51
    assert session.finished_record(status="ended")["history"][0]["target_price"] == 123.45


@pytest.mark.asyncio
async def test_preloaded_switch_preserves_early_orderbook_timeline(tmp_path, monkeypatch):
    current_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/eth-updown-5m-1000",
        slug="eth-updown-5m-1000",
        title="Ethereum Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="current-token",
                market_question="Current",
                outcome="Up",
            )
        ],
        crypto_symbol="eth/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    next_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/eth-updown-5m-1300",
        slug="eth-updown-5m-1300",
        title="Ethereum Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="next-token",
                market_question="Next",
                outcome="Up",
            )
        ],
        crypto_symbol="eth/usd",
        window_size_seconds=300,
        window_start_ts=1300,
        window_end_ts=1600,
    )
    session = LiveMonitorSession("session-1", "user-1", current_event, tmp_path)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1300.01)
    session.state.crypto_price = 1563.85
    session.state.crypto_updated_at = 1300.0
    session._remember_crypto_update()
    early_quote = LiveQuote(
        bid=0.47,
        ask=0.48,
        updated_at=1292.0,
        event_at=1292.0,
        event_count=1,
        price_change_count=1,
    )
    latest_quote = LiveQuote(
        bid=0.51,
        ask=0.52,
        updated_at=1299.7,
        event_at=1299.7,
        event_count=2,
        price_change_count=2,
    )
    preload_state = LiveMonitorState(quotes={"next-token": latest_quote})

    async def on_preload_update(_kind, _detail):
        return None

    orderbook = PolymarketLiveOrderBook(
        ["next-token"],
        preload_state,
        on_preload_update,
    )
    task = asyncio.create_task(asyncio.sleep(60))
    preloaded = PreloadedPolymarketOrderBook(
        event=next_event,
        state=preload_state,
        orderbook=orderbook,
        task=task,
        quote_timeline={"next-token": [early_quote, latest_quote]},
    )
    try:
        await session._switch_event(
            next_event,
            target_price=1563.85,
            started_at_ts=1300.0,
            preloaded=preloaded,
        )
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert session._history[0]["t"] == 1300.0
    assert session._history[0]["yes_bid"] == 51.0
    assert session._history[0]["yes_ask"] == 52.0
    assert session._history[0]["quote_event_at"] == 1299.7


@pytest.mark.asyncio
async def test_preloaded_switch_preserves_already_captured_history(tmp_path, monkeypatch):
    current_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/doge-updown-5m-1000",
        slug="doge-updown-5m-1000",
        title="Dogecoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="current-token",
                market_question="Current",
                outcome="Up",
            )
        ],
        crypto_symbol="doge/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    next_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/doge-updown-5m-1300",
        slug="doge-updown-5m-1300",
        title="Dogecoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="next-token",
                market_question="Next",
                outcome="Up",
            )
        ],
        crypto_symbol="doge/usd",
        window_size_seconds=300,
        window_start_ts=1300,
        window_end_ts=1600,
    )
    session = LiveMonitorSession("session-1", "user-1", current_event, tmp_path)
    session.state.crypto_price = 0.0719
    session.state.crypto_updated_at = 1296.0
    session._remember_crypto_update()
    session.state.crypto_price = 0.072
    session.state.crypto_updated_at = 1299.0
    session._remember_crypto_update()
    session.state.crypto_price = 0.07213
    session.state.crypto_updated_at = 1300.0
    session._remember_crypto_update()
    quote = LiveQuote(
        bid=0.5,
        ask=0.53,
        updated_at=1296.0,
        event_at=1296.0,
        event_count=1,
        price_change_count=1,
    )
    preload_state = LiveMonitorState(quotes={"next-token": quote})

    async def on_preload_update(_kind, _detail):
        return None

    orderbook = PolymarketLiveOrderBook(
        ["next-token"],
        preload_state,
        on_preload_update,
    )
    task = asyncio.create_task(asyncio.sleep(60))
    history_task = asyncio.create_task(asyncio.sleep(60))
    preloaded = PreloadedPolymarketOrderBook(
        event=next_event,
        state=preload_state,
        orderbook=orderbook,
        task=task,
        quote_timeline={"next-token": [quote]},
        history_task=history_task,
    )
    session._capture_preloaded_history_row(
        preloaded,
        timestamp=1296.5,
        captured_at=1296.51,
    )
    session._capture_preloaded_history_row(
        preloaded,
        timestamp=1297.0,
        captured_at=1297.01,
    )
    session._capture_preloaded_history_row(
        preloaded,
        timestamp=1300.0,
        captured_at=1300.01,
    )
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1301.01)

    try:
        await session._switch_event(
            next_event,
            target_price=None,
            started_at_ts=1300.0,
            preloaded=preloaded,
        )
    finally:
        task.cancel()
        history_task.cancel()
        await asyncio.gather(task, history_task, return_exceptions=True)

    assert [row["t"] for row in preloaded.history] == [1296.5, 1297.0, 1300.0]
    assert [row["t"] for row in session._history] == [1296.5, 1297.0, 1300.0, 1301.0]
    assert session._history[0]["t"] == 1296.5
    assert session._history[2]["t"] == 1300.0
    assert session._history[2]["captured_at"] == 1300.01
    assert session._history[2]["yes_bid"] == 50.0
    assert session._history[2]["yes_ask"] == 53.0


@pytest.mark.asyncio
async def test_switch_event_does_not_wait_for_previous_orderbook_close(
    tmp_path,
    monkeypatch,
):
    current_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/eth-updown-5m-1000",
        slug="eth-updown-5m-1000",
        title="Ethereum Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="current-token",
                market_question="Current",
                outcome="Up",
            )
        ],
        crypto_symbol="eth/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    next_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/eth-updown-5m-1300",
        slug="eth-updown-5m-1300",
        title="Ethereum Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="next-token",
                market_question="Next",
                outcome="Up",
            )
        ],
        crypto_symbol="eth/usd",
        window_size_seconds=300,
        window_start_ts=1300,
        window_end_ts=1600,
    )
    session = LiveMonitorSession("session-1", "user-1", current_event, tmp_path)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1300.01)
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    async def slow_close():
        close_started.set()
        await release_close.wait()

    session.polymarket.close = slow_close
    preload_state = LiveMonitorState(
        quotes={
            "next-token": LiveQuote(
                bid=0.51,
                ask=0.52,
                updated_at=1299.7,
                event_at=1299.7,
                event_count=1,
                price_change_count=1,
            )
        },
    )

    async def on_preload_update(_kind, _detail):
        return None

    orderbook = PolymarketLiveOrderBook(
        ["next-token"],
        preload_state,
        on_preload_update,
    )
    task = asyncio.create_task(asyncio.sleep(60))
    preloaded = PreloadedPolymarketOrderBook(
        event=next_event,
        state=preload_state,
        orderbook=orderbook,
        task=task,
        quote_timeline={"next-token": [preload_state.quotes["next-token"]]},
    )
    try:
        await asyncio.wait_for(
            session._switch_event(
                next_event,
                target_price=1563.85,
                started_at_ts=1300.0,
                preloaded=preloaded,
            ),
            timeout=0.5,
        )

        assert session.event.slug == "eth-updown-5m-1300"
        assert session.polymarket is orderbook
        await asyncio.wait_for(close_started.wait(), timeout=0.5)
    finally:
        release_close.set()
        task.cancel()
        await asyncio.gather(*session._tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_switch_event_resets_crypto_disconnect_metrics(tmp_path):
    current_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="current-token",
                market_question="Current",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
    )
    next_event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639900",
        slug="btc-updown-5m-1782639900",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="next-token",
                market_question="Next",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639900,
        window_end_ts=1782640200,
    )
    session = LiveMonitorSession("session-1", "user-1", current_event, tmp_path)
    session.state.crypto_price = 125.0
    session.state.target_price = 100.0
    session.state.quotes["current-token"] = LiveQuote(bid=0.4, ask=0.45)
    session.crypto._ws = object()
    session.crypto._ever_connected = True
    session.crypto.disconnect_count = 1
    session.crypto.disconnected_seconds = 5.5

    try:
        await session._switch_event(
            next_event,
            target_price=123.45,
            started_at_ts=1782639900.0,
        )

        assert session.crypto.connection_stats() == {
            "disconnect_count": 0,
            "disconnected_seconds": 0.0,
            "disconnect_periods": [],
        }
    finally:
        await session.polymarket.close()
        for task in session._tasks:
            task.cancel()
        await asyncio.gather(*session._tasks, return_exceptions=True)


def test_legacy_saved_record_partial_fields_detect_early_end():
    fields = _record_partial_fields(
        {
            "window_start_ts": 100,
            "window_end_ts": 400,
            "history": [{"t": 103}, {"t": 250}],
        }
    )

    assert fields["unsuitable_contract"] is True
    assert fields["partial_contract"] is False
    assert fields["tracking_started_after_seconds"] is None
    assert fields["tracking_ended_before_seconds"] == 150
    assert fields["unsuitable_reason"] == "Tracking stopped before the contract window ended."


def test_legacy_saved_record_partial_fields_marks_start_after_five_seconds_unsuitable():
    fields = _record_partial_fields(
        {
            "window_start_ts": 100,
            "window_end_ts": 400,
            "history": [{"t": 106}, {"t": 400}],
        }
    )

    assert fields["unsuitable_contract"] is True
    assert fields["partial_contract"] is False
    assert fields["tracking_started_after_seconds"] == 6
    assert "Tracking started after" in fields["unsuitable_reason"]


def test_history_preserves_missing_ask_price(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1782639600",
        slug="btc-updown-5m-1782639600",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1782639600,
        window_end_ts=1782639900,
        event_title="10:00:00 - 10:05:00",
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.state.target_price = 100.0
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.99, ask=None)

    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639600.0)
    session._capture_history_row()

    row = session.finished_record(status="ended")["history"][0]
    assert row["yes_bid"] == 99.0
    assert row["yes_ask"] is None
    assert row["no_bid"] is None
    assert row["no_ask"] == 1.0
    assert row["spread"] is None


def test_history_records_at_half_second_resolution(tmp_path, monkeypatch):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1000",
        slug="btc-updown-5m-1000",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
        event_title="10:00:00 - 10:05:00",
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.state.target_price = 100.0
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)

    monkeypatch.setattr(live_monitor.time, "time", lambda: 1000.01)
    session._capture_history_row()
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1000.24)
    session._capture_history_row()
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1000.51)
    session._capture_history_row()

    history = session.finished_record(status="ended")["history"]
    assert len(history) == 2
    assert history[0]["t"] == 1000.0
    assert history[0]["time"].endswith(":16:40")
    assert history[1]["t"] == 1000.5
    assert history[1]["time"].endswith(".500")
    assert history[1]["captured_at"] == 1000.51
    assert history[1]["sample_delay_ms"] == 10.0


def test_finished_record_preserves_only_recorded_history_rows(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1000",
        slug="btc-updown-5m-1000",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.state.target_price = 100.0
    session._target_price_ts = 1000.0
    quote = LiveQuote(bid=0.4, ask=0.45, event_at=990.0, updated_at=990.0)
    session._history = [
        session._history_row(
            timestamp,
            quote,
            101.0,
            captured_at=timestamp,
            crypto_updated_at=timestamp,
        )
        for timestamp in (990.0, 990.5, 999.5, 1001.5, 1300.0)
    ]

    history = session.finished_record(status="ended")["history"]
    times = [row["t"] for row in history]

    assert len(history) == 5
    assert times[0] == 990.0
    assert times[-1] == 1300.0
    assert {1000.0, 1000.5, 1001.0}.isdisjoint(times)
    assert history[times.index(999.5)]["target_price"] is None
    assert history[times.index(1001.5)]["target_price"] == 100.0


def test_live_history_memory_limit_trims_long_running_session(tmp_path, monkeypatch):
    monkeypatch.setattr(live_monitor, "LIVE_HISTORY_MEMORY_LIMIT", 3)
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/custom-long-contract",
        slug="custom-long-contract",
        title="Custom Long Contract",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Custom Long Contract",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    quote = LiveQuote(bid=0.4, ask=0.45, event_at=1000.0, updated_at=1000.0)

    for index in range(5):
        timestamp = 1000.0 + index * live_monitor.HISTORY_CAPTURE_INTERVAL_SECONDS
        session._append_history_row(
            timestamp,
            quote,
            101.0 + index,
            captured_at=timestamp,
            crypto_updated_at=timestamp,
        )

    assert [row["t"] for row in session._history] == [1001.0, 1001.5, 1002.0]
    assert session._history_truncated_count == 2
    record = session.finished_record(status="ended")
    assert record["history_truncated_count"] == 2
    assert record["history_truncated_before_ts"] == 1000.5


@pytest.mark.asyncio
async def test_preloaded_history_memory_limit_trims_long_running_preload(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(live_monitor, "LIVE_HISTORY_MEMORY_LIMIT", 3)
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/custom-long-contract",
        slug="custom-long-contract",
        title="Custom Long Contract",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Custom Long Contract",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    state = LiveMonitorState(quotes={"token-1": LiveQuote()})
    orderbook = PolymarketLiveOrderBook(["token-1"], state, session._handle_update)
    task = asyncio.create_task(asyncio.sleep(60))
    preloaded = PreloadedPolymarketOrderBook(event, state, orderbook, task)
    quote = LiveQuote(bid=0.4, ask=0.45, event_at=1000.0, updated_at=1000.0)

    try:
        for index in range(5):
            timestamp = 1000.0 + index * live_monitor.HISTORY_CAPTURE_INTERVAL_SECONDS
            session._append_preloaded_history_row(
                preloaded,
                timestamp,
                quote,
                101.0 + index,
                captured_at=timestamp,
                crypto_updated_at=timestamp,
            )

        assert [row["t"] for row in preloaded.history] == [1001.0, 1001.5, 1002.0]
        assert preloaded.history_truncated_count == 2
        assert preloaded.history_truncated_before_ts == 1000.5
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_saved_record_preview_cache_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(live_monitor, "SAVED_RECORD_PREVIEW_CACHE_LIMIT", 2)
    live_monitor._SAVED_RECORD_PREVIEW_CACHE.clear()

    try:
        directories = []
        for index in range(3):
            directory = tmp_path / f"user-{index}"
            directory.mkdir()
            directories.append(directory)
            (directory / "record.json").write_text(
                json.dumps({
                    "id": f"record-{index}",
                    "saved_at": f"2026-07-02T00:00:0{index}+00:00",
                }),
                encoding="utf-8",
            )
            live_monitor._read_saved_record_metadata_preview_paths(directory)

        assert len(live_monitor._SAVED_RECORD_PREVIEW_CACHE) == 2
        assert directories[0] not in live_monitor._SAVED_RECORD_PREVIEW_CACHE
        assert directories[1] in live_monitor._SAVED_RECORD_PREVIEW_CACHE
        assert directories[2] in live_monitor._SAVED_RECORD_PREVIEW_CACHE
    finally:
        live_monitor._SAVED_RECORD_PREVIEW_CACHE.clear()


def test_disconnect_periods_are_bounded(monkeypatch):
    monkeypatch.setattr(live_monitor, "LIVE_DISCONNECT_PERIOD_LIMIT", 2)
    periods = []

    for index in range(4):
        live_monitor._append_limited(
            periods,
            {"start_ts": index, "end_ts": index + 1},
            live_monitor.LIVE_DISCONNECT_PERIOD_LIMIT,
        )

    assert periods == [
        {"start_ts": 2, "end_ts": 3},
        {"start_ts": 3, "end_ts": 4},
    ]


def test_history_sample_does_not_use_future_quote_update(tmp_path):
    event = ResolvedPolymarketEvent(
        url="https://polymarket.com/event/btc-updown-5m-1000",
        slug="btc-updown-5m-1000",
        title="Bitcoin Up or Down",
        markets=[],
        tokens=[
            LiveToken(
                token_id="token-1",
                market_question="Bitcoin Up or Down",
                outcome="Up",
            )
        ],
        crypto_symbol="btc/usd",
        window_size_seconds=300,
        window_start_ts=1000,
        window_end_ts=1300,
    )
    session = LiveMonitorSession("session-1", "user-1", event, tmp_path)
    session.state.target_price = 100.0
    session.state.crypto_price = 101.0
    session.state.crypto_updated_at = 1000.0
    session._remember_crypto_update()

    session.state.quotes["token-1"] = LiveQuote(
        bid=0.4,
        ask=0.45,
        updated_at=1000.1,
        event_at=1000.1,
    )
    session._remember_quote_update("token-1")
    session.state.quotes["token-1"] = LiveQuote(
        bid=0.5,
        ask=0.55,
        updated_at=1000.6,
        event_at=1000.6,
    )
    session._remember_quote_update("token-1")

    session._capture_history_row(timestamp=1000.5, captured_at=1000.61)

    row = session.finished_record(status="ended")["history"][0]
    assert row["t"] == 1000.5
    assert row["yes_bid"] == 40.0
    assert row["yes_ask"] == 45.0
    assert row["quote_event_at"] == 1000.1
    assert row["quote_age_seconds"] == 0.4


@pytest.mark.asyncio
async def test_saved_records_are_grouped_by_crypto_category(tmp_path):
    manager = LiveMonitorManager(tmp_path)
    saved_dir = tmp_path / "users" / "user-1" / "saved_contracts"
    saved_dir.mkdir(parents=True)
    btc_record = {
        "id": "btc-record",
        "question": "Bitcoin Up or Down",
        "crypto_symbol": "btc/usd",
        "saved_at": "2026-01-01T00:00:00Z",
        "history": [{"t": 1, "crypto_price": 100, "yes_bid": 40, "yes_ask": 45}],
    }
    eth_record = {
        "id": "eth-record",
        "question": "Ethereum Up or Down",
        "crypto_symbol": "eth/usd",
        "saved_at": "2026-01-01T00:01:00Z",
        "history": [{"t": 2, "crypto_price": 200, "yes_bid": 50, "yes_ask": 55}],
    }
    (saved_dir / "btc-record.json").write_text(json.dumps(btc_record), encoding="utf-8")
    (saved_dir / "eth-record.json").write_text(json.dumps(eth_record), encoding="utf-8")

    categories = manager.saved_categories("user-1")
    assert categories == [
        {"id": "btc", "symbol": "btc/usd", "label": "Bitcoin", "count": 1},
        {"id": "eth", "symbol": "eth/usd", "label": "Ethereum", "count": 1},
    ]

    btc_saved = manager.list_saved("user-1", category="btc")
    assert [record["id"] for record in btc_saved] == ["btc-record"]
    assert btc_saved[0]["category"]["label"] == "Bitcoin"
    assert manager.get_saved("user-1", "eth-record")["history"][0]["yes_ask"] == 55

    assert manager.delete_saved_category("user-1", "btc") == 1
    assert [record["id"] for record in manager.list_saved("user-1")] == ["eth-record"]


def test_saved_records_can_be_loaded_by_ids_in_requested_order(tmp_path, monkeypatch):
    monkeypatch.setenv("LIVE_MONITOR_SAVED_READ_WORKERS", "4")
    manager = LiveMonitorManager(tmp_path)
    saved_dir = tmp_path / "users" / "user-1" / "saved_contracts"
    saved_dir.mkdir(parents=True)
    for index in range(6):
        record = {
            "id": f"record-{index}",
            "question": f"Contract {index}",
            "crypto_symbol": "btc/usd",
            "saved_at": f"2026-01-01T00:00:0{index}Z",
            "history": [{"t": index, "crypto_price": 100 + index}],
        }
        (saved_dir / f"20260101-00000{index}Z__btc-updown__record-{index}.json").write_text(
            json.dumps(record),
            encoding="utf-8",
        )

    records = manager.list_saved_by_ids(
        "user-1",
        ["record-4", "missing", "record-1", "record-4", "record-5"],
    )

    assert [record["id"] for record in records] == ["record-4", "record-1", "record-5"]
    assert all(record["category"]["id"] == "btc" for record in records)


def test_saved_records_are_paginated_newest_first_without_history(tmp_path):
    manager = LiveMonitorManager(tmp_path)
    saved_dir = tmp_path / "users" / "user-1" / "saved_contracts"
    saved_dir.mkdir(parents=True)
    for index in range(25):
        symbol = "btc/usd" if index % 2 == 0 else "eth/usd"
        record = {
            "id": f"record-{index:02d}",
            "question": f"Contract {index:02d}",
            "crypto_symbol": symbol,
            "saved_at": f"2026-01-01T00:{index:02d}:00Z",
            "history_count": 1,
            "history": [{"t": index, "crypto_price": 100 + index}],
        }
        (saved_dir / f"record-{index:02d}.json").write_text(
            json.dumps(record),
            encoding="utf-8",
        )

    first_page = manager.list_saved_page("user-1", page=1, page_size=20)
    second_page = manager.list_saved_page("user-1", page=2, page_size=20)
    oversized_page = manager.list_saved_page("user-1", page=99, page_size=20)
    btc_page = manager.list_saved_page(
        "user-1",
        category="btc",
        page=1,
        page_size=20,
    )

    assert first_page["total"] == 25
    assert first_page["total_pages"] == 2
    assert first_page["has_next"] is True
    assert [record["id"] for record in first_page["records"][:2]] == [
        "record-24",
        "record-23",
    ]
    assert "history" not in first_page["records"][0]
    assert first_page["records"][0]["history_count"] == 1
    assert [record["id"] for record in second_page["records"]] == [
        "record-04",
        "record-03",
        "record-02",
        "record-01",
        "record-00",
    ]
    assert oversized_page["page"] == 2
    assert oversized_page["has_previous"] is True
    assert oversized_page["has_next"] is False
    assert [record["id"] for record in oversized_page["records"]] == [
        "record-04",
        "record-03",
        "record-02",
        "record-01",
        "record-00",
    ]
    assert btc_page["total"] == 13
    assert [record["id"] for record in btc_page["records"][:2]] == [
        "record-24",
        "record-22",
    ]


def test_saved_record_page_reads_metadata_without_full_history_parse(tmp_path):
    manager = LiveMonitorManager(tmp_path)
    saved_dir = tmp_path / "users" / "user-1" / "saved_contracts"
    saved_dir.mkdir(parents=True)
    (saved_dir / "20260101-000000Z__btc-updown__record-1.json").write_text(
        "\n".join(
            [
                "{",
                '  "id": "record-1",',
                '  "question": "Bitcoin Up or Down",',
                '  "crypto_symbol": "btc/usd",',
                '  "saved_at": "2026-01-01T00:00:00Z",',
                '  "window_start_ts": 1000,',
                '  "window_end_ts": 1300,',
                '  "history": [',
                "    this is intentionally not valid json",
                "  ]",
                "}",
            ]
        ),
        encoding="utf-8",
    )

    page = manager.list_saved_page("user-1", page=1, page_size=20)
    categories = manager.saved_categories("user-1")

    assert page["total"] == 1
    assert page["records"][0]["id"] == "record-1"
    assert "history" not in page["records"][0]
    assert "history_count" not in page["records"][0]
    assert categories == [
        {"id": "btc", "symbol": "btc/usd", "label": "Bitcoin", "count": 1},
    ]


@pytest.mark.asyncio
async def test_fetch_clob_book_uses_single_book_snapshot():
    class Response:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "bids": [
                    {"price": "0.39", "size": "2"},
                    {"price": "0.42", "size": "5"},
                ],
                "asks": [
                    {"price": "0.45", "size": "7"},
                    {"price": "0.43", "size": "11"},
                ],
            }

    class Client:
        async def get(self, url, params):
            assert url.endswith("/book")
            assert params == {"token_id": "token-1"}
            return Response()

    assert await _fetch_clob_book(Client(), "token-1") == (
        0.42,
        0.43,
        5.0,
        11.0,
    )


@pytest.mark.asyncio
async def test_fetch_clob_book_treats_inactive_token_status_as_empty():
    class Response:
        status_code = 422

        def raise_for_status(self):
            raise AssertionError("inactive token responses should be ignored")

    class Client:
        async def get(self, url, params):
            return Response()

    assert await _fetch_clob_book(Client(), "inactive-token") == (
        None,
        None,
        None,
        None,
    )
