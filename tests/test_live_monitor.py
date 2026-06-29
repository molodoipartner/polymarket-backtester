from __future__ import annotations

import asyncio
import json

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
    assert normalize_crypto_symbol("ETH") == "eth/usd"
    assert normalize_crypto_symbol(" sol/usd ") == "sol/usd"


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
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639700)

    record = session.finished_record(status="stopped")

    assert record["unsuitable_contract"] is True
    assert record["partial_contract"] is False
    assert record["tracking_started_after_seconds"] is None
    assert record["tracking_ended_before_seconds"] == 200
    assert record["unsuitable_reason"] == "Tracking stopped before the contract window ended."


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
    session.state.crypto_price = 101.0
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session.polymarket.disconnect_count = 2
    session.polymarket.disconnected_seconds = 7.25
    session.polymarket.disconnect_periods = [{
        "start_ts": 1782639660.0,
        "end_ts": 1782639667.25,
        "duration_seconds": 7.25,
        "reason": "transport ended without a WebSocket close frame",
    }]
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639901)

    record = session.finished_record(status="ended")

    assert record["partial_contract"] is True
    assert record["unsuitable_contract"] is False
    assert record["polymarket_disconnect_count"] == 2
    assert record["polymarket_disconnected_seconds"] == 7.25
    assert record["connection_gap_periods"][0]["source"] == "polymarket"
    assert record["connection_gap_periods"][0]["start_second"] == 60.0
    assert record["connection_gap_periods"][0]["end_second"] == 67.25
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
    session.state.crypto_price = 101.0
    session.state.crypto_updated_at = 1782639899
    session.state.quotes["token-1"] = LiveQuote(bid=0.4, ask=0.45)
    session.crypto.disconnect_count = 1
    session.crypto.disconnected_seconds = 4.5
    session.crypto.disconnect_periods = [{
        "start_ts": 1782639700.0,
        "end_ts": 1782639704.5,
        "duration_seconds": 4.5,
        "reason": "Crypto price feed stale for 10.4s",
    }]
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1782639901)

    record = session.finished_record(status="ended")

    assert record["partial_contract"] is True
    assert record["unsuitable_contract"] is False
    assert record["crypto_disconnect_count"] == 1
    assert record["crypto_disconnected_seconds"] == 4.5
    assert record["connection_gap_periods"][0]["source"] == "crypto"
    assert record["connection_gap_periods"][0]["start_second"] == 100.0
    assert record["connection_gap_periods"][0]["end_second"] == 104.5
    assert record["connection_gap_periods"][0]["reason"] == "Crypto price feed stale for 10.4s"
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
        reason="Crypto price feed stale for 10.4s",
    )
    monkeypatch.setattr(live_monitor.time, "monotonic", lambda: 14.5)
    monkeypatch.setattr(live_monitor.time, "time", lambda: 1004.5)
    feed._mark_connected()

    assert feed.disconnect_count == 1
    assert feed.disconnect_periods == [{
        "start_ts": 1000.0,
        "end_ts": 1004.5,
        "duration_seconds": 4.5,
        "reason": "Crypto price feed stale for 10.4s",
    }]


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


def test_stale_crypto_price_age_is_recorded_on_grid(tmp_path, monkeypatch):
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
    assert history[1]["t"] == 1782639604.5
    assert history[1]["crypto_age_seconds"] == 4.5


@pytest.mark.asyncio
async def test_crypto_watchdog_raises_when_price_feed_goes_stale(monkeypatch):
    async def on_update(_kind, _detail):
        return None

    async def immediate_sleep(_seconds):
        return None

    state = LiveMonitorState()
    feed = PolymarketCryptoPriceFeed("eth/usd", state, on_update)
    feed._last_price_at = live_monitor.time.monotonic() - (
        live_monitor.CRYPTO_RECONNECT_STALE_AFTER_SECONDS + 1
    )
    monkeypatch.setattr(live_monitor.asyncio, "sleep", immediate_sleep)

    with pytest.raises(TimeoutError, match="Crypto price feed stale"):
        await feed._watchdog()


def test_crypto_price_payload_reads_nested_timestamp():
    point = live_monitor._crypto_price_point_from_payload({
        "symbol": "eth/usd",
        "timestamp": 1_000_000_001_000,
        "data": [{"value": "99.005", "timestamp": 1_000_000_000_250}],
    })

    assert point == (99.005, 1_000_000_000.25)


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
    assert session._history == []

    session.state.crypto_price = 101.005
    session.state.crypto_updated_at = 1000.2
    session._remember_crypto_update()
    session._capture_history_row(timestamp=1000.5, captured_at=1000.5)

    assert [row["t"] for row in session._history] == [1000.5]
    assert session.state.target_price == 101.01
    assert session._history[0]["target_price"] == 101.01
    assert session._history[0]["target_price_ts"] == 1000.2


def test_initial_history_backfills_first_half_second_samples(tmp_path):
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
    session.state.crypto_updated_at = 1000.2
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

    sample_ts = session._initial_history_sample_ts(1001.5)
    captured_at = 1001.502
    while sample_ts <= captured_at + 0.001:
        session._capture_history_row(timestamp=sample_ts, captured_at=captured_at)
        sample_ts = round(sample_ts + live_monitor.HISTORY_CAPTURE_INTERVAL_SECONDS, 3)

    assert [row["t"] for row in session._history[:4]] == [
        1000.0,
        1000.5,
        1001.0,
        1001.5,
    ]
    assert session._history[0]["target_price"] == 101.01
    assert session._history[0]["target_price_ts"] == 1000.2
    assert session._history[0]["crypto_updated_at"] == 1000.2
    assert session._history[0]["quote_event_at"] == 1001.5
    assert session._history[0]["captured_at"] == 1001.502


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
    try:
        await session._switch_event(
            next_event,
            target_price=123.45,
            started_at_ts=1782639900.0,
            preloaded=preloaded,
        )
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert session.event.slug == "btc-updown-5m-1782639900"
    assert session.state.target_price == 123.45
    assert session.polymarket is orderbook
    assert session.state.quotes["next-token"].bid == 0.51
    assert session.finished_record(status="ended")["history"][0]["target_price"] == 123.45


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


def test_history_preserves_missing_ask_price(tmp_path):
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
            "history": [{"t": index, "crypto_price": 100 + index}],
        }
        (saved_dir / f"record-{index:02d}.json").write_text(
            json.dumps(record),
            encoding="utf-8",
        )

    first_page = manager.list_saved_page("user-1", page=1, page_size=20)
    second_page = manager.list_saved_page("user-1", page=2, page_size=20)
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
    assert btc_page["total"] == 13
    assert [record["id"] for record in btc_page["records"][:2]] == [
        "record-24",
        "record-22",
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
