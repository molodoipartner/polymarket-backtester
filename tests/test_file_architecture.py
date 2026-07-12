from __future__ import annotations

import asyncio
import io
import json
import tarfile
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
import httpx
from fastapi.testclient import TestClient

import backend.api.app as app_module
import backend.api.routes as routes_module
import backend.live_monitor as live_monitor_module
from backend.auth import hash_password, verify_password
from backend.backtester import FileBacktester
from backend.fixed_time import TIMEFRAME_SECONDS, parse_period, parse_utc_datetime
from backend.storage import FileStore
from backend.strategy_generator import LiveMonitorStrategyGenerator, StrategyGroupGenerator
from config.settings import settings


def event(
    event_id: str,
    asset: str,
    minute: int,
    outcome: str,
    entry_up_price: float = 0.5,
    timeframe: str = "5m",
    entry_down_price: float | None = None,
) -> dict:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=minute)
    end = start + timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
    return {
        "id": event_id,
        "market_id": f"market-{event_id}",
        "question": f"{asset} contract {event_id}",
        "asset": asset,
        "timeframe": timeframe,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "entry_time": start.isoformat(),
        "expiration_time": end.isoformat(),
        "quote_time_up": (start - timedelta(seconds=1)).isoformat(),
        "quote_time_down": (start - timedelta(seconds=1)).isoformat(),
        "ask_up_price": entry_up_price,
        "ask_down_price": entry_down_price if entry_down_price is not None else 1 - entry_up_price,
        "resolved_outcome": outcome,
        "source": "test",
        "data_quality": {"is_synthetic": False, "quote_source": "test_clob"},
    }


def populated_store(tmp_path: Path) -> tuple[FileStore, dict]:
    store = FileStore(tmp_path / "server_data")
    store.save_fixed_events([
        event(str(index), "BTC", index * 5, "up" if index % 3 else "down", 0.43 + index % 4 * 0.04)
        for index in range(288)
    ])
    user = store.create_user("researcher@example.com", "Researcher", hash_password("secure-pass"))
    return store, user


def generate_group(store: FileStore, user: dict, name: str = "Research Pack", count: int = 10) -> dict:
    return StrategyGroupGenerator(store).generate(
        user["id"], name, ["BTC"], "5m", "2025-01-01", "2025-01-01", count, 10_000,
    )


def write_live_monitor_record(
    store: FileStore,
    user: dict,
    record_id: str,
    index: int,
    outcome: str = "Up",
    partial: bool = False,
    unsuitable: bool = False,
    target_price_ts: float | None = None,
    session_id: str | None = None,
) -> None:
    start_ts = 1782639600 + index * 300
    end_ts = start_ts + 300
    base_price = 100 + index
    history = []
    for second, yes_mid in ((10, 54), (60, 58), (120, 62)):
        yes_bid = yes_mid - 1
        yes_ask = yes_mid + 1
        history.append({
            "t": start_ts + second,
            "crypto_price": base_price + (0.08 if outcome == "Up" else -0.08),
            "target_price": base_price,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "yes_mid": yes_mid,
            "no_bid": 100 - yes_ask,
            "no_ask": 100 - yes_bid,
        })
    record = {
        "id": record_id,
        "session_id": session_id or "test-session",
        "question": f"Bitcoin Up or Down {index}",
        "crypto_symbol": "btc/usd",
        "slug": f"btc-updown-5m-{start_ts}",
        "condition_id": f"condition-{record_id}",
        "saved_at": datetime.fromtimestamp(end_ts, timezone.utc).isoformat(),
        "window_start_ts": start_ts,
        "window_end_ts": end_ts,
        "target_price": base_price,
        "target_price_ts": target_price_ts if target_price_ts is not None else start_ts,
        "final_crypto_price": base_price + (0.08 if outcome == "Up" else -0.08),
        "winning_outcome": outcome,
        "partial_contract": partial,
        "unsuitable_contract": unsuitable,
        "history_count": len(history),
        "history": history,
    }
    saved_dir = store.root / "live_monitor" / "users" / user["id"] / "saved_contracts"
    saved_dir.mkdir(parents=True, exist_ok=True)
    (saved_dir / f"{record_id}.json").write_text(json.dumps(record), encoding="utf-8")


def write_quality_live_monitor_record(
    store: FileStore,
    user: dict,
    record_id: str,
    index: int,
    first_offset: float = 0.0,
    missing_offsets: set[float] | None = None,
    target_price_ts: float | None = None,
) -> None:
    start_ts = 1782639600 + index * 300
    end_ts = start_ts + 300
    base_price = 100 + index
    missing_offsets = missing_offsets or set()
    history = []
    offset = first_offset
    while offset <= 300 + 1e-9:
        rounded_offset = round(offset, 3)
        if rounded_offset not in missing_offsets:
            history.append({
                "t": round(start_ts + rounded_offset, 3),
                "crypto_price": base_price + 0.08,
                "target_price": base_price,
                "target_price_ts": target_price_ts if target_price_ts is not None else start_ts,
                "yes_bid": 53,
                "yes_ask": 55,
                "yes_mid": 54,
                "no_bid": 45,
                "no_ask": 47,
            })
        offset = round(offset + 0.5, 3)
    record = {
        "id": record_id,
        "question": f"Bitcoin Quality {index}",
        "crypto_symbol": "btc/usd",
        "slug": f"btc-updown-5m-{start_ts}",
        "condition_id": f"condition-{record_id}",
        "saved_at": datetime.fromtimestamp(end_ts, timezone.utc).isoformat(),
        "window_start_ts": start_ts,
        "window_end_ts": end_ts,
        "target_price": base_price,
        "target_price_ts": target_price_ts if target_price_ts is not None else start_ts,
        "final_crypto_price": base_price + 0.08,
        "winning_outcome": "Up",
        "partial_contract": False,
        "unsuitable_contract": False,
        "history_count": len(history),
        "history": history,
    }
    saved_dir = store.root / "live_monitor" / "users" / user["id"] / "saved_contracts"
    saved_dir.mkdir(parents=True, exist_ok=True)
    (saved_dir / f"{record_id}.json").write_text(json.dumps(record), encoding="utf-8")


def test_passwords_users_and_initial_group_state_are_isolated(tmp_path: Path):
    store = FileStore(tmp_path / "server_data")
    encoded = hash_password("secure-pass")
    first = store.create_user("one@example.com", "One", encoded)
    second = store.create_user("two@example.com", "Two", hash_password("another-pass"))
    assert verify_password("secure-pass", encoded)
    assert not verify_password("wrong-pass", encoded)
    assert store.list_user_records(first["id"], "groups") == []
    assert store.list_user_records(second["id"], "groups") == []


def test_period_parser_is_utc_inclusive_and_rejects_reverse_ranges():
    start, end = parse_period("2025-01-01", "2025-01-02")
    assert start.isoformat() == "2025-01-01T00:00:00+00:00"
    assert end.isoformat() == "2025-01-03T00:00:00+00:00"
    with pytest.raises(ValueError, match="End date"):
        parse_period("2025-01-02", "2025-01-01")


def test_backtester_uses_parameterized_signal_without_lookahead():
    events = [
        event("btc-1", "BTC", 0, "up"),
        event("btc-2", "BTC", 5, "down"),
        event("btc-3", "BTC", 10, "up"),
        event("eth-1", "ETH", 0, "down"),
        event("eth-2", "ETH", 5, "up"),
        event("eth-3", "ETH", 10, "down"),
    ]
    specification = {
        "family": "history_momentum", "lookback": 1, "min_history": 1,
        "history_skip_recent": 1, "min_edge": 0,
        "allocation_pct": 0.1, "max_allocation_pct": 0.1,
        "fee_bps": 0, "exit_condition": "settlement",
    }
    result = FileBacktester().run(events, specification, 1_000)
    assert result["event_count"] == 6
    assert [trade["event_id"] for trade in result["transactions"]] == ["btc-3", "eth-3"]
    assert [trade["side"] for trade in result["transactions"]] == ["UP", "DOWN"]
    assert all(trade["analysis_history_count"] == 1 for trade in result["transactions"])


def test_backtester_locks_margin_buys_at_ask_and_settles_exactly_on_expiration():
    contract = event("15m", "BTC", 50, "up", 0.4, "15m", 0.62)
    prior_one = event("prior-1", "BTC", 20, "up", timeframe="15m")
    prior_two = event("prior-2", "BTC", 35, "down", timeframe="15m")
    specification = {
        "family": "history_momentum", "lookback": 1, "min_history": 1,
        "history_skip_recent": 1, "min_edge": 0,
        "allocation_pct": 0.1, "assumed_ask_price": 0.4,
        "max_allocation_pct": 0.1,
        "assumed_ask_price_cents": 40.0, "fee_bps": 0,
        "exit_condition": "settlement",
    }
    result = FileBacktester().run([prior_one, prior_two, contract], specification, 1_000)
    trade = result["transactions"][0]
    assert trade["opened_at"].endswith("00:50:00+00:00")
    assert trade["closed_at"].endswith("01:05:00+00:00")
    assert trade["holding_seconds"] == 900
    assert trade["ask_price"] == 0.4
    assert trade["assumed_ask_price_cents"] == 40
    assert trade["margin_locked"] == 100
    assert trade["contracts_volume"] == 250
    assert trade["available_cash_after_entry"] == 900
    assert trade["settlement_value"] == 250
    assert result["final_capital"] == 1150


def test_backtester_skips_positions_below_one_dollar_minimum():
    prior_one = event("prior-1", "BTC", 0, "up")
    prior_two = event("prior-2", "BTC", 5, "up")
    contract = event("contract", "BTC", 10, "up", 0.4)
    specification = {
        "family": "history_momentum", "lookback": 1, "min_history": 1,
        "history_skip_recent": 1, "min_edge": 0,
        "allocation_pct": 0.1, "assumed_ask_price": 0.4,
        "max_allocation_pct": 0.1, "fee_bps": 0, "exit_condition": "settlement",
    }
    result = FileBacktester().run([prior_one, prior_two, contract], specification, 5)
    assert result["transactions"] == []
    assert result["final_capital"] == 5


def test_backtester_rejects_misaligned_expiration():
    contract = event("bad", "BTC", 50, "up", 0.4, "15m")
    contract["expiration_time"] = (
        parse_utc_datetime(contract["start_time"]) + timedelta(minutes=25)
    ).isoformat()
    specification = {
        "family": "history_momentum", "lookback": 1, "min_history": 1,
        "history_skip_recent": 1, "min_edge": 0,
        "allocation_pct": 0.1, "fee_bps": 0, "exit_condition": "settlement",
    }
    with pytest.raises(ValueError, match="expiration"):
        FileBacktester().run([contract], specification, 1_000)


def test_backtester_reports_legacy_schema_without_key_error():
    contract = event("legacy", "BTC", 0, "up", 0.6)
    contract.pop("expiration_time")
    specification = {
        "family": "history_momentum", "lookback": 1, "min_history": 1,
        "history_skip_recent": 1, "min_edge": 0,
        "allocation_pct": 0.1, "fee_bps": 0, "exit_condition": "settlement",
    }
    with pytest.raises(ValueError, match="missing normalized fields.*expiration_time"):
        FileBacktester().run([contract], specification, 1_000)


def test_generator_creates_unique_diverse_strategies_and_nested_json(tmp_path: Path):
    store, user = populated_store(tmp_path)
    group = generate_group(store, user, count=20)
    strategies = store.list_user_records(user["id"], "strategies")
    assert group["strategy_count"] == 20
    assert len(strategies) == 20
    assert len({tuple(sorted(item["parameters"].items())) for item in strategies}) == 20
    assert len({item["family"] for item in strategies}) >= 4
    assert all(item["metrics"]["initial_capital"] == 10_000 for item in strategies)
    assert all(isinstance(item["equity_curve"], list) for item in strategies)

    reopened = FileStore(store.root)
    restored = reopened.get_user_record(user["id"], "groups", group["id"])
    restored_strategy = reopened.list_user_records(user["id"], "strategies")[0]
    assert isinstance(restored["period"], dict)
    assert isinstance(restored_strategy["parameters"], dict)
    assert isinstance(restored_strategy["metrics"]["roi"], float)
    assert restored["coverage"]["BTC"]["event_count"] == 288
    assert restored["coverage"]["BTC"]["largest_gap_seconds"] == 300
    assert restored["contracts_total"] == 288
    assert restored["contracts_available"] == 288
    assert restored["contracts_missing"] == 0
    assert "contracts_used" in restored_strategy["metrics"]
    assert restored_strategy["metrics"]["contracts_available"] == 288


def test_generator_supports_three_hundred_unique_configurations():
    specifications = StrategyGroupGenerator.specifications(300)
    assert StrategyGroupGenerator.MAX_STRATEGIES == 300
    assert len(specifications) == 300
    assert len({tuple(sorted(item.items())) for item in specifications}) == 300
    assert {item["family"] for item in specifications} >= {
        "history_momentum", "history_contrarian", "streak_momentum",
        "streak_reversal", "alternation_follow", "alternation_fade",
        "imbalance_follow", "imbalance_reversal", "lag_follow", "lag_fade",
        "double_follow", "double_fade",
    }
    assert all(item["history_skip_recent"] == 1 for item in specifications)
    assert max(item["lookback"] for item in specifications) >= 21
    assert max(item["min_edge"] for item in specifications) >= 0.2
    assert max(item["allocation_pct"] for item in specifications) >= 0.15
    assert {item["allocation_mode"] for item in specifications} >= {
        "fixed", "confidence_scaled", "streak_scaled", "depth_scaled",
    }


def test_live_monitor_smart_generator_samples_unique_dynamic_parameters():
    first = LiveMonitorStrategyGenerator.specifications(18, "smart")
    second = LiveMonitorStrategyGenerator.specifications(18, "smart")

    assert len(first) == 18
    assert len({tuple(sorted(item.items())) for item in first}) == 18
    assert {item["family"] for item in first} == {
        "smart_value_edge",
        "smart_momentum_edge",
        "smart_tight_spread_edge",
        "smart_acceleration_edge",
        "smart_breakout_confirmation",
        "smart_pullback_continuation",
        "smart_settlement_drift",
        "price_threshold_70_stop_40",
    }
    assert {item["generation_method"] for item in first} == {
        "system_random_parameter_sample"
    }
    assert {item["exit_condition"] for item in first} <= {
        "dynamic_bid_or_settlement",
        "conviction_hold_or_stop",
        "stop_loss_40_or_settlement",
    }
    assert {item["entry_mode"] for item in first} <= {"single_entry", "scale_in"}
    assert any(item["entry_mode"] == "scale_in" for item in first)
    assert any(item["target_hold_seconds"] < item["max_hold_seconds"] for item in first)
    assert any(item["time_exit_edge_cents"] != item["exit_edge_floor_cents"] for item in first)
    assert first != second


def test_live_monitor_smart_generator_supports_one_thousand_configurations():
    specifications = LiveMonitorStrategyGenerator.specifications(1000, "smart")
    assert LiveMonitorStrategyGenerator.MAX_STRATEGIES == 1000
    assert len(specifications) == 1000
    assert len({tuple(sorted(item.items())) for item in specifications}) == 1000


def test_live_monitor_smart_exit_can_leave_on_time_decay_before_settlement():
    generator = LiveMonitorStrategyGenerator(None, None)
    event_data = {
        "id": "time-exit",
        "market_id": "condition-time-exit",
        "question": "BTC dynamic exit",
        "asset": "BTC",
        "timeframe": "5m",
        "start_ts": 1_000.0,
        "end_ts": 1_300.0,
        "start_time": datetime.fromtimestamp(1_000, timezone.utc).isoformat(),
        "expiration_time": datetime.fromtimestamp(1_300, timezone.utc).isoformat(),
        "resolved_outcome": "up",
        "partial_contract": False,
        "history": [],
    }
    rows = [
        {
            "t": 1_010.0,
            "crypto_price": 100.0,
            "target_price": 100.0,
            "yes_bid": 49.0,
            "yes_ask": 51.0,
            "yes_mid": 50.0,
            "no_bid": 49.0,
            "no_ask": 51.0,
        },
        {
            "t": 1_080.0,
            "crypto_price": 100.0,
            "target_price": 100.0,
            "yes_bid": 50.0,
            "yes_ask": 52.0,
            "yes_mid": 51.0,
            "no_bid": 48.0,
            "no_ask": 50.0,
        },
    ]
    strategy = {
        "family": "smart_value_edge",
        "min_hold_seconds": 5.0,
        "target_hold_seconds": 30.0,
        "max_hold_seconds": 200.0,
        "exit_before_expiration_seconds": 5.0,
        "take_profit_cents": 50.0,
        "stop_loss_cents": 50.0,
        "exit_edge_floor_cents": -50.0,
        "time_exit_edge_cents": 10.0,
        "profit_lock_cents": 25.0,
        "trailing_stop_cents": 25.0,
        "momentum_lookback_seconds": 10.0,
        "slow_momentum_lookback_seconds": 30.0,
        "distance_scale_pct": 0.05,
        "momentum_scale_pct": 0.03,
        "prior_window": 3,
        "prior_weight": 0.0,
        "market_mid_weight": 0.0,
    }

    row, exit_type, probability, edge_cents = generator._smart_exit(
        event_data,
        rows,
        {"row": rows[0], "side": "up", "ask_price": 0.51},
        strategy,
        [],
    )

    assert row == rows[1]
    assert exit_type == "time_edge_decay_bid"
    assert probability is not None
    assert edge_cents is not None


def test_live_monitor_smart_scale_in_can_open_multiple_entries_in_one_contract():
    generator = LiveMonitorStrategyGenerator(None, None)
    event_data = {
        "id": "scale-in",
        "market_id": "condition-scale-in",
        "question": "BTC scale-in",
        "asset": "BTC",
        "timeframe": "5m",
        "start_ts": 1_000.0,
        "end_ts": 1_300.0,
        "start_time": datetime.fromtimestamp(1_000, timezone.utc).isoformat(),
        "expiration_time": datetime.fromtimestamp(1_300, timezone.utc).isoformat(),
        "resolved_outcome": "up",
        "partial_contract": False,
        "history": [],
    }
    rows = []
    for second, ask in ((10, 45.0), (40, 44.0), (70, 43.0), (120, 50.0)):
        rows.append({
            "t": 1_000.0 + second,
            "crypto_price": 100.1,
            "target_price": 100.0,
            "yes_bid": ask - 2,
            "yes_ask": ask,
            "yes_mid": ask - 1,
            "no_bid": 100 - ask,
            "no_ask": 102 - ask,
        })
    event_data["history"] = rows
    strategy = {
        "family": "smart_value_edge",
        "entry_second": 5.0,
        "entry_mode": "scale_in",
        "max_entries_per_contract": 3,
        "add_entry_min_gap_seconds": 20.0,
        "add_entry_edge_cents": 1.0,
        "add_entry_max_price_worse_cents": 2.0,
        "min_edge_cents": 1.0,
        "min_hold_seconds": 5.0,
        "target_hold_seconds": 40.0,
        "max_hold_seconds": 200.0,
        "exit_before_expiration_seconds": 5.0,
        "take_profit_cents": 50.0,
        "stop_loss_cents": 50.0,
        "exit_edge_floor_cents": -50.0,
        "time_exit_edge_cents": -50.0,
        "profit_lock_cents": 50.0,
        "trailing_stop_cents": 50.0,
        "momentum_lookback_seconds": 10.0,
        "slow_momentum_lookback_seconds": 30.0,
        "distance_scale_pct": 0.05,
        "momentum_scale_pct": 0.03,
        "prior_window": 3,
        "prior_weight": 0.0,
        "market_mid_weight": 0.0,
        "fee_bps": 10.0,
        "exit_condition": "conviction_hold_or_stop",
        "settlement_hold_edge_cents": 1.0,
    }

    trades, final_cash = generator._smart_trades(
        event_data, strategy, 1_000.0, 0.12, 0.001, []
    )

    assert len(trades) == 3
    assert final_cash > 1_000
    assert {trade["event_id"] for trade in trades} == {"scale-in"}
    assert [trade["scale_entry_index"] for trade in trades] == [1, 2, 3]
    assert {trade["scale_entry_count"] for trade in trades} == {3}
    assert {trade["exit_type"] for trade in trades} == {"settlement"}


def test_live_monitor_smart_conviction_hold_can_skip_take_profit_for_settlement():
    generator = LiveMonitorStrategyGenerator(None, None)
    event_data = {
        "start_ts": 1_000.0,
        "end_ts": 1_300.0,
        "resolved_outcome": "up",
    }
    rows = [
        {"t": 1_010.0, "crypto_price": 100.0, "target_price": 100.0, "yes_bid": 50.0, "yes_ask": 52.0, "yes_mid": 51.0},
        {"t": 1_050.0, "crypto_price": 100.0, "target_price": 100.0, "yes_bid": 70.0, "yes_ask": 72.0, "yes_mid": 71.0},
    ]
    strategy = {
        "family": "smart_value_edge",
        "min_edge_cents": 2.0,
        "min_hold_seconds": 5.0,
        "target_hold_seconds": 20.0,
        "max_hold_seconds": 200.0,
        "exit_before_expiration_seconds": 5.0,
        "take_profit_cents": 5.0,
        "stop_loss_cents": 50.0,
        "exit_edge_floor_cents": 99.0,
        "time_exit_edge_cents": 99.0,
        "profit_lock_cents": 1.0,
        "trailing_stop_cents": 1.0,
        "momentum_lookback_seconds": 10.0,
        "slow_momentum_lookback_seconds": 30.0,
        "distance_scale_pct": 0.05,
        "momentum_scale_pct": 0.03,
        "prior_window": 3,
        "prior_weight": 0.0,
        "market_mid_weight": 0.0,
        "exit_condition": "conviction_hold_or_stop",
        "settlement_hold_edge_cents": 2.0,
    }

    row, exit_type, probability, edge_cents = generator._smart_exit(
        event_data,
        rows,
        {"row": rows[0], "side": "up", "ask_price": 0.52, "edge_cents": 10.0},
        strategy,
        [],
    )

    assert row is None
    assert exit_type == "settlement"
    assert probability is None
    assert edge_cents is None


def test_backtester_cash_accounting_has_no_phantom_profit():
    contracts = [
        event("btc-1", "BTC", 0, "up", 0.5),
        event("btc-2", "BTC", 5, "down", 0.5),
        event("win", "BTC", 10, "up", 0.5),
        event("eth-1", "ETH", 0, "down", 0.5),
        event("eth-2", "ETH", 5, "up", 0.5),
        event("loss", "ETH", 10, "up", 0.5),
    ]
    specification = {
        "family": "history_momentum", "lookback": 1, "min_history": 1,
        "history_skip_recent": 1, "min_edge": 0,
        "allocation_pct": 0.2, "max_allocation_pct": 0.2,
        "fee_bps": 0, "exit_condition": "settlement",
    }
    result = FileBacktester().run(contracts, specification, 1_000)
    assert len(result["transactions"]) == 2
    assert sum(item["margin_locked"] for item in result["transactions"]) == 400
    assert {item["available_cash_after_entry"] for item in result["transactions"]} == {600, 800}
    assert sum(item["pnl"] for item in result["transactions"]) == 0
    assert result["final_capital"] == 1_000
    assert result["contracts_used"] == 2
    assert result["up_trade_count"] == 1
    assert result["down_trade_count"] == 1
    assert result["winning_trade_count"] == 1
    assert result["losing_trade_count"] == 1
    assert result["skipped_contract_count"] == 4
    assert result["average_allocation_pct"] == 20
    assert result["max_allocation_pct_used"] == 20


def test_backtester_final_capital_equals_initial_plus_realized_pnl_and_fees():
    contracts = [
        event(str(index), "BTC", index * 5, "up" if index % 2 else "down", 0.37)
        for index in range(12)
    ]
    specification = {
        "family": "history_momentum", "lookback": 1, "min_history": 1,
        "history_skip_recent": 1, "min_edge": 0,
        "allocation_pct": 0.2, "max_allocation_pct": 0.2,
        "fee_bps": 10, "exit_condition": "settlement",
    }
    result = FileBacktester().run(contracts, specification, 1_000)
    realized = sum(item["pnl"] for item in result["transactions"])
    assert result["final_capital"] == pytest.approx(1_000 + realized, abs=0.0001)
    assert all(item["available_cash_after_entry"] >= 0 for item in result["transactions"])
    assert all(item["entry_fee"] > 0 for item in result["transactions"])


def test_retest_preserves_parameters_and_saves_a_separate_result(tmp_path: Path):
    store, user = populated_store(tmp_path)
    original_group = generate_group(store, user, count=3)
    original_strategy = store.list_user_records(user["id"], "strategies")[1]
    result_group, result_strategy = StrategyGroupGenerator(store).retest(
        user["id"], original_group, original_strategy, "2025-01-01", "2025-01-01"
    )
    assert result_group["id"] != original_group["id"]
    assert result_group["retest_of_group_id"] == original_group["id"]
    assert result_strategy["parameters"] == original_strategy["parameters"]
    assert result_strategy["retest_of_strategy_id"] == original_strategy["id"]
    assert result_group["assets"] == original_group["assets"]
    assert result_group["timeframe"] == original_group["timeframe"]
    assert len(store.list_user_records(user["id"], "groups")) == 2


def test_favorite_strategy_persists_after_store_restart(tmp_path: Path):
    store, user = populated_store(tmp_path)
    group = generate_group(store, user, count=1)
    strategy = store.list_user_records(user["id"], "strategies")[0]
    store.update_user_record(user["id"], "strategies", strategy["id"], {"favorite": True})
    restored = FileStore(store.root).get_user_record(user["id"], "strategies", strategy["id"])
    assert restored["favorite"] is True
    assert restored["group_id"] == group["id"]


def test_transaction_pagination_returns_latest_twenty_without_loading_all(tmp_path: Path):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("pages@example.com", "Pages", hash_password("secure-pass"))
    for index in range(45):
        store.append_user_record(user["id"], "transactions", {
            "id": f"trade-{index}", "group_id": "group", "strategy_id": "strategy",
            "sequence": index,
        })
    first, total, page, summary = store.list_user_records_page(
        user["id"], "transactions", {"group_id": "group", "strategy_id": "strategy"}
    )
    last, _, last_page, _ = store.list_user_records_page(
        user["id"], "transactions", {"group_id": "group", "strategy_id": "strategy"},
        page=99,
    )
    assert total == 45
    assert page == 1
    assert summary == {"wins": 0, "losses": 0, "flat": 45, "up": 0, "down": 0}
    assert [item["sequence"] for item in first] == list(range(44, 24, -1))
    assert last_page == 3
    assert [item["sequence"] for item in last] == [4, 3, 2, 1, 0]


def test_recent_transaction_preview_scans_limited_file_tail(tmp_path: Path):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("preview@example.com", "Preview", hash_password("secure-pass"))
    for index in range(60):
        store.append_user_record(user["id"], "transactions", {
            "id": f"noise-{index}", "group_id": "noise", "strategy_id": "noise",
            "payload": "x" * 200,
        })
    for index in range(3):
        store.append_user_record(user["id"], "transactions", {
            "id": f"hit-{index}", "group_id": "group", "strategy_id": "strategy",
            "sequence": index,
        })
    rows = store.list_recent_user_records_preview(
        user["id"], "transactions", {"group_id": "group", "strategy_id": "strategy"},
        limit=2, max_scan_bytes=4096,
    )
    assert [item["sequence"] for item in rows] == [2, 1]


def test_strategy_detail_displays_assumed_ask_without_trade_snapshots(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    group = generate_group(store, user, count=1)
    strategy = store.list_user_records(user["id"], "strategies")[0]
    store.append_user_record(user["id"], "transactions", {
        "group_id": group["id"],
        "strategy_id": strategy["id"],
        "event_id": "legacy",
        "market_id": "legacy-market",
        "market_question": "Legacy contract",
        "asset": "BTC",
        "timeframe": "5m",
        "side": "UP",
        "resolved_outcome": "UP",
        "opened_at": "2025-01-01T00:00:00+00:00",
        "expiration_time": "2025-01-01T00:05:00+00:00",
        "closed_at": "2025-01-01T00:05:00+00:00",
        "ask_price": 0.5,
        "contracts_volume": 10,
        "margin_locked": 5,
        "pnl": 4.9,
        "balance_after": 104.9,
    })

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        response = client.get(f"/groups/{group['id']}/strategies/{strategy['id']}")

    assert response.status_code == 200
    assert "Assumed Ask" in response.text
    assert "UP trades" in response.text
    assert "DOWN trades" in response.text
    assert "Winning trades" in response.text


def test_stability_rewards_smooth_profitable_growth():
    smooth = FileBacktester.stability_metrics(
        [100, 110, 120, 130], 100, 130, 2.0, 0.0
    )
    volatile = FileBacktester.stability_metrics(
        [100, 125, 105, 130], 100, 130, 0.5, 16.0
    )
    losing = FileBacktester.stability_metrics(
        [100, 95, 90], 100, 90, -1.0, 10.0
    )
    assert 0 < volatile["stability_score"] < smooth["stability_score"] <= 100
    assert smooth["recovery_factor"] == 999
    assert losing["stability_score"] == 0


def test_generator_skips_a_gap_and_reports_coverage(tmp_path: Path):
    store, user = populated_store(tmp_path)
    events = [item for item in store.list_fixed_events() if item["id"] != "100"]
    store.save_fixed_events(events, replace_scope=(
        ["BTC"], "5m",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
    ))
    group = generate_group(store, user)
    coverage = group["coverage"]["BTC"]
    assert coverage["event_count"] == 287
    assert coverage["expected_event_count"] == 288
    assert coverage["missing_event_count"] == 1
    assert group["contracts_total"] == 288
    assert group["contracts_available"] == 287
    assert group["contracts_missing"] == 1
    assert coverage["coverage_percent"] == pytest.approx(99.6528)
    assert coverage["missing_ranges"] == [{
        "start_time": "2025-01-01T08:20:00+00:00",
        "resume_at": "2025-01-01T08:25:00+00:00",
        "missing_events": 1,
    }]


def test_generator_saves_flat_strategies_when_no_contracts_are_usable(tmp_path: Path):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("empty@example.com", "Empty", hash_password("secure-pass"))

    group = StrategyGroupGenerator(store).generate(
        user["id"], "Empty period", ["BTC"], "5m",
        "2025-01-01", "2025-01-01", 3, 100,
    )
    strategies = store.list_user_records_filtered(
        user["id"], "strategies", {"group_id": group["id"]}
    )

    assert group["event_count"] == 0
    assert group["summary"] == {
        "best_roi": 0.0,
        "average_roi": 0.0,
        "profitable_strategies": 0,
    }
    assert len(strategies) == 3
    assert all(item["metrics"]["trade_count"] == 0 for item in strategies)
    assert all(item["metrics"]["final_capital"] == 100 for item in strategies)


def test_backtester_rejects_shifted_entry_time():
    contract = event("future-quote", "BTC", 0, "up", 0.6)
    contract["entry_time"] = (
        parse_utc_datetime(contract["start_time"]) + timedelta(seconds=1)
    ).isoformat()
    specification = {
        "family": "history_momentum", "lookback": 1, "min_history": 1,
        "history_skip_recent": 1, "min_edge": 0,
        "allocation_pct": 0.1, "fee_bps": 0, "exit_condition": "settlement",
    }
    with pytest.raises(ValueError, match="entry must equal"):
        FileBacktester().run([contract], specification, 1_000)


def test_bundle_rejects_non_finite_nested_json(tmp_path: Path):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("json@example.com", "JSON", hash_password("secure-pass"))
    with pytest.raises(ValueError):
        store.save_group_bundle(user["id"], {"id": "g", "value": float("nan")}, [], [])
    assert store.list_user_records(user["id"], "groups") == []


def test_process_safe_store_preserves_concurrent_appends(tmp_path: Path):
    root = tmp_path / "server_data"
    first_store = FileStore(root)
    second_store = FileStore(root)
    user = first_store.create_user("parallel@example.com", "Parallel", hash_password("secure-pass"))

    def append(index: int) -> None:
        store = first_store if index % 2 else second_store
        store.log(user["id"], "parallel", {"index": index})

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(append, range(80)))
    logs = FileStore(root).list_user_records(user["id"], "logs")
    assert len([item for item in logs if item["event"] == "parallel"]) == 80


def test_delete_group_cascades_only_its_nested_records(tmp_path: Path):
    store, user = populated_store(tmp_path)
    first = generate_group(store, user, "First", 5)
    second = generate_group(store, user, "Second", 3)
    first_strategy = store.list_user_records_filtered(
        user["id"], "strategies", {"group_id": first["id"]}
    )[0]
    second_strategy = store.list_user_records_filtered(
        user["id"], "strategies", {"group_id": second["id"]}
    )[0]
    store.set_strategy_favorite(user["id"], first_strategy["id"], True)
    store.set_strategy_favorite(user["id"], second_strategy["id"], True)
    assert store.delete_group(user["id"], first["id"])
    assert [item["id"] for item in store.list_user_records(user["id"], "groups")] == [second["id"]]
    assert all(item["group_id"] == second["id"] for item in store.list_user_records(user["id"], "strategies"))
    assert all(item["group_id"] == second["id"] for item in store.list_user_records(user["id"], "transactions"))
    favorites = {
        item["id"]: item["favorite"] for item in store.list_user_records(user["id"], "strategies")
    }
    assert first_strategy["id"] not in favorites
    assert favorites[second_strategy["id"]] is True


def test_group_dashboard_filters_and_user_starts_with_zero_groups(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    group = generate_group(store, user, count=10)

    class IdleCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        response = client.get(f"/groups/{group['id']}?min_roi=1e100&min_win_rate=&min_trades=")
        assert response.status_code == 200
        assert "No strategies match these filters" in response.text
        assert "Family profitability" in response.text
        assert "Top trades" not in response.text
        assert "Phase 1" in response.text
        assert "Phase 2" in response.text
        assert "Phase 3" in response.text
        stable = client.get(
            f"/groups/{group['id']}?min_stability=101&sort=stability&direction=desc"
        )
        assert stable.status_code == 200
        assert "No strategies match these filters" in stable.text
        assert "Stability" in stable.text
        assert "Found contracts" in response.text
        assert "Missing contracts" in response.text
        new_group = client.get("/groups/new")
        assert new_group.status_code == 200
        assert "Group name" in new_group.text
        assert 'value="100.00"' in new_group.text
        strategy = max(
            store.list_user_records(user["id"], "strategies"),
            key=lambda item: (
                item["metrics"].get("winning_trade_count", 0),
                item["metrics"]["trade_count"],
            ),
        )
        assert strategy["metrics"]["trade_count"] > 20
        assert strategy["metrics"].get("winning_trade_count", 0) > 0
        detail = client.get(f"/groups/{group['id']}/strategies/{strategy['id']}")
        assert detail.status_code == 200
        assert "Equity curve and executions" in detail.text
        assert "Top trades" in detail.text
        assert "createEquityChart" in detail.text
        assert "Show advanced statistics and charts" in detail.text
        assert "position-table-wrap" in detail.text
        assert "Assumed Ask" in detail.text
        assert "Backtest window" in detail.text
        assert "Contracts total" in detail.text
        assert "Contracts found" in detail.text
        assert "Strategy used" in detail.text
        assert detail.text.count("<tr>") == 21
        assert "Page 1 of" in detail.text
        second_page = client.get(
            f"/groups/{group['id']}/strategies/{strategy['id']}?trade_page=2"
        )
        assert second_page.status_code == 200
        assert "Page 2 of" in second_page.text
        top_trades = client.get(
            f"/groups/{group['id']}/strategies/{strategy['id']}"
            "?show_top_trades=1&top_trade_filter=wins"
        )
        assert top_trades.status_code == 200
        assert "Winning only" in top_trades.text
        assert "loaded" in top_trades.text
        assert "$+" in top_trades.text


def test_group_creation_post_parses_typed_inputs_and_redirects_to_dashboard(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    collection_calls = []

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def collect(self, assets, timeframe, start_date, end_date):
            collection_calls.append((assets, timeframe, start_date, end_date))
            return {"events": 0, "assets": len(assets)}

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        response = client.post(
            "/groups",
            data={
                "group_name": "Crypto_Scalping_BTC",
                "starting_balance": "12500.50",
                "start_date": "2025-01-01",
                "end_date": "2025-01-01",
                "strategy_count": "7",
                "assumed_ask_price_cents": "42",
                "asset_symbols": "BTC",
                "timeframe": "5m",
            },
            follow_redirects=True,
        )
        assert response.status_code == 200
        assert "Crypto_Scalping_BTC" in response.text
        assert "7 generated" in response.text
    assert collection_calls == [(["BTC"], "5m", "2025-01-01", "2025-01-01")]
    group = store.list_user_records(user["id"], "groups")[0]
    assert group["starting_balance"] == 12500.5
    assert group["assumed_ask_price_cents"] == 42
    assert group["strategy_count"] == 7


def test_live_monitor_analysis_counts_and_generates_group(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    write_live_monitor_record(store, user, "full-up", 0, outcome="Up")
    write_live_monitor_record(store, user, "partial-down", 1, outcome="Down", partial=True)
    write_live_monitor_record(store, user, "unsuitable-up", 2, outcome="Up", unsuitable=True)

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        page = client.get("/live-monitor/analysis?asset_symbol=BTC&timeframe=5m")
        assert page.status_code == 200
        assert "Saved matching" in page.text
        assert "Loading saved contracts" in page.text
        assert 'data-live-analysis-metric="saved_matching">...</strong>' in page.text

        summary = client.get("/api/live-monitor/analysis/summary?asset_symbol=BTC&timeframe=5m")
        assert summary.status_code == 200
        assert summary.json()["summary"]["saved_matching"] == 3
        assert summary.json()["summary"]["selected_contracts"] == 1
        assert summary.json()["summary"]["selected_contract_ids"] == ["full-up"]
        assert summary.json()["summary"]["contracts_loaded"] is False
        assert summary.json()["summary"]["contracts"] == []
        detailed_summary = client.get(
            "/api/live-monitor/analysis/summary?asset_symbol=BTC&timeframe=5m&include_contracts=true"
        )
        assert detailed_summary.status_code == 200
        assert detailed_summary.json()["summary"]["contracts_loaded"] is True
        assert len(detailed_summary.json()["summary"]["contracts"]) == 1

        mixed_summary = client.get(
            "/api/live-monitor/analysis/summary?asset_symbol=BTC&timeframe=5m&include_partial_contracts=true"
        )
        assert mixed_summary.status_code == 200
        assert mixed_summary.json()["summary"]["selected_contracts"] == 2

        started = client.post(
            "/api/live-monitor/analysis/jobs",
            data={
                "group_name": "Live BTC checks",
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "include_partial_contracts": "true",
                "strategy_count": "4",
                "starting_balance": "500",
                "start_ts": "",
                "end_ts": "",
                "phase_1_contracts": "",
                "phase_2_contracts": "",
                "phase_3_contracts": "",
            },
        )
        assert started.status_code == 202
        stream = client.get(started.json()["events_url"])
        assert stream.status_code == 200
        assert '"stage": "validation"' in stream.text
        assert '"stage": "backtest"' in stream.text
        assert '"stage": "finalization"' in stream.text
        assert '"stage": "complete"' in stream.text

        group = store.list_user_records(user["id"], "groups")[-1]
        assert group["source"] == "live_monitor_saved_contracts"
        assert group["contracts_available"] == 2
        assert group["saved_contract_summary"]["unsuitable_contracts"] == 1
        group_page = client.get(f"/groups/{group['id']}")
        assert group_page.status_code == 200
        assert "Live BTC checks" in group_page.text
        assert "Entry +" in group_page.text
        strategies = store.list_user_records_filtered(
            user["id"], "strategies", {"group_id": group["id"]}
        )
        assert len(strategies) == 4
        write_live_monitor_record(store, user, "future-up", 3, outcome="Up")
        detail = client.get(f"/groups/{group['id']}/strategies/{strategies[0]['id']}")
        assert detail.status_code == 200
        assert "Entry Ask" in detail.text
        assert "Exit Price" in detail.text
        assert "Start using this strategy in live trading" in detail.text
        assert f"/groups/{group['id']}/strategies/{strategies[0]['id']}/paper-trading" in detail.text
        assert "Run live retest" in detail.text
        assert "data-live-retest-refresh" in detail.text
        assert "data-live-retest-loading" in detail.text
        assert "future-up" not in detail.text
        fresh_retest_summary = client.get(
            f"/api/groups/{group['id']}/strategies/{strategies[0]['id']}/live-retest/summary"
        )
        assert fresh_retest_summary.status_code == 200
        fresh_summary = fresh_retest_summary.json()["summary"]
        assert fresh_summary["selected_contracts"] == 3
        assert "future-up" in fresh_summary["selected_contract_ids"]
        assert "data-trade-contract-toggle" in detail.text
        trades = store.list_user_records_filtered(
            user["id"], "transactions", {"strategy_id": strategies[0]["id"]}
        )
        assert trades
        chart = client.get(
            f"/api/groups/{group['id']}/strategies/{strategies[0]['id']}/trades/{trades[0]['id']}/contract-chart"
        )
        assert chart.status_code == 200
        chart_payload = chart.json()
        assert chart_payload["record"]["id"] == trades[0]["saved_record_id"]
        assert chart_payload["record"]["history"]
        assert chart_payload["trade"]["entry_price_cents"] == round(trades[0]["ask_price"] * 100, 4)
        expected_axis = (
            round(100 - chart_payload["trade"]["entry_price_cents"], 4)
            if chart_payload["trade"]["side"] == "DOWN"
            else chart_payload["trade"]["entry_price_cents"]
        )
        assert chart_payload["trade"]["entry_yes_axis_cents"] == expected_axis
        assert chart_payload["trade"]["exit_price_cents"] in {0.0, 100.0}

        paper_page = client.get(
            f"/groups/{group['id']}/strategies/{strategies[0]['id']}/paper-trading"
        )
        assert paper_page.status_code == 200
        assert "Start trading" in paper_page.text
        assert "Starting balance" in paper_page.text
        assert 'name="real_trading"' in paper_page.text
        assert "Real trading" in paper_page.text
        assert "<th>Entry $</th>" in paper_page.text
        assert "<th>Balance</th>" in paper_page.text
        assert 'id="paper-trades-delete-all"' in paper_page.text
        assert "Contracts with paper trades" in paper_page.text
        assert f"/api/paper-trading/{group['id']}/strategies/{strategies[0]['id']}/sync" in paper_page.text
        run_group, run_strategy = routes_module._create_paper_run_group(
            store,
            user["id"],
            group,
            strategies[0],
            "paper-session",
            starting_balance=750,
        )
        assert run_group["starting_balance"] == 750
        write_live_monitor_record(
            store,
            user,
            "paper-first-skip",
            4,
            outcome="Up",
            session_id="paper-session",
        )
        write_live_monitor_record(
            store,
            user,
            "paper-live-up",
            5,
            outcome="Up",
            session_id="paper-session",
        )
        paper_id = "paper-live-up-paper-record"
        trade_id = "paper-live-up-live-trade"
        paper_start_ts = 1782639600 + 5 * 300
        paper_end_ts = paper_start_ts + 300
        paper_opened_at = datetime.fromtimestamp(paper_start_ts + 10, timezone.utc).isoformat()
        paper_closed_at = datetime.fromtimestamp(paper_end_ts, timezone.utc).isoformat()
        store.append_user_record(user["id"], "paper_trades", {
            "id": paper_id,
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "source_group_id": group["id"],
            "source_strategy_id": strategies[0]["id"],
            "session_id": "paper-session",
            "strategy_name": run_strategy["name"],
            "family": run_strategy.get("family"),
            "saved_record_id": "paper-live-up",
            "live_event_key": "paper-session:btc-updown-5m-1782641100:1782641100",
            "question": "Bitcoin Up or Down 5",
            "event_title": "Bitcoin Up or Down 5",
            "crypto_symbol": "btc/usd",
            "target_price": 105,
            "final_crypto_price": 105.08,
            "winning_outcome": "Up",
            "window_start_ts": paper_start_ts,
            "window_end_ts": paper_end_ts,
            "saved_at": paper_closed_at,
            "paper_mode": True,
            "trade_count": 1,
            "net_profit": 45,
            "roi": 9,
            "initial_capital": 500,
            "final_capital": 545,
            "history_count": 60,
        })
        store.append_user_record(user["id"], "transactions", {
            "id": trade_id,
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "paper_trade_id": paper_id,
            "paper_mode": True,
            "event_id": "paper-live-up",
            "market_id": "condition-paper-live-up",
            "market_question": "Bitcoin Up or Down 5",
            "asset": "BTC",
            "timeframe": "5m",
            "side": "UP",
            "resolved_outcome": "UP",
            "opened_at": paper_opened_at,
            "closed_at": paper_closed_at,
            "expiration_time": paper_closed_at,
            "entry_price": 0.55,
            "ask_price": 0.55,
            "exit_price": 1.0,
            "exit_type": "settlement",
            "contracts_volume": 100,
            "margin_locked": 55,
            "entry_fee": 0,
            "pnl": 45,
            "balance_before": 500,
            "balance_after": 545,
            "saved_record_id": "paper-live-up",
        })
        routes_module._refresh_paper_run_summary(store, user["id"], run_group, run_strategy)
        rebalanced_trade = store.get_user_record(user["id"], "transactions", trade_id)
        assert rebalanced_trade["balance_before"] == 750
        assert rebalanced_trade["balance_after"] == 795
        refreshed_run_strategy = store.get_user_record(user["id"], "strategies", run_strategy["id"])
        assert refreshed_run_strategy["metrics"]["initial_capital"] == 750
        assert refreshed_run_strategy["metrics"]["final_capital"] == 795
        synced = client.post(
            f"/api/paper-trading/{group['id']}/strategies/{strategies[0]['id']}/sync"
        )
        assert synced.status_code == 200
        assert synced.json()["disabled"] is False
        assert synced.json()["created"] == 0
        assert synced.json()["run_count"] == 1
        assert synced.json()["skipped"] >= 1
        assert synced.json()["skipped_first_contract"] == 1
        paper_list = client.get(
            f"/api/paper-trading/{group['id']}/strategies/{strategies[0]['id']}/contracts"
        )
        assert paper_list.status_code == 200
        paper_record = paper_list.json()["records"][0]
        assert paper_record["trade_count"] >= 1
        assert paper_record["group_id"] == run_group["id"]
        assert paper_record["strategy_id"] == run_strategy["id"]
        assert paper_record["saved_record_id"] == "paper-live-up"
        assert "contract_record" not in paper_record
        paper_detail = client.get(
            f"/api/paper-trading/{group['id']}/strategies/{strategies[0]['id']}/contracts/{paper_record['id']}"
        )
        assert paper_detail.status_code == 200
        paper_payload = paper_detail.json()["record"]
        assert paper_payload["trades"]
        assert paper_payload["history"]
        assert paper_payload["trades"][0]["balance_after"] == 795
        run_transactions = store.list_user_records_filtered(
            user["id"],
            "transactions",
            {"group_id": run_group["id"], "strategy_id": run_strategy["id"]},
        )
        assert run_transactions
        run_detail = client.get(f"/groups/{run_group['id']}/strategies/{run_strategy['id']}")
        assert run_detail.status_code == 200
        assert "data-trade-contract-toggle" in run_detail.text
        run_chart = client.get(
            f"/api/groups/{run_group['id']}/strategies/{run_strategy['id']}/trades/{run_transactions[0]['id']}/contract-chart"
        )
        assert run_chart.status_code == 200
        assert run_chart.json()["record"]["id"] == "paper-live-up"
        account = client.get("/account")
        assert account.status_code == 200
        assert "Paper trading run groups" in account.text
        assert run_group["name"] in account.text
        deleted_all_trades = client.delete(
            f"/api/paper-trading/{group['id']}/strategies/{strategies[0]['id']}/trades"
        )
        assert deleted_all_trades.status_code == 200
        assert deleted_all_trades.json()["removed_transactions"] >= 1
        assert deleted_all_trades.json()["removed_contracts"] >= 1
        synced_after_delete = client.post(
            f"/api/paper-trading/{group['id']}/strategies/{strategies[0]['id']}/sync"
        )
        assert synced_after_delete.status_code == 200
        assert synced_after_delete.json()["disabled"] is False
        assert synced_after_delete.json()["created"] == 0
        assert not store.list_user_records_filtered(
            user["id"],
            "transactions",
            {"group_id": run_group["id"], "strategy_id": run_strategy["id"]},
        )
        assert not store.list_user_records_filtered(
            user["id"],
            "paper_trades",
            {"group_id": run_group["id"], "strategy_id": run_strategy["id"]},
        )

        live_retest = client.post(
            f"/groups/{group['id']}/strategies/{strategies[0]['id']}/live-retest",
            data={
                "start_ts": "1782639600",
                "end_ts": "1782639900",
            },
            follow_redirects=True,
        )
        assert live_retest.status_code == 200
        assert "Retest:" in live_retest.text
        result_group = store.list_user_records(user["id"], "groups")[-1]
        assert result_group["retest_of_group_id"] == group["id"]
        assert result_group["event_count"] == 1
        result_strategy = store.list_user_records_filtered(
            user["id"], "strategies", {"group_id": result_group["id"]}
        )[0]
        assert result_strategy["metrics"]["trade_count"] == 1
        assert result_strategy["metrics"]["final_capital"] > result_group["starting_balance"]
        result_trades = store.list_user_records_filtered(
            user["id"], "transactions", {"strategy_id": result_strategy["id"]}
        )
        assert len(result_trades) == 1
        assert result_trades[0]["saved_record_id"] == "full-up"


def test_paper_trading_sync_matches_live_monitor_backtest(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    write_live_monitor_record(
        store,
        user,
        "paper-sync-first",
        0,
        outcome="Up",
        session_id="paper-sync-session",
    )
    write_live_monitor_record(
        store,
        user,
        "paper-sync-up",
        1,
        outcome="Up",
        session_id="paper-sync-session",
    )
    created_at = datetime.now(timezone.utc).isoformat()
    source_group = {
        "id": "source-paper-sync",
        "user_id": user["id"],
        "name": "Source paper sync",
        "assets": ["BTC"],
        "timeframe": "5m",
        "source": "live_monitor_saved_contracts",
        "starting_balance": 1000,
        "strategy_count": 1,
        "event_count": 1,
        "created_at": created_at,
    }
    source_strategy = {
        "id": "source-paper-sync-strategy",
        "group_id": source_group["id"],
        "name": "Quote momentum sync",
        "family": "quote_momentum",
        "parameters": {
            "family": "quote_momentum",
            "entry_second": 10,
            "quote_edge_cents": 2,
            "deviation_threshold_pct": 0.03,
            "allocation_pct": 0.1,
            "max_allocation_pct": 0.1,
            "fee_bps": 0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_ask",
        },
        "metrics": {"final_capital": 1000},
        "created_at": created_at,
    }
    store.save_group_bundle(user["id"], source_group, [source_strategy], [])
    run_group, run_strategy = routes_module._create_paper_run_group(
        store,
        user["id"],
        source_group,
        source_strategy,
        "paper-sync-session",
        starting_balance=1000,
    )

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    with TestClient(app_module.create_app()) as client:
        login = client.post(
            "/login",
            data={"email": user["email"], "password": "secure-pass"},
            follow_redirects=False,
        )
        assert login.status_code == 303
        synced = client.post(
            f"/api/paper-trading/{source_group['id']}/strategies/{source_strategy['id']}/sync"
        )
        assert synced.status_code == 200, synced.text
        assert synced.json()["disabled"] is False
        assert synced.json()["created"] == 1
        assert synced.json()["skipped_first_contract"] == 1

    saved = live_monitor_module.LiveMonitorManager(
        store.root / "live_monitor"
    ).get_saved(
        user["id"], "paper-sync-up"
    )
    generator = LiveMonitorStrategyGenerator(store, None)
    expected_event = generator._record_to_event(saved, "BTC", "5m")
    expected_result = generator._run(
        [expected_event],
        routes_module._paper_strategy_spec(source_strategy, run_strategy),
        1000,
    )
    expected_trade = expected_result["transactions"][0]
    paper_records = store.list_user_records_filtered(
        user["id"],
        "paper_trades",
        {
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "saved_record_id": "paper-sync-up",
        },
    )
    assert len(paper_records) == 1
    paper_trades = store.list_user_records_filtered(
        user["id"], "transactions", {"paper_trade_id": paper_records[0]["id"]}
    )
    assert len(paper_trades) == 1
    paper_trade = paper_trades[0]
    assert paper_trade["side"] == expected_trade["side"]
    assert paper_trade["opened_at"] == expected_trade["opened_at"]
    assert paper_trade["closed_at"] == expected_trade["closed_at"]
    assert paper_trade["ask_price"] == expected_trade["ask_price"]
    assert paper_trade["entry_price"] == expected_trade["entry_price"]
    assert paper_trade["exit_price"] == expected_trade["exit_price"]
    assert paper_trade["exit_type"] == expected_trade["exit_type"]
    assert paper_trade["pnl"] == expected_trade["pnl"]
    assert paper_records[0]["final_capital"] == pytest.approx(
        expected_result["final_capital"], abs=0.0001
    )


def test_live_monitor_analysis_filters_target_price_anomaly_contracts(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    write_live_monitor_record(store, user, "good-up", 0, outcome="Up", target_price_ts=1782639600)
    write_live_monitor_record(
        store,
        user,
        "late-target-up",
        1,
        outcome="Up",
        target_price_ts=1782639600 + 300 + 0.5,
    )
    write_live_monitor_record(
        store,
        user,
        "legacy-start-history-target",
        2,
        outcome="Up",
        target_price_ts=1782639600 + 600 + 120,
    )
    legacy_path = (
        store.root
        / "live_monitor"
        / "users"
        / user["id"]
        / "saved_contracts"
        / "legacy-start-history-target.json"
    )
    legacy_payload = json.loads(legacy_path.read_text(encoding="utf-8"))
    legacy_payload["history"].insert(0, {
        "t": 1782639600 + 600,
        "crypto_price": 102.004,
        "target_price": 102,
        "target_price_ts": 1782639600 + 600 + 120,
        "yes_bid": 53,
        "yes_ask": 55,
        "yes_mid": 54,
    })
    legacy_path.write_text(json.dumps(legacy_payload), encoding="utf-8")

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        summary = client.get("/api/live-monitor/analysis/summary?asset_symbol=BTC&timeframe=5m")
        assert summary.status_code == 200
        payload = summary.json()["summary"]
        assert payload["saved_matching"] == 3
        assert payload["target_price_anomaly_contracts"] == 1
        assert payload["available_contracts"] == 2
        assert payload["selected_contracts"] == 2
        assert payload["target_price_anomalies"][0]["id"] == "late-target-up"
        assert payload["target_price_anomalies"][0]["delay_seconds"] == 0.5
        assert payload["target_price_anomalies"][0]["target_price_time"]
        assert payload["target_price_anomalies"][0]["window_start_time"]

        started = client.post(
            "/api/live-monitor/analysis/jobs",
            data={
                "group_name": "Anomaly filtered BTC",
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "strategy_mode": "lightweight",
                "strategy_count": "2",
                "starting_balance": "500",
            },
        )
        assert started.status_code == 202
        assert '"stage": "complete"' in client.get(started.json()["events_url"]).text
        group = store.list_user_records(user["id"], "groups")[-1]
        assert group["contracts_missing"] == 1
        assert group["saved_contract_summary"]["target_price_anomaly_contracts"] == 1
        page = client.get(f"/groups/{group['id']}")
        assert page.status_code == 200
        assert "Filtered target-price anomalies" in page.text
        assert "data-saved-contract-toggle" in page.text
        assert "Delete all" in page.text
        assert "late-target-up" in page.text or "Bitcoin Up or Down 1" in page.text
        anomaly_detail = client.get("/api/live-monitor/saved/late-target-up")
        assert anomaly_detail.status_code == 200
        assert anomaly_detail.json()["record"]["target_price_anomaly"] is True
        deleted = client.post(
            f"/groups/{group['id']}/target-anomalies/delete",
            follow_redirects=False,
        )
        assert deleted.status_code == 303
        assert store.list_user_records(user["id"], "groups")[-1]["saved_contract_summary"][
            "target_price_anomaly_contracts"
        ] == 0
        assert client.get("/api/live-monitor/saved/late-target-up").status_code == 404
        cleaned_page = client.get(f"/groups/{group['id']}")
        assert "Filtered target-price anomalies" not in cleaned_page.text


def test_paper_trading_requires_target_price_at_window_start():
    on_time = {
        "window_start_ts": 1782639600,
        "target_price_ts": 1782639600,
        "target_price": 100.0,
    }
    late = {
        "window_start_ts": 1782639600,
        "target_price_ts": 1782639600.5,
        "target_price": 100.0,
    }
    fallback_history = [
        {"t": 1782639600, "target_price_ts": 1782639600, "target_price": 100.0}
    ]

    assert routes_module._paper_target_known_at_window_start(on_time)
    assert routes_module._paper_target_known_at_window_start(
        {"window_start_ts": 1782639600},
        fallback_history,
    )
    assert not routes_module._paper_target_known_at_window_start(late)


def test_paper_real_trading_executes_signal_once(tmp_path: Path, monkeypatch):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("real-paper@example.com", "Real Paper", hash_password("secure-pass"))
    source_group = {
        "id": "source-real-paper",
        "name": "Source Real Paper",
        "assets": ["BTC"],
        "asset_labels": ["Bitcoin"],
        "timeframe": "5m",
        "timeframe_label": "5m",
        "starting_balance": 100,
    }
    source_strategy = {
        "id": "source-real-paper-strategy",
        "group_id": source_group["id"],
        "name": "Quote momentum real paper",
        "family": "quote_momentum",
        "family_label": "Quote Momentum",
        "description": "Test strategy",
        "parameters": {
            "family": "quote_momentum",
            "entry_second": 10,
            "quote_edge_cents": 1,
            "allocation_pct": 0.1,
            "fee_bps": 0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_bid_ask",
        },
        "metrics": {"initial_capital": 100},
    }
    store.save_group_bundle(user["id"], source_group, [source_strategy], [])
    run_group, run_strategy = routes_module._create_paper_run_group(
        store,
        user["id"],
        source_group,
        source_strategy,
        "real-paper-session",
        SimpleNamespace(slug="previous", window_start_ts=1782639300),
        100,
        True,
    )

    class FakeExecutor:
        def __init__(self):
            self.calls = []
            self.sells = []
            self.balance_checks = []
            self.balances = {}

        def market_buy(self, order_request):
            self.calls.append(order_request)
            self.balances[order_request.token_id] = 1.8
            return {
                "success": True,
                "orderID": "fake-order",
                "_timing": {
                    "send_at": routes_module._paper_clock_ms(),
                    "duration_ms": 12.34,
                },
            }

        def market_sell(self, order_request):
            self.sells.append(order_request)
            current = self.balances.get(order_request.token_id, 0)
            self.balances[order_request.token_id] = max(0, current - order_request.amount)
            return {
                "success": True,
                "orderID": "fake-close-order",
                "_timing": {
                    "send_at": routes_module._paper_clock_ms(),
                    "duration_ms": 9.87,
                },
            }

        def token_balance(self, token_id):
            self.balance_checks.append(token_id)
            return self.balances.get(token_id, 0)

    executor = FakeExecutor()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                store=store,
                polymarket_live_order_executor=executor,
                live_monitor=SimpleNamespace(),
            )
        )
    )
    monkeypatch.setattr(settings, "polymarket_live_order_amount", 1.0)
    start_ts = 1782639600
    snapshot = {
        "event": {
            "slug": "btc-updown-5m-1782639600",
            "title": "Bitcoin Up or Down",
            "window_start_ts": start_ts,
            "window_end_ts": start_ts + 300,
            "tokens": [
                {"token_id": "up-token", "outcome": "Up", "condition_id": "condition"},
                {"token_id": "down-token", "outcome": "Down", "condition_id": "condition"},
            ],
        },
        "history": [
            {
                "t": start_ts + 10,
                "crypto_price": 100.1,
                "target_price": 100,
                "target_price_ts": start_ts,
                "yes_bid": 0.53,
                "yes_ask": 0.55,
                "yes_mid": 54,
                "no_bid": 0.45,
                "no_ask": 0.47,
            }
        ],
    }

    first = routes_module._paper_live_trade_preview(
        request,
        user["id"],
        source_group,
        source_strategy,
        run_group,
        run_strategy,
        snapshot,
    )
    second = routes_module._paper_live_trade_preview(
        request,
        user["id"],
        source_group,
        source_strategy,
        run_group,
        run_strategy,
        snapshot,
    )

    assert first["paper_trades"]
    assert first["real_orders"][0]["status"] == "submitted"
    assert first["real_orders"][0]["token_id"] == "up-token"
    assert second["real_orders"][0]["status"] == "submitted"
    assert len(executor.calls) == 1
    assert executor.calls[0].token_id == "up-token"
    assert executor.calls[0].amount == 1.0
    assert executor.calls[0].order_type == "FAK"
    assert executor.calls[0].price == 0.99
    assert executor.balance_checks == ["up-token"]
    real_orders = store.list_user_records(user["id"], "real_orders")
    assert len(real_orders) == 1
    assert real_orders[0]["response"]["orderID"] == "fake-order"
    assert real_orders[0]["real_open_quantity"] == pytest.approx(1.8)
    assert real_orders[0]["paper_signal_at"]
    assert real_orders[0]["clob_send_at"]
    assert real_orders[0]["clob_duration_ms"] == pytest.approx(12.34)
    assert routes_module._paper_real_order_retryable({
        "status": "failed",
        "error": "order couldn't be fully filled. FOK orders are fully filled or killed.",
    })
    assert routes_module._paper_real_order_retryable({
        "status": "failed",
        "error": "no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.",
    })
    assert routes_module._paper_real_order_retryable({
        "status": "failed",
        "error": "not enough balance / allowance: the balance is not enough -> balance: 1408448, order amount: 1420000",
    })

    closed_trade = {
        **first["paper_trades"][0],
        "status": "closed",
        "closed_at": datetime.fromtimestamp(start_ts + 40, timezone.utc).isoformat(),
        "exit_price": 0.58,
        "exit_bid_price": 0.58,
    }
    closed_once = routes_module._execute_paper_real_trading_signals(
        request,
        user["id"],
        run_group,
        run_strategy,
        "real-paper-session",
        snapshot["event"],
        snapshot,
        [closed_trade],
    )
    closed_twice = routes_module._execute_paper_real_trading_signals(
        request,
        user["id"],
        run_group,
        run_strategy,
        "real-paper-session",
        snapshot["event"],
        snapshot,
        [closed_trade],
    )

    assert closed_once[0]["status"] == "submitted"
    assert closed_once[0]["order_action"] == "close"
    assert closed_once[0]["amount"] == pytest.approx(1.8)
    assert closed_once[0]["close_quantity_source"] == "stored_open_balance"
    assert closed_once[0]["paper_signal_at"]
    assert closed_once[0]["clob_send_at"]
    assert closed_once[0]["clob_duration_ms"] == pytest.approx(9.87)
    assert closed_twice[0]["status"] == "submitted"
    assert len(executor.sells) == 1
    assert executor.sells[0].token_id == "up-token"
    assert executor.sells[0].amount == pytest.approx(1.8)
    assert executor.sells[0].order_type == "FAK"
    assert executor.sells[0].price == 0.01
    assert executor.balance_checks == ["up-token"]
    real_orders = store.list_user_records(user["id"], "real_orders")
    assert len(real_orders) == 2

    down_trade = {
        **first["paper_trades"][0],
        "side": "DOWN",
        "status": "open",
        "opened_at": datetime.fromtimestamp(start_ts + 55, timezone.utc).isoformat(),
        "closed_at": None,
        "entry_price": 0.47,
        "ask_price": 0.47,
    }
    down_open = routes_module._execute_paper_real_trading_signals(
        request,
        user["id"],
        run_group,
        run_strategy,
        "real-paper-session",
        snapshot["event"],
        snapshot,
        [down_trade],
    )
    assert down_open[0]["status"] == "submitted"
    assert down_open[0]["side"] == "DOWN"
    assert down_open[0]["token_id"] == "down-token"
    assert down_open[0]["limit_price"] == 0.99
    assert executor.calls[-1].token_id == "down-token"
    assert executor.calls[-1].price == 0.99
    assert executor.balance_checks[-1] == "down-token"

    down_closed_trade = {
        **down_trade,
        "status": "closed",
        "closed_at": datetime.fromtimestamp(start_ts + 80, timezone.utc).isoformat(),
        "exit_price": 0.43,
        "exit_bid_price": 0.43,
    }
    down_close = routes_module._execute_paper_real_trading_signals(
        request,
        user["id"],
        run_group,
        run_strategy,
        "real-paper-session",
        snapshot["event"],
        snapshot,
        [down_closed_trade],
    )
    assert down_close[0]["status"] == "submitted"
    assert down_close[0]["side"] == "DOWN"
    assert down_close[0]["token_id"] == "down-token"
    assert down_close[0]["limit_price"] == 0.01
    assert executor.sells[-1].token_id == "down-token"
    assert executor.sells[-1].price == 0.01


def test_paper_real_trading_close_retry_uses_clob_error_balance_without_lookup(tmp_path: Path):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("real-paper-close-retry@example.com", "Real Paper", hash_password("secure-pass"))
    run_group = {
        "id": "run-real-paper",
        "paper_trading": {
            "real_trading_enabled": True,
            "source_group_id": "source-real-paper",
            "source_strategy_id": "source-real-paper-strategy",
        },
    }
    run_strategy = {"id": "run-real-paper-strategy"}
    session_id = "real-paper-session"
    event_payload = {
        "slug": "btc-updown-5m-1782639600",
        "title": "Bitcoin Up or Down",
        "tokens": [
            {"token_id": "up-token", "outcome": "Up"},
            {"token_id": "down-token", "outcome": "Down"},
        ],
    }
    snapshot = {"event": event_payload}
    trade = {
        "side": "UP",
        "status": "closed",
        "opened_at": datetime.fromtimestamp(1782639610, timezone.utc).isoformat(),
        "closed_at": datetime.fromtimestamp(1782639640, timezone.utc).isoformat(),
        "scale_entry_index": 1,
        "exit_price": 0.58,
    }
    paper_id = routes_module.uuid.uuid5(
        routes_module.uuid.NAMESPACE_URL,
        f"paper-live:{run_strategy['id']}:{routes_module._paper_event_key(session_id, event_payload)}",
    ).hex
    open_order_id = routes_module._paper_real_order_id(paper_id, trade)
    close_order_id = routes_module._paper_real_close_order_id(paper_id, trade)
    store.append_user_record(
        user["id"],
        "real_orders",
        {
            "id": open_order_id,
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "status": "submitted",
            "token_id": "up-token",
            "real_open_quantity": 1.42,
        },
    )
    store.append_user_record(
        user["id"],
        "real_orders",
        {
            "id": close_order_id,
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "status": "failed",
            "order_action": "close",
            "error": "not enough balance / allowance: the balance is not enough -> balance: 1408448, order amount: 1420000",
        },
    )

    class FakeExecutor:
        def __init__(self):
            self.sells = []
            self.balance_checks = []

        def market_sell(self, order_request):
            self.sells.append(order_request)
            return {
                "success": True,
                "orderID": "retry-close-order",
                "_timing": {
                    "send_at": routes_module._paper_clock_ms(),
                    "duration_ms": 8.5,
                },
            }

        def token_balance(self, token_id):
            self.balance_checks.append(token_id)
            return 999

    executor = FakeExecutor()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                store=store,
                polymarket_live_order_executor=executor,
            )
        )
    )

    closed = routes_module._execute_paper_real_trading_signals(
        request,
        user["id"],
        run_group,
        run_strategy,
        session_id,
        event_payload,
        snapshot,
        [trade],
    )

    assert closed[0]["status"] == "submitted"
    assert closed[0]["amount"] == pytest.approx(1.408448)
    assert closed[0]["close_quantity_source"] == "previous_clob_error_balance"
    assert executor.sells[0].amount == pytest.approx(1.408448)
    assert executor.balance_checks == []


def test_polymarket_executor_extracts_balance_from_balance_allowance_response():
    executor_class = routes_module.PolymarketLiveOrderExecutor
    assert executor_class._balance_value_from_response({"balance": "1350000"}) == "1350000"
    assert executor_class._balance_value_from_response({"data": {"balance": "1350000"}}) == "1350000"
    assert executor_class._balance_value_from_response({"allowance": "0"}) is None


def test_paper_real_trading_open_saves_taking_amount_from_matched_order(tmp_path: Path, monkeypatch):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("real-paper-zero-balance@example.com", "Real Paper", hash_password("secure-pass"))
    source_group = {
        "id": "source-real-paper-zero",
        "name": "Source Real Paper",
        "assets": ["BTC"],
        "timeframe": "5m",
        "starting_balance": 100,
    }
    source_strategy = {
        "id": "source-real-paper-zero-strategy",
        "group_id": source_group["id"],
        "name": "Quote momentum real paper",
        "family": "quote_momentum",
        "parameters": {
            "family": "quote_momentum",
            "entry_second": 10,
            "quote_edge_cents": 1,
            "allocation_pct": 0.1,
            "fee_bps": 0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_bid_ask",
        },
        "metrics": {"initial_capital": 100},
    }
    store.save_group_bundle(user["id"], source_group, [source_strategy], [])
    run_group, run_strategy = routes_module._create_paper_run_group(
        store,
        user["id"],
        source_group,
        source_strategy,
        "real-paper-zero-session",
        SimpleNamespace(slug="previous", window_start_ts=1782639300),
        100,
        True,
    )

    class FakeExecutor:
        def __init__(self):
            self.buys = []
            self.sells = []
            self.balance_checks = []

        def market_buy(self, order_request):
            self.buys.append(order_request)
            return {
                "success": True,
                "orderID": "zero-balance-open",
                "status": "matched",
                "makingAmount": "0.999999",
                "takingAmount": "1.42857",
                "_timing": {
                    "send_at": routes_module._paper_clock_ms(),
                    "duration_ms": 7.5,
                    "signed_token_quantity": 1.010101,
                },
            }

        def market_sell(self, order_request):
            self.sells.append(order_request)
            return {
                "success": True,
                "orderID": "zero-balance-close",
                "_timing": {
                    "send_at": routes_module._paper_clock_ms(),
                    "duration_ms": 6.5,
                },
            }

        def token_balance_info(self, token_id):
            self.balance_checks.append(token_id)
            return {
                "balance": 0,
                "raw_balance": "0",
                "response": {"balance": "0"},
                "source": "get_balance_allowance",
            }

    executor = FakeExecutor()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                store=store,
                polymarket_live_order_executor=executor,
                live_monitor=SimpleNamespace(),
            )
        )
    )
    monkeypatch.setattr(settings, "polymarket_live_order_amount", 1.0)
    start_ts = 1782639600
    snapshot = {
        "event": {
            "slug": "btc-updown-5m-1782639600",
            "title": "Bitcoin Up or Down",
            "window_start_ts": start_ts,
            "window_end_ts": start_ts + 300,
            "tokens": [
                {"token_id": "up-token", "outcome": "Up"},
                {"token_id": "down-token", "outcome": "Down"},
            ],
        },
        "history": [
            {
                "t": start_ts + 10,
                "crypto_price": 99.9,
                "target_price": 100,
                "target_price_ts": start_ts,
                "yes_bid": 0.45,
                "yes_ask": 0.47,
                "yes_mid": 46,
                "no_bid": 0.53,
                "no_ask": 0.55,
            }
        ],
    }

    opened = routes_module._paper_live_trade_preview(
        request,
        user["id"],
        source_group,
        source_strategy,
        run_group,
        run_strategy,
        snapshot,
    )
    closed_trade = {
        **opened["paper_trades"][0],
        "status": "closed",
        "closed_at": datetime.fromtimestamp(start_ts + 40, timezone.utc).isoformat(),
        "exit_price": 0.5,
        "exit_bid_price": 0.5,
    }
    closed = routes_module._execute_paper_real_trading_signals(
        request,
        user["id"],
        run_group,
        run_strategy,
        "real-paper-zero-session",
        snapshot["event"],
        snapshot,
        [closed_trade],
    )

    assert opened["real_orders"][0]["status"] == "submitted"
    assert opened["real_orders"][0]["real_open_balance_after_open"] == 0
    assert opened["real_orders"][0]["real_open_quantity"] == pytest.approx(1.42857)
    assert opened["real_orders"][0]["real_open_quantity_source"] == "order_response_taking_amount"
    assert opened["real_orders"][0]["real_open_avg_price"] == pytest.approx(0.7, abs=0.001)
    assert closed[0]["status"] == "submitted"
    assert closed[0]["amount"] == pytest.approx(1.42857)
    assert executor.sells[0].amount == pytest.approx(1.42857)
    assert executor.balance_checks == ["down-token"]


def test_paper_real_trading_close_old_zero_quantity_open_does_not_sell_estimate(tmp_path: Path):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("real-paper-old-zero@example.com", "Real Paper", hash_password("secure-pass"))
    run_group = {
        "id": "run-real-paper",
        "paper_trading": {
            "real_trading_enabled": True,
            "source_group_id": "source-real-paper",
            "source_strategy_id": "source-real-paper-strategy",
        },
    }
    run_strategy = {"id": "run-real-paper-strategy"}
    session_id = "real-paper-session"
    event_payload = {
        "slug": "btc-updown-5m-1782639600",
        "title": "Bitcoin Up or Down",
        "tokens": [
            {"token_id": "up-token", "outcome": "Up"},
            {"token_id": "down-token", "outcome": "Down"},
        ],
    }
    snapshot = {"event": event_payload}
    trade = {
        "side": "DOWN",
        "status": "closed",
        "opened_at": datetime.fromtimestamp(1782639610, timezone.utc).isoformat(),
        "closed_at": datetime.fromtimestamp(1782639640, timezone.utc).isoformat(),
        "scale_entry_index": 1,
        "exit_price": 0.5,
    }
    paper_id = routes_module.uuid.uuid5(
        routes_module.uuid.NAMESPACE_URL,
        f"paper-live:{run_strategy['id']}:{routes_module._paper_event_key(session_id, event_payload)}",
    ).hex
    open_order_id = routes_module._paper_real_order_id(paper_id, trade)
    store.append_user_record(
        user["id"],
        "real_orders",
        {
            "id": open_order_id,
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "status": "submitted",
            "side": "DOWN",
            "token_id": "down-token",
            "amount": 1.0,
            "limit_price": 0.99,
            "real_open_quantity": 0,
            "response": {"success": True, "orderID": "old-open-without-signed-quantity"},
        },
    )

    class FakeExecutor:
        def __init__(self):
            self.sells = []
            self.balance_checks = []

        def market_sell(self, order_request):
            self.sells.append(order_request)
            return {
                "success": True,
                "orderID": "old-zero-close",
                "_timing": {
                    "send_at": routes_module._paper_clock_ms(),
                    "duration_ms": 6.5,
                },
            }

        def token_balance(self, token_id):
            self.balance_checks.append(token_id)
            return 999

    executor = FakeExecutor()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                store=store,
                polymarket_live_order_executor=executor,
            )
        )
    )

    closed = routes_module._execute_paper_real_trading_signals(
        request,
        user["id"],
        run_group,
        run_strategy,
        session_id,
        event_payload,
        snapshot,
        [trade],
    )

    assert closed[0]["status"] == "failed"
    assert closed[0]["amount"] is None
    assert "Could not resolve quantity to close" in closed[0]["error"]
    assert executor.sells == []
    assert executor.balance_checks == []


def test_manual_paper_trade_open_and_close_updates_same_transaction(tmp_path: Path, monkeypatch):
    store = FileStore(tmp_path / "server_data")
    user = store.create_user("manual-paper@example.com", "Manual Paper", hash_password("secure-pass"))
    source_group = {
        "id": "source-manual-paper",
        "name": "Source Manual Paper",
        "assets": ["BTC"],
        "asset_labels": ["Bitcoin"],
        "timeframe": "5m",
        "timeframe_label": "5m",
        "starting_balance": 100,
    }
    source_strategy = {
        "id": "source-manual-paper-strategy",
        "group_id": source_group["id"],
        "name": "Manual paper strategy",
        "family": "quote_momentum",
        "family_label": "Quote Momentum",
        "description": "Test strategy",
        "parameters": {
            "family": "quote_momentum",
            "entry_second": 10,
            "quote_edge_cents": 1,
            "allocation_pct": 0.1,
            "fee_bps": 0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_bid_ask",
        },
        "metrics": {"initial_capital": 100},
    }
    store.save_group_bundle(user["id"], source_group, [source_strategy], [])
    run_group, run_strategy = routes_module._create_paper_run_group(
        store,
        user["id"],
        source_group,
        source_strategy,
        "manual-paper-session",
        SimpleNamespace(slug="previous", window_start_ts=1782639300),
        100,
        True,
    )

    class FakeExecutor:
        def __init__(self):
            self.buys = []
            self.sells = []
            self.balance_checks = []
            self.balances = {}

        def market_buy(self, order_request):
            self.buys.append(order_request)
            self.balances[order_request.token_id] = 1.4
            return {"success": True, "orderID": "manual-buy"}

        def market_sell(self, order_request):
            self.sells.append(order_request)
            current = self.balances.get(order_request.token_id, 0)
            self.balances[order_request.token_id] = max(0, current - order_request.amount)
            return {"success": True, "orderID": "manual-sell"}

        def token_balance(self, token_id):
            self.balance_checks.append(token_id)
            return self.balances.get(token_id, 0)

    executor = FakeExecutor()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                store=store,
                polymarket_live_order_executor=executor,
                live_monitor=SimpleNamespace(),
            )
        )
    )
    monkeypatch.setattr(settings, "polymarket_live_order_amount", 1.0)
    start_ts = 1782639600
    snapshot = {
        "event": {
            "slug": "btc-updown-5m-1782639600",
            "title": "Bitcoin Up or Down",
            "window_start_ts": start_ts,
            "window_end_ts": start_ts + 300,
            "tokens": [
                {"token_id": "up-token", "outcome": "Up", "condition_id": "condition"},
                {"token_id": "down-token", "outcome": "Down", "condition_id": "condition"},
            ],
        },
        "quotes": [
            {"token_id": "up-token", "outcome": "Up", "bid": 0.48, "ask": 0.5},
            {"token_id": "down-token", "outcome": "Down", "bid": 0.49, "ask": 0.52},
        ],
        "history": [
            {
                "t": start_ts + 20,
                "crypto_price": 100.1,
                "target_price": 100,
                "target_price_ts": start_ts,
                "yes_bid": 48,
                "yes_ask": 50,
                "yes_mid": 49,
                "no_bid": 49,
                "no_ask": 52,
            }
        ],
    }

    opened = routes_module._paper_manual_open_transaction(
        request,
        user["id"],
        source_group,
        source_strategy,
        run_group,
        run_strategy,
        "manual-paper-session",
        snapshot,
        "UP",
    )
    closed = routes_module._paper_manual_close_transaction(
        request,
        user["id"],
        run_group,
        run_strategy,
        snapshot,
    )

    assert opened["status"] == "open"
    assert closed["id"] == opened["id"]
    assert closed["status"] == "closed"
    assert closed["exit_type"] == "manual_market"
    assert closed["exit_price"] == 0.48
    assert closed["closed_quantity"] == pytest.approx(1.4)
    assert closed["actual_token_balance_before_close"] == pytest.approx(1.4)
    assert closed["closed_quantity_source"] == "stored_open_balance"
    assert closed["pnl"] == pytest.approx(-0.328)
    assert len(executor.buys) == 1
    assert len(executor.sells) == 1
    assert executor.buys[0].order_type == "FAK"
    assert executor.sells[0].order_type == "FAK"
    assert executor.buys[0].price == 0.99
    assert executor.sells[0].price == 0.01
    assert executor.balance_checks == ["up-token"]
    assert executor.sells[0].amount == pytest.approx(1.4)
    paper_records = store.list_user_records(user["id"], "paper_trades")
    assert len(paper_records) == 1
    assert paper_records[0]["trade_count"] == 1
    assert store.get_user_record(user["id"], "transactions", opened["id"])["closed_at"]


def test_live_monitor_quality_scan_finds_target_and_history_issues(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "quality-good", 0, first_offset=0.0)
    write_quality_live_monitor_record(store, user, "quality-start-late", 1, first_offset=6.0)
    write_quality_live_monitor_record(
        store,
        user,
        "quality-gap",
        2,
        first_offset=2.0,
        missing_offsets={10.0},
    )
    write_quality_live_monitor_record(
        store,
        user,
        "quality-target-late",
        3,
        first_offset=0.5,
        target_price_ts=1782639600 + 900 + 0.5,
    )

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)

    async def fake_gamma_payload(_client, slug: str):
        start_ts = int(slug.rsplit("-", 1)[-1])
        index = (start_ts - 1782639600) // 300
        return [{"eventMetadata": {"priceToBeat": 100 + index}}]

    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        page = client.get("/live-monitor/quality")
        assert page.status_code == 200
        assert "Scan saved contracts" in page.text
        assert "data-quality-progress" in page.text
        assert "quality-start-late" not in page.text

        scanned = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert scanned.status_code == 200
        assert "Problem contracts" in scanned.text
        assert "Gamma target comparison" in scanned.text
        assert "Problem density" in scanned.text
        assert "quality-good" not in scanned.text
        assert "quality-start-late" in scanned.text
        assert "quality-gap" in scanned.text
        assert "quality-target-late" in scanned.text
        assert "History starts more than 5.0s" in scanned.text
        assert "History has missing 0.5s samples" in scanned.text
        assert "Target price locked 0.5s" in scanned.text
        assert "data-saved-contract-toggle" in scanned.text

        deleted = client.post(
            "/live-monitor/quality/delete-problems",
            data={"asset_symbol": "BTC", "timeframe": "5m"},
            follow_redirects=False,
        )
        assert deleted.status_code == 303
        assert "scan=1" not in deleted.headers["location"]
        remaining = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert remaining.status_code == 200
        assert "quality-good" not in remaining.text
        assert "quality-start-late" not in remaining.text
        assert "quality-gap" not in remaining.text
        assert "quality-target-late" not in remaining.text
        assert client.get("/api/live-monitor/saved/quality-good").status_code == 200
        assert client.get("/api/live-monitor/saved/quality-gap").status_code == 404


def test_live_monitor_quality_gamma_target_check_lists_mismatches(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-target-good", 0)
    write_quality_live_monitor_record(store, user, "gamma-target-bad", 1)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        if slug.endswith(str(1782639600)):
            return [{"eventMetadata": {"priceToBeat": 100.004}}]
        return [{"eventMetadata": {"priceToBeat": 109.99}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Gamma target comparison" in checked.text
        assert "Target mismatches" in checked.text
        assert "gamma-target-bad" in checked.text
        assert "109.99" in checked.text
        assert "101" in checked.text
        assert "Delete all problems" in checked.text
        assert 'name="problem_ids" value="gamma-target-bad"' in checked.text
        assert len(gamma_calls) == 2

        deleted = client.post(
            "/live-monitor/quality/delete-problems",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "problem_ids": "gamma-target-bad",
            },
        )
        assert deleted.status_code == 200
        assert len(gamma_calls) == 2
        assert client.get("/api/live-monitor/saved/gamma-target-good").status_code == 200
        assert client.get("/api/live-monitor/saved/gamma-target-bad").status_code == 404


def test_live_monitor_quality_parses_string_gamma_metadata(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-string-metadata", 0)

    async def fake_gamma_payload(_client, _slug: str):
        return [{
            "eventMetadata": json.dumps({"priceToBeat": 100.004}),
            "markets": json.dumps([{
                "outcomes": json.dumps(["Up", "Down"]),
                "outcomePrices": json.dumps(["1", "0"]),
            }]),
        }]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Gamma response did not include priceToBeat." not in checked.text
        assert "gamma-string-metadata" not in checked.text


def test_live_monitor_quality_can_delete_single_listed_contract(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "single-delete-keep", 0)
    write_quality_live_monitor_record(
        store,
        user,
        "single-delete-remove",
        1,
        first_offset=6.0,
    )

    async def fake_gamma_payload(_client, slug: str):
        start_ts = int(slug.rsplit("-", 1)[-1])
        index = (start_ts - 1782639600) // 300
        return [{"eventMetadata": {"priceToBeat": 100 + index}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert 'name="single_record_id" value="single-delete-remove"' in checked.text
        assert 'name="contract_limit" value="250"' in checked.text
        assert "Delete this saved live-monitor contract?" in checked.text

        deleted = client.post(
            "/live-monitor/quality/delete-problems",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "single_record_id": "single-delete-remove",
            },
            follow_redirects=False,
        )
        assert deleted.status_code == 303
        assert "scan=1" not in deleted.headers["location"]
        assert client.get("/api/live-monitor/saved/single-delete-keep").status_code == 200
        assert client.get("/api/live-monitor/saved/single-delete-remove").status_code == 404


def test_live_monitor_quality_contract_limit_checks_latest_records_only(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    for index in range(11):
        write_quality_live_monitor_record(store, user, f"limited-{index}", index)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        start_ts = int(slug.rsplit("-", 1)[-1])
        index = (start_ts - 1782639600) // 300
        return [{"eventMetadata": {"priceToBeat": 100 + index}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get(
            "/live-monitor/quality?asset_symbol=BTC&timeframe=5m&contract_limit=10&scan=1"
        )
        assert checked.status_code == 200
        assert "Last 10" in checked.text
        assert "limited-0" not in checked.text
        assert len(gamma_calls) == 10
        assert f"btc-updown-5m-{1782639600}" not in gamma_calls
        assert f"btc-updown-5m-{1782639600 + 10 * 300}" in gamma_calls


def test_live_monitor_quality_progress_stream_returns_cached_report(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "progress-target-bad", 0)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        return [{"eventMetadata": {"priceToBeat": 109.99}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        stream = client.get(
            "/live-monitor/quality/progress?asset_symbol=BTC&timeframe=5m&contract_limit=10"
        )
        assert stream.status_code == 200
        assert stream.headers["content-type"].startswith("text/event-stream")
        events = [
            json.loads(line.removeprefix("data: "))
            for line in stream.text.splitlines()
            if line.startswith("data: ")
        ]
        local_events = [event for event in events if event.get("stage") == "local"]
        assert any(event.get("done") == 1 for event in local_events)
        assert any(event.get("remaining") == 0 for event in local_events)
        gamma_events = [event for event in events if event.get("stage") == "gamma"]
        assert any(event.get("done") == 1 for event in gamma_events)
        assert any(event.get("remaining") == 0 for event in gamma_events)
        complete = next(event for event in events if event.get("stage") == "complete")
        assert complete["report_url"].startswith("/live-monitor/quality?")
        assert len(gamma_calls) == 1

        report_page = client.get(complete["report_url"])
        assert report_page.status_code == 200
        assert "Gamma target mismatch." in report_page.text
        assert "progress-target-bad" in report_page.text
        assert len(gamma_calls) == 1


def test_live_monitor_quality_gamma_outcome_check_lists_mismatches(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-outcome-good", 0)
    write_quality_live_monitor_record(store, user, "gamma-outcome-bad", 1)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        start_ts = int(slug.rsplit("-", 1)[-1])
        index = (start_ts - 1782639600) // 300
        outcome_prices = ["1", "0"] if index == 0 else ["0", "1"]
        return [{
            "eventMetadata": {"priceToBeat": 100 + index},
            "markets": [{
                "outcomes": json.dumps(["Up", "Down"]),
                "outcomePrices": json.dumps(outcome_prices),
            }],
        }]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Outcome mismatches" in checked.text
        assert "Gamma outcome mismatch." in checked.text
        assert "saved UP, Gamma DOWN" in checked.text
        assert "gamma-outcome-good" not in checked.text
        assert "gamma-outcome-bad" in checked.text
        assert 'name="problem_ids" value="gamma-outcome-bad"' in checked.text
        assert len(gamma_calls) == 2

        deleted = client.post(
            "/live-monitor/quality/delete-problems",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "problem_ids": "gamma-outcome-bad",
            },
        )
        assert deleted.status_code == 200
        assert len(gamma_calls) == 2
        assert client.get("/api/live-monitor/saved/gamma-outcome-good").status_code == 200
        assert client.get("/api/live-monitor/saved/gamma-outcome-bad").status_code == 404


def test_live_monitor_quality_updates_only_gamma_outcome_mismatches(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-outcome-fix", 1)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        return [{
            "eventMetadata": {"priceToBeat": 101},
            "markets": [{
                "outcomes": json.dumps(["Up", "Down"]),
                "outcomePrices": json.dumps(["0", "1"]),
            }],
        }]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Update Gamma outcomes" in checked.text
        assert "Gamma outcome mismatch." in checked.text
        assert 'name="outcome_updates" value="gamma-outcome-fix|DOWN"' in checked.text
        assert len(gamma_calls) == 1

        updated = client.post(
            "/live-monitor/quality/update-gamma-outcomes",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "outcome_updates": "gamma-outcome-fix|DOWN",
            },
            follow_redirects=False,
        )
        assert updated.status_code == 303
        assert "scan=1" not in updated.headers["location"]
        assert len(gamma_calls) == 1
        record = client.get("/api/live-monitor/saved/gamma-outcome-fix")
        assert record.status_code == 200
        payload = record.json()
        assert payload["record"]["winning_outcome"] == "Down"
        assert payload["record"]["target_price"] == 101
        assert payload["record"]["history"][0]["target_price"] == 101

        rescanned = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert rescanned.status_code == 200
        assert "Gamma outcome mismatch." not in rescanned.text
        assert client.get("/api/live-monitor/saved/gamma-outcome-fix").status_code == 200


def test_live_monitor_quality_updates_gamma_target_mismatches_without_refetch(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-target-fix", 1)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        return [{"eventMetadata": {"priceToBeat": 109.99}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Update Gamma targets" in checked.text
        assert "Gamma target mismatch." in checked.text
        assert 'name="target_updates" value="gamma-target-fix|109.99"' in checked.text
        assert len(gamma_calls) == 1

        updated = client.post(
            "/live-monitor/quality/update-gamma-targets",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "target_updates": "gamma-target-fix|109.99",
            },
            follow_redirects=False,
        )
        assert updated.status_code == 303
        assert "scan=1" not in updated.headers["location"]
        assert len(gamma_calls) == 1
        record = client.get("/api/live-monitor/saved/gamma-target-fix")
        assert record.status_code == 200
        payload = record.json()
        assert payload["record"]["target_price"] == 109.99
        assert payload["record"]["history"][0]["target_price"] == 109.99
        assert payload["record"]["winning_outcome"] == "Up"

        rescanned = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert rescanned.status_code == 200
        assert "Gamma target mismatch." not in rescanned.text


def test_live_monitor_quality_updates_target_and_outcome_for_same_contract_without_refetch(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-both-fix", 1)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        return [{
            "eventMetadata": {"priceToBeat": 109.99},
            "markets": [{
                "outcomes": json.dumps(["Up", "Down"]),
                "outcomePrices": json.dumps(["0", "1"]),
            }],
        }]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert 'name="target_updates" value="gamma-both-fix|109.99"' in checked.text
        assert 'name="outcome_updates" value="gamma-both-fix|DOWN"' in checked.text
        assert len(gamma_calls) == 1

        target_updated = client.post(
            "/live-monitor/quality/update-gamma-targets",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "target_updates": "gamma-both-fix|109.99",
            },
            follow_redirects=False,
        )
        outcome_updated = client.post(
            "/live-monitor/quality/update-gamma-outcomes",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "outcome_updates": "gamma-both-fix|DOWN",
            },
            follow_redirects=False,
        )
        assert target_updated.status_code == 303
        assert outcome_updated.status_code == 303
        assert len(gamma_calls) == 1

        record = client.get("/api/live-monitor/saved/gamma-both-fix")
        assert record.status_code == 200
        payload = record.json()["record"]
        assert payload["target_price"] == 109.99
        assert payload["history"][0]["target_price"] == 109.99
        assert payload["winning_outcome"] == "Down"


def test_live_monitor_quality_rate_limits_are_not_listed_as_contract_issues(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-rate-limited", 0)

    async def fake_gamma_payload(_client, slug: str):
        request = httpx.Request(
            "GET",
            f"https://gamma-api.polymarket.com/events?slug={slug}",
        )
        response = httpx.Response(429, request=request)
        raise httpx.HTTPStatusError(
            "rate limited",
            request=request,
            response=response,
        )

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Rate limited" in checked.text
        assert "Too Many Requests" not in checked.text
        assert "gamma-rate-limited" not in checked.text
        assert 'name="problem_ids" value="gamma-rate-limited"' not in checked.text


def test_live_monitor_quality_can_delete_only_missing_data_contracts(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "missing-data-delete", 0)
    write_quality_live_monitor_record(store, user, "target-mismatch-keep", 1)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        if slug.endswith(str(1782639600)):
            return [{"eventMetadata": {}}]
        return [{"eventMetadata": {"priceToBeat": 109.99}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Delete missing data" in checked.text
        assert 'name="missing_data_ids" value="missing-data-delete"' in checked.text
        assert 'name="missing_data_ids" value="target-mismatch-keep"' not in checked.text
        assert len(gamma_calls) == 2

        deleted = client.post(
            "/live-monitor/quality/delete-missing-data",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "missing_data_ids": "missing-data-delete",
            },
            follow_redirects=False,
        )
        assert deleted.status_code == 303
        assert len(gamma_calls) == 2
        assert client.get("/api/live-monitor/saved/missing-data-delete").status_code == 404
        assert client.get("/api/live-monitor/saved/target-mismatch-keep").status_code == 200


def test_live_monitor_quality_can_delete_only_gamma_target_missing_contracts(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-target-missing-delete", 0)
    write_quality_live_monitor_record(store, user, "gamma-target-mismatch-keep", 1)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        if slug.endswith(str(1782639600)):
            return [{"eventMetadata": {}}]
        return [{"eventMetadata": {"priceToBeat": 109.99}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Delete missing Gamma target" in checked.text
        assert (
            'name="gamma_target_missing_ids" value="gamma-target-missing-delete"'
            in checked.text
        )
        assert (
            'name="gamma_target_missing_ids" value="gamma-target-mismatch-keep"'
            not in checked.text
        )
        assert len(gamma_calls) == 2

        deleted = client.post(
            "/live-monitor/quality/delete-gamma-target-missing",
            data={
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "gamma_target_missing_ids": "gamma-target-missing-delete",
            },
            follow_redirects=False,
        )
        assert deleted.status_code == 303
        assert len(gamma_calls) == 2
        assert client.get("/api/live-monitor/saved/gamma-target-missing-delete").status_code == 404
        assert client.get("/api/live-monitor/saved/gamma-target-mismatch-keep").status_code == 200


def test_live_monitor_quality_recovers_rate_limited_gamma_requests(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-rate-limit-retry", 0)
    calls = 0

    async def fake_gamma_payload(_client, slug: str):
        nonlocal calls
        calls += 1
        if calls == 1:
            request = httpx.Request(
                "GET",
                f"https://gamma-api.polymarket.com/events?slug={slug}",
            )
            response = httpx.Response(429, request=request)
            raise httpx.HTTPStatusError(
                "rate limited",
                request=request,
                response=response,
            )
        return [{"eventMetadata": {"priceToBeat": 109.99}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Gamma target mismatch." in checked.text
        assert "gamma-rate-limit-retry" in checked.text
        assert "Rate limited</p><strong class=\"profit\">0" in checked.text
        assert calls == 2


def test_live_monitor_quality_reuses_gamma_cache_between_scans(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-cache-hit", 0)
    gamma_calls = []

    async def fake_gamma_payload(_client, slug: str):
        gamma_calls.append(slug)
        return [{"eventMetadata": {"priceToBeat": 109.99}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        first = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        second = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert first.status_code == 200
        assert second.status_code == 200
        assert "Gamma target mismatch." in second.text
        assert gamma_calls == ["btc-updown-5m-1782639600"]


def test_live_monitor_quality_does_not_delete_missing_target_results(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_quality_live_monitor_record(store, user, "gamma-target-unavailable", 0)

    start_ts = 1782639900
    history = [
        {
            "t": round(start_ts + index * 0.5, 3),
            "crypto_price": 101.08,
            "target_price": None,
            "yes_bid": 53,
            "yes_ask": 55,
            "yes_mid": 54,
        }
        for index in range(601)
    ]
    missing_ts_record = {
        "id": "local-target-timestamp-missing",
        "question": "Bitcoin Missing Target Timestamp",
        "crypto_symbol": "btc/usd",
        "slug": f"btc-updown-5m-{start_ts}",
        "condition_id": "condition-local-target-timestamp-missing",
        "saved_at": datetime.fromtimestamp(start_ts + 300, timezone.utc).isoformat(),
        "window_start_ts": start_ts,
        "window_end_ts": start_ts + 300,
        "target_price": 101,
        "target_price_ts": None,
        "final_crypto_price": 101.08,
        "winning_outcome": "Up",
        "partial_contract": False,
        "unsuitable_contract": False,
        "history_count": len(history),
        "history": history,
    }
    saved_dir = store.root / "live_monitor" / "users" / user["id"] / "saved_contracts"
    saved_dir.mkdir(parents=True, exist_ok=True)
    (saved_dir / "local-target-timestamp-missing.json").write_text(
        json.dumps(missing_ts_record),
        encoding="utf-8",
    )

    async def fake_gamma_payload(_client, _slug: str):
        return [{"eventMetadata": {}}]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    monkeypatch.setattr(
        live_monitor_module,
        "_fetch_gamma_event_payload",
        fake_gamma_payload,
    )

    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        checked = client.get("/live-monitor/quality?asset_symbol=BTC&timeframe=5m&scan=1")
        assert checked.status_code == 200
        assert "Target price timestamp is missing." in checked.text
        assert "Gamma target unavailable; saved target was not compared." in checked.text
        assert 'name="problem_ids" value="local-target-timestamp-missing"' not in checked.text
        assert 'name="problem_ids" value="gamma-target-unavailable"' not in checked.text

        deleted = client.post(
            "/live-monitor/quality/delete-problems",
            data={"asset_symbol": "BTC", "timeframe": "5m"},
            follow_redirects=False,
        )
        assert deleted.status_code == 303
        assert client.get("/api/live-monitor/saved/local-target-timestamp-missing").status_code == 200
        assert client.get("/api/live-monitor/saved/gamma-target-unavailable").status_code == 200


def test_live_monitor_analysis_three_phase_validation_keeps_only_survivors(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    for index in range(6):
        write_live_monitor_record(store, user, f"phase-up-{index}", index, outcome="Up")

    specifications = [
        {
            "family": "quote_momentum",
            "entry_second": 10,
            "quote_edge_cents": 2,
            "deviation_threshold_pct": 0.03,
            "allocation_pct": 0.1,
            "max_allocation_pct": 0.1,
            "fee_bps": 10.0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_ask",
        },
        {
            "family": "quote_reversal",
            "entry_second": 10,
            "quote_edge_cents": 2,
            "deviation_threshold_pct": 0.03,
            "allocation_pct": 0.1,
            "max_allocation_pct": 0.1,
            "fee_bps": 10.0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_ask",
        },
    ]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(
        LiveMonitorStrategyGenerator,
        "specifications",
        staticmethod(lambda count, strategy_mode="lightweight": specifications[:count]),
    )
    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        page = client.get("/live-monitor/analysis?asset_symbol=BTC&timeframe=5m")
        assert page.status_code == 200
        assert "Three-phase validation" in page.text

        started = client.post(
            "/api/live-monitor/analysis/jobs",
            data={
                "group_name": "Three phase BTC checks",
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "strategy_mode": "lightweight",
                "strategy_count": "2",
                "starting_balance": "500",
                "phase_1_contracts": "2",
                "phase_2_contracts": "2",
                "phase_3_contracts": "2",
            },
        )
        assert started.status_code == 202
        stream = client.get(started.json()["events_url"])
        assert stream.status_code == 200
        assert "Phase 1 complete" in stream.text
        assert "Phase 2 complete" in stream.text
        assert "Phase 3 complete" in stream.text

        group = store.list_user_records(user["id"], "groups")[-1]
        assert group["validation_mode"] == "three_phase"
        assert group["event_count"] == 6
        assert group["strategy_count"] == 1
        assert group["phase_validation"]["input_strategy_count"] == 2
        assert group["phase_validation"]["final_strategy_count"] == 1
        assert [item["family"] for item in group["phase_validation"]["family_rankings"]] == [
            "quote_momentum",
            "quote_reversal",
        ]
        assert group["phase_validation"]["family_rankings"][0]["rank"] == 1
        assert group["phase_validation"]["family_rankings"][1]["rank"] == 2
        assert group["phase_validation"]["family_rankings"][1]["phases"][0]["average_roi"] < 0
        assert [
            phase["profitable_strategies"]
            for phase in group["phase_validation"]["phases"]
        ] == [1, 1, 1]
        strategies = store.list_user_records_filtered(
            user["id"], "strategies", {"group_id": group["id"]}
        )
        assert len(strategies) == 1
        assert strategies[0]["family"] == "quote_momentum"
        assert strategies[0]["metrics"]["phase_1_roi"] > 0
        assert strategies[0]["metrics"]["phase_2_roi"] > 0
        assert strategies[0]["metrics"]["phase_3_roi"] > 0
        group_page = client.get(f"/groups/{group['id']}")
        assert group_page.status_code == 200
        assert "Three-phase survivor check" in group_page.text
        assert "1 / 2" in group_page.text
        assert "Top 1" in group_page.text
        assert "Top 2" in group_page.text
        assert "Quote Reversal" in group_page.text


def test_live_monitor_analysis_keeps_previous_phase_survivors_when_next_phase_fails(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    for index, outcome in enumerate(["Up", "Up", "Down", "Down", "Up", "Up"]):
        write_live_monitor_record(store, user, f"phase-mixed-{index}", index, outcome=outcome)

    specifications = [
        {
            "family": "quote_momentum",
            "entry_second": 10,
            "quote_edge_cents": 2,
            "deviation_threshold_pct": 0.03,
            "allocation_pct": 0.1,
            "max_allocation_pct": 0.1,
            "fee_bps": 10.0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_ask",
        },
        {
            "family": "quote_reversal",
            "entry_second": 10,
            "quote_edge_cents": 2,
            "deviation_threshold_pct": 0.03,
            "allocation_pct": 0.1,
            "max_allocation_pct": 0.1,
            "fee_bps": 10.0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_ask",
        },
    ]

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(
        LiveMonitorStrategyGenerator,
        "specifications",
        staticmethod(lambda count, strategy_mode="lightweight": specifications[:count]),
    )
    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        started = client.post(
            "/api/live-monitor/analysis/jobs",
            data={
                "group_name": "Fallback phase checks",
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "strategy_mode": "lightweight",
                "strategy_count": "2",
                "starting_balance": "500",
                "phase_1_contracts": "2",
                "phase_2_contracts": "2",
                "phase_3_contracts": "2",
            },
        )
        assert started.status_code == 202
        stream = client.get(started.json()["events_url"])
        assert stream.status_code == 200
        assert '"stage": "error"' not in stream.text
        assert "Phase 2 complete: 0 of 1" in stream.text

        group = store.list_user_records(user["id"], "groups")[-1]
        assert group["validation_mode"] == "three_phase"
        assert group["phase_validation"]["survivor_phase"] == 1
        assert group["phase_validation"]["stopped_at_phase"] == 2
        assert group["phase_validation"]["final_strategy_count"] == 1
        strategies = store.list_user_records_filtered(
            user["id"], "strategies", {"group_id": group["id"]}
        )
        assert len(strategies) == 1
        assert strategies[0]["metrics"]["phase_1_roi"] > 0
        assert strategies[0]["metrics"]["phase_2_roi"] < 0
        page = client.get(f"/groups/{group['id']}")
        assert page.status_code == 200
        assert "validation stopped at phase 2" in page.text
        assert "Phase 1" in page.text
        assert "Phase 2" in page.text


def test_live_monitor_parallel_backtests_preserve_filtered_strategy_indexes(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    generator = LiveMonitorStrategyGenerator(store, None)
    start_ts = 1_800_000_000.0
    events = []
    history = []
    for offset in range(500):
        yes_mid = 55.0
        history.append({
            "t": start_ts + offset,
            "crypto_price": 100.1,
            "target_price": 100.0,
            "yes_bid": yes_mid - 1,
            "yes_ask": yes_mid + 1,
            "yes_mid": yes_mid,
            "no_bid": 100 - (yes_mid + 1),
            "no_ask": 100 - (yes_mid - 1),
        })
    for index in range(30):
        event_start = start_ts + index * 300
        events.append({
            "id": f"parallel-{index}",
            "market_id": f"condition-parallel-{index}",
            "question": f"Parallel {index}",
            "asset": "BTC",
            "timeframe": "5m",
            "start_ts": event_start,
            "end_ts": event_start + 300,
            "start_time": datetime.fromtimestamp(event_start, timezone.utc).isoformat(),
            "expiration_time": datetime.fromtimestamp(event_start + 300, timezone.utc).isoformat(),
            "history": [
                {**row, "t": event_start + (row["t"] - start_ts)}
                for row in history
            ],
            "resolved_outcome": "up",
            "partial_contract": False,
        })
    specifications = [
        {
            "family": "quote_momentum",
            "entry_second": 10,
            "quote_edge_cents": 2,
            "deviation_threshold_pct": 0.03,
            "allocation_pct": 0.1,
            "max_allocation_pct": 0.1,
            "fee_bps": 10.0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_ask",
        },
        {
            "family": "quote_reversal",
            "entry_second": 10,
            "quote_edge_cents": 2,
            "deviation_threshold_pct": 0.03,
            "allocation_pct": 0.1,
            "max_allocation_pct": 0.1,
            "fee_bps": 10.0,
            "exit_condition": "settlement",
            "price_source": "saved_live_monitor_ask",
        },
    ]

    monkeypatch.setenv("STRATEGY_BACKTEST_WORKERS", "2")
    results = generator._run_strategy_backtests(
        events,
        specifications,
        500.0,
        None,
        None,
        [287, 299],
    )

    assert [index for index, _specification, _result in results] == [287, 299]
    assert len(results) == 2


def test_live_monitor_events_from_saved_ids_uses_batch_loader(tmp_path: Path):
    store, _user = populated_store(tmp_path)
    records = []
    for index in range(2):
        start_ts = 1_800_000_000.0 + index * 300
        records.append({
            "id": f"record-{index}",
            "window_start_ts": start_ts,
            "window_end_ts": start_ts + 300,
            "winning_outcome": "Up",
            "history": [
                {
                    "t": start_ts + 10,
                    "crypto_price": 100.1,
                    "target_price": 100.0,
                    "yes_bid": 54.0,
                    "yes_ask": 56.0,
                    "no_bid": 44.0,
                    "no_ask": 46.0,
                },
            ],
        })

    class BatchOnlyMonitor:
        def __init__(self):
            self.requested_ids = []

        def list_saved_by_ids(self, user_id, record_ids):
            assert user_id == "user-1"
            self.requested_ids = list(record_ids)
            return records

        def get_saved(self, _user_id, _record_id):
            raise AssertionError("expected the batch saved-record loader")

    monitor = BatchOnlyMonitor()
    generator = LiveMonitorStrategyGenerator(store, monitor)

    events = generator._events_from_saved_record_ids(
        "user-1",
        "BTC",
        "5m",
        False,
        ["record-0", "record-1"],
    )

    assert monitor.requested_ids == ["record-0", "record-1"]
    assert [event["id"] for event in events] == ["record-0", "record-1"]


def test_live_monitor_metrics_only_backtest_matches_full_phase_metrics(tmp_path: Path):
    store, _user = populated_store(tmp_path)
    generator = LiveMonitorStrategyGenerator(store, None)
    start_ts = 1_800_000_000.0
    events = []
    for event_index, outcome in enumerate(("up", "down")):
        event_start = start_ts + event_index * 300
        history = []
        for offset in range(20):
            history.append({
                "t": event_start + offset * 10,
                "crypto_price": 100 + offset * (0.05 if outcome == "up" else -0.05),
                "target_price": 100.0,
                "yes_bid": 54.0,
                "yes_ask": 56.0,
                "yes_mid": 55.0,
                "no_bid": 44.0,
                "no_ask": 46.0,
            })
        events.append({
            "id": f"metrics-only-{event_index}",
            "market_id": f"condition-{event_index}",
            "question": f"Metrics only {event_index}",
            "asset": "BTC",
            "timeframe": "5m",
            "start_ts": event_start,
            "end_ts": event_start + 300,
            "start_time": datetime.fromtimestamp(event_start, timezone.utc).isoformat(),
            "expiration_time": datetime.fromtimestamp(event_start + 300, timezone.utc).isoformat(),
            "history": history,
            "resolved_outcome": outcome,
            "partial_contract": False,
        })
    strategy = {
        "family": "quote_momentum",
        "entry_second": 10,
        "quote_edge_cents": 2,
        "deviation_threshold_pct": 0.03,
        "allocation_pct": 0.1,
        "max_allocation_pct": 0.1,
        "fee_bps": 10.0,
        "exit_condition": "settlement",
        "price_source": "saved_live_monitor_ask",
    }

    full = generator._run(events, strategy, 500.0)
    metrics_only = generator._run(events, strategy, 500.0, metrics_only=True)

    for key in ("net_profit", "roi", "trade_count", "contracts_used"):
        assert metrics_only[key] == full[key]
    assert metrics_only["transactions"] == []
    assert metrics_only["equity_curve"] == []


def test_live_monitor_analysis_generates_smart_intrachart_strategies(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    write_live_monitor_record(store, user, "smart-up", 0, outcome="Up")
    write_live_monitor_record(store, user, "smart-down", 1, outcome="Down")

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        page = client.get("/live-monitor/analysis?asset_symbol=BTC&timeframe=5m&strategy_mode=smart")
        assert page.status_code == 200
        assert "Smart intrachart families" in page.text

        started = client.post(
            "/api/live-monitor/analysis/jobs",
            data={
                "group_name": "Smart BTC checks",
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "strategy_mode": "smart",
                "include_partial_contracts": "true",
                "strategy_count": "8",
                "starting_balance": "500",
            },
        )
        assert started.status_code == 202
        assert '"stage": "complete"' in client.get(started.json()["events_url"]).text

        group = store.list_user_records(user["id"], "groups")[-1]
        assert group["strategy_mode"] == "smart"
        family_reports = store.list_user_records(user["id"], "family_reports")
        assert family_reports[-1]["group_id"] == group["id"]
        assert family_reports[-1]["rows"]
        strategies = store.list_user_records_filtered(
            user["id"], "strategies", {"group_id": group["id"]}
        )
        smart_families = {strategy["family"] for strategy in strategies}
        assert smart_families <= {
            "smart_value_edge",
            "smart_momentum_edge",
            "smart_tight_spread_edge",
            "smart_acceleration_edge",
            "smart_breakout_confirmation",
            "smart_pullback_continuation",
            "smart_settlement_drift",
            "price_threshold_70_stop_40",
        }
        assert {
            "smart_value_edge",
            "smart_momentum_edge",
            "smart_tight_spread_edge",
            "smart_acceleration_edge",
            "smart_breakout_confirmation",
            "smart_pullback_continuation",
            "smart_settlement_drift",
            "price_threshold_70_stop_40",
        } <= smart_families
        all_trades = store.list_user_records_filtered(
            user["id"], "transactions", {"group_id": group["id"]}
        )
        traded_strategy = next(
            strategy for strategy in strategies
            if any(trade["strategy_id"] == strategy["id"] for trade in all_trades)
        )
        trades = [
            trade for trade in all_trades if trade["strategy_id"] == traded_strategy["id"]
        ]
        assert trades
        assert all(trade["ask_price"] == trade["entry_price"] for trade in all_trades)
        assert all(
            trade["quote_source"] == "live_monitor_smart_history"
            for trade in all_trades
        )
        assert all(
            trade["entry_history_count"] <= trade["analysis_history_count"]
            for trade in all_trades
        )
        chart_trade = all_trades[0]
        bid_exit = next(
            (trade for trade in all_trades if trade["exit_type"] != "settlement"),
            None,
        )
        if bid_exit:
            chart_trade = bid_exit
            bid_exit_strategy = next(
                strategy for strategy in strategies
                if strategy["id"] == bid_exit["strategy_id"]
            )
            assert bid_exit["exit_bid_price"] == bid_exit["exit_price"]
            assert bid_exit["resolved_outcome"] in {"UP", "DOWN", "FLAT"}
            assert bid_exit["resolved_outcome"] != "BID_EXIT"
            store.update_user_record(
                user["id"], "transactions", bid_exit["id"], {"resolved_outcome": "BID_EXIT"}
            )
            detail = client.get(f"/groups/{group['id']}/strategies/{bid_exit_strategy['id']}")
            assert detail.status_code == 200
            assert "BID_EXIT" not in detail.text

        chart = client.get(
            f"/api/groups/{group['id']}/strategies/{chart_trade['strategy_id']}/trades/{chart_trade['id']}/contract-chart"
        )
        assert chart.status_code == 200
        payload = chart.json()
        assert payload["trade"]["exit_type"] == chart_trade["exit_type"]
        assert payload["trade"]["entry_edge_cents"] == chart_trade["entry_edge_cents"]
        assert payload["trade"]["resolved_outcome"] in {"UP", "DOWN", "FLAT"}


def test_live_monitor_analysis_filters_selected_strategy_families(
    tmp_path: Path,
    monkeypatch,
):
    store, user = populated_store(tmp_path)
    write_live_monitor_record(store, user, "family-up", 0, outcome="Up")
    write_live_monitor_record(store, user, "family-down", 1, outcome="Down")

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        page = client.get("/live-monitor/analysis?asset_symbol=BTC&timeframe=5m")
        assert page.status_code == 200
        assert "Quote Reversal" in page.text
        assert "strategy_family_filter_enabled" in page.text

        started = client.post(
            "/api/live-monitor/analysis/jobs",
            data={
                "group_name": "Filtered family checks",
                "asset_symbol": "BTC",
                "timeframe": "5m",
                "strategy_mode": "lightweight",
                "strategy_family_filter_enabled": "true",
                "strategy_families": "quote_reversal",
                "strategy_count": "2",
                "starting_balance": "500",
            },
        )
        assert started.status_code == 202
        assert '"stage": "complete"' in client.get(started.json()["events_url"]).text

        group = store.list_user_records(user["id"], "groups")[-1]
        assert group["strategy_families"] == ["quote_reversal"]
        strategies = store.list_user_records_filtered(
            user["id"], "strategies", {"group_id": group["id"]}
        )
        assert len(strategies) == 2
        assert {strategy["family"] for strategy in strategies} == {"quote_reversal"}


def test_favorite_and_retest_routes_persist_results(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    group = generate_group(store, user, count=2)
    strategy = store.list_user_records(user["id"], "strategies")[0]
    collection_calls = []

    class PreparedCollector:
        def __init__(self, current_store):
            assert current_store is store

        async def collect(self, assets, timeframe, start_date, end_date, progress=None):
            collection_calls.append((assets, timeframe, start_date, end_date))
            if progress:
                progress({
                    "stage": "download", "message": "API requests completed: 4 of 4",
                    "api_requests_completed": 4, "api_requests_total": 4,
                })
            return {"events": 288, "new_events": 0, "assets": len(assets)}

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", PreparedCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        strategies_path = store.root / "users" / user["id"] / "strategies.jsonl"
        strategies_before = strategies_path.read_text(encoding="utf-8")
        favorite = client.post(
            f"/groups/{group['id']}/strategies/{strategy['id']}/favorite",
            headers={"Accept": "application/json"},
        )
        assert favorite.status_code == 200
        assert favorite.json() == {"favorite": True, "label": "Favorited"}
        assert strategies_path.read_text(encoding="utf-8") == strategies_before
        favorites = client.get("/saved-strategies")
        assert favorites.status_code == 200
        assert strategy["name"] in favorites.text
        note = client.post(
            f"/groups/{group['id']}/strategies/{strategy['id']}/favorite-comment",
            data={"comment": "Forward retest passed at 51c."},
            follow_redirects=True,
        )
        assert note.status_code == 200
        favorites = client.get("/saved-strategies")
        assert "Forward retest passed at 51c." in favorites.text

        retest = client.post(
            f"/groups/{group['id']}/strategies/{strategy['id']}/retest",
            data={"start_date": "2025-01-01", "end_date": "2025-01-01"},
            follow_redirects=False,
        )
        assert retest.status_code == 303
        progress_page = client.get(retest.headers["location"])
        assert progress_page.status_code == 200
        assert "Preparing historical data" in progress_page.text
        job_id = retest.headers["location"].rsplit("/", 1)[-1]
        stream = client.get(f"/api/group-jobs/{job_id}/events")
        assert '"stage": "complete"' in stream.text
        assert "API requests completed: 4 of 4" in stream.text
    assert collection_calls == [(["BTC"], "5m", "2025-01-01", "2025-01-01")]
    assert len(store.list_user_records(user["id"], "groups")) == 2


def test_summary_only_strategy_detail_skips_transaction_file_scan(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    group = generate_group(store, user, count=10)
    strategy = next(
        item for item in store.list_user_records(user["id"], "strategies")
        if item["metrics"]["trade_count"] > 0
    )
    store.update_user_record(
        user["id"], "strategies", strategy["id"], {"transactions_summary_only": True}
    )

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    def fail_transaction_scan(*args, **kwargs):
        raise AssertionError("summary-only strategies must not scan transactions")

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    monkeypatch.setattr(store, "list_user_records_page", fail_transaction_scan)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        response = client.get(f"/groups/{group['id']}/strategies/{strategy['id']}")
    assert response.status_code == 200
    assert "Execution rows are reconstructed on demand" in response.text
    assert "Assumed Ask" in response.text
    assert "summary-only validation run" not in response.text


def test_retest_uses_cached_events_when_remote_refresh_fails(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    group = generate_group(store, user, count=1)
    strategy = store.list_user_records(user["id"], "strategies")[0]

    class FailingCollector:
        def __init__(self, current_store):
            pass

        async def collect(self, assets, timeframe, start_date, end_date, progress=None):
            if progress:
                progress({"stage": "download", "message": "remote refresh started"})
            raise TimeoutError()

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", FailingCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        retest = client.post(
            f"/groups/{group['id']}/strategies/{strategy['id']}/retest",
            data={"start_date": "2025-01-01", "end_date": "2025-01-01"},
            follow_redirects=False,
        )
        job_id = retest.headers["location"].rsplit("/", 1)[-1]
        stream = client.get(f"/api/group-jobs/{job_id}/events")
    assert '"stage": "complete"' in stream.text
    assert "using the locally cached contracts" in stream.text


def test_server_side_cookie_session_is_rotated_and_isolates_clients(tmp_path: Path, monkeypatch):
    store = FileStore(tmp_path / "server_data")

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as first, TestClient(app_module.create_app()) as second:
        login = first.post(
            "/login",
            data={"email": settings.username, "password": settings.password},
            follow_redirects=False,
        )
        cookie = login.headers["set-cookie"]
        assert settings.session_cookie_name in cookie
        assert "HttpOnly" in cookie
        assert "SameSite=lax" in cookie
        assert settings.username not in cookie
        assert first.get("/").status_code == 200
        assert second.get("/", follow_redirects=False).status_code == 303
        assert first.app.state.session_store.count_active() == 1


def test_local_env_login_and_data_profile_controls_are_local_only(tmp_path: Path, monkeypatch):
    store = FileStore(tmp_path / "server_data")

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(settings, "environment", "development")
    monkeypatch.setattr(settings, "username", "cached-user")
    monkeypatch.setattr(settings, "password", "CachedUser123!")
    monkeypatch.setattr(routes_module, "ROOT_DIR", tmp_path)
    (tmp_path / ".env").write_text(
        "USERNAME=fresh-user@example.com\nPASSWORD=FreshUser123!\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as client:
        page = client.get("/login")
        assert page.status_code == 200
        assert "Choose data profile" in page.text
        assert "Log in from .env" in page.text
        assert "fresh-user@example.com" in page.text

        login = client.post("/login/env", follow_redirects=False)
        assert login.status_code == 303
        assert login.headers["location"] == "/"
        state = client.get("/api/state")
        assert state.status_code == 200
        assert state.json()["user"]["email"] == "fresh-user@example.com"
        account = client.get("/account")
        assert account.status_code == 200
        assert "fresh-user@example.com" in account.text
        assert "cached-user@local.test" not in account.text
        assert str(store.root) in account.text
        assert "Choose data profile" in account.text

        client.post("/logout")
        monkeypatch.setattr(settings, "environment", "production")
        blocked = client.post("/login/env", follow_redirects=False)
        assert blocked.status_code == 404


def test_account_can_clear_project_cache_without_deleting_accounts(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    generate_group(store, user, "Cache Clear Pack", 2)
    write_live_monitor_record(store, user, "cache-clear-live", 0, outcome="Up")
    (store.root / "cache" / "polymarket").mkdir(parents=True)
    (store.root / "cache" / "polymarket" / "entry.json").write_text("{}", encoding="utf-8")
    (store.root / "jobs").mkdir(parents=True)
    (store.root / "jobs" / "job.json").write_text("{}", encoding="utf-8")

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        account = client.get("/account")
        assert account.status_code == 200
        assert "Delete all project cache" in account.text

        cleared = client.post("/account/clear-project-cache", follow_redirects=False)
        assert cleared.status_code == 303
        assert cleared.headers["location"] == "/account"

        new_store = client.app.state.store
        assert new_store.get_user(user["id"]) is not None
        assert new_store.list_user_records(user["id"], "groups") == []
        assert new_store.list_user_records(user["id"], "strategies") == []
        assert new_store.list_user_records(user["id"], "transactions") == []
        assert new_store.count_fixed_events() == 0
        assert not (new_store.root / "live_monitor" / "users" / user["id"] / "saved_contracts" / "cache-clear-live.json").exists()
        assert not (new_store.root / "cache" / "polymarket" / "entry.json").exists()
        assert not (new_store.root / "jobs" / "job.json").exists()
        assert client.get("/api/state").json()["user"]["id"] == user["id"]


def test_account_shows_aggregate_smart_family_report(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    group = {
        "id": "smart-report-group",
        "name": "Smart report group",
        "source": "live_monitor_saved_contracts",
        "theme": "live_monitor_saved_contracts",
        "strategy_mode": "smart",
        "strategy_count": 3,
        "assets": ["BTC"],
        "timeframe": "5m",
        "starting_balance": 500,
        "event_count": 2,
        "period": {
            "start_date": "2026-07-01",
            "end_date": "2026-07-01",
            "start_time": "2026-07-01T00:00:00+00:00",
            "end_time_exclusive": "2026-07-01T00:10:00+00:00",
        },
        "phase_validation": {
            "enabled": True,
            "family_rankings": [
                {
                    "family": "smart_reversion_edge",
                    "family_label": "Smart Reversion Edge",
                    "generated_count": 4,
                    "strategy_count": 4,
                    "final_strategy_count": 0,
                    "total_trades": 0,
                    "phases": [
                        {
                            "phase": 1,
                            "tested_count": 4,
                            "profitable_count": 0,
                            "average_roi": -6.0,
                            "best_roi": -1.0,
                            "worst_roi": -12.0,
                            "profitable_percent": 0.0,
                        },
                        {
                            "phase": 2,
                            "tested_count": 0,
                            "profitable_count": 0,
                            "average_roi": 0.0,
                            "best_roi": 0.0,
                            "worst_roi": 0.0,
                            "profitable_percent": 0.0,
                        },
                        {
                            "phase": 3,
                            "tested_count": 0,
                            "profitable_count": 0,
                            "average_roi": 0.0,
                            "best_roi": 0.0,
                            "worst_roi": 0.0,
                            "profitable_percent": 0.0,
                        },
                    ],
                },
            ],
        },
        "summary": {"best_roi": 12.5, "average_roi": 3.6667, "profitable_strategies": 2},
        "created_at": "2026-07-01T00:00:00+00:00",
    }
    base_metrics = {
        "initial_capital": 500,
        "final_capital": 500,
        "net_profit": 0,
        "roi": 0,
        "sharpe_ratio": 0,
        "win_rate": 0,
        "max_drawdown": 0,
        "profit_factor": 0,
        "average_trade": 0,
        "trade_count": 0,
        "event_count": 2,
        "contracts_used": 0,
        "stability_score": 0,
        "recovery_factor": 0,
        "up_trade_count": 0,
        "down_trade_count": 0,
        "winning_trade_count": 0,
        "losing_trade_count": 0,
        "flat_trade_count": 0,
        "skipped_contract_count": 2,
        "average_allocation_pct": 0,
        "max_allocation_pct_used": 0,
    }
    strategies = [
        {
            "id": "smart-value-good",
            "group_id": group["id"],
            "name": "Smart Value",
            "family": "smart_value_edge",
            "family_label": "Smart Value Edge",
            "parameters": {},
            "metrics": {**base_metrics, "roi": 12.5, "net_profit": 62.5, "trade_count": 3, "win_rate": 66.7},
            "equity_curve": [],
        },
        {
            "id": "smart-value-flat",
            "group_id": group["id"],
            "name": "Smart Value 2",
            "family": "smart_value_edge",
            "family_label": "Smart Value Edge",
            "parameters": {},
            "metrics": {**base_metrics, "roi": 2.5, "net_profit": 12.5, "trade_count": 1, "win_rate": 100},
            "equity_curve": [],
        },
        {
            "id": "smart-momentum-bad",
            "group_id": group["id"],
            "name": "Smart Momentum",
            "family": "smart_momentum_edge",
            "family_label": "Smart Momentum Edge",
            "parameters": {},
            "metrics": {**base_metrics, "roi": -4.0, "net_profit": -20, "trade_count": 2, "win_rate": 0},
            "equity_curve": [],
        },
    ]
    store.save_group_bundle(user["id"], group, strategies, [])

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        account = client.get("/account")
        assert account.status_code == 200
        assert "All-analysis family report" in account.text
        assert "Refresh report" in account.text
        assert "Smart Value Edge" in account.text
        assert "Smart Momentum Edge" in account.text
        assert "Smart Reversion Edge" in account.text
        assert "avg ROI +7.50%" in account.text
        assert "avg ROI -4.00%" in account.text
        assert "avg ROI -6.00%" in account.text
        assert "not tested yet" in account.text
        assert "Not tested" in account.text
        assert "Please analyze which smart strategy families look strongest" in account.text
        refreshed = client.get("/api/account/smart-family-report")
        assert refreshed.status_code == 200
        payload = refreshed.json()
        assert payload["family_count"] == len(payload["rows"])
        assert {row["family"] for row in payload["rows"]} >= {
            "smart_value_edge",
            "smart_momentum_edge",
            "smart_reversion_edge",
        }
        reversion = next(row for row in payload["rows"] if row["family"] == "smart_reversion_edge")
        assert reversion["tested"] is True
        assert reversion["average_roi"] == -6.0
        assert reversion["final_strategy_count"] == 0
        assert next(row for row in payload["rows"] if row["family"] == "smart_tight_spread_edge")["tested"] is False


def test_sse_stream_reports_all_generation_stages(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)

    class ProgressCollector:
        def __init__(self, current_store):
            pass

        async def collect(self, assets, timeframe, start_date, end_date, progress=None):
            if progress:
                progress({"stage": "download", "message": "download"})
                progress({"stage": "validation", "message": "saved 12", "saved_events": 12})
            return {"events": 12, "new_events": 0, "assets": 1}

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", ProgressCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        started = client.post(
            "/api/group-jobs",
            data={
                "group_name": "SSE Group", "starting_balance": "10000",
                "start_date": "2025-01-01", "end_date": "2025-01-01",
                "strategy_count": "5", "assumed_ask_price_cents": "50",
                "asset_symbols": "BTC", "timeframe": "5m",
            },
        )
        assert started.status_code == 202
        stream = client.get(started.json()["events_url"])
        assert stream.status_code == 200
        assert stream.headers["content-type"].startswith("text/event-stream")
        payload = stream.text
        for stage in ("download", "validation", "backtest", "finalization", "complete"):
            assert f'"stage": "{stage}"' in payload
        assert "5 of 5" in payload
    assert len(store.list_user_records(user["id"], "groups")) == 1


def test_generation_job_can_be_cancelled(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)

    class WaitingCollector:
        def __init__(self, current_store):
            self.cancelled = False

        async def collect(self, assets, timeframe, start_date, end_date, progress=None):
            if progress:
                progress({"stage": "download", "message": "waiting for remote API"})
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.cancelled = True
                raise

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", WaitingCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        started = client.post(
            "/api/group-jobs",
            data={
                "group_name": "Stop Me", "starting_balance": "10000",
                "start_date": "2025-01-01", "end_date": "2025-01-01",
                "strategy_count": "5", "assumed_ask_price_cents": "50",
                "asset_symbols": "BTC", "timeframe": "5m",
            },
        )
        assert started.status_code == 202
        job_id = started.json()["job_id"]
        cancelled = client.post(f"/api/group-jobs/{job_id}/cancel")
        assert cancelled.status_code == 200
        stream = client.get(started.json()["events_url"])
        assert '"stage": "cancelled"' in stream.text
        assert client.app.state.job_store.get(job_id)["status"] == "cancelled"


def test_fixed_cache_coverage_api_reports_cached_days(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    start, end = parse_period("2025-01-01", "2025-01-01")
    store.mark_fixed_coverage("BTC", "5m", start, end)

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as client:
        client.post("/login", data={"email": user["email"], "password": "secure-pass"})
        response = client.get(
            "/api/fixed-cache/coverage",
            params={
                "asset_symbols": "BTC",
                "timeframe": "5m",
                "start_date": "2025-01-01",
                "end_date": "2025-01-02",
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["days"][0] == {
            "date": "2025-01-01",
            "covered_assets": ["BTC"],
            "complete": True,
        }
        assert payload["days"][1]["complete"] is False
        deleted = client.post(
            "/api/fixed-cache/coverage/delete",
            data={
                "asset_symbols": "BTC",
                "timeframe": "5m",
                "start_date": "2025-01-01",
                "end_date": "2025-01-01",
            },
        )
        assert deleted.status_code == 200
        assert deleted.json()["removed_events"] == 288
        assert store.list_fixed_events(["BTC"], "5m", start, end) == []
        assert store.fixed_coverage_calendar(["BTC"], "5m", start, end)["days"][0]["complete"] is False


def test_admin_panel_reports_groups_and_requires_credentials(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)
    generate_group(store, user, count=2)

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(settings, "admin_username", "root-admin")
    monkeypatch.setattr(settings, "admin_password", "deployment-secret")
    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as client:
        assert client.get("/admin").status_code == 403
        response = client.post(
            "/login", data={"email": "root-admin", "password": "deployment-secret"},
            follow_redirects=True,
        )
        assert response.status_code == 200
        assert "System overview" in response.text
        assert "researcher@example.com" in response.text
        assert "Groups" in response.text
        assert "System metrics" in response.text
        assert "Active sessions" in response.text
        assert "Recent logs" in response.text
        updated = client.post(
            f"/admin/users/{user['id']}",
            data={"display_name": "Updated Researcher", "balance": "25000.50"},
            follow_redirects=True,
        )
        assert updated.status_code == 200
        assert store.get_user(user["id"])["display_name"] == "Updated Researcher"
        assert store.get_user(user["id"])["balance"] == 25000.5
        deleted = client.post(
            f"/admin/users/{user['id']}/delete", follow_redirects=True
        )
        assert deleted.status_code == 200
        assert store.get_user(user["id"]) is None


def test_admin_export_data_requires_admin_or_token(tmp_path: Path, monkeypatch):
    store, user = populated_store(tmp_path)

    class IdleCollector:
        def __init__(self, current_store):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(settings, "export_token", "download-token")
    monkeypatch.setattr(app_module, "FileStore", lambda *_: store)
    monkeypatch.setattr(app_module, "DataCollector", IdleCollector)
    with TestClient(app_module.create_app()) as client:
        assert client.get("/api/admin/export-data").status_code == 403
        response = client.get(
            "/api/admin/export-data",
            headers={"Authorization": "Bearer download-token"},
        )
        assert response.status_code == 200
        assert response.headers["content-disposition"].endswith(
            "polymarket-server-data.tar.gz"
        )
        with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as archive:
            assert f"server_data/users/{user['id']}/profile.json" in archive.getnames()
