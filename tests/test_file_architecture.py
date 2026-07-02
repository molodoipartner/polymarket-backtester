from __future__ import annotations

import asyncio
import io
import json
import tarfile
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import backend.api.app as app_module
from backend.auth import hash_password, verify_password
from backend.backtester import FileBacktester
from backend.fixed_time import TIMEFRAME_SECONDS, parse_period, parse_utc_datetime
from backend.storage import FileStore
from backend.strategy_generator import StrategyGroupGenerator
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
        "question": f"Bitcoin Up or Down {index}",
        "crypto_symbol": "btc/usd",
        "slug": f"btc-updown-5m-{start_ts}",
        "condition_id": f"condition-{record_id}",
        "saved_at": datetime.fromtimestamp(end_ts, timezone.utc).isoformat(),
        "window_start_ts": start_ts,
        "window_end_ts": end_ts,
        "target_price": base_price,
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
            key=lambda item: item["metrics"]["trade_count"],
        )
        assert strategy["metrics"]["trade_count"] > 20
        detail = client.get(f"/groups/{group['id']}/strategies/{strategy['id']}")
        assert detail.status_code == 200
        assert "Equity curve and executions" in detail.text
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
        detail = client.get(f"/groups/{group['id']}/strategies/{strategies[0]['id']}")
        assert detail.status_code == 200
        assert "Entry Ask" in detail.text
        assert "Run live retest" in detail.text
        assert "data-live-retest-loading" in detail.text
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
