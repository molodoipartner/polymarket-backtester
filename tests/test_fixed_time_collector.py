from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from backend.data.collector import DataCollector
from backend.fixed_time import SERIES_IDS
from backend.storage import FileStore


class FakeClient:
    def __init__(self) -> None:
        self.series_ids = []

    async def get_closed_events(
        self, series_id: str, start_time, end_time,
        request_progress=None, semaphore=None,
    ) -> list[dict]:
        self.series_ids.append(series_id)
        if request_progress:
            await request_progress("scheduled")
            await request_progress("completed")
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        return [{
            "id": f"event-{series_id}",
            "title": "Fixed event",
            "startTime": start.isoformat(),
            "endDate": (start + timedelta(minutes=15)).isoformat(),
            "markets": [{
                "id": f"market-{series_id}",
                "conditionId": f"0x{int(series_id):064x}",
                "outcomes": json.dumps(["Up", "Down"]),
                "outcomePrices": json.dumps(["1", "0"]),
                "clobTokenIds": json.dumps([f"token-{series_id}", "down-token"]),
            }],
        }]

    async def close(self) -> None:
        pass


class RangeAwareClient(FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.requests = []

    async def get_closed_events(
        self, series_id: str, start_time, end_time,
        request_progress=None, semaphore=None,
    ) -> list[dict]:
        self.requests.append((series_id, start_time, end_time))
        if request_progress:
            await request_progress("scheduled")
            await request_progress("completed")
        raw_start = start_time
        return [{
            "id": f"event-{series_id}-{raw_start.date().isoformat()}",
            "title": "Fixed event",
            "startTime": raw_start.isoformat(),
            "endDate": (raw_start + timedelta(minutes=15)).isoformat(),
            "markets": [{
                "id": f"market-{series_id}-{raw_start.date().isoformat()}",
                "conditionId": f"0x{abs(hash((series_id, raw_start.date().isoformat()))) % (16 ** 64):064x}",
                "outcomes": json.dumps(["Up", "Down"]),
                "outcomePrices": json.dumps(["1", "0"]),
                "clobTokenIds": json.dumps([f"token-{series_id}", "down-token"]),
            }],
        }]

@pytest.mark.asyncio
async def test_collector_requests_only_selected_series_and_normalizes_entry(tmp_path):
    store = FileStore(tmp_path / "server_data")
    client = FakeClient()
    result = await DataCollector(store, client).collect(
        ["BTC", "ETH"], "15m", "2025-01-01", "2025-01-01"
    )
    assert client.series_ids == [SERIES_IDS["BTC"]["15m"], SERIES_IDS["ETH"]["15m"]]
    assert result == {"events": 2, "new_events": 2, "assets": 2}
    events = store.list_fixed_events(["BTC", "ETH"], "15m")
    assert len(events) == 2
    assert all(item["entry_time"] == "2025-01-01T00:00:00+00:00" for item in events)
    assert all(item["expiration_time"] == "2025-01-01T00:15:00+00:00" for item in events)
    assert all(item["data_quality"]["quote_source"] == "gamma_resolution_only" for item in events)
    assert all(item["data_quality"]["is_synthetic"] is False for item in events)
    assert all(item["resolved_outcome"] == "up" for item in events)
    assert not any(name.endswith("market_trades") for name in dir(client))


def test_gamma_mapping_derives_expiration_when_api_has_no_expiration_field():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    raw = {
        "id": "mapped",
        "title": "BTC fixed event",
        "startTime": start.isoformat(),
        "closedTime": (start + timedelta(minutes=16)).isoformat(),
    }
    market = {
        "id": "market-mapped",
        "conditionId": "0x" + "1" * 64,
        "outcomes": json.dumps(["Up", "Down"]),
        "outcomePrices": json.dumps(["1", "0"]),
    }
    mapped = DataCollector._event(
        raw, market, "BTC", "15m", market["conditionId"]
    )
    assert mapped is not None
    assert mapped["entry_time"] == "2025-01-01T00:00:00+00:00"
    assert mapped["expiration_time"] == "2025-01-01T00:15:00+00:00"
    assert mapped["end_time"] == mapped["expiration_time"]
    assert mapped["closed_time"] == "2025-01-01T00:16:00+00:00"


@pytest.mark.asyncio
async def test_collection_replaces_legacy_event_without_expiration_time(tmp_path):
    store = FileStore(tmp_path / "server_data")
    store.save_fixed_events([{
        "id": f"event-{SERIES_IDS['BTC']['15m']}",
        "asset": "BTC",
        "timeframe": "15m",
        "start_time": "2025-01-01T00:00:00+00:00",
        "end_time": "2025-01-01T00:15:00+00:00",
        "entry_up_price": 0.5,
    }])
    result = await DataCollector(store, FakeClient()).collect(
        ["BTC"], "15m", "2025-01-01", "2025-01-01"
    )
    events = store.list_fixed_events(["BTC"], "15m")
    assert result["events"] == 1
    assert len(events) == 1
    assert events[0]["expiration_time"] == "2025-01-01T00:15:00+00:00"
    assert "entry_up_price" not in events[0]


def test_store_startup_prunes_incompatible_cached_events(tmp_path):
    root = tmp_path / "server_data"
    store = FileStore(root)
    store.save_fixed_events([{
        "id": "legacy", "asset": "BTC", "timeframe": "15m",
        "start_time": "2025-01-01T00:00:00+00:00",
        "end_time": "2025-01-01T00:15:00+00:00",
    }])
    reopened = FileStore(root)
    assert reopened.list_fixed_events() == []


@pytest.mark.asyncio
async def test_collector_uses_local_coverage_cache_on_repeated_period(tmp_path):
    store = FileStore(tmp_path / "server_data")
    client = RangeAwareClient()
    collector = DataCollector(store, client)

    first = await collector.collect(["BTC"], "15m", "2025-01-01", "2025-01-01")
    second = await collector.collect(["BTC"], "15m", "2025-01-01", "2025-01-01")

    assert first == {"events": 1, "new_events": 1, "assets": 1}
    assert second == {"events": 1, "new_events": 0, "assets": 1}
    assert len(client.requests) == 1


@pytest.mark.asyncio
async def test_collector_downloads_only_missing_incremental_ranges(tmp_path):
    store = FileStore(tmp_path / "server_data")
    client = RangeAwareClient()
    collector = DataCollector(store, client)

    await collector.collect(["BTC"], "15m", "2025-01-01", "2025-01-01")
    await collector.collect(["BTC"], "15m", "2025-01-01", "2025-01-02")

    assert len(client.requests) == 2
    assert client.requests[1][1].date().isoformat() == "2025-01-02"
    assert client.requests[1][2].date().isoformat() == "2025-01-03"
    assert len(store.list_fixed_events(["BTC"], "15m")) == 2
