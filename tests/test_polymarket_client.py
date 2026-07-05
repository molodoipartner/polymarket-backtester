from datetime import datetime, timezone
import asyncio

import httpx
import pytest

import backend.data.polymarket_client as polymarket_client_module
from backend.data.polymarket_client import PolymarketClient, parse_datetime, parse_list


def test_gamma_string_lists_are_parsed():
    assert parse_list('["Yes", "No"]') == ["Yes", "No"]
    assert parse_list(["a"]) == ["a"]
    assert parse_list("invalid") == []


def test_iso_datetime_is_parsed_as_aware():
    value = parse_datetime("2026-01-02T03:04:05Z")
    assert value is not None
    assert value.utcoffset() is not None


@pytest.mark.asyncio
async def test_event_history_is_chunked_across_the_entire_period():
    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self.payload

    class FakeHTTP:
        def __init__(self):
            self.calls = []

        async def get(self, url, params):
            self.calls.append(params)
            marker = params["end_date_min"]
            return Response([{"id": marker, "endDate": marker}])

        async def aclose(self):
            pass

    client = PolymarketClient()
    await client.http.aclose()
    client.http = FakeHTTP()
    rows = await client.get_closed_events(
        "series",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 11, tzinfo=timezone.utc),
    )
    assert len(rows) == 4
    assert len(client.http.calls) == 4
    assert client.http.calls[0]["end_date_min"].startswith("2025-01-01")
    assert client.http.calls[-1]["end_date_max"].startswith("2025-01-11")


@pytest.mark.asyncio
async def test_event_history_uses_gamma_page_cap_and_fetches_next_page():
    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self.payload

    class FakeHTTP:
        def __init__(self):
            self.calls = []

        async def get(self, url, params):
            self.calls.append(params)
            if params["offset"] == 0:
                return Response([
                    {"id": str(index), "endDate": f"2025-01-01T{index % 24:02d}:00:00Z"}
                    for index in range(100)
                ])
            return Response([{"id": "100", "endDate": "2025-01-01T23:59:00Z"}])

        async def aclose(self):
            pass

    client = PolymarketClient()
    await client.http.aclose()
    client.http = FakeHTTP()
    rows = await client.get_closed_events(
        "series",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
    )

    assert len(rows) == 101
    assert [call["limit"] for call in client.http.calls] == [100, 100]
    assert [call["offset"] for call in client.http.calls] == [0, 100]


@pytest.mark.asyncio
async def test_json_cache_survives_client_restart(tmp_path):
    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return [{"id": "cached", "endDate": "2025-01-01T00:00:00Z"}]

    class FakeHTTP:
        def __init__(self):
            self.calls = 0

        async def get(self, url, params):
            self.calls += 1
            return Response()

        async def aclose(self):
            pass

    cache_dir = tmp_path / "cache"
    first = PolymarketClient(cache_dir=cache_dir)
    await first.http.aclose()
    first.http = FakeHTTP()
    assert len(await first.get_closed_events(
        "series",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
    )) == 1
    assert first.http.calls == 1

    second = PolymarketClient(cache_dir=cache_dir)
    await second.http.aclose()
    second.http = FakeHTTP()
    assert len(await second.get_closed_events(
        "series",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
    )) == 1
    assert second.http.calls == 0


@pytest.mark.asyncio
async def test_gamma_chunks_are_fetched_in_parallel_and_report_progress():
    class Response:
        def __init__(self, marker):
            self.marker = marker

        def raise_for_status(self):
            pass

        def json(self):
            return [{"id": self.marker, "endDate": self.marker}]

    class FakeHTTP:
        def __init__(self):
            self.active = 0
            self.max_active = 0

        async def get(self, url, params):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0.01)
            self.active -= 1
            return Response(params["end_date_min"])

        async def aclose(self):
            pass

    updates = []

    async def progress(event):
        updates.append(event)

    client = PolymarketClient()
    await client.http.aclose()
    client.http = FakeHTTP()
    rows = await client.get_closed_events(
        "series",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 11, tzinfo=timezone.utc),
        request_progress=progress,
        semaphore=asyncio.Semaphore(4),
    )
    assert len(rows) == 4
    assert client.http.max_active == 4
    assert updates.count("scheduled") == 4
    assert updates.count("completed") == 4


@pytest.mark.asyncio
async def test_gamma_history_retries_after_rate_limit(monkeypatch):
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)

    class Response:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self.payload = payload
            self.headers = {"retry-after": "0.1"} if status_code == 429 else {}

        def raise_for_status(self):
            if self.status_code >= 400:
                request = httpx.Request("GET", "https://gamma-api.polymarket.com/events")
                response = httpx.Response(self.status_code, request=request)
                raise httpx.HTTPStatusError("rate limited", request=request, response=response)

        def json(self):
            return self.payload

    class FakeHTTP:
        def __init__(self):
            self.calls = 0

        async def get(self, _url, params):
            self.calls += 1
            if self.calls == 1:
                return Response(429, {"error": "slow down"})
            return Response(200, [{"id": "ok", "endDate": "2025-01-01T00:00:00Z"}])

        async def aclose(self):
            pass

    monkeypatch.setattr(polymarket_client_module.asyncio, "sleep", sleep)

    client = PolymarketClient()
    await client.http.aclose()
    client.http = FakeHTTP()
    rows = await client.get_closed_events(
        "series",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
    )

    assert rows == [{"id": "ok", "endDate": "2025-01-01T00:00:00Z"}]
    assert client.http.calls == 2
    assert sleeps == [0.1]
