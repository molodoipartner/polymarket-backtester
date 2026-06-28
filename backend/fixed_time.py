from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

ASSETS = {
    "BTC": "Bitcoin",
    "ETH": "Ethereum",
    "SOL": "Solana",
    "XRP": "XRP",
    "BNB": "BNB",
    "DOGE": "Dogecoin",
    "HYPE": "Hyperliquid",
}

TIMEFRAMES = {
    "5m": "5 minutes",
    "15m": "15 minutes",
    "1h": "1 hour",
    "4h": "4 hours",
}

TIMEFRAME_SECONDS = {"5m": 300, "15m": 900, "1h": 3_600, "4h": 14_400}

SERIES_IDS = {
    "BTC": {"5m": "10684", "15m": "10192", "1h": "10114", "4h": "10331"},
    "ETH": {"5m": "10683", "15m": "10191", "1h": "10117", "4h": "10332"},
    "SOL": {"5m": "10686", "15m": "10423", "1h": "10122", "4h": "10333"},
    "XRP": {"5m": "10685", "15m": "10422", "1h": "10123", "4h": "10327"},
    "BNB": {"5m": "11326", "15m": "11330", "1h": "11291", "4h": "11332"},
    "DOGE": {"5m": "11325", "15m": "11328", "1h": "11287", "4h": "11331"},
    "HYPE": {"5m": "11327", "15m": "11329", "1h": "11289", "4h": "11333"},
}

STRATEGY_THEME = "fixed_time_activity"


def parse_utc_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as error:
            raise ValueError(f"Invalid datetime: {value}") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_period(start_value: str | date, end_value: str | date) -> tuple[datetime, datetime]:
    try:
        start_date = start_value if isinstance(start_value, date) else date.fromisoformat(start_value)
        end_date = end_value if isinstance(end_value, date) else date.fromisoformat(end_value)
    except (TypeError, ValueError) as error:
        raise ValueError("Dates must use YYYY-MM-DD format") from error
    if end_date < start_date:
        raise ValueError("End date must not be before start date")
    start = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return start, end_exclusive


def validate_selection(asset_symbols: list[str], timeframe: str) -> list[str]:
    assets = list(dict.fromkeys(symbol.strip().upper() for symbol in asset_symbols))
    if not assets:
        raise ValueError("Select at least one asset")
    unsupported = [symbol for symbol in assets if symbol not in ASSETS]
    if unsupported:
        raise ValueError(f"Unsupported assets: {', '.join(unsupported)}")
    if timeframe not in TIMEFRAMES:
        raise ValueError("Unsupported timeframe")
    return assets
