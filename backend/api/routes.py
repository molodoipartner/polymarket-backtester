from __future__ import annotations

import asyncio
import hmac
import json
import logging
import math
import queue
import re
import shutil
import tarfile
import threading
import time
import traceback
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any
from urllib.parse import urlencode

import httpx
from dotenv import dotenv_values
from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from backend.auth import FileSessionStore, hash_password, verify_password
from backend.backtester import FileBacktester
from backend.core.jobs import GenerationJobStore
from backend.data import DataCollector
from backend.fixed_time import (
    ASSETS,
    TIMEFRAMES,
    parse_period,
    parse_utc_datetime,
    validate_selection,
)
from backend.live_monitor import LIVE_STREAM_HISTORY_LIMIT, LiveMonitorManager
from backend.storage import FileStore
from backend.storage.file_store import utc_iso
from backend.strategy_generator import LiveMonitorStrategyGenerator, StrategyGroupGenerator
from backend.strategy_generator.live_monitor_generator import (
    ALL_LIVE_FAMILY_LABELS,
    SMART_LIVE_FAMILY_LABELS,
)
from backend.trading import PolymarketLiveOrderExecutor, PolymarketLiveOrderRequest
from config.settings import ROOT_DIR, settings

router = APIRouter()
templates = Jinja2Templates(directory=ROOT_DIR / "frontend" / "templates")
logger = logging.getLogger(__name__)
_PAPER_VISUAL_PREVIEW_LOCK = threading.Lock()
_PAPER_VISUAL_PREVIEWS: dict[str, dict] = {}
_PAPER_TOKEN_WARM_LOCK = threading.Lock()
_PAPER_WARMED_TOKEN_IDS: set[str] = set()
_PAPER_WARMING_TOKEN_IDS: set[str] = set()
LOCAL_DATA_TOOLS_CONFIG = ROOT_DIR / ".local_data_tools.json"
QUALITY_REPORT_CACHE_TTL_SECONDS = 900
QUALITY_CONTRACT_LIMITS = {
    "10": "Last 10",
    "25": "Last 25",
    "50": "Last 50",
    "100": "Last 100",
    "250": "Last 250",
    "500": "Last 500",
    "1000": "Last 1000",
    "all": "All contracts",
}


async def _stream_json(payload: Any) -> str:
    """Serialize an SSE payload without blocking live sampling or trade decisions."""
    return await asyncio.to_thread(
        json.dumps,
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _normalize_quality_contract_limit(value: str | int | None) -> str:
    normalized = str(value or "250").strip().lower()
    return normalized if normalized in QUALITY_CONTRACT_LIMITS else "250"


def _quality_contract_limit_value(value: str) -> int | None:
    normalized = _normalize_quality_contract_limit(value)
    return None if normalized == "all" else int(normalized)


def _parse_quality_update_pairs(values: list[str]) -> list[tuple[str, str]]:
    updates: list[tuple[str, str]] = []
    for value in values:
        record_id, separator, payload = str(value).partition("|")
        record_id = record_id.strip()
        payload = payload.strip()
        if separator and record_id and payload:
            updates.append((record_id, payload))
    return updates


def _quality_report_cache(request: Request) -> dict:
    cache = getattr(request.app.state, "quality_report_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        request.app.state.quality_report_cache = cache
    now = time.time()
    expired = [
        key for key, value in cache.items()
        if now - float(value.get("created_at") or 0) > QUALITY_REPORT_CACHE_TTL_SECONDS
    ]
    for key in expired:
        cache.pop(key, None)
    return cache


def _save_quality_report(request: Request, user_id: str, payload: dict) -> str:
    report_id = uuid.uuid4().hex
    _quality_report_cache(request)[report_id] = {
        "created_at": time.time(),
        "user_id": user_id,
        **payload,
    }
    return report_id


def _read_quality_report(request: Request, user_id: str, report_id: str | None) -> dict | None:
    if not report_id:
        return None
    cached = _quality_report_cache(request).get(str(report_id))
    if not isinstance(cached, dict) or cached.get("user_id") != user_id:
        return None
    return cached


def _quality_summary(report: dict | None, target_report: dict | None) -> dict:
    report = report or {}
    target_report = target_report or {}
    return {
        "matching": report.get("matching") or target_report.get("matching_total") or 0,
        "scanned": report.get("scanned") or target_report.get("matching") or 0,
        "local_problem_contracts": report.get("problem_contracts") or 0,
        "target_mismatches": target_report.get("mismatch_contracts") or 0,
        "outcome_mismatches": target_report.get("outcome_mismatch_contracts") or 0,
        "missing_data": (
            len(report.get("missing_data_problem_ids") or [])
            + len(target_report.get("unavailable_problem_ids") or [])
        ),
        "unavailable": target_report.get("unavailable_contracts") or 0,
        "rate_limited": target_report.get("rate_limited") or 0,
    }


def _quality_timeline(report: dict | None, target_report: dict | None) -> list[dict]:
    buckets: dict[int, dict] = {}

    def add(timestamp: Any, key: str) -> None:
        parsed = _optional_float(str(timestamp or ""), "timestamp")
        if parsed is None:
            return
        bucket_ts = int(parsed // 3600 * 3600)
        bucket = buckets.setdefault(bucket_ts, {
            "ts": bucket_ts,
            "time": datetime.fromtimestamp(bucket_ts, timezone.utc).strftime("%m-%d %H:%M"),
            "local": 0,
            "target": 0,
            "outcome": 0,
            "missing": 0,
            "total": 0,
        })
        bucket[key] += 1
        bucket["total"] += 1

    for item in (report or {}).get("issues") or []:
        add(item.get("window_start_ts"), "local")
    for item in (target_report or {}).get("mismatches") or []:
        add(item.get("window_start_ts"), "target")
    for item in (target_report or {}).get("outcome_mismatches") or []:
        add(item.get("window_start_ts"), "outcome")
    for item in (target_report or {}).get("unavailable") or []:
        add(item.get("window_start_ts"), "missing")
    timeline = sorted(buckets.values(), key=lambda item: item["ts"])
    max_total = max((item["total"] for item in timeline), default=0)
    first_ts = timeline[0]["ts"] if timeline else 0
    last_ts = timeline[-1]["ts"] if timeline else 0
    span = max(3600, last_ts - first_ts + 3600)
    for item in timeline:
        item["pct"] = round(item["total"] / max_total * 100, 1) if max_total else 0
        item["offset_pct"] = round((item["ts"] - first_ts) / span * 100, 3)
        item["width_pct"] = round(max(0.8, 3600 / span * 100), 3)
        item["height_pct"] = max(8, item["pct"]) if max_total else 0
    return timeline


def _quality_timeline_range(timeline: list[dict]) -> dict:
    if not timeline:
        return {"start": None, "end": None}
    return {
        "start": timeline[0].get("time"),
        "end": timeline[-1].get("time"),
    }


def _live_monitor_session_payload(session, history_limit: int | None = LIVE_STREAM_HISTORY_LIMIT) -> dict:
    return {
        "session_id": session.id,
        "events_url": f"/api/live-monitors/{session.id}/events",
        "stop_url": f"/api/live-monitors/{session.id}/stop",
        "snapshot": session.snapshot(history_limit=history_limit),
    }


def _store(request: Request) -> FileStore:
    return request.app.state.store


def _is_admin(request: Request) -> bool:
    return request.session.get("role") == "admin"


def _user(request: Request) -> dict | None:
    if _is_admin(request):
        return {"id": "admin", "display_name": settings.admin_username, "is_admin": True}
    user_id = request.session.get("user_id")
    return _store(request).get_user(user_id) if user_id else None


def _context(request: Request, **values) -> dict:
    return {
        "request": request,
        "app_name": settings.app_name,
        "asset_version": _asset_version(),
        "user": _user(request),
        "is_admin": _is_admin(request),
        "local_data_tools": _local_data_tools_available(request),
        "message": request.session.pop("message", None),
        "error": request.session.pop("error", None),
        **values,
    }


def _asset_version() -> str:
    paths = (
        ROOT_DIR / "frontend" / "static" / "css" / "app.css",
        ROOT_DIR / "frontend" / "static" / "js" / "app.js",
    )
    try:
        return str(max(path.stat().st_mtime_ns for path in paths))
    except OSError:
        return "dev"


def _local_data_tools_available(request: Request) -> bool:
    if settings.environment.lower() == "production":
        return False
    host = request.client.host if request.client else ""
    url_host = request.url.hostname or ""
    return host in {"127.0.0.1", "::1", "testclient"} or url_host in {
        "127.0.0.1", "localhost", "::1", "testserver",
    }


def _require_local_data_tools(request: Request) -> None:
    if not _local_data_tools_available(request):
        raise HTTPException(404, "Local data tools are available only in local development")


def _local_env_login_credentials() -> tuple[str, str]:
    values = dotenv_values(ROOT_DIR / ".env")
    username = str(values.get("USERNAME") or settings.username).strip()
    password = str(values.get("PASSWORD") or settings.password)
    if not username or not password:
        raise HTTPException(400, "USERNAME and PASSWORD must be set in .env")
    return username, password


def _ensure_stability(strategy: dict) -> None:
    metrics = strategy["metrics"]
    if "stability_score" not in metrics:
        values = [float(point["value"]) for point in strategy.get("equity_curve", [])]
        metrics.update(FileBacktester.stability_metrics(
            values, float(metrics["initial_capital"]), float(metrics["final_capital"]),
            float(metrics["sharpe_ratio"]), float(metrics["max_drawdown"]),
        ))
    metrics.setdefault("contracts_used", metrics.get("trade_count", 0))
    metrics.setdefault("up_trade_count", 0)
    metrics.setdefault("down_trade_count", 0)
    metrics.setdefault("winning_trade_count", 0)
    metrics.setdefault("losing_trade_count", 0)
    metrics.setdefault("flat_trade_count", 0)
    metrics.setdefault(
        "skipped_contract_count",
        max(0, int(metrics.get("event_count", 0)) - int(metrics.get("contracts_used", 0))),
    )
    metrics.setdefault("average_allocation_pct", 0.0)
    metrics.setdefault("max_allocation_pct_used", 0.0)


def _ensure_contract_summary(group: dict, strategy: dict | None = None) -> None:
    coverage = group.get("coverage", {})
    group.setdefault(
        "contracts_total",
        sum(
            int(stats.get("expected_event_count", stats.get("event_count", 0)))
            for stats in coverage.values()
        ),
    )
    group.setdefault(
        "contracts_missing",
        sum(int(stats.get("missing_event_count", 0)) for stats in coverage.values()),
    )
    group.setdefault(
        "contracts_available",
        sum(int(stats.get("event_count", 0)) for stats in coverage.values()),
    )
    if strategy is not None:
        metrics = strategy["metrics"]
        metrics.setdefault("event_count", group.get("contracts_available", group.get("event_count", 0)))
        metrics.setdefault("contracts_total", group.get("contracts_total", metrics["event_count"]))
        metrics.setdefault("contracts_missing", group.get("contracts_missing", 0))
        metrics.setdefault("contracts_available", group.get("contracts_available", metrics["event_count"]))


def _live_retest_summary_from_group(group: dict) -> dict | None:
    summary = dict(group.get("saved_contract_summary") or {})
    if not summary:
        return None
    contracts = summary.get("contracts")
    if not isinstance(contracts, list) or not contracts:
        return None
    required = ("range_start_ts", "range_end_ts", "selected_start_ts", "selected_end_ts")
    if any(summary.get(key) is None for key in required):
        return None
    return summary


def _sample_series(rows: list[dict], limit: int = 1200) -> list[dict]:
    if len(rows) <= limit or limit < 2:
        return rows
    last_index = len(rows) - 1
    return [rows[round(index * last_index / (limit - 1))] for index in range(limit)]


def _trade_summary_from_metrics(metrics: dict) -> dict[str, int]:
    wins = int(metrics.get("winning_trade_count", 0))
    losses = int(metrics.get("losing_trade_count", 0))
    trade_count = int(metrics.get("trade_count", 0))
    return {
        "wins": wins,
        "losses": losses,
        "flat": int(metrics.get("flat_trade_count", max(0, trade_count - wins - losses))),
        "up": int(metrics.get("up_trade_count", 0)),
        "down": int(metrics.get("down_trade_count", 0)),
    }


def _family_profit_summary(strategies: list[dict]) -> list[dict]:
    buckets: dict[str, dict] = {}
    phases = (1, 2, 3)
    for strategy in strategies:
        key = strategy.get("family") or "unknown"
        metrics = strategy.get("metrics", {})
        bucket = buckets.setdefault(
            key,
            {
                "family": key,
                "family_label": strategy.get("family_label") or key.replace("_", " ").title(),
                "strategy_count": 0,
                "profitable_count": 0,
                "total_roi": 0.0,
                "best_roi": None,
                "total_trades": 0,
                "total_net_profit": 0.0,
                "phases": {
                    phase: {
                        "phase": phase,
                        "tested_count": 0,
                        "profitable_count": 0,
                        "total_roi": 0.0,
                        "best_roi": None,
                    }
                    for phase in phases
                },
            },
        )
        roi = float(metrics.get("roi", 0) or 0)
        bucket["strategy_count"] += 1
        bucket["profitable_count"] += int(roi > 0)
        bucket["total_roi"] += roi
        bucket["best_roi"] = roi if bucket["best_roi"] is None else max(bucket["best_roi"], roi)
        bucket["total_trades"] += int(metrics.get("trade_count", 0) or 0)
        bucket["total_net_profit"] += float(metrics.get("net_profit", 0) or 0)
        for phase in phases:
            phase_roi = _number_or_none(metrics.get(f"phase_{phase}_roi"))
            if phase_roi is None:
                continue
            phase_bucket = bucket["phases"][phase]
            phase_bucket["tested_count"] += 1
            phase_bucket["profitable_count"] += int(phase_roi > 0)
            phase_bucket["total_roi"] += phase_roi
            phase_bucket["best_roi"] = (
                phase_roi
                if phase_bucket["best_roi"] is None
                else max(phase_bucket["best_roi"], phase_roi)
            )
    rows = []
    for bucket in buckets.values():
        count = max(1, int(bucket["strategy_count"]))
        phase_rows = []
        has_phase_metrics = False
        for phase in phases:
            phase_bucket = bucket["phases"][phase]
            tested_count = int(phase_bucket["tested_count"])
            has_phase_metrics = has_phase_metrics or tested_count > 0
            average_roi = phase_bucket["total_roi"] / tested_count if tested_count else 0.0
            profitable_percent = (
                phase_bucket["profitable_count"] / tested_count * 100 if tested_count else 0.0
            )
            phase_rows.append({
                "phase": phase,
                "tested_count": tested_count,
                "profitable_count": int(phase_bucket["profitable_count"]),
                "average_roi": round(average_roi, 6),
                "best_roi": round(float(phase_bucket["best_roi"] or 0), 6),
                "profitable_percent": round(profitable_percent, 4),
            })
        bucket = {**bucket, "phases": phase_rows, "has_phase_metrics": has_phase_metrics}
        rows.append({
            **bucket,
            "average_roi": round(bucket["total_roi"] / count, 6),
            "best_roi": round(float(bucket["best_roi"] or 0), 6),
            "profitable_percent": round(bucket["profitable_count"] / count * 100, 4),
            "total_net_profit": round(bucket["total_net_profit"], 4),
        })
    rows = sorted(
        rows,
        key=lambda item: (
            item["phases"][2]["average_roi"],
            item["phases"][1]["average_roi"],
            item["phases"][0]["average_roi"],
            item["profitable_percent"],
            item["average_roi"],
            item["best_roi"],
            item["strategy_count"],
        ),
        reverse=True,
    )
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
        row["generated_count"] = row["strategy_count"]
        row["final_strategy_count"] = row["strategy_count"]
    return rows


def _smart_family_rows_from_strategies(
    strategies: list[dict],
    groups: list[dict] | None = None,
) -> list[dict]:
    buckets: dict[str, dict] = {
        family: {
            "family": family,
            "family_label": label,
            "strategy_count": 0,
            "final_strategy_count": 0,
            "generated_count": 0,
            "profitable_count": 0,
            "total_roi": 0.0,
            "best_roi": None,
            "worst_roi": None,
            "phase_tested_count": 0,
            "phase_profitable_count": 0,
            "phase_total_roi": 0.0,
            "phase_best_roi": None,
            "phase_worst_roi": None,
            "latest_phase": 0,
            "total_net_profit": 0.0,
            "total_trades": 0,
            "total_contracts_used": 0,
            "total_win_rate": 0.0,
            "total_stability": 0.0,
            "total_sharpe": 0.0,
            "groups": set(),
        }
        for family, label in SMART_LIVE_FAMILY_LABELS.items()
    }
    for group in groups or []:
        group_id = group.get("id")
        rankings = group.get("phase_validation", {}).get("family_rankings") or []
        for ranking in rankings:
            family = str(ranking.get("family") or "")
            if family not in SMART_LIVE_FAMILY_LABELS:
                continue
            bucket = buckets[family]
            bucket["generated_count"] += int(
                ranking.get("generated_count", ranking.get("strategy_count", 0)) or 0
            )
            if group_id:
                bucket["groups"].add(group_id)
            latest_phase = None
            for phase in ranking.get("phases") or []:
                if int(phase.get("tested_count", 0) or 0) > 0:
                    latest_phase = phase
            if not latest_phase:
                continue
            tested_count = int(latest_phase.get("tested_count", 0) or 0)
            average_roi = float(latest_phase.get("average_roi", 0) or 0)
            bucket["phase_tested_count"] += tested_count
            bucket["phase_profitable_count"] += int(latest_phase.get("profitable_count", 0) or 0)
            bucket["phase_total_roi"] += average_roi * tested_count
            bucket["latest_phase"] = max(bucket["latest_phase"], int(latest_phase.get("phase", 0) or 0))
            for source_key, target_key, reducer in (
                ("best_roi", "phase_best_roi", max),
                ("worst_roi", "phase_worst_roi", min),
            ):
                value = _number_or_none(latest_phase.get(source_key))
                if value is None:
                    continue
                bucket[target_key] = (
                    value if bucket[target_key] is None else reducer(bucket[target_key], value)
                )
    for strategy in strategies:
        family = str(strategy.get("family") or "")
        if family not in SMART_LIVE_FAMILY_LABELS:
            continue
        metrics = strategy.get("metrics", {})
        bucket = buckets[family]
        roi = float(metrics.get("roi", 0) or 0)
        bucket["strategy_count"] += 1
        bucket["final_strategy_count"] += 1
        if bucket["generated_count"] <= 0:
            bucket["generated_count"] += 1
        bucket["profitable_count"] += int(roi > 0)
        bucket["total_roi"] += roi
        bucket["best_roi"] = roi if bucket["best_roi"] is None else max(bucket["best_roi"], roi)
        bucket["worst_roi"] = roi if bucket["worst_roi"] is None else min(bucket["worst_roi"], roi)
        bucket["total_net_profit"] += float(metrics.get("net_profit", 0) or 0)
        bucket["total_trades"] += int(metrics.get("trade_count", 0) or 0)
        bucket["total_contracts_used"] += int(metrics.get("contracts_used", 0) or 0)
        bucket["total_win_rate"] += float(metrics.get("win_rate", 0) or 0)
        bucket["total_stability"] += float(metrics.get("stability_score", 0) or 0)
        bucket["total_sharpe"] += float(metrics.get("sharpe_ratio", 0) or 0)
        if strategy.get("group_id"):
            bucket["groups"].add(strategy["group_id"])

    rows = []
    for bucket in buckets.values():
        phase_tested_count = int(bucket["phase_tested_count"])
        final_count = int(bucket["final_strategy_count"])
        tested_count = phase_tested_count or final_count
        count = max(1, tested_count)
        uses_phase_metrics = phase_tested_count > 0
        total_roi = bucket["phase_total_roi"] if uses_phase_metrics else bucket["total_roi"]
        profitable_count = (
            bucket["phase_profitable_count"] if uses_phase_metrics else bucket["profitable_count"]
        )
        best_roi = bucket["phase_best_roi"] if uses_phase_metrics else bucket["best_roi"]
        worst_roi = bucket["phase_worst_roi"] if uses_phase_metrics else bucket["worst_roi"]
        rows.append({
            **bucket,
            "analysis_count": len(bucket["groups"]),
            "tested": tested_count > 0,
            "strategy_count": tested_count,
            "average_roi": round(total_roi / count, 6) if tested_count else 0.0,
            "best_roi": round(float(best_roi or 0), 6),
            "worst_roi": round(float(worst_roi or 0), 6),
            "profitable_percent": round(profitable_count / count * 100, 4) if tested_count else 0.0,
            "total_net_profit": round(bucket["total_net_profit"], 4),
            "average_net_profit": round(bucket["total_net_profit"] / max(1, final_count), 6) if final_count else 0.0,
            "average_win_rate": round(bucket["total_win_rate"] / max(1, final_count), 4) if final_count else 0.0,
            "average_stability": round(bucket["total_stability"] / max(1, final_count), 4) if final_count else 0.0,
            "average_sharpe": round(bucket["total_sharpe"] / max(1, final_count), 6) if final_count else 0.0,
            "groups": sorted(bucket["groups"]),
        })
    rows.sort(
        key=lambda item: (
            item["tested"],
            item["average_roi"],
            item["profitable_percent"],
            item["best_roi"],
            item["total_net_profit"],
            item["generated_count"],
            item["final_strategy_count"],
        ),
        reverse=True,
    )
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    return rows


def _smart_family_report(store: FileStore, user_id: str) -> dict:
    smart_groups = [
        group for group in store.list_user_records(user_id, "groups")
        if group.get("source") == "live_monitor_saved_contracts"
        and group.get("strategy_mode") == "smart"
    ]
    group_ids = {group["id"] for group in smart_groups if group.get("id")}
    strategies = [
        strategy for strategy in store.list_user_records(user_id, "strategies")
        if strategy.get("group_id") in group_ids
        and strategy.get("family") in SMART_LIVE_FAMILY_LABELS
    ]
    rows = _smart_family_rows_from_strategies(strategies, smart_groups)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "analysis_count": len(smart_groups),
        "strategy_count": len(strategies),
        "family_count": len(rows),
        "rows": rows,
        "copy_text": _smart_family_report_text(rows, len(smart_groups), len(strategies)),
    }


def _smart_family_report_text(rows: list[dict], analysis_count: int, strategy_count: int) -> str:
    lines = [
        "Smart live-monitor family report",
        f"Analyses: {analysis_count}",
        f"Strategies: {strategy_count}",
        "",
    ]
    if not rows:
        lines.append("No smart family statistics have been collected yet.")
        return "\n".join(lines)
    for row in rows:
        if not row.get("tested"):
            lines.append(
                f"{row['rank']}. {row['family_label']} ({row['family']}): not tested yet."
            )
            continue
        lines.append(
            (
                f"{row['rank']}. {row['family_label']} ({row['family']}): "
                f"avg ROI {row['average_roi']:+.2f}%, best {row['best_roi']:+.2f}%, "
                f"worst {row['worst_roi']:+.2f}%, profitable "
                f"{row['profitable_percent']:.1f}%, tested {row['strategy_count']}, "
                f"generated {row['generated_count']}, final {row['final_strategy_count']}, "
                f"analyses {row['analysis_count']}, trades {row['total_trades']}, "
                f"net profit {row['total_net_profit']:+.2f}, avg win rate "
                f"{row['average_win_rate']:.1f}%, avg stability {row['average_stability']:.1f}."
            )
        )
    lines.extend([
        "",
        "Please analyze which smart strategy families look strongest overall, which look weak, "
        "and what I should test next.",
    ])
    return "\n".join(lines)


def _record_smart_family_report_snapshot(store: FileStore, user_id: str, group: dict) -> None:
    if group.get("source") != "live_monitor_saved_contracts" or group.get("strategy_mode") != "smart":
        return
    strategies = store.list_user_records_filtered_limited(
        user_id,
        "strategies",
        {"group_id": group["id"]},
        int(group.get("strategy_count", 0) or 0),
    )
    rows = _smart_family_rows_from_strategies(strategies, [group])
    store.append_user_record(user_id, "family_reports", {
        "group_id": group["id"],
        "group_name": group.get("name"),
        "strategy_mode": group.get("strategy_mode"),
        "source": group.get("source"),
        "analysis_created_at": group.get("created_at"),
        "analysis_count": 1,
        "strategy_count": len(strategies),
        "rows": rows,
    })


def _normalize_live_bid_exit_outcomes(live_monitor, user_id: str, transactions: list[dict]) -> None:
    for transaction in transactions:
        if str(transaction.get("resolved_outcome") or "").upper() != "BID_EXIT":
            continue
        saved_record_id = str(transaction.get("saved_record_id") or "").strip()
        if not saved_record_id:
            continue
        record = live_monitor.get_saved(user_id, saved_record_id)
        outcome = str(record.get("winning_outcome") or "").strip().upper() if record else ""
        if outcome in {"UP", "DOWN", "FLAT"}:
            transaction["resolved_outcome"] = outcome


def _number_or_none(value) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _live_strategy_family_options(strategy_mode: str) -> list[dict]:
    mode = LiveMonitorStrategyGenerator._validate_strategy_mode(strategy_mode)
    return [
        {"id": family, "label": ALL_LIVE_FAMILY_LABELS[family]}
        for family in LiveMonitorStrategyGenerator.available_families(mode)
    ]


def _selected_live_strategy_families(
    strategy_mode: str,
    strategy_families: list[str] | None,
    *,
    filter_enabled: bool = False,
) -> tuple[str, ...] | None:
    if not filter_enabled and not strategy_families:
        return None
    return LiveMonitorStrategyGenerator._validate_strategy_families(
        strategy_mode,
        strategy_families or [],
    )


def _reconstructed_strategy_transactions(
    store: FileStore, group: dict, strategy: dict
) -> list[dict]:
    start, end = parse_period(group["period"]["start_date"], group["period"]["end_date"])
    events = store.list_fixed_events(group["assets"], group["timeframe"], start, end)
    if not events:
        return []
    result = FileBacktester().run(
        events, dict(strategy["parameters"]), float(group["starting_balance"])
    )
    return [
        {"strategy_id": strategy["id"], "group_id": group["id"], **transaction}
        for transaction in result["transactions"]
    ]


def _error_message(error: BaseException) -> str:
    text = str(error).strip()
    if text:
        return text
    return error.__class__.__name__


def _form_float(value, field: str) -> float | None:
    return _optional_float(str(value or ""), field)


def _form_int(value, field: str, default: int = 0) -> int:
    return _optional_int(str(value or ""), field) or default


def _require_user(request: Request) -> dict:
    user = _user(request)
    if not user or _is_admin(request):
        raise HTTPException(401, "User authentication required")
    return user


def _require_admin(request: Request) -> None:
    if not _is_admin(request):
        raise HTTPException(403, "Administrator access required")


def _track_job_task(app, job_id: str, task: asyncio.Task) -> None:
    app.state.background_tasks.add(task)
    app.state.job_tasks[job_id] = task

    def cleanup(done: asyncio.Task) -> None:
        app.state.background_tasks.discard(done)
        app.state.job_tasks.pop(job_id, None)

    task.add_done_callback(cleanup)


def _background_tasks(app) -> set:
    tasks = getattr(app.state, "background_tasks", None)
    if not isinstance(tasks, set):
        tasks = set()
        app.state.background_tasks = tasks
    return tasks


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if _user(request):
        return RedirectResponse("/admin" if _is_admin(request) else "/", status_code=303)
    local_env_username = ""
    if _local_data_tools_available(request):
        local_env_username, _ = _local_env_login_credentials()
    return templates.TemplateResponse(
        request,
        "auth.html",
        _context(request, mode="login", local_env_username=local_env_username),
    )


@router.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request):
    if _user(request):
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(request, "auth.html", _context(request, mode="signup"))


@router.post("/signup")
def signup(
    request: Request,
    email: Annotated[str, Form()],
    display_name: Annotated[str, Form()],
    password: Annotated[str, Form()],
):
    try:
        user = _store(request).create_user(email, display_name, hash_password(password))
    except ValueError as error:
        request.session["error"] = str(error)
        return RedirectResponse("/signup", status_code=303)
    request.session.clear()
    request.session.update({"user_id": user["id"], "role": "user"})
    request.state.rotate_session = True
    return RedirectResponse("/", status_code=303)


@router.post("/login")
def login(
    request: Request,
    email: Annotated[str, Form()],
    password: Annotated[str, Form()],
):
    admin_match = bool(settings.admin_username and settings.admin_password) and hmac.compare_digest(
        email.strip(), settings.admin_username
    ) and hmac.compare_digest(password, settings.admin_password)
    if admin_match:
        request.session.clear()
        request.session["role"] = "admin"
        request.state.rotate_session = True
        _store(request).log_system("admin_login", {"username": settings.admin_username})
        return RedirectResponse("/admin", status_code=303)
    user = _store(request).find_user_by_login(email)
    if not user or not verify_password(password, user.get("password_hash", "")):
        request.session["error"] = "Invalid credentials"
        return RedirectResponse("/login", status_code=303)
    request.session.clear()
    request.session.update({"user_id": user["id"], "role": "user"})
    request.state.rotate_session = True
    _store(request).log(user["id"], "login")
    return RedirectResponse("/", status_code=303)


@router.post("/login/env")
def login_from_environment(request: Request):
    _require_local_data_tools(request)
    store = _store(request)
    username, password = _local_env_login_credentials()
    user = store.ensure_user(username, hash_password(password), "Test User")
    request.session.clear()
    request.session.update({"user_id": user["id"], "role": "user"})
    request.state.rotate_session = True
    store.log(user["id"], "local_env_login")
    return RedirectResponse("/", status_code=303)


@router.post("/logout")
def logout(request: Request):
    if _is_admin(request):
        _store(request).log_system("admin_logout", {"username": settings.admin_username})
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


def _account_recent_page(
    store: FileStore,
    user_id: str,
    collection: str,
    page: int,
    page_size: int,
) -> tuple[list[dict], bool]:
    """Read only enough newest JSONL rows for one account page."""
    return store.list_user_records_reverse_page(
        user_id, collection, {}, page, page_size
    )


@router.get("/account", response_class=HTMLResponse)
def account_page(request: Request):
    user = _user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    store = _store(request)
    account_tab = str(request.query_params.get("tab") or "overview").strip().lower()
    if account_tab not in {"overview", "real-trades", "real-console"}:
        account_tab = "overview"
    try:
        account_page_number = max(1, int(request.query_params.get("page") or 1))
    except (TypeError, ValueError):
        account_page_number = 1
    console_kind = str(request.query_params.get("kind") or "orders").strip().lower()
    if console_kind not in {"orders", "connections"}:
        console_kind = "orders"
    account = {
        "id": user.get("id"),
        "email": user.get("email", ""),
        "username": user.get("username", ""),
        "display_name": user.get("display_name", ""),
        "role": "admin" if _is_admin(request) else "user",
        "data_root": str(store.root),
    }
    show_overview = account_tab == "overview" or _is_admin(request)
    smart_family_report = (
        _smart_family_report(store, user["id"])
        if not _is_admin(request) and show_overview
        else {"rows": [], "copy_text": "", "analysis_count": 0, "strategy_count": 0}
    )
    paper_trading_groups = [
        group for group in sorted(
            store.list_user_records(user["id"], "groups"),
            key=lambda item: item.get("created_at", ""),
            reverse=True,
        )
        if group.get("source") == "paper_trading_live"
    ] if show_overview else []
    real_orders = []
    real_console = []
    account_has_next_page = False
    if not _is_admin(request) and account_tab in {"real-trades", "real-console"}:
        if account_tab == "real-trades":
            saved_orders, account_has_next_page = _account_recent_page(
                store, user["id"], "real_orders", account_page_number, 50
            )
            real_orders = [_real_order_account_row(item) for item in saved_orders]
        elif console_kind == "connections":
            real_console, account_has_next_page = _account_recent_page(
                store, user["id"], "real_trade_console", account_page_number, 100
            )
            real_console.reverse()
        else:
            saved_orders, account_has_next_page = _account_recent_page(
                store, user["id"], "real_orders", account_page_number, 30
            )
            real_console = sorted([
                entry
                for order in saved_orders
                for entry in _real_order_console_rows(order)
            ], key=lambda item: str(item.get("created_at") or ""))
    return templates.TemplateResponse(
        request,
        "account.html",
        _context(
            request,
            account=account,
            account_tab=account_tab,
            account_page_number=account_page_number,
            account_has_next_page=account_has_next_page,
            console_kind=console_kind,
            smart_family_report=smart_family_report,
            paper_trading_groups=paper_trading_groups,
            real_orders=real_orders,
            real_console=real_console,
        ),
    )


@router.get("/api/account/smart-family-report")
def account_smart_family_report(request: Request):
    user = _require_user(request)
    return _smart_family_report(_store(request), user["id"])


@router.get("/api/account/real-trades/balance-chart")
async def account_real_trades_balance_chart(
    request: Request,
    limit: Annotated[int, Query()] = 100,
):
    user = _require_user(request)
    if limit not in {25, 50, 100, 250, 500}:
        raise HTTPException(422, "Chart range must be 25, 50, 100, 250, or 500 orders")
    orders, _has_more = await asyncio.to_thread(
        _store(request).list_user_records_reverse_page,
        user["id"],
        "real_orders",
        {},
        1,
        limit,
    )
    return _real_trade_balance_chart_payload(list(reversed(orders)), limit)


@router.post("/account/clear-project-cache")
async def clear_project_cache(request: Request):
    user = _require_user(request)
    _require_local_data_tools(request)
    data_root = _store(request).root
    tasks = tuple(getattr(request.app.state, "background_tasks", set()))
    job_tasks = tuple(getattr(request.app.state, "job_tasks", {}).values())
    for task in tasks + job_tasks:
        task.cancel()
    if tasks or job_tasks:
        await asyncio.gather(*(tasks + job_tasks), return_exceptions=True)
    old_live_monitor = getattr(request.app.state, "live_monitor", None)
    old_collector = getattr(request.app.state, "collector", None)
    if old_live_monitor:
        await old_live_monitor.close()
    if old_collector:
        await old_collector.close()

    result = _store(request).clear_project_cache_preserving_accounts()
    store = FileStore(data_root)
    request.app.state.store = store
    request.app.state.collector = DataCollector(store)
    request.app.state.job_store = GenerationJobStore(store.root / "jobs")
    request.app.state.live_monitor = LiveMonitorManager(store.root / "live_monitor")
    request.app.state.background_tasks = set()
    request.app.state.job_tasks = {}
    store.log(user["id"], "project_cache_cleared", result)
    request.session["message"] = (
        "Project cache cleared. "
        f"Accounts preserved: {result['accounts_preserved']}; "
        f"records removed: {result['user_records_removed']}; "
        f"fixed events removed: {result['fixed_events_removed']}."
    )
    return RedirectResponse("/account", status_code=303)


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    if _is_admin(request):
        return RedirectResponse("/admin", status_code=303)
    user = _user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    groups = sorted(
        _store(request).list_user_records(user["id"], "groups"),
        key=lambda item: item.get("created_at", ""),
        reverse=True,
    )
    return templates.TemplateResponse(
        request, "welcome.html", _context(request, groups=groups, group_count=len(groups))
    )


@router.get("/generator")
def legacy_generator(request: Request):
    _require_user(request)
    return RedirectResponse("/groups/new", status_code=303)


@router.get("/groups/new", response_class=HTMLResponse)
def group_builder(request: Request):
    _require_user(request)
    default_end = date.today()
    return templates.TemplateResponse(
        request,
        "file_generator.html",
        _context(
            request, assets=ASSETS, timeframes=TIMEFRAMES,
            default_start=(default_end - timedelta(days=1)).isoformat(),
            default_end=default_end.isoformat(),
            default_balance=100.0,
        ),
    )


@router.get("/live-monitor", response_class=HTMLResponse)
def live_monitor_page(request: Request):
    _require_user(request)
    return templates.TemplateResponse(
        request,
        "live_monitor.html",
        _context(request),
    )


@router.get("/live-monitor/quality", response_class=HTMLResponse)
async def live_monitor_quality_page(
    request: Request,
    asset_symbol: str = Query("all"),
    timeframe: str = Query("5m"),
    contract_limit: str = Query("250"),
    report_id: str | None = Query(None),
    scan: bool = Query(False),
    check_targets: bool = Query(False),
):
    user = _require_user(request)
    selected_asset = asset_symbol.upper()
    if selected_asset != "ALL" and selected_asset not in ASSETS:
        selected_asset = "ALL"
    selected_timeframe = timeframe if timeframe in {"5m"} else "5m"
    selected_contract_limit = _normalize_quality_contract_limit(contract_limit)
    recent_limit = _quality_contract_limit_value(selected_contract_limit)
    report = None
    target_report = None
    cached = _read_quality_report(request, user["id"], report_id)
    if cached:
        report = cached.get("report")
        target_report = cached.get("target_report")
        selected_asset = cached.get("selected_asset") or selected_asset
        selected_timeframe = cached.get("selected_timeframe") or selected_timeframe
        selected_contract_limit = cached.get("selected_contract_limit") or selected_contract_limit
    elif scan or check_targets:
        category = None if selected_asset == "ALL" else selected_asset.lower()
        report, target_report = await asyncio.gather(
            asyncio.to_thread(
                request.app.state.live_monitor.scan_saved_quality,
                user["id"],
                category=category,
                timeframe=selected_timeframe,
                recent_limit=recent_limit,
            ),
            request.app.state.live_monitor.check_saved_external_targets(
                user["id"],
                category=category,
                timeframe=selected_timeframe,
                recent_limit=recent_limit,
            ),
        )
    return templates.TemplateResponse(
        request,
        "live_monitor_quality.html",
        _context(
            request,
            assets=ASSETS,
            timeframes={"5m": TIMEFRAMES["5m"]},
            contract_limits=QUALITY_CONTRACT_LIMITS,
            selected_asset=selected_asset,
            selected_timeframe=selected_timeframe,
            selected_contract_limit=selected_contract_limit,
            quality_report_id=report_id if cached else None,
            report=report,
            target_report=target_report,
            quality_summary=_quality_summary(report, target_report),
            quality_timeline=(quality_timeline := _quality_timeline(report, target_report)),
            quality_timeline_range=_quality_timeline_range(quality_timeline),
            scan=scan or bool(cached),
            check_targets=check_targets or scan or bool(cached),
        ),
    )


@router.get("/live-monitor/quality/progress")
async def live_monitor_quality_progress(
    request: Request,
    asset_symbol: str = Query("all"),
    timeframe: str = Query("5m"),
    contract_limit: str = Query("250"),
):
    user = _require_user(request)
    selected_asset = asset_symbol.upper()
    if selected_asset != "ALL" and selected_asset not in ASSETS:
        selected_asset = "ALL"
    selected_timeframe = timeframe if timeframe in {"5m"} else "5m"
    selected_contract_limit = _normalize_quality_contract_limit(contract_limit)
    recent_limit = _quality_contract_limit_value(selected_contract_limit)
    category = None if selected_asset == "ALL" else selected_asset.lower()
    queue: asyncio.Queue[dict | None] = asyncio.Queue()
    loop = asyncio.get_running_loop()

    async def publish(payload: dict) -> None:
        await queue.put(payload)

    def publish_from_thread(payload: dict) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, payload)

    async def run_scan() -> None:
        try:
            await publish({
                "stage": "local",
                "message": "Reading saved contracts and checking local history...",
            })
            report = await asyncio.to_thread(
                request.app.state.live_monitor.scan_saved_quality,
                user["id"],
                category=category,
                timeframe=selected_timeframe,
                recent_limit=recent_limit,
                progress=publish_from_thread,
            )
            await publish({
                "stage": "local",
                "done": report.get("scanned"),
                "total": report.get("scanned"),
                "remaining": 0,
                "message": (
                    f"Local quality checked: {report.get('scanned')} scanned, "
                    f"{report.get('problem_contracts')} problem contract(s)."
                ),
            })
            await publish({
                "stage": "gamma",
                "done": 0,
                "total": None,
                "remaining": None,
                "message": "Starting Gamma target and outcome checks...",
            })
            target_report = await request.app.state.live_monitor.check_saved_external_targets(
                user["id"],
                category=category,
                timeframe=selected_timeframe,
                recent_limit=recent_limit,
                progress=publish,
            )
            report_id = _save_quality_report(
                request,
                user["id"],
                {
                    "report": report,
                    "target_report": target_report,
                    "selected_asset": selected_asset,
                    "selected_timeframe": selected_timeframe,
                    "selected_contract_limit": selected_contract_limit,
                },
            )
            report_url = "/live-monitor/quality?" + urlencode({
                "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
                "timeframe": selected_timeframe,
                "contract_limit": selected_contract_limit,
                "report_id": report_id,
            })
            await publish({
                "stage": "complete",
                "done": target_report.get("unique_slugs"),
                "total": target_report.get("unique_slugs"),
                "remaining": 0,
                "report_url": report_url,
                "message": "Quality scan complete. Opening report...",
            })
        except Exception as exc:
            await publish({
                "stage": "error",
                "message": str(exc) or exc.__class__.__name__,
            })
        finally:
            await queue.put(None)

    task = asyncio.create_task(run_scan())

    async def stream():
        try:
            yield "retry: 1000\n\n"
            while True:
                if await request.is_disconnected():
                    task.cancel()
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=2)
                except asyncio.TimeoutError:
                    yield (
                        "event: ping\n"
                        f"data: {json.dumps({'server_time': time.time()})}\n\n"
                    )
                    continue
                if message is None:
                    break
                yield (
                    "event: progress\n"
                    f"data: {json.dumps(message, ensure_ascii=False)}\n\n"
                )
        finally:
            if not task.done():
                task.cancel()

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/live-monitor/quality/delete-problems")
async def delete_live_monitor_quality_problems(
    request: Request,
    asset_symbol: Annotated[str, Form()] = "all",
    timeframe: Annotated[str, Form()] = "5m",
    contract_limit: Annotated[str, Form()] = "250",
    report_id: Annotated[str, Form()] = "",
):
    user = _require_user(request)
    selected_asset = asset_symbol.upper()
    if selected_asset != "ALL" and selected_asset not in ASSETS:
        selected_asset = "ALL"
    selected_timeframe = timeframe if timeframe in {"5m"} else "5m"
    selected_contract_limit = _normalize_quality_contract_limit(contract_limit)
    form = await request.form()
    single_record_id = str(form.get("single_record_id") or "").strip()
    if single_record_id:
        deleted = 1 if request.app.state.live_monitor.delete_saved(
            user["id"],
            single_record_id,
        ) else 0
        request.session["message"] = (
            f"Deleted {deleted} saved live-monitor contract(s)."
        )
        params = urlencode({
            "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
            "timeframe": selected_timeframe,
            "contract_limit": selected_contract_limit,
        })
        return RedirectResponse(f"/live-monitor/quality?{params}", status_code=303)

    problem_ids = form.getlist("problem_ids")
    if problem_ids:
        deleted = request.app.state.live_monitor.delete_saved_items(
            user["id"],
            problem_ids,
        )
        request.session["message"] = (
            f"Deleted {deleted} problematic saved live-monitor contract(s)."
        )
        params = urlencode({
            "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
            "timeframe": selected_timeframe,
            "contract_limit": selected_contract_limit,
        })
        return RedirectResponse(f"/live-monitor/quality?{params}", status_code=303)

    quality_deleted = request.app.state.live_monitor.delete_saved_quality_issues(
        user["id"],
        category=None if selected_asset == "ALL" else selected_asset.lower(),
        timeframe=selected_timeframe,
    )
    request.session["message"] = (
        f"Deleted {quality_deleted} local quality issue contract(s). "
        "No Gamma problem ids were submitted for deletion."
    )
    params = urlencode({
        "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
        "timeframe": selected_timeframe,
        "contract_limit": selected_contract_limit,
    })
    return RedirectResponse(f"/live-monitor/quality?{params}", status_code=303)


@router.post("/live-monitor/quality/delete-gamma-results")
async def delete_live_monitor_gamma_results(
    request: Request,
    asset_symbol: Annotated[str, Form()] = "all",
    timeframe: Annotated[str, Form()] = "5m",
    contract_limit: Annotated[str, Form()] = "250",
):
    user = _require_user(request)
    selected_asset = asset_symbol.upper()
    if selected_asset != "ALL" and selected_asset not in ASSETS:
        selected_asset = "ALL"
    selected_timeframe = timeframe if timeframe in {"5m"} else "5m"
    selected_contract_limit = _normalize_quality_contract_limit(contract_limit)
    deleted = await request.app.state.live_monitor.delete_saved_external_target_issues(
        user["id"],
        category=None if selected_asset == "ALL" else selected_asset.lower(),
        timeframe=selected_timeframe,
        recent_limit=_quality_contract_limit_value(selected_contract_limit),
    )
    request.session["message"] = (
        f"Deleted {deleted} saved live-monitor contract(s) from the Gamma check results."
    )
    params = urlencode({
        "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
        "timeframe": selected_timeframe,
        "contract_limit": selected_contract_limit,
        "check_targets": "1",
    })
    return RedirectResponse(f"/live-monitor/quality?{params}", status_code=303)


@router.post("/live-monitor/quality/delete-missing-data")
async def delete_live_monitor_missing_data(
    request: Request,
    asset_symbol: Annotated[str, Form()] = "all",
    timeframe: Annotated[str, Form()] = "5m",
    contract_limit: Annotated[str, Form()] = "250",
    report_id: Annotated[str, Form()] = "",
):
    user = _require_user(request)
    selected_asset = asset_symbol.upper()
    if selected_asset != "ALL" and selected_asset not in ASSETS:
        selected_asset = "ALL"
    selected_timeframe = timeframe if timeframe in {"5m"} else "5m"
    selected_contract_limit = _normalize_quality_contract_limit(contract_limit)
    form = await request.form()
    missing_ids = form.getlist("missing_data_ids")
    deleted = request.app.state.live_monitor.delete_saved_items(
        user["id"],
        missing_ids,
    )
    request.session["message"] = (
        f"Deleted {deleted} saved live-monitor contract(s) with missing data."
    )
    params = urlencode({
        "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
        "timeframe": selected_timeframe,
        "contract_limit": selected_contract_limit,
    })
    return RedirectResponse(f"/live-monitor/quality?{params}", status_code=303)


@router.post("/live-monitor/quality/delete-gamma-target-missing")
async def delete_live_monitor_gamma_target_missing(
    request: Request,
    asset_symbol: Annotated[str, Form()] = "all",
    timeframe: Annotated[str, Form()] = "5m",
    contract_limit: Annotated[str, Form()] = "250",
    report_id: Annotated[str, Form()] = "",
):
    user = _require_user(request)
    selected_asset = asset_symbol.upper()
    if selected_asset != "ALL" and selected_asset not in ASSETS:
        selected_asset = "ALL"
    selected_timeframe = timeframe if timeframe in {"5m"} else "5m"
    selected_contract_limit = _normalize_quality_contract_limit(contract_limit)
    form = await request.form()
    target_missing_ids = form.getlist("gamma_target_missing_ids")
    deleted = request.app.state.live_monitor.delete_saved_items(
        user["id"],
        target_missing_ids,
    )
    request.session["message"] = (
        f"Deleted {deleted} saved live-monitor contract(s) where Gamma did not provide priceToBeat."
    )
    params = urlencode({
        "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
        "timeframe": selected_timeframe,
        "contract_limit": selected_contract_limit,
    })
    return RedirectResponse(f"/live-monitor/quality?{params}", status_code=303)


@router.post("/live-monitor/quality/update-gamma-outcomes")
async def update_live_monitor_gamma_outcomes(
    request: Request,
    asset_symbol: Annotated[str, Form()] = "all",
    timeframe: Annotated[str, Form()] = "5m",
    contract_limit: Annotated[str, Form()] = "250",
    report_id: Annotated[str, Form()] = "",
):
    user = _require_user(request)
    selected_asset = asset_symbol.upper()
    if selected_asset != "ALL" and selected_asset not in ASSETS:
        selected_asset = "ALL"
    selected_timeframe = timeframe if timeframe in {"5m"} else "5m"
    selected_contract_limit = _normalize_quality_contract_limit(contract_limit)
    form = await request.form()
    updates = _parse_quality_update_pairs(form.getlist("outcome_updates"))
    if updates:
        updated = request.app.state.live_monitor.update_saved_outcome_items(
            user["id"],
            updates,
        )
    else:
        updated = await request.app.state.live_monitor.update_saved_external_outcome_issues(
            user["id"],
            category=None if selected_asset == "ALL" else selected_asset.lower(),
            timeframe=selected_timeframe,
            recent_limit=_quality_contract_limit_value(selected_contract_limit),
        )
    request.session["message"] = (
        f"Updated {updated} saved live-monitor resolution outcome(s) from Gamma."
    )
    params = urlencode({
        "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
        "timeframe": selected_timeframe,
        "contract_limit": selected_contract_limit,
        **({"report_id": report_id} if report_id else {}),
    })
    return RedirectResponse(f"/live-monitor/quality?{params}", status_code=303)


@router.post("/live-monitor/quality/update-gamma-targets")
async def update_live_monitor_gamma_targets(
    request: Request,
    asset_symbol: Annotated[str, Form()] = "all",
    timeframe: Annotated[str, Form()] = "5m",
    contract_limit: Annotated[str, Form()] = "250",
    report_id: Annotated[str, Form()] = "",
):
    user = _require_user(request)
    selected_asset = asset_symbol.upper()
    if selected_asset != "ALL" and selected_asset not in ASSETS:
        selected_asset = "ALL"
    selected_timeframe = timeframe if timeframe in {"5m"} else "5m"
    selected_contract_limit = _normalize_quality_contract_limit(contract_limit)
    form = await request.form()
    target_updates = []
    for record_id, value in _parse_quality_update_pairs(form.getlist("target_updates")):
        parsed = _optional_float(value, "target_updates")
        if parsed is not None:
            target_updates.append((record_id, parsed))
    updated = request.app.state.live_monitor.update_saved_target_items(
        user["id"],
        target_updates,
    )
    request.session["message"] = (
        f"Updated {updated} saved live-monitor target price(s) from Gamma."
    )
    params = urlencode({
        "asset_symbol": selected_asset.lower() if selected_asset == "ALL" else selected_asset,
        "timeframe": selected_timeframe,
        "contract_limit": selected_contract_limit,
        **({"report_id": report_id} if report_id else {}),
    })
    return RedirectResponse(f"/live-monitor/quality?{params}", status_code=303)


@router.get("/live-monitor/analysis", response_class=HTMLResponse)
def live_monitor_analysis_page(
    request: Request,
    asset_symbol: str = Query("BTC"),
    timeframe: str = Query("5m"),
    strategy_mode: str = Query("lightweight"),
    strategy_families: list[str] = Query(default=[]),
    include_partial_contracts: bool = Query(False),
    start_ts: float | None = Query(None),
    end_ts: float | None = Query(None),
    phase_1_contracts: int = Query(0),
    phase_2_contracts: int = Query(0),
    phase_3_contracts: int = Query(0),
):
    _require_user(request)
    selected_asset = asset_symbol.upper() if asset_symbol.upper() in ASSETS else "BTC"
    selected_timeframe = timeframe if timeframe in TIMEFRAMES else "5m"
    selected_strategy_mode = (
        strategy_mode if strategy_mode in {"lightweight", "smart"} else "lightweight"
    )
    try:
        selected_families = _selected_live_strategy_families(
            selected_strategy_mode,
            strategy_families,
            filter_enabled=bool(strategy_families),
        )
    except ValueError:
        selected_families = None
    family_options = _live_strategy_family_options(selected_strategy_mode)
    return templates.TemplateResponse(
        request,
        "live_monitor_analysis.html",
        _context(
            request,
            assets=ASSETS,
            timeframes={"5m": TIMEFRAMES["5m"]},
            selected_asset=selected_asset,
            selected_timeframe=selected_timeframe,
            selected_strategy_mode=selected_strategy_mode,
            strategy_family_options=family_options,
            strategy_family_options_by_mode={
                "lightweight": _live_strategy_family_options("lightweight"),
                "smart": _live_strategy_family_options("smart"),
            },
            selected_strategy_families=set(
                selected_families or [option["id"] for option in family_options]
            ),
            include_partial_contracts=include_partial_contracts,
            selected_start_ts=start_ts,
            selected_end_ts=end_ts,
            phase_1_contracts=max(0, phase_1_contracts),
            phase_2_contracts=max(0, phase_2_contracts),
            phase_3_contracts=max(0, phase_3_contracts),
            default_balance=100.0,
        ),
    )


@router.get("/api/live-monitor/analysis/summary")
def live_monitor_analysis_summary(
    request: Request,
    asset_symbol: str = Query("BTC"),
    timeframe: str = Query("5m"),
    include_partial_contracts: bool = Query(False),
    start_ts: float | None = Query(None),
    end_ts: float | None = Query(None),
    include_contracts: bool = Query(False),
):
    user = _require_user(request)
    generator = LiveMonitorStrategyGenerator(_store(request), request.app.state.live_monitor)
    try:
        summary = generator.saved_contract_summary(
            user["id"],
            asset_symbol,
            timeframe,
            include_partial_contracts,
            start_ts,
            end_ts,
            include_contracts,
        )
    except ValueError as error:
        raise HTTPException(422, _error_message(error)) from error
    return {
        "summary": summary,
        "selected_contract_count": summary["selected_contracts"],
    }


@router.post("/live-monitor/analysis")
def create_live_monitor_analysis_group(
    request: Request,
    group_name: Annotated[str, Form()],
    asset_symbol: Annotated[str, Form()],
    timeframe: Annotated[str, Form()],
    strategy_count: Annotated[int, Form()],
    starting_balance: Annotated[float, Form()],
    strategy_mode: Annotated[str, Form()] = "lightweight",
    strategy_families: Annotated[list[str] | None, Form()] = None,
    strategy_family_filter_enabled: Annotated[bool, Form()] = False,
    include_partial_contracts: Annotated[bool, Form()] = False,
    start_ts: Annotated[str, Form()] = "",
    end_ts: Annotated[str, Form()] = "",
    phase_1_contracts: Annotated[str, Form()] = "0",
    phase_2_contracts: Annotated[str, Form()] = "0",
    phase_3_contracts: Annotated[str, Form()] = "0",
):
    user = _require_user(request)
    parsed_start_ts = _form_float(start_ts, "start_ts")
    parsed_end_ts = _form_float(end_ts, "end_ts")
    phase_counts = (
        max(0, _form_int(phase_1_contracts, "phase_1_contracts")),
        max(0, _form_int(phase_2_contracts, "phase_2_contracts")),
        max(0, _form_int(phase_3_contracts, "phase_3_contracts")),
    )
    try:
        selected_families = _selected_live_strategy_families(
            strategy_mode,
            strategy_families,
            filter_enabled=strategy_family_filter_enabled,
        )
        generator = LiveMonitorStrategyGenerator(_store(request), request.app.state.live_monitor)
        group = generator.generate(
            user["id"],
            group_name,
            asset_symbol,
            timeframe,
            bool(include_partial_contracts),
            strategy_count,
            starting_balance,
            parsed_start_ts,
            parsed_end_ts,
            strategy_mode,
            phase_contract_counts=phase_counts,
            strategy_families=selected_families,
        )
        _record_smart_family_report_snapshot(_store(request), user["id"], group)
        request.session["message"] = (
            f"Created {group['strategy_count']} live-monitor strategies from "
            f"{group['event_count']} saved contract(s)."
        )
        return RedirectResponse(f"/groups/{group['id']}", status_code=303)
    except (ValueError, OSError) as error:
        request.session["error"] = str(error)
    params = {
        "asset_symbol": asset_symbol,
        "timeframe": timeframe,
        "include_partial_contracts": "true" if include_partial_contracts else "false",
        "strategy_mode": strategy_mode,
    }
    for family in strategy_families or []:
        params.setdefault("strategy_families", [])
        params["strategy_families"].append(family)
    if parsed_start_ts is not None:
        params["start_ts"] = str(parsed_start_ts)
    if parsed_end_ts is not None:
        params["end_ts"] = str(parsed_end_ts)
    for key, value in (
        ("phase_1_contracts", phase_counts[0]),
        ("phase_2_contracts", phase_counts[1]),
        ("phase_3_contracts", phase_counts[2]),
    ):
        if value:
            params[key] = str(value)
    params = urlencode(params, doseq=True)
    return RedirectResponse(f"/live-monitor/analysis?{params}", status_code=303)


@router.post("/api/live-monitor/analysis/jobs")
async def start_live_monitor_analysis_job(
    request: Request,
    group_name: Annotated[str, Form()],
    asset_symbol: Annotated[str, Form()],
    timeframe: Annotated[str, Form()],
    strategy_count: Annotated[int, Form()],
    starting_balance: Annotated[float, Form()],
    strategy_mode: Annotated[str, Form()] = "lightweight",
    strategy_families: Annotated[list[str] | None, Form()] = None,
    strategy_family_filter_enabled: Annotated[bool, Form()] = False,
    include_partial_contracts: Annotated[bool, Form()] = False,
    start_ts: Annotated[str, Form()] = "",
    end_ts: Annotated[str, Form()] = "",
    phase_1_contracts: Annotated[str, Form()] = "0",
    phase_2_contracts: Annotated[str, Form()] = "0",
    phase_3_contracts: Annotated[str, Form()] = "0",
):
    user = _require_user(request)
    parsed_start_ts = _form_float(start_ts, "start_ts")
    parsed_end_ts = _form_float(end_ts, "end_ts")
    phase_counts = (
        max(0, _form_int(phase_1_contracts, "phase_1_contracts")),
        max(0, _form_int(phase_2_contracts, "phase_2_contracts")),
        max(0, _form_int(phase_3_contracts, "phase_3_contracts")),
    )
    try:
        selected_families = _selected_live_strategy_families(
            strategy_mode,
            strategy_families,
            filter_enabled=strategy_family_filter_enabled,
        )
    except ValueError as error:
        raise HTTPException(422, _error_message(error)) from error
    job = request.app.state.job_store.create(user["id"])
    task = asyncio.create_task(
        _run_live_monitor_analysis_job(
            request.app,
            job["id"],
            user["id"],
            group_name,
            asset_symbol,
            timeframe,
            bool(include_partial_contracts),
            strategy_count,
            starting_balance,
            parsed_start_ts,
            parsed_end_ts,
            strategy_mode,
            phase_counts,
            selected_families,
        )
    )
    _track_job_task(request.app, job["id"], task)
    return JSONResponse(
        {"job_id": job["id"], "events_url": f"/api/group-jobs/{job['id']}/events"},
        status_code=202,
    )


@router.post("/api/live-monitors")
async def start_live_monitor(
    request: Request,
    event_url: Annotated[str, Form()],
):
    user = _require_user(request)
    try:
        session = await request.app.state.live_monitor.start(
            user["id"],
            event_url,
        )
    except (httpx.HTTPError, ValueError, OSError) as error:
        raise HTTPException(422, _error_message(error)) from error
    return JSONResponse(_live_monitor_session_payload(session), status_code=202)


@router.get("/api/live-monitors/active")
def active_live_monitor(request: Request):
    user = _require_user(request)
    sessions = request.app.state.live_monitor.active_sessions(user["id"])
    if not sessions:
        return {"active": False}
    payloads = [_live_monitor_session_payload(session) for session in sessions]
    return {
        "active": True,
        "sessions": payloads,
        **payloads[0],
    }


@router.get("/api/live-monitors/{session_id}")
def live_monitor_snapshot(request: Request, session_id: str):
    user = _require_user(request)
    session = request.app.state.live_monitor.get(session_id)
    if not session or session.user_id != user["id"]:
        raise HTTPException(404, "Live monitor session not found")
    return session.snapshot()


@router.post("/api/live-monitors/{session_id}/stop")
async def stop_live_monitor(request: Request, session_id: str):
    user = _require_user(request)
    session = request.app.state.live_monitor.get(session_id)
    if not session or session.user_id != user["id"]:
        raise HTTPException(404, "Live monitor session not found")
    stopped = await request.app.state.live_monitor.stop(session_id)
    if not stopped:
        raise HTTPException(404, "Live monitor session not found")
    return JSONResponse({"status": "stopped"})


@router.get("/api/live-monitors/{session_id}/events")
async def live_monitor_events(request: Request, session_id: str):
    user = _require_user(request)
    session = request.app.state.live_monitor.get(session_id)
    if not session or session.user_id != user["id"]:
        raise HTTPException(404, "Live monitor session not found")
    queue = session.add_subscriber()

    async def stream():
        try:
            yield "retry: 1000\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=2)
                except asyncio.TimeoutError:
                    yield (
                        "event: ping\n"
                        f"data: {json.dumps({'server_time': time.time()})}\n\n"
                    )
                    continue
                encoded = await _stream_json(message["data"])
                yield (
                    f"event: {message['event']}\n"
                    f"data: {encoded}\n\n"
                )
        finally:
            session.remove_subscriber(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/live-monitor/saved")
def saved_live_monitors(
    request: Request,
    category: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
):
    user = _require_user(request)
    manager = request.app.state.live_monitor
    page_payload = manager.list_saved_page(
        user["id"],
        category=category,
        page=page,
        page_size=page_size,
    )
    return {
        "categories": manager.saved_categories(user["id"]),
        **page_payload,
        "selected_category": category or "all",
    }


@router.get("/api/live-monitor/saved/{record_id}")
def saved_live_monitor_detail(request: Request, record_id: str):
    user = _require_user(request)
    record = request.app.state.live_monitor.get_saved(user["id"], record_id)
    if not record:
        raise HTTPException(404, "Saved live monitor record not found")
    return {"record": record}


@router.delete("/api/live-monitor/saved/categories/{category}")
def delete_saved_live_monitor_category(request: Request, category: str):
    user = _require_user(request)
    deleted = request.app.state.live_monitor.delete_saved_category(
        user["id"],
        category,
    )
    return {"deleted": deleted}


@router.delete("/api/live-monitor/saved/{record_id}")
def delete_saved_live_monitor(request: Request, record_id: str):
    user = _require_user(request)
    deleted = request.app.state.live_monitor.delete_saved(user["id"], record_id)
    if not deleted:
        raise HTTPException(404, "Saved live monitor record not found")
    return {"deleted": True}


def _paper_trading_strategy_scope(
    request: Request, group_id: str, strategy_id: str
) -> tuple[dict, FileStore, dict, dict]:
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    strategy = store.get_user_record(user["id"], "strategies", strategy_id)
    if not group or not strategy or strategy.get("group_id") != group_id:
        raise HTTPException(404, "Strategy not found in this group")
    if group.get("source") != "live_monitor_saved_contracts":
        raise HTTPException(422, "Paper trading is available for live-monitor strategies")
    return user, store, group, strategy


def _paper_trade_list_record(record: dict) -> dict:
    return {
        key: record.get(key)
        for key in (
            "id",
            "group_id",
            "strategy_id",
            "source_group_id",
            "source_strategy_id",
            "strategy_name",
            "family",
            "saved_record_id",
            "question",
            "event_title",
            "crypto_symbol",
            "target_price",
            "final_crypto_price",
            "winning_outcome",
            "window_start_ts",
            "window_end_ts",
            "paper_mode",
            "trade_count",
            "net_profit",
            "roi",
            "initial_capital",
            "final_capital",
            "saved_at",
            "created_at",
        )
    } | {"history_count": record.get("history_count", 0)}


def _paper_run_metrics(transactions: list[dict], initial_capital: float) -> tuple[dict, list[dict]]:
    ordered = sorted(transactions, key=lambda item: str(item.get("closed_at") or item.get("created_at") or ""))
    balance = initial_capital
    equity_curve = [{"t": utc_iso(), "value": round(initial_capital, 4)}]
    profits = []
    for transaction in ordered:
        pnl = float(transaction.get("pnl", 0) or 0)
        balance += pnl
        profits.append(pnl)
        equity_curve.append({
            "t": transaction.get("closed_at") or transaction.get("created_at") or utc_iso(),
            "value": round(balance, 4),
        })
    gross_profit = sum(value for value in profits if value > 0)
    gross_loss = abs(sum(value for value in profits if value < 0))
    returns = [
        float(item.get("pnl", 0) or 0) / max(
            float(item.get("margin_locked", 0) or 0) + float(item.get("entry_fee", 0) or 0),
            1e-9,
        )
        for item in ordered
    ]
    max_drawdown = FileBacktester._max_drawdown([point["value"] for point in equity_curve])
    sharpe = LiveMonitorStrategyGenerator._sharpe(returns) if returns else 0.0
    stability = FileBacktester.stability_metrics(
        [point["value"] for point in equity_curve],
        initial_capital,
        balance,
        sharpe,
        max_drawdown,
    )
    metrics = {
        "initial_capital": round(initial_capital, 4),
        "final_capital": round(balance, 4),
        "net_profit": round(balance - initial_capital, 4),
        "roi": round((balance / initial_capital - 1) * 100, 6) if initial_capital else 0.0,
        "sharpe_ratio": round(sharpe, 6),
        "win_rate": round(sum(value > 0 for value in profits) / len(profits) * 100, 4) if profits else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        **stability,
        "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss else (999.0 if gross_profit else 0.0),
        "average_trade": round(sum(profits) / len(profits), 6) if profits else 0.0,
        "trade_count": len(ordered),
        "event_count": len({item.get("saved_record_id") or item.get("event_id") for item in ordered}),
        "contracts_used": len({item.get("saved_record_id") or item.get("event_id") for item in ordered}),
        "contracts_total": len({item.get("saved_record_id") or item.get("event_id") for item in ordered}),
        "contracts_available": len({item.get("saved_record_id") or item.get("event_id") for item in ordered}),
        "contracts_missing": 0,
        "up_trade_count": sum(str(item.get("side")).upper() == "UP" for item in ordered),
        "down_trade_count": sum(str(item.get("side")).upper() == "DOWN" for item in ordered),
        "winning_trade_count": sum(value > 0 for value in profits),
        "losing_trade_count": sum(value < 0 for value in profits),
        "flat_trade_count": sum(value == 0 for value in profits),
        "skipped_contract_count": 0,
        "average_allocation_pct": round(
            sum(float(item.get("allocation_pct_used", 0) or 0) for item in ordered) / max(1, len(ordered)) * 100,
            6,
        ) if ordered else 0.0,
        "max_allocation_pct_used": round(
            max((float(item.get("allocation_pct_used", 0) or 0) for item in ordered), default=0.0) * 100,
            6,
        ),
    }
    return metrics, equity_curve


def _rebalance_paper_transactions(
    store: FileStore,
    user_id: str,
    group_id: str,
    strategy_id: str,
    initial_capital: float,
) -> list[dict]:
    transactions = store.list_user_records_filtered(
        user_id,
        "transactions",
        {"group_id": group_id, "strategy_id": strategy_id},
    )
    transactions.sort(key=lambda item: str(item.get("closed_at") or item.get("opened_at") or item.get("created_at") or ""))
    balance = round(initial_capital, 8)
    rebalanced = []
    for transaction in transactions:
        pnl = round(float(transaction.get("pnl", 0) or 0), 8)
        balance_before = balance
        balance_after = round(balance_before + pnl, 8)
        updates = {
            "balance_before": balance_before,
            "balance_after": balance_after,
            "available_cash_after_close": balance_after,
        }
        if any(transaction.get(key) != value for key, value in updates.items()):
            store.update_user_record(user_id, "transactions", transaction["id"], updates)
            transaction = {**transaction, **updates}
        rebalanced.append(transaction)
        balance = balance_after
    return rebalanced


def _refresh_paper_run_summary(store: FileStore, user_id: str, group: dict, strategy: dict) -> None:
    initial_capital = float(group.get("starting_balance") or strategy.get("metrics", {}).get("initial_capital") or 100)
    transactions = _rebalance_paper_transactions(
        store, user_id, group["id"], strategy["id"], initial_capital
    )
    metrics, equity_curve = _paper_run_metrics(transactions, initial_capital)
    contract_count = len({item.get("saved_record_id") or item.get("event_id") for item in transactions})
    store.update_user_record(
        user_id,
        "strategies",
        strategy["id"],
        {"metrics": metrics, "equity_curve": equity_curve},
    )
    store.update_user_record(
        user_id,
        "groups",
        group["id"],
        {
            "event_count": contract_count,
            "contracts_total": contract_count,
            "contracts_available": contract_count,
            "contracts_missing": 0,
            "strategy_count": 1,
            "summary": {
                "best_roi": metrics["roi"],
                "average_roi": metrics["roi"],
                "profitable_strategies": int(metrics["roi"] > 0),
            },
            "coverage": {
                (group.get("assets") or ["LIVE"])[0]: {
                    "event_count": contract_count,
                    "expected_event_count": contract_count,
                    "missing_event_count": 0,
                    "coverage_percent": 100.0,
                    "missing_ranges": [],
                }
            },
        },
    )


def _paper_run_for_source(
    store: FileStore,
    user_id: str,
    source_group_id: str,
    source_strategy_id: str,
    run_group_id: str | None = None,
) -> list[tuple[dict, dict]]:
    groups = [
        group for group in store.list_user_records(user_id, "groups")
        if group.get("source") == "paper_trading_live"
        and group.get("paper_trading", {}).get("source_group_id") == source_group_id
        and group.get("paper_trading", {}).get("source_strategy_id") == source_strategy_id
        and (run_group_id is None or group.get("id") == run_group_id)
    ]
    result = []
    for group in groups:
        strategies = store.list_user_records_filtered_limited(
            user_id, "strategies", {"group_id": group["id"]}, 1
        )
        if strategies:
            result.append((group, strategies[0]))
    return result


def _paper_visible_runs_for_source(
    request: Request,
    store: FileStore,
    user_id: str,
    source_group_id: str,
    source_strategy_id: str,
) -> list[tuple[dict, dict]]:
    runs = _paper_run_for_source(store, user_id, source_group_id, source_strategy_id)
    active = []
    for group, strategy in runs:
        session_id = str(group.get("paper_trading", {}).get("session_id") or "")
        session = request.app.state.live_monitor.get(session_id)
        if session and session.user_id == user_id and not session.is_finished:
            active.append((group, strategy))
    if active:
        return active
    if not runs:
        return []
    return [
        max(
            runs,
            key=lambda pair: str(
                pair[0].get("paper_trading", {}).get("started_at")
                or pair[0].get("created_at")
                or ""
            ),
        )
    ]


def _create_paper_run_group(
    store: FileStore,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    session_id: str,
    initial_event: object | None = None,
    starting_balance: float | None = None,
    real_trading_enabled: bool = False,
    server_optimized: bool = False,
    balance_guard_enabled: bool = False,
    min_open_balance: float = 130.0,
) -> tuple[dict, dict]:
    now = utc_iso()
    group_id = uuid.uuid4().hex
    strategy_id = uuid.uuid4().hex
    asset = str((source_group.get("assets") or ["LIVE"])[0])
    timeframe = str(source_group.get("timeframe") or "live")
    starting_balance = float(
        starting_balance
        if starting_balance is not None
        else (
            source_group.get("starting_balance")
            or source_strategy.get("metrics", {}).get("initial_capital")
            or 100
        )
    )
    metrics, equity_curve = _paper_run_metrics([], starting_balance)
    group = {
        "id": group_id,
        "name": f"Paper trading: {source_strategy.get('name', 'Strategy')} {now[:19]}",
        "theme": "paper_trading_live",
        "source": "paper_trading_live",
        "strategy_mode": "paper",
        "assets": [asset],
        "asset_labels": source_group.get("asset_labels") or [asset],
        "timeframe": timeframe,
        "timeframe_label": source_group.get("timeframe_label") or timeframe,
        "period": {
            "start_date": now[:10],
            "end_date": now[:10],
            "start_time": now,
            "end_time_exclusive": now,
        },
        "starting_balance": round(starting_balance, 4),
        "strategy_count": 1,
        "event_count": 0,
        "contracts_total": 0,
        "contracts_missing": 0,
        "contracts_available": 0,
        "coverage": {
            asset: {
                "event_count": 0,
                "expected_event_count": 0,
                "missing_event_count": 0,
                "coverage_percent": 100.0,
                "missing_ranges": [],
            }
        },
        "summary": {"best_roi": 0.0, "average_roi": 0.0, "profitable_strategies": 0},
        "paper_trading": {
            "source_group_id": source_group["id"],
            "source_strategy_id": source_strategy["id"],
            "strategy_id": strategy_id,
            "source_strategy_name": source_strategy.get("name"),
            "session_id": session_id,
            "real_trading_enabled": bool(real_trading_enabled),
            "server_optimized": bool(server_optimized),
            "balance_guard_enabled": bool(balance_guard_enabled),
            "min_open_balance": round(float(min_open_balance), 2),
            "real_order_amount": _paper_real_order_amount(),
            "initial_event_slug": getattr(initial_event, "slug", None),
            "initial_window_start_ts": getattr(initial_event, "window_start_ts", None),
            "deleted_saved_record_ids": [],
            "started_at": now,
            "status": "active",
        },
        "created_at": now,
    }
    strategy = {
        "id": strategy_id,
        "group_id": group_id,
        "name": f"{source_strategy.get('name', 'Strategy')} paper run",
        "theme": "paper_trading_live",
        "family": source_strategy.get("family"),
        "family_label": source_strategy.get("family_label"),
        "description": source_strategy.get("description", ""),
        "parameters": dict(source_strategy.get("parameters") or {}),
        "favorite": False,
        "metrics": metrics,
        "equity_curve": equity_curve,
        "paper_trading": {
            "source_group_id": source_group["id"],
            "source_strategy_id": source_strategy["id"],
            "session_id": session_id,
            "real_trading_enabled": bool(real_trading_enabled),
            "server_optimized": bool(server_optimized),
            "balance_guard_enabled": bool(balance_guard_enabled),
            "min_open_balance": round(float(min_open_balance), 2),
            "real_order_amount": _paper_real_order_amount(),
        },
        "created_at": now,
    }
    store.save_group_bundle(user_id, group, [strategy], [])
    return group, strategy


def _paper_trade_contract_payload(request: Request, user_id: str, record: dict) -> dict:
    saved_record_id = str(record.get("saved_record_id") or "").strip()
    if saved_record_id:
        contract = request.app.state.live_monitor.get_saved(user_id, saved_record_id) or {}
    else:
        contract = dict(record.get("contract_record") or {})
    contract["history"] = _sample_series(contract.get("history") or [], limit=1600)
    trades = []
    stored_trades = request.app.state.store.list_user_records_filtered(
        user_id, "transactions", {"paper_trade_id": record["id"]}
    )
    for trade in stored_trades:
        trades.append(_paper_trade_marker_payload(trade))
    return {
        "record": {
            **_paper_trade_list_record(record),
            "history": contract.get("history") or [],
            "summary": contract.get("summary") or {},
            "partial_contract": contract.get("partial_contract", False),
            "unsuitable_contract": contract.get("unsuitable_contract", False),
            "partial_reason": contract.get("partial_reason"),
            "unsuitable_reason": contract.get("unsuitable_reason"),
            "connection_gap_periods": contract.get("connection_gap_periods") or [],
            "polymarket_disconnect_count": contract.get("polymarket_disconnect_count", 0),
            "polymarket_disconnected_seconds": contract.get("polymarket_disconnected_seconds", 0),
            "crypto_disconnect_count": contract.get("crypto_disconnect_count", 0),
            "crypto_disconnected_seconds": contract.get("crypto_disconnected_seconds", 0),
            "trades": trades,
        }
    }


def _update_paper_trade_totals(
    store: FileStore,
    user_id: str,
    paper_id: str,
) -> None:
    record = store.get_user_record(user_id, "paper_trades", paper_id)
    if not record:
        return
    trades = store.list_user_records_filtered(
        user_id, "transactions", {"paper_trade_id": paper_id}
    )
    initial_capital = float(record.get("initial_capital") or 100)
    net_profit = round(sum(float(item.get("pnl", 0) or 0) for item in trades), 8)
    final_capital = round(initial_capital + net_profit, 8)
    store.update_user_record(
        user_id,
        "paper_trades",
        paper_id,
        {
            "trade_count": len(trades),
            "net_profit": net_profit,
            "final_capital": final_capital,
            "roi": round((final_capital / initial_capital - 1) * 100, 6) if initial_capital else 0.0,
        },
    )


def _paper_trade_marker_payload(trade: dict) -> dict:
    side = str(trade.get("side") or "").upper()
    entry_price = _number_or_none(trade.get("ask_price", trade.get("entry_price")))
    exit_price = _number_or_none(trade.get("exit_bid_price", trade.get("exit_price")))
    entry_price_cents = round(entry_price * 100, 4) if entry_price is not None else None
    exit_price_cents = round(exit_price * 100, 4) if exit_price is not None else None
    return {
        **trade,
        "side": side,
        "entry_price_cents": entry_price_cents,
        "entry_yes_axis_cents": (
            round(100 - entry_price_cents, 4)
            if side == "DOWN" and entry_price_cents is not None
            else entry_price_cents
        ),
        "exit_price_cents": exit_price_cents,
        "exit_yes_axis_cents": (
            round(100 - exit_price_cents, 4)
            if side == "DOWN" and exit_price_cents is not None
            else exit_price_cents
        ),
    }


def _paper_event_key(session_id: str, event_payload: dict) -> str:
    window_start = _number_or_none(event_payload.get("window_start_ts"))
    return ":".join(
        str(part)
        for part in (
            session_id,
            event_payload.get("slug") or event_payload.get("title") or "contract",
            round(window_start, 3) if window_start is not None else "",
        )
    )


def _paper_trade_id(paper_id: str, trade: dict) -> str:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        ":".join(
            str(part)
            for part in (
                "paper-live-trade",
                paper_id,
                trade.get("opened_at") or "",
                trade.get("side") or "",
                trade.get("scale_entry_index") or 1,
            )
        ),
    ).hex


def _paper_strategy_spec(source_strategy: dict, run_strategy: dict | None = None) -> dict:
    spec = dict((source_strategy or {}).get("parameters") or {})
    if run_strategy:
        spec.update(dict(run_strategy.get("parameters") or {}))
    for key in (
        "family",
        "family_label",
        "entry_second",
        "quote_edge_cents",
        "deviation_threshold_pct",
        "allocation_pct",
        "max_allocation_pct",
        "fee_bps",
        "exit_condition",
        "price_source",
        "min_seconds_left",
        "max_seconds_left",
        "confirmation_ticks",
        "lookback_seconds",
        "min_spread_cents",
        "max_spread_cents",
        "momentum_threshold_pct",
        "target_distance_pct",
        "direction",
        "side",
    ):
        if key not in spec and (source_strategy or {}).get(key) is not None:
            spec[key] = source_strategy[key]
        if run_strategy and key not in spec and run_strategy.get(key) is not None:
            spec[key] = run_strategy[key]
    return spec


def _paper_target_known_at_window_start(record: dict, history: list[dict] | None = None) -> bool:
    window_start = _number_or_none(record.get("window_start_ts"))
    if window_start is None:
        return False
    target_ts = (
        _number_or_none(record.get("target_price_effective_ts"))
        or _number_or_none(record.get("target_price_ts"))
    )
    if target_ts is None:
        for row in history or record.get("history") or []:
            if not isinstance(row, dict):
                continue
            row_target_ts = _number_or_none(row.get("target_price_ts"))
            row_target = _number_or_none(row.get("target_price"))
            if row_target_ts is not None and row_target is not None:
                target_ts = row_target_ts
                break
    return target_ts is not None and abs(target_ts - window_start) <= 1e-6


def _paper_record_from_snapshot(
    run_group: dict,
    run_strategy: dict,
    source_group: dict,
    source_strategy: dict,
    session_id: str,
    event_payload: dict,
    history: list[dict],
    trades: list[dict],
    initial_capital: float | None = None,
) -> dict:
    event_key = _paper_event_key(session_id, event_payload)
    paper_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"paper-live:{run_strategy['id']}:{event_key}",
    ).hex
    now = utc_iso()
    net_profit = round(sum(float(trade.get("pnl", 0) or 0) for trade in trades), 8)
    initial_capital = float(
        initial_capital
        if initial_capital is not None
        else (
            run_strategy.get("metrics", {}).get("final_capital")
            or run_group.get("starting_balance")
            or 100
        )
    )
    final_capital = round(initial_capital + net_profit, 8)
    return {
        "id": paper_id,
        "group_id": run_group["id"],
        "strategy_id": run_strategy["id"],
        "source_group_id": source_group["id"],
        "source_strategy_id": source_strategy["id"],
        "session_id": session_id,
        "strategy_name": source_strategy.get("name") or run_strategy.get("name"),
        "family": source_strategy.get("family") or run_strategy.get("family"),
        "saved_record_id": None,
        "live_event_key": event_key,
        "question": event_payload.get("title"),
        "event_title": event_payload.get("event_title") or event_payload.get("title"),
        "crypto_symbol": (source_group.get("assets") or [""])[0],
        "target_price": (history[0].get("target_price") if history else None),
        "final_crypto_price": (history[-1].get("crypto_price") if history else None),
        "winning_outcome": None,
        "window_start_ts": event_payload.get("window_start_ts"),
        "window_end_ts": event_payload.get("window_end_ts"),
        "saved_at": now,
        "created_at": now,
        "paper_mode": True,
        "trade_count": len(trades),
        "net_profit": net_profit,
        "roi": round((final_capital / initial_capital - 1) * 100, 6) if initial_capital else 0.0,
        "initial_capital": initial_capital,
        "final_capital": final_capital,
        "history_count": len(history),
    }


def _paper_run_current_cash(
    store: FileStore,
    user_id: str,
    run_group: dict,
    run_strategy: dict,
    current_window_start: float | None = None,
) -> float:
    rows = store.list_user_records_filtered(
        user_id,
        "paper_trades",
        {"group_id": run_group["id"], "strategy_id": run_strategy["id"]},
    )
    candidates: list[tuple[float, str, float]] = []
    for row in rows:
        row_start = _number_or_none(row.get("window_start_ts"))
        if (
            current_window_start is not None
            and row_start is not None
            and row_start >= current_window_start - 1e-6
        ):
            continue
        final_capital = _number_or_none(row.get("final_capital"))
        if final_capital is None:
            continue
        row_end = _number_or_none(row.get("window_end_ts"))
        ordering_ts = row_end if row_end is not None else (row_start if row_start is not None else -1.0)
        candidates.append(
            (ordering_ts, str(row.get("saved_at") or row.get("created_at") or ""), float(final_capital))
        )
    if candidates:
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[-1][2]
    return float(
        run_strategy.get("metrics", {}).get("final_capital")
        or run_group.get("starting_balance")
        or run_strategy.get("metrics", {}).get("initial_capital")
        or 100
    )


def _persist_paper_live_trades(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    session_id: str,
    event_payload: dict,
    history: list[dict],
    trades: list[dict],
) -> None:
    if not trades:
        return
    store = _store(request)
    event_key = _paper_event_key(session_id, event_payload)
    paper_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"paper-live:{run_strategy['id']}:{event_key}",
    ).hex
    normalized = []
    for trade in trades:
        item = dict(trade)
        item["id"] = str(item.get("id") or _paper_trade_id(paper_id, item))
        item["paper_trade_id"] = paper_id
        item["paper_mode"] = True
        item["live_event_key"] = event_key
        item["saved_record_id"] = None
        normalized.append(item)
    existing = store.get_user_record(user_id, "paper_trades", paper_id)
    if not existing:
        initial_capital = _paper_run_current_cash(
            store,
            user_id,
            run_group,
            run_strategy,
            _number_or_none(event_payload.get("window_start_ts")),
        )
        store.append_user_record(
            user_id,
            "paper_trades",
            _paper_record_from_snapshot(
                run_group,
                run_strategy,
                source_group,
                source_strategy,
                session_id,
                event_payload,
                history,
                normalized,
                initial_capital=initial_capital,
            ),
        )
    changed = False
    for item in normalized:
        transaction = store.get_user_record(user_id, "transactions", item["id"])
        changes = {
            key: value
            for key, value in {
                **item,
                "group_id": run_group["id"],
                "strategy_id": run_strategy["id"],
                "paper_trade_id": paper_id,
                "paper_mode": True,
            }.items()
            if key not in {"id", "group_id", "strategy_id", "created_at"}
        }
        if transaction:
            if any(transaction.get(key) != value for key, value in changes.items()):
                store.update_user_record(user_id, "transactions", item["id"], changes)
                changed = True
        else:
            store.append_user_record(
                user_id,
                "transactions",
                {
                    "id": item["id"],
                    "group_id": run_group["id"],
                    "strategy_id": run_strategy["id"],
                    "paper_trade_id": paper_id,
                    "paper_mode": True,
                    **item,
                },
            )
            changed = True
    if changed or not existing:
        transactions = store.list_user_records_filtered(
            user_id, "transactions", {"paper_trade_id": paper_id}
        )
        initial_capital = float((existing or {}).get("initial_capital") or run_group.get("starting_balance") or 100)
        net_profit = round(sum(float(trade.get("pnl", 0) or 0) for trade in transactions), 8)
        final_capital = round(initial_capital + net_profit, 8)
        store.update_user_record(
            user_id,
            "paper_trades",
            paper_id,
            {
                "trade_count": len(transactions),
                "net_profit": net_profit,
                "roi": round((final_capital / initial_capital - 1) * 100, 6) if initial_capital else 0.0,
                "final_capital": final_capital,
                "final_crypto_price": (history[-1].get("crypto_price") if history else None),
                "history_count": len(history),
            },
        )
        refreshed_group = store.get_user_record(user_id, "groups", run_group["id"]) or run_group
        refreshed_strategy = store.get_user_record(user_id, "strategies", run_strategy["id"]) or run_strategy
        _refresh_paper_run_summary(store, user_id, refreshed_group, refreshed_strategy)


def _finalize_paper_live_contract(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    saved_preview: dict | None,
) -> None:
    if not saved_preview:
        return
    session_id = str(run_group.get("paper_trading", {}).get("session_id") or "").strip()
    saved_record_id = str(saved_preview.get("id") or "").strip()
    if not session_id or not saved_record_id:
        return
    saved_window_start = _number_or_none(saved_preview.get("window_start_ts"))
    initial_window_start = _number_or_none(
        (run_group.get("paper_trading") or {}).get("initial_window_start_ts")
    )
    if (
        saved_window_start is not None
        and initial_window_start is not None
        and abs(saved_window_start - initial_window_start) <= 1e-6
    ):
        return
    event_payload = {
        "slug": saved_preview.get("slug"),
        "title": saved_preview.get("question") or saved_preview.get("event_title"),
        "window_start_ts": saved_preview.get("window_start_ts"),
        "window_end_ts": saved_preview.get("window_end_ts"),
    }
    event_key = _paper_event_key(session_id, event_payload)
    paper_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"paper-live:{run_strategy['id']}:{event_key}",
    ).hex
    store = _store(request)
    existing = store.get_user_record(user_id, "paper_trades", paper_id)
    saved = request.app.state.live_monitor.get_saved(user_id, saved_record_id)
    if not saved:
        return
    if not _paper_target_known_at_window_start(saved):
        return
    asset = str((source_group.get("assets") or [""])[0]).upper()
    timeframe = str(source_group.get("timeframe") or "")
    generator = LiveMonitorStrategyGenerator(store, request.app.state.live_monitor)
    event = generator._record_to_event(saved, asset, timeframe)
    if not event:
        return
    result = generator._run(
        [event],
        _paper_strategy_spec(source_strategy, run_strategy),
        float(
            (existing or {}).get("initial_capital")
            or _paper_run_current_cash(
                store,
                user_id,
                run_group,
                run_strategy,
                _number_or_none(saved_preview.get("window_start_ts")),
            )
        ),
    )
    generated = []
    for trade in result.get("transactions") or []:
        item = _paper_trade_marker_payload(trade)
        item["id"] = _paper_trade_id(paper_id, item)
        item["paper_trade_id"] = paper_id
        item["paper_mode"] = True
        item["live_event_key"] = event_key
        item["saved_record_id"] = saved_record_id
        item["preview"] = False
        item["status"] = "closed"
        generated.append(item)
    if not generated:
        return
    if existing:
        existing_transactions = store.list_user_records_filtered(
            user_id, "transactions", {"paper_trade_id": paper_id}
        )
        existing_ids = {str(trade.get("id")) for trade in existing_transactions}
        finalized = [trade for trade in generated if str(trade.get("id")) in existing_ids]
    else:
        finalized = generated
    if not finalized:
        return
    net_profit = round(sum(float(trade.get("pnl", 0) or 0) for trade in finalized), 8)
    initial_capital = float(
        (existing or {}).get("initial_capital")
        or _paper_run_current_cash(
            store,
            user_id,
            run_group,
            run_strategy,
            _number_or_none(saved_preview.get("window_start_ts")),
        )
    )
    final_capital = round(initial_capital + net_profit, 8)
    changes = {
        "saved_record_id": saved_record_id,
        "winning_outcome": saved.get("winning_outcome"),
        "final_crypto_price": saved.get("final_crypto_price"),
        "history_count": len(saved.get("history") or []),
        "trade_count": len(finalized),
        "net_profit": net_profit,
        "roi": round((final_capital / initial_capital - 1) * 100, 6) if initial_capital else 0.0,
        "final_capital": final_capital,
    }
    if existing:
        store.update_user_record(user_id, "paper_trades", paper_id, changes)
    else:
        record = _paper_record_from_snapshot(
            run_group,
            run_strategy,
            source_group,
            source_strategy,
            session_id,
            event_payload,
            saved.get("history") or [],
            finalized,
            initial_capital=initial_capital,
        )
        record.update(changes)
        store.append_user_record(user_id, "paper_trades", record)
    for item in finalized:
        transaction = store.get_user_record(user_id, "transactions", item["id"])
        changes = {
            key: value
            for key, value in {
                **item,
                "paper_trade_id": paper_id,
                "paper_mode": True,
            }.items()
            if key not in {"id", "group_id", "strategy_id", "created_at"}
        }
        if transaction:
            store.update_user_record(user_id, "transactions", item["id"], changes)
        else:
            store.append_user_record(
                user_id,
                "transactions",
                {
                    "id": item["id"],
                    "group_id": run_group["id"],
                    "strategy_id": run_strategy["id"],
                    "paper_trade_id": paper_id,
                    "paper_mode": True,
                    **item,
                },
            )
    refreshed_group = store.get_user_record(user_id, "groups", run_group["id"]) or run_group
    refreshed_strategy = store.get_user_record(user_id, "strategies", run_strategy["id"]) or run_strategy
    _refresh_paper_run_summary(store, user_id, refreshed_group, refreshed_strategy)


def _paper_first_session_contract_id(
    request: Request,
    user_id: str,
    session_id: str,
    asset: str,
    timeframe: str,
) -> str | None:
    generator = LiveMonitorStrategyGenerator(_store(request), request.app.state.live_monitor)
    previews = [
        record for record in request.app.state.live_monitor.list_saved_previews(
            user_id, category=asset.lower()
        )
        if str(record.get("session_id") or "") == session_id
        and generator._record_timeframe(record) == timeframe
    ]
    previews.sort(
        key=lambda item: (
            _number_or_none(item.get("window_start_ts")) or math.inf,
            str(item.get("saved_at") or ""),
        )
    )
    return str(previews[0].get("id") or "") if previews else None


def _paper_live_trade_preview(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    snapshot: dict,
    *,
    persist: bool = False,
    execute_real_orders: bool = True,
) -> dict:
    store = _store(request)
    run_group = store.get_user_record(user_id, "groups", run_group["id"]) or run_group
    run_strategy = store.get_user_record(user_id, "strategies", run_strategy["id"]) or run_strategy
    asset = str((source_group.get("assets") or [""])[0]).upper()
    timeframe = str(source_group.get("timeframe") or "")
    session_id = str(run_group.get("paper_trading", {}).get("session_id") or "").strip()
    paper_meta = run_group.get("paper_trading") or {}
    event_payload = snapshot.get("event") or {}
    window_start = _number_or_none(event_payload.get("window_start_ts"))
    window_end = _number_or_none(event_payload.get("window_end_ts"))
    history = [
        row for row in snapshot.get("history") or []
        if isinstance(row, dict) and _number_or_none(row.get("t")) is not None
    ]
    history.sort(key=lambda row: float(row["t"]))
    initial_window_start = _number_or_none(paper_meta.get("initial_window_start_ts"))
    if (
        initial_window_start is not None
        and window_start is not None
        and abs(initial_window_start - window_start) <= 1e-6
    ):
        return {
            "paper_trades": [],
            "paper_trade_status": {
                "status": "skipping_first_contract",
                "message": "Paper trading waits for the next contract after initial connection.",
            },
        }
    if initial_window_start is None:
        completed_first_id = _paper_first_session_contract_id(
            request, user_id, session_id, asset, timeframe
        )
        if not completed_first_id:
            return {
                "paper_trades": [],
                "paper_trade_status": {
                    "status": "skipping_first_contract",
                    "message": "Paper trading waits for the next contract after initial connection.",
                },
            }
    if not asset or not timeframe or window_start is None or window_end is None or not history:
        return {"paper_trades": [], "paper_trade_status": {"status": "waiting"}}
    if not _paper_target_known_at_window_start(event_payload, history):
        return {
            "paper_trades": [],
            "paper_trade_status": {
                "status": "waiting_for_target",
                "message": "Skipping this contract because target price was not known at window start.",
            },
        }
    generator = LiveMonitorStrategyGenerator(store, request.app.state.live_monitor)
    current_cash = _paper_run_current_cash(
        store,
        user_id,
        run_group,
        run_strategy,
        window_start,
    )
    event = {
        "id": str(event_payload.get("slug") or event_payload.get("title") or session_id),
        "market_id": str(event_payload.get("slug") or session_id),
        "question": str(event_payload.get("title") or "Live paper contract"),
        "asset": asset,
        "timeframe": timeframe,
        "start_ts": window_start,
        "end_ts": window_end,
        "start_time": datetime.fromtimestamp(window_start, timezone.utc).isoformat(),
        "expiration_time": datetime.fromtimestamp(window_end, timezone.utc).isoformat(),
        "history": history,
        "resolved_outcome": "flat",
        "partial_contract": True,
        "saved_record_id": None,
        "slug": event_payload.get("slug"),
    }
    try:
        result = generator._run(
            [event],
            _paper_strategy_spec(source_strategy, run_strategy),
            current_cash,
        )
    except (KeyError, TypeError, ValueError, ZeroDivisionError) as error:
        return {
            "paper_trades": [],
            "paper_trade_status": {"status": "error", "message": _error_message(error)},
        }
    last_history_ts = _number_or_none(history[-1].get("t")) if history else None
    trades = []
    for trade in result.get("transactions") or []:
        opened_ts = _timestamp_from_iso(trade.get("opened_at"))
        closed_ts = _timestamp_from_iso(trade.get("closed_at"))
        if last_history_ts is not None and opened_ts is not None and opened_ts > last_history_ts + 1e-6:
            continue
        marker = _paper_trade_marker_payload(trade)
        is_live_exit = (
            str(trade.get("exit_type") or "") != "settlement"
            and closed_ts is not None
            and (last_history_ts is None or closed_ts <= last_history_ts + 1e-6)
        )
        if not is_live_exit:
            marker["closed_at"] = None
            marker["exit_price"] = None
            marker["exit_price_cents"] = None
            marker["exit_yes_axis_cents"] = None
            marker["exit_type"] = "open"
            marker["settlement_value"] = 0
            marker["pnl"] = 0
            marker["analysis_history_count"] = marker.get("entry_history_count")
            marker["available_cash_after_close"] = marker.get("available_cash_after_entry")
            marker["balance_after"] = marker.get("balance_before")
        marker["preview"] = True
        marker["status"] = "closed" if is_live_exit else "open"
        trades.append(marker)
    _paper_attach_signal_timing(trades, history)
    if persist:
        _persist_paper_live_trades(
            request,
            user_id,
            source_group,
            source_strategy,
            run_group,
            run_strategy,
            session_id,
            event_payload,
            history,
            trades,
        )
    if execute_real_orders:
        real_orders = _execute_paper_real_trading_signals(
            request,
            user_id,
            run_group,
            run_strategy,
            session_id,
            event_payload,
            snapshot,
            trades,
        )
    else:
        real_orders = _paper_existing_real_orders_for_trades(
            request,
            user_id,
            run_strategy,
            session_id,
            event_payload,
            trades,
        )
    real_order_failures = [
        order for order in real_orders if str(order.get("status") or "") == "failed"
    ]
    real_order_submitted = [
        order for order in real_orders if str(order.get("status") or "") == "submitted"
    ]
    message = (
        f"{len(trades)} live paper marker(s)"
        if trades
        else "Waiting for this strategy's entry conditions."
    )
    if real_order_submitted:
        message += f" Real order submitted: ${_paper_real_order_amount():.2f}."
    if real_order_failures:
        message += f" Real order failed: {real_order_failures[-1].get('error')}"
    return {
        "paper_trades": trades,
        "real_orders": real_orders,
        "paper_trade_status": {
            "status": "active" if trades else "watching",
            "real_trading_enabled": bool((run_group.get("paper_trading") or {}).get("real_trading_enabled")),
            "message": message,
        },
    }


def _timestamp_from_iso(value) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _iso_from_ts(value) -> str | None:
    timestamp = _number_or_none(value)
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat(timespec="milliseconds")


def _paper_history_row_by_index(history: list[dict], index: int | None) -> dict | None:
    if index is None or index < 0 or index >= len(history):
        return None
    row = history[index]
    return row if isinstance(row, dict) else None


def _paper_history_row_by_timestamp(history: list[dict], timestamp_iso: str | None) -> dict | None:
    timestamp = _timestamp_from_iso(timestamp_iso)
    if timestamp is None:
        return None
    for row in reversed(history):
        row_ts = _number_or_none(row.get("t")) if isinstance(row, dict) else None
        if row_ts is not None and abs(row_ts - timestamp) <= 1e-6:
            return row
    return None


def _paper_signal_row(history: list[dict], trade: dict, action: str) -> dict | None:
    if action == "open":
        timestamp_row = _paper_history_row_by_timestamp(history, trade.get("opened_at"))
        if timestamp_row is not None:
            return timestamp_row
        entry_index = int(trade.get("entry_history_count") or 0) - 1
        return _paper_history_row_by_index(history, entry_index)
    return _paper_history_row_by_timestamp(history, trade.get("closed_at"))


def _paper_attach_signal_timing(trades: list[dict], history: list[dict]) -> None:
    for trade in trades:
        action = "close" if str(trade.get("status") or "").lower() == "closed" else "open"
        row = _paper_signal_row(history, trade, action)
        if not row:
            continue
        captured_at = _number_or_none(row.get("captured_at"))
        if captured_at is not None:
            trade["signal_tick_captured_at"] = _iso_from_ts(captured_at)
        sample_delay = _number_or_none(row.get("sample_delay_ms"))
        if sample_delay is not None:
            trade["signal_sample_delay_ms"] = sample_delay
        row_ts = _number_or_none(row.get("t"))
        if row_ts is not None:
            trade["signal_history_at"] = _iso_from_ts(row_ts)


def _paper_session_payload(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    session,
) -> dict:
    paper_meta = run_group.get("paper_trading") or {}
    server_optimized = bool(paper_meta.get("server_optimized"))
    payload = _live_monitor_session_payload(
        session,
        history_limit=0 if server_optimized else None,
    )
    payload["events_url"] = (
        f"/api/paper-trading/{source_group['id']}/strategies/"
        f"{source_strategy['id']}/sessions/{session.id}/events"
    )
    payload["paper_mode"] = True
    payload["source_group_id"] = source_group["id"]
    payload["source_strategy_id"] = source_strategy["id"]
    payload["group_id"] = run_group["id"]
    payload["strategy_id"] = run_strategy["id"]
    payload["run_group_id"] = run_group["id"]
    payload["run_strategy_id"] = run_strategy["id"]
    payload["run_url"] = f"/groups/{run_group['id']}/strategies/{run_strategy['id']}"
    payload["manual_open_url"] = (
        f"/api/paper-trading/{source_group['id']}/strategies/"
        f"{source_strategy['id']}/sessions/{session.id}/manual/open"
    )
    payload["manual_close_url"] = (
        f"/api/paper-trading/{source_group['id']}/strategies/"
        f"{source_strategy['id']}/sessions/{session.id}/manual/close"
    )
    payload["real_trading_enabled"] = bool(paper_meta.get("real_trading_enabled"))
    payload["server_optimized"] = server_optimized
    payload["real_order_amount"] = paper_meta.get(
        "real_order_amount",
        _paper_real_order_amount(),
    )
    payload["snapshot"].update(
        _paper_existing_trade_preview(request, user_id, run_group, payload["snapshot"])
    )
    return payload


def _paper_existing_trade_preview(
    request: Request,
    user_id: str,
    run_group: dict,
    snapshot: dict,
) -> dict:
    session_id = str(run_group.get("paper_trading", {}).get("session_id") or "").strip()
    event_payload = snapshot.get("event") or {}
    if not session_id or not event_payload:
        return {"paper_trades": [], "paper_trade_status": {"status": "watching"}}
    strategy_id = str((run_group.get("paper_trading") or {}).get("strategy_id") or "")
    if not strategy_id:
        strategies = _store(request).list_user_records_filtered(
            user_id, "strategies", {"group_id": run_group["id"]}
        )
        strategy_id = str(strategies[0].get("id") or "") if strategies else ""
    if not strategy_id:
        return {"paper_trades": [], "paper_trade_status": {"status": "watching"}}
    event_key = _paper_event_key(session_id, event_payload)
    if strategy_id:
        paper_id = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"paper-live:{strategy_id}:{event_key}",
        ).hex
    trades = [
        _paper_trade_marker_payload(trade)
        for trade in _store(request).list_user_records_filtered(
            user_id, "transactions", {"paper_trade_id": paper_id}
        )
    ]
    real_orders = _store(request).list_user_records_filtered(
        user_id,
        "real_orders",
        {
            "group_id": run_group["id"],
            "strategy_id": strategy_id,
            "live_event_key": event_key,
        },
    )
    return {
        "paper_trades": trades,
        "real_orders": real_orders,
        "paper_trade_status": {
            "status": "active" if trades else "watching",
            "message": (
                f"{len(trades)} recorded paper marker(s)"
                if trades
                else "Waiting for this strategy's entry conditions."
            ),
        },
    }


def _paper_real_order_id(paper_id: str, trade: dict) -> str:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        ":".join(
            str(part)
            for part in (
                "paper-real-order",
                paper_id,
                trade.get("opened_at") or "",
                trade.get("side") or "",
                trade.get("scale_entry_index") or 1,
            )
        ),
    ).hex


def _paper_real_close_order_id(paper_id: str, trade: dict) -> str:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        ":".join(
            str(part)
            for part in (
                "paper-real-close-order",
                paper_id,
                trade.get("opened_at") or "",
                trade.get("closed_at") or "",
                trade.get("side") or "",
                trade.get("scale_entry_index") or 1,
            )
        ),
    ).hex


def _paper_existing_real_orders_for_trades(
    request: Request,
    user_id: str,
    run_strategy: dict,
    session_id: str,
    event_payload: dict,
    trades: list[dict],
) -> list[dict]:
    if not trades:
        return []
    paper_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"paper-live:{run_strategy['id']}:{_paper_event_key(session_id, event_payload)}",
    ).hex
    store = _store(request)
    orders = []
    seen = set()
    for trade in trades:
        order_id = _paper_real_order_id(paper_id, trade)
        for candidate_id in (order_id, _paper_real_close_order_id(paper_id, trade)):
            if candidate_id in seen:
                continue
            record = store.get_user_record(user_id, "real_orders", candidate_id)
            if record:
                orders.append(record)
                seen.add(candidate_id)
    return orders


def _paper_token_id_for_side(snapshot: dict, side: str) -> str | None:
    wanted = "up" if str(side or "").upper() == "UP" else "down"
    tokens = (snapshot.get("event") or {}).get("tokens") or []
    for token in tokens:
        if str(token.get("outcome") or "").strip().lower() == wanted:
            return str(token.get("token_id") or "").strip() or None
    if len(tokens) >= 2:
        index = 0 if wanted == "up" else 1
        return str(tokens[index].get("token_id") or "").strip() or None
    return None


def _paper_real_order_amount() -> float:
    amount = _number_or_none(settings.polymarket_live_order_amount)
    if amount is None or amount <= 0:
        amount = 1.0
    return round(amount, 2)


def _paper_real_order_type() -> str:
    return "FAK"


def _paper_aggressive_limit_price(action: str) -> float:
    return 0.99 if str(action or "").upper() == "BUY" else 0.01


def _paper_clock_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _paper_timing_from_response(response: dict | None) -> dict:
    if not isinstance(response, dict):
        return {}
    timing = response.get("_timing")
    return timing if isinstance(timing, dict) else {}


def _paper_open_balance_info(executor, token_id: str) -> tuple[float | None, dict | None, str]:
    balance_info = getattr(executor, "token_balance_info", None)
    if callable(balance_info):
        info = balance_info(token_id)
        if isinstance(info, dict):
            return _number_or_none(info.get("balance")), info, str(info.get("source") or "post_open_balance")
    return executor.token_balance(token_id), None, "post_open_balance"


def _paper_min_open_balance() -> float:
    value = _number_or_none(settings.polymarket_min_open_balance)
    return value if value is not None and value >= 0 else 130.0


def _paper_collateral_balance_info(executor) -> tuple[float, dict]:
    info = executor.collateral_balance_info()
    if not isinstance(info, dict):
        raise RuntimeError("Could not read Polymarket collateral balance response")
    balance = _number_or_none(info.get("balance"))
    if balance is None:
        raise RuntimeError(f"Could not read Polymarket collateral balance: {info!r}")
    return balance, info


def _paper_signal_audit_snapshot(
    snapshot: dict,
    trade: dict,
    action: str,
) -> dict | None:
    history = _paper_current_history(snapshot)
    row = _paper_signal_row(history, trade, action)
    if not row:
        return None
    fields = (
        "t",
        "captured_at",
        "sample_delay_ms",
        "crypto_price",
        "target_price",
        "target_price_ts",
        "deviation_pct",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
    )
    return {field: row.get(field) for field in fields}


def _append_real_trade_audit(
    store: FileStore,
    user_id: str,
    event: str,
    order: dict,
) -> None:
    """Best-effort append-only audit; persistence must never alter order handling."""
    try:
        store.append_user_record(
            user_id,
            "real_trade_audit",
            {
                "audit_event": event,
                "audited_at": utc_iso(),
                "order_id": order.get("id"),
                "session_id": order.get("session_id"),
                "event_slug": order.get("event_slug"),
                "order": dict(order),
            },
        )
    except Exception:
        logger.exception(
            "Could not append real trade audit order_id=%s event=%s",
            order.get("id"),
            event,
        )


def _paper_close_fill(response: dict | None) -> tuple[float | None, float | None, float | None]:
    if not isinstance(response, dict) or str(response.get("success")).lower() == "false":
        return None, None, None
    quantity = _number_or_none(response.get("makingAmount"))
    proceeds = _number_or_none(response.get("takingAmount"))
    average = proceeds / quantity if quantity and proceeds is not None else None
    return quantity, proceeds, round(average, 8) if average is not None else None


def _real_order_account_row(order: dict) -> dict:
    row = dict(order)
    action = str(row.get("order_action") or "open").lower()
    response = row.get("response") if isinstance(row.get("response"), dict) else {}
    if action == "close":
        quantity, notional, average = _paper_close_fill(response)
        row["actual_quantity"] = row.get("real_close_quantity") or quantity
        row["actual_notional"] = row.get("real_close_proceeds") or notional
        row["actual_price"] = row.get("real_close_avg_price") or average
    else:
        response_quantity = _number_or_none(response.get("takingAmount"))
        response_notional = _number_or_none(response.get("makingAmount"))
        row["actual_quantity"] = response_quantity or row.get("real_open_quantity")
        row["actual_notional"] = (
            response_notional or row.get("real_open_notional_amount") or row.get("amount")
        )
        row["actual_price"] = (
            round(response_notional / response_quantity, 8)
            if response_notional and response_quantity
            else row.get("real_open_avg_price")
        )
    row["signal_price"] = _number_or_none(
        row.get("signal_exit_price") if action == "close" else row.get("signal_entry_price")
    )
    actual_price = _number_or_none(row.get("actual_price"))
    signal_price = row["signal_price"]
    difference = (
        round(actual_price - signal_price, 8)
        if actual_price is not None and signal_price is not None
        else None
    )
    row["execution_difference"] = difference
    row["execution_difference_pct"] = (
        round((difference / signal_price) * 100.0, 4)
        if difference is not None and signal_price
        else None
    )
    if difference is None:
        row["execution_quality"] = None
    elif abs(difference) <= 1e-9:
        row["execution_quality"] = "same"
    else:
        favorable = difference < 0 if action == "open" else difference > 0
        row["execution_quality"] = "better" if favorable else "worse"
    row["display_time"] = (
        row.get("submitted_at")
        or row.get("failed_at")
        or row.get("blocked_at")
        or row.get("created_at")
    )
    return row


def _real_trade_balance_chart_payload(orders: list[dict], limit: int) -> dict:
    balance = 0.0
    minimum = 0.0
    maximum = 0.0
    points = []
    for order in orders:
        if str(order.get("status") or "").lower() != "submitted":
            continue
        row = _real_order_account_row(order)
        actual_notional = _number_or_none(row.get("actual_notional"))
        if actual_notional is None or actual_notional <= 0:
            continue
        action = str(row.get("order_action") or "open").lower()
        cash_flow = actual_notional if action == "close" else -actual_notional
        balance = round(balance + cash_flow, 8)
        minimum = min(minimum, balance)
        maximum = max(maximum, balance)
        points.append({
            "t": (
                row.get("clob_send_at")
                or row.get("submitted_at")
                or row.get("paper_signal_at")
                or row.get("created_at")
            ),
            "balance": balance,
            "cash_flow": round(cash_flow, 8),
            "action": action,
            "side": row.get("side"),
            "event_slug": row.get("event_slug"),
            "actual_price": row.get("actual_price"),
            "actual_quantity": row.get("actual_quantity"),
        })
    return {
        "range": limit,
        "points": points,
        "summary": {
            "order_count": len(points),
            "final_balance": round(balance, 8),
            "minimum_balance": round(minimum, 8),
            "maximum_balance": round(maximum, 8),
        },
    }


def _real_order_console_rows(order: dict) -> list[dict]:
    account_row = _real_order_account_row(order)
    action = str(order.get("order_action") or "open").upper()
    side = str(order.get("side") or "-").upper()
    amount = _number_or_none(order.get("amount"))
    token = str(order.get("token_id") or "")
    token_short = f"{token[:8]}...{token[-6:]}" if len(token) > 16 else token or "-"
    signal_lag = _number_or_none(order.get("signal_lag_ms"))
    sample_delay = _number_or_none(order.get("signal_sample_delay_ms"))
    rows = [{
        "created_at": order.get("paper_signal_at") or order.get("created_at"),
        "level": "info",
        "source": "paper",
        "session_id": order.get("session_id"),
        "event_slug": order.get("event_slug"),
        "message": (
            f"PAPER SIGNAL {action} {side} amount {(amount or 0):.6f} "
            f"token {token_short} signal lag {(signal_lag or 0):.2f}ms "
            f"sample delay {(sample_delay or 0):.2f}ms"
        ),
    }]
    response = order.get("response") if isinstance(order.get("response"), dict) else {}
    price_comparison = ""
    if account_row.get("signal_price") is not None:
        price_comparison = f" signal_price {float(account_row['signal_price']):.6f}"
    if account_row.get("actual_price") is not None:
        price_comparison += f" actual_fill {float(account_row['actual_price']):.6f}"
    if account_row.get("execution_difference") is not None:
        price_comparison += (
            f" difference {float(account_row['execution_difference']):+.6f} "
            f"({account_row.get('execution_quality')})"
        )
    rows.append({
        "created_at": order.get("clob_send_at") or order.get("submitted_at") or order.get("failed_at"),
        "level": "error" if str(order.get("status")) == "failed" else "info",
        "source": "clob",
        "session_id": order.get("session_id"),
        "event_slug": order.get("event_slug"),
        "message": (
            f"CLOB SEND {action} {side} {order.get('status', 'unknown')} "
            f"amount {(amount or 0):.6f} @ {float(order.get('limit_price') or 0):.2f} "
            f"token {token_short} delay {float(order.get('signal_to_clob_ms') or 0):.2f}ms "
            f"duration {float(order.get('clob_duration_ms') or 0):.2f}ms "
            f"clob_status {response.get('status', '-')}"
            + price_comparison
            + (f" error {order.get('error')}" if order.get("error") else "")
        ),
    })
    if action == "OPEN" and order.get("real_open_quantity") is not None:
        rows.append({
            "created_at": order.get("submitted_at"),
            "level": "info",
            "source": "balance",
            "session_id": order.get("session_id"),
            "event_slug": order.get("event_slug"),
            "message": (
                f"OPEN FILL {side} dollars {float(order.get('real_open_notional_amount') or 0):.6f} "
                f"contracts {float(order.get('real_open_quantity') or 0):.6f} "
                f"avg {float(order.get('real_open_avg_price') or 0):.6f} token {token_short}"
            ),
        })
    return [row for row in rows if row.get("created_at")]


def _paper_open_fill_quantity(response: dict | None) -> tuple[float | None, str | None]:
    if not isinstance(response, dict):
        return None, None
    if str(response.get("success")).lower() == "false":
        return None, None
    status = str(response.get("status") or "").lower()
    if status and status not in {"matched", "filled", "partially_filled", "partial"}:
        return None, None
    quantity = _number_or_none(response.get("takingAmount"))
    if quantity is None or quantity <= 0:
        return None, None
    return math.floor(quantity * 1_000_000) / 1_000_000, "order_response_taking_amount"


def _paper_signal_to_clob_ms(signal_at: str | None, clob_send_at: str | None) -> float | None:
    if not signal_at or not clob_send_at:
        return None
    try:
        start = datetime.fromisoformat(str(signal_at).replace("Z", "+00:00"))
        sent = datetime.fromisoformat(str(clob_send_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((sent - start).total_seconds() * 1000, 3)


def _paper_signal_lag_ms(signal_at: str | None, tick_at: str | None) -> float | None:
    if not signal_at or not tick_at:
        return None
    signal_ts = _timestamp_from_iso(signal_at)
    tick_ts = _timestamp_from_iso(tick_at)
    if signal_ts is None or tick_ts is None:
        return None
    return round(max(0.0, signal_ts - tick_ts) * 1000, 3)


def _paper_mutable_real_order_fields(record: dict) -> dict:
    immutable = {"id", "group_id", "strategy_id", "created_at"}
    return {key: value for key, value in record.items() if key not in immutable}


def _paper_live_order_executor(request: Request) -> PolymarketLiveOrderExecutor:
    executor = getattr(request.app.state, "polymarket_live_order_executor", None)
    if executor is None:
        executor = PolymarketLiveOrderExecutor()
        request.app.state.polymarket_live_order_executor = executor
    return executor


def _warm_paper_live_order_executor(request: Request, user_id: str, session_id: str) -> None:
    try:
        executor = _paper_live_order_executor(request)
        prepare = getattr(executor, "prepare", None)
        if callable(prepare):
            prepare()
        else:
            executor.validate_ready()
        _store(request).log(user_id, "paper_real_order_executor_warmed", {"session_id": session_id})
    except Exception as error:
        logger.exception("Paper real trading executor warmup failed")
        try:
            _store(request).log(
                user_id,
                "paper_real_order_executor_warm_failed",
                {"session_id": session_id, "error": _error_message(error)},
            )
        except Exception:
            logger.exception("Paper real trading executor warmup failure logging failed")


def _paper_snapshot_token_ids(snapshot: dict) -> list[str]:
    tokens = (snapshot.get("event") or {}).get("tokens") or []
    return [
        str(token.get("token_id") or "").strip()
        for token in tokens
        if isinstance(token, dict) and str(token.get("token_id") or "").strip()
    ]


def _warm_paper_live_order_tokens(request: Request, token_ids: list[str]) -> None:
    with _PAPER_TOKEN_WARM_LOCK:
        pending = [
            token_id
            for token_id in token_ids
            if token_id not in _PAPER_WARMED_TOKEN_IDS
            and token_id not in _PAPER_WARMING_TOKEN_IDS
        ]
        _PAPER_WARMING_TOKEN_IDS.update(pending)
    if not pending:
        return
    try:
        executor = _paper_live_order_executor(request)
        prepare_tokens = getattr(executor, "prepare_tokens", None)
        if callable(prepare_tokens):
            prepare_tokens(pending)
        prepare_order = getattr(executor, "prepare_market_order", None)
        if callable(prepare_order):
            for token_id in pending:
                prepare_order(
                    PolymarketLiveOrderRequest(
                        token_id=token_id,
                        amount=_paper_real_order_amount(),
                        order_type=_paper_real_order_type(),
                        price=_paper_aggressive_limit_price("BUY"),
                    )
                )
    except Exception:
        logger.exception("Paper real trading token warmup failed")
    else:
        with _PAPER_TOKEN_WARM_LOCK:
            _PAPER_WARMED_TOKEN_IDS.update(pending)
    finally:
        with _PAPER_TOKEN_WARM_LOCK:
            _PAPER_WARMING_TOKEN_IDS.difference_update(pending)


def _schedule_paper_live_order_token_warmup(request: Request, token_ids: list[str]) -> None:
    if not token_ids:
        return
    task = asyncio.create_task(
        asyncio.to_thread(_warm_paper_live_order_tokens, request, token_ids)
    )
    _background_tasks(request.app).add(task)

    def cleanup(done: asyncio.Task) -> None:
        _background_tasks(request.app).discard(done)

    task.add_done_callback(cleanup)


def _prepare_paper_close_order(request: Request, token_id: str | None, quantity: float | None) -> None:
    if not token_id or quantity is None or quantity <= 0:
        return
    try:
        executor = _paper_live_order_executor(request)
        prepare_order = getattr(executor, "prepare_market_order", None)
        if callable(prepare_order):
            prepare_order(
                PolymarketLiveOrderRequest(
                    token_id=token_id,
                    amount=quantity,
                    side="SELL",
                    order_type=_paper_real_order_type(),
                    price=_paper_aggressive_limit_price("SELL"),
                )
            )
    except Exception:
        logger.exception("Paper real trading close order warmup failed")


def _paper_real_order_retryable(record: dict) -> bool:
    if str(record.get("status") or "") != "failed":
        return False
    error = str(record.get("error") or "").lower()
    return any(
        text in error
        for text in (
            "real trading is disabled",
            "private_key",
            "proxy_address",
            "polymarket_private_key",
            "polymarket_funder",
            "api_key",
            "api_secret",
            "api_passphrase",
            "py-clob-client-v2 is not installed",
            "couldn't be fully filled",
            "fully filled or killed",
            "no orders found to match",
            "not enough balance / allowance",
            "balance is not enough",
        )
    )


def _paper_error_balance_quantity(record: dict | None) -> float | None:
    error = str((record or {}).get("error") or "")
    match = re.search(r"\bbalance:\s*(\d+)", error, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return max(0.0, int(match.group(1)) / 1_000_000)
    except (TypeError, ValueError):
        return None


def _paper_open_record_quantity(record: dict | None) -> tuple[float | None, str]:
    stored_quantity = _number_or_none((record or {}).get("real_open_quantity"))
    if stored_quantity is not None and stored_quantity > 0:
        return math.floor(stored_quantity * 1_000_000) / 1_000_000, "stored_open_balance"
    return None, "stored_open_balance"


def _paper_quote_for_side(snapshot: dict, side: str) -> dict | None:
    wanted = "up" if str(side or "").upper() == "UP" else "down"
    for quote in snapshot.get("quotes") or []:
        if str(quote.get("outcome") or "").strip().lower() == wanted:
            return quote
    return None


def _paper_current_price(snapshot: dict, side: str, field: str) -> float | None:
    quote = _paper_quote_for_side(snapshot, side)
    if not quote:
        return None
    return _number_or_none(quote.get(field))


def _paper_current_history(snapshot: dict) -> list[dict]:
    history = [
        row for row in snapshot.get("history") or []
        if isinstance(row, dict) and _number_or_none(row.get("t")) is not None
    ]
    history.sort(key=lambda row: float(row["t"]))
    return history


def _paper_manual_scope(
    request: Request,
    user_id: str,
    source_group_id: str,
    source_strategy_id: str,
    session_id: str,
) -> tuple[dict, dict, dict, dict, object, dict]:
    store = _store(request)
    source_group = store.get_user_record(user_id, "groups", source_group_id)
    source_strategy = store.get_user_record(user_id, "strategies", source_strategy_id)
    if not source_group or not source_strategy or source_strategy.get("group_id") != source_group_id:
        raise HTTPException(404, "Paper trading source strategy not found")
    session = request.app.state.live_monitor.get(session_id)
    if not session or session.user_id != user_id:
        raise HTTPException(404, "Paper trading live session not found")
    run_pair = next(
        (
            (run_group, run_strategy)
            for run_group, run_strategy in _paper_run_for_source(
                store, user_id, source_group_id, source_strategy_id
            )
            if str((run_group.get("paper_trading") or {}).get("session_id") or "") == session_id
        ),
        None,
    )
    if run_pair is None:
        raise HTTPException(404, "Paper trading run not found for this session")
    snapshot = session.snapshot(history_limit=None)
    return source_group, source_strategy, run_pair[0], run_pair[1], session, snapshot


def _ensure_manual_paper_record(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    session_id: str,
    event_payload: dict,
    history: list[dict],
) -> dict:
    store = _store(request)
    event_key = _paper_event_key(session_id, event_payload)
    paper_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"paper-live:{run_strategy['id']}:{event_key}",
    ).hex
    existing = store.get_user_record(user_id, "paper_trades", paper_id)
    if existing:
        return existing
    initial_capital = _paper_run_current_cash(
        store,
        user_id,
        run_group,
        run_strategy,
        _number_or_none(event_payload.get("window_start_ts")),
    )
    record = _paper_record_from_snapshot(
        run_group,
        run_strategy,
        source_group,
        source_strategy,
        session_id,
        event_payload,
        history,
        [],
        initial_capital=initial_capital,
    )
    record["manual_trade"] = True
    record["contract_record"] = {
        "id": record["id"],
        "history": history,
        "question": event_payload.get("title"),
        "event_title": event_payload.get("event_title") or event_payload.get("title"),
        "slug": event_payload.get("slug"),
        "window_start_ts": event_payload.get("window_start_ts"),
        "window_end_ts": event_payload.get("window_end_ts"),
    }
    return store.append_user_record(user_id, "paper_trades", record)


def _paper_manual_open_transaction(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    session_id: str,
    snapshot: dict,
    side: str = "UP",
) -> dict:
    event_payload = snapshot.get("event") or {}
    history = _paper_current_history(snapshot)
    if not event_payload or not history:
        raise HTTPException(422, "Live session snapshot is not ready yet")
    token_id = _paper_token_id_for_side(snapshot, side)
    if not token_id:
        raise HTTPException(422, f"Could not resolve {side} Polymarket token")
    ask_price = _paper_current_price(snapshot, side, "ask")
    if ask_price is None or ask_price <= 0:
        raise HTTPException(422, f"No live {side} ask is available")
    amount = _paper_real_order_amount()
    executor = _paper_live_order_executor(request)
    response = executor.market_buy(
        PolymarketLiveOrderRequest(
            token_id=token_id,
            amount=amount,
            order_type=_paper_real_order_type(),
            price=_paper_aggressive_limit_price("BUY"),
        )
    )
    real_open_balance = None
    real_open_quantity = None
    real_open_balance_info = None
    real_open_quantity_source = "post_open_balance"
    try:
        real_open_balance, real_open_balance_info, real_open_quantity_source = _paper_open_balance_info(
            executor,
            token_id,
        )
        real_open_quantity = math.floor(real_open_balance * 1_000_000) / 1_000_000
    except Exception:
        logger.exception("Paper manual real trading balance lookup failed after open")
    paper_record = _ensure_manual_paper_record(
        request,
        user_id,
        source_group,
        source_strategy,
        run_group,
        run_strategy,
        session_id,
        event_payload,
        history,
    )
    now = utc_iso()
    trade_id = uuid.uuid5(uuid.NAMESPACE_URL, f"paper-manual-open:{paper_record['id']}:{now}:{side}").hex
    balance_before = _paper_run_current_cash(
        _store(request),
        user_id,
        run_group,
        run_strategy,
        _number_or_none(event_payload.get("window_start_ts")),
    )
    trade = _store(request).append_user_record(
        user_id,
        "transactions",
        {
            "id": trade_id,
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "paper_trade_id": paper_record["id"],
            "paper_mode": True,
            "manual_trade": True,
            "status": "open",
            "real_order_status": "submitted",
            "real_open_response": response,
            "real_open_balance_after_open": (
                round(real_open_balance, 8) if real_open_balance is not None else None
            ),
            "real_open_quantity": real_open_quantity,
            "real_open_quantity_source": real_open_quantity_source,
            "real_open_balance_response": real_open_balance_info,
            "token_id": token_id,
            "event_id": str(event_payload.get("slug") or paper_record["id"]),
            "market_id": str(event_payload.get("slug") or session_id),
            "market_question": event_payload.get("title") or "Manual live paper trade",
            "asset": (source_group.get("assets") or ["LIVE"])[0],
            "timeframe": source_group.get("timeframe") or "live",
            "side": side,
            "resolved_outcome": "OPEN",
            "opened_at": now,
            "closed_at": None,
            "expiration_time": (
                datetime.fromtimestamp(float(event_payload["window_end_ts"]), timezone.utc).isoformat()
                if _number_or_none(event_payload.get("window_end_ts")) is not None
                else None
            ),
            "entry_price": round(ask_price, 8),
            "ask_price": round(ask_price, 8),
            "allocation_pct_used": round(amount / max(balance_before, amount, 1e-9), 8),
            "contracts_volume": real_open_quantity,
            "quantity": real_open_quantity,
            "margin_locked": amount,
            "entry_fee": 0,
            "fee_bps": 0,
            "available_cash_before_entry": round(balance_before, 4),
            "available_cash_after_entry": round(balance_before - amount, 4),
            "available_cash_after_close": round(balance_before, 4),
            "balance_before": round(balance_before, 4),
            "balance_after": round(balance_before, 4),
            "exit_price": None,
            "exit_type": "open",
            "settlement_value": 0,
            "pnl": 0,
            "quote_source": "manual_live_market",
            "saved_record_id": None,
            "partial_contract": True,
        },
    )
    _update_paper_trade_totals(_store(request), user_id, paper_record["id"])
    refreshed_group = _store(request).get_user_record(user_id, "groups", run_group["id"]) or run_group
    refreshed_strategy = _store(request).get_user_record(user_id, "strategies", run_strategy["id"]) or run_strategy
    _refresh_paper_run_summary(_store(request), user_id, refreshed_group, refreshed_strategy)
    return trade


def _paper_manual_close_transaction(
    request: Request,
    user_id: str,
    run_group: dict,
    run_strategy: dict,
    snapshot: dict,
) -> dict:
    trade = _store(request).find_user_record_reverse(
        user_id,
        "transactions",
        {
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "manual_trade": True,
            "status": "open",
        },
    )
    if not trade:
        raise HTTPException(422, "No open manual paper trade found")
    side = str(trade.get("side") or "UP").upper()
    token_id = str(trade.get("token_id") or "").strip()
    if not token_id:
        raise HTTPException(422, "Open manual trade is missing token_id")
    current_token_id = _paper_token_id_for_side(snapshot, side)
    if current_token_id and current_token_id != token_id:
        raise HTTPException(
            422,
            "Open manual trade token does not match the current live market token",
        )
    bid_price = _paper_current_price(snapshot, side, "bid")
    if bid_price is None or bid_price < 0:
        raise HTTPException(422, f"No live {side} bid is available")
    stored_real_quantity = _number_or_none(trade.get("real_open_quantity"))
    close_quantity = (
        math.floor(stored_real_quantity * 1_000_000) / 1_000_000
        if stored_real_quantity is not None and stored_real_quantity > 0
        else None
    )
    if close_quantity is None or close_quantity <= 0:
        raise HTTPException(422, "No Polymarket token balance is available to close this manual trade")
    response = _paper_live_order_executor(request).market_sell(
        PolymarketLiveOrderRequest(
            token_id=token_id,
            amount=close_quantity,
            side="SELL",
            order_type=_paper_real_order_type(),
            price=_paper_aggressive_limit_price("SELL"),
        )
    )
    now = utc_iso()
    exit_value = round(close_quantity * bid_price, 8)
    entry_cost = float(trade.get("margin_locked") or 0) + float(trade.get("entry_fee") or 0)
    pnl = round(exit_value - entry_cost, 8)
    balance_before = float(trade.get("balance_before") or 0)
    updated = _store(request).update_user_record(
        user_id,
        "transactions",
        trade["id"],
        {
            "status": "closed",
            "real_close_response": response,
            "closed_at": now,
            "quantity": close_quantity,
            "contracts_volume": close_quantity,
            "actual_token_balance_before_close": (
                round(stored_real_quantity, 8) if stored_real_quantity is not None else None
            ),
            "closed_quantity": close_quantity,
            "closed_quantity_source": "stored_open_balance",
            "exit_price": round(bid_price, 8),
            "exit_bid_price": round(bid_price, 8),
            "exit_type": "manual_market",
            "settlement_value": exit_value,
            "pnl": pnl,
            "available_cash_after_close": round(balance_before + pnl, 4),
            "balance_after": round(balance_before + pnl, 4),
            "holding_seconds": (
                round(datetime.fromisoformat(now).timestamp() - _timestamp_from_iso(trade.get("opened_at")), 3)
                if _timestamp_from_iso(trade.get("opened_at")) is not None
                else None
            ),
        },
    )
    paper_id = str(updated.get("paper_trade_id") or "")
    if paper_id:
        _update_paper_trade_totals(_store(request), user_id, paper_id)
    refreshed_group = _store(request).get_user_record(user_id, "groups", run_group["id"]) or run_group
    refreshed_strategy = _store(request).get_user_record(user_id, "strategies", run_strategy["id"]) or run_strategy
    _refresh_paper_run_summary(_store(request), user_id, refreshed_group, refreshed_strategy)
    return updated


def _execute_paper_real_trading_signals(
    request: Request,
    user_id: str,
    run_group: dict,
    run_strategy: dict,
    session_id: str,
    event_payload: dict,
    snapshot: dict,
    trades: list[dict],
) -> list[dict]:
    paper_meta = run_group.get("paper_trading") or {}
    if not paper_meta.get("real_trading_enabled"):
        return []
    store = _store(request)
    event_key = _paper_event_key(session_id, event_payload)
    paper_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"paper-live:{run_strategy['id']}:{event_key}",
    ).hex
    results = []
    for trade in trades:
        order_id = _paper_real_order_id(paper_id, trade)
        signal_status = str(trade.get("status") or "").lower()
        token_id = _paper_token_id_for_side(snapshot, str(trade.get("side") or ""))
        amount = _paper_real_order_amount()
        if signal_status == "closed":
            open_record = store.get_user_record(user_id, "real_orders", order_id)
            if not open_record or str(open_record.get("status") or "") != "submitted":
                continue
            close_order_id = _paper_real_close_order_id(paper_id, trade)
            existing_close = store.get_user_record(user_id, "real_orders", close_order_id)
            if existing_close and not _paper_real_order_retryable(existing_close):
                results.append(existing_close)
                continue
            token_id = str(open_record.get("token_id") or token_id or "").strip() or None
            stored_quantity = _number_or_none(open_record.get("real_open_quantity"))
            quantity, quantity_source = _paper_open_record_quantity(open_record)
            error_balance_quantity = _paper_error_balance_quantity(existing_close)
            if (
                error_balance_quantity is not None
                and error_balance_quantity > 0
                and quantity is not None
                and error_balance_quantity < quantity
                and _paper_real_order_retryable(existing_close or {})
            ):
                quantity = error_balance_quantity
                quantity_source = "previous_clob_error_balance"
            paper_signal_at = _paper_clock_ms()
            signal_tick_at = trade.get("signal_tick_captured_at") or trade.get("closed_at")
            close_base = {
                "user_id": user_id,
                "group_id": run_group["id"],
                "strategy_id": run_strategy["id"],
                "source_group_id": paper_meta.get("source_group_id"),
                "source_strategy_id": paper_meta.get("source_strategy_id"),
                "session_id": session_id,
                "paper_trade_id": paper_id,
                "live_event_key": event_key,
                "side": str(trade.get("side") or "").upper(),
                "token_id": token_id,
                "amount": quantity,
                "order_type": _paper_real_order_type(),
                "limit_price": _paper_aggressive_limit_price("SELL"),
                "order_action": "close",
                "open_order_id": order_id,
                "signal_opened_at": trade.get("opened_at"),
                "signal_closed_at": trade.get("closed_at"),
                "signal_history_at": trade.get("signal_history_at") or trade.get("closed_at"),
                "signal_sample_delay_ms": _number_or_none(trade.get("signal_sample_delay_ms")),
                "signal_tick_at": signal_tick_at,
                "paper_signal_at": paper_signal_at,
                "signal_lag_ms": _paper_signal_lag_ms(paper_signal_at, signal_tick_at),
                "signal_exit_price": trade.get("exit_price") or trade.get("exit_bid_price"),
                "signal_snapshot": _paper_signal_audit_snapshot(snapshot, trade, "close"),
                "event_slug": event_payload.get("slug"),
                "event_title": event_payload.get("title") or event_payload.get("event_title"),
                "retry_count": int((existing_close or {}).get("retry_count") or 0) + (1 if existing_close else 0),
                "last_attempt_at": utc_iso(),
            }
            try:
                if not token_id:
                    raise ValueError("Could not resolve Polymarket token_id for signal side.")
                if quantity is None or quantity <= 0:
                    raise ValueError("Could not resolve quantity to close for signal.")
                quantity = math.floor(float(quantity) * 1_000_000) / 1_000_000
                response = _paper_live_order_executor(request).market_sell(
                    PolymarketLiveOrderRequest(
                        token_id=token_id,
                        amount=quantity,
                        side="SELL",
                        order_type=_paper_real_order_type(),
                        price=_paper_aggressive_limit_price("SELL"),
                    )
                )
                timing = _paper_timing_from_response(response)
                clob_send_at = timing.get("send_at")
                real_close_quantity, real_close_proceeds, real_close_avg_price = (
                    _paper_close_fill(response)
                )
                submitted_close = {
                    **close_base,
                    "status": "submitted",
                    "submitted_at": utc_iso(),
                    "response": response,
                    "amount": quantity,
                    "actual_token_balance_before_close": (
                        round(stored_quantity, 8)
                        if stored_quantity is not None
                        else None
                    ),
                    "close_quantity_source": quantity_source,
                    "real_close_quantity": real_close_quantity,
                    "real_close_proceeds": real_close_proceeds,
                    "real_close_avg_price": real_close_avg_price,
                    "clob_send_at": clob_send_at,
                    "signal_to_clob_ms": _paper_signal_to_clob_ms(paper_signal_at, clob_send_at),
                    "clob_duration_ms": timing.get("duration_ms"),
                }
                if existing_close:
                    record = store.update_user_record(
                        user_id,
                        "real_orders",
                        close_order_id,
                        _paper_mutable_real_order_fields(submitted_close),
                    )
                else:
                    record = store.append_user_record(
                        user_id,
                        "real_orders",
                        {
                            "id": close_order_id,
                            "created_at": utc_iso(),
                            **submitted_close,
                        },
                    )
                store.log(user_id, "paper_real_order_closed", {
                    "order_id": close_order_id,
                    "open_order_id": order_id,
                    "session_id": session_id,
                    "side": record["side"],
                    "amount": quantity,
                    "token_id": token_id,
                })
            except Exception as error:
                failed_close = {
                    **close_base,
                    "status": "failed",
                    "failed_at": utc_iso(),
                    "error": _error_message(error),
                }
                if existing_close:
                    record = store.update_user_record(
                        user_id,
                        "real_orders",
                        close_order_id,
                        _paper_mutable_real_order_fields(failed_close),
                    )
                else:
                    record = store.append_user_record(
                        user_id,
                        "real_orders",
                        {
                            "id": close_order_id,
                            "created_at": utc_iso(),
                            **failed_close,
                        },
                    )
                logger.exception("Paper real trading close order failed")
                store.log(user_id, "paper_real_order_close_failed", {
                    "order_id": close_order_id,
                    "open_order_id": order_id,
                    "session_id": session_id,
                    "side": record.get("side"),
                    "amount": quantity,
                    "token_id": token_id,
                    "error": _error_message(error),
                })
            _append_real_trade_audit(
                store,
                user_id,
                f"close_{record.get('status') or 'unknown'}",
                record,
            )
            results.append(record)
            continue
        if signal_status != "open":
            continue
        existing = store.get_user_record(user_id, "real_orders", order_id)
        if existing and not _paper_real_order_retryable(existing):
            results.append(existing)
            continue
        paper_signal_at = _paper_clock_ms()
        signal_tick_at = trade.get("signal_tick_captured_at") or trade.get("opened_at")
        open_base = {
            "user_id": user_id,
            "group_id": run_group["id"],
            "strategy_id": run_strategy["id"],
            "source_group_id": paper_meta.get("source_group_id"),
            "source_strategy_id": paper_meta.get("source_strategy_id"),
            "session_id": session_id,
            "paper_trade_id": paper_id,
            "live_event_key": event_key,
            "side": str(trade.get("side") or "").upper(),
            "token_id": token_id,
            "amount": amount,
            "order_type": _paper_real_order_type(),
            "limit_price": _paper_aggressive_limit_price("BUY"),
            "order_action": "open",
            "signal_opened_at": trade.get("opened_at"),
            "signal_history_at": trade.get("signal_history_at") or trade.get("opened_at"),
            "signal_sample_delay_ms": _number_or_none(trade.get("signal_sample_delay_ms")),
            "signal_tick_at": signal_tick_at,
            "paper_signal_at": paper_signal_at,
            "signal_lag_ms": _paper_signal_lag_ms(paper_signal_at, signal_tick_at),
            "signal_entry_price": trade.get("entry_price") or trade.get("ask_price"),
            "signal_snapshot": _paper_signal_audit_snapshot(snapshot, trade, "open"),
            "event_slug": event_payload.get("slug"),
            "event_title": event_payload.get("title") or event_payload.get("event_title"),
            "retry_count": int((existing or {}).get("retry_count") or 0) + (1 if existing else 0),
            "last_attempt_at": utc_iso(),
        }
        try:
            if not token_id:
                raise ValueError("Could not resolve Polymarket token_id for signal side.")
            executor = _paper_live_order_executor(request)
            response = executor.market_buy(
                PolymarketLiveOrderRequest(
                    token_id=token_id,
                    amount=amount,
                    order_type=_paper_real_order_type(),
                    price=_paper_aggressive_limit_price("BUY"),
                )
            )
            real_open_balance = None
            real_open_quantity, real_open_quantity_source = _paper_open_fill_quantity(response)
            real_open_balance_info = None
            if real_open_quantity_source is None:
                real_open_quantity_source = "post_open_balance"
            timing = _paper_timing_from_response(response)
            clob_send_at = timing.get("send_at")
            real_open_avg_price = (
                round(amount / real_open_quantity, 8)
                if real_open_quantity is not None and real_open_quantity > 0
                else None
            )
            threading.Thread(
                target=_prepare_paper_close_order,
                args=(request, token_id, real_open_quantity),
                daemon=True,
            ).start()
            submitted_open = {
                **open_base,
                "status": "submitted",
                "submitted_at": utc_iso(),
                "response": response,
                "real_open_balance_after_open": (
                    round(real_open_balance, 8) if real_open_balance is not None else None
                ),
                "real_open_quantity": real_open_quantity,
                "real_open_quantity_source": real_open_quantity_source,
                "real_open_balance_response": real_open_balance_info,
                "real_open_notional_amount": amount,
                "real_open_avg_price": real_open_avg_price,
                "clob_send_at": clob_send_at,
                "signal_to_clob_ms": _paper_signal_to_clob_ms(paper_signal_at, clob_send_at),
                "clob_duration_ms": timing.get("duration_ms"),
            }
            if existing:
                record = store.update_user_record(
                    user_id,
                    "real_orders",
                    order_id,
                    _paper_mutable_real_order_fields(submitted_open),
                )
            else:
                record = store.append_user_record(
                    user_id,
                    "real_orders",
                    {
                        "id": order_id,
                        "created_at": utc_iso(),
                        **submitted_open,
                    },
                )
            store.log(user_id, "paper_real_order_submitted", {
                "order_id": order_id,
                "session_id": session_id,
                "side": record["side"],
                "amount": amount,
                "token_id": token_id,
            })
        except Exception as error:
            failed_open = {
                **open_base,
                "status": "failed",
                "failed_at": utc_iso(),
                "error": _error_message(error),
            }
            if existing:
                record = store.update_user_record(
                    user_id,
                    "real_orders",
                    order_id,
                    _paper_mutable_real_order_fields(failed_open),
                )
            else:
                record = store.append_user_record(
                    user_id,
                    "real_orders",
                    {
                        "id": order_id,
                        "created_at": utc_iso(),
                        **failed_open,
                    },
                )
            logger.exception("Paper real trading order failed")
            store.log(user_id, "paper_real_order_failed", {
                "order_id": order_id,
                "session_id": session_id,
                "side": record.get("side"),
                "amount": amount,
                "token_id": token_id,
                "error": _error_message(error),
            })
        _append_real_trade_audit(
            store,
            user_id,
            f"open_{record.get('status') or 'unknown'}",
            record,
        )
        results.append(record)
    return results


def _paper_visual_preview_key(session_id: str, run_strategy: dict, snapshot: dict) -> str:
    return ":".join([
        session_id,
        str(run_strategy.get("id") or ""),
        _paper_event_key(session_id, snapshot.get("event") or {}),
    ])


def _paper_cached_visual_preview(cache_key: str) -> dict | None:
    with _PAPER_VISUAL_PREVIEW_LOCK:
        entry = _PAPER_VISUAL_PREVIEWS.get(cache_key)
        payload = dict(entry.get("payload") or {}) if entry else None
    return payload or None


def _paper_lightweight_preview() -> dict:
    return {
        "paper_trades": [],
        "real_orders": [],
        "paper_trade_status": {"status": "watching"},
    }


def _paper_preview_running(cache_key: str, now: float, min_interval: float = 0.75) -> bool:
    with _PAPER_VISUAL_PREVIEW_LOCK:
        entry = _PAPER_VISUAL_PREVIEWS.setdefault(cache_key, {})
        if entry.get("running"):
            return True
        if now - float(entry.get("started_at") or 0) < min_interval:
            return True
        entry["running"] = True
        entry["started_at"] = now
        return False


def _store_paper_visual_preview(cache_key: str, payload: dict | None, error: Exception | None = None) -> None:
    with _PAPER_VISUAL_PREVIEW_LOCK:
        entry = _PAPER_VISUAL_PREVIEWS.setdefault(cache_key, {})
        entry["running"] = False
        entry["updated_at"] = time.time()
        if payload is not None:
            entry["payload"] = payload
        if error is not None:
            entry["payload"] = {
                "paper_trades": [],
                "paper_trade_status": {"status": "error", "message": _error_message(error)},
            }


def _paper_real_trading_tasks(app) -> dict[str, asyncio.Task]:
    tasks = getattr(app.state, "paper_real_trading_tasks", None)
    if not isinstance(tasks, dict):
        tasks = {}
        app.state.paper_real_trading_tasks = tasks
    return tasks


def _persist_real_session_log(
    store: FileStore,
    user_id: str,
    session,
    entry: dict,
) -> None:
    store.append_user_record(
        user_id,
        "real_trade_console",
        {
            **entry,
            "session_id": session.id,
            "event_slug": getattr(session.event, "slug", None),
        },
    )


async def _paper_balance_guard_check(
    request: Request,
    threshold: float,
    session,
) -> tuple[bool, str]:
    started = time.perf_counter()
    try:
        balance, _response = await asyncio.to_thread(
            _paper_collateral_balance_info,
            _paper_live_order_executor(request),
        )
        duration_ms = (time.perf_counter() - started) * 1000.0
        allowed = balance >= threshold
        message = (
            f"Polymarket collateral balance ${balance:.2f}; minimum ${threshold:.2f}; "
            f"check {duration_ms:.2f}ms."
        )
        await session._log("info" if allowed else "warning", "balance", message)
        return allowed, message
    except Exception as error:
        message = f"Could not verify Polymarket collateral balance: {_error_message(error)}"
        await session._log("error", "balance", message)
        return False, message


def _track_paper_real_trading_task(app, key: str, task: asyncio.Task) -> None:
    _background_tasks(app).add(task)
    _paper_real_trading_tasks(app)[key] = task

    def cleanup(done: asyncio.Task) -> None:
        _background_tasks(app).discard(done)
        if _paper_real_trading_tasks(app).get(key) is done:
            _paper_real_trading_tasks(app).pop(key, None)

    task.add_done_callback(cleanup)


def _schedule_paper_contract_finalization(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    saved_preview: dict | None,
) -> None:
    if not saved_preview:
        return

    def finalize() -> None:
        started = time.perf_counter()
        try:
            _finalize_paper_live_contract(
                request,
                user_id,
                source_group,
                source_strategy,
                run_group,
                run_strategy,
                saved_preview,
            )
        finally:
            duration_ms = (time.perf_counter() - started) * 1000.0
            if duration_ms >= 250.0:
                logger.warning(
                    "Slow paper contract finalization session=%s strategy=%s "
                    "duration_ms=%.1f saved_record=%s",
                    (run_group.get("paper_trading") or {}).get("session_id"),
                    run_strategy.get("id"),
                    duration_ms,
                    saved_preview.get("id"),
                )

    task = asyncio.create_task(
        asyncio.to_thread(finalize)
    )
    _background_tasks(request.app).add(task)

    def cleanup(done: asyncio.Task) -> None:
        _background_tasks(request.app).discard(done)
        try:
            done.result()
        except Exception:
            logger.exception("Paper trading contract finalization failed")

    task.add_done_callback(cleanup)


def _paper_decision_key(session_id: str, run_strategy: dict, snapshot: dict) -> tuple:
    history = [
        row for row in snapshot.get("history") or []
        if isinstance(row, dict) and _number_or_none(row.get("t")) is not None
    ]
    last_history_ts = float(history[-1]["t"]) if history else None
    return (
        session_id,
        str(run_strategy.get("id") or ""),
        _paper_event_key(session_id, snapshot.get("event") or {}),
        len(history),
        last_history_ts,
    )


def _paper_threshold_signal_changed(
    session_id: str,
    source_strategy: dict,
    run_strategy: dict,
    snapshot: dict,
    state: dict,
) -> bool:
    """Return true only when the threshold strategy can change trading state.

    Its canonical preview still creates and executes the trade. This small scanner only
    avoids rerunning the full backtest and file-backed accounting on every quote tick.
    """
    spec = _paper_strategy_spec(source_strategy, run_strategy)
    if str(spec.get("family") or "") != "price_threshold_70_stop_40":
        return True

    event = snapshot.get("event") or {}
    event_key = _paper_event_key(session_id, event)
    event_changed = state.get("event_key") != event_key
    if event_changed:
        state.clear()
        state.update({
            "event_key": event_key,
            "processed_rows": 0,
            "entry_index": None,
            "entry_side": None,
            "last_processed_ts": None,
            "target_known": False,
        })

    window_start = _number_or_none(event.get("window_start_ts"))
    window_end = _number_or_none(event.get("window_end_ts"))
    rows = sorted(
        (
            row for row in snapshot.get("history") or []
            if isinstance(row, dict)
            and _number_or_none(row.get("t")) is not None
            and (window_start is None or float(row["t"]) >= window_start)
            and (window_end is None or float(row["t"]) <= window_end)
        ),
        key=lambda row: float(row["t"]),
    )
    processed_rows = int(state.get("processed_rows") or 0)
    prefix_changed = (
        processed_rows > len(rows)
        or (
            processed_rows > 0
            and state.get("last_processed_ts") != float(rows[processed_rows - 1]["t"])
        )
    )
    if prefix_changed:
        processed_rows = 0
        state["entry_index"] = None
        state["entry_side"] = None
        state.pop("stop_index", None)

    target_known = _paper_target_known_at_window_start(event, rows)
    target_became_known = target_known and not bool(state.get("target_known"))
    state["target_known"] = target_known

    entry_threshold = float(spec.get("entry_price_cents", 70.0)) / 100
    stop_loss = float(spec.get("stop_loss_cents", 40.0)) / 100
    min_entry_ts = (window_start or 0.0) + float(spec.get("entry_second", 0.0))
    signal_changed = False
    for index in range(processed_rows, len(rows)) if state.get("stop_index") is None else ():
        row = rows[index]
        if state.get("entry_index") is None:
            if float(row["t"]) < min_entry_ts:
                continue
            candidates = []
            for side in ("up", "down"):
                ask = LiveMonitorStrategyGenerator._side_ask(row, side)
                if ask is not None and ask >= entry_threshold:
                    candidates.append((side, ask))
            if candidates:
                side, _ = max(candidates, key=lambda item: item[1])
                state["entry_index"] = index
                state["entry_side"] = side
                signal_changed = True
            continue
        if index <= int(state["entry_index"]):
            continue
        bid = LiveMonitorStrategyGenerator._side_bid(row, str(state["entry_side"]))
        if bid is not None and bid <= stop_loss:
            state["stop_index"] = index
            signal_changed = True
            break

    state["processed_rows"] = len(rows)
    state["last_processed_ts"] = float(rows[-1]["t"]) if rows else None
    # A rollover by itself cannot open or close this strategy. Running the full
    # file-backed preview here used to contend with old-contract finalization at
    # exactly the moment the new sampler needed to stay responsive.
    has_entry = state.get("entry_index") is not None
    return signal_changed or (target_became_known and has_entry)


async def _paper_real_trading_worker(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    session,
) -> None:
    queue = session.add_subscriber()
    last_decision_key = None
    last_health_log_at = 0.0
    threshold_signal_state: dict = {}
    logger.info(
        "Paper real trading worker started session=%s strategy=%s",
        session.id,
        run_strategy.get("id"),
    )
    try:
        while True:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=2)
            except asyncio.TimeoutError:
                if session.is_finished:
                    break
                continue
            event_name = str(message.get("event") or "")
            if event_name == "saved":
                saved_preview = (message.get("data") or {}).get("record")
                _schedule_paper_contract_finalization(
                    request,
                    user_id,
                    source_group,
                    source_strategy,
                    run_group,
                    run_strategy,
                    saved_preview,
                )
                if session.is_finished:
                    break
                continue
            if event_name not in {"snapshot", "tick", "rolled"}:
                if session.is_finished:
                    break
                continue
            snapshot = session.snapshot(history_limit=None)
            paper_meta = run_group.get("paper_trading") or {}
            if event_name == "rolled" and paper_meta.get("balance_guard_enabled"):
                configured_threshold = _number_or_none(paper_meta.get("min_open_balance"))
                threshold = (
                    configured_threshold
                    if configured_threshold is not None and configured_threshold >= 0
                    else _paper_min_open_balance()
                )
                allowed, reason = await _paper_balance_guard_check(
                    request,
                    threshold,
                    session,
                )
                if not allowed:
                    await session._log(
                        "warning",
                        "balance",
                        f"{reason} Monitoring stopped before trading the new contract.",
                    )
                    await request.app.state.live_monitor.stop(session.id)
                    break
            if event_name in {"snapshot", "rolled"}:
                _schedule_paper_live_order_token_warmup(
                    request,
                    _paper_snapshot_token_ids(snapshot),
                )
            decision_key = _paper_decision_key(session.id, run_strategy, snapshot)
            if decision_key == last_decision_key:
                if session.is_finished:
                    break
                continue
            last_decision_key = decision_key
            if not _paper_threshold_signal_changed(
                session.id,
                source_strategy,
                run_strategy,
                snapshot,
                threshold_signal_state,
            ):
                if session.is_finished:
                    break
                continue
            cache_key = _paper_visual_preview_key(session.id, run_strategy, snapshot)
            preview_snapshot = dict(snapshot)
            preview_snapshot["history"] = list(snapshot.get("history") or [])
            decision_started = time.perf_counter()
            try:
                paper_payload = await asyncio.to_thread(
                    _paper_live_trade_preview,
                    request,
                    user_id,
                    source_group,
                    source_strategy,
                    run_group,
                    run_strategy,
                    preview_snapshot,
                    persist=False,
                    execute_real_orders=True,
                )
                _store_paper_visual_preview(cache_key, paper_payload)
                duration_ms = (time.perf_counter() - decision_started) * 1000.0
                history = preview_snapshot.get("history") or []
                captured_at = (
                    _number_or_none(history[-1].get("captured_at"))
                    if history and isinstance(history[-1], dict)
                    else None
                )
                decision_lag_ms = (
                    max(0.0, (time.time() - captured_at) * 1000.0)
                    if captured_at is not None
                    else None
                )
                now = time.monotonic()
                if duration_ms >= 500.0 or (decision_lag_ms or 0.0) >= 1000.0:
                    logger.warning(
                        "Slow paper decision session=%s strategy=%s duration_ms=%.1f "
                        "decision_lag_ms=%s history_rows=%s queue_size=%s",
                        session.id,
                        run_strategy.get("id"),
                        duration_ms,
                        round(decision_lag_ms, 1) if decision_lag_ms is not None else None,
                        len(history),
                        queue.qsize(),
                    )
                elif now - last_health_log_at >= 60.0:
                    last_health_log_at = now
                    logger.info(
                        "Paper decision health session=%s strategy=%s duration_ms=%.1f "
                        "history_rows=%s queue_size=%s",
                        session.id,
                        run_strategy.get("id"),
                        duration_ms,
                        len(history),
                        queue.qsize(),
                    )
            except Exception as error:
                logger.exception("Paper real trading decision worker failed")
                _store_paper_visual_preview(cache_key, None, error)
            if session.is_finished:
                break
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "Paper real trading worker stopped unexpectedly session=%s strategy=%s",
            session.id,
            run_strategy.get("id"),
        )
        raise
    finally:
        session.remove_subscriber(queue)
        logger.info(
            "Paper real trading worker stopped session=%s strategy=%s",
            session.id,
            run_strategy.get("id"),
        )


def _ensure_paper_real_trading_worker(
    request: Request,
    user_id: str,
    source_group: dict,
    source_strategy: dict,
    run_group: dict,
    run_strategy: dict,
    session,
) -> None:
    if not (run_group.get("paper_trading") or {}).get("real_trading_enabled"):
        return
    key = f"{user_id}:{session.id}:{run_strategy['id']}"
    tasks = _paper_real_trading_tasks(request.app)
    existing = tasks.get(key)
    if existing and not existing.done():
        return
    request_proxy = SimpleNamespace(app=request.app)
    task = asyncio.create_task(
        _paper_real_trading_worker(
            request_proxy,
            user_id,
            source_group,
            source_strategy,
            run_group,
            run_strategy,
            session,
        )
    )
    _track_paper_real_trading_task(request.app, key, task)


def _paper_deleted_saved_record_ids(group: dict) -> set[str]:
    values = (group.get("paper_trading") or {}).get("deleted_saved_record_ids") or []
    return {str(value) for value in values if str(value or "").strip()}


def _mark_paper_saved_record_deleted(
    store: FileStore,
    user_id: str,
    group: dict,
    saved_record_id: str | None,
) -> None:
    record_id = str(saved_record_id or "").strip()
    if not record_id:
        return
    paper_meta = dict(group.get("paper_trading") or {})
    deleted = sorted(_paper_deleted_saved_record_ids(group) | {record_id})
    paper_meta["deleted_saved_record_ids"] = deleted
    store.update_user_record(user_id, "groups", group["id"], {"paper_trading": paper_meta})
    group["paper_trading"] = paper_meta


@router.get("/groups/{group_id}/strategies/{strategy_id}/paper-trading", response_class=HTMLResponse)
def paper_trading_page(request: Request, group_id: str, strategy_id: str):
    _user, _store_obj, group, strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    return templates.TemplateResponse(
        request,
        "paper_trading.html",
        _context(
            request,
            group=group,
            strategy=strategy,
            min_open_balance=_paper_min_open_balance(),
        ),
    )


@router.post("/api/paper-trading/{group_id}/strategies/{strategy_id}/monitors")
async def start_paper_trading_monitor(
    request: Request,
    group_id: str,
    strategy_id: str,
    event_url: Annotated[str, Form()],
    starting_balance: Annotated[float, Form()] = 100.0,
    real_trading: Annotated[bool, Form()] = False,
    server_optimized: Annotated[bool, Form()] = False,
    balance_guard_enabled: Annotated[bool, Form()] = False,
    min_open_balance: Annotated[float, Form()] = 130.0,
):
    user, _store_obj, _group, _strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    initial_balance_message = None
    initial_guard_failure_logged = False
    try:
        if not math.isfinite(starting_balance) or starting_balance <= 0:
            raise ValueError("Starting balance must be greater than zero")
        if not math.isfinite(min_open_balance) or min_open_balance < 0:
            raise ValueError("Minimum Polymarket balance must be zero or greater")
        if real_trading and balance_guard_enabled:
            balance_started = time.perf_counter()
            account_balance, _balance_response = await asyncio.to_thread(
                _paper_collateral_balance_info,
                _paper_live_order_executor(request),
            )
            balance_duration_ms = (time.perf_counter() - balance_started) * 1000.0
            initial_balance_message = (
                f"Polymarket collateral balance ${account_balance:.2f}; minimum "
                f"${min_open_balance:.2f}; pre-monitor check {balance_duration_ms:.2f}ms."
            )
            if account_balance < min_open_balance:
                _store(request).append_user_record(
                    user["id"],
                    "real_trade_console",
                    {
                        "created_at": _paper_clock_ms(),
                        "level": "warning",
                        "source": "balance",
                        "message": initial_balance_message + " Monitoring was not started.",
                    },
                )
                initial_guard_failure_logged = True
                raise ValueError(initial_balance_message + " Monitoring was not started.")
        session = await request.app.state.live_monitor.start(user["id"], event_url)
    except (httpx.HTTPError, ValueError, OSError, RuntimeError) as error:
        if real_trading and balance_guard_enabled and not initial_guard_failure_logged:
            try:
                _store(request).append_user_record(
                    user["id"],
                    "real_trade_console",
                    {
                        "created_at": _paper_clock_ms(),
                        "level": "error",
                        "source": "balance",
                        "message": (
                            "Pre-monitor balance check or monitor start failed: "
                            f"{_error_message(error)}"
                        ),
                    },
                )
            except Exception:
                logger.exception("Could not persist pre-monitor balance guard failure")
        raise HTTPException(422, _error_message(error)) from error
    if real_trading:
        await session.enable_persistent_log(
            lambda entry: _persist_real_session_log(
                _store(request), user["id"], session, entry
            )
        )
        if initial_balance_message:
            await session._log("info", "balance", initial_balance_message)
    run_group, run_strategy = _create_paper_run_group(
        _store(request),
        user["id"],
        _group,
        _strategy,
        session.id,
        session.event,
        starting_balance,
        bool(real_trading),
        bool(server_optimized),
        bool(balance_guard_enabled),
        float(min_open_balance),
    )
    _ensure_paper_real_trading_worker(
        request,
        user["id"],
        _group,
        _strategy,
        run_group,
        run_strategy,
        session,
    )
    if real_trading:
        token_ids = _paper_snapshot_token_ids(session.snapshot(history_limit=None))
        if token_ids:
            if server_optimized:
                _schedule_paper_live_order_token_warmup(request, token_ids)
            else:
                await asyncio.to_thread(_warm_paper_live_order_tokens, request, token_ids)
        else:
            asyncio.create_task(
                asyncio.to_thread(
                    _warm_paper_live_order_executor,
                    request,
                    user["id"],
                    session.id,
                )
            )
    payload = _paper_session_payload(
        request, user["id"], _group, _strategy, run_group, run_strategy, session
    )
    return JSONResponse(payload, status_code=202)


@router.get("/api/paper-trading/{group_id}/strategies/{strategy_id}/active")
def active_paper_trading_monitor(request: Request, group_id: str, strategy_id: str):
    user, store, _group, _strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    payloads = []
    for run_group, run_strategy in _paper_run_for_source(
        store, user["id"], group_id, strategy_id
    ):
        session_id = str(run_group.get("paper_trading", {}).get("session_id") or "")
        session = request.app.state.live_monitor.get(session_id)
        if not session or session.user_id != user["id"] or session.is_finished:
            continue
        payload = _paper_session_payload(
            request, user["id"], _group, _strategy, run_group, run_strategy, session
        )
        payloads.append(payload)
    if not payloads:
        return {"active": False, "sessions": []}
    return {"active": True, "sessions": payloads, **payloads[0]}


@router.get("/api/paper-trading/{group_id}/strategies/{strategy_id}/sessions/{session_id}/events")
async def paper_trading_monitor_events(
    request: Request,
    group_id: str,
    strategy_id: str,
    session_id: str,
):
    user, store, source_group, source_strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    session = request.app.state.live_monitor.get(session_id)
    if not session or session.user_id != user["id"]:
        raise HTTPException(404, "Paper trading live session not found")
    runs = _paper_run_for_source(store, user["id"], group_id, strategy_id)
    run_pair = next(
        (
            (run_group, run_strategy)
            for run_group, run_strategy in runs
            if str(run_group.get("paper_trading", {}).get("session_id") or "") == session_id
        ),
        None,
    )
    if run_pair is None:
        raise HTTPException(404, "Paper trading run not found for this session")
    run_group, run_strategy = run_pair
    _ensure_paper_real_trading_worker(
        request,
        user["id"],
        source_group,
        source_strategy,
        run_group,
        run_strategy,
        session,
    )
    queue = session.add_subscriber()

    async def stream():
        try:
            yield "retry: 1000\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=2)
                except asyncio.TimeoutError:
                    yield (
                        "event: ping\n"
                        f"data: {json.dumps({'server_time': time.time()})}\n\n"
                    )
                    continue
                event_name = message["event"]
                if event_name == "saved":
                    saved_record_preview = (message.get("data") or {}).get("record")

                    async def finalize_saved_contract(saved_preview=saved_record_preview) -> None:
                        try:
                            await asyncio.to_thread(
                                _finalize_paper_live_contract,
                                request,
                                user["id"],
                                source_group,
                                source_strategy,
                                run_group,
                                run_strategy,
                                saved_preview,
                            )
                        except Exception:
                            logger.exception("Paper trading contract finalization failed")

                    if not (run_group.get("paper_trading") or {}).get("real_trading_enabled"):
                        asyncio.create_task(finalize_saved_contract())
                snapshot = dict(message.get("data") or {})
                if not snapshot:
                    snapshot = session.snapshot(history_limit=0)
                cache_key = _paper_visual_preview_key(session_id, run_strategy, snapshot)
                cached_paper_payload = _paper_cached_visual_preview(cache_key)
                if (run_group.get("paper_trading") or {}).get("real_trading_enabled"):
                    paper_payload = (
                        cached_paper_payload
                        or _paper_lightweight_preview()
                    )
                else:
                    paper_payload = (
                        cached_paper_payload
                        or _paper_lightweight_preview()
                    )
                if not (run_group.get("paper_trading") or {}).get("real_trading_enabled") and not _paper_preview_running(cache_key, time.time()):
                    preview_snapshot = dict(snapshot)
                    preview_snapshot["history"] = list(snapshot.get("history") or [])

                    async def refresh_visual_preview(
                        key=cache_key,
                        current_snapshot=preview_snapshot,
                    ) -> None:
                        try:
                            payload = await asyncio.to_thread(
                                _paper_live_trade_preview,
                                request,
                                user["id"],
                                source_group,
                                source_strategy,
                                run_group,
                                run_strategy,
                                current_snapshot,
                                persist=False,
                            )
                            _store_paper_visual_preview(key, payload)
                        except Exception as error:
                            logger.exception("Paper trading visual preview failed")
                            _store_paper_visual_preview(key, None, error)

                    asyncio.create_task(refresh_visual_preview())
                snapshot.update(paper_payload)
                encoded = await _stream_json(snapshot)
                yield (
                    f"event: {event_name}\n"
                    f"data: {encoded}\n\n"
                )
        finally:
            session.remove_subscriber(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/api/paper-trading/{group_id}/strategies/{strategy_id}/sessions/{session_id}/manual/open")
def open_manual_paper_trade(request: Request, group_id: str, strategy_id: str, session_id: str):
    user = _require_user(request)
    source_group, source_strategy, run_group, run_strategy, _session, snapshot = _paper_manual_scope(
        request, user["id"], group_id, strategy_id, session_id
    )
    if not (run_group.get("paper_trading") or {}).get("real_trading_enabled"):
        raise HTTPException(422, "Start this paper session with Real trading enabled first")
    trade = _paper_manual_open_transaction(
        request,
        user["id"],
        source_group,
        source_strategy,
        run_group,
        run_strategy,
        session_id,
        snapshot,
        "UP",
    )
    snapshot.update(_paper_existing_trade_preview(request, user["id"], run_group, snapshot))
    return {
        "status": "opened",
        "trade": _paper_trade_marker_payload(trade),
        "snapshot": snapshot,
    }


@router.post("/api/paper-trading/{group_id}/strategies/{strategy_id}/sessions/{session_id}/manual/close")
def close_manual_paper_trade(request: Request, group_id: str, strategy_id: str, session_id: str):
    user = _require_user(request)
    _source_group, _source_strategy, run_group, run_strategy, _session, snapshot = _paper_manual_scope(
        request, user["id"], group_id, strategy_id, session_id
    )
    if not (run_group.get("paper_trading") or {}).get("real_trading_enabled"):
        raise HTTPException(422, "Start this paper session with Real trading enabled first")
    trade = _paper_manual_close_transaction(
        request,
        user["id"],
        run_group,
        run_strategy,
        snapshot,
    )
    snapshot.update(_paper_existing_trade_preview(request, user["id"], run_group, snapshot))
    return {
        "status": "closed",
        "trade": _paper_trade_marker_payload(trade),
        "snapshot": snapshot,
    }


@router.post("/api/paper-trading/{group_id}/strategies/{strategy_id}/sync")
def sync_paper_trading_contracts(request: Request, group_id: str, strategy_id: str):
    user, store, source_group, source_strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    visible_runs = _paper_visible_runs_for_source(
        request, store, user["id"], group_id, strategy_id
    )
    generator = LiveMonitorStrategyGenerator(store, request.app.state.live_monitor)
    created = 0
    skipped = 0
    no_trade = 0
    skipped_first_contract = 0
    total = 0
    for run_group, run_strategy in visible_runs:
        session_id = str(run_group.get("paper_trading", {}).get("session_id") or "").strip()
        if not session_id:
            continue
        asset = str((source_group.get("assets") or [""])[0]).upper()
        timeframe = str(source_group.get("timeframe") or "")
        first_saved_record_id = _paper_first_session_contract_id(
            request, user["id"], session_id, asset, timeframe
        )
        deleted_saved_record_ids = _paper_deleted_saved_record_ids(run_group)
        previews = [
            record for record in request.app.state.live_monitor.list_saved_previews(
                user["id"], category=asset.lower()
            )
            if str(record.get("session_id") or "") == session_id
            and generator._record_timeframe(record) == timeframe
        ]
        previews.sort(
            key=lambda item: (
                _number_or_none(item.get("window_start_ts")) or math.inf,
                str(item.get("saved_at") or ""),
            )
        )
        existing_saved_record_ids = {
            str(record.get("saved_record_id") or "")
            for record in store.list_user_records_filtered(
                user["id"],
                "paper_trades",
                {
                    "group_id": run_group["id"],
                    "strategy_id": run_strategy["id"],
                },
            )
            if str(record.get("saved_record_id") or "")
        }
        for preview in previews:
            saved_record_id = str(preview.get("id") or "").strip()
            if not saved_record_id:
                continue
            total += 1
            if saved_record_id == first_saved_record_id:
                skipped_first_contract += 1
                continue
            if saved_record_id in deleted_saved_record_ids:
                skipped += 1
                continue
            if saved_record_id in existing_saved_record_ids:
                skipped += 1
                continue
            _finalize_paper_live_contract(
                request,
                user["id"],
                source_group,
                source_strategy,
                run_group,
                run_strategy,
                preview,
            )
            finalized = store.list_user_records_filtered(
                user["id"],
                "paper_trades",
                {
                    "group_id": run_group["id"],
                    "strategy_id": run_strategy["id"],
                    "saved_record_id": saved_record_id,
                },
            )
            if finalized:
                created += 1
                existing_saved_record_ids.add(saved_record_id)
            else:
                no_trade += 1
    return {
        "created": created,
        "skipped": skipped,
        "no_trade": no_trade,
        "skipped_first_contract": skipped_first_contract,
        "total": total,
        "run_count": len(visible_runs),
        "disabled": False,
        "message": "Paper trades synchronized from saved live-monitor contracts.",
    }


@router.get("/api/paper-trading/{group_id}/strategies/{strategy_id}/contracts")
def paper_trading_contracts(
    request: Request,
    group_id: str,
    strategy_id: str,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
):
    user, store, _group, _strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    visible_runs = _paper_visible_runs_for_source(
        request, store, user["id"], group_id, strategy_id
    )
    run_group_ids = {group["id"] for group, _strategy in visible_runs}
    all_rows = [] if not run_group_ids else [
        row for row in store.list_user_records(user["id"], "paper_trades")
        if row.get("source_group_id") == group_id
        and row.get("source_strategy_id") == strategy_id
        and row.get("group_id") in run_group_ids
    ]
    all_rows.sort(key=lambda item: str(item.get("saved_at") or item.get("created_at") or ""), reverse=True)
    total = len(all_rows)
    total_pages = max(1, math.ceil(total / page_size))
    current_page = min(page, total_pages)
    start = (current_page - 1) * page_size
    rows = all_rows[start:start + page_size]
    return {
        "records": [_paper_trade_list_record(row) for row in rows],
        "page": current_page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "has_previous": current_page > 1,
        "has_next": current_page < total_pages,
        "categories": [],
    }


@router.get("/api/paper-trading/{group_id}/strategies/{strategy_id}/contracts/{paper_id}")
def paper_trading_contract_detail(
    request: Request,
    group_id: str,
    strategy_id: str,
    paper_id: str,
):
    user, store, _group, _strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    record = store.get_user_record(user["id"], "paper_trades", paper_id)
    if (
        not record
        or record.get("source_group_id") != group_id
        or record.get("source_strategy_id") != strategy_id
    ):
        raise HTTPException(404, "Paper-traded contract not found")
    return _paper_trade_contract_payload(request, user["id"], record)


@router.delete("/api/paper-trading/{group_id}/strategies/{strategy_id}/contracts/{paper_id}")
def delete_paper_trading_contract(
    request: Request,
    group_id: str,
    strategy_id: str,
    paper_id: str,
):
    user, store, _group, _strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    record = store.get_user_record(user["id"], "paper_trades", paper_id)
    if (
        not record
        or record.get("source_group_id") != group_id
        or record.get("source_strategy_id") != strategy_id
    ):
        raise HTTPException(404, "Paper-traded contract not found")
    deleted = store.delete_user_record(user["id"], "paper_trades", paper_id)
    removed_transactions = store.delete_user_records_filtered(
        user["id"], "transactions", {"paper_trade_id": paper_id}
    )
    run_group = store.get_user_record(user["id"], "groups", record["group_id"])
    if run_group:
        _mark_paper_saved_record_deleted(
            store, user["id"], run_group, record.get("saved_record_id")
        )
    return {"deleted": deleted, "removed_transactions": removed_transactions}


@router.delete("/api/paper-trading/{group_id}/strategies/{strategy_id}/contracts/{paper_id}/trades/{trade_id}")
def delete_paper_trading_trade(
    request: Request,
    group_id: str,
    strategy_id: str,
    paper_id: str,
    trade_id: str,
):
    user, store, _group, _strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    record = store.get_user_record(user["id"], "paper_trades", paper_id)
    if (
        not record
        or record.get("source_group_id") != group_id
        or record.get("source_strategy_id") != strategy_id
    ):
        raise HTTPException(404, "Paper-traded contract not found")
    trade = store.get_user_record(user["id"], "transactions", trade_id)
    if (
        not trade
        or trade.get("paper_trade_id") != paper_id
        or trade.get("group_id") != record.get("group_id")
        or trade.get("strategy_id") != record.get("strategy_id")
    ):
        raise HTTPException(404, "Paper trade not found")
    removed_transactions = 1 if store.delete_user_record(user["id"], "transactions", trade_id) else 0
    if removed_transactions:
        _update_paper_trade_totals(store, user["id"], paper_id)
    return {
        "deleted": bool(removed_transactions),
        "removed_transactions": removed_transactions,
    }


@router.delete("/api/paper-trading/{group_id}/strategies/{strategy_id}/contracts/{paper_id}/trades")
def delete_all_paper_trading_trades(
    request: Request,
    group_id: str,
    strategy_id: str,
    paper_id: str,
):
    user, store, _group, _strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    record = store.get_user_record(user["id"], "paper_trades", paper_id)
    if (
        not record
        or record.get("source_group_id") != group_id
        or record.get("source_strategy_id") != strategy_id
    ):
        raise HTTPException(404, "Paper-traded contract not found")
    removed_transactions = store.delete_user_records_filtered(
        user["id"], "transactions", {"paper_trade_id": paper_id}
    )
    if removed_transactions:
        _update_paper_trade_totals(store, user["id"], paper_id)
    return {
        "deleted": removed_transactions > 0,
        "removed_transactions": removed_transactions,
    }


@router.delete("/api/paper-trading/{group_id}/strategies/{strategy_id}/trades")
def delete_all_paper_trading_runs_trades(
    request: Request,
    group_id: str,
    strategy_id: str,
):
    user, store, _group, _strategy = _paper_trading_strategy_scope(
        request, group_id, strategy_id
    )
    visible_runs = _paper_visible_runs_for_source(
        request, store, user["id"], group_id, strategy_id
    )
    run_group_ids = {group["id"] for group, _run_strategy in visible_runs}
    records = [] if not run_group_ids else [
        row for row in store.list_user_records(user["id"], "paper_trades")
        if row.get("source_group_id") == group_id
        and row.get("source_strategy_id") == strategy_id
        and row.get("group_id") in run_group_ids
    ]
    removed_transactions = 0
    removed_contracts = 0
    for record in records:
        run_group = store.get_user_record(user["id"], "groups", record.get("group_id"))
        if run_group:
            _mark_paper_saved_record_deleted(
                store, user["id"], run_group, record.get("saved_record_id")
            )
        removed_transactions += store.delete_user_records_filtered(
            user["id"], "transactions", {"paper_trade_id": record["id"]}
        )
        removed_contracts += int(store.delete_user_record(user["id"], "paper_trades", record["id"]))
    return {
        "deleted": bool(removed_transactions or removed_contracts),
        "removed_transactions": removed_transactions,
        "removed_contracts": removed_contracts,
    }


@router.post("/groups")
async def create_group(
    request: Request,
    group_name: Annotated[str, Form()],
    starting_balance: Annotated[float, Form()],
    start_date: Annotated[str, Form()],
    end_date: Annotated[str, Form()],
    strategy_count: Annotated[int, Form()],
    asset_symbols: Annotated[list[str], Form()],
    timeframe: Annotated[str, Form()],
    assumed_ask_price_cents: Annotated[float, Form()] = 51.0,
):
    user = _require_user(request)
    try:
        collected = await request.app.state.collector.collect(
            asset_symbols, timeframe, start_date, end_date
        )
        group = await asyncio.to_thread(
            StrategyGroupGenerator(_store(request)).generate,
            user["id"], group_name, asset_symbols, timeframe, start_date, end_date,
            strategy_count, starting_balance, assumed_ask_price_cents,
        )
        request.session["message"] = (
            f"Created {group['strategy_count']} strategies from {group['event_count']} events "
            f"({collected.get('new_events', collected['events'])} newly saved)."
        )
        return RedirectResponse(f"/groups/{group['id']}", status_code=303)
    except (ValueError, OSError) as error:
        request.session["error"] = str(error)
    except Exception as error:
        request.session["error"] = f"Group generation failed: {error}"
    return RedirectResponse("/groups/new", status_code=303)


@router.post("/api/group-jobs")
async def start_group_job(
    request: Request,
    group_name: Annotated[str, Form()],
    starting_balance: Annotated[float, Form()],
    start_date: Annotated[str, Form()],
    end_date: Annotated[str, Form()],
    strategy_count: Annotated[int, Form()],
    asset_symbols: Annotated[list[str], Form()],
    timeframe: Annotated[str, Form()],
    assumed_ask_price_cents: Annotated[float, Form()] = 51.0,
):
    user = _require_user(request)
    job = request.app.state.job_store.create(user["id"])
    task = asyncio.create_task(
        _run_generation_job(
            request.app, job["id"], user["id"], group_name, starting_balance,
            start_date, end_date, strategy_count, assumed_ask_price_cents,
            asset_symbols, timeframe,
        )
    )
    _track_job_task(request.app, job["id"], task)
    return JSONResponse(
        {"job_id": job["id"], "events_url": f"/api/group-jobs/{job['id']}/events"},
        status_code=202,
    )


@router.post("/api/group-jobs/{job_id}/cancel")
async def cancel_group_job(request: Request, job_id: str):
    user = _require_user(request)
    try:
        job = request.app.state.job_store.get(job_id)
    except ValueError as error:
        raise HTTPException(404, "Generation job not found") from error
    if not job or job.get("user_id") != user["id"]:
        raise HTTPException(404, "Generation job not found")
    if job.get("status") in request.app.state.job_store.TERMINAL:
        return JSONResponse({"status": job["status"]})
    task = request.app.state.job_tasks.get(job_id)
    request.app.state.job_store.cancel(job_id)
    if task and not task.done():
        task.cancel()
    return JSONResponse({"status": "cancelling"})


@router.get("/api/group-jobs/{job_id}/events")
async def group_job_events(request: Request, job_id: str):
    user = _require_user(request)
    try:
        job = request.app.state.job_store.get(job_id)
    except ValueError as error:
        raise HTTPException(404, "Generation job not found") from error
    if not job or job.get("user_id") != user["id"]:
        raise HTTPException(404, "Generation job not found")
    try:
        last_sequence = int(request.headers.get("last-event-id", "0"))
    except ValueError:
        last_sequence = 0

    async def stream():
        nonlocal last_sequence
        idle_cycles = 0
        while True:
            current = request.app.state.job_store.get(job_id)
            if not current or current.get("user_id") != user["id"]:
                break
            pending = [
                event for event in current["events"] if event["sequence"] > last_sequence
            ]
            for event in pending:
                last_sequence = event["sequence"]
                yield (
                    f"id: {last_sequence}\n"
                    "event: progress\n"
                    f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                )
            if current["status"] in request.app.state.job_store.TERMINAL:
                break
            idle_cycles += 1
            if idle_cycles % 30 == 0:
                yield ": keep-alive\n\n"
            await asyncio.sleep(0.25)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/fixed-cache/coverage")
def fixed_cache_coverage(
    request: Request,
    asset_symbols: list[str] = Query(default=[]),
    timeframe: str = Query("5m"),
    start_date: str = Query(""),
    end_date: str = Query(""),
):
    _require_user(request)
    assets = validate_selection(asset_symbols, timeframe)
    if start_date and end_date:
        start_time, end_time = parse_period(start_date, end_date)
    else:
        end_day = date.today()
        start_time, end_time = parse_period(
            (end_day - timedelta(days=60)).isoformat(), end_day.isoformat()
        )
    return _store(request).fixed_coverage_calendar(
        assets, timeframe, start_time, end_time
    )


@router.post("/api/fixed-cache/coverage/delete")
def delete_fixed_cache_coverage(
    request: Request,
    asset_symbols: Annotated[list[str], Form()],
    timeframe: Annotated[str, Form()],
    start_date: Annotated[str, Form()],
    end_date: Annotated[str, Form()],
):
    _require_user(request)
    assets = validate_selection(asset_symbols, timeframe)
    start_time, end_time = parse_period(start_date, end_date)
    removed = _store(request).clear_fixed_cache(assets, timeframe, start_time, end_time)
    return {
        **removed,
        "message": (
            f"Deleted cached historical data for {', '.join(assets)} "
            f"{timeframe} from {start_date} through {end_date}."
        ),
    }


async def _run_generation_job(
    app,
    job_id: str,
    user_id: str,
    group_name: str,
    starting_balance: float,
    start_date: str,
    end_date: str,
    strategy_count: int,
    assumed_ask_price_cents: float,
    asset_symbols: list[str],
    timeframe: str,
) -> None:
    def ensure_not_cancelled() -> None:
        current = app.state.job_store.get(job_id)
        if current and current.get("status") == "cancelled":
            raise asyncio.CancelledError

    def report(payload: dict) -> dict:
        ensure_not_cancelled()
        return app.state.job_store.append(job_id, payload)

    try:
        await app.state.collector.collect(
            asset_symbols, timeframe, start_date, end_date, progress=report
        )
        ensure_not_cancelled()
        report({
            "stage": "backtest",
            "message": f"Starting strategy backtests (strategy 0 of {strategy_count})...",
            "current": 0,
            "total": strategy_count,
        })
        group = await asyncio.to_thread(
            StrategyGroupGenerator(app.state.store).generate,
            user_id, group_name, asset_symbols, timeframe, start_date, end_date,
            strategy_count, starting_balance, assumed_ask_price_cents,
            report, ensure_not_cancelled,
        )
        report({
            "stage": "complete",
            "message": "Strategy group saved successfully.",
            "result_url": f"/groups/{group['id']}",
        })
    except asyncio.CancelledError:
        app.state.job_store.append(
            job_id, {"stage": "cancelled", "message": "Generation cancelled by user."}
        )
        raise
    except Exception as error:
        message = _error_message(error)
        app.state.store.log_system(
            "group_generation_failed",
            {
                "job_id": job_id,
                "error": message,
                "error_type": error.__class__.__name__,
                "traceback": traceback.format_exc(limit=8),
            },
        )
        report({"stage": "error", "message": f"Group generation failed: {message}"})


async def _run_live_monitor_analysis_job(
    app,
    job_id: str,
    user_id: str,
    group_name: str,
    asset_symbol: str,
    timeframe: str,
    include_partial_contracts: bool,
    strategy_count: int,
    starting_balance: float,
    start_ts: float | None,
    end_ts: float | None,
    strategy_mode: str,
    phase_contract_counts: tuple[int, int, int] | None = None,
    strategy_families: tuple[str, ...] | None = None,
) -> None:
    def ensure_not_cancelled() -> None:
        current = app.state.job_store.get(job_id)
        if current and current.get("status") == "cancelled":
            raise asyncio.CancelledError

    def report(payload: dict) -> dict:
        ensure_not_cancelled()
        return app.state.job_store.append(job_id, payload)

    try:
        report({
            "stage": "validation",
            "message": "Loading saved live-monitor contracts...",
        })
        generator = LiveMonitorStrategyGenerator(app.state.store, app.state.live_monitor)
        summary = await asyncio.to_thread(
            generator.saved_contract_summary,
            user_id,
            asset_symbol,
            timeframe,
            include_partial_contracts,
            start_ts,
            end_ts,
        )
        ensure_not_cancelled()
        report({
            "stage": "backtest",
            "message": (
                "Starting live-monitor strategy backtests "
                f"(strategy 0 of {strategy_count}) on "
                f"{summary['selected_contracts']} contract(s)..."
            ),
            "current": 0,
            "total": strategy_count,
            "event_count": summary["selected_contracts"],
        })
        group = await asyncio.to_thread(
            generator.generate,
            user_id,
            group_name,
            asset_symbol,
            timeframe,
            include_partial_contracts,
            strategy_count,
            starting_balance,
            start_ts,
            end_ts,
            strategy_mode,
            report,
            ensure_not_cancelled,
            phase_contract_counts,
            strategy_families,
        )
        await asyncio.to_thread(
            _record_smart_family_report_snapshot,
            app.state.store,
            user_id,
            group,
        )
        report({
            "stage": "complete",
            "message": "Live-monitor strategy group saved successfully.",
            "result_url": f"/groups/{group['id']}",
        })
    except asyncio.CancelledError:
        app.state.job_store.append(
            job_id,
            {
                "stage": "cancelled",
                "message": "Live-monitor analysis generation cancelled by user.",
            },
        )
        raise
    except Exception as error:
        message = _error_message(error)
        app.state.store.log_system(
            "live_monitor_analysis_generation_failed",
            {
                "job_id": job_id,
                "error": message,
                "error_type": error.__class__.__name__,
                "traceback": traceback.format_exc(limit=8),
            },
        )
        report({
            "stage": "error",
            "message": f"Live-monitor analysis generation failed: {message}",
        })


@router.get("/groups/{group_id}", response_class=HTMLResponse)
def group_detail(
    request: Request,
    group_id: str,
    min_roi: str = "",
    min_win_rate: str = "",
    max_drawdown: str = "",
    min_trades: str = "",
    min_stability: str = "",
    family: str = "",
    sort: str = Query("roi"),
    direction: str = Query("desc"),
    strategy_page: int = Query(1, ge=1),
):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    if not group:
        raise HTTPException(404, "Strategy group not found")
    _ensure_contract_summary(group)
    strategies = store.list_user_records_filtered_limited(
        user["id"], "strategies", {"group_id": group_id},
        int(group.get("strategy_count", 0) or 0),
    )
    for item in strategies:
        _ensure_stability(item)
        _ensure_contract_summary(group, item)
    all_group_strategies = list(strategies)
    family_summary = (
        group.get("phase_validation", {}).get("family_rankings")
        or _family_profit_summary(all_group_strategies)
    )
    numeric_filters = {
        "min_roi": _optional_float(min_roi, "min_roi"),
        "min_win_rate": _optional_float(min_win_rate, "min_win_rate"),
        "max_drawdown": _optional_float(max_drawdown, "max_drawdown"),
        "min_trades": _optional_int(min_trades, "min_trades"),
        "min_stability": _optional_float(min_stability, "min_stability"),
    }
    if numeric_filters["min_roi"] is not None:
        strategies = [item for item in strategies if item["metrics"]["roi"] >= numeric_filters["min_roi"]]
    if numeric_filters["min_win_rate"] is not None:
        strategies = [item for item in strategies if item["metrics"]["win_rate"] >= numeric_filters["min_win_rate"]]
    if numeric_filters["max_drawdown"] is not None:
        strategies = [item for item in strategies if item["metrics"]["max_drawdown"] <= numeric_filters["max_drawdown"]]
    if numeric_filters["min_trades"] is not None:
        strategies = [item for item in strategies if item["metrics"]["trade_count"] >= numeric_filters["min_trades"]]
    if numeric_filters["min_stability"] is not None:
        strategies = [item for item in strategies if item["metrics"]["stability_score"] >= numeric_filters["min_stability"]]
    if family:
        strategies = [item for item in strategies if item.get("family") == family]
    sort_keys = {
        "roi": lambda item: item["metrics"]["roi"],
        "win_rate": lambda item: item["metrics"]["win_rate"],
        "drawdown": lambda item: item["metrics"]["max_drawdown"],
        "sharpe": lambda item: item["metrics"]["sharpe_ratio"],
        "profit_factor": lambda item: item["metrics"]["profit_factor"],
        "trades": lambda item: item["metrics"]["trade_count"],
        "stability": lambda item: item["metrics"]["stability_score"],
    }
    strategies.sort(key=sort_keys.get(sort, sort_keys["roi"]), reverse=direction != "asc")
    filtered_count = len(strategies)
    page_size = 50
    strategy_page_count = max(1, (filtered_count + page_size - 1) // page_size)
    strategy_page = min(strategy_page, strategy_page_count)
    start = (strategy_page - 1) * page_size
    strategies_page = strategies[start:start + page_size]

    def page_query(page: int) -> str:
        params = dict(request.query_params)
        params["strategy_page"] = str(page)
        return urlencode(params)

    return templates.TemplateResponse(
        request,
        "group_detail.html",
        _context(
            request, group=group, rows=strategies_page, filtered_count=filtered_count,
            families=sorted({item.get("family", "") for item in strategies}),
            min_roi=min_roi, min_win_rate=min_win_rate, max_drawdown=max_drawdown,
            min_trades=min_trades, min_stability=min_stability,
            family=family, sort=sort, direction=direction,
            strategy_page=strategy_page, strategy_page_count=strategy_page_count,
            strategy_prev_query=page_query(strategy_page - 1) if strategy_page > 1 else "",
            strategy_next_query=(
                page_query(strategy_page + 1)
                if strategy_page < strategy_page_count else ""
            ),
            family_summary=family_summary,
        ),
    )


@router.post("/groups/{group_id}/target-anomalies/delete")
def delete_group_target_anomalies(request: Request, group_id: str):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    if not group:
        raise HTTPException(404, "Strategy group not found")
    summary = dict(group.get("saved_contract_summary") or {})
    anomalies = summary.get("target_price_anomalies")
    if not isinstance(anomalies, list):
        anomalies = []
    record_ids = [
        str(item.get("id") or "").strip()
        for item in anomalies
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    deleted = 0
    for record_id in dict.fromkeys(record_ids):
        if request.app.state.live_monitor.delete_saved(user["id"], record_id):
            deleted += 1
    summary["target_price_anomalies"] = []
    summary["target_price_anomaly_contracts"] = 0
    try:
        store.update_user_record(
            user["id"],
            "groups",
            group_id,
            {"saved_contract_summary": summary},
        )
    except ValueError:
        pass
    request.session["message"] = f"Deleted {deleted} target-price anomaly contract(s)."
    return RedirectResponse(f"/groups/{group_id}", status_code=303)


@router.get("/groups/{group_id}/strategies/{strategy_id}", response_class=HTMLResponse)
def strategy_detail(
    request: Request,
    group_id: str,
    strategy_id: str,
    trade_page: int = Query(1, ge=1),
    show_trades: bool = Query(False),
    show_top_trades: bool = Query(False),
    top_trade_filter: str = Query("best"),
):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    strategy = store.get_user_record(user["id"], "strategies", strategy_id)
    if not group or not strategy or strategy.get("group_id") != group_id:
        raise HTTPException(404, "Strategy not found in this group")
    _ensure_stability(strategy)
    _ensure_contract_summary(group, strategy)
    known_trade_summary = _trade_summary_from_metrics(strategy["metrics"])
    large_transaction_log = store.user_collection_size(user["id"], "transactions") > 100_000_000
    live_monitor_fast_preview = group.get("source") == "live_monitor_saved_contracts"
    defer_transactions = (large_transaction_log or live_monitor_fast_preview) and not show_trades
    reconstructed_transactions = bool(
        strategy.get("transactions_summary_only") or group.get("transactions_summary_only")
    )
    reconstructed_top_source = None
    if reconstructed_transactions:
        all_transactions = _reconstructed_strategy_transactions(store, group, strategy)
        all_transactions.reverse()
        reconstructed_top_source = all_transactions
        transaction_count = len(all_transactions)
        trade_page_count = max(1, (transaction_count + 19) // 20)
        trade_page = min(trade_page, trade_page_count)
        start_index = (trade_page - 1) * 20
        transactions = all_transactions[start_index:start_index + 20]
        trade_summary = known_trade_summary
        defer_transactions = False
    elif defer_transactions:
        transactions = store.list_recent_user_records_preview(
            user["id"], "transactions",
            {"group_id": group_id, "strategy_id": strategy_id},
            limit=20,
        )
        transaction_count, trade_page, trade_summary = (
            int(strategy["metrics"].get("trade_count", 0)), 1, known_trade_summary
        )
    else:
        transactions, transaction_count, trade_page, trade_summary = store.list_user_records_page(
            user["id"], "transactions",
            {"group_id": group_id, "strategy_id": strategy_id},
            page=trade_page, page_size=20,
            known_total=int(strategy["metrics"].get("trade_count", 0)),
            known_summary=known_trade_summary,
        )
    top_trade_filter = str(top_trade_filter or "best").lower()
    if top_trade_filter not in {"best", "wins", "losses", "worst"}:
        top_trade_filter = "best"
    top_trades = []
    can_scan_top_trades = not (
        reconstructed_top_source is None
        and store.user_collection_size(user["id"], "transactions") > 250_000_000
    )
    if show_top_trades and can_scan_top_trades:
        largest = top_trade_filter not in {"losses", "worst"}
        if reconstructed_top_source is not None:
            top_trades = sorted(
                reconstructed_top_source,
                key=lambda trade: float(trade.get("pnl", 0) or 0),
                reverse=largest,
            )[:50]
        else:
            top_trades = store.top_user_records_by_numeric(
                user["id"],
                "transactions",
                {"group_id": group_id, "strategy_id": strategy_id},
                "pnl",
                limit=50,
                largest=largest,
            )
        if top_trade_filter == "wins":
            top_trades = [trade for trade in top_trades if float(trade.get("pnl", 0) or 0) > 0]
        elif top_trade_filter in {"losses", "worst"}:
            top_trades = [trade for trade in top_trades if float(trade.get("pnl", 0) or 0) < 0]
        top_trades = top_trades[:20]
        if group.get("source") == "live_monitor_saved_contracts":
            _normalize_live_bid_exit_outcomes(request.app.state.live_monitor, user["id"], top_trades)
    if group.get("source") == "live_monitor_saved_contracts":
        _normalize_live_bid_exit_outcomes(request.app.state.live_monitor, user["id"], transactions)
    strategy["metrics"].update({
        "up_trade_count": trade_summary.get("up", strategy["metrics"].get("up_trade_count", 0)),
        "down_trade_count": trade_summary.get("down", strategy["metrics"].get("down_trade_count", 0)),
        "winning_trade_count": trade_summary.get(
            "wins", strategy["metrics"].get("winning_trade_count", 0)
        ),
        "losing_trade_count": trade_summary.get(
            "losses", strategy["metrics"].get("losing_trade_count", 0)
        ),
        "flat_trade_count": trade_summary.get(
            "flat", strategy["metrics"].get("flat_trade_count", 0)
        ),
    })
    trade_page_count = max(1, (transaction_count + 19) // 20)
    try:
        period_days = (
            date.fromisoformat(group["period"]["end_date"])
            - date.fromisoformat(group["period"]["start_date"])
        ).days + 1
    except (KeyError, TypeError, ValueError):
        period_days = None
    live_retest_summary = (
        _live_retest_summary_from_group(group)
        if group.get("source") == "live_monitor_saved_contracts"
        else None
    )
    return templates.TemplateResponse(
        request,
        "file_strategy_detail.html",
        _context(
            request, group=group, strategy=strategy, transactions=transactions,
            transaction_count=transaction_count, trade_page=trade_page,
            trade_page_count=trade_page_count, trade_summary=trade_summary,
            defer_transactions=defer_transactions, show_trades=show_trades,
            reconstructed_transactions=reconstructed_transactions,
            top_trades=top_trades, show_top_trades=show_top_trades,
            top_trade_filter=top_trade_filter, can_scan_top_trades=can_scan_top_trades,
            test_period_days=period_days,
            chart_equity_curve=_sample_series(strategy.get("equity_curve", [])),
            live_retest_summary=live_retest_summary,
        ),
    )


@router.get("/api/groups/{group_id}/strategies/{strategy_id}/live-retest/summary")
def live_retest_summary(
    request: Request,
    group_id: str,
    strategy_id: str,
):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    strategy = store.get_user_record(user["id"], "strategies", strategy_id)
    if not group or not strategy or strategy.get("group_id") != group_id:
        raise HTTPException(404, "Strategy not found")
    if group.get("source") != "live_monitor_saved_contracts":
        raise HTTPException(404, "Live retest is available only for live-monitor groups")
    try:
        summary = LiveMonitorStrategyGenerator(
            store,
            request.app.state.live_monitor,
        ).saved_contract_summary(
            user["id"],
            group["assets"][0],
            group["timeframe"],
            bool(group.get("include_partial_contracts", False)),
            include_contracts=True,
        )
    except (IndexError, KeyError, ValueError) as error:
        raise HTTPException(422, _error_message(error)) from error
    return {"summary": summary}


@router.get("/api/groups/{group_id}/strategies/{strategy_id}/trades/{transaction_id}/contract-chart")
def strategy_trade_contract_chart(
    request: Request,
    group_id: str,
    strategy_id: str,
    transaction_id: str,
):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    strategy = store.get_user_record(user["id"], "strategies", strategy_id)
    transaction = store.find_user_record_reverse(
        user["id"],
        "transactions",
        {"id": transaction_id, "group_id": group_id, "strategy_id": strategy_id},
    )
    if (
        not group
        or not strategy
        or not transaction
        or strategy.get("group_id") != group_id
        or transaction.get("group_id") != group_id
        or transaction.get("strategy_id") != strategy_id
        or group.get("source") not in {"live_monitor_saved_contracts", "paper_trading_live"}
    ):
        raise HTTPException(404, "Trade contract chart not found")
    if group.get("source") == "paper_trading_live":
        paper_trade_id = str(transaction.get("paper_trade_id") or "").strip()
        paper_record = (
            store.get_user_record(user["id"], "paper_trades", paper_trade_id)
            if paper_trade_id
            else None
        )
        saved_record_id = str(
            transaction.get("saved_record_id")
            or (paper_record or {}).get("saved_record_id")
            or ""
        ).strip()
        record = (
            request.app.state.live_monitor.get_saved(user["id"], saved_record_id)
            if saved_record_id
            else None
        )
        if not record and paper_record:
            record = dict(paper_record.get("contract_record") or {})
    else:
        saved_record_id = str(transaction.get("saved_record_id") or "").strip()
        if not saved_record_id:
            raise HTTPException(404, "This trade is not linked to a saved live contract")
        record = request.app.state.live_monitor.get_saved(user["id"], saved_record_id)
    if not record:
        raise HTTPException(404, "Saved live contract not found")
    resolved_outcome = transaction.get("resolved_outcome")
    if str(resolved_outcome or "").upper() == "BID_EXIT":
        winning = str(record.get("winning_outcome") or "").strip().upper()
        if winning in {"UP", "DOWN", "FLAT"}:
            resolved_outcome = winning
        else:
            resolved_outcome = None
    history = _sample_series(record.get("history") or [], limit=1600)
    side = str(transaction.get("side") or "").upper()
    entry_price = _number_or_none(transaction.get("ask_price"))
    exit_price = _number_or_none(transaction.get("exit_price"))
    entry_price_cents = round(entry_price * 100, 4) if entry_price is not None else None
    exit_price_cents = round(exit_price * 100, 4) if exit_price is not None else None
    entry_yes_axis_cents = (
        round(100 - entry_price_cents, 4)
        if side == "DOWN" and entry_price_cents is not None
        else entry_price_cents
    )
    exit_yes_axis_cents = (
        round(100 - exit_price_cents, 4)
        if side == "DOWN" and exit_price_cents is not None
        else exit_price_cents
    )
    return {
        "record": {
            "id": record.get("id"),
            "slug": record.get("slug"),
            "question": record.get("question"),
            "event_title": record.get("event_title"),
            "crypto_symbol": record.get("crypto_symbol"),
            "target_price": record.get("target_price"),
            "final_crypto_price": record.get("final_crypto_price"),
            "winning_outcome": record.get("winning_outcome"),
            "window_start_ts": record.get("window_start_ts"),
            "window_end_ts": record.get("window_end_ts"),
            "history": history,
            "summary": record.get("summary") or {},
        },
        "trade": {
            "id": transaction.get("id"),
            "side": side,
            "opened_at": transaction.get("opened_at"),
            "closed_at": transaction.get("closed_at"),
            "entry_price": entry_price,
            "entry_price_cents": entry_price_cents,
            "entry_yes_axis_cents": entry_yes_axis_cents,
            "exit_price": exit_price,
            "exit_price_cents": exit_price_cents,
            "exit_yes_axis_cents": exit_yes_axis_cents,
            "exit_type": transaction.get("exit_type"),
            "exit_bid_price": transaction.get("exit_bid_price"),
            "entry_probability": transaction.get("entry_probability"),
            "entry_edge_cents": transaction.get("entry_edge_cents"),
            "exit_probability": transaction.get("exit_probability"),
            "exit_edge_cents": transaction.get("exit_edge_cents"),
            "pnl": transaction.get("pnl"),
            "resolved_outcome": resolved_outcome,
        },
    }


@router.post("/groups/{group_id}/strategies/{strategy_id}/favorite")
def toggle_strategy_favorite(request: Request, group_id: str, strategy_id: str):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    strategy = store.get_user_record(user["id"], "strategies", strategy_id)
    if not group or not strategy or strategy.get("group_id") != group_id:
        raise HTTPException(404, "Strategy not found in this group")
    favorite = store.set_strategy_favorite(
        user["id"], strategy_id, not bool(strategy.get("favorite", False))
    )
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse({
            "favorite": favorite,
            "label": "Favorited" if favorite else "Add to favorites",
        })
    return RedirectResponse(
        f"/groups/{group_id}/strategies/{strategy_id}", status_code=303
    )


@router.post("/groups/{group_id}/strategies/{strategy_id}/favorite-comment")
def update_strategy_favorite_comment(
    request: Request,
    group_id: str,
    strategy_id: str,
    comment: Annotated[str, Form()] = "",
):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    strategy = store.get_user_record(user["id"], "strategies", strategy_id)
    if not group or not strategy or strategy.get("group_id") != group_id:
        raise HTTPException(404, "Strategy not found in this group")
    saved = store.set_strategy_favorite_comment(user["id"], strategy_id, comment)
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse({"comment": saved})
    request.session["message"] = "Favorite note saved."
    return RedirectResponse(
        f"/groups/{group_id}/strategies/{strategy_id}", status_code=303
    )


@router.post("/groups/{group_id}/strategies/{strategy_id}/retest")
async def retest_strategy(
    request: Request,
    group_id: str,
    strategy_id: str,
    start_date: Annotated[str, Form()],
    end_date: Annotated[str, Form()],
):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    strategy = store.get_user_record(user["id"], "strategies", strategy_id)
    if not group or not strategy or strategy.get("group_id") != group_id:
        raise HTTPException(404, "Strategy not found in this group")
    job = request.app.state.job_store.create(user["id"])
    task = asyncio.create_task(
        _run_retest_job(
            request.app, job["id"], user["id"], group_id, strategy_id,
            start_date, end_date,
        )
    )
    _track_job_task(request.app, job["id"], task)
    return RedirectResponse(f"/retest-jobs/{job['id']}", status_code=303)


@router.post("/groups/{group_id}/strategies/{strategy_id}/live-retest")
def live_retest_strategy(
    request: Request,
    group_id: str,
    strategy_id: str,
    start_ts: Annotated[float, Form()],
    end_ts: Annotated[float, Form()],
):
    user = _require_user(request)
    store = _store(request)
    group = store.get_user_record(user["id"], "groups", group_id)
    strategy = store.get_user_record(user["id"], "strategies", strategy_id)
    if (
        not group
        or not strategy
        or strategy.get("group_id") != group_id
        or group.get("source") != "live_monitor_saved_contracts"
    ):
        raise HTTPException(404, "Live monitor strategy not found")
    try:
        result_group, _result_strategy = LiveMonitorStrategyGenerator(
            store, request.app.state.live_monitor
        ).retest(user["id"], group, strategy, start_ts, end_ts)
        request.session["message"] = (
            f"Retested live-monitor strategy on {result_group['event_count']} saved contract(s)."
        )
        return RedirectResponse(f"/groups/{result_group['id']}", status_code=303)
    except (ValueError, OSError) as error:
        request.session["error"] = str(error)
        return RedirectResponse(f"/groups/{group_id}/strategies/{strategy_id}", status_code=303)


@router.get("/retest-jobs/{job_id}", response_class=HTMLResponse)
def retest_job_page(request: Request, job_id: str):
    user = _require_user(request)
    try:
        job = request.app.state.job_store.get(job_id)
    except ValueError as error:
        raise HTTPException(404, "Retest job not found") from error
    if not job or job.get("user_id") != user["id"]:
        raise HTTPException(404, "Retest job not found")
    return templates.TemplateResponse(
        request, "retest_progress.html", _context(request, job=job)
    )


async def _run_retest_job(
    app,
    job_id: str,
    user_id: str,
    group_id: str,
    strategy_id: str,
    start_date: str,
    end_date: str,
) -> None:
    def ensure_not_cancelled() -> None:
        current = app.state.job_store.get(job_id)
        if current and current.get("status") == "cancelled":
            raise asyncio.CancelledError

    def report(payload: dict) -> dict:
        ensure_not_cancelled()
        return app.state.job_store.append(job_id, payload)

    try:
        group = app.state.store.get_user_record(user_id, "groups", group_id)
        strategy = app.state.store.get_user_record(user_id, "strategies", strategy_id)
        if not group or not strategy or strategy.get("group_id") != group_id:
            raise ValueError("Source strategy no longer exists")
        try:
            await app.state.collector.collect(
                group["assets"], group["timeframe"], start_date, end_date,
                progress=report,
            )
        except Exception as error:
            start_time, end_time = parse_period(start_date, end_date)
            cached_events = app.state.store.list_fixed_events(
                group["assets"], group["timeframe"], start_time, end_time
            )
            if not cached_events:
                raise
            report({
                "stage": "validation",
                "message": (
                    "Remote data refresh failed, using the locally cached contracts "
                    f"already available for this period ({len(cached_events)} events). "
                    f"Original error: {_error_message(error)}"
                ),
                "saved_events": len(cached_events),
            })
        ensure_not_cancelled()
        report({"stage": "backtest", "message": "Running the selected strategy..."})
        result_group, result_strategy = await asyncio.to_thread(
            StrategyGroupGenerator(app.state.store).retest,
            user_id, group, strategy, start_date, end_date, ensure_not_cancelled,
        )
        report({
            "stage": "complete",
            "message": "Retest completed successfully.",
            "result_url": (
                f"/groups/{result_group['id']}/strategies/{result_strategy['id']}"
            ),
        })
    except asyncio.CancelledError:
        app.state.job_store.append(
            job_id, {"stage": "cancelled", "message": "Retest cancelled by user."}
        )
        raise
    except Exception as error:
        message = _error_message(error)
        app.state.store.log_system(
            "strategy_retest_failed",
            {
                "job_id": job_id,
                "error": message,
                "error_type": error.__class__.__name__,
                "traceback": traceback.format_exc(limit=8),
            },
        )
        report({"stage": "error", "message": f"Retest failed: {message}"})


@router.get("/saved-strategies", response_class=HTMLResponse)
def favorite_strategies(request: Request):
    user = _require_user(request)
    store = _store(request)
    groups = {
        item["id"]: item for item in store.list_user_records(user["id"], "groups")
    }
    favorite_ids = {
        strategy_id
        for strategy_id, override in store.strategy_favorite_overrides(user["id"]).items()
        if override.get("favorite")
    }
    favorite_strategies = store.list_user_records_by_ids(user["id"], "strategies", favorite_ids)
    rows = [
        {**strategy, "group": groups.get(strategy.get("group_id"))}
        for strategy in favorite_strategies
        if strategy.get("favorite") and strategy.get("group_id") in groups
    ]
    rows.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return templates.TemplateResponse(
        request, "favorites.html", _context(request, rows=rows)
    )


@router.post("/groups/{group_id}/delete")
def delete_group(request: Request, group_id: str):
    user = _require_user(request)
    if not _store(request).delete_group(user["id"], group_id):
        raise HTTPException(404, "Strategy group not found")
    request.session["message"] = "The strategy group and all of its data were permanently deleted."
    return RedirectResponse("/", status_code=303)


@router.post("/groups/clear")
def clear_groups(request: Request):
    user = _require_user(request)
    _store(request).clear_groups(user["id"])
    request.session["message"] = "All strategy groups were permanently deleted."
    return RedirectResponse("/", status_code=303)


@router.get("/admin", response_class=HTMLResponse)
def admin_panel(request: Request):
    _require_admin(request)
    store = _store(request)
    users = store.user_summaries()
    now = datetime.now(timezone.utc)
    data_bytes = sum(
        path.stat().st_size for path in store.root.rglob("*")
        if path.is_file() and not path.name.endswith(".lock")
    )
    metrics = {
        "uptime_seconds": int((now - request.app.state.started_at).total_seconds()),
        "active_sessions": request.app.state.session_store.count_active(),
        "data_bytes": data_bytes,
        "environment": settings.environment,
    }
    return templates.TemplateResponse(
        request,
        "admin.html",
        _context(
            request, users=users, user_count=len(users),
            group_count=sum(user["group_count"] for user in users),
            strategy_count=sum(user["strategy_count"] for user in users),
            activity_count=sum(user["activity_count"] for user in users),
            event_count=store.count_fixed_events(),
            metrics=metrics,
            logs=store.admin_logs(),
            bootstrap_username=settings.username.strip().lower(),
        ),
    )


@router.post("/admin/users/{user_id}")
def admin_update_user(
    request: Request,
    user_id: str,
    display_name: Annotated[str, Form()],
    balance: Annotated[float, Form()],
):
    _require_admin(request)
    try:
        _store(request).update_user_profile(user_id, display_name, balance)
    except ValueError as error:
        request.session["error"] = str(error)
    else:
        request.session["message"] = "User profile updated."
    return RedirectResponse("/admin", status_code=303)


@router.post("/admin/users/{user_id}/delete")
def admin_delete_user(request: Request, user_id: str):
    _require_admin(request)
    user = _store(request).get_user(user_id)
    if not user:
        raise HTTPException(404, "User not found")
    if user.get("username") == settings.username.strip().lower():
        request.session["error"] = "The environment-managed test user cannot be deleted."
        return RedirectResponse("/admin", status_code=303)
    request.app.state.session_store.revoke_user(user_id)
    _store(request).delete_user(user_id)
    request.session["message"] = "User and all associated data were permanently deleted."
    return RedirectResponse("/admin", status_code=303)


class _QueueWriter:
    def __init__(self, output: queue.Queue):
        self.output = output

    def write(self, data: bytes) -> int:
        if data:
            self.output.put(bytes(data))
        return len(data)

    def flush(self) -> None:
        return None


def _valid_export_token(request: Request) -> bool:
    expected = settings.export_token.strip()
    if not expected:
        return False
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    return scheme.lower() == "bearer" and hmac.compare_digest(token.strip(), expected)


def _data_archive_chunks(store: FileStore):
    output: queue.Queue = queue.Queue(maxsize=8)
    finished = object()

    def archive_worker() -> None:
        try:
            root = store.root
            writer = _QueueWriter(output)
            with tarfile.open(fileobj=writer, mode="w|gz") as archive:
                for path in root.rglob("*"):
                    if path.name.endswith(".lock"):
                        continue
                    archive.add(
                        path,
                        arcname=f"server_data/{path.relative_to(root).as_posix()}",
                        recursive=False,
                    )
        except BaseException as error:
            output.put(error)
        finally:
            output.put(finished)

    threading.Thread(target=archive_worker, daemon=True).start()
    while True:
        item = output.get()
        if item is finished:
            break
        if isinstance(item, BaseException):
            raise item
        yield item


@router.get("/api/admin/export-data")
def export_data(request: Request):
    if not (_is_admin(request) or _valid_export_token(request)):
        raise HTTPException(403, "Administrator access required")
    return StreamingResponse(
        _data_archive_chunks(_store(request)),
        media_type="application/gzip",
        headers={
            "Content-Disposition": "attachment; filename=polymarket-server-data.tar.gz",
            "Cache-Control": "no-store",
        },
    )


def _snapshot_base() -> Path:
    return Path(settings.data_snapshots_root).expanduser().resolve()


def _local_data_tools_config() -> dict:
    if not LOCAL_DATA_TOOLS_CONFIG.exists():
        return {}
    try:
        value = json.loads(LOCAL_DATA_TOOLS_CONFIG.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _save_local_data_tools_config(value: dict) -> None:
    temporary = LOCAL_DATA_TOOLS_CONFIG.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(LOCAL_DATA_TOOLS_CONFIG)


def _render_export_token() -> str:
    return settings.render_export_token.strip() or str(
        _local_data_tools_config().get("render_export_token", "")
    ).strip()


def _local_server_data_root() -> Path:
    return (ROOT_DIR / "server_data").resolve()


def _render_snapshot_root(snapshot_id: str) -> Path:
    safe_id = "".join(character for character in snapshot_id if character.isalnum() or character in "-_")
    if safe_id != snapshot_id or not snapshot_id:
        raise HTTPException(422, "Invalid snapshot identifier")
    return (_snapshot_base() / "render" / snapshot_id / "server_data").resolve()


def _latest_render_snapshot_id() -> str | None:
    marker = _snapshot_base() / "render" / "latest.txt"
    if not marker.exists():
        return None
    value = marker.read_text(encoding="utf-8").strip()
    return value or None


def _local_data_profiles(current_root: Path) -> list[dict]:
    profiles = []

    def add_profile(profile_id: str, label: str, path: Path, kind: str, latest: bool = False) -> None:
        resolved = path.resolve()
        profiles.append({
            "id": profile_id,
            "label": label,
            "path": str(resolved),
            "kind": kind,
            "exists": resolved.exists(),
            "current": resolved == current_root.resolve(),
            "latest": latest,
        })

    add_profile("local", "Local server_data", _local_server_data_root(), "local")
    render_root = _snapshot_base() / "render"
    latest_id = _latest_render_snapshot_id()
    if latest_id:
        add_profile(
            "render-latest",
            f"Render latest ({latest_id})",
            _render_snapshot_root(latest_id),
            "render",
            latest=True,
        )
    if render_root.exists():
        for directory in sorted(
            (path for path in render_root.iterdir() if path.is_dir()),
            key=lambda path: path.name,
            reverse=True,
        ):
            server_data = directory / "server_data"
            if not server_data.exists():
                continue
            add_profile(
                f"render:{directory.name}",
                f"Render snapshot {directory.name}",
                server_data,
                "render",
                latest=directory.name == latest_id,
            )
    return profiles


def _resolve_local_data_profile(profile_id: str) -> Path:
    if profile_id == "local":
        return _local_server_data_root()
    if profile_id == "render-latest":
        latest_id = _latest_render_snapshot_id()
        if not latest_id:
            raise HTTPException(404, "No Render snapshot has been downloaded yet")
        return _render_snapshot_root(latest_id)
    if profile_id.startswith("render:"):
        return _render_snapshot_root(profile_id.split(":", 1)[1])
    raise HTTPException(422, "Unknown data profile")


def _safe_extract_server_data_archive(archive_path: Path, target_root: Path) -> None:
    target_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, mode="r:gz") as archive:
        for member in archive.getmembers():
            name = member.name.replace("\\", "/")
            if member.issym() or member.islnk() or not name.startswith("server_data/"):
                continue
            destination = (target_root / name).resolve()
            if destination != target_root and target_root not in destination.parents:
                raise RuntimeError("Archive contains an unsafe path")
            archive.extract(member, path=target_root)


async def _replace_local_data_root(request: Request, data_root: Path) -> None:
    data_root = data_root.resolve()
    tasks = tuple(getattr(request.app.state, "background_tasks", set()))
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    old_live_monitor = getattr(request.app.state, "live_monitor", None)
    old_collector = getattr(request.app.state, "collector", None)
    if old_live_monitor:
        await old_live_monitor.close()
    if old_collector:
        await old_collector.close()

    store = FileStore(data_root)
    store.ensure_user(settings.username, hash_password(settings.password), "Test User")
    store.log_system("local_data_profile_selected", {"data_root": str(data_root)})
    request.app.state.store = store
    request.app.state.collector = DataCollector(store)
    request.app.state.session_store = FileSessionStore(
        store.root / "sessions", settings.session_max_age_seconds
    )
    request.app.state.job_store = GenerationJobStore(store.root / "jobs")
    request.app.state.live_monitor = LiveMonitorManager(store.root / "live_monitor")
    request.app.state.background_tasks = set()
    request.app.state.job_tasks = {}


@router.get("/api/local-data/profiles")
def local_data_profiles(request: Request):
    _require_local_data_tools(request)
    return {
        "current_root": str(_store(request).root),
        "download_configured": bool(_render_export_token()),
        "profiles": _local_data_profiles(_store(request).root),
    }


@router.post("/api/local-data/config")
def save_local_data_config(
    request: Request,
    render_export_token: Annotated[str, Form()] = "",
):
    _require_local_data_tools(request)
    config = _local_data_tools_config()
    token = render_export_token.strip()
    if token:
        config["render_export_token"] = token
    else:
        config.pop("render_export_token", None)
    _save_local_data_tools_config(config)
    return {"download_configured": bool(_render_export_token())}


@router.post("/api/local-data/download-render")
async def download_render_data(request: Request):
    _require_local_data_tools(request)
    token = _render_export_token()
    if not token:
        raise HTTPException(400, "Set RENDER_EXPORT_TOKEN in .env before downloading")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    render_root = _snapshot_base() / "render"
    archive_dir = render_root / "archives"
    target_root = render_root / timestamp
    archive_path = archive_dir / f"{timestamp}.tar.gz"
    temporary_path = archive_path.with_suffix(".tar.gz.tmp")
    archive_dir.mkdir(parents=True, exist_ok=True)

    try:
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            async with client.stream(
                "GET",
                settings.render_export_url,
                headers={"Authorization": f"Bearer {token}"},
            ) as response:
                if response.status_code != 200:
                    detail = (await response.aread()).decode("utf-8", errors="replace")[:300]
                    raise HTTPException(
                        response.status_code,
                        detail or "Render data export failed",
                    )
                with temporary_path.open("wb") as handle:
                    async for chunk in response.aiter_bytes():
                        handle.write(chunk)
        temporary_path.replace(archive_path)
        if target_root.exists():
            shutil.rmtree(target_root)
        await asyncio.to_thread(_safe_extract_server_data_archive, archive_path, target_root)
        (render_root / "latest.txt").write_text(timestamp, encoding="utf-8")
    except HTTPException:
        temporary_path.unlink(missing_ok=True)
        raise
    except Exception as error:
        temporary_path.unlink(missing_ok=True)
        if target_root.exists():
            shutil.rmtree(target_root)
        raise HTTPException(500, f"Render data download failed: {error}") from error

    return {
        "snapshot_id": timestamp,
        "archive_path": str(archive_path.resolve()),
        "data_root": str((target_root / "server_data").resolve()),
        "profiles": _local_data_profiles(_store(request).root),
    }


@router.post("/api/local-data/use")
async def use_local_data_profile(request: Request, profile_id: Annotated[str, Form()]):
    _require_local_data_tools(request)
    data_root = _resolve_local_data_profile(profile_id)
    if not data_root.exists():
        raise HTTPException(404, "The selected data profile does not exist")
    request.session.clear()
    await _replace_local_data_root(request, data_root)
    return {
        "current_root": str(data_root),
        "profiles": _local_data_profiles(_store(request).root),
        "reload_url": "/login",
    }


@router.get("/api/state")
def state(request: Request):
    user = _require_user(request)
    store = _store(request)
    return {
        "user": {key: user[key] for key in ("id", "email", "display_name")},
        "groups": store.count_user_records(user["id"], "groups"),
        "strategies": store.count_user_records(user["id"], "strategies"),
        "transactions": store.count_user_records(user["id"], "transactions"),
        "fixed_time_events": store.count_fixed_events(),
    }


def _optional_float(value: str, field: str) -> float | None:
    if not value.strip():
        return None
    try:
        result = float(value)
    except ValueError as error:
        raise HTTPException(422, f"{field} must be a number") from error
    if result != result or result in {float("inf"), float("-inf")}:
        raise HTTPException(422, f"{field} must be finite")
    return result


def _optional_int(value: str, field: str) -> int | None:
    if not value.strip():
        return None
    try:
        return int(value)
    except ValueError as error:
        raise HTTPException(422, f"{field} must be an integer") from error
