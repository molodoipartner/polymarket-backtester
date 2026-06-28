from __future__ import annotations

import asyncio
import hmac
import json
import queue
import tarfile
import threading
import traceback
from datetime import date, datetime, timedelta, timezone
from typing import Annotated
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from backend.auth import hash_password, verify_password
from backend.backtester import FileBacktester
from backend.fixed_time import ASSETS, TIMEFRAMES, parse_period, validate_selection
from backend.storage import FileStore
from backend.strategy_generator import StrategyGroupGenerator
from config.settings import ROOT_DIR, settings

router = APIRouter()
templates = Jinja2Templates(directory=ROOT_DIR / "frontend" / "templates")


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
        "user": _user(request),
        "is_admin": _is_admin(request),
        "message": request.session.pop("message", None),
        "error": request.session.pop("error", None),
        **values,
    }


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


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if _user(request):
        return RedirectResponse("/admin" if _is_admin(request) else "/", status_code=303)
    return templates.TemplateResponse(request, "auth.html", _context(request, mode="login"))


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


@router.post("/logout")
def logout(request: Request):
    if _is_admin(request):
        _store(request).log_system("admin_logout", {"username": settings.admin_username})
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


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
    return JSONResponse(
        {
            "session_id": session.id,
            "events_url": f"/api/live-monitors/{session.id}/events",
            "stop_url": f"/api/live-monitors/{session.id}/stop",
            "snapshot": session.snapshot(),
        },
        status_code=202,
    )


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
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield (
                    f"event: {message['event']}\n"
                    f"data: {json.dumps(message['data'], ensure_ascii=False)}\n\n"
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
        ),
    )


@router.get("/groups/{group_id}/strategies/{strategy_id}", response_class=HTMLResponse)
def strategy_detail(
    request: Request,
    group_id: str,
    strategy_id: str,
    trade_page: int = Query(1, ge=1),
    show_trades: bool = Query(False),
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
    defer_transactions = large_transaction_log and not show_trades
    reconstructed_transactions = bool(
        strategy.get("transactions_summary_only") or group.get("transactions_summary_only")
    )
    if reconstructed_transactions:
        all_transactions = _reconstructed_strategy_transactions(store, group, strategy)
        all_transactions.reverse()
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
    return templates.TemplateResponse(
        request,
        "file_strategy_detail.html",
        _context(
            request, group=group, strategy=strategy, transactions=transactions,
            transaction_count=transaction_count, trade_page=trade_page,
            trade_page_count=trade_page_count, trade_summary=trade_summary,
            defer_transactions=defer_transactions, show_trades=show_trades,
            reconstructed_transactions=reconstructed_transactions,
            test_period_days=period_days,
            chart_equity_curve=_sample_series(strategy.get("equity_curve", [])),
        ),
    )


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
