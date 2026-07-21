from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from backend.auth import FileSessionStore, ServerSessionMiddleware, hash_password
from backend.api.routes import router
from backend.core.logging import configure_logging
from backend.core.jobs import GenerationJobStore
from backend.data import DataCollector
from backend.live_monitor import LiveMonitorManager
from backend.storage import FileStore
from config.settings import ROOT_DIR, settings


logger = logging.getLogger(__name__)
RUNTIME_WATCHDOG_INTERVAL_SECONDS = 10.0
RUNTIME_LAG_WARNING_MS = 500.0


async def _runtime_watchdog(app: FastAPI) -> None:
    next_tick = time.monotonic() + RUNTIME_WATCHDOG_INTERVAL_SECONDS
    report_count = 0
    while True:
        await asyncio.sleep(max(0.0, next_tick - time.monotonic()))
        now = time.monotonic()
        lag_ms = max(0.0, (now - next_tick) * 1000.0)
        next_tick = now + RUNTIME_WATCHDOG_INTERVAL_SECONDS
        stats = app.state.live_monitor.runtime_stats()
        health = {
            "event_loop_lag_ms": round(lag_ms, 1),
            "background_tasks": len(app.state.background_tasks),
            **stats,
        }
        app.state.runtime_health = health
        report_count += 1
        if lag_ms >= RUNTIME_LAG_WARNING_MS:
            logger.warning("Runtime event loop is delayed: %s", health)
        elif report_count % 6 == 0:
            logger.info("Runtime health: %s", health)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.validate_runtime()
    configure_logging(settings.debug)
    store = FileStore(settings.data_root)
    collector = DataCollector(store)
    app.state.store = store
    app.state.collector = collector
    app.state.session_store = FileSessionStore(
        store.root / "sessions", settings.session_max_age_seconds
    )
    app.state.job_store = GenerationJobStore(store.root / "jobs")
    app.state.live_monitor = LiveMonitorManager(store.root / "live_monitor")
    app.state.background_tasks = set()
    app.state.job_tasks = {}
    app.state.started_at = datetime.now(timezone.utc)
    store.ensure_user(settings.username, hash_password(settings.password), "Test User")
    store.log_system("application_started", {"environment": settings.environment})
    watchdog = asyncio.create_task(_runtime_watchdog(app))
    app.state.background_tasks.add(watchdog)
    try:
        yield
    finally:
        tasks = tuple(app.state.background_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await app.state.live_monitor.close()
        await collector.close()


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name, version="1.0.0", lifespan=lifespan)
    app.add_middleware(
        ServerSessionMiddleware,
        secret_key=settings.session_secret,
        cookie_name=settings.session_cookie_name,
        max_age=settings.session_max_age_seconds,
        https_only=settings.session_cookie_secure,
    )
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.allowed_host_list)
    app.mount("/static", StaticFiles(directory=ROOT_DIR / "frontend" / "static"), name="static")

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, object]:
        runtime = dict(getattr(app.state, "runtime_health", {}))
        runtime.setdefault("event_loop_lag_ms", 0.0)
        runtime.setdefault("background_tasks", len(getattr(app.state, "background_tasks", ())))
        if hasattr(app.state, "live_monitor"):
            runtime.update(app.state.live_monitor.runtime_stats())
        return {"status": "ok", "runtime": runtime}

    app.include_router(router)
    return app


app = create_app()
