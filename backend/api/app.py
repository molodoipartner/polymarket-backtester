from __future__ import annotations

import asyncio
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
    app.include_router(router)
    return app


app = create_app()
