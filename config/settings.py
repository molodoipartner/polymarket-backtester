from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")


class Settings(BaseSettings):
    app_name: str = "Polymarket Backtester"
    environment: str = "development"
    debug: bool = False
    host: str = "127.0.0.1"
    port: int = 8000
    workers: int = 1
    data_root: Path = ROOT_DIR / "server_data"
    allowed_hosts: str = "127.0.0.1,localhost,testserver"
    session_secret: str = "change-this-local-session-secret"
    session_cookie_name: str = "poly_session"
    session_cookie_secure: bool = False
    session_max_age_seconds: int = 86_400
    export_token: str = ""
    username: str = "test"
    password: str = "TestUser123!"
    admin_username: str = "admin"
    admin_password: str = "Admin1234!"

    gamma_api_url: str = "https://gamma-api.polymarket.com"
    history_page_size: int = 500
    history_max_events_per_series: int = 5_000
    history_chunk_days: int = 3
    history_request_concurrency: int = 128
    market_cache_ttl_seconds: int = 900

    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    def validate_runtime(self) -> None:
        if len(self.session_secret) < 32:
            raise RuntimeError("SESSION_SECRET must contain at least 32 characters")
        for name, value in (("PASSWORD", self.password), ("ADMIN_PASSWORD", self.admin_password)):
            if len(value) < 10:
                raise RuntimeError(f"{name} must contain at least 10 characters")
        if self.environment.lower() == "production" and not self.session_cookie_secure:
            raise RuntimeError("SESSION_COOKIE_SECURE must be true in production")

    @property
    def allowed_host_list(self) -> list[str]:
        return [host.strip() for host in self.allowed_hosts.split(",") if host.strip()]

@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
