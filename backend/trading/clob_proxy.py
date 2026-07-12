from __future__ import annotations

import logging
import os
import re
from functools import lru_cache

import httpx

from config.settings import settings

logger = logging.getLogger(__name__)


def _configured_proxy_urls() -> list[str]:
    raw = os.getenv("POLYMARKET_CLOB_PROXY_URLS") or settings.polymarket_clob_proxy_urls
    return [part.strip() for part in re.split(r"[\s,;]+", str(raw or "")) if part.strip()]


@lru_cache(maxsize=1)
def clob_proxy_url() -> str | None:
    for url in _configured_proxy_urls():
        scheme = url.split(":", 1)[0].lower()
        if scheme in {"http", "https"}:
            return url
        if scheme in {"socks5", "socks5h"}:
            try:
                import socksio  # noqa: F401
            except ImportError:
                logger.warning("Skipping CLOB SOCKS proxy because socksio is not installed: %s", url)
                continue
            return url
        logger.warning("Skipping unsupported CLOB proxy scheme: %s", url)
    return None


def clob_async_client_kwargs(**kwargs) -> dict:
    proxy = clob_proxy_url()
    if proxy:
        kwargs["proxy"] = proxy
        kwargs["trust_env"] = False
    return kwargs


def clob_async_client(**kwargs) -> httpx.AsyncClient:
    return httpx.AsyncClient(**clob_async_client_kwargs(**kwargs))


def configure_py_clob_http_client() -> str | None:
    proxy = clob_proxy_url()
    if not proxy:
        return None
    try:
        from py_clob_client_v2.http_helpers import helpers
    except ImportError:
        return proxy
    if getattr(helpers, "_polymarket_backtester_proxy", None) == proxy:
        return proxy
    old_client = getattr(helpers, "_http_client", None)
    try:
        if old_client is not None:
            old_client.close()
    except Exception:
        logger.debug("Could not close previous py-clob HTTP client", exc_info=True)
    helpers._http_client = httpx.Client(http2=True, proxy=proxy)
    helpers._polymarket_backtester_proxy = proxy
    logger.info("Configured py-clob CLOB HTTP proxy: %s", proxy)
    return proxy
