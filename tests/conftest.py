from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from config.settings import settings


@pytest.fixture(autouse=True)
def allow_testclient_host(monkeypatch):
    hosts = settings.allowed_host_list
    if "testserver" not in hosts:
        hosts.append("testserver")
    monkeypatch.setattr(settings, "allowed_hosts", ",".join(hosts))
    monkeypatch.setattr(settings, "session_cookie_secure", False)
