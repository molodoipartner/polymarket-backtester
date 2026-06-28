from __future__ import annotations

import json
import os
import secrets
import threading
import time
import uuid
from pathlib import Path

from itsdangerous import BadSignature, SignatureExpired, TimestampSigner
from starlette.middleware.base import BaseHTTPMiddleware

from backend.storage.locking import ProcessFileLock


class FileSessionStore:
    def __init__(self, root: Path, max_age: int) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            self.root.chmod(0o700)
        self.max_age = max_age
        self._thread_lock = threading.RLock()
        self._lock = ProcessFileLock(self.root / ".sessions.lock")

    def load(self, session_id: str) -> dict:
        path = self._path(session_id)
        with self._guard():
            if not path.exists():
                return {}
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                path.unlink(missing_ok=True)
                return {}
            if float(payload.get("expires_at", 0)) <= time.time():
                path.unlink(missing_ok=True)
                return {}
            data = payload.get("data")
            return data if isinstance(data, dict) else {}

    def save(self, session_id: str, data: dict) -> None:
        path = self._path(session_id)
        payload = {"expires_at": time.time() + self.max_age, "data": data}
        temporary = path.with_suffix(f".{uuid.uuid4().hex}.tmp")
        with self._guard():
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
            )
            if os.name != "nt":
                temporary.chmod(0o600)
            temporary.replace(path)

    def delete(self, session_id: str) -> None:
        with self._guard():
            self._path(session_id).unlink(missing_ok=True)

    def count_active(self) -> int:
        now = time.time()
        count = 0
        with self._guard():
            for path in self.root.glob("*.json"):
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    if float(payload.get("expires_at", 0)) > now:
                        count += 1
                    else:
                        path.unlink(missing_ok=True)
                except (OSError, ValueError, json.JSONDecodeError):
                    path.unlink(missing_ok=True)
        return count

    def revoke_user(self, user_id: str) -> int:
        removed = 0
        with self._guard():
            for path in self.root.glob("*.json"):
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    if payload.get("data", {}).get("user_id") == user_id:
                        path.unlink(missing_ok=True)
                        removed += 1
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
        return removed

    def _path(self, session_id: str) -> Path:
        if len(session_id) != 64 or any(character not in "0123456789abcdef" for character in session_id):
            raise ValueError("Invalid session identifier")
        return self.root / f"{session_id}.json"

    def _guard(self):
        class Guard:
            def __init__(self, lock):
                self.lock = lock

            def __enter__(self):
                self.owner._thread_lock.acquire()
                self.lock.acquire()

            def __exit__(self, exc_type, exc_value, traceback):
                self.lock.release()
                self.owner._thread_lock.release()

        guard = Guard(self._lock)
        guard.owner = self
        return guard


class ServerSessionMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        secret_key: str,
        cookie_name: str,
        max_age: int,
        https_only: bool,
    ) -> None:
        super().__init__(app)
        self.signer = TimestampSigner(secret_key)
        self.cookie_name = cookie_name
        self.max_age = max_age
        self.https_only = https_only

    async def dispatch(self, request, call_next):
        store: FileSessionStore = request.app.state.session_store
        session_id = self._read_cookie(request.cookies.get(self.cookie_name))
        data = store.load(session_id) if session_id else {}
        request.scope["session"] = data
        request.state.rotate_session = False
        response = await call_next(request)

        if request.state.rotate_session and session_id:
            store.delete(session_id)
            session_id = None
        if request.session:
            session_id = session_id or secrets.token_hex(32)
            store.save(session_id, dict(request.session))
            signed = self.signer.sign(session_id.encode()).decode()
            response.set_cookie(
                self.cookie_name,
                signed,
                max_age=self.max_age,
                httponly=True,
                secure=self.https_only,
                samesite="lax",
                path="/",
            )
        else:
            if session_id:
                store.delete(session_id)
            response.delete_cookie(self.cookie_name, path="/", samesite="lax")
        return response

    def _read_cookie(self, value: str | None) -> str | None:
        if not value:
            return None
        try:
            return self.signer.unsign(value, max_age=self.max_age).decode()
        except (BadSignature, SignatureExpired, UnicodeDecodeError):
            return None
