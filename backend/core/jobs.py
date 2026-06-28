from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from backend.storage.locking import ProcessFileLock


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class GenerationJobStore:
    """Process-safe progress state shared by HTTP and SSE workers."""

    TERMINAL = {"completed", "failed", "cancelled"}

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._thread_lock = threading.RLock()
        self._lock = ProcessFileLock(self.root / ".jobs.lock")

    def create(self, user_id: str) -> dict:
        job = {
            "id": uuid.uuid4().hex,
            "user_id": user_id,
            "status": "running",
            "events": [],
            "created_at": _now(),
            "updated_at": _now(),
            "result_url": None,
        }
        self._write(job)
        return job

    def append(self, job_id: str, payload: dict) -> dict:
        with self._guard():
            job = self._read(job_id)
            event = {"sequence": len(job["events"]) + 1, "timestamp": _now(), **payload}
            job["events"].append(event)
            job["updated_at"] = _now()
            if payload.get("stage") == "complete":
                job["status"] = "completed"
                job["result_url"] = payload.get("result_url")
            elif payload.get("stage") == "error":
                job["status"] = "failed"
            elif payload.get("stage") == "cancelled":
                job["status"] = "cancelled"
            self._write_unlocked(job)
            return event

    def cancel(self, job_id: str, message: str = "Job cancelled by user.") -> dict:
        return self.append(job_id, {"stage": "cancelled", "message": message})

    def get(self, job_id: str) -> dict | None:
        try:
            with self._guard():
                return self._read(job_id)
        except FileNotFoundError:
            return None

    def _read(self, job_id: str) -> dict:
        return json.loads(self._path(job_id).read_text(encoding="utf-8"))

    def _write(self, job: dict) -> None:
        with self._guard():
            self._write_unlocked(job)

    def _write_unlocked(self, job: dict) -> None:
        path = self._path(job["id"])
        temporary = path.with_suffix(f".{uuid.uuid4().hex}.tmp")
        temporary.write_text(
            json.dumps(job, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
        )
        try:
            for attempt in range(5):
                try:
                    temporary.replace(path)
                    return
                except PermissionError:
                    if attempt == 4:
                        raise
                    time.sleep(0.05 * (2 ** attempt))
        except Exception:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _path(self, job_id: str) -> Path:
        if len(job_id) != 32 or any(character not in "0123456789abcdef" for character in job_id):
            raise ValueError("Invalid job identifier")
        return self.root / f"{job_id}.json"

    def _guard(self):
        class Guard:
            def __init__(self, lock):
                self.lock = lock
                self.owner = None

            def __enter__(self):
                self.owner._thread_lock.acquire()
                self.lock.acquire()

            def __exit__(self, exc_type, exc_value, traceback):
                self.lock.release()
                self.owner._thread_lock.release()

        guard = Guard(self._lock)
        guard.owner = self
        return guard
