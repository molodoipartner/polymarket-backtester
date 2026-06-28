from pathlib import Path

from backend.core.jobs import GenerationJobStore


def test_job_write_retries_a_temporarily_blocked_replace(tmp_path, monkeypatch):
    original_replace = Path.replace
    attempts = 0

    def temporarily_blocked(path, target):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(5, "Access is denied")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", temporarily_blocked)
    store = GenerationJobStore(tmp_path / "jobs")
    job = store.create("user-id")

    assert attempts == 3
    assert store.get(job["id"])["status"] == "running"
    assert list((tmp_path / "jobs").glob("*.tmp")) == []


def test_job_cancel_marks_terminal_state(tmp_path):
    store = GenerationJobStore(tmp_path / "jobs")
    job = store.create("user-id")

    event = store.cancel(job["id"], "Stopped")
    saved = store.get(job["id"])

    assert event["stage"] == "cancelled"
    assert saved["status"] == "cancelled"
    assert "cancelled" in store.TERMINAL
