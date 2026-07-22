from __future__ import annotations

import json
import math
import shutil
import threading
import uuid
import heapq
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.fixed_time import parse_utc_datetime
from backend.storage.locking import ProcessFileLock
from config.settings import ROOT_DIR

DEFAULT_USER_BALANCE = 100.0


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FileStore:
    """Process-safe JSON/JSONL repository for application data."""

    COLLECTIONS = (
        "groups",
        "strategies",
        "transactions",
        "logs",
        "family_reports",
        "paper_trades",
        "real_orders",
        "real_trade_audit",
        "real_trade_console",
    )

    def __init__(self, root: Path | None = None) -> None:
        self.root = Path(root or ROOT_DIR / "server_data").resolve()
        self.users_dir = self.root / "users"
        self.market_dir = self.root / "market_data"
        self.events_path = self.market_dir / "fixed_time_events.jsonl"
        self.coverage_path = self.market_dir / "fixed_time_coverage.jsonl"
        self.system_logs_path = self.root / "system_logs.jsonl"
        self._thread_lock = threading.RLock()
        self._lock_state = threading.local()
        self._process_lock = ProcessFileLock(self.root / ".store.lock")
        self._jsonl_cache: dict[Path, dict[str, Any]] = {}
        for path in (self.users_dir, self.market_dir):
            path.mkdir(parents=True, exist_ok=True)
        self.system_logs_path.touch(exist_ok=True)
        self.coverage_path.touch(exist_ok=True)
        for profile_path in self.users_dir.glob("*/profile.json"):
            for collection in self.COLLECTIONS:
                (profile_path.parent / f"{collection}.jsonl").touch(exist_ok=True)
        self.prune_legacy_fixed_events()

    def create_user(
        self,
        email: str,
        display_name: str,
        password_hash: str,
        username: str | None = None,
    ) -> dict:
        normalized_email = email.strip().lower()
        normalized_username = (username or normalized_email).strip().lower()
        if not normalized_email or "@" not in normalized_email:
            raise ValueError("Enter a valid email address")
        if not normalized_username:
            raise ValueError("Username is required")
        with self._guard():
            if self.find_user_by_login(normalized_email) or self.find_user_by_login(normalized_username):
                raise ValueError("An account with this email or username already exists")
            user = {
                "id": uuid.uuid4().hex,
                "email": normalized_email,
                "username": normalized_username,
                "display_name": display_name.strip() or normalized_username,
                "balance": DEFAULT_USER_BALANCE,
                "password_hash": password_hash,
                "created_at": utc_iso(),
            }
            directory = self._user_dir(user["id"])
            directory.mkdir(parents=True, exist_ok=False)
            self._write_json(directory / "profile.json", user)
            for collection in self.COLLECTIONS:
                (directory / f"{collection}.jsonl").touch()
            self.log(user["id"], "account_created", {"username": normalized_username})
            return user

    def ensure_user(self, username: str, password_hash: str, display_name: str) -> dict:
        normalized = username.strip().lower()
        with self._guard():
            existing = self.find_user_by_login(normalized)
            if existing:
                updated = {
                    **existing,
                    "username": normalized,
                    "display_name": display_name,
                    "password_hash": password_hash,
                    "balance": float(existing.get("balance", DEFAULT_USER_BALANCE)),
                }
                self._write_json(self._user_dir(existing["id"]) / "profile.json", updated)
                return updated
            email = normalized if "@" in normalized else f"{normalized}@local.test"
            return self.create_user(email, display_name, password_hash, username=normalized)

    def list_users(self) -> list[dict]:
        with self._guard():
            users = [self._read_json(path, {}) for path in self.users_dir.glob("*/profile.json")]
            return sorted((user for user in users if user.get("id")), key=lambda item: item["created_at"])

    def user_summaries(self) -> list[dict]:
        summaries = []
        for user in self.list_users():
            groups = self.list_user_records(user["id"], "groups")
            group_ids = {group["id"] for group in groups}
            strategies = [
                item for item in self.list_user_records(user["id"], "strategies")
                if item.get("group_id") in group_ids
            ]
            logs = self.list_user_records(user["id"], "logs")
            latest = max(
                (item.get("timestamp") or item.get("created_at", "") for item in logs),
                default=user["created_at"],
            )
            summaries.append({
                **{key: user[key] for key in ("id", "email", "display_name", "created_at")},
                "username": user.get("username", user["email"]),
                "balance": float(user.get("balance", DEFAULT_USER_BALANCE)),
                "group_count": len(groups),
                "strategy_count": len(strategies),
                "transaction_count": sum(
                    int(strategy.get("metrics", {}).get("trade_count", 0))
                    for strategy in strategies
                ),
                "activity_count": len(logs),
                "last_activity": latest,
            })
        return sorted(summaries, key=lambda item: item["last_activity"], reverse=True)

    def find_user_by_login(self, login: str) -> dict | None:
        normalized = login.strip().lower()
        return next(
            (
                user for user in self.list_users()
                if user.get("email") == normalized or user.get("username") == normalized
            ),
            None,
        )

    def find_user_by_email(self, email: str) -> dict | None:
        return self.find_user_by_login(email)

    def get_user(self, user_id: str) -> dict | None:
        with self._guard():
            path = self._user_dir(user_id) / "profile.json"
            return self._read_json(path, None) if path.exists() else None

    def update_user_profile(
        self, user_id: str, display_name: str, balance: float
    ) -> dict:
        name = display_name.strip()
        if not 1 <= len(name) <= 80:
            raise ValueError("Display name must contain 1 to 80 characters")
        if not math.isfinite(balance) or balance < 0:
            raise ValueError("Balance must be a non-negative finite number")
        with self._guard():
            user = self.get_user(user_id)
            if not user:
                raise ValueError("User does not exist")
            updated = {**user, "display_name": name, "balance": round(balance, 2)}
            self._write_json(self._user_dir(user_id) / "profile.json", updated)
            self.log_system(
                "admin_user_updated",
                {"user_id": user_id, "display_name": name, "balance": updated["balance"]},
            )
            return updated

    def delete_user(self, user_id: str) -> bool:
        with self._guard():
            user = self.get_user(user_id)
            if not user:
                return False
            directory = self._user_dir(user_id).resolve()
            if directory.parent != self.users_dir.resolve():
                raise ValueError("Invalid user directory")
            shutil.rmtree(directory)
            self.log_system(
                "admin_user_deleted",
                {"user_id": user_id, "username": user.get("username", user["email"])},
            )
            return True

    def append_user_record(self, user_id: str, collection: str, record: dict) -> dict:
        self._validate_collection(collection)
        with self._guard():
            if not self.get_user(user_id):
                raise ValueError("User does not exist")
            item = self._prepare_record(record)
            path = self._user_dir(user_id) / f"{collection}.jsonl"
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
                handle.flush()
            self._invalidate_jsonl_cache(path)
            return item

    def list_user_records(self, user_id: str, collection: str) -> list[dict]:
        self._validate_collection(collection)
        with self._guard():
            records = self._read_jsonl(self._user_dir(user_id) / f"{collection}.jsonl")
            if collection == "strategies":
                self._apply_strategy_favorites(user_id, records)
            return records

    def list_user_records_filtered(
        self,
        user_id: str,
        collection: str,
        filters: dict[str, Any],
    ) -> list[dict]:
        self._validate_collection(collection)
        def matches(item: dict) -> bool:
            return all(item.get(key) == value for key, value in filters.items())

        with self._guard():
            return [item for item in self.list_user_records(user_id, collection) if matches(item)]

    def list_user_records_by_ids(
        self, user_id: str, collection: str, record_ids: set[str]
    ) -> list[dict]:
        self._validate_collection(collection)
        if not record_ids:
            return []
        safe_ids = {self._safe_id(record_id) for record_id in record_ids}
        path = self._user_dir(user_id) / f"{collection}.jsonl"
        found = {}
        with self._guard():
            for item in self._iter_jsonl_reverse(path):
                item_id = item.get("id")
                if item_id in safe_ids and item_id not in found:
                    found[item_id] = item
                    if len(found) == len(safe_ids):
                        break
            records = [found[record_id] for record_id in record_ids if record_id in found]
            if collection == "strategies":
                self._apply_strategy_favorites(user_id, records)
            return records

    def list_user_records_filtered_limited(
        self,
        user_id: str,
        collection: str,
        filters: dict[str, Any],
        expected_count: int,
    ) -> list[dict]:
        self._validate_collection(collection)
        if expected_count <= 0:
            return []

        def matches(item: dict) -> bool:
            return all(item.get(key) == value for key, value in filters.items())

        path = self._user_dir(user_id) / f"{collection}.jsonl"
        records = []
        with self._guard():
            for item in self._iter_jsonl_reverse(path):
                if not matches(item):
                    continue
                records.append(item)
                if len(records) >= expected_count:
                    break
            records.reverse()
            if collection == "strategies":
                self._apply_strategy_favorites(user_id, records)
            return records

    def count_user_records(self, user_id: str, collection: str) -> int:
        self._validate_collection(collection)
        path = self._user_dir(user_id) / f"{collection}.jsonl"
        with self._guard():
            if not path.exists():
                return 0
            with path.open("r", encoding="utf-8") as handle:
                return sum(1 for line in handle if line.strip())

    def user_collection_size(self, user_id: str, collection: str) -> int:
        self._validate_collection(collection)
        path = self._user_dir(user_id) / f"{collection}.jsonl"
        with self._guard():
            return path.stat().st_size if path.exists() else 0

    def count_fixed_events(self) -> int:
        with self._guard():
            if not self.events_path.exists():
                return 0
            with self.events_path.open("r", encoding="utf-8") as handle:
                return sum(1 for line in handle if line.strip())

    def list_user_records_page(
        self,
        user_id: str,
        collection: str,
        filters: dict[str, Any],
        page: int = 1,
        page_size: int = 20,
        known_total: int | None = None,
        known_summary: dict[str, int] | None = None,
    ) -> tuple[list[dict], int, int, dict[str, int]]:
        self._validate_collection(collection)
        if page < 1 or not 1 <= page_size <= 200:
            raise ValueError("Invalid pagination parameters")
        path = self._user_dir(user_id) / f"{collection}.jsonl"

        def matches(item: dict) -> bool:
            return all(item.get(key) == value for key, value in filters.items())

        with self._guard():
            if known_total is not None and known_summary is not None:
                total = max(0, int(known_total))
                page_count = max(1, math.ceil(total / page_size))
                current_page = min(page, page_count)
                skip = (current_page - 1) * page_size
                rows = []
                matched_count = 0
                for item in self._iter_jsonl_reverse(path):
                    if not matches(item):
                        continue
                    if matched_count >= skip:
                        rows.append(item)
                        if len(rows) >= page_size:
                            break
                    matched_count += 1
                return rows, total, current_page, known_summary

            matched_count = wins = losses = flat = up = down = 0
            newest_matches = []
            page_limit = page * page_size
            for item in self._iter_jsonl(path):
                if not matches(item):
                    continue
                matched_count += 1
                newest_matches.append(item)
                if len(newest_matches) > page_limit:
                    newest_matches.pop(0)
                if known_summary is None:
                    side = str(item.get("side", "")).upper()
                    if side == "UP":
                        up += 1
                    elif side == "DOWN":
                        down += 1
                    pnl = float(item.get("pnl", 0))
                    if pnl > 0:
                        wins += 1
                    elif pnl < 0:
                        losses += 1
                    else:
                        flat += 1
            total = known_total if known_total is not None else matched_count
            page_count = max(1, math.ceil(total / page_size))
            current_page = min(page, page_count)
            newest_offset = (current_page - 1) * page_size
            end = len(newest_matches) - newest_offset
            if end <= 0:
                rows = []
            else:
                start = max(0, end - page_size)
                rows = newest_matches[start:end]
            rows.reverse()
            summary = known_summary or {
                "wins": wins, "losses": losses, "flat": flat, "up": up, "down": down,
            }
            return rows, total, current_page, summary

    def list_recent_user_records_preview(
        self,
        user_id: str,
        collection: str,
        filters: dict[str, Any],
        limit: int = 20,
        max_scan_bytes: int = 32 * 1024 * 1024,
    ) -> list[dict]:
        self._validate_collection(collection)
        if not 1 <= limit <= 200:
            raise ValueError("Invalid preview limit")
        path = self._user_dir(user_id) / f"{collection}.jsonl"

        def matches(item: dict) -> bool:
            return all(item.get(key) == value for key, value in filters.items())

        rows = []
        with self._guard():
            for item in self._iter_jsonl_reverse_limited(path, max_scan_bytes=max_scan_bytes):
                if matches(item):
                    rows.append(item)
                    if len(rows) >= limit:
                        break
        return rows

    def top_user_records_by_numeric(
        self,
        user_id: str,
        collection: str,
        filters: dict[str, Any],
        numeric_field: str,
        limit: int = 20,
        largest: bool = True,
    ) -> list[dict]:
        self._validate_collection(collection)
        if not 1 <= limit <= 100:
            raise ValueError("Invalid top record limit")
        path = self._user_dir(user_id) / f"{collection}.jsonl"

        def matches(item: dict) -> bool:
            return all(item.get(key) == value for key, value in filters.items())

        heap: list[tuple[float, int, dict]] = []
        counter = 0
        with self._guard():
            for item in self._iter_jsonl(path):
                if not matches(item):
                    continue
                try:
                    value = float(item.get(numeric_field, 0))
                except (TypeError, ValueError):
                    continue
                score = value if largest else -value
                entry = (score, counter, item)
                counter += 1
                if len(heap) < limit:
                    heapq.heappush(heap, entry)
                elif score > heap[0][0]:
                    heapq.heapreplace(heap, entry)
        rows = [item for _score, _counter, item in sorted(heap, reverse=True)]
        return sorted(
            rows,
            key=lambda item: float(item.get(numeric_field, 0)),
            reverse=largest,
        )

    def find_user_record_reverse(
        self,
        user_id: str,
        collection: str,
        filters: dict[str, Any],
        max_scan_bytes: int | None = None,
    ) -> dict | None:
        self._validate_collection(collection)

        def matches(item: dict) -> bool:
            return all(item.get(key) == value for key, value in filters.items())

        path = self._user_dir(user_id) / f"{collection}.jsonl"
        iterator = (
            self._iter_jsonl_reverse_limited(path, max_scan_bytes=max_scan_bytes)
            if max_scan_bytes is not None
            else self._iter_jsonl_reverse(path)
        )
        with self._guard():
            for item in iterator:
                if matches(item):
                    return item
        return None

    def get_user_record(self, user_id: str, collection: str, record_id: str) -> dict | None:
        self._validate_collection(collection)
        self._safe_id(record_id)
        with self._guard():
            if collection == "strategies":
                item = next(
                    (
                        row for row in self._iter_jsonl_reverse(
                            self._user_dir(user_id) / f"{collection}.jsonl"
                        )
                        if row.get("id") == record_id
                    ),
                    None,
                )
                if item:
                    self._apply_strategy_favorites(user_id, [item])
                return item
            item = self._record_index(
                self._user_dir(user_id) / f"{collection}.jsonl"
            ).get(record_id)
            if item and collection == "strategies":
                self._apply_strategy_favorites(user_id, [item])
            return item

    def update_user_record(
        self, user_id: str, collection: str, record_id: str, changes: dict
    ) -> dict:
        self._validate_collection(collection)
        immutable = {"id", "group_id", "strategy_id", "created_at"}
        if immutable & changes.keys():
            raise ValueError("Record identity fields cannot be changed")
        with self._guard():
            records = self.list_user_records(user_id, collection)
            updated = None
            for index, record in enumerate(records):
                if record.get("id") == record_id:
                    updated = {**record, **changes}
                    records[index] = updated
                    break
            if updated is None:
                raise ValueError("Record does not exist")
            json.dumps(updated, ensure_ascii=False, allow_nan=False)
            self._write_jsonl(self._user_dir(user_id) / f"{collection}.jsonl", records)
            return updated

    def delete_user_record(self, user_id: str, collection: str, record_id: str) -> bool:
        self._validate_collection(collection)
        self._safe_id(record_id)
        path = self._user_dir(user_id) / f"{collection}.jsonl"
        removed = False

        def keep(record: dict) -> bool:
            nonlocal removed
            match = record.get("id") == record_id
            removed = removed or match
            return not match

        with self._guard():
            self._filter_jsonl(path, keep)
        return removed

    def delete_user_records_filtered(
        self, user_id: str, collection: str, filters: dict[str, Any]
    ) -> int:
        self._validate_collection(collection)
        path = self._user_dir(user_id) / f"{collection}.jsonl"

        def matches(item: dict) -> bool:
            return all(item.get(key) == value for key, value in filters.items())

        with self._guard():
            _kept, removed = self._filter_jsonl(path, lambda item: not matches(item))
        return removed

    def set_strategy_favorite(
        self, user_id: str, strategy_id: str, favorite: bool, comment: str | None = None
    ) -> bool:
        self._safe_id(strategy_id)
        with self._guard():
            if not self.get_user_record(user_id, "strategies", strategy_id):
                raise ValueError("Strategy does not exist")
            overrides = self._normalize_strategy_favorites(
                self._read_json(self._favorites_path(user_id), {})
            )
            existing = overrides.get(strategy_id, {})
            overrides[strategy_id] = {
                "favorite": bool(favorite),
                "comment": (
                    self._clean_favorite_comment(comment)
                    if comment is not None
                    else str(existing.get("comment", ""))
                ),
            }
            self._write_json(self._favorites_path(user_id), overrides)
            return bool(favorite)

    def set_strategy_favorite_comment(self, user_id: str, strategy_id: str, comment: str) -> str:
        self._safe_id(strategy_id)
        cleaned = self._clean_favorite_comment(comment)
        with self._guard():
            if not self.get_user_record(user_id, "strategies", strategy_id):
                raise ValueError("Strategy does not exist")
            overrides = self._normalize_strategy_favorites(
                self._read_json(self._favorites_path(user_id), {})
            )
            existing = overrides.get(strategy_id, {})
            overrides[strategy_id] = {
                "favorite": bool(existing.get("favorite", True)),
                "comment": cleaned,
            }
            self._write_json(self._favorites_path(user_id), overrides)
            return cleaned

    def strategy_favorite_overrides(self, user_id: str) -> dict[str, dict[str, Any]]:
        with self._guard():
            return self._normalize_strategy_favorites(
                self._read_json(self._favorites_path(user_id), {})
            )

    def save_group_bundle(
        self,
        user_id: str,
        group: dict,
        strategies: list[dict],
        transactions: list[dict],
    ) -> None:
        group_id = group.get("id")
        if not group_id or any(item.get("group_id") != group_id for item in strategies):
            raise ValueError("Every strategy must belong to the saved group")
        strategy_ids = {item["id"] for item in strategies}
        if any(
            item.get("group_id") != group_id or item.get("strategy_id") not in strategy_ids
            for item in transactions
        ):
            raise ValueError("Every transaction must belong to a strategy in the saved group")
        prepared = {
            "groups": [self._prepare_record(group)],
            "strategies": [self._prepare_record(item) for item in strategies],
            "transactions": [self._prepare_record(item) for item in transactions],
        }
        json.dumps(prepared, ensure_ascii=False, allow_nan=False)
        with self._guard():
            if not self.get_user(user_id):
                raise ValueError("User does not exist")
            for collection, records in prepared.items():
                self._append_jsonl_many(self._user_dir(user_id) / f"{collection}.jsonl", records)

    def delete_group(self, user_id: str, group_id: str) -> bool:
        self._safe_id(group_id)
        with self._guard():
            groups_path = self._user_dir(user_id) / "groups.jsonl"
            strategies_path = self._user_dir(user_id) / "strategies.jsonl"
            transactions_path = self._user_dir(user_id) / "transactions.jsonl"
            paper_trades_path = self._user_dir(user_id) / "paper_trades.jsonl"
            group_seen = False

            def keep_group(group: dict) -> bool:
                nonlocal group_seen
                remove = group.get("id") == group_id
                group_seen = group_seen or remove
                return not remove

            self._filter_jsonl(groups_path, keep_group)
            if not group_seen:
                return False

            _kept, _removed, removed_ids = self._filter_jsonl_field_value(
                strategies_path, "group_id", group_id, collect_ids=True
            )
            self._filter_jsonl_field_value(transactions_path, "group_id", group_id)
            self._filter_jsonl_field_value(paper_trades_path, "group_id", group_id)
            self._remove_strategy_favorites(user_id, removed_ids)
            self.log(user_id, "strategy_group_deleted", {"group_id": group_id, "permanent": True})
            return True

    def clear_groups(self, user_id: str) -> None:
        with self._guard():
            if not self.get_user(user_id):
                raise ValueError("User does not exist")
            for collection in ("groups", "strategies", "transactions", "paper_trades"):
                self._write_jsonl(self._user_dir(user_id) / f"{collection}.jsonl", [])
            self._write_json(self._favorites_path(user_id), {})
            self.log(user_id, "strategy_groups_deleted", {"permanent": True})

    def clear_project_cache_preserving_accounts(self) -> dict[str, int]:
        with self._guard():
            profile_paths = list(self.users_dir.glob("*/profile.json"))
            removed_user_records = 0
            for profile_path in profile_paths:
                user_dir = profile_path.parent
                for collection in self.COLLECTIONS:
                    path = user_dir / f"{collection}.jsonl"
                    removed_user_records += self._jsonl_row_count(path)
                    self._write_jsonl(path, [])
                favorites_path = user_dir / "strategy_favorites.json"
                if favorites_path.exists():
                    favorites_path.unlink()

            removed_fixed_events = self.count_fixed_events()
            removed_paths = 0
            for path in (
                self.market_dir,
                self.root / "cache",
                self.root / "jobs",
                self.root / "live_monitor",
            ):
                if path.exists():
                    shutil.rmtree(path)
                    removed_paths += 1

            self.market_dir.mkdir(parents=True, exist_ok=True)
            self.events_path.touch(exist_ok=True)
            self.coverage_path.touch(exist_ok=True)
            self.system_logs_path.write_text("", encoding="utf-8")
            self._jsonl_cache.clear()
            return {
                "accounts_preserved": len(profile_paths),
                "user_records_removed": removed_user_records,
                "fixed_events_removed": removed_fixed_events,
                "directories_removed": removed_paths,
            }

    def log(self, user_id: str, event: str, details: dict | None = None) -> dict:
        return self.append_user_record(
            user_id, "logs", {"event": event, "details": details or {}, "timestamp": utc_iso()}
        )

    def log_system(self, event: str, details: dict | None = None) -> dict:
        item = self._prepare_record({
            "event": event, "details": details or {}, "timestamp": utc_iso(), "scope": "system"
        })
        with self._guard():
            with self.system_logs_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
                handle.flush()
        return item

    def admin_logs(self, limit: int = 200) -> list[dict]:
        with self._guard():
            rows = self._read_jsonl(self.system_logs_path)
            for user in self.list_users():
                for item in self.list_user_records(user["id"], "logs"):
                    rows.append({
                        **item,
                        "scope": "user",
                        "username": user.get("username", user["email"]),
                    })
        return sorted(
            rows, key=lambda item: item.get("timestamp") or item.get("created_at", ""), reverse=True
        )[:limit]

    def save_fixed_events(
        self,
        events: list[dict],
        replace_scope: tuple[list[str], str, datetime, datetime] | None = None,
    ) -> int:
        with self._guard():
            current = self.list_fixed_events()
            existing_ids = {item["id"] for item in current}
            if replace_scope:
                assets, timeframe, start_time, end_time = replace_scope
                asset_set = set(assets)
                retained = []
                for item in current:
                    try:
                        in_scope = (
                            item.get("asset") in asset_set
                            and item.get("timeframe") == timeframe
                            and parse_utc_datetime(item.get("start_time")) >= start_time
                            and parse_utc_datetime(item.get("end_time")) <= end_time
                        )
                    except (TypeError, ValueError):
                        in_scope = item.get("asset") in asset_set and item.get("timeframe") == timeframe
                    if not in_scope:
                        retained.append(item)
                current = retained
            existing = {item["id"]: item for item in current}
            existing.update({item["id"]: item for item in events})
            ordered = sorted(
                existing.values(), key=lambda item: (item["start_time"], item["asset"], item["id"])
            )
            self._write_jsonl(self.events_path, ordered)
            return sum(item["id"] not in existing_ids for item in events)

    def mark_fixed_coverage(
        self,
        asset: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        if end_time <= start_time:
            return
        start_time = parse_utc_datetime(start_time)
        end_time = parse_utc_datetime(end_time)
        with self._guard():
            rows = self._read_jsonl(self.coverage_path)
            same_scope = [
                item for item in rows
                if item.get("asset") == asset and item.get("timeframe") == timeframe
            ]
            others = [
                item for item in rows
                if item.get("asset") != asset or item.get("timeframe") != timeframe
            ]
            ranges = [
                (parse_utc_datetime(item["start_time"]), parse_utc_datetime(item["end_time"]))
                for item in same_scope
            ]
            ranges.append((start_time, end_time))
            merged = self._merge_ranges(ranges)
            updated = others + [
                {
                    "id": uuid.uuid4().hex,
                    "asset": asset,
                    "timeframe": timeframe,
                    "start_time": start.isoformat(),
                    "end_time": end.isoformat(),
                    "quote_source": "gamma_resolution_only",
                    "created_at": utc_iso(),
                }
                for start, end in merged
            ]
            updated.sort(key=lambda item: (item["asset"], item["timeframe"], item["start_time"]))
            self._write_jsonl(self.coverage_path, updated)

    def fixed_coverage_ranges(
        self,
        asset_symbols: list[str] | None = None,
        timeframe: str | None = None,
    ) -> list[dict]:
        assets = set(asset_symbols or [])
        with self._guard():
            return [
                item for item in self._read_jsonl(self.coverage_path)
                if (not assets or item.get("asset") in assets)
                and (not timeframe or item.get("timeframe") == timeframe)
                and item.get("quote_source") == "gamma_resolution_only"
            ]

    def missing_fixed_coverage(
        self,
        asset_symbols: list[str],
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, list[tuple[datetime, datetime]]]:
        start_time = parse_utc_datetime(start_time)
        end_time = parse_utc_datetime(end_time)
        rows = self.fixed_coverage_ranges(asset_symbols, timeframe)
        result: dict[str, list[tuple[datetime, datetime]]] = {}
        for asset in asset_symbols:
            covered = [
                (
                    max(start_time, parse_utc_datetime(item["start_time"])),
                    min(end_time, parse_utc_datetime(item["end_time"])),
                )
                for item in rows
                if item.get("asset") == asset
                and parse_utc_datetime(item["end_time"]) > start_time
                and parse_utc_datetime(item["start_time"]) < end_time
            ]
            result[asset] = self._subtract_ranges(start_time, end_time, covered)
        return result

    def fixed_coverage_calendar(
        self,
        asset_symbols: list[str],
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict:
        start_time = parse_utc_datetime(start_time)
        end_time = parse_utc_datetime(end_time)
        rows = self.fixed_coverage_ranges(asset_symbols, timeframe)
        days = []
        cursor = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        while cursor < end_time:
            day_end = cursor + timedelta(days=1)
            covered_assets = []
            for asset in asset_symbols:
                ranges = [
                    (parse_utc_datetime(item["start_time"]), parse_utc_datetime(item["end_time"]))
                    for item in rows if item.get("asset") == asset
                ]
                missing = self._subtract_ranges(cursor, day_end, ranges)
                if not missing:
                    covered_assets.append(asset)
            days.append({
                "date": cursor.date().isoformat(),
                "covered_assets": covered_assets,
                "complete": len(covered_assets) == len(asset_symbols),
            })
            cursor = day_end
        return {
            "assets": asset_symbols,
            "timeframe": timeframe,
            "days": days,
            "ranges": rows,
        }

    def clear_fixed_cache(
        self,
        asset_symbols: list[str],
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, int]:
        start_time = parse_utc_datetime(start_time)
        end_time = parse_utc_datetime(end_time)
        assets = set(asset_symbols)
        with self._guard():
            events = self.list_fixed_events()
            retained_events = []
            removed_events = 0
            for item in events:
                try:
                    in_scope = (
                        item.get("asset") in assets
                        and item.get("timeframe") == timeframe
                        and parse_utc_datetime(item.get("start_time")) >= start_time
                        and parse_utc_datetime(item.get("end_time")) <= end_time
                    )
                except (TypeError, ValueError):
                    in_scope = item.get("asset") in assets and item.get("timeframe") == timeframe
                if in_scope:
                    removed_events += 1
                else:
                    retained_events.append(item)
            self._write_jsonl(self.events_path, retained_events)

            coverage_rows = self._read_jsonl(self.coverage_path)
            updated_coverage = []
            removed_ranges = 0
            for item in coverage_rows:
                if item.get("asset") not in assets or item.get("timeframe") != timeframe:
                    updated_coverage.append(item)
                    continue
                current_start = parse_utc_datetime(item["start_time"])
                current_end = parse_utc_datetime(item["end_time"])
                if current_end <= start_time or current_start >= end_time:
                    updated_coverage.append(item)
                    continue
                removed_ranges += 1
                leftovers = []
                if current_start < start_time:
                    leftovers.append((current_start, start_time))
                if current_end > end_time:
                    leftovers.append((end_time, current_end))
                for left_start, left_end in leftovers:
                    updated_coverage.append({
                        **item,
                        "id": uuid.uuid4().hex,
                        "start_time": left_start.isoformat(),
                        "end_time": left_end.isoformat(),
                        "quote_source": "gamma_resolution_only",
                        "created_at": utc_iso(),
                    })
            updated_coverage.sort(key=lambda item: (item["asset"], item["timeframe"], item["start_time"]))
            self._write_jsonl(self.coverage_path, updated_coverage)
            return {"removed_events": removed_events, "removed_ranges": removed_ranges}

    def prune_legacy_fixed_events(self) -> int:
        required = {"expiration_time", "entry_time", "data_quality"}
        with self._guard():
            rows = self._read_jsonl(self.events_path)
        valid = [
            item for item in rows
            if required <= item.keys()
            and item.get("data_quality", {}).get("is_synthetic") is False
            and item.get("data_quality", {}).get("quote_source") == "gamma_resolution_only"
        ]
        removed = len(rows) - len(valid)
        if removed:
            self._write_jsonl(self.events_path, valid)
            self._write_jsonl(self.coverage_path, [])
        return removed

    def list_fixed_events(
        self,
        asset_symbols: list[str] | None = None,
        timeframe: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict]:
        assets = set(asset_symbols or [])
        with self._guard():
            return [
                event for event in self._read_jsonl(self.events_path)
                if (not assets or event.get("asset") in assets)
                and (not timeframe or event.get("timeframe") == timeframe)
                and (not start_time or parse_utc_datetime(event["start_time"]) >= start_time)
                and (not end_time or parse_utc_datetime(event["end_time"]) <= end_time)
            ]

    @staticmethod
    def _merge_ranges(
        ranges: list[tuple[datetime, datetime]]
    ) -> list[tuple[datetime, datetime]]:
        ordered = sorted((start, end) for start, end in ranges if end > start)
        if not ordered:
            return []
        merged = [ordered[0]]
        for start, end in ordered[1:]:
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))
        return merged

    @classmethod
    def _subtract_ranges(
        cls,
        start_time: datetime,
        end_time: datetime,
        covered: list[tuple[datetime, datetime]],
    ) -> list[tuple[datetime, datetime]]:
        missing = []
        cursor = start_time
        for start, end in cls._merge_ranges(covered):
            if end <= cursor or start >= end_time:
                continue
            clipped_start = max(start, start_time)
            clipped_end = min(end, end_time)
            if clipped_start > cursor:
                missing.append((cursor, clipped_start))
            cursor = max(cursor, clipped_end)
            if cursor >= end_time:
                break
        if cursor < end_time:
            missing.append((cursor, end_time))
        return missing

    @contextmanager
    def _guard(self):
        with self._thread_lock:
            depth = getattr(self._lock_state, "depth", 0)
            if depth == 0:
                self._process_lock.acquire()
            self._lock_state.depth = depth + 1
            try:
                yield
            finally:
                self._lock_state.depth -= 1
                if depth == 0:
                    self._process_lock.release()

    def _user_dir(self, user_id: str) -> Path:
        return self.users_dir / self._safe_id(user_id)

    def _favorites_path(self, user_id: str) -> Path:
        return self._user_dir(user_id) / "strategy_favorites.json"

    def _apply_strategy_favorites(self, user_id: str, records: list[dict]) -> None:
        if not records:
            return
        overrides = self._normalize_strategy_favorites(
            self._read_json(self._favorites_path(user_id), {})
        )
        if not overrides:
            return
        for record in records:
            strategy_id = record.get("id")
            if strategy_id in overrides:
                record["favorite"] = bool(overrides[strategy_id].get("favorite", False))
                record["favorite_comment"] = str(overrides[strategy_id].get("comment", ""))

    def _remove_strategy_favorites(self, user_id: str, strategy_ids: set[str]) -> None:
        if not strategy_ids:
            return
        path = self._favorites_path(user_id)
        overrides = self._normalize_strategy_favorites(self._read_json(path, {}))
        if not overrides:
            return
        updated = {
            key: value for key, value in overrides.items()
            if key not in strategy_ids
        }
        if len(updated) != len(overrides):
            self._write_json(path, updated)

    @staticmethod
    def _normalize_strategy_favorites(value: Any) -> dict[str, dict[str, Any]]:
        if not isinstance(value, dict):
            return {}
        normalized = {}
        for key, entry in value.items():
            strategy_id = str(key)
            if isinstance(entry, dict):
                normalized[strategy_id] = {
                    "favorite": bool(entry.get("favorite", False)),
                    "comment": str(entry.get("comment", ""))[:2000],
                }
            else:
                normalized[strategy_id] = {"favorite": bool(entry), "comment": ""}
        return normalized

    @staticmethod
    def _clean_favorite_comment(comment: str | None) -> str:
        return str(comment or "").replace("\r\n", "\n").replace("\r", "\n").strip()[:2000]

    @staticmethod
    def _safe_id(value: str) -> str:
        safe = "".join(character for character in str(value) if character.isalnum() or character in "-_")
        if not safe or safe != str(value):
            raise ValueError("Invalid record identifier")
        return safe

    def _validate_collection(self, collection: str) -> None:
        if collection not in self.COLLECTIONS:
            raise ValueError(f"Unsupported collection: {collection}")

    @staticmethod
    def _prepare_record(record: dict) -> dict:
        item = dict(record)
        item.setdefault("id", uuid.uuid4().hex)
        item.setdefault("created_at", utc_iso())
        return item

    @staticmethod
    def _read_json(path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    def _read_jsonl(self, path: Path) -> list[dict]:
        if not path.exists():
            return []
        signature = self._jsonl_signature(path)
        cached = self._jsonl_cache.get(path)
        if cached and cached["signature"] == signature:
            return cached["rows"]
        rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self._jsonl_cache[path] = {
            "signature": signature,
            "rows": rows,
            "by_id": {item.get("id"): item for item in rows if item.get("id")},
        }
        return rows

    def _iter_jsonl(self, path: Path):
        if not path.exists():
            return
        cached = self._jsonl_cache.get(path)
        signature = self._jsonl_signature(path)
        if cached and cached["signature"] == signature:
            yield from cached["rows"]
            return
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)

    @staticmethod
    def _iter_jsonl_reverse(path: Path, block_size: int = 65536):
        if not path.exists():
            return
        with path.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            buffer = b""
            while position > 0:
                read_size = min(block_size, position)
                position -= read_size
                handle.seek(position)
                buffer = handle.read(read_size) + buffer
                lines = buffer.split(b"\n")
                buffer = lines[0]
                for line in reversed(lines[1:]):
                    if line.strip():
                        yield json.loads(line.decode("utf-8"))
            if buffer.strip():
                yield json.loads(buffer.decode("utf-8"))

    @staticmethod
    def _iter_jsonl_reverse_limited(
        path: Path, block_size: int = 65536, max_scan_bytes: int = 32 * 1024 * 1024
    ):
        if not path.exists() or max_scan_bytes <= 0:
            return
        with path.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            buffer = b""
            scanned = 0
            while position > 0 and scanned < max_scan_bytes:
                read_size = min(block_size, position, max_scan_bytes - scanned)
                position -= read_size
                scanned += read_size
                handle.seek(position)
                buffer = handle.read(read_size) + buffer
                lines = buffer.split(b"\n")
                buffer = lines[0]
                for line in reversed(lines[1:]):
                    if line.strip():
                        yield json.loads(line.decode("utf-8"))
            if position == 0 and buffer.strip():
                yield json.loads(buffer.decode("utf-8"))

    @staticmethod
    def _write_json(path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)

    def _write_jsonl(self, path: Path, values: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        content = "".join(
            json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n" for value in values
        )
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(path)
        self._jsonl_cache[path] = {
            "signature": self._jsonl_signature(path),
            "rows": values,
            "by_id": {item.get("id"): item for item in values if item.get("id")},
        }

    @staticmethod
    def _jsonl_row_count(path: Path) -> int:
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())

    def _append_jsonl_many(self, path: Path, values: list[dict]) -> None:
        if not values:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for value in values:
                handle.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")
            handle.flush()
        self._invalidate_jsonl_cache(path)

    def _filter_jsonl(self, path: Path, keep) -> tuple[int, int]:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()
            return 0, 0
        temporary = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        kept = removed = 0
        with path.open("r", encoding="utf-8") as source, temporary.open("w", encoding="utf-8") as target:
            for line in source:
                if not line.strip():
                    continue
                item = json.loads(line)
                if keep(item):
                    target.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
                    kept += 1
                else:
                    removed += 1
        temporary.replace(path)
        self._invalidate_jsonl_cache(path)
        return kept, removed

    def _filter_jsonl_field_value(
        self,
        path: Path,
        field: str,
        value: str,
        collect_ids: bool = False,
    ) -> tuple[int, int, set[str]]:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()
            return 0, 0, set()
        field_token = json.dumps(field, ensure_ascii=False, separators=(",", ":"))
        value_token = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        compact_match = f"{field_token}:{value_token}"
        temporary = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        kept = removed = 0
        removed_ids: set[str] = set()
        with path.open("r", encoding="utf-8") as source, temporary.open("w", encoding="utf-8") as target:
            for line in source:
                if not line.strip():
                    continue
                item = None
                remove = compact_match in line
                if not remove and value_token in line:
                    item = json.loads(line)
                    remove = item.get(field) == value
                if remove:
                    removed += 1
                    if collect_ids:
                        item = item or json.loads(line)
                        if item.get("id"):
                            removed_ids.add(item["id"])
                    continue
                target.write(line if line.endswith("\n") else line + "\n")
                kept += 1
        if removed:
            temporary.replace(path)
            self._invalidate_jsonl_cache(path)
        else:
            temporary.unlink(missing_ok=True)
        return kept, removed, removed_ids

    @staticmethod
    def _jsonl_signature(path: Path) -> tuple[int, int] | None:
        if not path.exists():
            return None
        stat = path.stat()
        return stat.st_mtime_ns, stat.st_size

    def _record_index(self, path: Path) -> dict[str, dict]:
        self._read_jsonl(path)
        cached = self._jsonl_cache.get(path)
        return cached["by_id"] if cached else {}

    def _invalidate_jsonl_cache(self, path: Path) -> None:
        self._jsonl_cache.pop(path, None)
