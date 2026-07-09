from __future__ import annotations

import hashlib
import itertools
import math
import os
import secrets
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from statistics import fmean, pstdev
from typing import Callable

from backend.backtester import FileBacktester
from backend.fixed_time import ASSETS, TIMEFRAMES, TIMEFRAME_SECONDS, parse_utc_datetime
from backend.storage import FileStore

LIVE_MONITOR_STRATEGY_THEME = "live_monitor_saved_contracts"
MIN_POSITION_BUDGET = 1.0

LIVE_FAMILY_LABELS = {
    "quote_momentum": "Quote Momentum",
    "quote_reversal": "Quote Reversal",
    "crypto_breakout": "Crypto Breakout",
    "crypto_mean_reversion": "Crypto Mean Reversion",
}

SMART_LIVE_FAMILY_LABELS = {
    "smart_value_edge": "Smart Value Edge",
    "smart_momentum_edge": "Smart Momentum Edge",
    "smart_reversion_edge": "Smart Reversion Edge",
    "smart_tight_spread_edge": "Smart Tight Spread Edge",
    "smart_late_conviction": "Smart Late Conviction",
    "smart_acceleration_edge": "Smart Acceleration Edge",
    "smart_breakout_confirmation": "Smart Breakout Confirmation",
    "smart_pullback_continuation": "Smart Pullback Continuation",
    "smart_settlement_drift": "Smart Settlement Drift",
}

ACTIVE_SMART_LIVE_FAMILIES = (
    "smart_value_edge",
    "smart_momentum_edge",
    "smart_tight_spread_edge",
    "smart_acceleration_edge",
    "smart_breakout_confirmation",
    "smart_pullback_continuation",
    "smart_settlement_drift",
)

ALL_LIVE_FAMILY_LABELS = {
    **LIVE_FAMILY_LABELS,
    **SMART_LIVE_FAMILY_LABELS,
}

_LIVE_WORKER_EVENTS: list[dict] = []
_LIVE_WORKER_STARTING_BALANCE = 0.0


def _live_strategy_workload_units(events: list[dict], strategy_count: int) -> int:
    history_rows = sum(max(1, len(event.get("history") or [])) for event in events)
    return strategy_count * history_rows


def _live_strategy_worker_count(strategy_count: int, workload_units: int) -> int:
    threshold = _live_strategy_parallel_threshold()
    if strategy_count <= 1 or workload_units < threshold:
        return 1
    configured = os.getenv("STRATEGY_BACKTEST_WORKERS", "").strip()
    if configured:
        try:
            requested = int(configured)
        except ValueError:
            requested = 0
        if requested <= 1:
            return 1
        return min(strategy_count, requested)
    return min(strategy_count, max(1, os.cpu_count() or 1))


def _live_strategy_parallel_threshold() -> int:
    configured = os.getenv("STRATEGY_BACKTEST_PARALLEL_THRESHOLD", "").strip()
    if configured:
        try:
            return max(0, int(configured))
        except ValueError:
            return 20_000
    return 8_000


def _init_live_strategy_worker(events: list[dict], starting_balance: float) -> None:
    global _LIVE_WORKER_EVENTS, _LIVE_WORKER_STARTING_BALANCE
    _LIVE_WORKER_EVENTS = events
    _LIVE_WORKER_STARTING_BALANCE = starting_balance


def _run_live_strategy_worker(task: tuple[int, dict, bool]) -> tuple[int, dict, dict]:
    index, specification, metrics_only = task
    result = LiveMonitorStrategyGenerator(None, None)._run(
        _LIVE_WORKER_EVENTS,
        specification,
        _LIVE_WORKER_STARTING_BALANCE,
        metrics_only=metrics_only,
    )
    return index, specification, result


class LiveMonitorStrategyGenerator:
    MAX_STRATEGIES = 1000

    def __init__(self, store: FileStore, live_monitor_manager) -> None:
        self.store = store
        self.live_monitor = live_monitor_manager

    def saved_contract_summary(
        self,
        user_id: str,
        asset_symbol: str,
        timeframe: str,
        include_partial: bool = True,
        start_ts: float | None = None,
        end_ts: float | None = None,
        include_contracts: bool = True,
    ) -> dict:
        asset = self._validate_asset(asset_symbol)
        self._validate_timeframe(timeframe)
        rows = self._matching_records(user_id, asset, timeframe, preview=True)
        selected_rows = self._filter_records_by_time(rows, start_ts, end_ts)
        anomalous_records = [
            record for record in selected_rows if record.get("target_price_anomaly")
        ]
        suitable = [record for record in rows if not record.get("unsuitable_contract")]
        partial = [record for record in suitable if record.get("partial_contract")]
        full = [record for record in suitable if not record.get("partial_contract")]
        selectable = self._summary_events_from_records(
            suitable, asset, timeframe, include_partial
        )
        selected = self._filter_events_by_time(selectable, start_ts, end_ts)
        bounds = self._time_bounds(selectable)
        summary = {
            "asset": asset,
            "timeframe": timeframe,
            "saved_matching": len(rows),
            "available_contracts": len(suitable),
            "full_contracts": len(full),
            "partial_contracts": len(partial),
            "unsuitable_contracts": len(rows) - len(suitable),
            "target_price_anomaly_contracts": len(anomalous_records),
            "target_price_anomalies": [
                self._contract_anomaly_summary(record)
                for record in anomalous_records[:200]
            ],
            "selectable_contracts": len(selectable),
            "selected_contracts": len(selected),
            "selected_partial_contracts": sum(
                1 for event in selected if event["partial_contract"]
            ),
            "selected_contract_ids": [event["id"] for event in selected],
            "range_start_ts": bounds["start_ts"],
            "range_end_ts": bounds["end_ts"],
            "selected_start_ts": self._float_or_none(start_ts) or bounds["start_ts"],
            "selected_end_ts": self._float_or_none(end_ts) or bounds["end_ts"],
            "contracts_loaded": bool(include_contracts),
        }
        if include_contracts:
            summary["contracts"] = [
                {
                    "id": event["id"],
                    "start_ts": event["start_ts"],
                    "end_ts": event["end_ts"],
                    "partial": event["partial_contract"],
                }
                for event in selectable
            ]
        else:
            summary["contracts"] = []
        return summary

    def generate(
        self,
        user_id: str,
        group_name: str,
        asset_symbol: str,
        timeframe: str,
        include_partial: bool,
        strategy_count: int,
        starting_balance: float,
        start_ts: float | None = None,
        end_ts: float | None = None,
        strategy_mode: str = "lightweight",
        progress: Callable[[dict], None] | None = None,
        cancel_check: Callable[[], None] | None = None,
        phase_contract_counts: tuple[int, int, int] | None = None,
        strategy_families: list[str] | tuple[str, ...] | None = None,
    ) -> dict:
        name = group_name.strip()
        if not 1 <= len(name) <= 80:
            raise ValueError("Group name must contain 1 to 80 characters")
        asset = self._validate_asset(asset_symbol)
        self._validate_timeframe(timeframe)
        if not 1 <= strategy_count <= self.MAX_STRATEGIES:
            raise ValueError(f"Strategy count must be between 1 and {self.MAX_STRATEGIES}")
        if not math.isfinite(starting_balance) or starting_balance <= 0:
            raise ValueError("Starting balance must be a positive finite number")
        mode = self._validate_strategy_mode(strategy_mode)

        summary = self.saved_contract_summary(
            user_id, asset, timeframe, include_partial, start_ts, end_ts
        )
        events = self._events_from_saved_record_ids(
            user_id,
            asset,
            timeframe,
            include_partial,
            summary.get("selected_contract_ids") or [],
        )
        if not events:
            events = self._events_from_saved_records(
                user_id, asset, timeframe, include_partial, start_ts, end_ts
            )
        if not events:
            raise ValueError("No suitable saved live-monitor contracts match these settings")
        if cancel_check:
            cancel_check()

        group_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        selected_families = self._validate_strategy_families(mode, strategy_families)
        specifications = (
            self.specifications(strategy_count, mode, selected_families)
            if selected_families is not None
            else self.specifications(strategy_count, mode)
        )
        phase_plan = self._phase_plan(events, phase_contract_counts)
        if phase_plan:
            results, phase_report, used_events = self._run_phased_strategy_backtests(
                phase_plan,
                specifications,
                starting_balance,
                progress,
                cancel_check,
            )
        else:
            results = self._run_strategy_backtests(
                events,
                specifications,
                starting_balance,
                progress,
                cancel_check,
            )
            phase_report = None
            used_events = events
        if not results:
            raise ValueError("No strategies were produced by validation")
        strategies = []
        transactions = []
        for index, specification, result in results:
            if cancel_check:
                cancel_check()
            strategy_id = uuid.uuid4().hex
            strategy = {
                "id": strategy_id,
                "group_id": group_id,
                "name": f"{ALL_LIVE_FAMILY_LABELS[specification['family']]} #{index}",
                "theme": LIVE_MONITOR_STRATEGY_THEME,
                "family": specification["family"],
                "family_label": ALL_LIVE_FAMILY_LABELS[specification["family"]],
                "description": self._description(specification, asset, timeframe),
                "parameters": specification,
                "favorite": False,
                "metrics": {key: result[key] for key in (
                    "initial_capital", "final_capital", "net_profit", "roi", "sharpe_ratio",
                    "win_rate", "max_drawdown", "profit_factor", "average_trade", "trade_count",
                    "event_count", "contracts_used", "stability_score", "recovery_factor",
                    "up_trade_count", "down_trade_count", "winning_trade_count",
                    "losing_trade_count", "flat_trade_count", "skipped_contract_count",
                    "average_allocation_pct", "max_allocation_pct_used",
                )},
                "equity_curve": result["equity_curve"],
                "created_at": created_at,
            }
            strategy["metrics"].update({
                "contracts_total": summary["selectable_contracts"],
                "contracts_missing": summary["unsuitable_contracts"],
                "contracts_available": len(used_events),
                "partial_contracts_available": sum(
                    1 for event in used_events if event["partial_contract"]
                ),
            })
            if phase_report:
                phase_metrics = phase_report["strategy_phase_metrics"].get(index, {})
                strategy["metrics"].update(phase_metrics)
                strategy["phase_validation"] = {
                    "passed": index in set(phase_report.get("survivor_strategy_indexes", [])),
                    "survivor_phase": phase_report.get("survivor_phase", 0),
                    "phase_metrics": phase_metrics,
                }
            strategies.append(strategy)
            transactions.extend(
                {"strategy_id": strategy_id, "group_id": group_id, **transaction}
                for transaction in result["transactions"]
            )

        rois = [strategy["metrics"]["roi"] for strategy in strategies]
        starts = [event["start_time"] for event in used_events]
        expirations = [event["expiration_time"] for event in used_events]
        coverage = {
            asset: {
                "first_entry": min(starts),
                "last_expiration": max(expirations),
                "event_count": len(used_events),
                "expected_event_count": summary["selected_contracts"],
                "missing_event_count": summary["unsuitable_contracts"],
                "coverage_percent": round(
                    len(used_events) / summary["selected_contracts"] * 100, 4
                ) if summary["selected_contracts"] else 100.0,
                "missing_ranges": [],
                "largest_gap_seconds": self._largest_gap_seconds(used_events, timeframe),
                "partial_event_count": sum(1 for event in used_events if event["partial_contract"]),
                "unsuitable_event_count": summary["unsuitable_contracts"],
                "unused_selected_event_count": max(0, len(events) - len(used_events)),
            }
        }
        group = {
            "id": group_id,
            "name": name,
            "theme": LIVE_MONITOR_STRATEGY_THEME,
            "source": "live_monitor_saved_contracts",
            "assets": [asset],
            "asset_labels": [ASSETS[asset]],
            "timeframe": timeframe,
            "timeframe_label": TIMEFRAMES[timeframe],
            "period": {
                "start_date": parse_utc_datetime(min(starts)).date().isoformat(),
                "end_date": parse_utc_datetime(max(expirations)).date().isoformat(),
                "start_time": min(starts),
                "end_time_exclusive": max(expirations),
            },
            "starting_balance": round(starting_balance, 4),
            "strategy_count": len(strategies),
            "strategy_mode": mode,
            "strategy_families": selected_families or self.available_families(mode),
            "event_count": len(used_events),
            "contracts_total": summary["selected_contracts"],
            "contracts_missing": summary["unsuitable_contracts"],
            "contracts_available": len(used_events),
            "partial_contracts_available": sum(1 for event in used_events if event["partial_contract"]),
            "include_partial_contracts": bool(include_partial),
            "saved_contract_summary": summary,
            "coverage": coverage,
            "summary": {
                "best_roi": round(max(rois), 6),
                "average_roi": round(sum(rois) / len(rois), 6),
                "profitable_strategies": sum(roi > 0 for roi in rois),
            },
            "created_at": created_at,
        }
        if phase_report:
            group["phase_validation"] = phase_report
            group["validation_mode"] = "three_phase"
        if progress:
            progress({
                "stage": "finalization",
                "message": "Calculating final balances and saving the live-monitor group...",
            })
        if cancel_check:
            cancel_check()
        self.store.save_group_bundle(user_id, group, strategies, transactions)
        self.store.log(user_id, "live_monitor_strategy_group_created", {
            "group_id": group_id,
            "strategy_count": len(strategies),
            "asset": asset,
            "timeframe": timeframe,
            "include_partial": include_partial,
            "strategy_mode": mode,
            "strategy_families": selected_families or self.available_families(mode),
            "validation_mode": group.get("validation_mode", "single_pass"),
        })
        return group

    def _phase_plan(
        self,
        events: list[dict],
        phase_contract_counts: tuple[int, int, int] | None,
    ) -> list[dict] | None:
        if not phase_contract_counts:
            return None
        counts = tuple(int(value or 0) for value in phase_contract_counts)
        if any(value < 0 for value in counts):
            raise ValueError("Phase contract counts must not be negative")
        if sum(counts) <= 0:
            return None
        if any(value <= 0 for value in counts):
            raise ValueError("All three phase contract counts must be positive")
        if sum(counts) > len(events):
            raise ValueError(
                "Not enough selected contracts for three-phase validation: "
                f"requested {sum(counts)}, available {len(events)}"
            )
        plan = []
        offset = 0
        for number, count in enumerate(counts, start=1):
            phase_events = events[offset:offset + count]
            plan.append({
                "phase": number,
                "contract_count": count,
                "events": phase_events,
                "start_time": phase_events[0]["start_time"],
                "end_time": phase_events[-1]["expiration_time"],
            })
            offset += count
        return plan

    def _run_phased_strategy_backtests(
        self,
        phase_plan: list[dict],
        specifications: list[dict],
        starting_balance: float,
        progress: Callable[[dict], None] | None,
        cancel_check: Callable[[], None] | None,
    ) -> tuple[list[tuple[int, dict, dict]], dict, list[dict]]:
        candidates = [
            (index, specification)
            for index, specification in enumerate(specifications, start=1)
        ]
        phase_summaries = []
        phase_metrics_by_strategy: dict[int, dict] = {}
        input_count = len(candidates)
        survivor_phase = 0
        survivor_candidates: list[tuple[int, dict]] = []
        used_phase_count = 0
        stopped_at_phase: int | None = None
        for phase in phase_plan:
            if cancel_check:
                cancel_check()
            if progress:
                progress({
                    "stage": "backtest",
                    "message": (
                        f"Phase {phase['phase']}: testing {len(candidates)} candidate "
                        f"strategy(s) on {phase['contract_count']} contract(s)..."
                    ),
                    "current": input_count - len(candidates),
                    "total": input_count,
                    "phase": phase["phase"],
                })
            phase_results = self._run_strategy_backtests(
                phase["events"],
                [specification for _, specification in candidates],
                starting_balance,
                None,
                cancel_check,
                [index for index, _ in candidates],
                True,
            )
            profitable = []
            for index, specification, result in phase_results:
                phase_metrics_by_strategy.setdefault(index, {}).update({
                    f"phase_{phase['phase']}_roi": result["roi"],
                    f"phase_{phase['phase']}_net_profit": result["net_profit"],
                    f"phase_{phase['phase']}_trade_count": result["trade_count"],
                    f"phase_{phase['phase']}_contracts_used": result["contracts_used"],
                })
                if float(result["net_profit"]) > 0:
                    profitable.append((index, specification))
            phase_summaries.append({
                "phase": phase["phase"],
                "contract_count": phase["contract_count"],
                "start_time": phase["start_time"],
                "end_time": phase["end_time"],
                "tested_strategies": len(candidates),
                "profitable_strategies": len(profitable),
            })
            if progress:
                progress({
                    "stage": "backtest",
                    "message": (
                        f"Phase {phase['phase']} complete: {len(profitable)} of "
                        f"{len(candidates)} strategy(s) stayed profitable."
                    ),
                    "current": input_count - len(profitable),
                    "total": input_count,
                    "phase": phase["phase"],
                })
            if not candidates:
                break
            if not profitable:
                stopped_at_phase = phase["phase"]
                if phase["phase"] == 1:
                    ranked = sorted(
                        phase_results,
                        key=lambda item: float(item[2]["net_profit"]),
                        reverse=True,
                    )
                    survivor_candidates = [
                        (index, specification)
                        for index, specification, _result in ranked[:min(20, len(ranked))]
                    ]
                    survivor_phase = 0
                    used_phase_count = 1
                break
            candidates = profitable
            survivor_candidates = profitable
            survivor_phase = phase["phase"]
            used_phase_count = phase["phase"]

        final_candidates = survivor_candidates or candidates
        final_phase_count = max(1, used_phase_count)
        used_events = [
            event
            for phase in phase_plan[:final_phase_count]
            for event in phase["events"]
        ]
        if progress:
            progress({
                "stage": "backtest",
                "message": (
                    f"Final pass: retesting {len(final_candidates)} surviving strategy(s) "
                    f"on all {len(used_events)} phase contract(s)..."
                ),
                "current": input_count - len(final_candidates),
                "total": input_count,
            })
        final_results = self._run_strategy_backtests(
            used_events,
            [specification for _, specification in final_candidates],
            starting_balance,
            progress,
            cancel_check,
            [index for index, _ in final_candidates],
        )
        if survivor_phase > 0:
            final_results = [
                (index, specification, result)
                for index, specification, result in final_results
                if index in {candidate_index for candidate_index, _ in final_candidates}
            ]
        final_indexes = {index for index, _, _ in final_results}
        phase_report = {
            "enabled": True,
            "input_strategy_count": input_count,
            "final_strategy_count": len(final_results),
            "survivor_phase": survivor_phase,
            "stopped_at_phase": stopped_at_phase,
            "used_phase_count": final_phase_count,
            "survivor_strategy_indexes": sorted(final_indexes),
            "phase_contract_counts": [
                int(phase["contract_count"]) for phase in phase_plan
            ],
            "phases": phase_summaries,
            "strategy_phase_metrics": phase_metrics_by_strategy,
            "family_rankings": self._family_phase_rankings(
                specifications,
                phase_metrics_by_strategy,
                final_results,
            ),
        }
        return final_results, phase_report, used_events

    def _family_phase_rankings(
        self,
        specifications: list[dict],
        phase_metrics_by_strategy: dict[int, dict],
        final_results: list[tuple[int, dict, dict]],
    ) -> list[dict]:
        final_by_index = {
            index: result for index, _specification, result in final_results
        }
        buckets: dict[str, dict] = {}
        for index, specification in enumerate(specifications, start=1):
            family = specification.get("family") or "unknown"
            bucket = buckets.setdefault(
                family,
                {
                    "family": family,
                    "family_label": ALL_LIVE_FAMILY_LABELS.get(
                        family, family.replace("_", " ").title()
                    ),
                    "strategy_count": 0,
                    "generated_count": 0,
                    "profitable_count": 0,
                    "total_roi": 0.0,
                    "best_roi": None,
                    "worst_roi": None,
                    "total_trades": 0,
                    "phases": [
                        {
                            "phase": phase,
                            "tested_count": 0,
                            "profitable_count": 0,
                            "total_roi": 0.0,
                            "best_roi": None,
                            "worst_roi": None,
                        }
                        for phase in (1, 2, 3)
                    ],
                },
            )
            bucket["strategy_count"] += 1
            bucket["generated_count"] += 1
            phase_metrics = phase_metrics_by_strategy.get(index, {})
            for phase_bucket in bucket["phases"]:
                phase = phase_bucket["phase"]
                roi = self._float_or_none(phase_metrics.get(f"phase_{phase}_roi"))
                net_profit = self._float_or_none(
                    phase_metrics.get(f"phase_{phase}_net_profit")
                )
                if roi is None:
                    continue
                phase_bucket["tested_count"] += 1
                phase_bucket["profitable_count"] += int((net_profit or 0) > 0)
                phase_bucket["total_roi"] += roi
                phase_bucket["best_roi"] = (
                    roi
                    if phase_bucket["best_roi"] is None
                    else max(phase_bucket["best_roi"], roi)
                )
                phase_bucket["worst_roi"] = (
                    roi
                    if phase_bucket["worst_roi"] is None
                    else min(phase_bucket["worst_roi"], roi)
                )
            final_result = final_by_index.get(index)
            if final_result:
                roi = float(final_result.get("roi", 0) or 0)
                bucket["profitable_count"] += int(roi > 0)
                bucket["total_roi"] += roi
                bucket["best_roi"] = (
                    roi if bucket["best_roi"] is None else max(bucket["best_roi"], roi)
                )
                bucket["worst_roi"] = (
                    roi if bucket["worst_roi"] is None else min(bucket["worst_roi"], roi)
                )
                bucket["total_trades"] += int(final_result.get("trade_count", 0) or 0)

        rows = []
        for bucket in buckets.values():
            final_count = max(0, int(bucket["profitable_count"]) + int(
                sum(
                    1
                    for index, result in final_by_index.items()
                    if specifications[index - 1].get("family") == bucket["family"]
                    and float(result.get("roi", 0) or 0) <= 0
                )
            ))
            phase_rows = []
            latest_phase = 0
            latest_average_roi = 0.0
            latest_profitable_percent = 0.0
            for phase_bucket in bucket["phases"]:
                tested_count = int(phase_bucket["tested_count"])
                average_roi = (
                    phase_bucket["total_roi"] / tested_count if tested_count else 0.0
                )
                profitable_percent = (
                    phase_bucket["profitable_count"] / tested_count * 100
                    if tested_count
                    else 0.0
                )
                if tested_count:
                    latest_phase = int(phase_bucket["phase"])
                    latest_average_roi = average_roi
                    latest_profitable_percent = profitable_percent
                phase_rows.append({
                    "phase": phase_bucket["phase"],
                    "tested_count": tested_count,
                    "profitable_count": int(phase_bucket["profitable_count"]),
                    "average_roi": round(average_roi, 6),
                    "best_roi": round(float(phase_bucket["best_roi"] or 0), 6),
                    "worst_roi": round(float(phase_bucket["worst_roi"] or 0), 6),
                    "profitable_percent": round(profitable_percent, 4),
                })
            average_roi = (
                bucket["total_roi"] / final_count if final_count else latest_average_roi
            )
            profitable_percent = (
                bucket["profitable_count"] / final_count * 100 if final_count else 0.0
            )
            rows.append({
                **bucket,
                "phases": phase_rows,
                "final_strategy_count": final_count,
                "average_roi": round(average_roi, 6),
                "best_roi": round(float(bucket["best_roi"] or latest_average_roi), 6),
                "worst_roi": round(float(bucket["worst_roi"] or latest_average_roi), 6),
                "profitable_percent": round(profitable_percent, 4),
                "latest_phase": latest_phase,
                "latest_average_roi": round(latest_average_roi, 6),
                "latest_profitable_percent": round(latest_profitable_percent, 4),
                "ranking_score": (
                    latest_phase,
                    latest_profitable_percent,
                    latest_average_roi,
                    average_roi,
                ),
                "total_trades": int(bucket["total_trades"]),
            })
        rows.sort(
            key=lambda item: (
                item["latest_phase"],
                item["latest_profitable_percent"],
                item["latest_average_roi"],
                item["average_roi"],
                item["strategy_count"],
            ),
            reverse=True,
        )
        for rank, row in enumerate(rows, start=1):
            row["rank"] = rank
        return rows

    def _run_strategy_backtests(
        self,
        events: list[dict],
        specifications: list[dict],
        starting_balance: float,
        progress: Callable[[dict], None] | None,
        cancel_check: Callable[[], None] | None,
        specification_indexes: list[int] | None = None,
        metrics_only: bool = False,
    ) -> list[tuple[int, dict, dict]]:
        total = len(specifications)
        indexes = specification_indexes or list(range(1, total + 1))
        worker_count = _live_strategy_worker_count(
            total, _live_strategy_workload_units(events, total)
        )
        if worker_count == 1:
            results = []
            for completed, (index, specification) in enumerate(
                zip(indexes, specifications), start=1
            ):
                if cancel_check:
                    cancel_check()
                result = self._run(
                    events,
                    specification,
                    starting_balance,
                    metrics_only=metrics_only,
                )
                results.append((index, specification, result))
                if progress:
                    progress({
                        "stage": "backtest",
                        "message": (
                            f"Running live-monitor strategy backtests: "
                            f"generated strategy {completed} of {total}..."
                        ),
                        "current": completed,
                        "total": total,
                    })
            return results

        results: list[tuple[int, dict, dict]] = []
        completed = 0
        index_order = {index: position for position, index in enumerate(indexes)}
        executor = ProcessPoolExecutor(
            max_workers=worker_count,
            initializer=_init_live_strategy_worker,
            initargs=(events, starting_balance),
        )
        try:
            futures = [
                executor.submit(
                    _run_live_strategy_worker,
                    (index, specification, metrics_only),
                )
                for index, specification in zip(indexes, specifications)
            ]
            for future in as_completed(futures):
                if cancel_check:
                    cancel_check()
                index, specification, result = future.result()
                results.append((index, specification, result))
                completed += 1
                if progress:
                    progress({
                        "stage": "backtest",
                        "message": (
                            f"Running live-monitor strategy backtests in parallel "
                            f"({worker_count} workers): generated strategy {completed} of {total}..."
                        ),
                        "current": completed,
                        "total": total,
                        "workers": worker_count,
                    })
        finally:
            executor.shutdown(cancel_futures=True)
        return sorted(results, key=lambda item: index_order.get(item[0], total))

    def retest(
        self,
        user_id: str,
        source_group: dict,
        source_strategy: dict,
        start_ts: float | None,
        end_ts: float | None,
    ) -> tuple[dict, dict]:
        asset = self._validate_asset(source_group["assets"][0])
        timeframe = source_group["timeframe"]
        self._validate_timeframe(timeframe)
        include_partial = bool(source_group.get("include_partial_contracts", False))
        starting_balance = float(source_group["starting_balance"])
        summary = self.saved_contract_summary(
            user_id, asset, timeframe, include_partial, start_ts, end_ts
        )
        events = self._events_from_saved_record_ids(
            user_id,
            asset,
            timeframe,
            include_partial,
            summary.get("selected_contract_ids") or [],
        )
        if not events:
            events = self._events_from_saved_records(
                user_id, asset, timeframe, include_partial, start_ts, end_ts
            )
        if not events:
            raise ValueError("No suitable saved live-monitor contracts match this retest window")

        result = self._run(events, dict(source_strategy["parameters"]), starting_balance)
        group_id = uuid.uuid4().hex
        strategy_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        starts = [event["start_time"] for event in events]
        expirations = [event["expiration_time"] for event in events]
        coverage = {
            asset: {
                "first_entry": min(starts),
                "last_expiration": max(expirations),
                "event_count": len(events),
                "expected_event_count": summary["selectable_contracts"],
                "missing_event_count": summary["unsuitable_contracts"],
                "coverage_percent": round(
                    len(events) / summary["selectable_contracts"] * 100, 4
                ) if summary["selectable_contracts"] else 100.0,
                "missing_ranges": [],
                "largest_gap_seconds": self._largest_gap_seconds(events, timeframe),
                "partial_event_count": sum(1 for event in events if event["partial_contract"]),
                "unsuitable_event_count": summary["unsuitable_contracts"],
            }
        }
        strategy = {
            "id": strategy_id,
            "group_id": group_id,
            "name": f"{source_strategy['family_label']} live retest",
            "theme": LIVE_MONITOR_STRATEGY_THEME,
            "family": source_strategy["family"],
            "family_label": source_strategy["family_label"],
            "description": self._description(source_strategy["parameters"], asset, timeframe),
            "parameters": dict(source_strategy["parameters"]),
            "favorite": False,
            "retest_of_strategy_id": source_strategy["id"],
            "metrics": {key: result[key] for key in (
                "initial_capital", "final_capital", "net_profit", "roi", "sharpe_ratio",
                "win_rate", "max_drawdown", "profit_factor", "average_trade", "trade_count",
                "event_count", "contracts_used", "stability_score", "recovery_factor",
                "up_trade_count", "down_trade_count", "winning_trade_count",
                "losing_trade_count", "flat_trade_count", "skipped_contract_count",
                "average_allocation_pct", "max_allocation_pct_used",
            )},
            "equity_curve": result["equity_curve"],
            "created_at": created_at,
        }
        strategy["metrics"].update({
            "contracts_total": summary["selectable_contracts"],
            "contracts_missing": summary["unsuitable_contracts"],
            "contracts_available": len(events),
            "partial_contracts_available": sum(1 for event in events if event["partial_contract"]),
        })
        group = {
            "id": group_id,
            "name": f"Retest: {source_strategy['name']}",
            "theme": LIVE_MONITOR_STRATEGY_THEME,
            "source": "live_monitor_saved_contracts",
            "assets": [asset],
            "asset_labels": [ASSETS[asset]],
            "timeframe": timeframe,
            "timeframe_label": TIMEFRAMES[timeframe],
            "period": {
                "start_date": parse_utc_datetime(min(starts)).date().isoformat(),
                "end_date": parse_utc_datetime(max(expirations)).date().isoformat(),
                "start_time": min(starts),
                "end_time_exclusive": max(expirations),
            },
            "starting_balance": round(starting_balance, 4),
            "strategy_count": 1,
            "event_count": len(events),
            "contracts_total": summary["selectable_contracts"],
            "contracts_missing": summary["unsuitable_contracts"],
            "contracts_available": len(events),
            "partial_contracts_available": sum(1 for event in events if event["partial_contract"]),
            "include_partial_contracts": include_partial,
            "saved_contract_summary": summary,
            "coverage": coverage,
            "summary": {
                "best_roi": result["roi"],
                "average_roi": result["roi"],
                "profitable_strategies": int(result["roi"] > 0),
            },
            "retest_of_group_id": source_group["id"],
            "created_at": created_at,
        }
        transactions = [
            {"strategy_id": strategy_id, "group_id": group_id, **transaction}
            for transaction in result["transactions"]
        ]
        self.store.save_group_bundle(user_id, group, [strategy], transactions)
        self.store.log(user_id, "live_monitor_strategy_retested", {
            "source_strategy_id": source_strategy["id"],
            "result_strategy_id": strategy_id,
            "start_ts": summary["selected_start_ts"],
            "end_ts": summary["selected_end_ts"],
        })
        return group, strategy

    def _events_from_saved_records(
        self,
        user_id: str,
        asset: str,
        timeframe: str,
        include_partial: bool,
        start_ts: float | None = None,
        end_ts: float | None = None,
    ) -> list[dict]:
        return self._filter_events_by_time(
            self._events_from_records(
                self._matching_records(user_id, asset, timeframe),
                asset,
                timeframe,
                include_partial,
            ),
            start_ts,
            end_ts,
        )

    def _events_from_saved_record_ids(
        self,
        user_id: str,
        asset: str,
        timeframe: str,
        include_partial: bool,
        record_ids: list[str],
    ) -> list[dict]:
        if hasattr(self.live_monitor, "list_saved_by_ids"):
            records = self.live_monitor.list_saved_by_ids(user_id, record_ids)
            return self._events_from_records(records, asset, timeframe, include_partial)
        records = []
        seen = set()
        for record_id in record_ids:
            normalized = str(record_id or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            record = self.live_monitor.get_saved(user_id, normalized)
            if record:
                records.append(record)
        return self._events_from_records(records, asset, timeframe, include_partial)

    def _events_from_records(
        self,
        records: list[dict],
        asset: str,
        timeframe: str,
        include_partial: bool,
    ) -> list[dict]:
        events = []
        for record in records:
            if record.get("unsuitable_contract"):
                continue
            if record.get("partial_contract") and not include_partial:
                continue
            event = self._record_to_event(record, asset, timeframe)
            if event:
                events.append(event)
        return sorted(events, key=lambda item: (item["start_time"], item["id"]))

    def _summary_events_from_records(
        self,
        records: list[dict],
        asset: str,
        timeframe: str,
        include_partial: bool,
    ) -> list[dict]:
        events = []
        for record in records:
            if record.get("unsuitable_contract"):
                continue
            if record.get("partial_contract") and not include_partial:
                continue
            start_ts = self._float_or_none(record.get("window_start_ts"))
            end_ts = self._float_or_none(record.get("window_end_ts"))
            if start_ts is None or end_ts is None:
                continue
            if abs((end_ts - start_ts) - TIMEFRAME_SECONDS[timeframe]) > 1:
                continue
            winning = str(record.get("winning_outcome") or "").strip().lower()
            if winning not in {"up", "down", "flat"}:
                continue
            events.append({
                "id": str(record.get("id") or record.get("slug") or uuid.uuid4().hex),
                "asset": asset,
                "timeframe": timeframe,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "start_time": datetime.fromtimestamp(start_ts, timezone.utc).isoformat(),
                "expiration_time": datetime.fromtimestamp(end_ts, timezone.utc).isoformat(),
                "partial_contract": bool(record.get("partial_contract")),
            })
        return sorted(events, key=lambda item: (item["start_time"], item["id"]))

    @classmethod
    def _filter_events_by_time(
        cls,
        events: list[dict],
        start_ts: float | None = None,
        end_ts: float | None = None,
    ) -> list[dict]:
        start = cls._float_or_none(start_ts)
        end = cls._float_or_none(end_ts)
        if start is not None and end is not None and end < start:
            raise ValueError("End time must not be before start time")
        return [
            event for event in events
            if (start is None or event["start_ts"] >= start)
            and (end is None or event["end_ts"] <= end)
        ]

    @classmethod
    def _filter_records_by_time(
        cls,
        records: list[dict],
        start_ts: float | None = None,
        end_ts: float | None = None,
    ) -> list[dict]:
        start = cls._float_or_none(start_ts)
        end = cls._float_or_none(end_ts)
        if start is not None and end is not None and end < start:
            raise ValueError("End time must not be before start time")
        result = []
        for record in records:
            record_start = cls._float_or_none(record.get("window_start_ts"))
            record_end = cls._float_or_none(record.get("window_end_ts"))
            if record_start is None or record_end is None:
                continue
            if start is not None and record_start < start:
                continue
            if end is not None and record_end > end:
                continue
            result.append(record)
        return result

    @staticmethod
    def _contract_anomaly_summary(record: dict) -> dict:
        anomalies = record.get("quality_anomalies")
        first_anomaly = anomalies[0] if isinstance(anomalies, list) and anomalies else {}
        target_ts = (
            first_anomaly.get("target_price_ts")
            or record.get("target_price_effective_ts")
            or record.get("target_price_ts")
        )
        return {
            "id": record.get("id"),
            "slug": record.get("slug"),
            "question": record.get("question") or record.get("event_title"),
            "window_start_ts": record.get("window_start_ts"),
            "window_start_time": (
                first_anomaly.get("window_start_time")
                or LiveMonitorStrategyGenerator._format_timestamp(record.get("window_start_ts"))
            ),
            "window_end_ts": record.get("window_end_ts"),
            "target_price": record.get("target_price"),
            "target_price_ts": target_ts,
            "target_price_time": (
                first_anomaly.get("target_price_time")
                or record.get("target_price_effective_time")
                or record.get("target_price_time")
                or LiveMonitorStrategyGenerator._format_timestamp(target_ts)
            ),
            "target_price_raw_ts": record.get("target_price_ts"),
            "target_price_raw_time": (
                record.get("target_price_time")
                or LiveMonitorStrategyGenerator._format_timestamp(record.get("target_price_ts"))
            ),
            "final_crypto_price": record.get("final_crypto_price"),
            "reason": (
                first_anomaly.get("message")
                or record.get("unsuitable_reason")
                or "Target price anomaly"
            ),
            "anomaly_type": first_anomaly.get("type"),
            "delay_seconds": first_anomaly.get("delay_seconds"),
            "min_deviation_pct": first_anomaly.get("min_deviation_pct"),
            "delta": first_anomaly.get("delta"),
        }

    @staticmethod
    def _format_timestamp(value) -> str | None:
        timestamp = LiveMonitorStrategyGenerator._float_or_none(value)
        if timestamp is None:
            return None
        parsed = datetime.fromtimestamp(timestamp)
        if abs(timestamp - int(timestamp)) < 0.001:
            return parsed.strftime("%H:%M:%S")
        return parsed.strftime("%H:%M:%S.%f")[:-3]

    def _matching_records(
        self, user_id: str, asset: str, timeframe: str, preview: bool = False
    ) -> list[dict]:
        category = asset.lower()
        if preview and hasattr(self.live_monitor, "list_saved_previews"):
            records = self.live_monitor.list_saved_previews(user_id, category=category)
        else:
            records = self.live_monitor.list_saved(user_id, category=category)
        return [
            record for record in records
            if self._record_timeframe(record) == timeframe
        ]

    def _record_to_event(self, record: dict, asset: str, timeframe: str) -> dict | None:
        start_ts = self._float_or_none(record.get("window_start_ts"))
        end_ts = self._float_or_none(record.get("window_end_ts"))
        if start_ts is None or end_ts is None:
            return None
        if abs((end_ts - start_ts) - TIMEFRAME_SECONDS[timeframe]) > 1:
            return None
        history = [
            row for row in (record.get("history") or [])
            if isinstance(row, dict) and self._float_or_none(row.get("t")) is not None
        ]
        history = sorted(history, key=lambda row: float(row["t"]))
        winning = str(record.get("winning_outcome") or "").strip().lower()
        if winning not in {"up", "down", "flat"}:
            return None
        return {
            "id": str(record.get("id") or record.get("slug") or uuid.uuid4().hex),
            "market_id": str(record.get("condition_id") or record.get("slug") or ""),
            "question": str(record.get("question") or record.get("event_title") or "Live monitor contract"),
            "asset": asset,
            "timeframe": timeframe,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_time": datetime.fromtimestamp(start_ts, timezone.utc).isoformat(),
            "expiration_time": datetime.fromtimestamp(end_ts, timezone.utc).isoformat(),
            "history": history,
            "resolved_outcome": winning,
            "partial_contract": bool(record.get("partial_contract")),
            "saved_record_id": record.get("id"),
            "slug": record.get("slug"),
        }

    def _run(
        self,
        events: list[dict],
        strategy: dict,
        initial_capital: float,
        metrics_only: bool = False,
    ) -> dict:
        available_cash = initial_capital
        equity_curve = [] if metrics_only else [
            {"t": events[0]["start_time"], "value": round(initial_capital, 4)}
        ]
        transactions = []
        allocation_pct = float(strategy["allocation_pct"])
        fee_rate = float(strategy["fee_bps"]) / 10_000
        prior_outcomes: list[str] = []

        for event in events:
            if strategy["family"] in SMART_LIVE_FAMILY_LABELS:
                smart_trades, available_after_smart = self._smart_trades(
                    event,
                    strategy,
                    available_cash,
                    allocation_pct,
                    fee_rate,
                    prior_outcomes,
                    metrics_only=metrics_only,
                )
                prior_outcomes.append(event["resolved_outcome"])
                if not smart_trades:
                    if not metrics_only:
                        equity_curve.append({
                            "t": event["expiration_time"],
                            "value": round(available_cash, 4),
                        })
                    continue
                available_cash = available_after_smart
                transactions.extend(smart_trades)
                if not metrics_only:
                    equity_curve.append({
                        "t": smart_trades[-1]["closed_at"],
                        "value": round(available_cash, 4),
                    })
                continue

            signal = self._signal(event, strategy)
            prior_outcomes.append(event["resolved_outcome"])
            if not signal or available_cash <= 0:
                if not metrics_only:
                    equity_curve.append({"t": event["expiration_time"], "value": round(available_cash, 4)})
                continue
            side, row = signal
            ask_price = self._entry_ask(row, side)
            if ask_price is None:
                if not metrics_only:
                    equity_curve.append({"t": event["expiration_time"], "value": round(available_cash, 4)})
                continue
            budget = min(available_cash, available_cash * allocation_pct)
            if budget < MIN_POSITION_BUDGET:
                continue
            margin_locked = budget / (1 + fee_rate)
            entry_fee = budget - margin_locked
            contracts_volume = margin_locked / ask_price
            available_cash -= margin_locked + entry_fee
            settlement_price = 1.0 if side == event["resolved_outcome"] else 0.0
            settlement_value = contracts_volume * settlement_price
            pnl = settlement_value - margin_locked - entry_fee
            cash_before = available_cash + margin_locked + entry_fee
            available_cash += settlement_value
            if metrics_only:
                transaction = {
                    "event_id": event["id"],
                    "side": side.upper(),
                    "pnl": pnl,
                    "margin_locked": margin_locked,
                    "entry_fee": entry_fee,
                    "allocation_pct_used": allocation_pct,
                }
            else:
                transaction = {
                    "event_id": event["id"],
                    "market_id": event["market_id"],
                    "market_question": event["question"],
                    "asset": event["asset"],
                    "timeframe": event["timeframe"],
                    "side": side.upper(),
                    "resolved_outcome": event["resolved_outcome"].upper(),
                    "opened_at": datetime.fromtimestamp(float(row["t"]), timezone.utc).isoformat(),
                    "closed_at": event["expiration_time"],
                    "expiration_time": event["expiration_time"],
                    "entry_price": round(ask_price, 8),
                    "ask_price": round(ask_price, 8),
                    "allocation_pct_used": round(allocation_pct, 8),
                    "analysis_history_count": len(event["history"]),
                    "contracts_volume": round(contracts_volume, 8),
                    "quantity": round(contracts_volume, 8),
                    "margin_locked": round(margin_locked, 8),
                    "entry_fee": round(entry_fee, 8),
                    "fee_bps": float(strategy["fee_bps"]),
                    "available_cash_before_entry": round(cash_before, 4),
                    "available_cash_after_entry": round(cash_before - budget, 4),
                    "available_cash_after_close": round(available_cash, 4),
                    "balance_before": round(cash_before, 4),
                    "balance_after": round(available_cash, 4),
                    "holding_seconds": round(event["end_ts"] - float(row["t"]), 3),
                    "exit_price": settlement_price,
                    "exit_type": "settlement",
                    "settlement_value": round(settlement_value, 8),
                    "pnl": round(pnl, 8),
                    "quote_source": "live_monitor_saved_history",
                    "saved_record_id": event.get("saved_record_id"),
                    "partial_contract": event.get("partial_contract", False),
                }
            transactions.append(transaction)
            if not metrics_only:
                equity_curve.append({"t": event["expiration_time"], "value": round(available_cash, 4)})

        profits = [trade["pnl"] for trade in transactions]
        if metrics_only:
            return {
                "initial_capital": round(initial_capital, 4),
                "final_capital": round(available_cash, 4),
                "net_profit": round(available_cash - initial_capital, 4),
                "roi": round((available_cash / initial_capital - 1) * 100, 6),
                "trade_count": len(transactions),
                "event_count": len(events),
                "contracts_used": len({trade["event_id"] for trade in transactions}),
                "transactions": [],
                "equity_curve": [],
            }
        returns = [
            trade["pnl"] / max(trade["margin_locked"] + trade["entry_fee"], 1e-9)
            for trade in transactions
        ]
        allocations = [float(trade.get("allocation_pct_used", 0)) for trade in transactions]
        gross_profit = sum(value for value in profits if value > 0)
        gross_loss = abs(sum(value for value in profits if value < 0))
        max_drawdown = FileBacktester._max_drawdown([point["value"] for point in equity_curve])
        sharpe_ratio = self._sharpe(returns)
        stability = FileBacktester.stability_metrics(
            [point["value"] for point in equity_curve],
            initial_capital,
            available_cash,
            sharpe_ratio,
            max_drawdown,
        )
        return {
            "initial_capital": round(initial_capital, 4),
            "final_capital": round(available_cash, 4),
            "net_profit": round(available_cash - initial_capital, 4),
            "roi": round((available_cash / initial_capital - 1) * 100, 6),
            "sharpe_ratio": round(sharpe_ratio, 6),
            "win_rate": round(sum(value > 0 for value in profits) / len(profits) * 100, 4) if profits else 0,
            "max_drawdown": round(max_drawdown, 6),
            **stability,
            "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss else (999.0 if gross_profit else 0),
            "average_trade": round(fmean(profits), 6) if profits else 0,
            "trade_count": len(transactions),
            "event_count": len(events),
            "contracts_used": len({trade["event_id"] for trade in transactions}),
            "up_trade_count": sum(trade["side"] == "UP" for trade in transactions),
            "down_trade_count": sum(trade["side"] == "DOWN" for trade in transactions),
            "winning_trade_count": sum(value > 0 for value in profits),
            "losing_trade_count": sum(value < 0 for value in profits),
            "flat_trade_count": sum(value == 0 for value in profits),
            "skipped_contract_count": max(0, len(events) - len({trade["event_id"] for trade in transactions})),
            "average_allocation_pct": round(fmean(allocations) * 100, 6) if allocations else 0.0,
            "max_allocation_pct_used": round(max(allocations) * 100, 6) if allocations else 0.0,
            "equity_curve": equity_curve,
            "transactions": transactions,
        }

    def _smart_trades(
        self,
        event: dict,
        strategy: dict,
        available_cash: float,
        allocation_pct: float,
        fee_rate: float,
        prior_outcomes: list[str],
        metrics_only: bool = False,
    ) -> tuple[list[dict], float]:
        if available_cash <= 0:
            return [], available_cash
        rows = [
            row for row in event["history"]
            if self._float_or_none(row.get("t")) is not None
            and event["start_ts"] <= float(row["t"]) <= event["end_ts"]
        ]
        rows = sorted(rows, key=lambda row: float(row["t"]))
        if not rows:
            return [], available_cash

        entries = []
        min_entry_ts = event["start_ts"] + float(strategy["entry_second"])
        latest_entry_ts = event["end_ts"] - float(strategy["min_hold_seconds"])
        entry_mode = str(strategy.get("entry_mode", "single_entry"))
        max_entries = max(1, int(strategy.get("max_entries_per_contract", 1)))
        if entry_mode != "scale_in":
            max_entries = 1
        entry_gap_seconds = float(strategy.get("add_entry_min_gap_seconds", 20.0))
        add_edge_cents = float(strategy.get("add_entry_edge_cents", strategy["min_edge_cents"]))
        max_price_worse = float(strategy.get("add_entry_max_price_worse_cents", 2.0))
        contract_budget = min(available_cash, available_cash * allocation_pct)
        entry_budget = contract_budget / max_entries
        if entry_budget < MIN_POSITION_BUDGET:
            return [], available_cash
        cash_after_entries = available_cash
        spent_budget = 0.0
        last_entry_ts = None
        average_entry_price = None

        for index, row in enumerate(rows):
            current_ts = self._cached_float(row, "t")
            if current_ts is None:
                continue
            if current_ts < min_entry_ts or current_ts > latest_entry_ts:
                continue
            if len(entries) >= max_entries:
                break
            if last_entry_ts is not None and current_ts < last_entry_ts + entry_gap_seconds:
                continue
            probability = self._estimated_up_probability_at(
                event, rows, index, row, strategy, prior_outcomes
            )
            if probability is None:
                continue
            side, ask_price, edge_cents = self._smart_entry_candidate(
                event, row, strategy, probability, rows, index
            )
            if not side or ask_price is None or edge_cents is None:
                continue
            if entries:
                if side != entries[0]["side"]:
                    continue
                if edge_cents < add_edge_cents:
                    continue
                if average_entry_price is not None and ask_price > (
                    average_entry_price + max_price_worse / 100
                ):
                    continue
            budget = min(entry_budget, available_cash - spent_budget)
            if budget < MIN_POSITION_BUDGET:
                break
            margin_locked = budget / (1 + fee_rate)
            entry_fee = budget - margin_locked
            contracts_volume = margin_locked / ask_price
            cash_before_entry = cash_after_entries
            cash_after_entries -= margin_locked + entry_fee
            spent_budget += margin_locked + entry_fee
            entries.append({
                "index": index,
                "row": row,
                "side": side,
                "ask_price": ask_price,
                "probability": probability,
                "edge_cents": edge_cents,
                "budget": budget,
                "margin_locked": margin_locked,
                "entry_fee": entry_fee,
                "contracts_volume": contracts_volume,
                "cash_before_entry": cash_before_entry,
                "cash_after_entry": cash_after_entries,
                "scale_entry_index": len(entries) + 1,
            })
            last_entry_ts = current_ts
            total_quantity = sum(item["contracts_volume"] for item in entries)
            average_entry_price = (
                sum(item["margin_locked"] for item in entries) / total_quantity
                if total_quantity else ask_price
            )
        if not entries:
            return [], available_cash

        exit_anchor = dict(entries[-1])
        exit_anchor["ask_price"] = average_entry_price or entries[-1]["ask_price"]
        exit_row, exit_type, exit_probability, exit_edge_cents = self._smart_exit(
            event, rows, exit_anchor, strategy, prior_outcomes
        )
        if exit_row:
            exit_price = self._side_bid(exit_row, entries[0]["side"])
            if exit_price is None:
                return [], available_cash
            closed_at = datetime.fromtimestamp(float(exit_row["t"]), timezone.utc).isoformat()
            resolved_outcome = event["resolved_outcome"].upper()
        else:
            settlement_price = 1.0 if entries[0]["side"] == event["resolved_outcome"] else 0.0
            exit_price = settlement_price
            closed_at = event["expiration_time"]
            resolved_outcome = event["resolved_outcome"].upper()
            exit_type = "settlement"
            exit_probability = None
            exit_edge_cents = None

        settlement_values = [entry["contracts_volume"] * exit_price for entry in entries]
        available_after_close = cash_after_entries + sum(settlement_values)
        transactions = []
        for entry, settlement_value in zip(entries, settlement_values):
            pnl = settlement_value - entry["margin_locked"] - entry["entry_fee"]
            if metrics_only:
                transactions.append({
                    "event_id": event["id"],
                    "side": entry["side"].upper(),
                    "pnl": pnl,
                    "margin_locked": entry["margin_locked"],
                    "entry_fee": entry["entry_fee"],
                    "allocation_pct_used": entry["budget"] / max(available_cash, 1e-9),
                })
                continue
            holding_seconds = (
                (float(exit_row["t"]) if exit_row else event["end_ts"])
                - float(entry["row"]["t"])
            )
            transactions.append({
                "event_id": event["id"],
                "market_id": event["market_id"],
                "market_question": event["question"],
                "asset": event["asset"],
                "timeframe": event["timeframe"],
                "side": entry["side"].upper(),
                "resolved_outcome": resolved_outcome,
                "opened_at": datetime.fromtimestamp(float(entry["row"]["t"]), timezone.utc).isoformat(),
                "closed_at": closed_at,
                "expiration_time": event["expiration_time"],
                "entry_price": round(entry["ask_price"], 8),
                "ask_price": round(entry["ask_price"], 8),
                "allocation_pct_used": round(entry["budget"] / max(available_cash, 1e-9), 8),
                "contract_allocation_cap_pct": round(allocation_pct, 8),
                "analysis_history_count": len(event["history"]),
                "entry_history_count": entry["index"] + 1,
                "contracts_volume": round(entry["contracts_volume"], 8),
                "quantity": round(entry["contracts_volume"], 8),
                "margin_locked": round(entry["margin_locked"], 8),
                "entry_fee": round(entry["entry_fee"], 8),
                "fee_bps": float(strategy["fee_bps"]),
                "available_cash_before_entry": round(entry["cash_before_entry"], 4),
                "available_cash_after_entry": round(entry["cash_after_entry"], 4),
                "available_cash_after_close": round(available_after_close, 4),
                "balance_before": round(entry["cash_before_entry"], 4),
                "balance_after": round(available_after_close, 4),
                "holding_seconds": round(holding_seconds, 3),
                "exit_price": round(exit_price, 8),
                "exit_bid_price": round(exit_price, 8) if exit_type != "settlement" else None,
                "exit_type": exit_type,
                "settlement_value": round(settlement_value, 8),
                "pnl": round(pnl, 8),
                "quote_source": "live_monitor_smart_history",
                "entry_probability": round(entry["probability"], 8),
                "entry_edge_cents": round(entry["edge_cents"], 4),
                "exit_probability": round(exit_probability, 8) if exit_probability is not None else None,
                "exit_edge_cents": round(exit_edge_cents, 4) if exit_edge_cents is not None else None,
                "entry_mode": entry_mode,
                "scale_entry_index": entry["scale_entry_index"],
                "scale_entry_count": len(entries),
                "average_entry_price": round(average_entry_price or entry["ask_price"], 8),
                "saved_record_id": event.get("saved_record_id"),
                "partial_contract": event.get("partial_contract", False),
            })
        return transactions, available_after_close

    def _smart_entry_candidate(
        self,
        event: dict,
        row: dict,
        strategy: dict,
        up_probability: float,
        rows: list[dict],
        row_index: int,
    ) -> tuple[str | None, float | None, float | None]:
        candidates = []
        for side, probability in (("up", up_probability), ("down", 1 - up_probability)):
            ask = self._side_ask(row, side)
            if ask is None:
                continue
            candidates.append((side, ask, probability * 100 - ask * 100))
        if not candidates:
            return None, None, None
        side, ask, edge_cents = max(candidates, key=lambda item: item[2])
        if edge_cents < float(strategy["min_edge_cents"]):
            return None, None, None

        family = strategy["family"]
        momentum = self._crypto_momentum_pct_at(
            rows, row_index, float(strategy["momentum_lookback_seconds"])
        )
        if family == "smart_momentum_edge":
            if momentum is None or abs(momentum) < float(strategy["momentum_threshold_pct"]):
                return None, None, None
            if side != ("up" if momentum > 0 else "down"):
                return None, None, None
        elif family == "smart_reversion_edge":
            deviation = self._target_deviation_pct(row)
            if deviation is None or abs(deviation) < float(strategy["deviation_threshold_pct"]):
                return None, None, None
            if side != ("down" if deviation > 0 else "up"):
                return None, None, None
        elif family == "smart_tight_spread_edge":
            spread = self._side_spread_cents(row, side)
            if spread is None or spread > float(strategy["max_spread_cents"]):
                return None, None, None
        elif family == "smart_late_conviction":
            row_ts = self._float_or_none(row.get("t"))
            if row_ts is None:
                return None, None, None
            elapsed_ratio = (row_ts - event["start_ts"]) / max(
                event["end_ts"] - event["start_ts"], 1.0
            )
            if elapsed_ratio < float(strategy["late_min_elapsed_ratio"]):
                return None, None, None
            deviation = self._target_deviation_pct(row)
            if deviation is None or abs(deviation) < float(strategy["deviation_threshold_pct"]):
                return None, None, None
            if side != ("up" if deviation > 0 else "down"):
                return None, None, None
        elif family == "smart_acceleration_edge":
            fast = momentum
            slow = self._crypto_momentum_pct_at(
                rows, row_index, float(strategy["slow_momentum_lookback_seconds"])
            )
            if fast is None or slow is None:
                return None, None, None
            acceleration = fast - slow
            if abs(acceleration) < float(strategy["acceleration_threshold_pct"]):
                return None, None, None
            if side != ("up" if acceleration > 0 else "down"):
                return None, None, None
        elif family == "smart_breakout_confirmation":
            deviation = self._target_deviation_pct(row)
            if deviation is None or momentum is None:
                return None, None, None
            if abs(deviation) < float(strategy["deviation_threshold_pct"]):
                return None, None, None
            if abs(momentum) < float(strategy["momentum_threshold_pct"]):
                return None, None, None
            if (deviation > 0) != (momentum > 0):
                return None, None, None
            if side != ("up" if deviation > 0 else "down"):
                return None, None, None
        elif family == "smart_pullback_continuation":
            slow = self._crypto_momentum_pct_at(
                rows, row_index, float(strategy["slow_momentum_lookback_seconds"])
            )
            if momentum is None or slow is None:
                return None, None, None
            if abs(slow) < float(strategy["momentum_threshold_pct"]):
                return None, None, None
            pullback_threshold = float(strategy["momentum_threshold_pct"]) * 0.5
            if slow > 0:
                if momentum > pullback_threshold:
                    return None, None, None
                if side != "up":
                    return None, None, None
            else:
                if momentum < -pullback_threshold:
                    return None, None, None
                if side != "down":
                    return None, None, None
        elif family == "smart_settlement_drift":
            row_ts = self._float_or_none(row.get("t"))
            deviation = self._target_deviation_pct(row)
            if row_ts is None or deviation is None:
                return None, None, None
            elapsed_ratio = (row_ts - event["start_ts"]) / max(
                event["end_ts"] - event["start_ts"], 1.0
            )
            if elapsed_ratio < float(strategy["late_min_elapsed_ratio"]):
                return None, None, None
            if abs(deviation) < float(strategy["deviation_threshold_pct"]):
                return None, None, None
            if side != ("up" if deviation > 0 else "down"):
                return None, None, None
        return side, ask, edge_cents

    def _smart_exit(
        self,
        event: dict,
        rows: list[dict],
        entry: dict,
        strategy: dict,
        prior_outcomes: list[str],
    ) -> tuple[dict | None, str, float | None, float | None]:
        entry_ts = float(entry["row"]["t"])
        side = entry["side"]
        entry_price = float(entry["ask_price"])
        min_exit_ts = entry_ts + float(strategy["min_hold_seconds"])
        forced_exit_ts = event["end_ts"] - float(strategy["exit_before_expiration_seconds"])
        max_hold_ts = entry_ts + float(strategy["max_hold_seconds"])
        target_hold_ts = entry_ts + float(strategy.get("target_hold_seconds", strategy["max_hold_seconds"]))
        time_exit_edge_cents = float(
            strategy.get("time_exit_edge_cents", strategy["exit_edge_floor_cents"])
        )
        profit_lock_cents = float(strategy.get("profit_lock_cents", strategy["take_profit_cents"]))
        trailing_stop_cents = float(strategy.get("trailing_stop_cents", 999.0))
        hold_to_settlement = (
            str(strategy.get("exit_condition")) == "conviction_hold_or_stop"
            and float(entry.get("edge_cents", 0.0)) >= float(
                strategy.get("settlement_hold_edge_cents", strategy["min_edge_cents"])
            )
        )
        best_bid_cents = entry_price * 100
        for index, row in enumerate(rows):
            current_ts = self._cached_float(row, "t")
            if current_ts is None:
                continue
            if current_ts <= entry_ts or current_ts < min_exit_ts:
                continue
            bid_price = self._side_bid(row, side)
            if bid_price is None:
                continue
            probability = self._estimated_up_probability_at(
                event, rows, index, row, strategy, prior_outcomes
            )
            side_probability = (
                probability if side == "up" else 1 - probability
            ) if probability is not None else None
            edge_cents = (
                side_probability * 100 - bid_price * 100
            ) if side_probability is not None else None
            bid_cents = bid_price * 100
            entry_cents = entry_price * 100
            best_bid_cents = max(best_bid_cents, bid_cents)
            if bid_cents >= entry_cents + float(strategy["take_profit_cents"]):
                if not hold_to_settlement:
                    return row, "take_profit_bid", probability, edge_cents
            if bid_cents <= entry_cents - float(strategy["stop_loss_cents"]):
                return row, "stop_loss_bid", probability, edge_cents
            if hold_to_settlement:
                continue
            if edge_cents is not None and edge_cents < float(strategy["exit_edge_floor_cents"]):
                return row, "edge_faded_bid", probability, edge_cents
            if current_ts >= target_hold_ts:
                if edge_cents is not None and edge_cents < time_exit_edge_cents:
                    return row, "time_edge_decay_bid", probability, edge_cents
                if (
                    best_bid_cents >= entry_cents + profit_lock_cents
                    and best_bid_cents - bid_cents >= trailing_stop_cents
                ):
                    return row, "profit_trail_bid", probability, edge_cents
            if current_ts >= max_hold_ts:
                return row, "max_hold_bid", probability, edge_cents
            if current_ts >= forced_exit_ts:
                return row, "pre_expiration_bid", probability, edge_cents
        return None, "settlement", None, None

    def _signal(self, event: dict, strategy: dict) -> tuple[str, dict] | None:
        row = self._entry_row(event, float(strategy["entry_second"]))
        if not row:
            return None
        family = strategy["family"]
        if family in {"quote_momentum", "quote_reversal"}:
            yes_mid = self._float_or_none(row.get("yes_mid"))
            if yes_mid is None:
                bid = self._float_or_none(row.get("yes_bid"))
                ask = self._float_or_none(row.get("yes_ask"))
                yes_mid = (bid + ask) / 2 if bid is not None and ask is not None else None
            if yes_mid is None:
                return None
            edge_cents = float(strategy["quote_edge_cents"])
            side = "up" if yes_mid >= 50 + edge_cents else "down" if yes_mid <= 50 - edge_cents else None
            if side and family == "quote_reversal":
                side = self._opposite(side)
            return (side, row) if side else None

        target = self._float_or_none(row.get("target_price"))
        crypto = self._float_or_none(row.get("crypto_price"))
        if target is None or crypto is None or target == 0:
            return None
        deviation_pct = (crypto - target) / target * 100
        if abs(deviation_pct) < float(strategy["deviation_threshold_pct"]):
            return None
        side = "up" if deviation_pct > 0 else "down"
        if family == "crypto_mean_reversion":
            side = self._opposite(side)
        return side, row

    @staticmethod
    def _entry_row(event: dict, entry_second: float) -> dict | None:
        target_ts = event["start_ts"] + entry_second
        candidates = [
            row for row in event["history"]
            if LiveMonitorStrategyGenerator._float_or_none(row.get("t")) is not None
            and float(row["t"]) >= target_ts
            and float(row["t"]) <= event["end_ts"]
        ]
        return candidates[0] if candidates else None

    @staticmethod
    def _entry_ask(row: dict, side: str) -> float | None:
        key = "yes_ask" if side == "up" else "no_ask"
        value = LiveMonitorStrategyGenerator._cached_float(row, key)
        if value is None:
            return None
        price = value / 100
        return price if 0 < price < 1 else None

    @staticmethod
    def _side_ask(row: dict, side: str) -> float | None:
        if side == "up":
            value = LiveMonitorStrategyGenerator._cached_float(row, "yes_ask")
        else:
            value = LiveMonitorStrategyGenerator._cached_float(row, "no_ask")
            if value is None:
                yes_bid = LiveMonitorStrategyGenerator._cached_float(row, "yes_bid")
                value = 100 - yes_bid if yes_bid is not None else None
        if value is None:
            return None
        price = value / 100
        return price if 0 < price < 1 else None

    @staticmethod
    def _side_bid(row: dict, side: str) -> float | None:
        if side == "up":
            value = LiveMonitorStrategyGenerator._cached_float(row, "yes_bid")
        else:
            value = LiveMonitorStrategyGenerator._cached_float(row, "no_bid")
            if value is None:
                yes_ask = LiveMonitorStrategyGenerator._cached_float(row, "yes_ask")
                value = 100 - yes_ask if yes_ask is not None else None
        if value is None:
            return None
        price = value / 100
        return price if 0 <= price <= 1 else None

    @staticmethod
    def _side_spread_cents(row: dict, side: str) -> float | None:
        bid = LiveMonitorStrategyGenerator._side_bid(row, side)
        ask = LiveMonitorStrategyGenerator._side_ask(row, side)
        if bid is None or ask is None:
            return None
        spread = (ask - bid) * 100
        return spread if spread >= 0 else None

    def _estimated_up_probability(
        self,
        event: dict,
        rows_so_far: list[dict],
        row: dict,
        strategy: dict,
        prior_outcomes: list[str],
    ) -> float | None:
        return self._estimated_up_probability_at(
            event, rows_so_far, len(rows_so_far) - 1, row, strategy, prior_outcomes
        )

    def _estimated_up_probability_at(
        self,
        event: dict,
        rows: list[dict],
        row_index: int,
        row: dict,
        strategy: dict,
        prior_outcomes: list[str],
    ) -> float | None:
        target = self._cached_float(row, "target_price")
        crypto = self._cached_float(row, "crypto_price")
        if target is None or crypto is None or target <= 0:
            return None
        distance_pct = (crypto - target) / target * 100
        momentum_pct = self._crypto_momentum_pct_at(
            rows, row_index, float(strategy["momentum_lookback_seconds"])
        ) or 0.0
        current_ts = self._cached_float(row, "t")
        if current_ts is None:
            return None
        elapsed_ratio = max(
            0.0,
            min(1.0, (current_ts - event["start_ts"]) / max(event["end_ts"] - event["start_ts"], 1)),
        )
        prior_bias = self._prior_outcome_bias(prior_outcomes, int(strategy["prior_window"]))

        distance_scale = max(float(strategy["distance_scale_pct"]), 1e-9)
        momentum_scale = max(float(strategy["momentum_scale_pct"]), 1e-9)
        family = strategy["family"]
        if family == "smart_momentum_edge":
            raw = distance_pct / distance_scale + 1.35 * momentum_pct / momentum_scale
        elif family == "smart_reversion_edge":
            raw = distance_pct / distance_scale - 1.1 * momentum_pct / momentum_scale
        elif family == "smart_late_conviction":
            raw = 1.45 * distance_pct / distance_scale + 0.35 * momentum_pct / momentum_scale
        elif family == "smart_acceleration_edge":
            slow_momentum = self._crypto_momentum_pct_at(
                rows, row_index, float(strategy["slow_momentum_lookback_seconds"])
            ) or 0.0
            acceleration_pct = momentum_pct - slow_momentum
            raw = distance_pct / distance_scale + 1.55 * acceleration_pct / momentum_scale
        elif family == "smart_breakout_confirmation":
            raw = 1.15 * distance_pct / distance_scale + 1.15 * momentum_pct / momentum_scale
        elif family == "smart_pullback_continuation":
            slow_momentum = self._crypto_momentum_pct_at(
                rows, row_index, float(strategy["slow_momentum_lookback_seconds"])
            ) or 0.0
            raw = 0.8 * distance_pct / distance_scale + 1.2 * slow_momentum / momentum_scale - 0.35 * momentum_pct / momentum_scale
        elif family == "smart_settlement_drift":
            raw = 1.65 * distance_pct / distance_scale + 0.25 * momentum_pct / momentum_scale
        else:
            raw = distance_pct / distance_scale + 0.75 * momentum_pct / momentum_scale
        raw += prior_bias * float(strategy["prior_weight"])

        confidence = 0.30 + 0.60 * elapsed_ratio
        model_probability = 0.5 + math.tanh(raw) * 0.5 * confidence
        yes_mid = self._yes_mid(row)
        market_weight = float(strategy["market_mid_weight"])
        if yes_mid is not None:
            model_probability = (
                model_probability * (1 - market_weight) + (yes_mid / 100) * market_weight
            )
        return max(0.01, min(0.99, model_probability))

    @staticmethod
    def _yes_mid(row: dict) -> float | None:
        value = LiveMonitorStrategyGenerator._cached_float(row, "yes_mid")
        if value is not None:
            return value if 0 <= value <= 100 else None
        bid = LiveMonitorStrategyGenerator._cached_float(row, "yes_bid")
        ask = LiveMonitorStrategyGenerator._cached_float(row, "yes_ask")
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2 if 0 <= bid <= ask <= 100 else None

    @staticmethod
    def _crypto_momentum_pct(rows: list[dict], lookback_seconds: float) -> float | None:
        if len(rows) < 2:
            return None
        return LiveMonitorStrategyGenerator._crypto_momentum_pct_at(
            rows, len(rows) - 1, lookback_seconds
        )

    @staticmethod
    def _crypto_momentum_pct_at(
        rows: list[dict], current_index: int, lookback_seconds: float
    ) -> float | None:
        if current_index < 1 or current_index >= len(rows):
            return None
        current = rows[current_index]
        lookback_key = round(max(float(lookback_seconds), 0.0), 6)
        momentum_cache = current.setdefault("_live_momentum_cache", {})
        if lookback_key in momentum_cache:
            return momentum_cache[lookback_key]
        current_ts = LiveMonitorStrategyGenerator._cached_float(current, "t")
        current_price = LiveMonitorStrategyGenerator._cached_float(current, "crypto_price")
        if current_ts is None or current_price is None or current_price <= 0:
            momentum_cache[lookback_key] = None
            return None
        target_ts = current_ts - lookback_key
        previous = None
        for index in range(current_index - 1, -1, -1):
            row = rows[index]
            row_ts = LiveMonitorStrategyGenerator._cached_float(row, "t")
            row_price = LiveMonitorStrategyGenerator._cached_float(row, "crypto_price")
            if row_ts is None or row_price is None or row_price <= 0:
                continue
            previous = row
            if row_ts <= target_ts:
                break
        if previous is None:
            momentum_cache[lookback_key] = None
            return None
        previous_price = LiveMonitorStrategyGenerator._cached_float(previous, "crypto_price")
        if previous_price is None or previous_price <= 0:
            momentum_cache[lookback_key] = None
            return None
        result = (current_price - previous_price) / previous_price * 100
        momentum_cache[lookback_key] = result
        return result

    @staticmethod
    def _target_deviation_pct(row: dict) -> float | None:
        if "_live_target_deviation_pct" in row:
            return row["_live_target_deviation_pct"]
        target = LiveMonitorStrategyGenerator._cached_float(row, "target_price")
        crypto = LiveMonitorStrategyGenerator._cached_float(row, "crypto_price")
        if target is None or crypto is None or target <= 0:
            row["_live_target_deviation_pct"] = None
            return None
        result = (crypto - target) / target * 100
        row["_live_target_deviation_pct"] = result
        return result

    @staticmethod
    def _cached_float(row: dict, key: str) -> float | None:
        cache = row.setdefault("_live_float_cache", {})
        if key not in cache:
            cache[key] = LiveMonitorStrategyGenerator._float_or_none(row.get(key))
        return cache[key]

    @staticmethod
    def _prior_outcome_bias(outcomes: list[str], window: int) -> float:
        if window <= 0:
            return 0.0
        recent = [item for item in outcomes[-window:] if item in {"up", "down"}]
        if not recent:
            return 0.0
        return (recent.count("up") - recent.count("down")) / len(recent)

    @staticmethod
    def specifications(
        count: int,
        strategy_mode: str = "lightweight",
        strategy_families: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict]:
        mode = LiveMonitorStrategyGenerator._validate_strategy_mode(strategy_mode)
        families = LiveMonitorStrategyGenerator._validate_strategy_families(
            mode,
            strategy_families,
        )
        if mode == "smart":
            return LiveMonitorStrategyGenerator._smart_specifications(count, families)
        families = tuple(families or LIVE_FAMILY_LABELS)
        entry_seconds = (10, 20, 30, 45, 60, 90, 120, 180)
        quote_edges = (2, 4, 6, 8, 10, 12)
        deviation_thresholds = (0.01, 0.02, 0.03, 0.05, 0.08, 0.12)
        allocations = (0.03, 0.05, 0.075, 0.1, 0.15, 0.2)
        candidates = []
        for family, entry_second, allocation in itertools.product(
            families, entry_seconds, allocations
        ):
            edge_values = quote_edges if family.startswith("quote_") else (4,)
            deviation_values = deviation_thresholds if family.startswith("crypto_") else (0.03,)
            for quote_edge, deviation in itertools.product(edge_values, deviation_values):
                candidates.append({
                    "family": family,
                    "entry_second": entry_second,
                    "quote_edge_cents": quote_edge,
                    "deviation_threshold_pct": deviation,
                    "allocation_pct": allocation,
                    "max_allocation_pct": allocation,
                    "fee_bps": 10.0,
                    "exit_condition": "settlement",
                    "price_source": "saved_live_monitor_ask",
                })

        def candidate_key(item: dict) -> bytes:
            return hashlib.sha256(
                (
                    f"{item['family']}:{item['entry_second']}:{item['quote_edge_cents']}:"
                    f"{item['deviation_threshold_pct']}:{item['allocation_pct']}"
                ).encode("ascii")
            ).digest()

        buckets = {
            family: sorted(
                (item for item in candidates if item["family"] == family),
                key=candidate_key,
            )
            for family in families
        }
        ordered = []
        while any(buckets.values()):
            for family in families:
                if buckets[family]:
                    ordered.append(buckets[family].pop(0))
        if count > len(ordered):
            raise ValueError("Requested more unique strategies than the generator supports")
        return ordered[:count]

    @staticmethod
    def _smart_specifications(
        count: int,
        strategy_families: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict]:
        families = tuple(strategy_families or ACTIVE_SMART_LIVE_FAMILIES)
        if count > LiveMonitorStrategyGenerator.MAX_STRATEGIES:
            raise ValueError("Requested more unique smart strategies than the generator supports")

        rng = secrets.SystemRandom()
        risk_profiles = {
            "scalper": {
                "take_profit_cents": (1.5, 2.0, 2.5, 3.0),
                "stop_loss_cents": (2.5, 3.5, 4.5),
                "min_hold_seconds": (5.0, 8.0, 12.0),
                "target_hold_seconds": (25.0, 35.0, 50.0),
                "max_hold_seconds": (55.0, 75.0, 95.0),
                "exit_before_expiration_seconds": (12.0, 20.0, 30.0),
            },
            "balanced": {
                "take_profit_cents": (3.0, 4.0, 5.0, 6.0),
                "stop_loss_cents": (4.0, 5.5, 7.0),
                "min_hold_seconds": (8.0, 12.0, 18.0),
                "target_hold_seconds": (45.0, 65.0, 90.0),
                "max_hold_seconds": (100.0, 130.0, 160.0),
                "exit_before_expiration_seconds": (10.0, 18.0, 28.0),
            },
            "patient": {
                "take_profit_cents": (4.0, 5.0, 6.0),
                "stop_loss_cents": (5.0, 7.0, 9.0),
                "min_hold_seconds": (12.0, 18.0, 25.0),
                "target_hold_seconds": (75.0, 105.0, 140.0),
                "max_hold_seconds": (150.0, 190.0, 235.0),
                "exit_before_expiration_seconds": (6.0, 10.0, 16.0),
            },
        }
        family_profiles = {
            "smart_value_edge": ("balanced", "patient"),
            "smart_momentum_edge": ("scalper", "balanced"),
            "smart_reversion_edge": ("scalper", "balanced"),
            "smart_tight_spread_edge": ("scalper", "balanced"),
            "smart_late_conviction": ("scalper", "balanced"),
            "smart_acceleration_edge": ("scalper", "balanced"),
            "smart_breakout_confirmation": ("balanced", "patient"),
            "smart_pullback_continuation": ("scalper", "balanced"),
            "smart_settlement_drift": ("balanced", "patient"),
        }

        def pick(values: tuple[float, ...]) -> float:
            return float(rng.choice(values))

        def sample_for_family(family: str) -> dict:
            profile_name = rng.choice(family_profiles[family])
            profile = risk_profiles[profile_name]
            max_hold_seconds = pick(profile["max_hold_seconds"])
            target_hold_options = [
                value for value in profile["target_hold_seconds"]
                if value < max_hold_seconds
            ] or [max_hold_seconds * 0.65]
            target_hold_seconds = float(rng.choice(target_hold_options))
            min_hold_options = [
                value for value in profile["min_hold_seconds"]
                if value < target_hold_seconds
            ] or [max(1.0, target_hold_seconds * 0.25)]
            take_profit_cents = pick(profile["take_profit_cents"])
            stop_loss_cents = pick(profile["stop_loss_cents"])
            momentum_lookback = pick((10.0, 15.0, 20.0, 30.0, 45.0))
            slow_lookback = pick((45.0, 60.0, 90.0, 120.0))
            if slow_lookback <= momentum_lookback:
                slow_lookback = momentum_lookback * 2
            exit_edge_floor = pick((-2.0, -1.25, -0.5, 0.0, 0.5))
            min_edge = pick((0.75, 1.25, 1.75, 2.5, 3.5, 5.0))
            entry_mode = rng.choice(("single_entry", "single_entry", "scale_in"))
            max_entries = 1 if entry_mode == "single_entry" else int(rng.choice((2, 3)))
            exit_condition = rng.choice((
                "dynamic_bid_or_settlement",
                "dynamic_bid_or_settlement",
                "conviction_hold_or_stop",
            ))
            if family == "smart_settlement_drift":
                exit_condition = rng.choice((
                    "conviction_hold_or_stop",
                    "conviction_hold_or_stop",
                    "dynamic_bid_or_settlement",
                ))
            time_exit_edge = float(
                rng.choice(
                    [
                        value for value in (-0.75, 0.0, 0.75, 1.5, 2.5)
                        if value != exit_edge_floor
                    ]
                )
            )
            return {
                "family": family,
                "entry_second": pick((5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0)),
                "min_edge_cents": min_edge,
                "entry_mode": entry_mode,
                "max_entries_per_contract": max_entries,
                "add_entry_min_gap_seconds": pick((15.0, 25.0, 40.0, 60.0)),
                "add_entry_edge_cents": min_edge + pick((0.0, 0.75, 1.5, 2.5)),
                "add_entry_max_price_worse_cents": pick((0.0, 1.0, 2.0, 3.5)),
                "take_profit_cents": take_profit_cents,
                "stop_loss_cents": stop_loss_cents,
                "exit_edge_floor_cents": exit_edge_floor,
                "time_exit_edge_cents": time_exit_edge,
                "profit_lock_cents": min(take_profit_cents - 0.25, pick((1.0, 1.5, 2.0, 3.0))),
                "trailing_stop_cents": pick((1.0, 1.5, 2.0, 3.0, 4.0)),
                "min_hold_seconds": float(rng.choice(min_hold_options)),
                "target_hold_seconds": target_hold_seconds,
                "max_hold_seconds": max_hold_seconds,
                "exit_before_expiration_seconds": pick(profile["exit_before_expiration_seconds"]),
                "momentum_lookback_seconds": momentum_lookback,
                "slow_momentum_lookback_seconds": slow_lookback,
                "momentum_threshold_pct": pick((0.004, 0.0075, 0.01, 0.015, 0.025)),
                "acceleration_threshold_pct": pick((0.004, 0.008, 0.012, 0.018, 0.028)),
                "deviation_threshold_pct": pick((0.004, 0.008, 0.012, 0.018, 0.03)),
                "late_min_elapsed_ratio": pick((0.35, 0.45, 0.55, 0.65, 0.75)),
                "max_spread_cents": pick((2.0, 3.0, 4.0, 6.0, 8.0)),
                "distance_scale_pct": pick((0.025, 0.035, 0.05, 0.075, 0.1)),
                "momentum_scale_pct": pick((0.015, 0.025, 0.035, 0.05, 0.075)),
                "prior_window": int(rng.choice((4, 6, 8, 12, 18, 24))),
                "prior_weight": pick((0.0, 0.08, 0.14, 0.2, 0.28)),
                "market_mid_weight": pick((0.0, 0.08, 0.15, 0.25, 0.35)),
                "allocation_pct": pick((0.02, 0.03, 0.05, 0.075, 0.1, 0.125)),
                "max_allocation_pct": 0.0,
                "fee_bps": 10.0,
                "exit_condition": exit_condition,
                "settlement_hold_edge_cents": min_edge + pick((1.5, 2.5, 4.0, 6.0)),
                "price_source": "saved_live_monitor_bid_ask",
                "uses_intrachart_exit": True,
                "generation_method": "system_random_parameter_sample",
                "risk_profile": profile_name,
            }

        ordered = []
        seen = set()
        family_cycle = list(families)
        rng.shuffle(family_cycle)
        attempts = 0
        while len(ordered) < count and attempts < count * 100:
            family = family_cycle[len(ordered) % len(family_cycle)]
            if len(ordered) and len(ordered) % len(family_cycle) == 0:
                rng.shuffle(family_cycle)
            candidate = sample_for_family(family)
            candidate["max_allocation_pct"] = candidate["allocation_pct"]
            signature = tuple(sorted(candidate.items()))
            if signature in seen:
                attempts += 1
                continue
            seen.add(signature)
            ordered.append(candidate)
        if len(ordered) < count:
            raise ValueError("Could not generate enough unique smart strategies")
        return ordered

    @staticmethod
    def _description(specification: dict, asset: str, timeframe: str) -> str:
        entry_second = specification["entry_second"]
        allocation = specification["allocation_pct"]
        family = specification["family"]
        if family in SMART_LIVE_FAMILY_LABELS:
            family_rules = {
                "smart_value_edge": (
                    "scans every saved quote row after the minimum entry time and buys the side "
                    "whose formula probability is underpriced versus the current ask"
                ),
                "smart_momentum_edge": (
                    "requires the formula edge to agree with recent crypto momentum before buying "
                    "the current ask"
                ),
                "smart_reversion_edge": (
                    "requires the formula edge to point against an overextended move away from the "
                    "contract target before buying the current ask"
                ),
                "smart_tight_spread_edge": (
                    "requires the formula edge to appear while the chosen side has a tight saved "
                    "bid/ask spread, so entries avoid paying through wide quotes"
                ),
                "smart_late_conviction": (
                    "waits until the later part of the contract, then buys only when the formula "
                    "edge agrees with the current crypto price being above or below the target"
                ),
                "smart_acceleration_edge": (
                    "compares fast and slow crypto momentum and buys only when the formula edge "
                    "points in the direction of fresh acceleration"
                ),
                "smart_breakout_confirmation": (
                    "requires price distance from target and short-term crypto momentum to confirm "
                    "the same breakout direction before entering"
                ),
                "smart_pullback_continuation": (
                    "looks for a short pullback against a stronger slow move, then enters in the "
                    "slow trend direction when the quote still leaves edge"
                ),
                "smart_settlement_drift": (
                    "waits for enough contract time to pass, then follows the side implied by the "
                    "current crypto price drifting away from the target"
                ),
            }
            if specification.get("entry_mode") == "scale_in":
                entry_rule = (
                    f"It can split the {allocation:.1%} contract budget into up to "
                    f"{int(specification.get('max_entries_per_contract', 1))} same-side entries. "
                    f"Additional entries must wait at least "
                    f"{specification.get('add_entry_min_gap_seconds', 0):g}s, keep model edge above "
                    f"{specification.get('add_entry_edge_cents', specification['min_edge_cents']):g}c, "
                    f"and avoid paying more than "
                    f"{specification.get('add_entry_max_price_worse_cents', 0):g}c worse than the "
                    f"current average entry."
                )
            else:
                entry_rule = f"It uses one entry with up to {allocation:.1%} of available cash."
            if specification.get("exit_condition") == "conviction_hold_or_stop":
                exit_rule = (
                    f"When entry edge is at least "
                    f"{specification.get('settlement_hold_edge_cents', specification['min_edge_cents']):g}c, "
                    f"it ignores normal take-profit and edge-fade exits and waits for settlement unless "
                    f"the saved bid hits the {specification['stop_loss_cents']:g}c stop loss."
                )
            else:
                exit_rule = (
                    f"After entry it scans only later rows and exits at the saved bid on take profit "
                    f"({specification['take_profit_cents']:g}c), stop loss "
                    f"({specification['stop_loss_cents']:g}c), faded edge below "
                    f"{specification['exit_edge_floor_cents']:g}c, time-decay edge below "
                    f"{specification.get('time_exit_edge_cents', specification['exit_edge_floor_cents']):g}c "
                    f"after about {specification.get('target_hold_seconds', specification['max_hold_seconds']):g}s, "
                    f"profit trailing, max hold ({specification['max_hold_seconds']:g}s), or just before "
                    f"expiration ({specification['exit_before_expiration_seconds']:g}s); if no valid bid exit "
                    f"appears, it waits for settlement."
                )
            return (
                f"This smart live-monitor algorithm trades saved {asset} {timeframe} contracts "
                f"inside the chart instead of only at expiration. It {family_rules[family]}. "
                f"The probability estimate uses only rows already reached at that timestamp: "
                f"crypto price versus target, recent crypto momentum, current YES bid/ask mid, "
                f"time elapsed in the contract, and already completed prior-contract outcomes. "
                f"Entry always pays the saved ask for the selected side. It starts scanning entries "
                f"after {entry_second:g}s and requires at least "
                f"{specification['min_edge_cents']:g}c of model edge. {entry_rule} {exit_rule} "
                f"Every entry charges a {specification['fee_bps']:.0f} bps entry fee. Parameters "
                f"were sampled with system randomness for this generation."
            )
        if family == "quote_momentum":
            rule = (
                f"waits {entry_second:g} seconds, then buys UP when the saved YES mid is at "
                f"least {50 + specification['quote_edge_cents']:g}c or DOWN when it is at most "
                f"{50 - specification['quote_edge_cents']:g}c"
            )
        elif family == "quote_reversal":
            rule = (
                f"waits {entry_second:g} seconds, then fades an expensive YES/NO quote after "
                f"the YES mid moves at least {specification['quote_edge_cents']:g}c away from 50c"
            )
        elif family == "crypto_breakout":
            rule = (
                f"waits {entry_second:g} seconds, then follows the live crypto move when price "
                f"is at least {specification['deviation_threshold_pct']:g}% away from the target"
            )
        else:
            rule = (
                f"waits {entry_second:g} seconds, then fades the live crypto move when price "
                f"is at least {specification['deviation_threshold_pct']:g}% away from the target"
            )
        return (
            f"This live-monitor algorithm trades saved {asset} {timeframe} contracts only after "
            f"contracts marked unsuitable have been removed. It {rule}. Entry uses the actual "
            f"saved Polymarket ask quote for the chosen side, not a fixed assumed price. It locks "
            f"{allocation:.1%} of available cash, charges a {specification['fee_bps']:.0f} bps "
            f"entry fee, and holds until the contract settlement. Full-contract-only analysis "
            f"excludes interrupted partial records; the mixed mode includes partial records but "
            f"still excludes unsuitable records."
        )

    @staticmethod
    def _record_timeframe(record: dict) -> str | None:
        start = LiveMonitorStrategyGenerator._float_or_none(record.get("window_start_ts"))
        end = LiveMonitorStrategyGenerator._float_or_none(record.get("window_end_ts"))
        if start is not None and end is not None:
            duration = round(end - start)
            for timeframe, seconds in TIMEFRAME_SECONDS.items():
                if abs(duration - seconds) <= 1:
                    return timeframe
        slug = str(record.get("slug") or "").lower()
        for timeframe in TIMEFRAMES:
            if f"-{timeframe}-" in slug or f"_{timeframe}_" in slug:
                return timeframe
        return None

    @staticmethod
    def _largest_gap_seconds(events: list[dict], timeframe: str) -> int:
        starts = [parse_utc_datetime(event["start_time"]) for event in events]
        gaps = [
            (starts[index] - starts[index - 1]).total_seconds()
            for index in range(1, len(starts))
        ]
        return int(max(gaps, default=TIMEFRAME_SECONDS[timeframe]))

    @staticmethod
    def _time_bounds(events: list[dict]) -> dict[str, float | None]:
        if not events:
            return {"start_ts": None, "end_ts": None}
        return {
            "start_ts": min(float(event["start_ts"]) for event in events),
            "end_ts": max(float(event["end_ts"]) for event in events),
        }

    @staticmethod
    def _validate_asset(asset_symbol: str) -> str:
        asset = asset_symbol.strip().upper()
        if asset not in ASSETS:
            raise ValueError(f"Unsupported asset: {asset_symbol}")
        return asset

    @staticmethod
    def _validate_timeframe(timeframe: str) -> None:
        if timeframe != "5m":
            raise ValueError("Live-monitor strategy analysis currently supports only 5m contracts")

    @staticmethod
    def _validate_strategy_mode(strategy_mode: str) -> str:
        mode = str(strategy_mode or "lightweight").strip().lower()
        if mode not in {"lightweight", "smart"}:
            raise ValueError("Strategy intelligence must be lightweight or smart")
        return mode

    @staticmethod
    def available_families(strategy_mode: str = "lightweight") -> tuple[str, ...]:
        mode = LiveMonitorStrategyGenerator._validate_strategy_mode(strategy_mode)
        if mode == "smart":
            return ACTIVE_SMART_LIVE_FAMILIES
        return tuple(LIVE_FAMILY_LABELS)

    @staticmethod
    def _validate_strategy_families(
        strategy_mode: str,
        strategy_families: list[str] | tuple[str, ...] | None,
    ) -> tuple[str, ...] | None:
        if strategy_families is None:
            return None
        available = LiveMonitorStrategyGenerator.available_families(strategy_mode)
        selected = []
        seen = set()
        for value in strategy_families:
            family = str(value or "").strip()
            if not family or family in seen:
                continue
            if family not in available:
                raise ValueError(f"Unsupported live-monitor strategy family: {family}")
            selected.append(family)
            seen.add(family)
        if not selected:
            raise ValueError("Select at least one strategy family")
        return tuple(selected)

    @staticmethod
    def _float_or_none(value) -> float | None:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @staticmethod
    def _opposite(side: str) -> str:
        return "down" if side == "up" else "up"

    @staticmethod
    def _sharpe(returns: list[float]) -> float:
        if len(returns) < 2:
            return 0
        deviation = pstdev(returns)
        return fmean(returns) / deviation * math.sqrt(len(returns)) if deviation else 0
