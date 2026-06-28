from __future__ import annotations

import itertools
import hashlib
import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable

from backend.backtester import FileBacktester
from backend.fixed_time import (
    ASSETS,
    STRATEGY_THEME,
    TIMEFRAMES,
    TIMEFRAME_SECONDS,
    parse_period,
    parse_utc_datetime,
    validate_selection,
)
from backend.storage import FileStore

FAMILY_LABELS = {
    "history_momentum": "History Momentum",
    "history_contrarian": "History Contrarian",
    "streak_momentum": "Streak Momentum",
    "streak_reversal": "Streak Reversal",
    "alternation_follow": "Alternation Follow",
    "alternation_fade": "Alternation Fade",
    "imbalance_follow": "Imbalance Follow",
    "imbalance_reversal": "Imbalance Reversal",
    "lag_follow": "Lag Follow",
    "lag_fade": "Lag Fade",
    "double_follow": "Double Follow",
    "double_fade": "Double Fade",
}


class StrategyGroupGenerator:
    MAX_STRATEGIES = 300

    def __init__(self, store: FileStore) -> None:
        self.store = store
        self.engine = FileBacktester()

    def generate(
        self,
        user_id: str,
        group_name: str,
        asset_symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        strategy_count: int,
        starting_balance: float,
        assumed_ask_price_cents: float = 51.0,
        progress: Callable[[dict], None] | None = None,
        cancel_check: Callable[[], None] | None = None,
    ) -> dict:
        name = group_name.strip()
        if not 1 <= len(name) <= 80:
            raise ValueError("Group name must contain 1 to 80 characters")
        assets = validate_selection(asset_symbols, timeframe)
        period_start, period_end = parse_period(start_date, end_date)
        if not 1 <= strategy_count <= self.MAX_STRATEGIES:
            raise ValueError(f"Strategy count must be between 1 and {self.MAX_STRATEGIES}")
        if not math.isfinite(starting_balance) or starting_balance <= 0:
            raise ValueError("Starting balance must be a positive finite number")
        assumed_ask_price = float(assumed_ask_price_cents) / 100
        if not math.isfinite(assumed_ask_price) or not 0 < assumed_ask_price < 1:
            raise ValueError("Assumed Ask price must be between 1 and 99 cents")

        events = self.store.list_fixed_events(assets, timeframe, period_start, period_end)
        coverage = self._calculate_coverage(events, assets, timeframe, period_start, period_end)
        coverage_summary = self._coverage_summary(coverage)

        group_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        strategies = []
        transactions = []
        for index, specification in enumerate(
            self.specifications(strategy_count, assumed_ask_price), start=1
        ):
            if cancel_check:
                cancel_check()
            result = (
                self.engine.run(events, specification, starting_balance)
                if events else
                self.engine.empty_result(starting_balance, period_start.isoformat())
            )
            strategy_id = uuid.uuid4().hex
            strategy = {
                "id": strategy_id,
                "group_id": group_id,
                "name": f"{FAMILY_LABELS[specification['family']]} #{index}",
                "theme": STRATEGY_THEME,
                "family": specification["family"],
                "family_label": FAMILY_LABELS[specification["family"]],
                "description": self._description(specification, assets, timeframe),
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
            strategy["metrics"].update(coverage_summary)
            strategies.append(strategy)
            transactions.extend(
                {"strategy_id": strategy_id, "group_id": group_id, **transaction}
                for transaction in result["transactions"]
            )
            if progress:
                progress({
                    "stage": "backtest",
                    "message": f"Running strategy backtests: generated strategy {index} of {strategy_count}...",
                    "current": index,
                    "total": strategy_count,
                })

        rois = [strategy["metrics"]["roi"] for strategy in strategies]
        group = {
            "id": group_id,
            "name": name,
            "theme": STRATEGY_THEME,
            "assets": assets,
            "asset_labels": [ASSETS[asset] for asset in assets],
            "timeframe": timeframe,
            "timeframe_label": TIMEFRAMES[timeframe],
            "period": {
                "start_date": period_start.date().isoformat(),
                "end_date": end_date,
                "start_time": period_start.isoformat(),
                "end_time_exclusive": period_end.isoformat(),
            },
            "starting_balance": round(starting_balance, 4),
            "assumed_ask_price": round(assumed_ask_price, 8),
            "assumed_ask_price_cents": round(assumed_ask_price * 100, 4),
            "strategy_count": len(strategies),
            "event_count": len(events),
            **coverage_summary,
            "coverage": coverage,
            "summary": {
                "best_roi": round(max(rois), 6),
                "average_roi": round(sum(rois) / len(rois), 6),
                "profitable_strategies": sum(roi > 0 for roi in rois),
            },
            "created_at": created_at,
        }
        if progress:
            progress({
                "stage": "finalization",
                "message": "Calculating final balances and saving the strategy group...",
            })
        if cancel_check:
            cancel_check()
        self.store.save_group_bundle(user_id, group, strategies, transactions)
        self.store.log(user_id, "strategy_group_created", {
            "group_id": group_id,
            "strategy_count": len(strategies),
            "assets": assets,
            "timeframe": timeframe,
        })
        return group

    def retest(
        self,
        user_id: str,
        source_group: dict,
        source_strategy: dict,
        start_date: str,
        end_date: str,
        cancel_check: Callable[[], None] | None = None,
    ) -> tuple[dict, dict]:
        assets = validate_selection(source_group["assets"], source_group["timeframe"])
        period_start, period_end = parse_period(start_date, end_date)
        events = self.store.list_fixed_events(
            assets, source_group["timeframe"], period_start, period_end
        )
        coverage = self._calculate_coverage(
            events, assets, source_group["timeframe"], period_start, period_end
        )
        coverage_summary = self._coverage_summary(coverage)

        initial_capital = float(source_group["starting_balance"])
        if cancel_check:
            cancel_check()
        result = (
            self.engine.run(events, dict(source_strategy["parameters"]), initial_capital)
            if events else
            self.engine.empty_result(initial_capital, period_start.isoformat())
        )
        group_id = uuid.uuid4().hex
        strategy_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        strategy = {
            "id": strategy_id,
            "group_id": group_id,
            "name": f"{source_strategy['family_label']} retest",
            "theme": STRATEGY_THEME,
            "family": source_strategy["family"],
            "family_label": source_strategy["family_label"],
            "description": self._description(
                source_strategy["parameters"], assets, source_group["timeframe"]
            ),
            "parameters": dict(source_strategy["parameters"]),
            "metrics": {key: result[key] for key in (
                "initial_capital", "final_capital", "net_profit", "roi", "sharpe_ratio",
                "win_rate", "max_drawdown", "profit_factor", "average_trade", "trade_count",
                "event_count", "contracts_used", "stability_score", "recovery_factor",
                "up_trade_count", "down_trade_count", "winning_trade_count",
                "losing_trade_count", "flat_trade_count", "skipped_contract_count",
                "average_allocation_pct", "max_allocation_pct_used",
            )},
            "equity_curve": result["equity_curve"],
            "favorite": False,
            "retest_of_strategy_id": source_strategy["id"],
            "created_at": created_at,
        }
        strategy["metrics"].update(coverage_summary)
        group = {
            "id": group_id,
            "name": f"Retest: {source_strategy['name']} ({start_date} to {end_date})",
            "theme": STRATEGY_THEME,
            "assets": assets,
            "asset_labels": [ASSETS[asset] for asset in assets],
            "timeframe": source_group["timeframe"],
            "timeframe_label": TIMEFRAMES[source_group["timeframe"]],
            "period": {
                "start_date": period_start.date().isoformat(),
                "end_date": end_date,
                "start_time": period_start.isoformat(),
                "end_time_exclusive": period_end.isoformat(),
            },
            "starting_balance": round(initial_capital, 4),
            "strategy_count": 1,
            "event_count": len(events),
            **coverage_summary,
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
        summary_only = (
            len(transactions) > 5_000
            or self.store.user_collection_size(user_id, "transactions") > 100_000_000
        )
        if summary_only:
            group["transactions_summary_only"] = True
            strategy["transactions_summary_only"] = True
            transactions = []
        if cancel_check:
            cancel_check()
        self.store.save_group_bundle(user_id, group, [strategy], transactions)
        self.store.log(user_id, "strategy_retested", {
            "source_strategy_id": source_strategy["id"],
            "result_strategy_id": strategy_id,
            "start_date": start_date,
            "end_date": end_date,
        })
        return group, strategy

    @staticmethod
    def _coverage_summary(coverage: dict) -> dict:
        contracts_total = sum(
            int(stats.get("expected_event_count", stats.get("event_count", 0)))
            for stats in coverage.values()
        )
        contracts_missing = sum(
            int(stats.get("missing_event_count", 0)) for stats in coverage.values()
        )
        contracts_available = sum(
            int(stats.get("event_count", 0)) for stats in coverage.values()
        )
        return {
            "contracts_total": contracts_total,
            "contracts_missing": contracts_missing,
            "contracts_available": contracts_available,
        }

    @staticmethod
    def _calculate_coverage(
        events: list[dict],
        assets: list[str],
        timeframe: str,
        period_start: datetime,
        period_end: datetime,
    ) -> dict:
        interval = TIMEFRAME_SECONDS[timeframe]
        target_end = min(period_end, datetime.now(timezone.utc))
        result = {}
        for asset in assets:
            rows = sorted(
                (event for event in events if event["asset"] == asset),
                key=lambda item: item["start_time"],
            )
            starts = [parse_utc_datetime(item["start_time"]) for item in rows]
            expirations = [parse_utc_datetime(item["expiration_time"]) for item in rows]
            gaps = [
                (starts[index] - starts[index - 1]).total_seconds()
                for index in range(1, len(starts))
            ]
            largest_gap = max(gaps, default=interval)
            expected_count = max(
                0, int((target_end - period_start).total_seconds() // interval)
            )
            available_starts = {
                start for start, expiration in zip(starts, expirations)
                if period_start <= start and expiration <= target_end
            }
            missing_starts = [
                period_start + timedelta(seconds=index * interval)
                for index in range(expected_count)
                if period_start + timedelta(seconds=index * interval) not in available_starts
            ]
            missing_ranges = []
            for missing_start in missing_starts:
                if (
                    missing_ranges
                    and parse_utc_datetime(missing_ranges[-1]["resume_at"]) == missing_start
                ):
                    missing_ranges[-1]["missing_events"] += 1
                    missing_ranges[-1]["resume_at"] = (
                        missing_start + timedelta(seconds=interval)
                    ).isoformat()
                else:
                    missing_ranges.append({
                        "start_time": missing_start.isoformat(),
                        "resume_at": (
                            missing_start + timedelta(seconds=interval)
                        ).isoformat(),
                        "missing_events": 1,
                    })
            available_count = len(available_starts)
            result[asset] = {
                "first_entry": starts[0].isoformat() if starts else None,
                "last_expiration": expirations[-1].isoformat() if expirations else None,
                "event_count": available_count,
                "expected_event_count": expected_count,
                "missing_event_count": len(missing_starts),
                "coverage_percent": round(
                    available_count / expected_count * 100, 4
                ) if expected_count else 100.0,
                "missing_ranges": missing_ranges,
                "largest_gap_seconds": int(largest_gap),
            }
        return result

    @staticmethod
    def specifications(count: int, assumed_ask_price: float = 0.51) -> list[dict]:
        if not math.isfinite(assumed_ask_price) or not 0 < assumed_ask_price < 1:
            raise ValueError("Assumed Ask price must be between 1 and 99 cents")
        lookbacks = (2, 3, 5, 8, 13, 21, 34)
        edges = (0.0, 0.1, 0.2, 0.3)
        allocations = (0.03, 0.05, 0.075, 0.1, 0.15, 0.2)
        allocation_modes = ("fixed", "confidence_scaled", "streak_scaled", "depth_scaled")
        streak_lengths = (2, 3, 4, 5)
        families = tuple(FAMILY_LABELS)
        candidates = []
        for family in families:
            family_lookbacks = (1,) if family in {"lag_follow", "lag_fade"} else (
                (2,) if family in {"double_follow", "double_fade"} else lookbacks
            )
            family_edges = edges if family in {
                "history_momentum", "history_contrarian",
                "imbalance_follow", "imbalance_reversal",
            } else (0.0,)
            family_streaks = streak_lengths if family in {
                "streak_momentum", "streak_reversal"
            } else (2,)
            for lookback, min_edge, allocation, allocation_mode, streak_length in itertools.product(
                family_lookbacks, family_edges, allocations, allocation_modes, family_streaks
            ):
                max_allocation = min(1.0, allocation * 2.5)
                min_history = 1 if family in {"lag_follow", "lag_fade"} else (
                    2 if family in {"double_follow", "double_fade"} else max(lookback, streak_length)
                )
                candidates.append({
                    "family": family,
                    "lookback": lookback,
                    "min_history": min_history,
                    "history_skip_recent": 1,
                    "min_edge": min_edge,
                    "streak_length": streak_length,
                    "allocation_pct": allocation,
                    "max_allocation_pct": round(max_allocation, 8),
                    "allocation_mode": allocation_mode,
                    "assumed_ask_price": round(assumed_ask_price, 8),
                    "assumed_ask_price_cents": round(assumed_ask_price * 100, 4),
                    "fee_bps": 10.0,
                    "exit_condition": "settlement",
                })
        def candidate_key(item: dict) -> bytes:
            return hashlib.sha256(
                (
                    f"{item['family']}:{item['lookback']}:{item['min_edge']}:"
                    f"{item['allocation_pct']}:{item['allocation_mode']}:"
                    f"{item['streak_length']}"
                ).encode("ascii")
            ).digest()

        buckets = {
            family: sorted(
                (item for item in candidates if item["family"] == family),
                key=candidate_key,
            )
            for family in families
        }
        candidates = []
        while any(buckets.values()):
            for family in families:
                if buckets[family]:
                    candidates.append(buckets[family].pop(0))
        if count > len(candidates):
            raise ValueError("Requested more unique strategies than the generator supports")
        return candidates[:count]

    @staticmethod
    def _description(specification: dict, assets: list[str], timeframe: str) -> str:
        family = specification["family"]
        if family == "history_momentum":
            rule = "follows the majority side in its filtered outcome window"
        elif family == "history_contrarian":
            rule = "trades against the majority side in its filtered outcome window"
        elif family == "streak_momentum":
            rule = "continues same-side streaks after the configured streak length appears"
        elif family == "streak_reversal":
            rule = "fades same-side streaks after the configured streak length appears"
        elif family == "alternation_follow":
            rule = "continues alternating UP/DOWN patterns"
        elif family == "alternation_fade":
            rule = "fades alternating UP/DOWN patterns"
        elif family == "imbalance_follow":
            rule = "follows strong UP/DOWN imbalance in the filtered outcome window"
        elif family == "imbalance_reversal":
            rule = "fades strong UP/DOWN imbalance in the filtered outcome window"
        elif family == "lag_follow":
            rule = "follows the most recent analyzable closed outcome after the latest outcome is skipped"
        elif family == "lag_fade":
            rule = "fades the most recent analyzable closed outcome after the latest outcome is skipped"
        elif family == "double_follow":
            rule = "follows two same-side analyzable closed outcomes in a row"
        else:
            rule = "fades two same-side analyzable closed outcomes in a row"
        asset_scope = ", ".join(assets)
        assumed_cents = specification.get("assumed_ask_price_cents", 50)
        return (
            f"This fixed-expiration algorithm trades {asset_scope} {timeframe} Up/Down contracts. "
            f"Its signal {rule}. The indicator first removes the most recent resolved contract, "
            f"then uses a {specification['lookback']}-contract lookback and requires "
            f"{specification['min_edge']:.0%} majority edge when that filter is applicable. "
            f"Contracts are skipped until at least {specification['min_history']} older outcomes "
            f"are available. The {timeframe} interval is used because it "
            f"matches the selected Polymarket contract series and its exact on-chain resolution "
            f"window. At entry, allocation starts at {specification['allocation_pct']:.0%} of "
            f"equity with {specification['allocation_mode'].replace('_', ' ')} sizing capped at "
            f"{specification['max_allocation_pct']:.0%}. Every UP or DOWN position is opened "
            f"exactly at the contract start using the user-defined assumed Ask price of "
            f"{assumed_cents:g} cents. "
            f"The position is held for exactly {TIMEFRAMES[timeframe]} until settlement. There "
            f"are no discretionary exits, stop-losses, or take-profits. Profit and loss equals "
            f"the binary settlement value minus locked margin and the {specification['fee_bps']:.0f} "
            f"bps entry fee. Signals only use contracts that had already expired, with the latest "
            f"expired contract deliberately ignored."
        )
