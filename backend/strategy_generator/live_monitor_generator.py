from __future__ import annotations

import hashlib
import itertools
import math
import uuid
from datetime import datetime, timezone
from statistics import fmean, pstdev
from typing import Callable

from backend.backtester import FileBacktester
from backend.fixed_time import ASSETS, TIMEFRAMES, TIMEFRAME_SECONDS, parse_utc_datetime
from backend.storage import FileStore

LIVE_MONITOR_STRATEGY_THEME = "live_monitor_saved_contracts"

LIVE_FAMILY_LABELS = {
    "quote_momentum": "Quote Momentum",
    "quote_reversal": "Quote Reversal",
    "crypto_breakout": "Crypto Breakout",
    "crypto_mean_reversion": "Crypto Mean Reversion",
}


class LiveMonitorStrategyGenerator:
    MAX_STRATEGIES = 300

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
    ) -> dict:
        asset = self._validate_asset(asset_symbol)
        self._validate_timeframe(timeframe)
        rows = self._matching_records(user_id, asset, timeframe)
        suitable = [record for record in rows if not record.get("unsuitable_contract")]
        partial = [record for record in suitable if record.get("partial_contract")]
        full = [record for record in suitable if not record.get("partial_contract")]
        selectable = [
            event for event in self._events_from_records(
                suitable, asset, timeframe, include_partial
            )
        ]
        selected = self._filter_events_by_time(selectable, start_ts, end_ts)
        bounds = self._time_bounds(selectable)
        return {
            "asset": asset,
            "timeframe": timeframe,
            "saved_matching": len(rows),
            "available_contracts": len(suitable),
            "full_contracts": len(full),
            "partial_contracts": len(partial),
            "unsuitable_contracts": len(rows) - len(suitable),
            "selectable_contracts": len(selectable),
            "selected_contracts": len(selected),
            "selected_partial_contracts": sum(
                1 for event in selected if event["partial_contract"]
            ),
            "range_start_ts": bounds["start_ts"],
            "range_end_ts": bounds["end_ts"],
            "selected_start_ts": self._float_or_none(start_ts) or bounds["start_ts"],
            "selected_end_ts": self._float_or_none(end_ts) or bounds["end_ts"],
            "contracts": [
                {
                    "id": event["id"],
                    "start_ts": event["start_ts"],
                    "end_ts": event["end_ts"],
                    "partial": event["partial_contract"],
                }
                for event in selectable
            ],
        }

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
        progress: Callable[[dict], None] | None = None,
        cancel_check: Callable[[], None] | None = None,
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

        summary = self.saved_contract_summary(
            user_id, asset, timeframe, include_partial, start_ts, end_ts
        )
        events = self._events_from_saved_records(
            user_id, asset, timeframe, include_partial, start_ts, end_ts
        )
        if not events:
            raise ValueError("No suitable saved live-monitor contracts match these settings")
        if cancel_check:
            cancel_check()

        group_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        strategies = []
        transactions = []
        for index, specification in enumerate(self.specifications(strategy_count), start=1):
            if cancel_check:
                cancel_check()
            result = self._run(events, specification, starting_balance)
            strategy_id = uuid.uuid4().hex
            strategy = {
                "id": strategy_id,
                "group_id": group_id,
                "name": f"{LIVE_FAMILY_LABELS[specification['family']]} #{index}",
                "theme": LIVE_MONITOR_STRATEGY_THEME,
                "family": specification["family"],
                "family_label": LIVE_FAMILY_LABELS[specification["family"]],
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
                "contracts_available": len(events),
                "partial_contracts_available": sum(
                    1 for event in events if event["partial_contract"]
                ),
            })
            strategies.append(strategy)
            transactions.extend(
                {"strategy_id": strategy_id, "group_id": group_id, **transaction}
                for transaction in result["transactions"]
            )
            if progress:
                progress({
                    "stage": "backtest",
                    "message": f"Running live-monitor strategy backtests: generated strategy {index} of {strategy_count}...",
                    "current": index,
                    "total": strategy_count,
                })

        rois = [strategy["metrics"]["roi"] for strategy in strategies]
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
            "event_count": len(events),
            "contracts_total": summary["selectable_contracts"],
            "contracts_missing": summary["unsuitable_contracts"],
            "contracts_available": len(events),
            "partial_contracts_available": sum(1 for event in events if event["partial_contract"]),
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
        })
        return group

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

    def _matching_records(self, user_id: str, asset: str, timeframe: str) -> list[dict]:
        category = asset.lower()
        return [
            record for record in self.live_monitor.list_saved(user_id, category=category)
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

    def _run(self, events: list[dict], strategy: dict, initial_capital: float) -> dict:
        available_cash = initial_capital
        equity_curve = [{"t": events[0]["start_time"], "value": round(initial_capital, 4)}]
        transactions = []
        allocation_pct = float(strategy["allocation_pct"])
        fee_rate = float(strategy["fee_bps"]) / 10_000

        for event in events:
            signal = self._signal(event, strategy)
            if not signal or available_cash <= 0:
                equity_curve.append({"t": event["expiration_time"], "value": round(available_cash, 4)})
                continue
            side, row = signal
            ask_price = self._entry_ask(row, side)
            if ask_price is None:
                equity_curve.append({"t": event["expiration_time"], "value": round(available_cash, 4)})
                continue
            budget = min(available_cash, available_cash * allocation_pct)
            if budget <= 0:
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
                "settlement_value": round(settlement_value, 8),
                "pnl": round(pnl, 8),
                "quote_source": "live_monitor_saved_history",
                "saved_record_id": event.get("saved_record_id"),
                "partial_contract": event.get("partial_contract", False),
            }
            transactions.append(transaction)
            equity_curve.append({"t": event["expiration_time"], "value": round(available_cash, 4)})

        profits = [trade["pnl"] for trade in transactions]
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
        value = LiveMonitorStrategyGenerator._float_or_none(row.get(key))
        if value is None:
            return None
        price = value / 100
        return price if 0 < price < 1 else None

    @staticmethod
    def specifications(count: int) -> list[dict]:
        families = tuple(LIVE_FAMILY_LABELS)
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
    def _description(specification: dict, asset: str, timeframe: str) -> str:
        entry_second = specification["entry_second"]
        allocation = specification["allocation_pct"]
        family = specification["family"]
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
