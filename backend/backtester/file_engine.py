from __future__ import annotations

import math
from itertools import groupby
from statistics import fmean, pstdev

from backend.fixed_time import TIMEFRAME_SECONDS, parse_utc_datetime

MIN_POSITION_BUDGET = 1.0


class FileBacktester:
    """Cash-and-margin backtester for fixed-expiration binary contracts."""

    @staticmethod
    def empty_result(initial_capital: float, timestamp: str) -> dict:
        if not math.isfinite(initial_capital) or initial_capital <= 0:
            raise ValueError("Starting balance must be a positive finite number")
        return {
            "initial_capital": round(initial_capital, 4),
            "final_capital": round(initial_capital, 4),
            "net_profit": 0.0,
            "roi": 0.0,
            "sharpe_ratio": 0.0,
            "win_rate": 0,
            "max_drawdown": 0.0,
            "stability_score": 0.0,
            "recovery_factor": 0.0,
            "profit_factor": 0,
            "average_trade": 0,
            "trade_count": 0,
            "event_count": 0,
            "contracts_used": 0,
            "up_trade_count": 0,
            "down_trade_count": 0,
            "winning_trade_count": 0,
            "losing_trade_count": 0,
            "flat_trade_count": 0,
            "skipped_contract_count": 0,
            "average_allocation_pct": 0.0,
            "max_allocation_pct_used": 0.0,
            "equity_curve": [{"t": timestamp, "value": round(initial_capital, 4)}],
            "transactions": [],
        }

    def run(self, events: list[dict], strategy: dict, initial_capital: float) -> dict:
        if not math.isfinite(initial_capital) or initial_capital <= 0:
            raise ValueError("Starting balance must be a positive finite number")
        ordered = sorted(events, key=lambda item: (item["entry_time"], item["asset"], item["id"]))
        if not ordered:
            raise ValueError("No historical fixed-time events match this group")
        for event in ordered:
            self._validate_event(event)

        available_cash = initial_capital
        open_positions: list[dict] = []
        pending_resolutions: list[dict] = []
        histories: dict[str, list[str]] = {}
        transactions = []
        equity_curve = [{"t": ordered[0]["start_time"], "value": round(initial_capital, 4)}]
        allocation_pct = float(strategy["allocation_pct"])
        max_allocation_pct = float(strategy.get("max_allocation_pct", allocation_pct))
        fee_rate = float(strategy["fee_bps"]) / 10_000
        assumed_ask_price = float(strategy.get("assumed_ask_price", 0.5))
        if not 0 < allocation_pct <= 1 or not allocation_pct <= max_allocation_pct <= 1 or fee_rate < 0:
            raise ValueError("Invalid allocation or fee parameters")
        if not math.isfinite(assumed_ask_price) or not 0 < assumed_ask_price < 1:
            raise ValueError("Invalid assumed ask price")

        def account_equity() -> float:
            return available_cash + sum(position["margin_locked"] for position in open_positions)

        def record_equity(timestamp: str) -> None:
            point = {"t": timestamp, "value": round(account_equity(), 4)}
            if equity_curve and equity_curve[-1]["t"] == timestamp:
                equity_curve[-1] = point
            else:
                equity_curve.append(point)

        def settle_due(timestamp: str, settle_all: bool = False) -> None:
            nonlocal available_cash, open_positions
            boundary = parse_utc_datetime(timestamp)
            due = [
                position for position in open_positions
                if settle_all or parse_utc_datetime(position["expiration_time"]) <= boundary
            ]
            if not due:
                return
            due_ids = {position["position_id"] for position in due}
            open_positions = [
                position for position in open_positions if position["position_id"] not in due_ids
            ]
            for position in sorted(due, key=lambda item: (item["expiration_time"], item["asset"])):
                settlement_price = (
                    1.0
                    if position["side"].lower() == position["resolved_outcome"].lower()
                    else 0.0
                )
                settlement_value = position["contracts_volume"] * settlement_price
                available_cash += settlement_value
                realized_pnl = (
                    settlement_value - position["margin_locked"] - position["entry_fee"]
                )
                transaction = {
                    **position,
                    "closed_at": position["expiration_time"],
                    "exit_price": settlement_price,
                    "settlement_value": round(settlement_value, 8),
                    "pnl": round(realized_pnl, 8),
                }
                for field in ("contracts_volume", "quantity", "margin_locked", "entry_fee"):
                    transaction[field] = round(float(transaction[field]), 8)
                transaction.pop("position_id", None)
                transactions.append(transaction)
            balance_after = round(account_equity(), 4)
            for transaction in transactions[-len(due):]:
                transaction["balance_after"] = balance_after
                transaction["available_cash_after_close"] = round(available_cash, 4)
            record_equity(max(position["expiration_time"] for position in due))

        for entry_time, grouped in groupby(ordered, key=lambda item: item["entry_time"]):
            settle_due(entry_time)
            start_boundary = parse_utc_datetime(entry_time)
            completed_events = [
                event for event in pending_resolutions
                if parse_utc_datetime(event["expiration_time"]) <= start_boundary
            ]
            completed_ids = {event["id"] for event in completed_events}
            pending_resolutions = [
                event for event in pending_resolutions if event["id"] not in completed_ids
            ]
            for event in sorted(
                completed_events, key=lambda item: (item["expiration_time"], item["asset"])
            ):
                histories.setdefault(event["asset"], []).append(event["resolved_outcome"])
            group = list(grouped)
            signals = []
            for event in group:
                analysis_outcomes = self._analysis_outcomes(
                    histories.get(event["asset"], []), strategy
                )
                prediction = self._signal(
                    strategy,
                    analysis_outcomes,
                )
                if prediction:
                    signals.append((
                        event, prediction, analysis_outcomes,
                        self._allocation_pct(strategy, analysis_outcomes),
                    ))
            if not signals or available_cash <= 0:
                pending_resolutions.extend(group)
                continue

            equity_before = account_equity()
            raw_budgets = [
                equity_before * allocation_pct_used
                for _, _, _, allocation_pct_used in signals
            ]
            total_budget = min(available_cash, sum(raw_budgets))
            budget_scale = total_budget / sum(raw_budgets) if sum(raw_budgets) else 0
            for (event, prediction, analysis_outcomes, allocation_pct_used), raw_budget in zip(signals, raw_budgets):
                budget_per_position = raw_budget * budget_scale
                if budget_per_position < MIN_POSITION_BUDGET:
                    continue
                ask_price = assumed_ask_price
                margin_locked = budget_per_position / (1 + fee_rate)
                entry_fee = budget_per_position - margin_locked
                contracts_volume = margin_locked / ask_price
                cash_before = available_cash
                available_cash -= margin_locked + entry_fee
                open_positions.append({
                    "position_id": f"{event['id']}:{prediction}",
                    "event_id": event["id"],
                    "market_id": event["market_id"],
                    "market_question": event["question"],
                    "asset": event["asset"],
                    "timeframe": event["timeframe"],
                    "side": prediction.upper(),
                    "resolved_outcome": event["resolved_outcome"].upper(),
                    "opened_at": event["entry_time"],
                    "expiration_time": event["expiration_time"],
                    "entry_price": round(ask_price, 8),
                    "ask_price": round(ask_price, 8),
                    "assumed_ask_price_cents": round(ask_price * 100, 4),
                    "allocation_pct_used": round(allocation_pct_used, 8),
                    "analysis_history_count": len(analysis_outcomes),
                    "history_skip_recent": int(strategy.get("history_skip_recent", 1)),
                    "contracts_volume": contracts_volume,
                    "quantity": contracts_volume,
                    "margin_locked": margin_locked,
                    "entry_fee": entry_fee,
                    "fee_bps": float(strategy["fee_bps"]),
                    "available_cash_before_entry": round(cash_before, 4),
                    "available_cash_after_entry": round(available_cash, 4),
                    "balance_before": round(equity_before, 4),
                    "holding_seconds": TIMEFRAME_SECONDS[event["timeframe"]],
                    "quote_source": "user_assumed_ask_price",
                })
            record_equity(entry_time)
            pending_resolutions.extend(group)

        for expiration_time, _ in groupby(
            sorted(open_positions, key=lambda item: item["expiration_time"]),
            key=lambda item: item["expiration_time"],
        ):
            settle_due(expiration_time)

        profits = [trade["pnl"] for trade in transactions]
        allocations = [float(trade.get("allocation_pct_used", 0)) for trade in transactions]
        returns = [
            trade["pnl"] / max(trade["margin_locked"] + trade["entry_fee"], 1e-9)
            for trade in transactions
        ]
        gross_profit = sum(value for value in profits if value > 0)
        gross_loss = abs(sum(value for value in profits if value < 0))
        max_drawdown = self._max_drawdown([point["value"] for point in equity_curve])
        sharpe_ratio = self._sharpe(returns)
        stability = self.stability_metrics(
            [point["value"] for point in equity_curve], initial_capital,
            available_cash, sharpe_ratio, max_drawdown,
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
            "event_count": len(ordered),
            "contracts_used": len({trade["event_id"] for trade in transactions}),
            "up_trade_count": sum(trade["side"] == "UP" for trade in transactions),
            "down_trade_count": sum(trade["side"] == "DOWN" for trade in transactions),
            "winning_trade_count": sum(value > 0 for value in profits),
            "losing_trade_count": sum(value < 0 for value in profits),
            "flat_trade_count": sum(value == 0 for value in profits),
            "skipped_contract_count": max(
                0, len(ordered) - len({trade["event_id"] for trade in transactions})
            ),
            "average_allocation_pct": round(fmean(allocations) * 100, 6) if allocations else 0.0,
            "max_allocation_pct_used": round(max(allocations) * 100, 6) if allocations else 0.0,
            "equity_curve": equity_curve,
            "transactions": transactions,
        }

    @staticmethod
    def _validate_event(event: dict) -> None:
        timeframe = event.get("timeframe")
        if timeframe not in TIMEFRAME_SECONDS:
            raise ValueError(f"Unsupported event timeframe: {timeframe}")
        required = (
            "id", "start_time", "entry_time", "expiration_time", "end_time",
        )
        missing = [field for field in required if not event.get(field)]
        if missing:
            raise ValueError(
                f"Event {event.get('id', '<unknown>')} is missing normalized fields: "
                f"{', '.join(missing)}. Refresh historical data."
            )
        start = parse_utc_datetime(event.get("start_time"))
        entry = parse_utc_datetime(event.get("entry_time"))
        expiration = parse_utc_datetime(event.get("expiration_time"))
        end = parse_utc_datetime(event.get("end_time"))
        expected_seconds = TIMEFRAME_SECONDS[timeframe]
        if entry != start:
            raise ValueError(f"Event {event['id']} entry must equal the contract start")
        if abs((expiration - start).total_seconds() - expected_seconds) > 1:
            raise ValueError(f"Event {event['id']} expiration does not match {timeframe}")
        if abs((end - expiration).total_seconds()) > 1:
            raise ValueError(f"Event {event['id']} resolution differs from expiration")
        if event.get("data_quality", {}).get("is_synthetic") is not False:
            raise ValueError(f"Event {event['id']} is not verified real market data")

    @staticmethod
    def _analysis_outcomes(outcomes: list[str], strategy: dict) -> list[str]:
        skip_recent = int(strategy.get("history_skip_recent", 1))
        if skip_recent < 0:
            raise ValueError("history_skip_recent must not be negative")
        if skip_recent == 0:
            return list(outcomes)
        return list(outcomes[:-skip_recent])

    @staticmethod
    def _signal(strategy: dict, outcomes: list[str]) -> str | None:
        family = strategy["family"]
        lookback = int(strategy["lookback"])
        min_history = int(strategy.get("min_history", lookback))
        min_edge = float(strategy["min_edge"])
        if lookback <= 0 or min_history <= 0:
            raise ValueError("Invalid lookback or min_history")
        if len(outcomes) < min_history:
            return None
        recent = outcomes[-lookback:]
        up_rate = sum(outcome == "up" for outcome in recent) / len(recent)
        edge = abs(up_rate - 0.5)
        majority = "up" if up_rate > 0.5 else "down" if up_rate < 0.5 else None
        if family == "history_momentum":
            return majority if majority and edge >= min_edge else None
        if family == "history_contrarian":
            return FileBacktester._opposite(majority) if majority and edge >= min_edge else None
        if family == "streak_momentum":
            streak = FileBacktester._tail_streak(outcomes)
            return streak[0] if streak[1] >= int(strategy.get("streak_length", 2)) else None
        if family == "streak_reversal":
            streak = FileBacktester._tail_streak(outcomes)
            return FileBacktester._opposite(streak[0]) if streak[1] >= int(strategy.get("streak_length", 2)) else None
        if family == "alternation_follow":
            return FileBacktester._opposite(recent[-1]) if FileBacktester._is_alternating(recent) else None
        if family == "alternation_fade":
            return recent[-1] if FileBacktester._is_alternating(recent) else None
        if family == "imbalance_follow":
            return majority if majority and edge >= min_edge else None
        if family == "imbalance_reversal":
            return FileBacktester._opposite(majority) if majority and edge >= min_edge else None
        if family == "lag_follow":
            return recent[-1]
        if family == "lag_fade":
            return FileBacktester._opposite(recent[-1])
        if family == "double_follow":
            tail = recent[-2:]
            return tail[-1] if len(tail) == 2 and tail[0] == tail[1] else None
        if family == "double_fade":
            tail = recent[-2:]
            return FileBacktester._opposite(tail[-1]) if len(tail) == 2 and tail[0] == tail[1] else None
        raise ValueError(f"Unsupported strategy family: {family}")

    @staticmethod
    def _allocation_pct(strategy: dict, outcomes: list[str]) -> float:
        base = float(strategy["allocation_pct"])
        maximum = float(strategy.get("max_allocation_pct", base))
        mode = strategy.get("allocation_mode", "fixed")
        if mode == "fixed" or not outcomes:
            return min(maximum, base)
        if mode == "confidence_scaled":
            lookback = max(1, int(strategy["lookback"]))
            recent = outcomes[-lookback:]
            up_rate = sum(outcome == "up" for outcome in recent) / len(recent)
            confidence = abs(up_rate - 0.5) * 2
            return min(maximum, base + (maximum - base) * confidence)
        if mode == "streak_scaled":
            streak_length = FileBacktester._tail_streak(outcomes)[1]
            target = base * (1 + max(0, streak_length - 1) * 0.25)
            return min(maximum, target)
        if mode == "depth_scaled":
            lookback = max(1, int(strategy["lookback"]))
            target = base * min(1.0, len(outcomes) / lookback)
            return min(maximum, max(base * 0.25, target))
        raise ValueError(f"Unsupported allocation mode: {mode}")

    @staticmethod
    def _tail_streak(outcomes: list[str]) -> tuple[str | None, int]:
        if not outcomes:
            return None, 0
        side = outcomes[-1]
        count = 0
        for outcome in reversed(outcomes):
            if outcome != side:
                break
            count += 1
        return side, count

    @staticmethod
    def _is_alternating(outcomes: list[str]) -> bool:
        return len(outcomes) >= 2 and all(
            current != previous for previous, current in zip(outcomes, outcomes[1:])
        )

    @staticmethod
    def _opposite(signal: str | None) -> str | None:
        return "down" if signal == "up" else "up" if signal == "down" else None

    @staticmethod
    def _sharpe(returns: list[float]) -> float:
        if len(returns) < 2:
            return 0
        deviation = pstdev(returns)
        return fmean(returns) / deviation * math.sqrt(len(returns)) if deviation else 0

    @staticmethod
    def _max_drawdown(values: list[float]) -> float:
        peak = drawdown = 0.0
        for value in values:
            peak = max(peak, value)
            if peak:
                drawdown = max(drawdown, (peak - value) / peak * 100)
        return drawdown

    @staticmethod
    def stability_metrics(
        values: list[float],
        initial_capital: float,
        final_capital: float,
        sharpe_ratio: float,
        max_drawdown: float,
    ) -> dict[str, float]:
        net_profit = final_capital - initial_capital
        drawdown_amount = initial_capital * max_drawdown / 100
        recovery_factor = (
            net_profit / drawdown_amount if drawdown_amount > 1e-12
            else 999.0 if net_profit > 0 else 0.0
        )
        smoothness = 0.0
        if net_profit > 0 and len(values) >= 2:
            expected = [
                values[0] + (values[-1] - values[0]) * index / (len(values) - 1)
                for index in range(len(values))
            ]
            mean_value = fmean(values)
            total_variance = sum((value - mean_value) ** 2 for value in values)
            residual = sum(
                (value - target) ** 2 for value, target in zip(values, expected)
            )
            if total_variance > 1e-12:
                smoothness = max(0.0, min(1.0, 1 - residual / total_variance))
        drawdown_component = 1 / (1 + max_drawdown / 10)
        sharpe_component = max(0.0, min(1.0, sharpe_ratio / 3))
        stability_score = (
            100 * (0.45 * smoothness + 0.30 * drawdown_component + 0.25 * sharpe_component)
            if net_profit > 0 else 0.0
        )
        return {
            "stability_score": round(stability_score, 6),
            "recovery_factor": round(max(-999.0, min(999.0, recovery_factor)), 6),
        }
