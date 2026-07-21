from __future__ import annotations

import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PolymarketLiveOrderRequest:
    token_id: str
    amount: float
    side: str = "BUY"
    order_type: str = "FAK"
    price: float | None = None


class PolymarketLiveOrderExecutor:
    def __init__(self) -> None:
        self._client = None
        self._prepared_lock = threading.Lock()
        self._prepared_orders: dict[tuple[str, str, float, str, float], dict[str, Any]] = {}

    def prepare(self) -> None:
        self.validate_ready()
        self._get_client()
        from py_clob_client_v2 import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import BUY, SELL

        _ = (MarketOrderArgs, OrderType, BUY, SELL)

    def prepare_tokens(self, token_ids: list[str]) -> None:
        self.prepare()
        client = self._get_client()
        try:
            client.get_version()
        except Exception:
            logger.exception("Polymarket CLOB version warmup failed")
        for token_id in token_ids:
            token = str(token_id or "").strip()
            if not token:
                continue
            try:
                client.get_tick_size(token)
            except Exception:
                logger.exception("Polymarket CLOB market warmup failed token_id=%s", token)

    def prepare_market_order(self, request: PolymarketLiveOrderRequest) -> None:
        token_id = request.token_id.strip()
        side = str(request.side or "BUY").upper()
        amount = float(request.amount)
        order_type = str(request.order_type or "FAK").upper()
        price = self._aggressive_limit_price(side, request.price)
        prepared = self._create_prepared_market_order(token_id, amount, side, order_type, price)
        with self._prepared_lock:
            self._prepared_orders[self._prepared_order_key(token_id, side, amount, order_type, price)] = prepared

    def validate_ready(self) -> None:
        if not self._live_trading_enabled():
            raise RuntimeError("Real trading is disabled. Set POLYMARKET_LIVE_TRADING_ENABLED=true.")
        if not self._private_key():
            raise RuntimeError("PRIVATE_KEY or POLYMARKET_PRIVATE_KEY is required for real trading.")
        if not self._funder():
            raise RuntimeError("PROXY_ADDRESS, POLYMARKET_PROXY_ADDRESS, or POLYMARKET_FUNDER is required for real trading.")
        api_values = self._api_credentials()
        if any(api_values) and not all(api_values):
            raise RuntimeError("API_KEY, API_SECRET, and API_PASSPHRASE must be set together.")

    def market_buy(self, request: PolymarketLiveOrderRequest) -> dict[str, Any]:
        self.validate_ready()
        token_id = request.token_id.strip()
        if not token_id:
            raise ValueError("Polymarket token_id is required.")
        amount = float(request.amount)
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("Order amount must be greater than zero.")
        return self._market_order(token_id, amount, "BUY", request.order_type, request.price)

    def market_sell(self, request: PolymarketLiveOrderRequest) -> dict[str, Any]:
        self.validate_ready()
        token_id = request.token_id.strip()
        if not token_id:
            raise ValueError("Polymarket token_id is required.")
        amount = float(request.amount)
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("Sell amount must be greater than zero.")
        return self._market_order(token_id, amount, "SELL", request.order_type, request.price)

    def token_balance(self, token_id: str) -> float:
        return float(self.token_balance_info(token_id)["balance"])

    def token_balance_info(self, token_id: str) -> dict[str, Any]:
        self.validate_ready()
        token_id = token_id.strip()
        if not token_id:
            raise ValueError("Polymarket token_id is required.")
        from py_clob_client_v2 import AssetType, BalanceAllowanceParams

        response = self._get_client().get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=self._signature_type(),
            )
        )
        raw_balance = self._balance_value_from_response(response)
        try:
            balance = max(0.0, float(raw_balance) / 1_000_000)
        except (TypeError, ValueError):
            raise RuntimeError(
                f"Could not read Polymarket token balance for {token_id}: {response!r}"
            )
        return {
            "balance": balance,
            "raw_balance": raw_balance,
            "response": response,
            "source": "get_balance_allowance",
        }

    @staticmethod
    def _balance_value_from_response(response) -> Any:
        if not isinstance(response, dict):
            return None
        direct = response.get("balance")
        if direct is not None:
            return direct
        for key in ("data", "result"):
            nested = response.get(key)
            if isinstance(nested, dict) and nested.get("balance") is not None:
                return nested.get("balance")
        return None

    def _market_order(
        self,
        token_id: str,
        amount: float,
        side: str,
        order_type: str,
        price: float | None,
    ) -> dict[str, Any]:
        client = self._get_client()
        limit_price = self._aggressive_limit_price(side, price)
        prepared_key = self._prepared_order_key(token_id, side, amount, order_type, limit_price)
        with self._prepared_lock:
            prepared = self._prepared_orders.pop(prepared_key, None)
        try:
            from py_clob_client_v2 import MarketOrderArgs, OrderType
            from py_clob_client_v2.order_builder.constants import BUY, SELL

            if prepared:
                signed = prepared["signed"]
                signed_meta = {**prepared["signed_meta"], "prepared_order_used": True}
                clob_order_type = prepared["clob_order_type"]
            else:
                order_side = BUY if side == "BUY" else SELL
                clob_order_type = self._clob_market_order_type(OrderType, order_type)
                order_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=round(amount, 8),
                    side=order_side,
                    price=limit_price,
                    order_type=clob_order_type,
                )
                signed = client.create_market_order(order_args)
                signed_meta = {**self._signed_order_metadata(signed, side), "prepared_order_used": False}

            send_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            started = time.perf_counter()
            logger.info(
                "Polymarket CLOB order send start send_at=%s side=%s token_id=%s amount=%.8f price=%.4f order_type=%s",
                send_at,
                side,
                token_id,
                amount,
                limit_price,
                clob_order_type,
            )
            try:
                response = client.post_order(signed, clob_order_type)
            except Exception:
                duration_ms = (time.perf_counter() - started) * 1000
                logger.exception(
                    "Polymarket CLOB order send failed send_at=%s duration_ms=%.2f side=%s token_id=%s amount=%.8f price=%.4f order_type=%s",
                    send_at,
                    duration_ms,
                    side,
                    token_id,
                    amount,
                    limit_price,
                    clob_order_type,
                )
                raise
            duration_ms = (time.perf_counter() - started) * 1000
            logger.info(
                "Polymarket CLOB order send complete send_at=%s duration_ms=%.2f side=%s token_id=%s amount=%.8f price=%.4f order_type=%s",
                send_at,
                duration_ms,
                side,
                token_id,
                amount,
                limit_price,
                clob_order_type,
            )
            return self._response_with_timing(
                response,
                send_at,
                duration_ms,
                side,
                token_id,
                amount,
                limit_price,
                clob_order_type,
                signed_meta,
            )
        except TypeError:
            from py_clob_client_v2 import MarketOrderArgs, OrderType
            from py_clob_client_v2.order_builder.constants import BUY, SELL

            order_side = BUY if side == "BUY" else SELL
            clob_order_type = self._clob_market_order_type(OrderType, order_type)

            signed = client.create_market_order(
                MarketOrderArgs(
                    token_id=token_id,
                    amount=round(amount, 8),
                    side=order_side,
                    price=limit_price,
                    order_type=clob_order_type,
                )
            )
            signed_meta = self._signed_order_metadata(signed, side)
            send_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            started = time.perf_counter()
            logger.info(
                "Polymarket CLOB legacy order send start send_at=%s side=%s token_id=%s amount=%.8f price=%.4f order_type=%s",
                send_at,
                side,
                token_id,
                amount,
                limit_price,
                clob_order_type,
            )
            try:
                response = client.post_order(signed, clob_order_type)
            except Exception:
                duration_ms = (time.perf_counter() - started) * 1000
                logger.exception(
                    "Polymarket CLOB legacy order send failed send_at=%s duration_ms=%.2f side=%s token_id=%s amount=%.8f price=%.4f order_type=%s",
                    send_at,
                    duration_ms,
                    side,
                    token_id,
                    amount,
                    limit_price,
                    clob_order_type,
                )
                raise
            duration_ms = (time.perf_counter() - started) * 1000
            logger.info(
                "Polymarket CLOB legacy order send complete send_at=%s duration_ms=%.2f side=%s token_id=%s amount=%.8f price=%.4f order_type=%s",
                send_at,
                duration_ms,
                side,
                token_id,
                amount,
                limit_price,
                clob_order_type,
            )
            return self._response_with_timing(
                response,
                send_at,
                duration_ms,
                side,
                token_id,
                amount,
                limit_price,
                clob_order_type,
                signed_meta,
            )

    def _create_prepared_market_order(
        self,
        token_id: str,
        amount: float,
        side: str,
        order_type: str,
        price: float,
    ) -> dict[str, Any]:
        client = self._get_client()
        from py_clob_client_v2 import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import BUY, SELL

        clob_order_type = self._clob_market_order_type(OrderType, order_type)
        signed = client.create_market_order(
            MarketOrderArgs(
                token_id=token_id,
                amount=round(amount, 8),
                side=BUY if side == "BUY" else SELL,
                price=price,
                order_type=clob_order_type,
            )
        )
        return {
            "signed": signed,
            "signed_meta": self._signed_order_metadata(signed, side),
            "clob_order_type": clob_order_type,
        }

    @staticmethod
    def _prepared_order_key(
        token_id: str,
        side: str,
        amount: float,
        order_type: str,
        price: float,
    ) -> tuple[str, str, float, str, float]:
        return (
            str(token_id).strip(),
            str(side or "").upper(),
            round(float(amount), 8),
            str(order_type or "").upper(),
            round(float(price), 4),
        )

    @staticmethod
    def _response_with_timing(
        response,
        send_at: str,
        duration_ms: float,
        side: str,
        token_id: str,
        amount: float,
        price: float,
        order_type: str,
        signed_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = dict(response or {})
        payload["_timing"] = {
            "send_at": send_at,
            "duration_ms": round(duration_ms, 2),
            "side": side,
            "token_id": token_id,
            "amount": round(amount, 8),
            "price": round(price, 4),
            "order_type": str(order_type),
            "clob_status": str(payload.get("status") or ""),
            **(signed_meta or {}),
        }
        return payload

    @staticmethod
    def _signed_order_metadata(order, side: str) -> dict[str, Any]:
        def raw_int(name: str) -> int | None:
            value = getattr(order, name, None)
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        maker_amount = raw_int("makerAmount")
        taker_amount = raw_int("takerAmount")
        raw_token_quantity = taker_amount if side == "BUY" else maker_amount
        token_quantity = (
            math.floor((raw_token_quantity / 1_000_000) * 1_000_000) / 1_000_000
            if raw_token_quantity is not None and raw_token_quantity > 0
            else None
        )
        return {
            "signed_maker_amount": maker_amount,
            "signed_taker_amount": taker_amount,
            "signed_token_quantity": token_quantity,
        }

    @staticmethod
    def _aggressive_limit_price(side: str, price: float | None) -> float:
        value = float(price) if price is not None else (0.99 if side == "BUY" else 0.01)
        if not math.isfinite(value) or not 0 < value < 1:
            raise ValueError("Aggressive limit price must be between 0 and 1.")
        return round(value, 4)

    @staticmethod
    def _clob_market_order_type(order_type_class, order_type: str):
        normalized = str(order_type or "FAK").strip().upper()
        if normalized not in {"FOK", "FAK"}:
            raise ValueError("Market order_type must be FOK or FAK.")
        return getattr(order_type_class, normalized)

    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            from py_clob_client_v2 import ClobClient
            from py_clob_client_v2.clob_types import ApiCreds
        except ImportError as error:
            raise RuntimeError(
                "py-clob-client-v2 is not installed. Run pip install -r requirements.txt."
            ) from error
        api_key, api_secret, api_passphrase = self._api_credentials()
        client = ClobClient(
            settings.polymarket_clob_url,
            key=self._private_key(),
            chain_id=settings.polymarket_chain_id,
            signature_type=self._signature_type(),
            funder=self._funder(),
            creds=(
                ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                )
                if api_key and api_secret and api_passphrase
                else None
            ),
        )
        if not (api_key and api_secret and api_passphrase):
            client.set_api_creds(client.create_or_derive_api_key())
        self._client = client
        return client

    @staticmethod
    def _env_or_setting(env_name: str, setting_name: str) -> str:
        return str(os.getenv(env_name) or getattr(settings, setting_name, "") or "").strip()

    @classmethod
    def _private_key(cls) -> str:
        return cls._env_or_setting("PRIVATE_KEY", "polymarket_private_key")

    @classmethod
    def _funder(cls) -> str:
        return (
            cls._env_or_setting("PROXY_ADDRESS", "polymarket_proxy_address")
            or str(settings.polymarket_funder or "").strip()
        )

    @classmethod
    def _api_credentials(cls) -> tuple[str, str, str]:
        return (
            cls._env_or_setting("API_KEY", "polymarket_api_key"),
            cls._env_or_setting("API_SECRET", "polymarket_api_secret"),
            cls._env_or_setting("API_PASSPHRASE", "polymarket_api_passphrase"),
        )

    @staticmethod
    def _signature_type() -> int:
        value = os.getenv("SIGNATURE_TYPE") or settings.polymarket_signature_type
        return int(value)

    @staticmethod
    def _live_trading_enabled() -> bool:
        value = os.getenv("LIVE_TRADING_ENABLED") or os.getenv("POLYMARKET_LIVE_TRADING_ENABLED")
        if value is not None:
            return str(value).strip().lower() in {"1", "true", "yes", "on"}
        return bool(settings.polymarket_live_trading_enabled)
