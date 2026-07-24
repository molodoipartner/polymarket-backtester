from types import SimpleNamespace

import pytest

from backend.api import routes
from backend.trading.polymarket_executor import (
    PolymarketLiveOrderExecutor,
    PolymarketLiveOrderRequest,
)
from config.settings import settings


def test_paper_real_order_amount_uses_configured_value_without_one_dollar_cap(monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_order_amount", 7.5)

    assert routes._paper_real_order_amount() == 7.5


def test_paper_real_order_amount_falls_back_to_one_for_invalid_values(monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_order_amount", 0)

    assert routes._paper_real_order_amount() == 1.0


def test_paper_real_order_amount_prefers_run_value_over_global_setting(monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_order_amount", 2.0)

    assert routes._paper_real_order_amount(7.25) == 7.25


def test_market_buy_allows_configured_amounts_above_one_dollar(monkeypatch):
    executor = PolymarketLiveOrderExecutor()
    captured = {}

    monkeypatch.setattr(executor, "validate_ready", lambda: None)

    def fake_market_order(token_id, amount, side, order_type, price):
        captured.update(
            token_id=token_id,
            amount=amount,
            side=side,
            order_type=order_type,
            price=price,
        )
        return {"status": "submitted"}

    monkeypatch.setattr(executor, "_market_order", fake_market_order)

    response = executor.market_buy(
        PolymarketLiveOrderRequest(
            token_id="token-1",
            amount=7.5,
            order_type="FAK",
            price=0.99,
        )
    )

    assert response == {"status": "submitted"}
    assert captured == {
        "token_id": "token-1",
        "amount": 7.5,
        "side": "BUY",
        "order_type": "FAK",
        "price": 0.99,
    }


def test_collateral_balance_uses_usdc_asset_without_token_id(monkeypatch):
    executor = PolymarketLiveOrderExecutor()
    captured = {}

    class FakeClient:
        def get_balance_allowance(self, params):
            captured["asset_type"] = params.asset_type
            captured["token_id"] = params.token_id
            return {"balance": "135500000"}

    monkeypatch.setattr(executor, "validate_ready", lambda: None)
    monkeypatch.setattr(executor, "_get_client", lambda: FakeClient())

    info = executor.collateral_balance_info()

    assert captured == {"asset_type": "COLLATERAL", "token_id": None}
    assert info["balance"] == pytest.approx(135.5)
    assert info["raw_balance"] == "135500000"


def test_market_order_type_error_does_not_resubmit(monkeypatch):
    executor = PolymarketLiveOrderExecutor()

    class FakeOrderType:
        FAK = "FAK"
        FOK = "FOK"

    class FakeMarketOrderArgs:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class FakeClient:
        def __init__(self):
            self.create_calls = 0
            self.post_calls = 0

        def create_market_order(self, _order_args):
            self.create_calls += 1
            return SimpleNamespace(makerAmount="1000000", takerAmount="1250000")

        def post_order(self, _signed, _order_type):
            self.post_calls += 1
            raise TypeError("response decoding failed after send")

    fake_client = FakeClient()
    monkeypatch.setattr(executor, "validate_ready", lambda: None)
    monkeypatch.setattr(executor, "_get_client", lambda: fake_client)
    monkeypatch.setitem(
        __import__("sys").modules,
        "py_clob_client_v2",
        SimpleNamespace(MarketOrderArgs=FakeMarketOrderArgs, OrderType=FakeOrderType),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "py_clob_client_v2.order_builder.constants",
        SimpleNamespace(BUY="BUY", SELL="SELL"),
    )

    with pytest.raises(TypeError, match="response decoding failed after send"):
        executor.market_buy(
            PolymarketLiveOrderRequest(
                token_id="token-1",
                amount=1.0,
                order_type="FAK",
                price=0.99,
            )
        )

    assert fake_client.create_calls == 1
    assert fake_client.post_calls == 1
