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
