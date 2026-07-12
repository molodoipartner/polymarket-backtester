# Paper Trading Real Orders AI Index

This file is a compact map for debugging Paper Trading real-order execution.
Use it to avoid re-scanning the whole project.

## Current Problem

Automatic Paper Trading signals still open/close too slowly or incorrectly.
The site log can show something like:

```text
17:40:12.545 [paper] PAPER SIGNAL OPEN DOWN amount 1.000000 token ...
17:40:14.655 [clob] CLOB SEND OPEN DOWN submitted amount 1.000000 @ 0.99 token ... delay 2110.00ms 1047.90ms
```

Important interpretation:

- `delay 2110.00ms` is backend delay between detecting the paper signal and reaching the CLOB order send start.
- `1047.90ms` is the CLOB/client order call duration.
- The desired target is very small signal-to-CLOB delay, ideally tens of milliseconds.

The latest attempted fix moved real order record writes after CLOB order execution, but the user reports the delay/problem remains.
So the next investigation should look earlier in the paper preview/signal path, not only inside `_execute_paper_real_trading_signals`.

## User Rules / Requirements

- Do not add extra balance checks before closing.
- Do not fetch balance at close time.
- After successful open, do exactly one token balance request and save that quantity.
- Close must use the saved `real_open_quantity`.
- No fallback balance lookup on close.
- Do not add extra verification loops unless explicitly asked.
- Opening and closing should use aggressive orders:
  - BUY uses limit price `0.99`.
  - SELL uses limit price `0.01`.
  - Order type currently is `FAK`.
- UP and DOWN are different Polymarket token IDs:
  - UP must use outcome `Up`.
  - DOWN must use outcome `Down`.
  - Close must sell the same saved `token_id` from the open record.
- The website live console must show:
  - exact paper signal time with milliseconds;
  - exact CLOB send start time with milliseconds;
  - delay between those two events.

## Main Backend Files

### `backend/api/routes.py`

This is the main file for Paper Trading live preview and automatic real-order execution.

Key functions:

- `_paper_live_trade_preview(...)` around line 2652
  - Builds live paper trades from the current snapshot.
  - Calls `_execute_paper_real_trading_signals(...)`.
  - If delay remains high, inspect work done before that call.

- `_paper_real_order_id(...)` around line 2919
  - Deterministic ID for open real orders.
  - Used for deduplication.

- `_paper_real_close_order_id(...)` near `_paper_real_order_id`
  - Deterministic ID for close real orders.

- `_paper_token_id_for_side(snapshot, side)` around line 2952
  - Maps `UP` to outcome `Up`, `DOWN` to outcome `Down`.
  - Falls back to first token for Up, second token for Down if outcomes are missing.

- `_paper_real_order_amount()`
  - Returns configured real order size, capped at `$1.00`.

- `_paper_real_order_type()`
  - Currently returns `"FAK"`.

- `_paper_aggressive_limit_price(action)`
  - BUY -> `0.99`
  - SELL -> `0.01`

- `_paper_clock_ms()` around line 2979
  - Generates ISO timestamp with milliseconds for paper/CLOB timing.

- `_paper_signal_to_clob_ms(...)`
  - Computes signal-to-CLOB delay.

- `_execute_paper_real_trading_signals(...)` around line 3356
  - Main automatic real trading function.
  - For open:
    - checks existing real order for dedupe;
    - records `paper_signal_at`;
    - sends `market_buy`;
    - after successful open, calls `token_balance(token_id)` once;
    - stores `real_open_quantity`, timing, response.
  - For close:
    - loads open record;
    - reads saved `real_open_quantity`;
    - sends `market_sell`;
    - does not call `token_balance`.

Manual trading helpers are also in this file:

- `_paper_manual_open_transaction(...)`
- `_paper_manual_close_transaction(...)`

Manual flow is useful for comparison, but the reported issue is automatic Paper Trading signal execution.

## CLOB Executor

### `backend/trading/polymarket_executor.py`

Key class:

- `PolymarketLiveOrderExecutor` around line 25

Key methods:

- `market_buy(...)`
- `market_sell(...)`
- `token_balance(...)`
- `_market_order(...)` around line 82

Important behavior:

- `_market_order(...)` logs CLOB send start and complete.
- Response includes `_timing`:
  - `send_at`
  - `duration_ms`
  - `side`
  - `token_id`
  - `amount`
  - `price`
  - `order_type`

The CLOB duration shown in the website log comes from this `_timing.duration_ms`.
If duration is around 1000ms, inspect py-clob-client behavior and any pre-order market metadata requests.

## Frontend Live Console

### `frontend/static/js/app.js`

Key locations:

- `paperRealOrderLogKeys` around line 742
  - Dedupes log rows in the browser.

- `renderRealOrderLogs(...)` around line 1070
  - Renders `[paper] PAPER SIGNAL ...`
  - Renders `[clob] CLOB SEND ... delay ...`

- `render(...)` around line 1123
  - Calls `renderRealOrderLogs(payload, controller)`.

The frontend only displays backend timestamps. It does not measure the actual backend delay itself.

## Strategy Involved

### `backend/strategy_generator/live_monitor_generator.py`

Relevant strategy family:

- `price_threshold_70_stop_40`
- Label: `Price Threshold 70 Stop 40`

Key locations:

- Family label/registration near lines 38-49.
- Trade generation references around lines 1169-1170.
- `_price_threshold_70_stop_40_trades(...)` around line 1349.
- More family-specific handling around lines 2212, 2343, 2469.

If signal detection itself is late, inspect this strategy path and the live preview path before real-order execution.

## Tests

### `tests/test_file_architecture.py`

Relevant test:

- `test_paper_real_trading_executes_signal_once(...)` around line 1524

This test checks:

- Auto real order opens once.
- UP uses `up-token`.
- DOWN uses `down-token`.
- BUY uses `FAK` and `0.99`.
- SELL uses `FAK` and `0.01`.
- Close quantity comes from saved `real_open_quantity`.
- Close does not trigger another balance check.

Useful command:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_file_architecture.py::test_paper_real_trading_executes_signal_once
```

Manual comparison test:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_file_architecture.py::test_manual_paper_trade_open_and_close_updates_same_transaction
```

## Likely Next Debugging Direction

If `delay` is still around 2 seconds after moving real order storage after CLOB, likely causes are upstream of `_execute_paper_real_trading_signals(...)`, for example:

- paper trades are generated late in `_paper_live_trade_preview(...)`;
- live snapshot/polling cadence is slow;
- frontend receives the signal only after backend work has already happened;
- strategy generation or preview calculation is expensive;
- file/storage work before `_execute_paper_real_trading_signals(...)` still blocks the signal path;
- real order execution is called only after a slower periodic refresh instead of immediately when a live tick creates the signal.

Recommended next instrumentation:

- Add timing around `_paper_live_trade_preview(...)`.
- Add timing immediately before and after strategy trade generation.
- Add timing immediately before `_execute_paper_real_trading_signals(...)`.
- Compare those times with `paper_signal_at` and executor `_timing.send_at`.

Do not add extra balance checks or close verification while investigating latency.

