# Paper Real Trading Latency Handoff

Compact index for the next AI agent. The goal is to avoid re-discovering the
same code paths when debugging automatic Paper Trading real-order latency.

## User-Facing Problem

The project is deployed on Fly and automatic Paper Trading can send real
Polymarket CLOB orders. Locally, the time between tick and signal can be around
`22ms`, but on the Fly server the user observed much larger delays:

```text
13:20:32.450 [paper] PAPER SIGNAL OPEN UP amount 2.000000 token 25631311...124740 signal lag 2450.00ms
13:20:33.493 [clob] CLOB SEND OPEN UP submitted amount 2.000000 @ 0.99 token 25631311...124740 delay 1043.00ms 637.35ms clob_status matched
13:21:35.979 [paper] PAPER SIGNAL CLOSE UP amount 2.941175 token 25631311...124740 signal lag 7479.00ms
13:21:35.979 [clob] CLOB SEND CLOSE UP submitted amount 2.941175 @ 0.01 token 25631311...124740 delay 0.00ms 792.16ms clob_status matched
```

Interpretation:

- `PAPER SIGNAL ...` is the backend-created paper/real signal record displayed
  by the browser.
- `CLOB SEND ... delay N ms` is frontend display of
  `order.signal_to_clob_ms`, computed from backend timestamps.
- The final number such as `637.35ms` / `792.16ms` is CLOB request duration from
  `PolymarketLiveOrderExecutor._market_order`.
- A previous experiment added `signal lag ...ms` to measure tick-to-signal
  decision latency, but the user said it did not improve the situation and asked
  to roll it back. That experiment is currently reverted.

## Current Deployment Context

- Fly app: `polymarket-backtester`
- Public URL: `https://polymarket-backtester.fly.dev/login`
- Region: `arn` Stockholm / Sweden
- Machine observed after rollback: `287e605a591578`
- Health endpoint: `/healthz`
- `fly.toml` contains `LIVE_MONITOR_STREAM_MIN_INTERVAL_SECONDS = "0.5"`.
- `POLYMARKET_LIVE_ORDER_AMOUNT` is intentionally not hardcoded in `fly.toml`;
  it should come from Fly secrets / environment.

Useful commands:

```powershell
curl.exe -sS -L https://polymarket-backtester.fly.dev/healthz
& "$env:USERPROFILE\.fly\bin\flyctl.exe" status --app polymarket-backtester
& "$env:USERPROFILE\.fly\bin\flyctl.exe" deploy --app polymarket-backtester --remote-only
```

## Do Not Re-Do These Unless Asked

- Do not re-add the rolled-back `signal lag` implementation as a fix. It was
  only instrumentation and did not solve the user's server latency.
- Do not re-cap real order amount at `$1`. That bug was fixed: orders should use
  `POLYMARKET_LIVE_ORDER_AMOUNT`.
- Do not add extra balance checks before close. The user explicitly wanted close
  to use saved open quantity.
- Do not add balance fetches at close time.
- Do not add verification loops unless the user explicitly accepts extra delay.

## Important Existing Behavior

- Open order amount comes from `_paper_real_order_amount()`.
- `_paper_real_order_amount()` reads `settings.polymarket_live_order_amount`,
  falls back to `1.0` only if missing/invalid, then rounds to 2 decimals.
- BUY/open uses aggressive limit price `0.99`.
- SELL/close uses aggressive limit price `0.01`.
- Order type is currently `FAK`.
- After successful open, the code fetches token balance once and stores
  `real_open_quantity`.
- Close sells the saved quantity from the open record, not a newly fetched
  balance.

## Main Backend Map

### `backend/api/routes.py`

This is the central file for Paper Trading live preview and automatic real
orders.

Key functions:

- `_paper_live_trade_preview(...)` near line 2656
  - Builds paper trades from the current live snapshot.
  - Eventually calls `_execute_paper_real_trading_signals(...)`.
  - If server tick-to-signal delay is high, inspect this function and the work
    before the signal is persisted.

- `_paper_real_order_amount()` near line 2968
  - Reads `settings.polymarket_live_order_amount`.
  - No longer caps amount at `$1`.

- `_paper_aggressive_limit_price(action)` near line 2979
  - BUY -> `0.99`
  - SELL -> `0.01`

- `_warm_paper_live_order_tokens(...)` near line 3071
  - Warms CLOB client/token/order data.
  - In the live SSE route, when real trading is enabled, this is awaited via
    `asyncio.to_thread(...)` before preview calculation. This may be a source of
    blocking delay on the event stream.

- `_execute_paper_real_trading_signals(...)` near line 3495
  - Main automatic real-order function.
  - Creates deterministic real order ids.
  - Sends open/close CLOB orders.
  - Stores timing fields such as `paper_signal_at`, `clob_send_at`,
    `signal_to_clob_ms`, and executor `_timing`.

- Live SSE / streaming route around lines 4002-4031
  - Gets `snapshot = session.snapshot(history_limit=None)`.
  - Builds `cache_key`.
  - If `real_trading_enabled`:
    - awaits `_warm_paper_live_order_tokens(...)`;
    - then awaits `_paper_live_trade_preview(..., persist=False)`;
    - stores preview in cache.
  - This synchronous real-trading branch is the highest-value place to inspect
    for server-only delays.

### `backend/trading/polymarket_executor.py`

Key class: `PolymarketLiveOrderExecutor`

Important methods:

- `prepare_tokens(...)`
  - Calls `client.get_version()` and `client.get_tick_size(token)`.

- `prepare_market_order(...)`
  - Pre-creates signed market orders and stores them in `_prepared_orders`.

- `market_buy(...)` / `market_sell(...)`
  - Validate and call `_market_order(...)`.

- `_market_order(...)` near line 141
  - Uses prepared signed order if available.
  - Otherwise creates order with `client.create_market_order(...)`.
  - Measures only `client.post_order(...)` duration as `_timing.duration_ms`.
  - Returns response with `_timing.send_at`, `duration_ms`, `price`,
    `order_type`, etc.

If `CLOB SEND ... 700ms` is the issue, inspect network/client behavior here.
If `PAPER SIGNAL` appears seconds after the tick, inspect upstream route and
strategy generation instead.

## Frontend Map

### `frontend/static/js/app.js`

Key function:

- `renderRealOrderLogs(...)` around line 1080
  - Displays `[paper] PAPER SIGNAL ...`.
  - Displays `[clob] CLOB SEND ... delay ... duration ... clob_status ...`.
  - It only displays backend timestamps; it is not the source of backend order
    latency.

Current rolled-back state:

- There should be no `signal lag`, `signal_decision_ms`, or
  `_paper_signal_decision_ms` in this file or `backend/api/routes.py`.

Quick check:

```powershell
rg -n "signal lag|signal_decision_ms|_paper_signal_decision_ms" backend frontend
```

No matches are expected.

## Live Monitor / Streaming

### `backend/live_monitor.py`

Relevant setting:

- `LIVE_MONITOR_STREAM_MIN_INTERVAL_SECONDS`
  - Currently configured in `fly.toml` as `0.5`.
  - This can throttle stream updates to the frontend, but be careful: reducing
    frontend stream cadence may not fix real order execution if order decisions
    are tied to the same streamed preview path.

## Tests

Useful tests for the amount/order behavior:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_live_order_amount.py tests\test_file_architecture.py -k "paper or real_order or live"
```

Previously passed after rollback:

```text
40 passed, 39 deselected
```

Also useful:

```powershell
.\.venv\Scripts\python.exe -m py_compile backend\api\routes.py
node --check frontend\static\js\app.js
```

## Recommended Next Debugging Plan

Start by separating three different delays:

1. Tick received -> strategy decision/paper signal created.
2. Paper signal created -> CLOB send start.
3. CLOB send start -> CLOB response/match.

Recommended low-risk instrumentation:

- Add server-side log timestamps around:
  - `session.snapshot(history_limit=None)`;
  - `_warm_paper_live_order_tokens(...)`;
  - `_paper_live_trade_preview(...)`;
  - strategy trade generation inside `_paper_live_trade_preview(...)`;
  - `_execute_paper_real_trading_signals(...)`;
  - `PolymarketLiveOrderExecutor.prepare_tokens(...)`;
  - `prepare_market_order(...)`;
  - `_market_order(...)`.

Do not make the instrumentation itself part of the user-visible frontend unless
the user asks. Prefer backend logs first.

Likely high-value hypothesis:

- On Fly, the SSE real-trading branch may wait for warmup and full preview work
  before the paper signal becomes visible/executable. If real order execution is
  effectively coupled to preview rendering/streaming cadence, server load or
  network calls in warmup can create seconds of delay.

Potential direction to investigate, not yet proven:

- Decouple real order decision/execution from the frontend preview/SSE render
  path. Let the monitor/tick path execute real-order decisions immediately, then
  let SSE only display already-recorded results.

## Git / Change State Note

At the time this handoff was written, the repository may contain uncommitted
deployment and trading changes from the Render -> Fly migration and related
fixes. Do not assume `git status` is clean. Inspect before editing:

```powershell
git status --short
```
