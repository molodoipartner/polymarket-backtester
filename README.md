# Polymarket Strategy Group Backtester

FastAPI application for generating groups of fixed-expiration Polymarket Up/Down strategies.

## Production architecture

- Authentication state is stored server-side in `server_data/sessions/`. The browser receives only a signed, random, `HttpOnly`, `SameSite=Lax` session cookie.
- The session identifier rotates after login. Session files expire according to `SESSION_MAX_AGE_SECONDS`.
- JSON/JSONL reads and writes use a reentrant thread lock plus a portable OS file lock. Multiple Uvicorn workers on one server can safely share `server_data/`.
- User bundles are validated as finite JSON before persistence and written through unique temporary files followed by atomic replacement.
- `TrustedHostMiddleware` restricts request hosts through `ALLOWED_HOSTS`.

For multiple application servers, move sessions and persistent data to a shared database/Redis service. The bundled file backend targets concurrent workers on one server.

## Bootstrap accounts

At startup the test account from `USERNAME`/`PASSWORD` is created or synchronized automatically. A username without `@` receives an internal `{username}@local.test` email. The administrator uses `ADMIN_USERNAME`/`ADMIN_PASSWORD` directly from the environment and requires no database setup.

The protected `/admin` page displays users, group/strategy/transaction totals, recent user and system logs, active sessions, uptime, environment, stored data size, and historical event count.
Administrators can update a user's display name and balance or permanently delete the user and all owned data. Deletion also revokes that user's active sessions. The environment-managed bootstrap account is protected from deletion.

## Execution model

Historical inputs come from Polymarket Gamma closed event metadata only. The downloader stores the official contract window and resolved `UP`/`DOWN` outcome. It does not load historical trades, historical order books, or any entry price from Polymarket.

Before generating a group, the user provides one assumed Ask price in cents. Every simulated UP or DOWN position opens exactly at the official contract start using that assumed price. Historical data is used only after a contract has expired, so strategy signals cannot look into the future.

Every strategy receives outcome history with the most recent resolved contract deliberately removed. For example, if three contracts have already resolved as `UP, DOWN, UP`, the strategy analyzes only `UP, DOWN` before deciding whether to trade the next contract. Strategies can still skip a contract when their filters do not have enough older history or no clear signal.

Every event must satisfy:

```text
entry_time = official contract start
expiration_time = entry_time + selected timeframe
resolution_time = expiration_time
```

At entry:

```text
position budget = min(available cash, account equity * allocation)
locked margin = position budget / (1 + fee rate)
entry fee = position budget - locked margin
contracts volume = locked margin / user-defined assumed Ask
available cash -= locked margin + entry fee
```

At expiration:

```text
settlement price = 1 for a correct side, otherwise 0
settlement value = contracts volume * settlement price
available cash += settlement value
PnL = settlement value - locked margin - entry fee
```

There are no manual exits, stop-losses, or take-profits. Positions close only at their exact fixed expiration. Transaction records include the assumed Ask, contracts volume, locked margin, cash before/after entry, settlement value, holding seconds, expiration, PnL, and final balance.

## Generation progress

The group form starts a background job and subscribes to `/api/group-jobs/{id}/events` with Server-Sent Events. Progress state is stored in `server_data/jobs/`, so an SSE request handled by another worker can read the same job. The stream reports data loading, event validation and saved count, every generated strategy, final balance calculation, completion, or a terminal error. Reverse proxies must preserve `text/event-stream`; the response sets `X-Accel-Buffering: no`.

Gamma mapping uses `event.startTime` (fallback `market.eventStartTime`) for the official start and `event.endDate`/`market.endDate` only to verify the scheduled end. Gamma `closedTime` is retained as metadata but never shifts the strategy expiration. Internal `expiration_time` is always derived from the official start plus the selected timeframe. On startup, cached rows from the obsolete schema that lack this normalized data are removed and later re-fetched from Gamma.

Long date ranges are divided into `HISTORY_CHUNK_DAYS` windows before Gamma pagination, so high-frequency `5m` series are not truncated at the API offset limit. Before a group is saved, every selected asset is checked for complete start/end coverage and gaps larger than its timeframe. A partial history fails explicitly instead of producing a strategy that silently stops trading.

The strategy page keeps its primary metrics compact. Advanced drawdown, win rate, Sharpe ratio, profit factor, average trade, a drawdown chart, and profitable/losing trade distribution are available under the expandable **Show advanced statistics and charts** control. Wide position and administration tables use responsive horizontal scrolling.

## Configuration

Copy `.env.example` to `.env`. Development credentials are included for local testing. Before deployment, replace all passwords and `SESSION_SECRET`, set the public hostname, and enable secure cookies:

```dotenv
ENVIRONMENT=production
SESSION_COOKIE_SECURE=true
ALLOWED_HOSTS=backtester.example.com
DATA_ROOT=/var/data/polymarket-backtester
WORKERS=1
```

Production startup fails when secure cookies are disabled, the session secret is too short, or bootstrap passwords are weak.

For Render deployments with file storage, keep `WORKERS=1` and attach a
Persistent Disk at `/var/data`. The app writes all JSON/JSONL data under
`DATA_ROOT`, so production data remains on the Render disk instead of the Git
repository or your local computer.

## Local data profiles

In development mode, the site shows a local-only **Data** drawer. It can download
a Render data snapshot, store it under `data_snapshots/`, and switch the running
local app between `server_data/` and downloaded snapshots. The drawer is disabled
in production.

Paste the Render `EXPORT_TOKEN` into the drawer once, or set it in `.env`:

```dotenv
RENDER_EXPORT_TOKEN=your-render-export-token
```

## Start and verify

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py

pytest -q
npm run build:css
```

This is research software, not financial advice.
