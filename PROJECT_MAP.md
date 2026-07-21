# Polymarket Backtester — AI Context & Project Map

> Срез репозитория: 2026-07-21. Это карта текущей реализации, а не финансовая спецификация. Перед изменениями проверять `git status`: в рабочем дереве могут быть пользовательские правки.

## 1. Quick Overview & Tech Stack

- **Назначение:** исследовательское web-приложение для загрузки закрытых Up/Down-контрактов Polymarket, генерации/бэктеста групп стратегий и мониторинга текущих контрактов. Есть paper trading и опциональная отправка реальных CLOB-ордеров; это не production-grade торговая платформа.
- **Стек:** Python 3.12 (Docker), FastAPI 0.115 + Uvicorn, Jinja2, Pydantic Settings, HTTPX, WebSockets, `py-clob-client-v2`; JSON/JSONL-файлы вместо СУБД; vanilla JS + Chart.js 4.4 + Tailwind CSS 3.4 (Vue фактически не используется). Node 20 нужен только для сборки CSS.
- **Запуск:** `python -m venv .venv`; `.venv\Scripts\Activate.ps1`; `pip install -r requirements.txt`; `python main.py` (по умолчанию `127.0.0.1:8000`). Альтернатива: `uvicorn backend.api.app:app --reload`.
- **Проверки:** `pytest -q` (180 тестов; 131.67 с на текущей машине); `npm ci`; `npm run build:css`. Отдельных lint/type-check/CI-команд нет.
- **Сборка/deploy:** multi-stage `Dockerfile` (Node 20 → Python 3.12), `render.yaml`, `fly.toml`. Миграций нет: постоянное состояние — файлы под `DATA_ROOT`.

## 2. Directory & File Map

```text
main.py                              # CLI-entrypoint: uvicorn backend.api.app:app
config/settings.py                   # Pydantic/env-конфигурация и production validation
backend/
  api/app.py                         # FastAPI factory/lifespan, middleware, app.state, /healthz
  api/routes.py                      # 75 HTML/API routes + orchestration (6123 строк; монолит)
  auth/service.py                    # PBKDF2 password hash/verify
  auth/sessions.py                   # signed cookie ID + server-side file sessions
  core/jobs.py                       # file-backed status/events/cancel для SSE jobs
  core/logging.py                    # базовый logging config
  fixed_time.py                      # активы, таймфреймы, series IDs, UTC validation
  data/polymarket_client.py          # Gamma HTTP client: chunks/pages/retry/cache
  data/collector.py                  # Gamma payload → нормализованные fixed-time events + coverage
  backtester/file_engine.py          # fixed-expiry cash/margin simulation и метрики
  strategy_generator/file_generator.py       # до 300 исторических спецификаций/бэктестов
  strategy_generator/live_monitor_generator.py # до 1000 live/smart стратегий; phased selection
  live_monitor.py                    # WS/REST feeds, sessions, sampling, saved-record quality (5583 строки)
  trading/polymarket_executor.py     # optional real CLOB buy/sell, signing/warmup/balance
  storage/file_store.py              # users, JSONL collections, cache/coverage, atomic writes/locking
  storage/locking.py                 # Windows/POSIX advisory process lock
frontend/
  templates/base.html                # общий Jinja layout, Chart.js/app.js/local-data drawer
  templates/*.html                  # auth/account/admin, groups, strategy, monitor/quality/paper pages
  static/js/app.js                   # весь browser UI/SSE/charts/live/paper logic (≈1550 строк)
  static/css/app.css                 # hand-written styles
  static/css/tailwind.input.css      # Tailwind source
  static/css/tailwind.css            # generated и tracked; нужен Render deploy
  static/vendor/chart.umd.js         # vendored Chart.js bundle
tests/
  conftest.py                        # root import path + TrustedHost test fixture
  test_file_architecture.py          # 76 broad integration/domain tests (4436 строк)
  test_live_monitor.py               # 86 live-feed/session/storage tests (3141 строк)
  test_polymarket_client.py          # Gamma pagination/cache/retry
  test_fixed_time_collector.py       # normalization/coverage
  test_jobs.py                       # GenerationJobStore
  test_live_order_amount.py          # real-order amount regression
README.md                            # behavior, accounting invariants, setup
DEPLOY.md                            # operational deployment notes
docs/paper_real_trading_ai_index.md  # focused index for paper/real trading code
requirements.txt / package*.json    # pinned Python/Node dependencies
Dockerfile / render.yaml / fly.toml # deployment variants
.env.example                        # env contract (includes legacy credential aliases)
server_data/                        # ignored runtime state; never treat as source code
data_snapshots/                     # ignored local Render snapshots
```

## 3. Architecture & Data Flow

- **Entrypoints:** `python main.py` imports `backend.api.app:app`; `create_app()` mounts static files, sessions and TrustedHost middleware. `lifespan()` validates config, creates `FileStore`, `DataCollector`, `GenerationJobStore`, `LiveMonitorManager`, bootstrap user and shutdown cleanup.
- **Web/API:** Jinja server-rendered pages plus JSON/SSE endpoints. `routes.py` owns auth, admin, group generation/retest, cache, live monitoring/quality, paper/real trading and local snapshot switching. Dependencies are fetched from `request.app.state`, not DI containers.
- **Persistent entities:** user `profile.json`; per-user `groups`, `strategies`, `transactions`, `logs`, `family_reports`, `paper_trades`, `real_orders` as JSONL; `strategy_favorites.json`; global fixed events/coverage and logs; file sessions/jobs; saved live contracts under `live_monitor/`. IDs are UUID hex; relationships use `user_id`, `group_id`, `strategy_id`, `paper_trade_id`.
- **Historical flow:** form/API → `DataCollector.collect()` → `PolymarketClient` Gamma chunks/pages/cache → validate start/end/outcome → `FileStore.fixed_time_events.jsonl` + coverage → `StrategyGroupGenerator` → `FileBacktester` (often process pool) → atomic group/strategy/transaction append → Jinja/Chart.js.
- **Live flow:** URL → `resolve_polymarket_event()` → Polymarket order-book WS + crypto WS, REST fallback → `LiveMonitorSession` samples 0.5 s history, publishes SSE snapshots and persists finished JSON records → quality scan / `LiveMonitorStrategyGenerator` / paper trading.
- **Real-order branch:** paper strategy signal → helpers in `routes.py` → `PolymarketLiveOrderExecutor` → `py-clob-client-v2` CLOB call → `real_orders.jsonl` and linked transaction state. Disabled unless explicitly enabled/configured.
- **Concurrency:** asyncio for feeds/HTTP/SSE, `to_thread` for heavy sync generation, process pools for strategy batches, thread + OS file locks for file storage. Live sessions/caches/tasks remain process-local; deployment therefore assumes one application worker/instance.

## 4. Coding Conventions & Patterns

- Layer-like package layout (`api` → collector/generator/backtester/storage), but `routes.py` bypasses clean boundaries and contains substantial domain logic. Persistence is Repository-like (`FileStore`); app wiring uses factory + lifespan.
- Python: `snake_case`, `PascalCase`, leading `_` for internal helpers, UTC-aware ISO timestamps, plain `dict` records rather than domain/Pydantic models. Frontend uses global `init*` functions and `data-*` hooks.
- Expected invariants: exact fixed expiry; no historical look-ahead; historical entry uses user-assumed ask; finite JSON only; user ownership checked on record routes; safe IDs; atomic replace and locks around mutations.
- Validation errors are mostly `ValueError` translated to flash messages/`HTTPException`; background errors are logged and emitted as terminal SSE events. Broad `except Exception` exists around network/background/order paths.
- Tests use pytest/pytest-asyncio, TestClient and hand-built dict fixtures; external APIs are mocked. There is no schema migration framework or static typing enforcement.

## 5. Known Issues, Technical Debt & Overengineering (КРИТИЧЕСКИЙ БЛОК)

### Переусложнения и избыточные абстракции

- **Два god modules:** `api/routes.py` (6123 строк, 75 routes) смешивает HTTP, reports, snapshots, paper accounting и real-order orchestration; `live_monitor.py` (5583 строк) смешивает transports, session state, persistence и quality analysis. Особенно рискованны `_execute_paper_real_trading_signals` (311 строк) и `check_saved_external_targets` (357 строк). Делить по bounded context, а не добавлять новые helpers в эти файлы.
- **FileStore эмулирует БД:** 1283 строки с JSONL indexes, reverse scans, rewrites, nested thread/process locks и ручными связями. Это оправдано только для single-host small data; дальнейшее развитие дешевле на SQLite/Postgres (+ Redis при нескольких инстансах), чем через новые file abstractions.
- **Strategy generator как набор ветвлений:** `live_monitor_generator.py` (2582 строки) кодирует families/specification/description/execution отдельно; изменение семейства затрагивает несколько больших методов. Нужен единый registry (`family → parameter schema/signal/description`), сохранив детерминизм тестов.
- **Дублирование API:** синхронный `POST /groups` и job-based `POST /api/group-jobs` реализуют один workflow; UI использует jobs. После проверки внешних клиентов оставить один путь. Local-data snapshot/export logic (~последние 300 строк `routes.py`) вынести в отдельный dev-only router/service.

### Мусорный, дублирующий и устаревший код

- Статически не вызываются `LiveMonitorSession._fresh_crypto_price`, `_crypto_price_from_payload`, `FileStore.find_user_by_email`; удалить после одной targeted проверки.
- `/generator` — compatibility redirect без внутренних ссылок; удалить, если внешних bookmarks нет. Legacy-record branches/tests (`legacy_*`) сохранять только при формально заданном сроке поддержки старых snapshots.
- `live_monitor.py` захардкодил CLOB REST URL и не использует `settings.polymarket_clob_url`; настройка влияет на executor, но не на monitor. WS URLs также неконфигурируемы.
- Конфигурация раздроблена: часть live limits читается при import через `_env_int/_env_float`, worker knobs — прямым `os.getenv`, остальное — `Settings`. `.env.example` дублирует `PRIVATE_KEY`/`POLYMARKET_PRIVATE_KEY`, proxy/API/signature aliases. Свести в `Settings`, aliases пометить deprecated.
- Generated `tailwind.css` и vendored Chart.js tracked намеренно, но это источник drift. Render не запускает npm build и зависит от закоммиченного CSS; Docker собирает его заново.

### Проблемы с тестами

- 180 тестов, но 162 находятся в двух файлах; full suite занимает ~132 с на текущей машине. Это затрудняет ownership и targeted runs: разбить по `routes/paper_trading/quality/live_transport/live_storage` и добавить markers (`unit`, `integration`, `slow`).
- Явных exact duplicates не найдено, но много ручных вариаций одного поведения: crossed quote (3 теста), reconnect/disconnect/stale matrices, target anomaly/rounding, ~15 quality mutation/recovery cases. Перевести входы/ожидания в `pytest.mark.parametrize` и общие fixtures; не удалять уникальные edge cases.
- `test_file_architecture.py` по имени уже не соответствует содержимому: это одновременно backtester, routes, auth, admin, quality и real-order integration. Большие inline payloads повышают стоимость изменений.
- Legacy-schema tests полезны только пока поддерживаются старые persisted files. После одноразовой миграции заменить их тестами мигратора и удалить runtime fallbacks.
- Нет coverage threshold, CI, lint/type tests; регрессии в мёртвых функциях, route auth и env drift обнаруживаются только вручную.

### Баги и bottlenecks

- **Single-process constraint:** `LiveMonitorManager`, active sessions, task maps и in-memory report/preview caches не разделяются между workers/instances. `WORKERS>1` или горизонтальное масштабирование ломает lookup/stop/SSE consistency; file locks решают только persistence.
- **Blocking I/O:** file session load/save + global process lock выполняются на каждом запросе; многие JSONL операции линейно читают/переписывают коллекции. Async routes местами вызывают их напрямую, блокируя event loop. Рост history/trades/users быстро увеличит latency.
- **Memory/CPU:** quality scans и generators материализуют крупные dict/history списки; process pools сериализуют events; 1000-strategy search и многопроходные phase rankings делают suite и production jobs дорогими. Нужны benchmarks/limits до оптимизации.
- **Coverage correctness:** collector помечает весь missing range покрытым после запроса, хотя отдельные Gamma rows могут быть отброшены normalization (`market/token/condition/time mismatch`). Coverage означает «диапазон запрошен», не доказанную полноту контрактов; хранить причины reject/counts и не считать silent rejects полным покрытием.
- **Security:** destructive POST/DELETE и real-order endpoints не имеют CSRF token; `SameSite=Lax` снижает, но не заменяет CSRF protection. Нет login throttling/lockout. Перед публичным multi-user deploy добавить CSRF, rate limiting, audit/redaction и отдельное подтверждение/permission для real trading.
- **Operational drift:** Render build не собирает frontend; `render.yaml` и `fly.toml` задают разные live limits; hardcoded monitor endpoints обходят env. Нет health/readiness проверки внешних feeds или writable `DATA_ROOT`.

## 6. Optimization Roadmap (Что исправлять в первую очередь)

1. **Защитить торговый контур:** CSRF + login throttling; отдельный real-trading permission/confirmation; централизованные/redacted audit logs; targeted tests всех mutating routes.
2. **Разрезать `routes.py`:** routers/services `auth_admin`, `groups_jobs`, `live_quality`, `paper_trading`, `local_data`; сначала вынести pure helpers без изменения API.
3. **Разрезать `live_monitor.py`:** `feeds`, `session`, `saved_repository`, `quality`; определить typed dataclasses/Pydantic schemas на границах вместо неформальных dict contracts.
4. **Централизовать config:** перенести все env knobs/URLs/legacy aliases в `Settings`, валидировать один раз; синхронизировать `.env.example`, Render/Fly/Docker и документировать single-worker invariant.
5. **Нормализовать тесты:** разнести два мегaфайла, параметризовать matrices, добавить markers/CI/coverage и быстрый default suite; benchmark slow strategy/quality cases.
6. **Убрать подтверждённый dead/legacy code** и объединить duplicate group-generation path; исправить coverage semantics и добавить reject telemetry.
7. **Заменить file persistence по мере роста:** минимум SQLite WAL для users/jobs/relations; Postgres + Redis/pub-sub для multi-instance live workloads. До миграции ограничить размеры JSONL и вынести blocking I/O в threads.

## 7. Critical Rules & Pitfalls ("Don'ts")

- Не нарушать anti-lookahead: стратегия видит только результаты, завершившиеся до entry; `entry_time = official start`, `expiration = start + timeframe`, resolution не сдвигает expiry.
- Не подменять историческую цену: Gamma history содержит metadata/outcome, не trade/order-book entry; historical backtest использует `assumed_ask_price` пользователя.
- Не включать real trading по умолчанию и не логировать private key/API secret/passphrase. Любые CLOB changes требуют мок-тестов buy, sell, retries, fill quantity и idempotent order IDs.
- Не запускать `WORKERS>1`/несколько инстансов без внешнего shared state для live sessions/tasks/SSE. Текущий production contract — один worker и persistent `DATA_ROOT`.
- Не писать напрямую в runtime JSON/JSONL: использовать `FileStore`/live saved-record helpers, finite JSON, safe IDs, locks и atomic replace. Не коммитить `.env`, `server_data/`, `data_snapshots/`.
- Не удалять compatibility/legacy paths, пока не проверены реальные persisted snapshots и не создан мигратор/rollback.
- После Tailwind/template class changes запускать `npm run build:css`; Render использует tracked `frontend/static/css/tailwind.css`. Не редактировать generated CSS/vendor bundle вручную.
- Сохранять user ownership checks (`_require_user` + scoped lookup), UTC-aware timestamps, cancellation/terminal SSE events и shutdown cleanup.
- Перед рефакторингом проверять пользовательские незакоммиченные изменения; не перезаписывать их и не менять публичные routes/data schema без migration plan.
