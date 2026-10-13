# AGENTS.md

Python 3.11+ project; src-layout package `cluster_source`. Realtime T-Investments tick collector with hybrid SQLite (hot, 7 days) + Parquet (cold) storage.

## Commands (run from repo root)

- Install: `pip install -e ".[dev]"` (dev includes SDK extra; the SDK `t-tech-investments` may be unavailable on PyPI in some networks — base install works without it).
- Test: `pytest -q` (asyncio auto-mode; 130 tests).
- Lint/format: `ruff check . && ruff format .`
- Typecheck: `mypy src/` (strict, python 3.13).
- Run: `cluster-source` or `.venv/bin/python -m cluster_source.main`.

Run order after changes: `ruff check` → `mypy src/` → `pytest -q`.

## Architecture rules (senior-level constraints — do not regress)

- **No raw SQL in business logic.** All SQLite/Parquet access goes through the repositories in database.py (`MarketDataRepository` per instrument, `IssTapeRepository` for the MOEX tape). This is a hard requirement.
- **One stream connection (SPEC-0009).** T-Invest allows 16 market-data stream connections (32 for this account per tariff), ≤300 subscriptions each. `stream_hub.StreamHub` holds ONE connection subscribed to all FIGIs and routes trades by FIGI/instrument_uid to `DataCollector.deliver`. Never open a stream per instrument. FIGIs are resolved only at start and on `request_reconnect(resolve=True)` (continuous-futures roll), not on every reconnect.
- **No DB writes from the gRPC stream.** `DataCollector` uses Producer-Consumer: producer (`deliver`) pushes `Tick` DTOs into `asyncio.Queue` (and keeps day refresh marks in memory); consumer batch-writes (`executemany` in one explicit transaction) every `flush_interval_sec` (5s) or `batch_size` (1000) ticks and persists the marks.
- **DI, not globals.** Config (`AppConfig`) and repositories are injected into constructors.
- **WAL mode required.** `session()` sets `PRAGMA journal_mode=WAL` — the bot reads while the collector writes. Never drop it.
- **Graceful shutdown.** `amain` stops the hub, cancels background tasks, then cancels collectors; `DataCollector.run` drains the queue in `finally` on any exit, then re-raises.
- **Docs.** Human-readable docs live in `docs/` (see `docs/README.md`; contracts in `docs/contracts/`). When behaviour changes, update the matching doc per the table in `docs/11-development.md`.
- **Degradation detectors (SPEC-0004).** `quality.InstrumentQuality` per collector holds conditions (stream_gap, subscription_failed, stream_silence, latency, backlog, write_failure) and one-off marks (unknown_direction, contract_roll, dealer_leak); minutes go to `degraded_minutes`, transitions to `quality_events` (log `DATA_DEGRADED`/`DATA_RESTORED`), written by the consumer only. Silence counts only during `NORMAL_TRADING` and never before the subscription was confirmed. The robot reads `reader.get_degraded_minutes` / `get_data_state` before any signal.
- **Specs first.** Every behaviour change starts with a spec in `docs/specs/` (format and registry in `docs/specs/README.md`; no openspec). Implementing code carries `# SPEC-NNNN G-N` at the guarantee, the covering test names the same id; a spec is "Готово" only when every acceptance criterion has a test.
- **Data guarantees (SPEC-0001).** ClusterSource owns data correctness end-to-end: (G-1) no signal while the data window or instrument is `DEGRADED`/`GAP`; (G-2) every signal gets `SIGNAL_CONFIRMED`/`SIGNAL_REVOKED` within 20 min (unverifiable ⇒ revoked); (G-3) thresholds/baselines use `FINAL` days only. Only exchange trades are stored — dealer trades are filtered at the source (SPEC-0002).
- **MOEX ISS tape (SPEC-0003).** `iss.IssCollector` (background task in `amain`, `iss.enabled`) pages the market-wide trade feeds of shares and FORTS into `_sqlite/_iss.sqlite3` (`UNIQUE(market, secid, tradeno)`), finalizes each ISS day to `_iss/{market}/{feed_date}.parquet` on feed change, marks a feed not caught up before the change `ISS_FEED_INCOMPLETE`, and hourly verifies trading days against ISS `NUMTRADES` (`ISS_DAY_VERIFIED` / `ISS_DAY_INCOMPLETE`). Free ISS serves only the current day: never delete `_iss*` data. Page the feed by cursor (`tradeno` for shares, `recno` for FORTS, `next_trade=1`) — never `start` (deprecated; deep offsets return empty pages). `previous_session=1` returns only the start of the previous FORTS session — never treat it as day recovery. Trade time: date from SYSTIME if recorded promptly, else TRADEDATE (FORTS weekend trades arrive Monday 06:00); trading day = TRADE_SESSION_DATE. The stage-1 script and its launchd agent were removed on 2026-10-13; `_iss_raw/` raw-file import remains for manual imports (docs/10-operations.md § 10.4.1).

## Storage model

- `data/history/{storage}/{YYYY-MM-DD}.parquet` — cold store (snappy).
- `data/history/_sqlite/{storage}.sqlite3` — hot store, WAL.
- `days_to_refresh` table (per-instrument SQLite) — days to replace from the official archive: `stream_gap` (stream opened), `compromised` (trade_id gap), `partial_backfill` (today from GetLastTrades), `dealer_cleanup` (one-off SPEC-0002 re-download of every stored past day; not limited by the `history_check_days` window, processed by the background `main.run_dealer_cleanup` pass under `maintenance_lock`). Cleared on successful re-download, or on 404 when the day has no data; kept on 404 while data exists (archive not yet published).
- Continuous futures (`type: continuous_futures`) store under `{ID}_CONTINUOUS`, not the ticker. Active contract = `config.nearest_contract` (ticker `{ID}{month code}{year}`, nearest expiry ≥ day) — the single rule for both the stream and backfill; never match by bare prefix. Backfill fetches futures with `INSTRUMENT_STATUS_ALL` (expired contracts needed for past days).
- Maintenance (`main.run_maintenance` = backfill → archiver) runs at startup and daily at `archiver.run_hour_utc:run_minute_utc` (00:05 UTC) via `main.run_scheduler`; afterwards the stream hub gets `request_reconnect(resolve=True)` to re-resolve continuous-futures contracts.
- Retention: hot store keeps the last 7 calendar days. Archiver writes every past day that has hot rows but no Parquet, validates the file exists and is non-empty, **then** purges hot rows older than the cutoff — and skips the purge entirely if any of those days lacks a Parquet file. Purge cutoff is the whole-day boundary (midnight of today-7) — aligned with `_target_for_day` so backfilled days routed to SQLite are not purged on the same run. Do not purge before validation.
- Reader: per day, Parquet wins over SQLite (no row dedup — real trades can be identical).

## Gotchas

- All timestamps are UTC. Never write naive datetimes; `_iso()` tz-normalizes and always writes microseconds. Parse timestamp strings with `format="ISO8601"` (legacy rows lack the fraction; without it pandas fails or, with `errors="coerce"`, silently drops rows).
- Directions are normalized by `config.parse_direction`: BUY / SELL / UNKNOWN (UNSPECIFIED or missing → UNKNOWN, never SELL/BUY).
- `t-tech-investments` is imported defensively in main.py (`_has_sdk`); the collector/reader/archiver run without the SDK for tests. Do not import SDK in other modules. The package is not on PyPI in some networks — then copy `t_tech/` + `t_tech_investments-*.dist-info` from another venv into `.venv/lib/python3.13/site-packages/` (plus deps: grpcio, protobuf, iprotopy, sentry-sdk, urllib3).
- **Keep awake (SPEC-0010).** On macOS `main.prevent_idle_sleep` starts `caffeinate -i -w <pid>` (config `keep_awake`, default true): while the Mac sleeps nothing is collected. Lid close still sleeps (L-13).
- **gRPC DNS:** `main.py` sets `GRPC_DNS_RESOLVER=native` before importing the SDK — the bundled c-ares resolver fails behind the user's VPN/fake-ip DNS ("Could not contact DNS servers"). Keep it (KI-19).
- **TLS on macOS python.org builds:** the interpreter has no default CA file, and T-Investments endpoints are signed by Russia's Trusted Root CA (present in the macOS keychain, absent from certifi). `autoload._ssl_context()` uses `truststore` (OS trust store) with a certifi fallback — keep verification on, never add `ssl._create_unverified_context`.
- Mypy strict: row dicts are typed as `dict[str, object]`; conversion helpers `_as_float`/`_as_int` in database.py exist for this.
- The trade_id gap log line marks a day "COMPROMISED" and records it in `days_to_refresh` → autoload re-downloads it. Keep that marker string intact. Note: the public trade stream of `t-tech-investments` v1.49 has **no** `trade_id` field — `_to_dto` then synthesizes a non-numeric id, which safely skips the numeric gap check. History-archive CSVs also have no trade id; `_history_rows` synthesizes `s{ts}_{idx}`.
- History backfill (`autoload.py`) primary source = the official history-data service: `GET https://invest-public-api.tbank.ru/history-trades/YYYY-MM-DD?instrumentId={TICKER_CLASS}` (Bearer auth) → gzip CSV `TRADE_TS,TICKER_CC,DIRECTION,PRICE,QUANTITY,TRADE_SOURCE,INSTRUMENT_UID`, UTC. Archives refresh nightly, exclude the current day, 404 = no market that day, rate limit ~30 files/min/IP. Archive class codes: shares `TQBR`/`SPBXM`, futures `SPBFUT` (API `classCode` from find_instrument does NOT match the convention for shares). Continuous futures map each day to the contract active that day (`_nearest_future`). Only the current day falls back to `GetLastTrades` (latest trades only → marked `partial_backfill`), routes days older than 7 to Parquet via `_target_for_day`. Resolution prefers SDK `InstrumentsService` (gRPC) because REST `FindInstrument` intermittently 404s; REST kept only as SDK-less fallback. Share FIGI requires an exact ticker match (TQBR → SPBXM priority), else `None`; futures FIGI comes from the futures list. The legacy tbank.ru archive (HTTP 404, gone) must NOT be reintroduced.
- A non-existent ticker raises `InstrumentNotFound` from `_resolve_stream_figi` (main.py); `_resolve_figis` logs a warning and leaves that instrument out of the subscription (the hub logs `Инструмент X не найден у провайдера`). Transient errors get exponential backoff in `StreamHub.run`.
- `tests/` use `datetime.UTC` alias; keep footnotes clean for ruff UP017.

## Files

- `config.py` — `AppConfig`, `Instrument`, `InstrumentType` (StrEnum), `Direction`/`parse_direction`, `FuturesMeta`/`nearest_contract`.
- `database.py` — `MarketDataRepository` (SQLite + Parquet, context manager sessions).
- `collector.py` — per-instrument sink: `deliver` (producer) + consumer, `Tick` DTO, `InstrumentNotFound`.
- `autoload.py` — backfill + re-download of marked days; official history-trades archive (`history-trades`, gzip CSV) for past days + `GetLastTrades` for today; REST/legacy CSV parser kept as fallback.
- `archiver.py` — archival + purge guard; `DataArchiver`, `seconds_until_next_run`.
- `reader.py` — `get_cluster_data()` footprint (pandas groupby, tick rounding).
- `quality.py` — degradation conditions and minute marks (`InstrumentQuality`); SPEC-0004.
- `stream_hub.py` — single T-Invest stream connection for all instruments (`StreamHub`, `HubConnection`); SPEC-0009.
- `iss.py` — MOEX ISS tape collector (`IssCollector`, `HttpIssClient`, `normalize_trades`); SPEC-0003.
- `main.py` — entrypoint, signal handling, DI wiring, `run_maintenance`, `run_scheduler`.