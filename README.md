# ClusterSource

Realtime tick-data collector for the T-Investments (Тинькофф Инвестиции) API
with a hybrid SQLite + Parquet storage, auto-history backfill and a 7-day
rolling window.

## Features

- Asynchronous gRPC trades streaming (`MarketDataStreamService.TradesStream`)
- Producer-Consumer pattern (`asyncio.Queue`) with batch SQLite writes
  (5s / 1000 ticks), no DB writes from the gRPC stream
- `MarketDataRepository` (Repository pattern, DI-injected) — the only layer
  that touches SQLite/Parquet; no raw SQL in business logic
- SQLite in **WAL** mode so a trading bot can read while the collector writes
- Continuous-futures support: active contract resolved per session, ticks
  stored under `{BASE}_CONTINUOUS/`
- Startup backfill of missing days via the HTTP history archive
  (older than 7 days → Parquet, within 7 days → SQLite)
- Nightly archiver (00:05 UTC): hot → Parquet with validation before purge
- `get_cluster_data()` footprint reader: tick-size rounding, BUY/SELL Delta
  split, merges SQLite + Parquet
- Automatic reconnect with exponential backoff; trade_id gap detection
  flags a day as "compromised" for re-download
- Graceful shutdown on SIGINT/SIGTERM: queued ticks are flushed, DB handles
  closed before exit

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[sdk]"   # base stack + tinkoff-investments SDK
pip install -e ".[dev]"   # + test/lint/typecheck tooling
```

## Configuration

Copy/edit `config.yaml`:

```yaml
TOKEN: "your-token-here"
tickers:
  - id: "SBER"
    type: "share"
    tick_size: 0.01
    lot_size: 1
  - id: "Si"
    type: "continuous_futures"   # stores to data/history/Si_CONTINUOUS/
    tick_size: 1.0
    lot_size: 1
```

## Run

```bash
cluster-source            # entrypoint from pyproject
# or
python -m cluster_source.main
```

Logs go to `cluster_source.log` and stdout.

## Development

```bash
ruff check . && ruff format .   # lint + format
mypy src/                       # type check
pytest -q                       # tests (23)
```

## Layout

- `src/cluster_source/config.py` — `AppConfig`, instrument model
- `src/cluster_source/database.py` — `MarketDataRepository` (SQLite + Parquet)
- `src/cluster_source/collector.py` — producer/consumer tick collector
- `src/cluster_source/autoload.py` — history backfill (HTTP archive)
- `src/cluster_source/archiver.py` — nightly hot→Parquet archival + purge
- `src/cluster_source/reader.py` — `get_cluster_data()` footprint API
- `src/cluster_source/main.py` — entrypoint, DI wiring, graceful shutdown

Data layout:

```
data/history/SBER/Si_CONTINUOUS/2026-09-04.parquet   # cold store
data/history/_sqlite/SBER.sqlite3                     # hot store (7 days, WAL)
```