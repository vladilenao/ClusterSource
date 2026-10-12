# ClusterSource

Realtime tick-data collector for the T-Investments (Тинькофф Инвестиции) API
with a hybrid SQLite + Parquet storage, auto-history backfill and a 7-day
rolling window.

## Features

- Asynchronous gRPC trades streaming (bidirectional
  `MarketDataStreamService/MarketDataStream` with `SubscribeTradesRequest`)
- Producer-Consumer pattern (`asyncio.Queue`) with batch SQLite writes
  (every 5s or 1000 ticks), no DB writes from the gRPC stream
- `MarketDataRepository` (Repository pattern, DI-injected) — the only layer
  that touches SQLite/Parquet; no raw SQL in business logic
- SQLite in **WAL** mode so a trading bot can read while the collector writes
- Continuous-futures support: the active contract (`{BASE}{month}{year}`,
  nearest expiry) is resolved at stream open and re-resolved after the
  nightly maintenance; ticks stored under `{BASE}_CONTINUOUS/`
- Backfill of missing days via the official `history-trades` archive
  (older than 7 days → Parquet, within 7 days → SQLite)
- Incomplete days (stream opened mid-day, trade_id gap, partial today
  backfill) are marked and replaced by the official archive once published
- Storage maintenance at startup and daily at 00:05 UTC (backfill +
  archiving): hot → Parquet with validation; hot rows are purged only for
  days that have a Parquet file
- `get_cluster_data()` footprint reader: tick-size rounding, BUY/SELL Delta
  split, merges SQLite + Parquet (Parquet wins per day)
- Automatic reconnect with exponential backoff (2 → 60 s); trade_id gap
  detection flags a day as "compromised" for re-download
- Graceful shutdown on SIGINT/SIGTERM: queued ticks are flushed, DB handles
  closed before exit

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[sdk]"   # base stack + t-tech-investments SDK
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
pytest -q                       # tests (59)
```

## Layout

- `src/cluster_source/config.py` — `AppConfig`, instrument model
- `src/cluster_source/database.py` — `MarketDataRepository` (SQLite + Parquet)
- `src/cluster_source/collector.py` — producer/consumer tick collector
- `src/cluster_source/autoload.py` — history backfill and re-download of
  marked days (`history-trades` archive)
- `src/cluster_source/archiver.py` — hot→Parquet archival + purge, schedule
- `src/cluster_source/reader.py` — `get_cluster_data()` footprint API
- `src/cluster_source/main.py` — entrypoint, DI wiring, graceful shutdown

Data layout:

```
data/history/SBER/2026-09-04.parquet            # cold store
data/history/Si_CONTINUOUS/2026-09-04.parquet   # cold store, continuous futures
data/history/_sqlite/SBER.sqlite3               # hot store (7 days, WAL)
```

## Documentation

See [docs/README.md](docs/README.md): architecture, data model, processes,
configuration, operations, and OpenAPI/AsyncAPI/JSON Schema contracts.
