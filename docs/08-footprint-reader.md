# 8. Чтение данных: footprint

Модуль: `reader.py`, функция `get_cluster_data()`. Это единственный
публичный интерфейс для потребителя (торгового робота); вызывается
in-process.

## 8.1. Сигнатура

```python
def get_cluster_data(
    config: AppConfig,
    repo: MarketDataRepository,
    ticker: str,
    start_time: datetime,
    end_time: datetime,
    timeframe: str = "5min",
) -> pd.DataFrame
```

| Параметр | Тип | Описание |
|----------|-----|----------|
| `config` | `AppConfig` | Используется для поиска `tick_size` по `storage_name` |
| `repo` | `MarketDataRepository` | Репозиторий инструмента; Parquet читается из его каталога |
| `ticker` | `str` | Ключ хранения (`storage_name`): `SBER`, `Si_CONTINUOUS` |
| `start_time` | `datetime` | Начало интервала включительно; naive трактуется как UTC |
| `end_time` | `datetime` | Конец интервала, не включая; naive трактуется как UTC |
| `timeframe` | `str` | Частота pandas (`1min`, `5min`, `15min`, `1h`, …) |

Пример:

```python
from datetime import UTC, datetime
from cluster_source.config import AppConfig
from cluster_source.database import MarketDataRepository
from cluster_source.reader import get_cluster_data

config = AppConfig.from_yaml("config.yaml")
si = next(i for i in config.instruments if i.storage_name == "Si_CONTINUOUS")
repo = MarketDataRepository(config.storage_root, si)
df = get_cluster_data(
    config,
    repo,
    "Si_CONTINUOUS",
    start_time=datetime(2026, 10, 12, 7, 0, tzinfo=UTC),
    end_time=datetime(2026, 10, 12, 8, 0, tzinfo=UTC),
    timeframe="5min",
)
```

## 8.2. Результат

Каждая строка — ячейка «интервал × уровень цены». JSON Schema строки:
[contracts/schemas/footprint-row.schema.json](contracts/schemas/footprint-row.schema.json).

| Колонка | Тип pandas | Описание |
|---------|------------|----------|
| `timestamp` | `datetime64[*, UTC]` | Левая граница интервала `timeframe` |
| `price` | `float64` | Цена, округлённая до `tick_size` |
| `buy_volume` | `int64` | Сумма `volume` сделок `BUY` |
| `sell_volume` | `int64` | Сумма `volume` сделок `SELL` |
| `delta` | `int64` | `buy_volume − sell_volume` |

Сортировка: `timestamp`, затем `price` по возрастанию. Ячейки без сделок
не выводятся. Сделки `UNKNOWN` не входят ни в одну сторону; ячейка только с
такими сделками выводится с нулевыми объёмами
([L-06](12-known-issues.md#l-06)).

Если в интервале нет сделок, возвращается пустой `DataFrame` с колонками
`timestamp, price, buy_volume, sell_volume, delta`.

Пример результата (`tick_size = 1.0`, `timeframe = "5min"`):

| timestamp | price | buy_volume | sell_volume | delta |
|-----------|-------|------------|-------------|-------|
| 2026-10-12 07:00:00+00:00 | 96512.0 | 40 | 15 | 25 |
| 2026-10-12 07:00:00+00:00 | 96513.0 | 3 | 22 | −19 |
| 2026-10-12 07:05:00+00:00 | 96513.0 | 11 | 11 | 0 |

## 8.3. Алгоритм

```mermaid
flowchart TD
    A([get_cluster_data]) --> TS["tick_size: инструмент из config<br/>с storage_name == ticker, иначе repo.instrument"]
    TS --> PQ["repo.read_parquet(start, end)<br/>файлы дней, пересекающих интервал;<br/>строки start ≤ ts < end"]
    TS --> SQ["repo.read_ticks(start, end, ticker)<br/>SQLite: ticker = ? AND start ≤ ts < end"]
    SQ --> EX["исключить строки дней,<br/>у которых есть Parquet-файл"]
    PQ --> U[concat]
    EX --> U
    U --> E{"строк нет?"}
    E -->|да| EMPTY(["пустой DataFrame с колонками результата"])
    E -->|нет| RND["price = round(price / tick_size) · tick_size"]
    RND --> SPL["buy_volume = volume, если BUY<br/>sell_volume = volume, если SELL"]
    SPL --> GB["groupby(Grouper(timestamp, freq=timeframe), price)<br/>sum(buy_volume), sum(sell_volume)"]
    GB --> DL["delta = buy − sell; сортировка"]
```

| Шаг | Правило | Обоснование |
|-----|---------|-------------|
| Фильтр Parquet | Файлы дней от `midnight(start)` до `end`; строки по точному интервалу `[start, end)` | |
| Фильтр SQLite | По `ticker` и точному интервалу `[start, end)` | |
| Выбор источника по дню | Если у дня есть Parquet-файл, берётся Parquet, строки SQLite этого дня отбрасываются | Parquet — архивная копия SQLite или перекачанный официальный архив; сделки не учитываются дважды, а реальные одинаковые сделки не схлопываются (дедупликации нет) |
| Округление | Половина шага — банковское округление (`Series.round`) | `tick_size` из конфигурации |

## 8.3.1. Качество данных ([SPEC-0004](specs/0004-degradation-detectors.md))

Footprint не знает о качестве данных. Перед расчётом сигнала робот ДОЛЖЕН
проверить окно ([SPEC-0001](specs/0001-data-guarantees.md) G-1):

```python
from cluster_source.reader import get_data_state, get_degraded_minutes

state = get_data_state(
    repo
)  # {"state": "OK"|"DEGRADED"|"UNKNOWN", "reasons": [...], "since": "..."}
bad = get_degraded_minutes(repo, start, end)  # DataFrame: minute (UTC), reasons
if state["state"] != "OK" or not bad.empty:
    ...  # сигнал по окну не выдаётся
```

| Функция | Возвращает |
|---------|------------|
| `get_degraded_minutes(repo, start, end)` | Минуты `DEGRADED` в `[start, end)`: `minute` — начало минуты UTC, `reasons` — причины через запятую. Минут нет в результате — они `PROVISIONAL` |
| `get_data_state(repo)` | Текущее состояние инструмента: `state` (`OK` / `DEGRADED`; `UNKNOWN` — процесс сбора ещё не сообщал о состоянии), `reasons`, `since` — момент последнего перехода |

Сверка с лентой Мосбиржи ([SPEC-0005](specs/0005-iss-reconciliation.md)) — через 15–17 минут
после минуты:

| Функция | Возвращает |
|---------|------------|
| `get_minute_verdicts(repo, start, end)` | Вердикты минут: `VERIFIED` (совпало с биржей), `REPAIRED` (недостающие сделки дозалиты из ISS), `GAP` (сверить нельзя, данные не тронуты); `stream_n`, `iss_n`, `repaired`, `detail` |
| `get_verified_until(repo)` | Момент, до которого данные сверены; минуты раньше него без вердикта — пустые и тоже сверены |

Окно, целиком лежащее раньше `get_verified_until` и без `GAP`, совпадает с
лентой биржи. Footprint по `REPAIRED`-минутам строится по полным данным: у
дозалитых сделок время с точностью до секунды.

Окончательный статус дня ([SPEC-0006](specs/0006-final-day.md)) — ночью:

| Функция | Возвращает |
|---------|------------|
| `get_day_verdicts(repo)` | Вердикты торговых дней: `FINAL` / `ARCHIVE_CONSISTENT` / `MISMATCH` / `UNVERIFIABLE`, наши и эталонные число сделок и объём, эталон (`iss_totals` / `iss_tape`) |
| `get_final_days(repo)` | Торговые дни `FINAL` — доказанно совпадают с эталоном биржи |
| `get_baseline_days(repo, n=20, allow_archive=True)` | Окно для норм (RVOL, перцентили, профили): последние `n` дней `FINAL`, при нехватке — добор `ARCHIVE_CONSISTENT`; `[(день, статус)]` |
| `baseline_quality(days)` | `pure` — все дни окна `FINAL`; `mixed` — есть архивные; `empty` — дней нет |

Нормы считаются только по окну `get_baseline_days` (SPEC-0001 G-3, SPEC-0006 G-4).
Статус `ARCHIVE_CONSISTENT` бывает только у фьючерсов: день из архива
согласуется с итогами биржи в пределах сделок вне анонимной ленты, но не
доказан полной лентой ISS.

Открытый интерес фьючерсов ([SPEC-0007](specs/0007-open-interest.md)):

| Функция | Возвращает |
|---------|------------|
| `get_open_interest(repo, start, end)` | Ряд ОИ из стрима T-Invest (реальное время, при каждом изменении): `ts`, `value`; 7 дней |
| `get_oi_flow(iss_repo, secid, start, end, timeframe="1min")` | Поток ОИ по ленте ISS (задержка 15 мин) по барам: `oi_close`, `oi_change`, `opened` / `closed` / `transferred` (контракты), `outside` (изменение ОИ сделками вне анонимной ленты), `trades` |
| `get_contract(repo, day)` | Тикер активного контракта непрерывного фьючерса за день — `secid` для `get_oi_flow` |

ОИ Мосбиржи считает позиции обеих сторон: `opened = ΔОИ/2` при росте, `closed
= −ΔОИ/2` при падении, `transferred = количество − |ΔОИ|/2`.

Причины: `stream_gap`, `subscription_failed`, `stream_silence`, `latency`,
`backlog`, `write_failure`, `unknown_direction`, `contract_roll`,
`dealer_leak` — см. [5.8](05-realtime-collection.md#58-детекторы-деградации-spec-0004).
Отметки появляются в базе при записи консьюмера, то есть с задержкой не
больше `flush_interval_sec`.

## 8.4. Конкурентный доступ

Чтение безопасно во время работы коллектора: SQLite в режиме WAL позволяет
читателям видеть последнее зафиксированное состояние, не блокируя запись.
Тики появляются в SQLite с задержкой до `flush_interval_sec`. Строки дня в
SQLite при перекачке заменяются одной транзакцией, поэтому читатель не
видит день пустым или задвоенным. Parquet-файл дня перезаписывается
обслуживанием хранилища (догрузка, перекачка, архивация).
