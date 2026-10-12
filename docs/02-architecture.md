# 2. Архитектура

## 2.1. Компоненты

```mermaid
flowchart TB
    MAIN["main.py<br/>main() / amain()<br/>DI-сборка, сигналы"]
    SCHED["main.run_scheduler()<br/>ночное обслуживание"]
    MAINT["main.run_maintenance()"]
    CFG["config.py<br/>AppConfig, Instrument"]
    AUTO["autoload.py<br/>run_backfill()"]
    ARCH["archiver.py<br/>DataArchiver"]
    COLL["collector.py<br/>DataCollector (на инструмент)"]
    FACT["main.make_stream_factory()<br/>create_stream()"]
    REPO["database.py<br/>MarketDataRepository (на инструмент)"]
    READ["reader.py<br/>get_cluster_data()"]
    SDK["t_tech.invest (SDK)<br/>импортируется только в main.py"]
    STORE[("SQLite + Parquet")]
    BOT["Торговый робот"]

    MAIN --> CFG
    MAIN -->|"при старте"| MAINT
    MAIN --> SCHED
    SCHED -->|"ежесуточно"| MAINT
    MAINT -->|"1"| AUTO
    MAINT -->|"2"| ARCH
    SCHED -.->|"request_reconnect()<br/>непрерывные фьючерсы"| COLL
    MAIN -->|"3. создаёт и запускает"| COLL
    MAIN --> FACT
    FACT --> SDK
    MAIN -->|"сервисы SDK как duck-typed объекты"| AUTO
    COLL -->|"StreamFactory (инъекция)"| FACT
    AUTO --> REPO
    ARCH --> REPO
    COLL --> REPO
    READ --> REPO
    REPO --> STORE
    BOT --> READ
```

## 2.2. Модули

| Модуль | Ключевые объекты | Ответственность | Зависит от |
|--------|------------------|-----------------|------------|
| `config.py` | `AppConfig`, `BatchConfig`, `ArchiverConfig`, `Instrument`, `InstrumentType`, `Direction`, `FuturesMeta`, `nearest_contract`, `parse_direction` | Разбор `config.yaml`, доменная модель: правило `storage_name`, выбор активного контракта, нормализация направления | `pyyaml` |
| `database.py` | `MarketDataRepository`, `DataCorruptionError`, `HOT_RETENTION_DAYS = 7` | Единственная точка доступа к SQLite и Parquet | `config`, `pandas`, `pyarrow`, `sqlite3` |
| `collector.py` | `DataCollector`, `Tick`, `StreamHandle`, `StreamFactory`, `InstrumentNotFound`, `_to_dto` | Producer/Consumer, переподключение, отметки неполных дней | `config`, `database` |
| `autoload.py` | `run_backfill`, `missing_days`, `refresh_days`, `backfill_day`, `BackfillResult`, `_RateGate` | Догрузка отсутствующих и перекачка отмеченных дней | `config`, `database`, `aiohttp`, `truststore` |
| `archiver.py` | `DataArchiver`, `run_archiver`, `seconds_until_next_run` | Перенос прошедших дней в Parquet, очистка SQLite, расчёт расписания | `config`, `database` |
| `reader.py` | `get_cluster_data` | Footprint-матрица для робота | `config`, `database`, `pandas` |
| `main.py` | `main`, `amain`, `run_maintenance`, `run_scheduler`, `create_stream`, `_resolve_stream_figi` | Точка входа, DI-сборка, ночное обслуживание, сигналы, работа с SDK | все модули, SDK (опционально) |

## 2.3. Внедрение зависимостей

Глобального состояния нет: все зависимости передаются через конструкторы и
параметры функций.

| Потребитель | Что внедряется | Кем создаётся |
|-------------|----------------|---------------|
| `MarketDataRepository(repo_path, instrument)` | `AppConfig.storage_root`, `Instrument` | `amain` — по одному на `storage_name` |
| `DataCollector(config, instrument, repo, stream_factory=)` | `AppConfig`, `Instrument`, репозиторий, `StreamFactory` | `amain` |
| `StreamFactory = Callable[[Instrument], Awaitable[StreamHandle]]` | `AppConfig` (замыкание) | `make_stream_factory(config)` |
| `run_backfill(config, repos, market_data=, instruments=, kind_share=, kind_futures=, futures_status=)` | сервисы SDK и значения enum SDK | `run_maintenance` (если SDK установлен) |
| `run_scheduler(config, job)` | задача обслуживания (`Callable[[], Awaitable[None]]`) | `amain` |
| `DataArchiver(config, repo)` | `AppConfig`, репозиторий | `run_archiver` |
| `get_cluster_data(config, repo, ...)` | `AppConfig`, репозиторий | потребитель (робот) |

SDK импортируется **только** в `main.py` (флаг `_has_sdk`). Остальные модули
получают объекты SDK как duck-typed параметры, поэтому работают и
тестируются без SDK.

## 2.4. Модель конкурентности

- Один процесс, один event loop (`asyncio.run(amain(config))`).
- На каждый инструмент — две задачи: `DataCollector.run()` (продюсер +
  переподключение) и `DataCollector._consumer()`; плюс одна задача
  планировщика ночного обслуживания.
- Блокирующий I/O к SQLite/Parquet выполняется в пуле потоков через
  `asyncio.to_thread`; каждое обращение открывает собственное подключение
  (`MarketDataRepository.session()`), поэтому подключения не разделяются
  между потоками.
- Догрузка: до 4 одновременных задач (`MAX_BACKFILL_CONCURRENCY`) и
  глобальный ограничитель темпа запросов к архиву (`_RateGate`,
  интервал `60/28 ≈ 2,14 с`).

## 2.5. Жизненный цикл процесса

```mermaid
sequenceDiagram
    autonumber
    participant OS as ОС / оператор
    participant M as main()
    participant A as amain()
    participant BF as autoload.run_backfill
    participant AR as archiver.run_archiver
    participant C as DataCollector × N
    participant R as MarketDataRepository × N

    OS->>M: cluster-source
    M->>M: setup_logging()
    M->>M: AppConfig.from_yaml(config.local.yaml | config.yaml)
    M->>A: asyncio.run(amain(config))
    A->>R: создать репозиторий на каждый storage_name
    rect rgba(127,127,127,0.1)
    Note over A,AR: run_maintenance (при старте и ежесуточно)
    A->>BF: run_backfill (SDK-клиент открывается и закрывается вокруг вызова)
    Note over A,BF: ошибка логируется и не прерывает работу
    A->>AR: run_archiver — архивировать прошедшие дни, очистить SQLite
    end
    A->>C: create_task(run()) для каждого инструмента
    A->>A: create_task(run_scheduler) — ночное обслуживание + переподключение непрерывных фьючерсов
    A->>A: ждать stop_event (SIGINT / SIGTERM)
    OS-->>A: SIGINT / SIGTERM
    A->>A: отменить планировщик
    A->>C: task.cancel() + gather
    C->>C: teardown стрима, консьюмер дописывает очередь (≤ 30 с)
    A->>C: shutdown()
    A-->>M: 0
```

Корректное завершение гарантирует запись накопленных тиков при отмене в
любом состоянии коллектора: дренаж очереди выполняется в `finally`.

## 2.6. Архитектурные инварианты

| № | Правило | Как обеспечивается | Где |
|---|---------|--------------------|-----|
| A1 | Нет сырого SQL вне репозитория | Весь SQL — внутри `MarketDataRepository`; остальные модули вызывают его методы | `database.py` |
| A2 | Нет записи в БД из gRPC-потока | Продюсер только кладёт `Tick` в `asyncio.Queue` и копит отметки дней в памяти; пишет консьюмер | `collector.py` |
| A3 | Пакетная запись в одной транзакции | `insert_ticks`: `BEGIN` → `executemany` → `COMMIT`/`ROLLBACK` | `database.py` |
| A4 | WAL обязателен | `session()` выполняет `PRAGMA journal_mode=WAL` при каждом подключении | `database.py` |
| A5 | DI вместо глобальных объектов | Конфигурация, репозитории, фабрика стрима, сервисы SDK — через параметры | все модули |
| A6 | SDK только в `main.py` | Опциональный импорт с флагом `_has_sdk` | `main.py` |
| A7 | Только UTC | `_iso()` приводит время к UTC и всегда пишет микросекунды; чтение — `pd.to_datetime(utc=True, format="ISO8601")` | `database.py`, `autoload.py`, `collector.py` |
| A8 | Строки SQLite удаляются только для дней, у которых есть проверенный Parquet | `write_parquet` бросает `DataCorruptionError`; перед очисткой `run_once` сверяет дни SQLite раньше `cutoff` с Parquet | `archiver.py` |
| A9 | Граница очистки совпадает с маршрутизацией догрузки | `_hot_purge_cutoff()` и `_target_for_day()` используют одну границу `today − 7` | `archiver.py`, `autoload.py` |
| A11 | Одно правило выбора контракта непрерывного фьючерса | `config.nearest_contract` используют и стрим, и догрузка | `config.py`, `main.py`, `autoload.py` |
| A10 | TLS-проверка всегда включена | `_ssl_context()` — `truststore`, фолбэк `certifi`; без `_create_unverified_context` | `autoload.py` |

## 2.7. Обработка ошибок

| Ситуация | Компонент | Реакция |
|----------|-----------|---------|
| Тикер не найден (`InstrumentNotFound`) | `DataCollector.run` | Одно предупреждение, коллектор инструмента останавливается без повторов |
| Ошибка открытия стрима (сеть, gRPC) | `DataCollector.run` | SDK-клиент закрывается; повтор с экспоненциальной задержкой 2 → 4 → … → 60 с |
| Ошибка/закрытие стрима во время чтения | `DataCollector.run` | `teardown`, повтор с той же экспоненциальной задержкой; сброс после 60 с стабильной работы |
| Ошибка записи пакета | `DataCollector._consumer` | Лог `Ошибка флаша консьюмера`, пакет **отбрасывается** |
| Ошибка записи отметок дней | `DataCollector._consumer` | Лог, отметки возвращаются в память и пишутся при следующей записи |
| Любая ошибка догрузки | `run_maintenance` | Лог `Ошибка автозагрузки`, работа продолжается |
| Parquet не создан или пуст | `DataArchiver` | Лог `АРХИВАЦИЯ НЕ УДАЛАСЬ`; если день старше `cutoff` — очистка SQLite отменяется целиком |
| HTTP 429 от архива | `_backfill_from_archive` | До 3 попыток, пауза `Retry-After` (по умолчанию 60 с) |
| Ошибка чтения Parquet-файла | `read_parquet` | Предупреждение, файл пропускается |
