# 4. Конфигурация

## 4.1. Выбор файла

`main()` читает конфигурацию из текущего рабочего каталога:

1. `config.local.yaml` — если существует (в `.gitignore`, для токена и
   локальных путей);
2. иначе `config.yaml`.

Файлы не объединяются: используется ровно один. Переменные окружения не
читаются (параметр `AppConfig.from_yaml(token=...)` существует, но `main()`
его не передаёт).

JSON Schema файла: [contracts/schemas/config.schema.json](contracts/schemas/config.schema.json).
Код не валидирует конфигурацию по схеме; схема служит документацией и
может применяться внешним валидатором, например:

```bash
check-jsonschema --schemafile docs/contracts/schemas/config.schema.json config.yaml
```

## 4.2. Параметры верхнего уровня

| Ключ YAML | Тип | По умолчанию | Поле `AppConfig` | Используется | Описание |
|-----------|-----|--------------|------------------|--------------|----------|
| `TOKEN` (или `token`) | string | `""` | `token` | SDK, REST, `history-trades` | Токен T-Investments. Пустое значение или `your-token-here` → предупреждение при старте, стриминг не работает |
| `data_dir` | string (путь) | `data` | `data_dir` | репозитории | Корень данных; хранилище — `{data_dir}/history` (`storage_root`). Относительный путь — от рабочего каталога |
| `batch.flush_interval_sec` | number | `5.0` | `batch.flush_interval_sec` | консьюмер | Период записи пакета, с: тики попадают в SQLite не реже раза за интервал ([5.5](05-realtime-collection.md#55-консьюмер-пакетная-запись)) |
| `batch.batch_size` | integer | `1000` | `batch.batch_size` | консьюмер | Размер пакета, при котором запись выполняется немедленно |
| `archiver.run_hour_utc` | integer 0–23 | `0` | `archiver.run_hour_utc` | планировщик | Час ежесуточного обслуживания (догрузка + архивация), UTC |
| `archiver.run_minute_utc` | integer 0–59 | `5` | `archiver.run_minute_utc` | планировщик | Минута ежесуточного обслуживания, UTC |
| `history_check_days` | integer ≥ 1 | `14` | `history_check_days` | догрузка | Окно в днях (включая сегодняшний) для догрузки отсутствующих и перекачки отмеченных дней; более старые отметки снимаются |
| `iss.enabled` | boolean | `true` | `iss.enabled` | `amain` | Собирать ленту Мосбиржи из ISS ([SPEC-0003](specs/0003-iss-collection.md)) |
| `iss.poll_interval_sec` | number | `60` | `iss.poll_interval_sec` | `IssCollector` | Пауза между циклами догонки ленты, с |
| `signals.enabled` | boolean | `false` | `signals.enabled` | `amain` | Сигналы по объёмам и их публикация в Redis Stream ([SPEC-0008](specs/0008-volume-signals.md)) |
| `signals.redis.host` / `port` / `db` | string / integer / integer | `127.0.0.1` / `6379` / `0` | `signals.redis.*` | `EventPublisher` | Redis робота; пароль — только переменная окружения `CLUSTER_SOURCE_REDIS_PASSWORD` |
| `signals.redis.stream` | string | `cluster.events` | `signals.redis.stream` | `EventPublisher` | Redis Stream событий ([AsyncAPI](contracts/asyncapi/cluster-source-events.asyncapi.yaml)) |
| `signals.redis.retention_days` | integer | `90` | `signals.redis.retention_days` | `EventPublisher` | Хранение событий: ежесуточный `XTRIM MINID`; столько же хранятся сигналы в `_signals.sqlite3` |
| `signals.verify_deadline_min` | number | `20` | `signals.verify_deadline_min` | `SignalEngine` | Срок итога проверки сигнала после конца окна (SPEC-0001 G-2) |
| `signals.baseline_days` / `baseline_min_days` | integer | `20` / `5` | `signals.*` | `compute_norms` | Окно норм (`get_baseline_days`) и минимум дней, без которого детекторы инструмента выключены |
| `signals.large_aggressor_percentile` | number | `99.9` | `signals.*` | `LargeAggressorDetector` | Перцентиль размера родительской заявки по режиму торгов |
| `signals.volume_spike_ratio` / `volume_spike_percentile` | number | `5` / `90` | `signals.*` | `VolumeSpikeDetector` | Объём минуты ≥ ratio × медиана той же минуты дня и ≥ перцентиля минутных объёмов режима |
| `signals.regimes` | object | акции: утро 06:50–10:00, основная 10:00–19:00, вечер 19:00–24:00; FORTS: утро 06:50–09:00, основная 09:00–19:00, вечер 19:00–24:00 | `signals.regimes.{shares,forts}.{режим}: [начало, конец]` | `regime_of` | Режимы торгов МСК для норм; суббота и воскресенье — `weekend` |
| `keep_awake` | boolean | `true` | `keep_awake` | `main` | macOS: запрещать сон по простою, пока работает процесс (`caffeinate -i -w PID`, [SPEC-0010](specs/0010-keep-awake.md)) |
| `tickers` | array | `[]` | `instruments` | все компоненты | Список инструментов (4.3) |

## 4.3. Элемент `tickers[]`

| Ключ | Тип | Обязателен | По умолчанию | Поле `Instrument` | Описание |
|------|-----|------------|--------------|-------------------|----------|
| `id` | string | да | — | `id` | Тикер акции/контракта или код базового актива непрерывного фьючерса (регистр важен: `Si`, а не `SI`) |
| `type` | enum | да | — | `type` | `share` \| `futures_contract` \| `continuous_futures` |
| `tick_size` | number > 0 | нет | `0.01` | `tick_size` | Шаг цены для округления footprint |
| `lot_size` | integer ≥ 1 | нет | `1` | `lot_size` | Размер лота; справочное поле ([L-09](12-known-issues.md#l-09)) |

Неизвестное значение `type` вызывает `ValueError` при запуске. Неизвестные
ключи игнорируются.

Если два элемента имеют одинаковый `storage_name`, в `amain` остаётся один
репозиторий на ключ, но коллекторов создаётся два — дубликатов в списке
следует избегать.

## 4.4. Пример

```yaml
TOKEN: "t.xxxxxxxx"            # лучше хранить в config.local.yaml
data_dir: "/srv/cluster-source"

batch:
  flush_interval_sec: 5
  batch_size: 1000

archiver:                      # ночное обслуживание, UTC
  run_hour_utc: 0
  run_minute_utc: 5

history_check_days: 14

tickers:
  - id: "SBER"
    type: "share"
    tick_size: 0.01
    lot_size: 1
  - id: "Si"
    type: "continuous_futures"   # данные в {data_dir}/history/Si_CONTINUOUS/
    tick_size: 1.0
    lot_size: 1000
```

## 4.5. Выбор `type`

| Нужно | `type` | `id` | Как выбирается контракт |
|-------|--------|------|-------------------------|
| Акция основной секции MOEX | `share` | тикер, напр. `SBER` | Точное совпадение тикера; приоритет класса `TQBR`, затем `SPBXM` |
| Конкретный фьючерсный контракт | `futures_contract` | полный тикер, напр. `SiZ6` | Точное совпадение тикера среди фьючерсов |
| Склейка контрактов | `continuous_futures` | код базового актива, напр. `Si` | Контракт с тикером `{id}{код месяца}{год}` (например `SiZ6`) и ближайшей экспирацией не раньше текущего дня |
