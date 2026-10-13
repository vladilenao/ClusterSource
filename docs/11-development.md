# 11. Разработка

## 11.1. Команды

```bash
pip install -e ".[dev]"
```

| Задача | Команда | Примечание |
|--------|---------|------------|
| Линтер | `ruff check .` | правила `E, F, I, W, UP`, длина строки 100 |
| Форматирование | `ruff format .` | |
| Типизация | `mypy src/` | `strict`, `python_version = 3.13` |
| Тесты | `pytest -q` | `asyncio_mode = auto`, 141 тест |

Порядок проверки после изменений: `ruff check` → `mypy src/` → `pytest -q`.

## 11.2. Тесты

| Файл | Что покрывает |
|------|---------------|
| `tests/conftest.py` | Фабрика `AppConfig` и репозитория на `tmp_path` |
| `tests/test_config.py` | Разбор YAML, `storage_name`, переопределение токена, шаблон тикера контракта, `nearest_contract`, `parse_direction` |
| `tests/test_database.py` | Вставка/чтение SQLite, WAL, Parquet (точный фильтр по времени), очистка, `replace_hot_day`, отметки перекачки, смешанные форматы времени |
| `tests/test_collector.py` | `_to_dto`, приём сделок `deliver`, пакетная запись по интервалу, дренаж при отмене и `shutdown`, отметки дней (`compromised`, `stream_gap`) |
| `tests/test_autoload.py` | Разбор CSV (`history-trades` и легаси), ключи архива и выбор контракта, FIGI на сегодня, выбор акции, `GetLastTrades`, `missing_days`, `refresh_days`, перекачка и судьба отметок |
| `tests/test_archiver.py` | Перенос дней в Parquet, архивация всех прошедших дней, отмена очистки без Parquet, расписание |
| `tests/test_exchange_only.py` | SPEC-0002: разбор источника сделки, подписка `TRADE_SOURCE_EXCHANGE`, фильтр стрима/архива/`GetLastTrades`, чистка `dealer_cleanup`, удаление дня без биржевых сделок |
| `tests/test_iss.py` | SPEC-0003: нормализация ленты ISS (время МСК → UTC, сессии выходного дня, FORTS), догонка с неполными и пустыми страницами, смена дня, отсутствие дублей, импорт этапа 1, проверка полноты по `NUMTRADES` |
| `tests/test_stream_hub.py` | SPEC-0009: одно соединение, раздача сделок по FIGI/UID, переподключение без новой резолюции, экспоненциальная пауза, неподтверждённая подписка, ролловер с резолюцией |
| `tests/test_quality.py` | SPEC-0004: условия и отметки минут, события `DATA_DEGRADED`/`DATA_RESTORED`, каждый детектор, тишина вне `NORMAL_TRADING`, сохранение консьюмером, функции чтения |
| `tests/test_keep_awake.py` | SPEC-0010: `caffeinate -i -w PID` на macOS, отключение настройкой, реальный запрет снимается с выходом процесса |
| `tests/test_reconcile.py` | SPEC-0005: VERIFIED с номерами, дозаливка REPAIRED, GAP (пусто у ISS, лишние сделки, сторона), граница ленты и «сейчас − 16 мин», архивные дни, идемпотентность, ожидание при backlog, миграция старой базы |
| `tests/test_reader.py` | Footprint, выбор источника по дню, точные границы интервала, пустой интервал, одинаковые сделки, `UNKNOWN`, округление |

Тесты не требуют SDK и сети: внешние сервисы заменяются duck-typed
заглушками.

## 11.3. Правила кода

- Соблюдать инварианты [A1–A10](02-architecture.md#26-архитектурные-инварианты).
- Новый доступ к данным — только новым методом `MarketDataRepository`.
- Строки данных типизируются как `dict[str, object]`; приведение — через
  `_as_float` / `_as_int`.
- В тестах использовать `datetime.UTC` (правило ruff `UP017`).
- Продюсер не пишет в БД даже отметки дней: они копятся в `_pending_marks` и сохраняются консьюмером.
- Время в хранилище — через `_iso()` (UTC, микросекунды); строки времени разбирать с `format="ISO8601"`.
- Выбор контракта непрерывного фьючерса — только через `config.nearest_contract`.
- SDK импортировать только в `main.py`.

## 11.4. Синхронизация документации с кодом

Любое изменение поведения начинается со спецификации в
[specs/](specs/README.md). Реализация гарантии помечается в коде комментарием
`# SPEC-NNNN G-N`, тест — тем же идентификатором в docstring.

| Изменение в коде | Обновить |
|------------------|----------|
| Новое или изменённое поведение | спецификация в [specs/](specs/README.md): статус, приёмка, журнал решений |
| Ключ `config.yaml` или значение по умолчанию | [04-configuration.md](04-configuration.md), `contracts/schemas/config.schema.json` |
| Колонка SQLite/Parquet, формат `trade_id` | [03-data-model.md](03-data-model.md), `contracts/schemas/tick.schema.json` |
| Детекторы, причины, пороги качества | [05 § 5.8](05-realtime-collection.md#58-детекторы-деградации-spec-0004), [08 § 8.3.1](08-footprint-reader.md#831-качество-данных-spec-0004), SPEC-0004 |
| Колонки результата `get_cluster_data` | [08-footprint-reader.md](08-footprint-reader.md), `contracts/schemas/footprint-row.schema.json` |
| Лента/итоги ISS, колонки `iss_trades` | [03-data-model.md § 3.7](03-data-model.md#37-лента-мосбиржи-iss), [09](09-external-integrations.md), `contracts/openapi/moex-iss.openapi.yaml` |
| Вызов внешнего API, URL, поля запроса/ответа | [09-external-integrations.md](09-external-integrations.md), `contracts/openapi/*`, `contracts/asyncapi/*` |
| Маппинг полей (`_to_dto`, `_trade_to_row`, `_history_rows`) | таблицы маппинга в [05](05-realtime-collection.md), [06](06-history-backfill.md) |
| Константы задержек, лимитов, удержания | [05](05-realtime-collection.md), [06](06-history-backfill.md), [07](07-archiving.md) |
| Новое ограничение / исправленное расхождение | [12-known-issues.md](12-known-issues.md): `L-NN` или `KI-NN` со ссылками на код и тесты |

Проверка синтаксиса контрактов:

```bash
python -c "import yaml,json,glob; [yaml.safe_load(open(f)) for f in glob.glob('docs/contracts/**/*.yaml', recursive=True)]; [json.load(open(f)) for f in glob.glob('docs/contracts/schemas/*.json')]"
```
