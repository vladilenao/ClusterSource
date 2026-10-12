# 12. Ограничения и история исправлений

Документ фиксирует (1) действующие ограничения системы — осознанные или
внешние, которые потребителю и оператору нужно учитывать, и (2) расхождения
между заявленным и фактическим поведением, найденные при документировании
и исправленные в коде.

## 12.1. Действующие ограничения

| ID | Ограничение | Последствие | Где учтено |
|----|-------------|-------------|------------|
| <a id="l-01"></a>L-01 | Время публикации архива `history-trades` не документировано; в 00:05 UTC архив за вчера обычно ещё недоступен | Отмеченный день перекачивается со сдвигом до суток (при следующем обслуживании) | [6.3](06-history-backfill.md#63-отметки-для-перекачки) |
| <a id="l-02"></a>L-02 | Ответ `404` архива не различает «торгов не было» и «архив ещё не опубликован» | Отметка снимается по `404` только для дня без данных | [6.3](06-history-backfill.md#63-отметки-для-перекачки) |
| <a id="l-03"></a>L-03 | `GetLastTrades` возвращает лишь последние сделки | Сегодняшний день до старта процесса неполон до перекачки из архива на следующий день | [6.5](06-history-backfill.md#65-источник-данных-по-дню) |
| <a id="l-04"></a>L-04 | В стриме `t-tech-investments` и в архиве нет `trade_id` | Проверка разрыва `trade_id` (`COMPROMISED`) работает только с легаси-SDK; полноту обеспечивают отметки `stream_gap` | [5.6](05-realtime-collection.md#56-контроль-полноты-дня) |
| <a id="l-05"></a>L-05 | Синтетический `trade_id` стрима не уникален (две сделки в одну микросекунду) | Не использовать `trade_id` как ключ | [3.4](03-data-model.md#34-идентификаторы-сделок) |
| <a id="l-06"></a>L-06 | Сделки с направлением `UNKNOWN` (аукционы, `UNSPECIFIED`) не входят ни в `buy_volume`, ни в `sell_volume` | Сумма сторон footprint может быть меньше общего объёма | [8.2](08-footprint-reader.md#82-результат) |
| <a id="l-07"></a>L-07 | Консьюмер замечает остановку не позже чем через `flush_interval_sec` | Завершение процесса занимает до `flush_interval_sec` (+ запись) | [5.5](05-realtime-collection.md#55-консьюмер-пакетная-запись) |
| <a id="l-08"></a>L-08 | Запрос переподключения срабатывает на следующем кадре стрима (сделка или ping сервера) | Смена контракта непрерывного фьючерса после ночного обслуживания — с задержкой до интервала ping | [5.7](05-realtime-collection.md#57-переподключение) |
| <a id="l-09"></a>L-09 | `Instrument.lot_size` — справочное поле | Объёмы хранятся в лотах, пересчёт в штуки не выполняется | [04](04-configuration.md#43-элемент-tickers) |
| <a id="l-11"></a>L-11 | Обёртка `get_last_trades` в SDK `t-tech-investments` 1.49 не принимает `trade_source` | Сервер отдаёт все сделки; дилерские отбрасываются по полю сделки. Если поле не заполнено, сегодняшний день может содержать дилерские сделки до ночной перекачки из архива | [6.5](06-history-backfill.md#65-источник-данных-по-дню) |
| <a id="l-10"></a>L-10 | Строки SQLite, записанные до унификации формата, могут быть без дробной части секунд | Читаются корректно (`format="ISO8601"`); сортировка строк остаётся верной | [3.3.1](03-data-model.md#331-sqlite-горячее-хранилище) |

## 12.2. Исправленные расхождения

Идентификаторы `KI-NN` сохранены для трассировки.

| ID | Было | Стало | Код | Тесты |
|----|------|-------|-----|-------|
| <a id="ki-01"></a>KI-01 | Архивация только при старте; `archiver.run_hour_utc/run_minute_utc` не использовались | Ночное обслуживание (догрузка + архивация) по расписанию `archiver.*`; после него коллекторы непрерывных фьючерсов переподключаются | `main.run_scheduler`, `main.run_maintenance`, `archiver.seconds_until_next_run` | `test_seconds_until_next_run`, `test_request_reconnect_reopens_stream` |
| <a id="ki-02"></a>KI-02 | Архивировался только «вчера»; прочие дни SQLite удалялись без Parquet | Архивируются все прошедшие дни без Parquet; очистка SQLite отменяется, если у удаляемого дня нет Parquet | `DataArchiver.run_once` | `test_run_once_archives_every_unarchived_past_day`, `test_purge_skipped_when_archive_fails` |
| <a id="ki-03"></a>KI-03 | Маркер `COMPROMISED` только логировался | День отмечается в `days_to_refresh` и перекачивается из архива | `DataCollector._flag_gap`, `autoload.refresh_days` | `test_gap_and_stream_open_mark_days_via_consumer`, `test_refresh_days_window_and_today` |
| <a id="ki-04"></a>KI-04 | Догрузка непрерывного фьючерса могла взять контракт другого актива | Контракт определяется по шаблону `{id}{месяц}{год}` и экспирации ≥ дня — одно правило для стрима и догрузки; справочник включает истёкшие контракты | `config.nearest_contract`, `Instrument.is_contract_ticker` | `test_continuous_key_ignores_other_underlyings`, `test_nearest_contract_*` |
| <a id="ki-05"></a>KI-05 | Parquet в ридере фильтровался с точностью до дня | Точный фильтр `[start, end)`, день `end_time` читается | `MarketDataRepository.read_parquet` | `test_read_parquet_filters_by_exact_time`, `test_cold_end_day_included_and_bounds_respected` |
| <a id="ki-06"></a>KI-06 | Запись при каждом опустошении очереди (транзакция почти на тик) | Запись по `batch_size` или раз в `flush_interval_sec` | `DataCollector._consumer` | `test_consumer_batches_by_interval` |
| <a id="ki-07"></a>KI-07 | Ошибки стрима поглощались, задержка всегда 2 с | Ошибки пробрасываются, задержка растёт 2 → 60 с и сбрасывается после 60 с стабильной работы | `DataCollector._stream_loop`, `_next_delay` | `test_stream_errors_back_off_exponentially`, `test_next_delay_is_exponential` |
| <a id="ki-08"></a>KI-08 | Отмена во время паузы переподключения пропускала дренаж очереди | Дренаж в `finally` при любом выходе из цикла | `DataCollector.run` | `test_cancel_during_backoff_drains_queue` |
| <a id="ki-09"></a>KI-09 | Частично догруженный сегодняшний день навсегда считался полным; REST слал поле `count` вне контракта | День отмечается `partial_backfill` и перекачивается; `count` удалён | `autoload._backfill_current_session` | `test_refresh_days_window_and_today`, `test_settle_refresh_mark` |
| <a id="ki-10"></a>KI-10 | `UNSPECIFIED` → `SELL`; CSV без направления → `BUY` | Оба случая → `UNKNOWN` | `config.parse_direction` | `test_parse_direction`, `test_to_dto_unspecified_direction_is_unknown`, `test_history_rows_direction_normalized` |
| <a id="ki-11"></a>KI-11 | Неточности `README.md` | README исправлен | `README.md` | — |
| <a id="ki-12"></a>KI-12 | REST-резолюция подставляла тикер вместо FIGI; при отсутствии точного совпадения брался первый результат | Только точный тикер (приоритет `TQBR` → `SPBXM`), иначе `None`; FIGI фьючерсов — из справочника | `autoload._pick_share`, `autoload.current_figi` | `test_pick_share_exact_ticker_and_class_priority`, `test_current_figi_for_today` |
| <a id="ki-13"></a>KI-13 | Неиспользуемые `reader._sqlite_only`, `ArchiverConfig` | Функция удалена; `ArchiverConfig` используется (KI-01); `lot_size` оставлен как справочное поле (L-09) | `reader.py` | — |
| <a id="ki-14"></a>KI-14 | `get_cluster_data` бросал `ValueError` при пустом интервале | Возвращается пустой `DataFrame` с колонками результата | `reader.get_cluster_data` | `test_empty_interval_returns_empty_frame` |
| <a id="ki-15"></a>KI-15 | Дедупликация по всем полям схлопывала реальные одинаковые сделки | Источник выбирается по дню целиком (Parquet приоритетнее), дедупликации нет | `reader.get_cluster_data` | `test_identical_trades_are_not_collapsed`, `test_hot_and_cold_overlap_not_double_counted` |
| <a id="ki-16"></a>KI-16 | Смешанные форматы времени в SQLite (с микросекундами и без) роняли чтение; в CSV архива такие строки молча отбрасывались | Запись всегда с микросекундами; чтение и разбор CSV с `format="ISO8601"` | `MarketDataRepository._iso`, `read_ticks`, `autoload._parse_history_csv` | `test_read_ticks_mixed_timestamp_formats`, `test_parse_history_csv_keeps_rows_without_fraction` |
| <a id="ki-18"></a>KI-18 | Стрим, `GetLastTrades` и архив по умолчанию отдают и дилерские сделки Т-Банка (`TRADE_SOURCE_ALL`); они сохранялись вместе с биржевыми (ночные часы, выходные без торгов на бирже) | Только биржевые сделки: фильтр в запросе и по источнику сделки; ранее сохранённые дни перекачиваются однократной чисткой `dealer_cleanup` ([SPEC-0002](specs/0002-exchange-only-trades.md)) | `main.build_trades_subscription`, `DataCollector.subscribe`, `autoload._exchange_only`, `autoload._trades_to_rows`, `autoload.schedule_dealer_cleanup`, `main.run_dealer_cleanup` | `tests/test_exchange_only.py` |
| <a id="ki-17"></a>KI-17 | При ошибке резолюции в `create_stream` не закрывался `AsyncClient` (утечка канала на каждый повтор) | Клиент закрывается при любой ошибке подготовки стрима | `main.create_stream` | смоук-тест |

KI-15 – KI-17 обнаружены при проверке исправлений: прогоном на реальном API
и ревью кода.
