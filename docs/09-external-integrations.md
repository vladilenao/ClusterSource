# 9. Внешние интеграции

Интеграции I1–I8 — с API T-Investments. Аутентификация везде одна: токен из
`TOKEN` (`Authorization: Bearer <token>` для HTTP; метаданные gRPC
формирует SDK). I9–I10 — информационно-статистический сервер Мосбиржи (ISS),
без аутентификации, данные с задержкой 15 минут.

## 9.1. Сводка

| № | Интеграция | Протокол | Endpoint | Кто вызывает | Когда | Контракт |
|---|------------|----------|----------|--------------|-------|----------|
| I1 | Поток сделок | gRPC bidi-stream | `invest-public-api.tbank.ru:443` · `MarketDataStreamService/MarketDataStream` | `main.make_connection_factory` → `stream_hub.StreamHub` (одно соединение на все инструменты) | постоянно | [AsyncAPI](contracts/asyncapi/t-invest-market-data-stream.asyncapi.yaml) |
| I2 | Поиск акции | gRPC unary | `InstrumentsService/FindInstrument` | `main._resolve_stream_figi`, `autoload._resolve_share_figi_grpc` | при открытии стрима; в обслуживании | SDK |
| I3 | Список фьючерсов | gRPC unary | `InstrumentsService/Futures` | `main._resolve_stream_figi` (статус `BASE`), `autoload._fetch_futures_grpc` (статус `ALL`) | при открытии стрима; в обслуживании | SDK |
| I4 | Последние сделки | gRPC unary | `MarketDataService/GetLastTrades` | `autoload._backfill_current_session` | в обслуживании, для отсутствующего `today` | SDK |
| I5 | Архив сделок | HTTPS GET | `https://invest-public-api.tbank.ru/history-trades/{date}` | `autoload._backfill_from_archive` | в обслуживании (при старте и ежесуточно), для прошлых дней | [OpenAPI](contracts/openapi/t-invest-rest.openapi.yaml) `getHistoryTrades` |
| I6 | Поиск акции (фолбэк) | HTTPS POST | `https://invest-public-api.tinkoff.ru/rest/…InstrumentsService/FindInstrument` | `autoload._resolve_share_figi_rest` | без SDK | OpenAPI `findInstrument` |
| I7 | Список фьючерсов (фолбэк) | HTTPS POST | `…/InstrumentsService/Futures` | `autoload._fetch_futures_rest` | без SDK | OpenAPI `listFutures` |
| I8 | Последние сделки (фолбэк) | HTTPS POST | `…/MarketDataService/GetLastTrades` | `autoload._backfill_current_session` | без SDK | OpenAPI `getLastTrades` |
| I9 | Лента сделок Мосбиржи | HTTPS GET | `https://iss.moex.com/iss/engines/{stock/markets/shares, futures/markets/forts}/trades.json` | `iss.IssCollector.poll_market` | раз в `iss.poll_interval_sec` | [OpenAPI](contracts/openapi/moex-iss.openapi.yaml) `getTrades` |
| I11 | Минутные свечи Мосбиржи | HTTPS GET | `…/iss/engines/…/securities/{SECID}/candles.json?interval=1` | `tail.check_tail` (из `final.run_final_check`) | ночью, раз на инструмент и торговый день с лентой — хвост вечерней сессии ([SPEC-0011](specs/0011-session-tail.md)) | [OpenAPI](contracts/openapi/moex-iss.openapi.yaml) `getCandles` |
| I12 | Redis Stream событий для робота | Redis (RESP) | `{signals.redis.host}:{port}`, stream `cluster.events` | `signals.publisher.EventPublisher` | раз в секунду outbox → `XADD`; раз в сутки `XTRIM MINID` ([SPEC-0008](specs/0008-volume-signals.md)) | [AsyncAPI](contracts/asyncapi/cluster-source-events.asyncapi.yaml) |
| I10 | Дневные итоги Мосбиржи | HTTPS GET | `…/iss/history/engines/…/securities.json?date=` | `iss.IssCollector.verify_pending`, `final.run_final_check` | раз в час для непроверенных дней ленты; ночью для вердикта дня (кэш в `_iss.sqlite3`) | [OpenAPI](contracts/openapi/moex-iss.openapi.yaml) `getHistory` |

Унарные gRPC-методы I2–I4 описаны в [OpenAPI](contracts/openapi/t-invest-rest.openapi.yaml)
через их REST-эквиваленты I6–I8: сообщения совпадают (REST-шлюз — это
grpc-gateway над тем же protobuf-контрактом `tinkoff.public.invest.api.contract.v1`).

Легаси-архив `https://tbank.ru/invest-api/data-api/...` выведен из
эксплуатации (HTTP 404) и не должен использоваться.

## 9.2. Матрица маппинга типа инструмента

| `InstrumentType` (config) | SDK `InstrumentType` (I2) | REST `instrument_kind` (I6) | Классы архива (I5) | Резолюция для стрима (I1) |
|---------------------------|---------------------------|-----------------------------|--------------------|---------------------------|
| `share` | `INSTRUMENT_TYPE_SHARE` (2) | `2` | `TQBR`, `SPBXM` | `FindInstrument`, точный тикер, приоритет `TQBR` → `SPBXM` |
| `futures_contract` | — (берётся из `Futures`) | — | `class_code` из справочника, иначе `SPBFUT` | `Futures`, точный тикер |
| `continuous_futures` | — (берётся из `Futures`) | — | `class_code` контракта, активного в день | `Futures`, `nearest_contract`: шаблон `{id}{месяц}{год}`, экспирация ≥ сегодня, минимальная |

Статус инструментов для `Futures`: стрим — `INSTRUMENT_STATUS_BASE` (REST `1`, торгуемые); догрузка — `INSTRUMENT_STATUS_ALL` (REST `2`, включая истёкшие контракты для прошедших дней).

## 9.3. Маппинг перечислений и типов

| Тип API | Значения API | Значение в ClusterSource |
|---------|--------------|--------------------------|
| `TradeDirection` | `TRADE_DIRECTION_BUY` (1) | `BUY` |
| `TradeDirection` | `TRADE_DIRECTION_SELL` (2) | `SELL` |
| `TradeDirection` | `TRADE_DIRECTION_UNSPECIFIED` (0) | `UNKNOWN` |
| `DIRECTION` (CSV I5) | `BUY` / `SELL`; иное или колонки нет | `BUY` / `SELL`; `UNKNOWN` (`parse_direction`) |
| `TradeSourceType` | `TRADE_SOURCE_EXCHANGE` (1) | сохраняется |
| `TradeSourceType` | `TRADE_SOURCE_DEALER` (2), `TRADE_SOURCE_ALL` (3), иное явное значение | отбрасывается (`parse_trade_source`, SPEC-0002) |
| `TradeSourceType` | `TRADE_SOURCE_UNSPECIFIED` (0), поля нет | сохраняется (фильтр стоит в запросе) |
| `TRADE_SOURCE` (CSV I5) | `EXCHANGE` / `DEALER` | `EXCHANGE` сохраняется, `DEALER` отбрасывается |
| `Quotation` | `{units: int64, nano: int32}` | `float(units) + nano / 1e9` |
| `google.protobuf.Timestamp` | SDK → `datetime` (UTC); REST → строка RFC 3339 | `datetime` UTC |
| int64 в REST JSON | строка, например `"312"` | `int()` / `float()` |

## 9.4. Ограничения и семантика ошибок

| Интеграция | Ограничение / ответ | Реакция ClusterSource |
|------------|---------------------|-----------------------|
| I5 | ~30 файлов/мин на IP | Глобальный темп ≤ 28/мин (`_RateGate`) |
| I5 | `404` — торгов не было или архив ещё не опубликован | Следующий ключ-кандидат, затем `NO_DATA`; отметка перекачки сохраняется, если у дня есть данные |
| I5 | `429` + `Retry-After` | Ожидание и до 3 попыток |
| I5 | Архив публикуется ночью, текущий день недоступен | Для `today` используется I4/I8, день отмечается `partial_backfill` |
| I6 | Периодически отвечает 404 | Основной путь — I2 через SDK |
| I1, I4, I5, I8 | По умолчанию `trade_source = TRADE_SOURCE_ALL`: вместе с биржевыми приходят дилерские сделки Т-Банка | I1 и I8 запрашивают `TRADE_SOURCE_EXCHANGE`; I4 (обёртка SDK без параметра) и I5 фильтруются по источнику каждой сделки |
| I9 | Бесплатно — только текущий торговый день, с задержкой 15 мин | Сбор непрерывно; день, не догнанный до смены ленты, помечается `ISS_FEED_INCOMPLETE` |
| I9 | Параметр `start` устарел: на глубоких смещениях (от ~1,3 млн строк) — пустые страницы | Курсор `tradeno` (акции) / `recno` (FORTS) с `next_trade=1`; пустой ответ перепроверяется 2 раза |
| I9 | `previous_session=1` (FORTS) отдаёт лишь начало прошлой сессии | Не используется |
| I10 | Итоги FORTS включают блочные/переговорные сделки, которых нет в анонимных лентах (до 0,76% сделок, до 15% объёма контракта за день) | Эталон фьючерсов для FINAL — полная лента ISS; полнота ленты FORTS — с допуском 1% (L-14) |
| I10 | `VOLUME` акций — в штуках | Сравнение с нашим объёмом × `lot_size` |
| I1 | Сообщение `open_interest` содержит только `instrument_uid`, приходит только при изменении ОИ | Маршрут по UID из резолюции; ряд — изменения, а не значение на каждую сделку |
| I9 | `OPENPOSITION` — позиции обеих сторон: ΔОИ сделки кратно 2 | Разметка открытия/закрытия делит ΔОИ на 2 (SPEC-0007) |
| I9 | Сделки сессии выходного дня FORTS приходят пачкой в понедельник (`SYSTIME` 06:00) | Дата сделки — из `TRADEDATE` ([3.7](03-data-model.md#37-лента-мосбиржи-iss)) |
| I10 | Итоги за день появляются после сессии; сессия выходного дня входит в следующий торговый день | Сверка дня D не раньше 06:00 МСК D+1, по `trade_date`; без итогов 3 суток — `unverifiable` |
| I1 | Сервер закрывает стрим или стрим обрывается | Переподключение с экспоненциальной задержкой 2 → 60 с; день отмечается `stream_gap` |

## 9.5. TLS

HTTP-клиент использует `_ssl_context()`: при наличии пакета `truststore`
TLS проверяется по системному хранилищу сертификатов ОС (сертификаты
T-Investments подписаны «Russian Trusted Root CA», который есть в связке
ключей macOS, но отсутствует в `certifi`); иначе — по бандлу `certifi`.
Проверка сертификата никогда не отключается. gRPC-канал SDK настраивает
TLS самостоятельно.
