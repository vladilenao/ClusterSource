# SPEC-0008: Торговые сигналы по объёмам

**Статус:** В работе — этап 1 готов (ждёт включения), этап 2 впереди · согласовано 2026-10-14 · **Обновлено:** 2026-10-14 · **Зависит от:** SPEC-0001, 0004, 0005, 0006, 0007, 0011

## Зачем

Робот получает уведомления о значимых событиях в объёмах сделок. Все сигналы
подчиняются контракту и гарантиям SPEC-0001: нет сигнала на известной
проблеме (G-1), каждый сигнал подтверждается или отзывается за 20 минут
(G-2), пороги — только по окончательным дням (G-3).

### Исследование 2026-10-14

- **Робот** (`My_roBot`, ветка `feature/add-market-structure-patterns`) уже
  публикует свечные паттерны в Redis Stream `market.events`: SQLite outbox →
  `XADD`, детерминированный `event_id` (sha256), `published_at` из `Redis
  TIME`, Consumer Group на потребителя, `XACK` после идемпотентной фиксации,
  ежесуточный `XTRIM MINID` на 90 суток, AOF `appendfsync always`. Контракт —
  AsyncAPI 3.0.0 (приложение A `openspec/changes/add-market-structure-patterns/proposal.md`).
  Дизайн робота: ClusterSource пишет в `market.events` только отдельным
  изменением со своей типизированной схемой.
- **Redis** развёрнут 2026-10-14: контейнер `redis` (`redis:7.4-alpine`,
  `127.0.0.1:6379`, AOF `appendfsync always`, том `redis-data`).
- **Время сделок стрима** — с микросекундами; сделки одной агрессивной заявки
  приходят с одинаковым временем и стороной (SBER 14.10, 07:00–07:30 МСК: 3 397
  сделок в 1 948 группах; пример — SELL 288 + 500 в 04:30:41.539346 UTC).
- **Хвост сессии** (SPEC-0011): минуты после ~23:44 МСК сверяются только ночью
  по свечам, за 20 минут их не проверить.

## Делаем / Не делаем

**Делаем:**
- свой Redis Stream `cluster.events` (не `market.events`): конверт в стиле
  `market.events`, `source = cluster_source`;
- события: сигналы, итоги проверки (`signal_confirmed` / `signal_revoked`),
  смена качества данных (`data_degraded` / `data_restored`);
- контракт — `docs/contracts/asyncapi/cluster-source-events.asyncapi.yaml`;
- движок сигналов в процессе: детекторы на разветвлении потока коллектора,
  отсев по G-1, проверка по вердиктам минут (SPEC-0005), публикация через outbox;
- нормы (пороги) по окну `reader.get_baseline_days` (SPEC-0006);
- детекторы MVP в два этапа (ниже).

**Не делаем:**
- публикацию в `market.events` и изменения в коде робота: потребителя
  `cluster.events` робот делает своим изменением по нашему контракту;
- торговые решения: политика по сигналам — за роботом (SPEC-0001);
- остальной каталог (sweep, перекос дельты, CVD, сессия, межинструментальные,
  ОИ) — после режима наблюдения и бэктеста на `FINAL`-днях;
- конкретные пороги «навсегда»: стартовые значения в конфигурации,
  калибровка — по режиму наблюдения.

## Каталог сигналов

### MVP, этап 1 — класс `fast`

| `event_type` | Детектирование | Окно |
|--------------|----------------|------|
| `large_aggressor` | «Родительская» заявка: сделки с одинаковым временем (до микросекунды) и стороной. Размер — в рублях у акций (`price × volume × lot_size`), в контрактах у фьючерсов. Порог — перцентиль `p` (по умолчанию 99,9) размеров родительских заявок инструмента по окну норм, отдельно для режима торгов. Сделки без стороны (аукционы, `UNKNOWN`) не участвуют | минута заявки |
| `volume_spike` | На закрытии минуты: объём минуты / медиана объёма **той же минуты дня** по окну норм ≥ `k` (по умолчанию 5) и объём ≥ перцентиля 90 минутных объёмов. Направление — по знаку дельты минуты (`neutral`, если \|дельта\| < 20% объёма) | закрытая минута |

### MVP, этап 2 — класс `level` (на закрытии бара, по умолчанию 5 минут)

| `event_type` | Детектирование |
|--------------|----------------|
| `stacked_imbalance` | Диагональный дисбаланс: покупки на цене P против продаж на P − шаг ≥ 3:1 (или продажи против покупок на P + шаг) на ≥ 3 уровнях подряд; уровни с объёмом ниже перцентиля 50 уровней по нормам не считаются. Направление — сторона дисбаланса |
| `absorption` | Уровень с наибольшим агрессивным объёмом одной стороны лежит на экстремуме бара по ходу этой агрессии (продажи — на минимуме, покупки — на максимуме), его объём ≥ перцентиля 99 объёмов уровня по нормам, а закрытие бара — не ближе `m` шагов цены (по умолчанию 2) к этому экстремуму. Направление — против поглощённой агрессии |

Каталог дальше (после MVP): sweep, перекос дельты, дивергенция CVD, POC на
экстремуме, сессия (Value Area, голый POC, VWAP), межинструментальные, ОИ
(SPEC-0007: всплеск, квадрант «цена × ОИ», ролловер).

## Правила

**Режим торгов** — нормы считаются отдельно для утренней, основной и
вечерней сессий и сессий выходного дня (границы по рынкам — в конфигурации,
время МСК). Клиринг FORTS — пауза, не режим.

**Фильтры.** Аукционы открытия и закрытия (сделки `UNKNOWN`) входят в объём,
но не в дельту и не в агрессию. Кластерные сигналы считаются только на
закрытом баре (`bar_closed = true`). Детекторы с точностью меньше секунды
(склейка заявки) не работают на `REPAIRED`-секундах: у сделок ISS время с
точностью до секунды (SPEC-0005).

**Нормы (G-3).** Окно — `reader.get_baseline_days(repo, n=20)`: `FINAL`
первыми, добор `ARCHIVE_CONSISTENT`. Нормы пересчитываются при старте и после
ночной проверки FINAL. Меньше 5 дней в окне — детектор инструмента выключен
(`SIGNAL_DETECTOR_OFF`), сигналов нет. В каждом сигнале — `baseline_quality`
(`pure` / `mixed`) и состав окна `baseline_days {final, archive}`.

**Отсев (SPEC-0001 G-1).** Сигнал не выдаётся, если инструмент сейчас
`DEGRADED` или в окне сигнала есть минута `DEGRADED` (отметки последних 3
часов движок берёт из памяти коллектора, не из БД); отсеянный сигнал
записывается со статусом `SUPPRESSED` и причиной (`degraded`,
`degraded_minute`, `signals_paused`) и не публикуется.

**Проверка (SPEC-0001 G-2).** Срок — конец окна + 20 минут. Раз в 30 секунд
для каждого непроверенного сигнала читаются вердикты минут окна (SPEC-0005):

| Вердикты минут окна | Итог |
|---------------------|------|
| все `VERIFIED` | `signal_confirmed` |
| есть `REPAIRED`, нет `GAP` | детектор пересчитывается на исправленных данных: сигнал есть — `signal_confirmed`, нет — `signal_revoked(repaired_changed_result)` |
| есть `GAP` с `extra_trades` | `signal_revoked(extra_trades)` |
| есть другой `GAP` | `signal_revoked(gap)` |
| к сроку вердиктов нет | `signal_revoked(verification_unavailable)` |

Пересчёт `large_aggressor` на `REPAIRED`: если секунда заявки не заменялась —
сигнал в силе; если заменялась (в ней не хватало сделок), заявка
пересчитывается по сделкам ISS этой секунды и стороны: размер ≥ порога —
`confirmed`, иначе `revoked(repaired_changed_result)`. Сигналы в хвосте
вечерней сессии (SPEC-0011) получают `verification_unavailable`.

**Доставка.** Событие сначала пишется в outbox (SQLite `_signals.sqlite3`),
затем фоновая задача публикует его `XADD cluster.events`; при ошибке Redis —
повтор не раньше чем через 1 с, сбор и детекторы не ждут. События качества
берутся из таблиц `quality_events` инструментов по курсору (таблица и есть
outbox). Порядок: итог сигнала публикуется после самого сигнала. Раз в сутки —
`XTRIM MINID` (время Redis − 90 суток), без `MAXLEN`. Пароль — только
переменная окружения `CLUSTER_SOURCE_REDIS_PASSWORD`.

**Разветвление потока.** `DataCollector.deliver` после фильтров отдаёт тик
движку сигналов неблокирующей очередью (`put_nowait`); переполнение — сигналы
инструмента приостанавливаются (`signals_paused`, как `DEGRADED` для G-1), сбор
не задерживается.

## Гарантии

- **G-1.** Каждый сигнал ДОЛЖЕН соответствовать контракту SPEC-0001 и
  AsyncAPI `cluster-source-events` (конверт, итог проверки, G-1…G-3).
- **G-2.** Детекторы ДОЛЖНЫ читать тики из разветвления потока коллектора, а
  не из SQLite; разветвление НЕ ДОЛЖНО задерживать сбор.
- **G-3.** Сигнал НЕ ДОЛЖЕН публиковаться, если инструмент `DEGRADED`,
  сигналы инструмента приостановлены или в окне есть минута `DEGRADED`.
- **G-4.** Каждый опубликованный сигнал ДОЛЖЕН получить ровно один итог
  (`signal_confirmed` / `signal_revoked`) не позднее конца окна + 20 минут.
- **G-5.** Событие НЕ ДОЛЖНО теряться при недоступности Redis или перезапуске
  процесса (outbox, at-least-once); повтор публикации ДОЛЖЕН нести тот же
  `event_id`.
- **G-6.** Нормы ДОЛЖНЫ строиться только по окну `get_baseline_days`; при
  окне меньше 5 дней детектор инструмента НЕ ДОЛЖЕН выдавать сигналы.

## Данные и контракты

**Redis Stream `cluster.events`** (контракт —
[cluster-source-events.asyncapi.yaml](../contracts/asyncapi/cluster-source-events.asyncapi.yaml)).
Поля сообщения — строки; объекты — JSON-строки (как в `market.events`).

| Поле | Где | Значение |
|------|-----|----------|
| `schema_version` | все | `1` |
| `event_id` | все | sha256 от `cluster_source/1/{category}/{event_type}/{ticker}/{ключ}`; ключ — окно и направление сигнала, `signal_id` итога, время и причины события качества |
| `source` | все | `cluster_source` |
| `category` | все | `signal` / `signal_result` / `data_quality` |
| `event_type` | все | `large_aggressor`, `volume_spike`, `stacked_imbalance`, `absorption` / `signal_confirmed`, `signal_revoked` / `data_degraded`, `data_restored` |
| `ticker` | все | ключ хранения ClusterSource (`SBER`, `Si_CONTINUOUS`) |
| `secid` | все | тикер биржи (`SBER`, активный контракт `SiZ6`) |
| `timestamp` | все | время рынка UTC: момент события сигнала (время заявки `large_aggressor`, конец закрытой минуты `volume_spike`); момент вердикта итога; момент смены качества |
| `published_at` | все | `Redis TIME` перед успешным `XADD` |
| `signal_id` | `signal`, `signal_result` | у сигнала = его `event_id`; у итога — `event_id` сигнала |
| `class`, `direction`, `timeframe` | `signal` | `fast`/`level`; `long`/`short`/`neutral`; `tick`, `1m`, `5m` |
| `window_from`, `window_to`, `bar_closed`, `quality` | `signal` | окно `[from, to)`; закрытый бар; `PROVISIONAL` при выдаче |
| `baseline_quality`, `baseline_days` | `signal` | `pure`/`mixed`; `{"final": n, "archive": m}` |
| `verify_deadline` | `signal` | `window_to` + 20 мин |
| `outcome` | `signal_confirmed` | `VERIFIED` / `REPAIRED` (пересчитан и сохранился) |
| `reason` | `signal_revoked` | `gap`, `repaired_changed_result`, `extra_trades`, `verification_unavailable` |
| `reasons` | `data_degraded` | причины SPEC-0004 |
| `payload` | `signal` | параметры детектора (схемы — в AsyncAPI) |

Потребитель (робот) создаёт свою Consumer Group (`XGROUP CREATE
cluster.events robot-v1 0-0 MKSTREAM`), после рестарта читает свои pending с
`0`, новые — с `>`, `XACK` — после идемпотентной фиксации по `event_id`.

**SQLite `_signals.sqlite3`** (через `SignalRepository`, инвариант A1):
`signals` (signal_id, ticker, event_type, class, direction, window_from,
window_to, verify_deadline, status `PENDING|CONFIRMED|REVOKED|SUPPRESSED`,
outcome, reason, event_json, emitted_at, resolved_at) и `outbox` (seq,
event_id, event_json, created_at, attempts, next_attempt_at, delivered_at,
last_error); `meta` — курсоры публикации событий качества по инструментам
(`quality_rowid:{ticker}`). Хранение — 90 суток. Схема —
[3.8](../03-data-model.md#38-сигналы-и-outbox-spec-0008).

**Конфигурация** (`signals:`): `enabled` (по умолчанию `false`), `redis`
(`host`, `port`, `db`, `stream`, `retention_days`), `verify_deadline_min` (20),
`baseline_min_days` (5), пороги детекторов, границы режимов торгов.

**Чтение:** `reader.get_signals(start, end, ticker=None)`,
`reader.get_signal_stats(days)` — для режима наблюдения (доля `revoked` по
инструментам и причинам, задержка итога, число подавленных).

## Наблюдаемость

- `SIGNAL X type direction: …` (INFO) — сигнал выдан.
- `SIGNAL_SUPPRESSED X type: причина` (INFO) — отсев по G-1 (счётчик).
- `SIGNAL_CONFIRMED X signal_id` (INFO), `SIGNAL_REVOKED X signal_id: reason` (WARNING).
- `SIGNAL_DETECTOR_OFF X type: в окне норм n дней` (WARNING).
- `SIGNALS_PAUSED X: очередь детекторов переполнена` (WARNING).
- `EVENTS_PUBLISH_FAILED: …; в outbox N событий` (WARNING, не чаще раза в минуту).
- Ежедневно: `SIGNAL_STATS` — сигналов, подтверждено, отозвано по причинам, подавлено.

## Приёмка

Тесты — `tests/test_signals.py`, `tests/test_events.py`.

| № | Критерий | Тест |
|---|----------|------|
| A-1 | `large_aggressor`: склейка по времени и стороне, порог по нормам, аукционы не участвуют | `test_large_aggressor_parent_order` |
| A-2 | `volume_spike`: та же минута дня, порог `k` и перцентиль | `test_volume_spike_same_minute_of_day` |
| A-3 | Отсев по G-1: `DEGRADED` сейчас, в окне, `signals_paused` (G-3) | `test_signal_suppressed_on_degraded` |
| A-4 | Итог проверки по вердиктам минут: все ветки таблицы, ровно один итог к сроку (G-4) | `test_verification_outcomes` |
| A-5 | Пересчёт `large_aggressor` на `REPAIRED` | `test_large_aggressor_recheck_on_repaired`, `test_recheck_large_aggressor_untouched_second` |
| A-6 | Outbox: Redis недоступен — событие ждёт и уходит позже с тем же `event_id`; порядок сигнал → итог (G-5) | `test_outbox_survives_redis_outage` |
| A-7 | Сообщение соответствует AsyncAPI-схеме (G-1) | `test_messages_match_asyncapi_schema` |
| A-8 | События качества публикуются из `quality_events` по курсору без потерь | `test_quality_events_published_by_cursor` |
| A-9 | Нормы только по `get_baseline_days`; меньше 5 дней — детектор выключен (G-6) | `test_detector_off_without_baseline` |
| A-10 | Разветвление не блокирует сбор: переполнение → `signals_paused` (G-2) | `test_fanout_overflow_pauses_signals` |
| A-11 | Этап 2: `stacked_imbalance`, `absorption` | этап 2 (не реализован) |

## Затронутые документы

[01-overview.md](../01-overview.md), [02-architecture.md](../02-architecture.md),
[03-data-model.md](../03-data-model.md), [04-configuration.md](../04-configuration.md),
[05-realtime-collection.md](../05-realtime-collection.md), [08-footprint-reader.md](../08-footprint-reader.md),
[09-external-integrations.md](../09-external-integrations.md), [10-operations.md](../10-operations.md),
`docs/contracts/asyncapi/cluster-source-events.asyncapi.yaml`,
`docs/contracts/schemas/config.schema.json`, SPEC-0001 (канал доставки), AGENTS.md.

## Открытые вопросы

- [x] Канал доставки роботу — Redis Stream `cluster.events` (2026-10-14).
- [ ] Стартовые пороги детекторов — согласовать; калибровка по режиму наблюдения (2–4 недели) и бэктесту на `FINAL`-днях.
- [ ] Границы режимов торгов по рынкам — сверить с расписанием Мосбиржи.
- [ ] Робот: изменение-потребитель `cluster.events` (группа `robot-v1`) — в репозитории робота.

## Журнал решений

- **2026-10-14** — Этап 1 реализован: `signals/` (`engine`, `detectors`, `baseline`, `publisher`, `events`), `SignalRepository`, `reader.get_signals` / `get_signal_stats`; проверено публикацией в живой Redis. Режимы торгов по умолчанию — по распределению сделок 13.10 (основная сессия акций с 10:00, FORTS — с 09:00, вечер — с 19:00). Прогон на 13.10 (нормы по 20 дням): SBER — 119 `large_aggressor` и 121 `volume_spike` за день, Si — 149 и 36, BR — 50 и 24; у неликвидных `volume_spike` шумит (MSNG — 148 при 8 592 сделках: медиана минуты почти нулевая) — калибровать в режиме наблюдения (например, перцентиль 99 вместо 90).
- **2026-10-14** — Канал доставки — собственный Redis Stream `cluster.events` в Redis робота: контракт робота (`market.events`) не меняется, конверт и механика доставки — как у `market.events` (outbox, `event_id`, Consumer Group, `XTRIM MINID` 90 суток). Контракт — AsyncAPI 3.0.0.
- **2026-10-13** — Нормы: окно 20 торговых дней, `FINAL` первыми, добор `ARCHIVE_CONSISTENT` (фьючерсы до 2026-10-13); сигнал несёт `baseline_quality` и состав окна; политика по `mixed` (меньший объём, выше порог или только запись) — за роботом.
- **2026-10-12** — Каталог и MVP согласованы как направление; реализация после SPEC-0002…0007.
