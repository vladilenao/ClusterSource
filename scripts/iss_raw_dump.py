"""ВРЕМЕННЫЙ сборщик сырой ленты Мосбиржи из ISS (SPEC-0003, до реализации в сервисе).

Бесплатный ISS отдаёт сделки только текущего торгового дня с задержкой 15 минут,
поэтому ленту нужно забирать каждый день, иначе номера сделок (``TRADENO``) и
открытый интерес по сделкам FORTS (``OPENPOSITION``) теряются безвозвратно.

Работает бесконечно (запускается launchd с KeepAlive): раз в ``POLL_SEC`` догоняет
общие ленты рынка акций и FORTS постранично (``start``/``limit``) и дописывает
строки как есть в ``{root}/{YYYY-MM-DD}/{market}.csv.gz`` (многочленный gzip).
Смена торгового дня в ISS определяется по смене первой сделки ленты; дата
каталога — дата ``SYSTIME`` первой сделки (МСК). Прогресс хранится в
``{root}/state.json``. Падение между записью и сохранением прогресса даёт дубли
— при загрузке в сервис дедуплицировать по (``SECID``, ``TRADENO``).

Запуск: ``python scripts/iss_raw_dump.py /path/to/history/_iss_raw``.
Удалить после реализации SPEC-0003.
"""

from __future__ import annotations

import csv
import gzip
import json
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

MARKETS = {
    "shares": "https://iss.moex.com/iss/engines/stock/markets/shares/trades.json",
    "forts": "https://iss.moex.com/iss/engines/futures/markets/forts/trades.json",
}
PAGE = 5000
POLL_SEC = 60
PAGE_PAUSE_SEC = 0.3
#: ISS при частых запросах временно отдаёт пустые страницы посреди ленты.
EMPTY_RETRY_SEC = 2.0
EMPTY_RETRY_MAX = 60
LOG_EVERY_ROWS = 100_000


def log(msg: str) -> None:
    print(f"{datetime.now(UTC):%Y-%m-%d %H:%M:%S}Z {msg}", flush=True)


def fetch(
    url: str, start: int, limit: int, reversed_: bool = False
) -> tuple[list[str], list[list[object]]]:
    query = f"{url}?iss.meta=off&iss.only=trades&limit={limit}&start={start}"
    if reversed_:
        query += "&reversed=1"
    for attempt in range(10):
        try:
            with urllib.request.urlopen(query, timeout=60) as resp:
                trades = json.load(resp)["trades"]
            return trades["columns"], trades["data"]
        except Exception as exc:  # noqa: BLE001 - сеть/ISS: повторяем с паузой
            wait = min(300, 2**attempt)
            log(f"повтор через {wait}s: {query}: {exc}")
            time.sleep(wait)
    raise RuntimeError(f"ISS недоступен: {query}")


def feed_last_tradeno(url: str) -> str | None:
    """Номер последней сделки ленты или ``None``, если ISS ответил пусто."""
    columns, rows = fetch(url, 0, 1, reversed_=True)
    return str(rows[0][columns.index("TRADENO")]) if rows else None


def head_key(columns: list[str], row: list[object]) -> tuple[str, str]:
    systime = str(row[columns.index("SYSTIME")])
    return f"{row[columns.index('TRADENO')]}|{systime}", systime[:10]


def catch_up(root: Path, name: str, url: str, state: dict[str, dict[str, object]]) -> None:
    columns, first = fetch(url, 0, 1)
    if not first:
        return
    key, day = head_key(columns, first[0])
    st = state.get(name)
    if st is None or st.get("head") != key:
        if st is not None:
            log(f"{name}: ISS перешёл на новый день {day}")
        st = {"head": key, "date": day, "offset": 0}
        state[name] = st
    offset = int(str(st["offset"]))
    path = root / str(st["date"]) / f"{name}.csv.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    added = 0
    empty_streak = 0
    while True:
        columns, rows = fetch(url, offset, PAGE)
        if not rows:
            # Пустая страница — не обязательно конец ленты: сверяем с её последней сделкой.
            last = feed_last_tradeno(url)
            if last is None or last == st.get("last") or empty_streak >= EMPTY_RETRY_MAX:
                break
            empty_streak += 1
            time.sleep(EMPTY_RETRY_SEC)
            continue
        empty_streak = 0
        new_file = not path.exists()
        with gzip.open(path, "at", newline="") as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(columns)
            writer.writerows(rows)
        offset += len(rows)
        added += len(rows)
        st["offset"] = offset
        st["last"] = str(rows[-1][columns.index("TRADENO")])
        save_state(root, state)
        if added % LOG_EVERY_ROWS < len(rows):
            log(f"{name} {st['date']}: догоняю, +{added}, всего {offset}")
        # Конец ленты — только пустая страница: ISS иногда отдаёт неполные страницы посреди дня.
        time.sleep(PAGE_PAUSE_SEC)
    if added:
        log(f"{name} {st['date']}: +{added}, всего {offset}")


def load_state(root: Path) -> dict[str, dict[str, object]]:
    path = root / "state.json"
    return json.loads(path.read_text()) if path.exists() else {}


def save_state(root: Path, state: dict[str, dict[str, object]]) -> None:
    tmp = root / "state.json.tmp"
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2))
    tmp.replace(root / "state.json")


def main() -> None:
    root = Path(sys.argv[1])
    root.mkdir(parents=True, exist_ok=True)
    state = load_state(root)
    log(f"старт, каталог {root}")
    while True:
        for name, url in MARKETS.items():
            try:
                catch_up(root, name, url, state)
            except Exception as exc:  # noqa: BLE001 - не роняем сборщик из-за одного рынка
                log(f"{name}: ошибка цикла: {exc}")
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
