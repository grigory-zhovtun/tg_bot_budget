"""Календарь записей: лист «Календарь» → записи, их дни в месяце, закрытие фактом,
подсказки сумм. Без сети: на входе значения листа и фрейм fact.

Запись — часть плана статьи (подкатегория + валюта) на день: разовая, каждый день,
по дням недели или раз в месяц. У вхождения записи в день два «открыто»: на утро —
без сегодняшних операций (резерв статьи и план в день за день не меняются) и на
сейчас — с ними (события прогноза и суммы на экране).
"""

import calendar
import logging
import secrets
import string
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import median
from typing import Any

import pandas as pd

from app.services.analytics_service import expenses, to_day
from app.services.google_sheets import CALENDAR_HEADER as HEADER

logger = logging.getLogger(__name__)

REPEATS = {
    "нет": "once",
    "каждый день": "daily",
    "по дням недели": "weekdays",
    "раз в месяц": "monthly",
}
REPEAT_TEXT = {code: text for text, code in REPEATS.items()}
WEEKDAYS = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")  # ISO: пн = 1 … вс = 7
EARLY_DAYS = 3  # разовую запись закрывает операция с её даты минус 3 дня
TOLERANCE = 0.1  # операция на сумму записи ±10 %
DONE_SHARE = 0.9  # траты дня покрыли 90 % — сегодняшние вхождения статьи закрыты
HINT_DAYS = 90  # «обычно» и «в прошлый раз» — по операциям за 90 дней
PER_DAY_DAYS = 30  # «в среднем в день» — за 30 дней
ID_LENGTH = 6


@dataclass(frozen=True)
class CalendarRecord:
    """Строка листа «Календарь»: сумма положительная, в валюте статьи."""

    id: str
    start: date  # день разовой записи или первый день повтора
    repeat: str  # once | daily | weekdays | monthly
    weekdays: frozenset[int]  # ISO-номера дней для weekdays, иначе пусто
    group: str
    subcategory: str
    currency: str
    amount: float
    name: str  # пусто — показывается подкатегория
    author: str

    @property
    def key(self) -> str:
        return f"{self.subcategory}|{self.currency}"

    @property
    def title(self) -> str:
        return self.name or self.subcategory


@dataclass(frozen=True)
class Occurrence:
    """Вхождение записи в день: сколько открыто на утро и на сейчас."""

    record: CalendarRecord
    day: date
    morning: float  # без сегодняшних операций — для резерва статьи
    now: float  # с ними — события прогноза и суммы на экране; 0 — закрыто
    covered: float  # сегодня: засчитано из трат статьи; в другие дни 0


@dataclass(frozen=True)
class CalendarState:
    occurrences: tuple[Occurrence, ...]  # по дням, потом по началу записи
    closed_morning: frozenset[int]  # строки fact, закрывшие разовые записи по вчера
    closed_now: frozenset[int]  # то же по сегодня


@dataclass(frozen=True)
class Hint:
    """Подсказка суммы для окна записи; None — в истории статьи пусто."""

    typical: float | None  # медиана операций за HINT_DAYS дней
    last: float | None  # последняя операция
    per_day: float | None  # сумма за PER_DAY_DAYS дней / PER_DAY_DAYS


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    text = _text(value).replace("\u00a0", "").replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _weekdays(text: str) -> frozenset[int] | None:
    """«пн ср» → {1, 3}; незнакомое слово — None."""
    found: set[int] = set()
    for word in text.lower().replace(",", " ").split():
        if word not in WEEKDAYS:
            return None
        found.add(WEEKDAYS.index(word) + 1)
    return frozenset(found)


def read_calendar(rows: Sequence[Sequence[Any]]) -> tuple[CalendarRecord, ...]:
    """Значения листа (UNFORMATTED, первая строка — шапка) → записи.

    Строка без ID, даты, статьи или суммы, с незнакомым повтором или без дней у
    «по дням недели» пропускается и пишется в лог.
    """
    found: list[CalendarRecord] = []
    for number, row in enumerate(rows[1:], start=2):
        cells = list(row[: len(HEADER)]) + [""] * (
            len(HEADER) - len(row[: len(HEADER)])
        )
        record_id, start = _text(cells[0]), to_day(cells[1])
        repeat = REPEATS.get(_text(cells[2]).lower())
        days = _weekdays(_text(cells[3])) if repeat == "weekdays" else frozenset()
        group, sub, currency = _text(cells[4]), _text(cells[5]), _text(cells[6])
        amount = _number(cells[7])
        valid = (
            record_id
            and start is not None
            and repeat is not None
            and days is not None
            and (repeat != "weekdays" or days)
            and group
            and sub
            and currency
            and amount is not None
            and amount > 0
        )
        if not valid:
            if any(_text(cell) for cell in cells):
                logger.warning("Calendar row %s is skipped: %s", number, cells)
            continue
        found.append(
            CalendarRecord(
                record_id,
                start,
                repeat,
                days,
                group,
                sub,
                currency.upper(),
                amount,
                _text(cells[8]),
                _text(cells[9]),
            )
        )
    return tuple(found)


def _falls(record: CalendarRecord, day: date) -> bool:
    if record.repeat == "daily":
        return True
    if record.repeat == "weekdays":
        return day.isoweekday() in record.weekdays
    last = calendar.monthrange(day.year, day.month)[1]
    return day.day == min(record.start.day, last)  # 31-е в коротком месяце — последний


def occurrences(record: CalendarRecord, first: date, last: date) -> list[date]:
    """Дни записи с first по last включительно."""
    if record.repeat == "once":
        return [record.start] if first <= record.start <= last else []
    found: list[date] = []
    day = max(first, record.start)
    while day <= last:
        if _falls(record, day):
            found.append(day)
        day += timedelta(days=1)
    return found


def record_row(record: CalendarRecord) -> list[Any]:
    """Строка листа A:J для записи как есть (RAW): дата — «ДД.ММ.ГГГГ» текстом."""
    return [
        record.id,
        f"{record.start:%d.%m.%Y}",
        REPEAT_TEXT[record.repeat],
        " ".join(WEEKDAYS[day - 1] for day in sorted(record.weekdays)),
        record.group,
        record.subcategory,
        record.currency,
        record.amount,
        record.name,
        record.author,
    ]


def new_id(taken: Collection[str]) -> str:
    """Код записи: буква и пять букв или цифр, которого ещё нет на листе."""
    alphabet = string.ascii_lowercase + string.digits
    while True:
        code = secrets.choice(string.ascii_lowercase) + "".join(
            secrets.choice(alphabet) for _ in range(ID_LENGTH - 1)
        )
        if code not in taken:
            return code


def _spending(frame: pd.DataFrame) -> pd.DataFrame:
    """Операции трат: без доходов и переводов, возвраты не в счёт."""
    spent = expenses(frame)
    return spent[spent["amount"] > 0]


def _close_once(
    spent: pd.DataFrame,
    records: Sequence[CalendarRecord],
    since: date,
    through: date,
    used: frozenset[int],
    closed: Mapping[str, int],
) -> dict[str, int]:
    """Разовые записи, закрытые одной операцией заранее: ID записи → строка fact.

    Записи с датой с since — по порядку дат; операция — той же статьи на сумму
    записи ±TOLERANCE с даты минус EARLY_DAYS по дату (не позже through), не из
    used и не взятая другой записью; из кандидатов — ближайшая по сумме, потом по
    дате. closed — уже закрытые записи: они и их операции остаются как были.
    """
    found = dict(closed)
    taken = set(found.values())
    once = sorted(
        (r for r in records if r.repeat == "once" and r.start >= since),
        key=lambda record: (record.start, record.id),
    )
    for record in once:
        if record.id in found:
            continue
        opens = record.start - timedelta(days=EARLY_DAYS)
        mask = (
            (spent["subcategory"] == record.subcategory)
            & (spent["currency"] == record.currency)
            & (spent["day"] >= opens)
            & (spent["day"] <= min(record.start, through))
        )
        best: tuple[tuple[float, int], int] | None = None
        rows = spent[mask]
        for index, day, amount in zip(
            rows.index, rows["day"], rows["amount"], strict=True
        ):
            number = int(index)
            gap = abs(float(amount) - record.amount)
            if number in used or number in taken or gap > TOLERANCE * record.amount:
                continue
            key = (gap, abs((day - record.start).days))
            if best is None or key < best[0]:
                best = (key, number)
        if best is not None:
            taken.add(best[1])
            found[record.id] = best[1]
    return found


def calendar_state(
    frame: pd.DataFrame,
    records: Sequence[CalendarRecord],
    today: date,
    last: date,
    since: date,
    used_morning: frozenset[int],
    used_now: frozenset[int],
) -> CalendarState:
    """Вхождения записей с today по last и операции, закрывшие разовые записи.

    since — с какой даты разовые записи ищут свою операцию (для темпа нужны и
    прошедшие); used_* — строки fact, которыми закрыты платежи жёлтого списка к утру
    и на сейчас: записям они не достаются.
    """
    spent = _spending(frame)
    morning = _close_once(
        spent, records, since, today - timedelta(days=1), used_morning, {}
    )
    now = _close_once(spent, records, since, today, used_now, morning)
    closed_now = frozenset(now.values())
    todays = spent[
        (spent["day"] == today) & ~spent.index.isin(list(used_now | closed_now))
    ]
    spent_today = todays.groupby(["subcategory", "currency"])["amount"].sum()
    days = {record.id: occurrences(record, today, last) for record in records}
    due: dict[tuple[str, str], float] = {}  # статья → Σ сегодняшних открытых вхождений
    for record in records:
        if today in days[record.id] and record.id not in now:
            article = (record.subcategory, record.currency)
            due[article] = due.get(article, 0.0) + record.amount
    share = {
        article: min(float(spent_today.get(article, 0.0)) / total, 1.0)
        for article, total in due.items()
    }
    found: list[Occurrence] = []
    for record in records:
        morning_open = 0.0 if record.id in morning else record.amount
        for day in days[record.id]:
            if record.id in now:
                found.append(Occurrence(record, day, morning_open, 0.0, 0.0))
            elif day != today:
                found.append(Occurrence(record, day, morning_open, record.amount, 0.0))
            else:
                part = share.get((record.subcategory, record.currency), 0.0)
                covered = record.amount * part
                left = 0.0 if part >= DONE_SHARE else record.amount - covered
                found.append(Occurrence(record, day, morning_open, left, covered))
    found.sort(key=lambda item: (item.day, item.record.start, item.record.id))
    return CalendarState(tuple(found), frozenset(morning.values()), closed_now)


def _rounded(value: float, currency: str) -> float:
    """Сумы — до тысячи, остальные валюты — до целых."""
    return float(round(value, -3)) if currency == "UZS" else float(round(value))


def hints(frame: pd.DataFrame, today: date) -> dict[str, Hint]:
    """Подсказки сумм по статьям «подкатегория|валюта» из истории трат до сегодня."""
    spent = _spending(frame)
    window = spent[
        (spent["day"] >= today - timedelta(days=HINT_DAYS)) & (spent["day"] < today)
    ]
    found: dict[str, Hint] = {}
    for (sub, currency), rows in window.groupby(["subcategory", "currency"]):
        ordered = rows.sort_values("day", kind="stable")
        recent = rows[rows["day"] >= today - timedelta(days=PER_DAY_DAYS)]
        per_day = float(recent["amount"].sum()) / PER_DAY_DAYS if len(recent) else None
        found[f"{sub}|{currency}"] = Hint(
            _rounded(float(median(rows["amount"])), str(currency)),
            _rounded(float(ordered["amount"].iloc[-1]), str(currency)),
            None if per_day is None else _rounded(per_day, str(currency)),
        )
    return found
