"""Сверка жёлтого списка вкладки месяца с листом fact: что пришло и что ушло.

Строка списка — поступление (+) или крупный платёж (−): день, сумма со знаком,
валюта. Поступления — по плану месяца, без окна: доходы валюты этого месяца по
порядку дней заполняют строки этой валюты по порядку их дней — первую до её суммы,
что сверх — следующую, что сверх последней — последнюю. Так зарплата, пришедшая
частями и раньше своего дня, засчитывается ей. Месяц дохода — пометка «за <месяц>»
в комментарии (её пишет банк, владелец или кнопка бота), без пометки — месяц даты:
часть ЗП, опоздавшая в следующий месяц, с пометкой остаётся своему месяцу. Платёж
закрывает одна трата той же валюты на сумму плана ± PAYMENT_TOLERANCE с дня строки −
EARLY_DAYS. Строка закрыта, когда пришло DONE_SHARE плана; иначе впереди —
недостающая часть.
"""

import calendar
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

import pandas as pd

from app.services.analytics_service import expenses, income

EARLY_DAYS = 5  # платёж может уйти на несколько дней раньше дня в списке
DONE_SHARE = 0.9  # строка закрыта, когда пришло 90 % плана
PAYMENT_TOLERANCE = 0.1  # платёж — одна трата на сумму плана ± 10 %
# «за октябрь», «Зарплата за Сентябрь 2026»: «за» — отдельным словом («из-за» — нет),
# месяц — целым; год — ровно четыре цифры, а не начало суммы
MONTH_MARK = re.compile(
    r"(?<![а-яёa-z-])за\s+(январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|"
    r"июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])"
    r"(?![а-яё])(?:\s+(\d{4})(?!\d))?",
    re.IGNORECASE,
)
MONTH_STEMS = ("янв", "фев", "мар", "апр", "ма", "июн", "июл", "авг", "сен", "окт",
               "ноя", "дек")  # fmt: skip


class ListLine(Protocol):
    """Строка списка: строка вкладки, день месяца, сумма со знаком, валюта."""

    row: int
    day: int
    amount: float
    currency: str


@dataclass(frozen=True)
class Line:
    row: int
    day: int
    amount: float
    currency: str


@dataclass(frozen=True)
class Receipt:
    """Что пришло по строке — со знаком строки, в её валюте."""

    received: float = 0.0
    on: date | None = None  # дата последней операции
    operations: frozenset[int] = frozenset()  # индексы строк fact во фрейме


NOTHING = Receipt()


def _due(first: date, day: int) -> date:
    """День строки в месяце first; 31-е в коротком месяце — его последний день."""
    last = calendar.monthrange(first.year, first.month)[1]
    return first.replace(day=min(max(day, 1), last))


def _opens(first: date, line: ListLine) -> date:
    return max(_due(first, line.day) - timedelta(days=EARLY_DAYS), first)


def income_month(day: date, comment: str) -> date:
    """1-е число месяца дохода: пометка «за <месяц>» в комментарии — ближайший к дню
    месяц с этим названием (поровну назад и вперёд — прошлый) или год из пометки, если
    он не дальше года от даты (иначе это цифры суммы или карты); без пометки — месяц
    даты."""
    mark = MONTH_MARK.search(comment)
    if mark is None:
        return day.replace(day=1)
    word = mark.group(1).lower()
    month = (
        5
        if word.startswith("ма") and not word.startswith("мар")
        else (MONTH_STEMS.index(word[:3]) + 1)
    )
    if mark.group(2) and abs(int(mark.group(2)) - day.year) <= 1:
        return date(int(mark.group(2)), month, 1)
    here = day.year * 12 + day.month - 1
    nearest = min(
        (year * 12 + month - 1 for year in (day.year - 1, day.year, day.year + 1)),
        key=lambda index: (abs(index - here), index),
    )
    return date(nearest // 12, nearest % 12 + 1, 1)


def match_receipts(
    frame: pd.DataFrame,
    lines: Sequence[ListLine],
    first: date,
    through: date,
) -> dict[int, Receipt]:
    """Что пришло по строкам списка месяца first к дню through: доходы этого месяца
    (income_month) с датой по through и траты с first по through.

    Ключ — строка вкладки; строк, по которым ничего не пришло, в ответе нет.
    """
    month = frame[(frame["day"] >= first) & (frame["day"] <= through)]
    found: dict[int, Receipt] = {}

    def add(line: ListLine, amount: float, day: date, index: int) -> None:
        old = found.get(line.row, NOTHING)
        found[line.row] = Receipt(
            old.received + amount,
            day if old.on is None else max(old.on, day),
            old.operations | {index},
        )

    got = income(frame)
    got = got[(got["amount"] > 0) & (got["day"] <= through)]
    ours = pd.Series(
        [
            income_month(day, str(comment)) == first
            for day, comment in zip(got["day"], got["comment"], strict=True)
        ],
        index=got.index,
        dtype=bool,
    )
    got = got[ours].sort_values("day", kind="stable")
    incomes = [line for line in lines if line.amount > 0]
    for currency in sorted({line.currency for line in incomes}):
        queue = sorted(
            (line for line in incomes if line.currency == currency),
            key=lambda line: (line.day, line.row),
        )
        room = {line.row: float(line.amount) for line in queue}
        mine = got[got["currency"] == currency]
        for index, day, amount in zip(
            mine.index, mine["day"], mine["amount"], strict=True
        ):
            rest = float(amount)
            for place, line in enumerate(queue):
                last = place == len(queue) - 1
                take = rest if last else min(rest, room[line.row])
                if take <= 0:
                    continue
                room[line.row] -= take
                add(line, take, day, int(index))
                rest -= take
                if rest <= 1e-9:
                    break

    spent = expenses(month)
    spent = spent[spent["amount"] > 0]
    used: set[int] = set()
    payments = sorted(
        (line for line in lines if line.amount < 0), key=lambda item: item.day
    )
    for line in payments:
        due = -line.amount
        best: tuple[tuple[float, int], int, date, float] | None = None
        for index, day, amount, currency in zip(
            spent.index, spent["day"], spent["amount"], spent["currency"], strict=True
        ):
            number = int(index)
            if currency != line.currency or day < _opens(first, line):
                continue
            if number in used or abs(float(amount) - due) > PAYMENT_TOLERANCE * due:
                continue
            key = (abs(float(amount) - due), abs((day - _due(first, line.day)).days))
            if best is None or key < best[0]:
                best = (key, number, day, float(amount))
        if best is not None:
            _, number, day, amount = best
            used.add(number)
            found[line.row] = Receipt(-amount, day, frozenset({number}))
    return found


def closed(line: ListLine, receipt: Receipt) -> bool:
    """Пришло DONE_SHARE плана — строки впереди больше нет."""
    return line.amount != 0 and abs(receipt.received) >= DONE_SHARE * abs(line.amount)


def remaining(line: ListLine, receipt: Receipt) -> float:
    """Что ещё впереди по строке, в её валюте; 0 — строка закрыта."""
    if line.amount == 0 or closed(line, receipt):
        return 0.0
    return line.amount - receipt.received


def receipt_status(line: ListLine, receipt: Receipt, today: date) -> str:
    """done — пришло, partial — пришла часть, waiting — день прошёл, future — впереди."""
    if closed(line, receipt):
        return "done"
    if receipt.received:
        return "partial"
    if _due(today.replace(day=1), line.day) < today:
        return "waiting"
    return "future"
