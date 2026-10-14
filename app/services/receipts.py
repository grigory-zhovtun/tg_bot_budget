"""Сверка жёлтого списка вкладки месяца с листом fact: что пришло и что ушло.

Строка списка — поступление (+) или крупный платёж (−): день, сумма со знаком,
валюта. Поступление набирается из доходов той же валюты с дня строки − EARLY_DAYS
до конца месяца: части складываются, доход достаётся строке с ближайшим днём.
Платёж закрывает одна трата той же валюты на сумму плана ± PAYMENT_TOLERANCE.
Строка закрыта, когда пришло DONE_SHARE плана; иначе впереди — недостающая часть.
"""

import calendar
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

import pandas as pd

from app.services.analytics_service import expenses, income

EARLY_DAYS = 5  # поступление может прийти на несколько дней раньше дня в списке
DONE_SHARE = 0.9  # строка закрыта, когда пришло 90 % плана
PAYMENT_TOLERANCE = 0.1  # платёж — одна трата на сумму плана ± 10 %


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


def match_receipts(
    frame: pd.DataFrame, lines: Sequence[ListLine], first: date, through: date
) -> dict[int, Receipt]:
    """Что пришло по строкам списка месяца first — по операциям с first по through.

    Ключ — строка вкладки; строк, по которым ничего не пришло, в ответе нет.
    """
    month = frame[(frame["day"] >= first) & (frame["day"] <= through)]
    found: dict[int, Receipt] = {}

    incomes = [line for line in lines if line.amount > 0]
    got = income(month)
    got = got[got["amount"] > 0]
    for index, day, amount, currency in zip(
        got.index, got["day"], got["amount"], got["currency"], strict=True
    ):
        fits = [
            line
            for line in incomes
            if line.currency == currency and _opens(first, line) <= day
        ]
        if not fits:
            continue
        line = min(
            fits,
            key=lambda item: (abs((day - _due(first, item.day)).days), item.day),
        )
        old = found.get(line.row, NOTHING)
        found[line.row] = Receipt(
            old.received + float(amount),
            day if old.on is None else max(old.on, day),
            old.operations | {int(index)},
        )

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
