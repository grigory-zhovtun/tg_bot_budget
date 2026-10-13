"""Вкладка «План» Mini App: план месяца из вкладки, подсказки, правки.

Вкладка месяца: статьи в A:M (тип A, группа B, подкатегория C, план D со знаком
минус, валюта G), справа блок прогноза O:S — список «Поступления (+) и крупные
платежи (−)» и цель накоплений. Статья — пара (подкатегория, валюта): по ней
вкладка считает факт, по ней же находим строку, а не по номеру.
"""

from dataclasses import dataclass, replace
from typing import Any

from app.services.day_budget import COLUMN_O, GOAL_LABEL, LIST_HEADER, TABLE_HEADER

KINDS = {"Расходы": "spend", "Доходы": "income"}
MAX_EVENTS = 20  # строк списка; дальше — уже не список, а сломанная вкладка

EventCells = tuple[str | None, int | None, float | None, str | None]


def article_key(subcategory: str, currency: str) -> str:
    """«кофе|UZS» — ключ статьи в API и черновике."""
    return f"{subcategory}|{currency}"


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _number(value: Any) -> float | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return None


@dataclass(frozen=True)
class Article:
    """Строка статьи: план положительный, в валюте строки; row — номер строки листа."""

    key: str
    kind: str  # spend | income
    group: str
    subcategory: str
    currency: str
    plan: float
    row: int
    editable: bool = True


@dataclass(frozen=True)
class Event:
    """Строка списка поступлений и платежей; у пустой name — None."""

    slot: int
    row: int
    name: str | None
    day: int | None
    amount: float | None
    currency: str | None

    def cells(self) -> EventCells:
        return self.name, self.day, self.amount, self.currency


@dataclass(frozen=True)
class Goal:
    row: int
    amount: float
    currency: str


@dataclass(frozen=True)
class MonthPlan:
    articles: tuple[Article, ...]
    events: tuple[Event, ...] | None  # None — во вкладке нет блока прогноза
    goal: Goal | None


def _articles(rows: list[list[Any]]) -> tuple[Article, ...]:
    found: list[Article] = []
    for number, row in enumerate(rows[1:], start=2):
        cells = list(row[:7]) + [""] * (7 - len(row[:7]))
        kind = KINDS.get(_text(cells[0]))
        group, sub = _text(cells[1]), _text(cells[2])
        if kind is None or not group or not sub:
            continue
        currency = _text(cells[6]).upper() or "UZS"
        plan = 0.0 - (_number(cells[3]) or 0.0)
        key = article_key(sub, currency)
        found.append(Article(key, kind, group, sub, currency, plan, number))
    counts: dict[str, int] = {}
    for article in found:
        counts[article.key] = counts.get(article.key, 0) + 1
    return tuple(
        replace(article, editable=False) if counts[article.key] > 1 else article
        for article in found
    )


def _block(row: list[Any]) -> list[Any]:
    """O:R строки: название, день, сумма, валюта."""
    cells = list(row[COLUMN_O : COLUMN_O + 4])
    return cells + [""] * (4 - len(cells))


def _forecast(
    rows: list[list[Any]],
) -> tuple[tuple[Event, ...] | None, Goal | None]:
    start = next(
        (
            index
            for index, row in enumerate(rows)
            if _text(_block(row)[0]).startswith(LIST_HEADER)
        ),
        None,
    )
    if start is None:
        return None, None
    events: list[Event] = []
    goal: Goal | None = None
    for index in range(start + 1, len(rows)):
        name, day, amount, currency = _block(rows[index])
        label = _text(name)
        if label == TABLE_HEADER or len(events) == MAX_EVENTS:
            break
        if GOAL_LABEL in label:
            code = _text(currency).upper() or "UZS"
            goal = Goal(index + 1, _number(amount) or 0.0, code)
            break
        number = _number(day)
        events.append(
            Event(
                slot=len(events),
                row=index + 1,
                name=label or None,
                day=None if number is None else int(number),
                amount=_number(amount),
                currency=_text(currency).upper() or None,
            )
        )
    return tuple(events), goal


def read_plan(rows: list[list[Any]]) -> MonthPlan:
    """Значения вкладки месяца (UNFORMATTED) → статьи, список и цель."""
    events, goal = _forecast(rows)
    return MonthPlan(_articles(rows), events, goal)
