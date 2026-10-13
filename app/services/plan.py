"""Вкладка «План» Mini App: план месяца из вкладки, подсказки, правки.

Вкладка месяца: статьи в A:M (тип A, группа B, подкатегория C, план D со знаком
минус, валюта G), справа блок прогноза O:S — список «Поступления (+) и крупные
платежи (−)» и цель накоплений. Статья — пара (подкатегория, валюта): по ней
вкладка считает факт, по ней же находим строку, а не по номеру.
"""

from collections.abc import Collection
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

import pandas as pd
from gspread.exceptions import WorksheetNotFound

from app import config
from app.domain import Rates
from app.services.analytics_service import MONTHS_RU, money, transactions_frame
from app.services.day_budget import (
    COLUMN_O,
    GOAL_LABEL,
    LIST_HEADER,
    TABLE_HEADER,
    short,
)
from app.services.google_sheets import GoogleSheetsService

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


def shift_month(first: date, months: int) -> date:
    """Первое число месяца на months вперёд (назад — с минусом)."""
    index = first.year * 12 + first.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


@dataclass(frozen=True)
class History:
    """Подсказки: среднее за три полных месяца, прошлый и текущий месяц."""

    average_months: tuple[date, ...]
    last_month: date
    current_month: date
    current_days: int
    sums: dict[str, tuple[float, float, float]]  # ключ → (среднее, прошлый, текущий)

    def of(self, key: str) -> tuple[float, float, float]:
        return self.sums.get(key, (0.0, 0.0, 0.0))


def article_history(frame: pd.DataFrame, today: date) -> History:
    """Суммы листа fact по паре (подкатегория, валюта) в валюте строки.

    Знак как во вкладке: трата и доход плюс, возврат минус. Месяцы считаются от
    сегодняшней даты, месяц без записей — ноль.
    """
    current = today.replace(day=1)
    months = tuple(shift_month(current, -back) for back in (3, 2, 1))
    window = frame[(frame["day"] >= months[0]) & (frame["day"] <= today)]
    by_key: dict[str, dict[date, float]] = {}
    if not window.empty:
        month = window["day"].map(lambda value: value.replace(day=1))
        totals = (
            window.assign(month=month)
            .groupby(["subcategory", "currency", "month"])["amount"]
            .sum()
        )
        for (sub, currency, first), amount in totals.items():
            by_key.setdefault(article_key(sub, currency), {})[first] = float(amount)
    sums = {
        key: (
            sum(values.get(first, 0.0) for first in months) / len(months),
            values.get(months[-1], 0.0),
            values.get(current, 0.0),
        )
        for key, values in by_key.items()
    }
    return History(months, months[-1], current, today.day, sums)


TOLERANCE = 0.005  # суммы сравниваем до копейки


@dataclass(frozen=True)
class EventValue:
    """Новое содержимое строки списка: сумма со знаком (+ поступление, − платёж)."""

    name: str
    day: int
    amount: float
    currency: str


@dataclass(frozen=True)
class ArticleChange:
    key: str
    base: float  # план, который видел пользователь
    plan: float


@dataclass(frozen=True)
class EventChange:
    slot: int
    base: EventCells  # строка списка, которую видел пользователь
    value: EventValue | None  # None — очистить строку


@dataclass(frozen=True)
class GoalChange:
    base: tuple[float, str]
    value: tuple[float, str]


@dataclass(frozen=True)
class PlanChanges:
    articles: tuple[ArticleChange, ...] = ()
    events: tuple[EventChange, ...] = ()
    goal: GoalChange | None = None

    def count(self) -> int:
        return len(self.articles) + len(self.events) + int(self.goal is not None)


def _same(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return abs(left - right) < TOLERANCE


def unknown(
    plan: MonthPlan, changes: PlanChanges, currencies: Collection[str]
) -> set[str]:
    """Поля, которые во вкладке не найти или нельзя править (ответ 422)."""
    bad: set[str] = set()
    editable = {a.key for a in plan.articles if a.editable}
    keys = [change.key for change in changes.articles]
    if len(set(keys)) != len(keys) or any(key not in editable for key in keys):
        bad.add("rows")
    slots = {event.slot for event in plan.events or ()}
    chosen = [change.slot for change in changes.events]
    if len(set(chosen)) != len(chosen) or any(
        change.slot not in slots
        or (change.value is not None and change.value.currency not in currencies)
        for change in changes.events
    ):
        bad.add("events")
    if changes.goal is not None and (
        plan.goal is None or changes.goal.value[1] not in currencies
    ):
        bad.add("goal")
    return bad


def conflicts(plan: MonthPlan, changes: PlanChanges) -> list[str]:
    """Что поменялось во вкладке после открытия плана: ключи, «event:<n>», «goal»."""
    found: list[str] = []
    articles = {a.key: a for a in plan.articles}
    for change in changes.articles:
        if not _same(articles[change.key].plan, change.base):
            found.append(change.key)
    events = {e.slot: e for e in plan.events or ()}
    for change in changes.events:
        name, day, amount, currency = events[change.slot].cells()
        base_name, base_day, base_amount, base_currency = change.base
        if (name, day, currency) != (base_name, base_day, base_currency) or not _same(
            amount, base_amount
        ):
            found.append(f"event:{change.slot}")
    if changes.goal is not None and plan.goal is not None:
        amount, currency = changes.goal.base
        if currency != plan.goal.currency or not _same(plan.goal.amount, amount):
            found.append("goal")
    return found


def cell_updates(
    title: str, plan: MonthPlan, changes: PlanChanges
) -> list[dict[str, Any]]:
    """Диапазоны для values_batch_update (RAW): D статей, O:R списка, Q:R цели."""
    articles = {a.key: a for a in plan.articles}
    events = {e.slot: e for e in plan.events or ()}
    data: list[dict[str, Any]] = []
    for change in changes.articles:
        row = articles[change.key].row
        data.append({"range": f"'{title}'!D{row}", "values": [[0.0 - change.plan]]})
    for change in changes.events:
        row = events[change.slot].row
        value = change.value
        cells: list[Any] = (
            ["", "", "", ""]
            if value is None
            else [value.name, value.day, value.amount, value.currency]
        )
        data.append({"range": f"'{title}'!O{row}:R{row}", "values": [cells]})
    if changes.goal is not None and plan.goal is not None:
        amount, currency = changes.goal.value
        row = plan.goal.row
        data.append(
            {"range": f"'{title}'!Q{row}:R{row}", "values": [[amount, currency]]}
        )
    return data


def _filled(event: Event, value: EventValue | None) -> Event:
    if value is None:
        return replace(event, name=None, day=None, amount=None, currency=None)
    return replace(
        event,
        name=value.name,
        day=value.day,
        amount=value.amount,
        currency=value.currency,
    )


def applied(plan: MonthPlan, changes: PlanChanges) -> MonthPlan:
    """План после правок — для «Свободно» в уведомлении."""
    plans = {change.key: change.plan for change in changes.articles}
    values = {change.slot: change.value for change in changes.events}
    articles = tuple(replace(a, plan=plans.get(a.key, a.plan)) for a in plan.articles)
    events: tuple[Event, ...] | None = None
    if plan.events is not None:
        events = tuple(
            _filled(e, values[e.slot]) if e.slot in values else e for e in plan.events
        )
    goal = plan.goal
    if changes.goal is not None and goal is not None:
        amount, currency = changes.goal.value
        goal = replace(goal, amount=amount, currency=currency)
    return MonthPlan(articles, events, goal)


@dataclass(frozen=True)
class Totals:
    """Итог плана в сумах по курсам system (как колонки H–J вкладки)."""

    incomes: float  # строки списка со знаком «+»
    spend: float  # план статей «Расходы»
    goal: float
    income_rows: float  # план статей «Доходы» — для сравнения со списком

    @property
    def free(self) -> float:
        return self.incomes - self.spend - self.goal


def totals(plan: MonthPlan, rates: Rates) -> Totals:
    def uzs(amount: float, currency: str | None) -> float:
        return amount * rates.to_uzs.get(currency or "", 0.0)

    spend = sum(uzs(a.plan, a.currency) for a in plan.articles if a.kind == "spend")
    income_rows = sum(
        uzs(a.plan, a.currency) for a in plan.articles if a.kind == "income"
    )
    incomes = sum(
        uzs(e.amount, e.currency)
        for e in plan.events or ()
        if e.name and e.amount is not None and e.amount > 0
    )
    goal = uzs(plan.goal.amount, plan.goal.currency) if plan.goal else 0.0
    return Totals(incomes, spend, goal, income_rows)


NOTE_LINES = 10  # дальше — «… и ещё N»


def _plain(value: float) -> str:
    """«1 500 000», «650,50»: копейки, только если они есть."""
    if float(value).is_integer():
        return money(value)
    return f"{value:,.2f}".replace(",", " ").replace(".", ",")


def _amount(value: float, currency: str) -> str:
    return _plain(value) if currency == "UZS" else f"{_plain(value)} {currency}"


def _event_text(cells: EventCells) -> str:
    name, day, amount, currency = cells
    if not name or day is None or amount is None or not currency:
        return "—"
    sign = "+" if amount > 0 else "−"
    return f"{day} · {name} {sign}{_amount(abs(amount), currency)}"


def change_note(
    plan: MonthPlan,
    changes: PlanChanges,
    rates: Rates,
    month: date,
    author: str | None,
    icons: dict[str, str],
) -> str:
    """Сообщение второму: кто менял, что было → стало и «Свободно» до и после."""
    articles = {a.key: a for a in plan.articles}
    lines: list[str] = []
    for change in changes.articles:
        article = articles[change.key]
        parts = (icons.get(article.subcategory, ""), article.subcategory)
        label = " ".join(part for part in parts if part)
        if article.currency != "UZS":
            label += f" {article.currency}"
        lines.append(f"{label}: {_plain(change.base)} → {_plain(change.plan)}")
    for change in changes.events:
        value = change.value
        after = (
            "удалено"
            if value is None
            else _event_text((value.name, value.day, value.amount, value.currency))
        )
        lines.append(f"📅 {_event_text(change.base)} → {after}")
    if changes.goal is not None:
        (was, was_code), (now, now_code) = changes.goal.base, changes.goal.value
        lines.append(
            f"🎯 Отложить: {_amount(was, was_code)} → {_amount(now, now_code)}"
        )
    shown = lines[:NOTE_LINES]
    if len(lines) > NOTE_LINES:
        shown.append(f"… и ещё {len(lines) - NOTE_LINES}")
    name = MONTHS_RU[month.month - 1]
    head = (
        f"✏️ {author} — правки в плане на {name}:"
        if author
        else f"✏️ Правки в плане на {name}:"
    )
    before = totals(plan, rates).free
    after_free = totals(applied(plan, changes), rates).free
    return "\n".join([head, *shown, f"Свободно: {short(before)} → {short(after_free)}"])


@dataclass(frozen=True)
class LoadedPlan:
    plan: MonthPlan
    rates: Rates
    history: History


def load_plan(gs: GoogleSheetsService, title: str, today: date) -> LoadedPlan | None:
    """Вкладка месяца, курсы и подсказки (синхронно); None — вкладки нет."""
    try:
        rows = gs.get_values(title)
    except WorksheetNotFound:
        return None
    rates = gs.get_rates()
    frame = transactions_frame(gs.get_values(config.FACT_SHEET_NAME), rates)
    return LoadedPlan(read_plan(rows), rates, article_history(frame, today))


@dataclass(frozen=True)
class SaveOutcome:
    plan: MonthPlan  # вкладка до записи
    rates: Rates
    invalid: frozenset[str] = frozenset()
    conflicts: tuple[str, ...] = ()


def save_plan(gs: GoogleSheetsService, title: str, changes: PlanChanges) -> SaveOutcome:
    """Проверить правки по свежей вкладке и записать; при ошибке ничего не пишет.

    WorksheetNotFound — вкладки месяца нет.
    """
    plan = read_plan(gs.get_values(title))
    rates = gs.get_rates()
    if bad := unknown(plan, changes, rates.to_uzs.keys()):
        return SaveOutcome(plan, rates, invalid=frozenset(bad))
    if found := conflicts(plan, changes):
        return SaveOutcome(plan, rates, conflicts=tuple(found))
    gs.write_cells(cell_updates(title, plan, changes))
    return SaveOutcome(plan, rates)
