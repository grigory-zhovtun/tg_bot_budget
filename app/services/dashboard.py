"""Данные экрана «Сводка» Mini App: те же расчёты, что /today и /subs.

Лимит, заморозка и траты по дням для графика — DayBudget (как утреннее
сообщение), план-факт — plan_lines вкладки месяца по группам, остаток по дням —
таблица дней вкладки (O:R под «Дата»), подписки — серии recurring с состоянием
этого месяца. Расходы по категориям — строки fact с 1-го числа по сегодня (то,
что потрачено на самом деле, даже по статье без строки во вкладке), план-факт по
категориям — они же рядом с планом вкладки, остатки карт — блок остатков листа
fact (services.cards).
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from app import config
from app.domain import Rates
from app.services.analytics_service import PlanLine, expenses, plan_lines
from app.services.calendar import CalendarRecord
from app.services.cards import CardBalance
from app.services.day_budget import (
    DayBudget,
    DayPoint,
    ForecastItem,
    day_budget,
    read_daily,
    read_forecast,
)
from app.services.plan import read_plan
from app.services.recurring import Series, find_series
from app.services.savings import (
    Cushion,
    Savings,
    cushion_of,
    month_savings,
    monthly_spending,
)

HALF_SUM = 0.5  # меньше полсума — ноль: остаток возвратов и пересчёта по курсу


@dataclass(frozen=True)
class GroupPlan:
    name: str
    plan: float
    fact: float
    items: tuple[PlanLine, ...]


@dataclass(frozen=True)
class Subscription:
    name: str
    day: int
    amount: float  # в валюте списания
    currency: str
    uzs: float
    state: str  # charged | twice | expected | missed


@dataclass(frozen=True)
class SpentItem:
    subcategory: str
    amount: float  # в сумах; возвратов больше, чем трат, — минус


@dataclass(frozen=True)
class SpentGroup:
    """Траты группы за месяц по строкам fact, в сумах."""

    name: str
    amount: float
    items: tuple[SpentItem, ...]  # от большей к меньшей


@dataclass(frozen=True)
class Untracked:
    """Траты месяца по статье (подкатегория, валюта), которой нет во вкладке."""

    group: str
    subcategory: str
    currency: str
    amount: float  # в сумах


@dataclass(frozen=True)
class CategoryLine:
    subcategory: str
    plan: float  # вкладка месяца, в сумах
    fact: float  # строки fact месяца, в сумах


@dataclass(frozen=True)
class Category:
    """План-факт группы: план — вкладка месяца, факт — строки fact с 1-го по сегодня."""

    name: str
    plan: float
    fact: float
    items: tuple[CategoryLine, ...]  # от больших трат к меньшим, без трат — по плану


@dataclass(frozen=True)
class Dashboard:
    status: str  # ok | no_forecast
    budget: DayBudget | None
    groups: tuple[GroupPlan, ...]
    daily: tuple[DayPoint, ...]
    upcoming: tuple[ForecastItem, ...]
    subscriptions: tuple[Subscription, ...]
    spent: tuple[SpentGroup, ...] = ()
    untracked: tuple[Untracked, ...] = ()
    cards: tuple[CardBalance, ...] = ()
    categories: tuple[Category, ...] = ()
    cushion: Cushion | None = None  # подушка — «Заморожено» в месяцах трат
    savings: Savings | None = None  # норма сбережений по полным месяцам


def group_plans(spend: list[PlanLine]) -> tuple[GroupPlan, ...]:
    """Статьи расходов по группам в порядке вкладки; пустые статьи не показываем."""
    groups: dict[str, list[PlanLine]] = {}
    for line in spend:
        if line.plan or line.fact:
            groups.setdefault(line.group, []).append(line)
    return tuple(
        GroupPlan(
            name,
            sum(line.plan for line in lines),
            sum(line.fact for line in lines),
            tuple(lines),
        )
        for name, lines in groups.items()
    )


def month_spending(frame: pd.DataFrame, today: date) -> pd.DataFrame:
    """Траты с 1-го числа по today: без доходов и переводов, возвраты с минусом."""
    spent = expenses(frame)
    return spent[(spent["day"] >= today.replace(day=1)) & (spent["day"] <= today)]


def spent_groups(frame: pd.DataFrame, today: date) -> tuple[SpentGroup, ...]:
    """Траты месяца по группам, от большей к меньшей; группа в нуле или минусе — нет."""
    found = []
    for name, part in month_spending(frame, today).groupby("group"):
        amount = float(part["amount_uzs"].sum())
        if amount < HALF_SUM:
            continue
        subs = part.groupby("subcategory")["amount_uzs"].sum()
        items = tuple(
            SpentItem(str(sub), float(value))
            for sub, value in subs.sort_values(ascending=False).items()
            if abs(value) >= HALF_SUM
        )
        found.append(SpentGroup(str(name), amount, items))
    return tuple(sorted(found, key=lambda group: group.amount, reverse=True))


def _by_spending(line: CategoryLine | Category) -> tuple[float, float]:
    return -line.fact, -line.plan


def categories(
    groups: Sequence[GroupPlan], spent: Sequence[SpentGroup]
) -> tuple[Category, ...]:
    """План вкладки и траты по строкам fact — по группам и подкатегориям.

    Всё, у чего есть план или траты: от больших трат к меньшим, без трат — по
    плану, при равенстве — в порядке вкладки. Траты статьи без плана — с планом 0.
    """
    plans = {g.name: {line.subcategory: line.plan for line in g.items} for g in groups}
    facts = {g.name: {item.subcategory: item.amount for item in g.items} for g in spent}
    totals = {group.name: group.amount for group in spent}
    found = []
    for name in [*plans, *(name for name in facts if name not in plans)]:
        planned, paid = plans.get(name, {}), facts.get(name, {})
        lines = [
            CategoryLine(sub, planned.get(sub, 0.0), paid.get(sub, 0.0))
            for sub in [*planned, *(sub for sub in paid if sub not in planned)]
        ]
        lines = [line for line in lines if line.plan > 0 or abs(line.fact) >= HALF_SUM]
        plan = sum(line.plan for line in lines)
        fact = totals.get(name, 0.0)
        if plan <= 0 and fact < HALF_SUM:
            continue
        found.append(Category(name, plan, fact, tuple(sorted(lines, key=_by_spending))))
    return tuple(sorted(found, key=_by_spending))


def untracked(
    frame: pd.DataFrame, rows: list[list[Any]], today: date
) -> tuple[Untracked, ...]:
    """Траты месяца по статьям, у которых нет строки расходов во вкладке месяца.

    Вкладка считает факт формулами своих строк по паре (подкатегория, валюта),
    подкатегорию — без учёта регистра, как SUMIFS: траты статьи без строки в её
    план-факт не попадают, а с подкатегорией строки доходов — уходят в доходы.
    """
    keys = {
        (article.subcategory.casefold(), article.currency)
        for article in read_plan(rows).articles
        if article.kind == "spend"
    }
    found = []
    columns = ["group", "subcategory", "currency"]
    for (group, sub, currency), part in month_spending(frame, today).groupby(columns):
        amount = float(part["amount_uzs"].sum())
        if (str(sub).casefold(), str(currency)) in keys or abs(amount) < HALF_SUM:
            continue
        found.append(Untracked(str(group), str(sub), str(currency), amount))
    return tuple(sorted(found, key=lambda item: item.amount, reverse=True))


def subscription_state(series: Series, today: date) -> str:
    """Списано в этом месяце, дважды, ещё ждём или день прошёл без списания."""
    charged = series.charged_in(today)
    if len(charged) >= 2:
        return "twice"
    if charged:
        return "charged"
    return "expected" if series.expected_on(today) >= today else "missed"


def subscriptions(frame: pd.DataFrame, today: date) -> tuple[Subscription, ...]:
    return tuple(
        Subscription(
            series.name,
            series.day,
            series.last.amount,
            series.last.currency,
            series.last.uzs,
            subscription_state(series, today),
        )
        for series in find_series(frame, today)
        if not series.lapsed(today)
    )


def build_dashboard(
    frame: pd.DataFrame,
    rows: list[list[Any]],
    today: date,
    rates: Rates,
    frozen_currency: str | None,
    records: Sequence[CalendarRecord] = (),
    cards: Sequence[CardBalance] = (),
) -> Dashboard:
    """Сводка по fact и вкладке месяца; без блока прогноза — без лимита и списка."""
    spend, _ = plan_lines(rows)
    groups = group_plans(spend)
    spent = spent_groups(frame, today)
    missing = untracked(frame, rows, today)
    joined = categories(groups, spent)
    forecast = read_forecast(rows)
    budget = day_budget(frame, rows, today, rates, frozen_currency, records)
    savings = month_savings(frame, today)
    if forecast is None or budget is None:
        return Dashboard(
            "no_forecast",
            None,
            groups,
            (),
            (),
            (),
            spent=spent,
            untracked=missing,
            cards=tuple(cards),
            categories=joined,
            savings=savings,
        )
    upcoming = budget.ahead
    return Dashboard(
        "ok",
        budget,
        groups,
        tuple(read_daily(rows)),
        upcoming,
        subscriptions(frame, today),
        spent=spent,
        untracked=missing,
        cards=tuple(cards),
        categories=joined,
        cushion=cushion_of(
            budget, rates, monthly_spending(frame, today), config.CUSHION_MONTHS
        ),
        savings=savings,
    )
