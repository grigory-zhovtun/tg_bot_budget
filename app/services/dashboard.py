"""Данные экрана «Сводка» Mini App: те же расчёты, что /today и /subs.

Лимит и заморозка — DayBudget (как утреннее сообщение), план-факт — plan_lines
вкладки месяца по группам, график — таблица дней вкладки (O:R под «Дата»),
подписки — серии recurring с состоянием этого месяца.
"""

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from app.domain import Rates
from app.services.analytics_service import PlanLine, plan_lines
from app.services.day_budget import (
    DayBudget,
    DayPoint,
    ForecastItem,
    day_budget,
    read_daily,
    read_forecast,
)
from app.services.recurring import Series, find_series


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
class Dashboard:
    status: str  # ok | no_forecast
    budget: DayBudget | None
    groups: tuple[GroupPlan, ...]
    daily: tuple[DayPoint, ...]
    upcoming: tuple[ForecastItem, ...]
    subscriptions: tuple[Subscription, ...]


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
) -> Dashboard:
    """Сводка по fact и вкладке месяца; без блока прогноза — только план-факт."""
    spend, _ = plan_lines(rows)
    groups = group_plans(spend)
    forecast = read_forecast(rows)
    budget = day_budget(frame, rows, today, rates, frozen_currency)
    if forecast is None or budget is None:
        return Dashboard("no_forecast", None, groups, (), (), ())
    upcoming = tuple(
        sorted((i for i in forecast.items if i.day >= today.day), key=lambda i: i.day)
    )
    return Dashboard(
        "ok",
        budget,
        groups,
        tuple(read_daily(rows)),
        upcoming,
        subscriptions(frame, today),
    )
