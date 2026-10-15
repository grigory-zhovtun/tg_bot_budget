"""Прогноз денег до конца месяца — по каждой валюте карт и всего.

Деньги «сейчас» — по листу fact вместе с сегодняшними операциями, как «Сейчас на
картах (факт)» вкладки. Дальше по дням до последнего дня месяца две линии: «по
плану» — остаток плана статей валюты, разложенный по дням, и «по темпу» — средние
траты за PACE_DAYS дней до сегодня. Строки жёлтого списка, которые ещё не пришли и
не ушли, — события в свой день; опоздавшие ждём сегодня.

План и темп в день считаются на утро (сверка списка по вчера) и за день не
меняются. Сегодняшние траты уже в деньгах «сейчас», поэтому за сегодня вычитается
только то, что от дня ещё осталось.
"""

import calendar
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

import pandas as pd

from app.domain import INCOME_GROUP
from app.services.analytics_service import expenses
from app.services.receipts import NOTHING, Receipt, match_receipts, remaining

PACE_DAYS = 14
CENT = 0.01  # ниже нуля — меньше −0,01: ноль из округлений минусом не считается


class PlanArticle(Protocol):
    """Статья вкладки месяца: план положительный, в валюте статьи."""

    kind: str  # spend | income
    subcategory: str
    currency: str
    plan: float


class ListItem(Protocol):
    """Строка жёлтого списка: строка вкладки, название, день, сумма со знаком."""

    row: int
    name: str
    day: int
    amount: float
    currency: str


@dataclass(frozen=True)
class Event:
    """Строка списка, которая ещё впереди: сумма — остаток, в валюте строки."""

    day: int
    name: str
    amount: float
    late: bool  # день строки прошёл, а денег нет — ждём сегодня


@dataclass(frozen=True)
class Evening:
    """Сколько будет вечером дня day: по плану и по темпу."""

    day: int
    plan: float
    pace: float


@dataclass(frozen=True)
class Dip:
    """Уход в минус: первый день ниже нуля и первый день после него снова не ниже."""

    start: int
    until: int | None  # None — до конца месяца не вернётся


@dataclass(frozen=True)
class CurrencyForecast:
    code: str
    now: float  # деньги сейчас, с сегодняшними операциями
    per_day_plan: float
    per_day_pace: float
    spent_today: float  # без платежей списка; не меньше нуля
    events: tuple[Event, ...]
    days: tuple[Evening, ...]  # с сегодня по последний день месяца
    plan_dip: Dip | None
    pace_dip: Dip | None


def below_zero(value: float) -> bool:
    return value < -CENT


def month_end(today: date) -> date:
    return today.replace(day=calendar.monthrange(today.year, today.month)[1])


def dip(days: Sequence[int], values: Sequence[float]) -> Dip | None:
    """Первый день ниже нуля и первый день после него не ниже; None — минуса нет."""
    start = next((i for i, value in enumerate(values) if below_zero(value)), None)
    if start is None:
        return None
    back = next(
        (i for i in range(start + 1, len(values)) if not below_zero(values[i])), None
    )
    return Dip(days[start], None if back is None else days[back])


def _paid(receipts: Mapping[int, Receipt]) -> frozenset[int]:
    """Строки fact, которыми закрыты строки списка."""
    return frozenset(i for receipt in receipts.values() for i in receipt.operations)


def _money_now(frame: pd.DataFrame, code: str, today: date) -> float:
    """Доходы плюсом, остальное с обратным знаком колонки D — по сегодня."""
    own = frame[(frame["currency"] == code) & (frame["day"] <= today)]
    flow = own["amount"].where(own["group"] == INCOME_GROUP, -own["amount"])
    return float(flow.sum())


def _spent(
    frame: pd.DataFrame, code: str, first: date, last: date, paid: frozenset[int]
) -> float:
    """Траты валюты с first по last: без доходов, переводов и платежей списка."""
    spent = expenses(frame)
    mask = (
        (spent["currency"] == code)
        & (spent["day"] >= first)
        & (spent["day"] <= last)
        & ~spent.index.isin(list(paid))
    )
    return float(spent.loc[mask, "amount"].sum())


def _plan_per_day(
    frame: pd.DataFrame,
    articles: Sequence[PlanArticle],
    items: Sequence[ListItem],
    morning: Mapping[int, Receipt],
    code: str,
    today: date,
) -> float:
    """Остаток плана статей без неоплаченных к утру платежей списка — на день."""
    plans: dict[str, float] = {}
    for article in articles:
        if article.kind == "spend" and article.currency == code:
            plans[article.subcategory] = (
                plans.get(article.subcategory, 0.0) + article.plan
            )
    month = frame[
        (frame["currency"] == code)
        & (frame["day"] >= today.replace(day=1))
        & (frame["day"] < today)
    ]
    facts = month.groupby("subcategory")["amount"].sum()
    rest = sum(
        max(plan - float(facts.get(sub, 0.0)), 0.0) for sub, plan in plans.items()
    )
    unpaid = -sum(
        remaining(item, morning.get(item.row, NOTHING))
        for item in items
        if item.currency == code and item.amount < 0
    )
    left = (month_end(today) - today).days + 1
    return max(rest - unpaid, 0.0) / left


def _events(
    items: Sequence[ListItem], now: Mapping[int, Receipt], code: str, today: date
) -> tuple[Event, ...]:
    """Незакрытые строки списка валюты: остаток в их день, опоздавшие — сегодня."""
    last = month_end(today).day
    found: list[Event] = []
    for item in items:
        if item.currency != code:
            continue
        rest = remaining(item, now.get(item.row, NOTHING))
        if not rest:
            continue
        day = min(item.day, last)  # 31-е в коротком месяце — его последний день
        late = day < today.day
        found.append(Event(today.day if late else day, item.name, rest, late))
    return tuple(sorted(found, key=lambda event: event.day))


def _line(
    now: float,
    events: Sequence[Event],
    per_day: float,
    spent_today: float,
    today: date,
) -> list[float]:
    """Остаток на вечер каждого дня: за сегодня — только то, что от дня осталось."""
    moves: dict[int, float] = {}
    for event in events:
        moves[event.day] = moves.get(event.day, 0.0) + event.amount
    left = now + moves.get(today.day, 0.0) - max(per_day - spent_today, 0.0)
    values = [left]
    for number in range(today.day + 1, month_end(today).day + 1):
        left += moves.get(number, 0.0) - per_day
        values.append(left)
    return values


def currency_forecast(
    frame: pd.DataFrame,
    articles: Sequence[PlanArticle],
    items: Sequence[ListItem],
    today: date,
    code: str,
) -> CurrencyForecast:
    """Прогноз одной валюты с сегодня по последний день месяца."""
    first = today.replace(day=1)
    yesterday = today - timedelta(days=1)
    morning = match_receipts(frame, items, first, yesterday)
    now = match_receipts(frame, items, first, today)
    start = today - timedelta(days=PACE_DAYS)
    window = _spent(frame, code, start, yesterday, _paid(morning))
    pace = max(window / PACE_DAYS, 0.0)
    spent = max(_spent(frame, code, today, today, _paid(now)), 0.0)
    per_day = _plan_per_day(frame, articles, items, morning, code, today)
    events = _events(items, now, code, today)
    money = _money_now(frame, code, today)
    plan = _line(money, events, per_day, spent, today)
    paced = _line(money, events, pace, spent, today)
    numbers = list(range(today.day, month_end(today).day + 1))
    return CurrencyForecast(
        code,
        money,
        per_day,
        pace,
        spent,
        events,
        tuple(
            Evening(number, by_plan, by_pace)
            for number, by_plan, by_pace in zip(numbers, plan, paced, strict=True)
        ),
        dip(numbers, plan),
        dip(numbers, paced),
    )
