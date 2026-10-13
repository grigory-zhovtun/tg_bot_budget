"""Прогноз по сумам: когда при нынешнем темпе закончатся деньги в сумах и сколько
долларов обменять, чтобы дотянуть до поступления, которое закроет минус.

Сумы — все источники в UZS (карты и наличные) по листу fact на вечер вчера.
Темп — средние траты в сумах за PACE_DAYS дней до сегодняшнего: без доходов и
переводов между своими картами, возврат уменьшает траты. Дальше по дням до
конца месяца: минус темп, плюс строки списка «Поступления и платежи» в сумах
(аванс, зарплата) в их день.
"""

import calendar
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

import pandas as pd

from app.domain import INCOME_GROUP, Rates
from app.services.analytics_service import expenses

PACE_DAYS = 14
DOLLAR_STEP = 10  # обмен округляем вверх до $10


class ListItem(Protocol):
    """Строка списка поступлений и платежей: день месяца, сумма со знаком, валюта."""

    day: int
    amount: float
    currency: str


@dataclass(frozen=True)
class Runway:
    balance: float  # сумы на вечер вчера
    pace: float  # средние траты из сумов в день
    last_day: date | None  # последний день, когда сумы ещё не в минусе; None — хватит
    short: float  # сколько сумов не хватит в самой низкой точке; 0 — хватает
    until: date | None  # поступление, которое закроет минус; None — до конца месяца
    dollars: float | None  # сколько долларов обменять; None — не нужно или нет курса


def uzs_balance(frame: pd.DataFrame, through: date) -> float:
    """Сумы на вечер дня: доход плюс, остальное с обратным знаком колонки D."""
    own = frame[frame["currency"] == "UZS"]
    flow = own["amount"].where(own["group"] == INCOME_GROUP, -own["amount"])
    return float(flow[own["day"] <= through].sum())


def uzs_pace(frame: pd.DataFrame, today: date) -> float:
    """Средние траты из сумов в день за PACE_DAYS дней до сегодняшнего."""
    spent = expenses(frame)
    spent = spent[spent["currency"] == "UZS"]
    start = today - timedelta(days=PACE_DAYS)
    window = spent[(spent["day"] >= start) & (spent["day"] < today)]
    return float(window["amount"].sum()) / PACE_DAYS


def uzs_runway(
    frame: pd.DataFrame, items: Iterable[ListItem], today: date, rates: Rates
) -> Runway:
    """Сумы по дням до конца месяца при нынешнем темпе трат."""
    days = calendar.monthrange(today.year, today.month)[1]
    moves: dict[int, float] = {}
    for item in items:
        if item.currency == "UZS":
            day = min(item.day, days)  # 31-е в коротком месяце — его последний день
            moves[day] = moves.get(day, 0.0) + item.amount
    balance = uzs_balance(frame, today - timedelta(days=1))
    pace = max(uzs_pace(frame, today), 0.0)

    path: list[tuple[date, float]] = []
    left = balance
    for number in range(today.day, days + 1):
        left += moves.get(number, 0.0) - pace
        path.append((today.replace(day=number), left))

    gone = next((day for day, value in path if value < 0), None)
    last_day = None if gone is None else gone - timedelta(days=1)
    lowest_day, lowest = min(path, key=lambda point: point[1])
    if lowest >= 0:
        return Runway(balance, pace, last_day, 0.0, None, None)
    short = -lowest
    until = next((day for day, value in path if day > lowest_day and value >= 0), None)
    rate = rates.to_uzs.get("USD")
    dollars = None
    if rate:
        dollars = float(math.ceil(short / rate / DOLLAR_STEP) * DOLLAR_STEP)
    return Runway(balance, pace, last_day, short, until, dollars)
