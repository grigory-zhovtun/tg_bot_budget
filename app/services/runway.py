"""Карточка «💳 Сумы»: когда при нынешнем темпе закончатся сумы и сколько долларов
обменять, чтобы дотянуть до поступления, которое закроет минус.

Её считает модель прогноза (services.forecast): линия «по темпу» в сумах — деньги
сейчас вместе с сегодняшними операциями, темп за 14 дней до сегодня без доходов,
переводов и платежей списка, строки списка в их день. Поэтому «Сводка», утреннее
сообщение и вкладка «Календарь» называют один и тот же день.
"""

import math
from dataclasses import dataclass
from datetime import date, timedelta

from app.domain import Rates
from app.services.forecast import CurrencyForecast, below_zero

DOLLAR_STEP = 10  # обмен округляем вверх до $10


@dataclass(frozen=True)
class Runway:
    balance: float  # сумы сейчас, с сегодняшними операциями
    pace: float  # средние траты из сумов в день
    last_day: date | None  # последний день, когда сумы ещё не в минусе; None — хватит
    short: float  # сколько сумов не хватит в самой низкой точке; 0 — хватает
    until: date | None  # поступление, которое закроет минус; None — до конца месяца
    dollars: float | None  # сколько долларов обменять; None — не нужно или нет курса


def uzs_runway(line: CurrencyForecast, today: date, rates: Rates) -> Runway:
    """Линия «по темпу» прогноза в сумах → карточка «💳 Сумы»."""
    path = [(today.replace(day=point.day), point.pace) for point in line.days]
    gone = next((day for day, value in path if below_zero(value)), None)
    last_day = None if gone is None else gone - timedelta(days=1)
    lowest_day, lowest = min(path, key=lambda point: point[1])
    if not below_zero(lowest):
        return Runway(line.now, line.per_day_pace, last_day, 0.0, None, None)
    short = -lowest
    until = next(
        (day for day, value in path if day > lowest_day and not below_zero(value)),
        None,
    )
    rate = rates.to_uzs.get("USD")
    dollars = None
    if rate:
        dollars = float(math.ceil(short / rate / DOLLAR_STEP) * DOLLAR_STEP)
    return Runway(line.now, line.per_day_pace, last_day, short, until, dollars)
