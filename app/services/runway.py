"""Карточка «💳 Сумы»: хватит ли сумов при нынешнем темпе до зарплаты и сколько
долларов обменять, если нет.

Расчёт свой, простой, по дням срока. Срок — по день следующей основной зарплаты,
которая ещё не пришла; сегодняшняя и опоздавшая, пока денег нет, ждутся сегодня, и
срок кончается сегодня; сама зарплата не прибавляется. Начало — сумы сейчас, вместе
с сегодняшними операциями. Сегодня уходит то, что осталось от темпа дня, дальше
каждый день — темп; авансы срока приходят, платежи списка и подписки в сумах уходят в
свой день (day_budget). Темп — средние траты в сумах за 14 дней до сегодня без
доходов, переводов, платежей списка и оплат разовых записей календаря: его, сумы
сейчас и траты сегодня даёт модель «Календаря» (services.forecast), сама она живёт
в днях календарного месяца и здесь не используется.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta

from app.domain import Rates
from app.services.forecast import below_zero

DOLLAR_STEP = 10  # обмен округляем вверх до $10


@dataclass(frozen=True)
class Runway:
    balance: float  # сумы сейчас, с сегодняшними операциями
    pace: float  # средние траты из сумов в день
    last_day: date | None  # последний день, когда сумы ещё не в минусе; None — хватит
    short: float  # сколько сумов не хватит в самой низкой точке; 0 — хватает
    # до какого дня обменять — день срока, если не хватает; None — хватает или срок до
    # конца месяца
    until: date | None
    dollars: float | None  # сколько долларов обменять; None — не нужно или нет курса
    left: float = 0.0  # сколько останется вечером в день срока
    payday: date | None = None  # день срока — день зарплаты; None — конец месяца
    payday_name: str = ""  # её строка списка: «ЗП», «Аванс»


def horizon_runway(
    now: float,
    pace: float,
    spent_today: float,
    today: date,
    end: date,
    events: Sequence[tuple[date, float]],
    rates: Rates,
    payday: date | None,
    payday_name: str = "",
) -> Runway:
    """Сумы на вечер каждого дня с сегодня по end включительно → карточка «💳 Сумы».

    Сегодня — сумы сейчас и события дня минус то, что осталось от темпа дня; дальше —
    события дня минус темп. events — (день, сумма): аванс плюсом, платёж минусом.
    """
    moves: dict[date, float] = {}
    for day, amount in events:
        moves[day] = moves.get(day, 0.0) + amount
    left = now + moves.get(today, 0.0) - max(pace - spent_today, 0.0)
    path = [(today, left)]
    day = today
    while day < end:
        day += timedelta(days=1)
        left += moves.get(day, 0.0) - pace
        path.append((day, left))
    lowest = min(value for _, value in path)
    if not below_zero(lowest):
        return Runway(now, pace, None, 0.0, None, None, left, payday, payday_name)
    gone = next(day for day, value in path if below_zero(value))
    short = -lowest
    rate = rates.to_uzs.get("USD")
    dollars = None
    if rate:
        dollars = float(math.ceil(short / rate / DOLLAR_STEP) * DOLLAR_STEP)
    return Runway(
        now,
        pace,
        gone - timedelta(days=1),
        short,
        payday,
        dollars,
        left,
        payday,
        payday_name,
    )
