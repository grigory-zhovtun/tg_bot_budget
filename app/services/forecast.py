"""Прогноз денег до конца месяца — по каждой валюте карт и всего.

Деньги «сейчас» — по листу fact вместе с сегодняшними операциями, как «Сейчас на
картах (факт)» вкладки. Дальше по дням до последнего дня месяца две линии: «по
плану» — остаток плана статей валюты, разложенный по дням, и «по темпу» — средние
траты за PACE_DAYS дней до сегодня. Строки жёлтого списка, которые ещё не пришли и
не ушли, — события в свой день; опоздавшие ждём сегодня. Остатки поступлений
прошлого месяца (перенос) сверка строк закрывает первыми; событий они не дают.

План и темп в день считаются на утро (сверка списка по вчера) и за день не
меняются. Сегодняшние траты уже в деньгах «сейчас», поэтому за сегодня вычитается
только то, что от дня ещё осталось.

Записи календаря (services.calendar) — часть плана своей статьи на день: остаток
статьи сначала покрывает её записи, по дням идёт то, что осталось; записи — события
в свой день, разовые — в обеих линиях, повторы — только «по плану».

Прошедшие дни месяца — по факту: деньги на вечер дня (так же, как «сейчас»), на
сколько они за день изменились и врозь — траты дня (статьи расходов, возвраты
вычитаются) и доходы дня; переводы между своими картами — ни то, ни другое.
"""

import calendar
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Protocol

import pandas as pd

from app.domain import INCOME_GROUP, Rates
from app.services.analytics_service import expenses, income
from app.services.calendar import (
    CalendarRecord,
    CalendarState,
    Hint,
    Occurrence,
    calendar_state,
    hints,
)
from app.services.receipts import NOTHING, Receipt, match_receipts, remaining

PACE_DAYS = 14
ORDER = ("UZS", "USD", "RUB")  # дальше — остальные валюты по алфавиту
CENT = 0.01  # ниже нуля — меньше −0,01: ноль из округлений минусом не считается


class PlanArticle(Protocol):
    """Статья вкладки месяца: план положительный, в валюте статьи."""

    kind: str  # spend | income
    group: str
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
class DayArticle:
    """Статья дня по факту: сумма трат (возврат — минусом) или дохода."""

    group: str
    subcategory: str
    amount: float  # в валюте строк
    uzs: float  # в сумах по курсу таблицы; 0 — курса нет
    income: bool


@dataclass(frozen=True)
class PastDay:
    """День по факту: деньги на вечер и изменение за день (доход плюсом).
    Сегодня — то, что уже было: деньги сейчас и сегодняшние операции."""

    day: int
    evening: float
    change: float
    spent: float = 0.0  # траты дня по статьям расходов; возвраты вычитаются
    earned: float = 0.0  # доходы дня без «нач остаток»
    articles: tuple[DayArticle, ...] = ()  # траты по статьям от больших, доходы


@dataclass(frozen=True)
class Dip:
    """Уход в минус: первый день ниже нуля и первый день после него снова не ниже."""

    start: int
    until: int | None  # None — до конца месяца не вернётся


@dataclass(frozen=True)
class RecordDay:
    """Вхождение записи календаря в день прогноза."""

    id: str
    day: int
    name: str  # название записи или подкатегория
    group: str
    subcategory: str
    repeat: str  # once | daily | weekdays | monthly
    amount: float  # сумма записи
    open: float  # открыто на сейчас; 0 — закрыта фактом
    over: bool  # записей статьи больше её остатка
    spent: float = 0.0  # сколько потрачено на самом деле (Occurrence.spent)


@dataclass(frozen=True)
class ArticleState:
    """Статья трат: план, остаток (план − факт по вчера) и записи на утро."""

    group: str
    subcategory: str
    currency: str
    plan: float
    rest: float
    booked: float


@dataclass(frozen=True)
class RecordMonth:
    """Запись для списка экрана: повтор или разовая с сегодня."""

    record: CalendarRecord
    month_total: float  # открыто на сейчас с сегодня до конца месяца
    booked: float  # её доля в записях статьи на утро
    over: bool


@dataclass(frozen=True)
class CurrencyForecast:
    code: str
    now: float  # деньги сейчас, с сегодняшними операциями
    per_day_plan: float
    per_day_pace: float
    spent_today: float  # обычные траты сегодня: без платежей списка и записей
    events: tuple[Event, ...]
    days: tuple[Evening, ...]  # с сегодня по последний день месяца
    plan_dip: Dip | None
    pace_dip: Dip | None
    records: tuple[RecordDay, ...] = ()
    articles: tuple[ArticleState, ...] = ()
    past: tuple[PastDay, ...] = ()  # с 1-го по сегодня — то, что уже было


@dataclass(frozen=True)
class MonthForecast:
    today: date
    end: date
    currencies: tuple[CurrencyForecast, ...]  # UZS, USD, RUB, остальные по алфавиту
    counted: frozenset[str]  # валюты с курсом таблицы — они во «Всего»
    total_uzs: tuple[Evening, ...]
    total_usd: tuple[Evening, ...]  # пусто — нет курса доллара
    plan_dip: Dip | None  # у «Всего»
    pace_dip: Dip | None
    calendar: tuple[RecordMonth, ...] = ()
    hints: Mapping[str, Hint] = field(default_factory=dict)
    rates: Mapping[str, float] = field(default_factory=dict)  # к суму, без UZS
    past_uzs: tuple[PastDay, ...] = ()  # «Всего» по сегодняшним курсам
    past_usd: tuple[PastDay, ...] = ()  # пусто — нет курса доллара


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


def _by_article(rows: pd.DataFrame, earned: bool) -> dict[date, list[DayArticle]]:
    """Суммы статей по дням, от больших к меньшим."""
    found: dict[date, list[DayArticle]] = {}
    if rows.empty:
        return found
    sums = (
        rows.groupby(["day", "group", "subcategory"], sort=False)[
            ["amount", "amount_uzs"]
        ]
        .sum()
        .reset_index()
        .sort_values(["day", "amount"], ascending=[True, False], kind="stable")
    )
    for row in sums.itertuples(index=False):
        found.setdefault(row.day, []).append(
            DayArticle(
                str(row.group),
                str(row.subcategory),
                float(row.amount),
                float(row.amount_uzs),
                earned,
            )
        )
    return found


def _past(frame: pd.DataFrame, code: str, today: date) -> tuple[PastDay, ...]:
    """Дни месяца по сегодня: деньги на вечер, как «сейчас», их изменение за день,
    траты и доходы дня — итогом и по статьям. Сегодня — то, что уже было."""
    own = frame[frame["currency"] == code]
    flow = own["amount"].where(own["group"] == INCOME_GROUP, -own["amount"])
    by_day = flow.groupby(own["day"]).sum()
    first = today.replace(day=1)
    # траты и статьи — только за месяц: остаток на его начало берётся из by_day
    month = own[(own["day"] >= first) & (own["day"] <= today)]
    spending, incomes = expenses(month), income(month)
    spent = spending.groupby("day")["amount"].sum()
    earned = incomes.groupby("day")["amount"].sum()
    spent_by = _by_article(spending, earned=False)
    earned_by = _by_article(incomes, earned=True)
    evening = float(by_day[by_day.index < first].sum())
    found = []
    for number in range(1, today.day + 1):
        day = first.replace(day=number)
        change = float(by_day.get(day, 0.0))
        evening += change
        found.append(
            PastDay(
                number,
                evening,
                change,
                float(spent.get(day, 0.0)),
                float(earned.get(day, 0.0)),
                tuple(spent_by.get(day, []) + earned_by.get(day, [])),
            )
        )
    return tuple(found)


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


def _book(
    frame: pd.DataFrame,
    articles: Sequence[PlanArticle],
    state: CalendarState,
    code: str,
    today: date,
) -> tuple[ArticleState, ...]:
    """Статьи трат валюты: план, остаток (план − факт с 1-го по вчера), записи на утро.

    Строки одной подкатегории складываются; у статьи записи без строки во вкладке
    план 0 — её записи целиком сверх плана.
    """
    plans: dict[str, tuple[str, float]] = {}
    for article in articles:
        if article.kind == "spend" and article.currency == code:
            group, plan = plans.get(article.subcategory, (article.group, 0.0))
            plans[article.subcategory] = (group, plan + article.plan)
    booked: dict[str, float] = {}
    groups: dict[str, str] = {}
    for occurrence in state.occurrences:
        sub = occurrence.record.subcategory
        booked[sub] = booked.get(sub, 0.0) + occurrence.morning
        groups.setdefault(sub, occurrence.record.group)
    month = frame[
        (frame["currency"] == code)
        & (frame["day"] >= today.replace(day=1))
        & (frame["day"] < today)
    ]
    facts = month.groupby("subcategory")["amount"].sum()
    found: list[ArticleState] = []
    for sub in [*plans, *(name for name in booked if name not in plans)]:
        group, plan = plans.get(sub, (groups.get(sub, ""), 0.0))
        rest = max(plan - float(facts.get(sub, 0.0)), 0.0)
        found.append(ArticleState(group, sub, code, plan, rest, booked.get(sub, 0.0)))
    return tuple(found)


def _spread(book: Sequence[ArticleState]) -> float:
    """Остаток статей после резерва записей — его план раскладывает по дням."""
    return sum(article.rest - min(article.booked, article.rest) for article in book)


def _unpaid(
    items: Sequence[ListItem], morning: Mapping[int, Receipt], code: str
) -> float:
    """Неоплаченные к утру платежи списка валюты — положительной суммой."""
    return -sum(
        remaining(item, morning.get(item.row, NOTHING))
        for item in items
        if item.currency == code and item.amount < 0
    )


def _calendar(
    frame: pd.DataFrame,
    records: Sequence[CalendarRecord],
    morning: Mapping[int, Receipt],
    now: Mapping[int, Receipt],
    today: date,
    code: str,
) -> CalendarState:
    """Вхождения записей валюты; операции платежей списка записям не достаются."""
    return calendar_state(
        frame,
        [record for record in records if record.currency == code],
        today,
        month_end(today),
        today - timedelta(days=PACE_DAYS),
        _paid(morning),
        _paid(now),
    )


def _share(
    spreads: Mapping[str, float],
    unpaid: Mapping[str, float],
    rates: Rates,
    codes: Sequence[str],
) -> dict[str, float]:
    """Сколько плана статей ещё разложить по дням в каждой валюте.

    Неоплаченный платёж списка вычитается из статей своей валюты. Если их не
    хватает (платёж в долларах, а статья в сумах), остаток по курсу таблицы
    снимается со статей других валют, сначала сумов: как и вкладка, прогноз не
    считает платёж дважды — днями в одной валюте и событием в другой.
    """
    left: dict[str, float] = {}
    extra = 0.0  # в сумах: платежи, которым не хватило статей своей валюты
    for code in codes:
        left[code] = max(spreads[code] - unpaid[code], 0.0)
        extra += max(unpaid[code] - spreads[code], 0.0) * rates.to_uzs.get(code, 0.0)
    for code in codes:
        rate = rates.to_uzs.get(code)
        if not rate or extra <= 0:
            continue
        taken = min(left[code] * rate, extra)
        left[code] -= taken / rate
        extra -= taken
    return left


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


def _moves(
    events: Sequence[Event], occurrences: Sequence[Occurrence], once_only: bool
) -> dict[int, float]:
    """Движения денег по дням: строки списка и открытое на сейчас у записей."""
    moves: dict[int, float] = {}
    for event in events:
        moves[event.day] = moves.get(event.day, 0.0) + event.amount
    for occurrence in occurrences:
        if occurrence.now and (not once_only or occurrence.record.repeat == "once"):
            day = occurrence.day.day
            moves[day] = moves.get(day, 0.0) - occurrence.now
    return moves


def _line(
    now: float,
    moves: Mapping[int, float],
    per_day: float,
    spent_today: float,
    today: date,
) -> list[float]:
    """Остаток на вечер каждого дня: за сегодня — только то, что от дня осталось."""
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
    plan_left: float | None = None,
    records: Sequence[CalendarRecord] = (),
    carried: Sequence[ListItem] = (),
) -> CurrencyForecast:
    """Прогноз одной валюты с сегодня по последний день месяца.

    plan_left — сколько плана статей разложить по дням (month_forecast делит его
    между валютами); None — статьи своей валюты без резерва записей и без её
    неоплаченных платежей. records — записи календаря всех валют. carried — остатки
    поступлений прошлого месяца (day_budget.carried_items): только в сверку строк.
    """
    first = today.replace(day=1)
    yesterday = today - timedelta(days=1)
    morning = match_receipts(frame, items, first, yesterday, carried)
    now = match_receipts(frame, items, first, today, carried)
    state = _calendar(frame, records, morning, now, today, code)
    start = today - timedelta(days=PACE_DAYS)
    paid_morning = _paid(morning) | state.closed_morning
    pace = max(_spent(frame, code, start, yesterday, paid_morning) / PACE_DAYS, 0.0)
    spent = _spent(frame, code, today, today, _paid(now) | state.closed_now)
    covered = sum(item.covered for item in state.occurrences)
    covered_once = sum(
        item.covered for item in state.occurrences if item.record.repeat == "once"
    )
    book = _book(frame, articles, state, code, today)
    if plan_left is None:
        plan_left = max(_spread(book) - _unpaid(items, morning, code), 0.0)
    per_day = plan_left / ((month_end(today) - today).days + 1)
    events = _events(items, now, code, today)
    money = _money_now(frame, code, today)
    spent_plan = max(spent - covered, 0.0)
    plan = _line(
        money, _moves(events, state.occurrences, False), per_day, spent_plan, today
    )
    spent_pace = max(spent - covered_once, 0.0)
    paced = _line(
        money, _moves(events, state.occurrences, True), pace, spent_pace, today
    )
    over = {
        article.subcategory for article in book if article.booked > article.rest + CENT
    }
    numbers = list(range(today.day, month_end(today).day + 1))
    return CurrencyForecast(
        code,
        money,
        per_day,
        pace,
        spent_plan,
        events,
        tuple(
            Evening(number, by_plan, by_pace)
            for number, by_plan, by_pace in zip(numbers, plan, paced, strict=True)
        ),
        dip(numbers, plan),
        dip(numbers, paced),
        tuple(
            RecordDay(
                item.record.id,
                item.day.day,
                item.record.title,
                item.record.group,
                item.record.subcategory,
                item.record.repeat,
                item.record.amount,
                item.now,
                item.record.subcategory in over,
                item.spent,
            )
            for item in state.occurrences
        ),
        book,
        _past(frame, code, today),
    )


def currency_order(codes: Iterable[str]) -> list[str]:
    """UZS, USD, RUB, остальные по алфавиту; без повторов и пустых."""
    unique = {code for code in codes if code}
    return [code for code in ORDER if code in unique] + sorted(unique - set(ORDER))


def _month_records(
    lines: Sequence[CurrencyForecast],
    records: Sequence[CalendarRecord],
    booked: Mapping[str, float],
    today: date,
) -> tuple[RecordMonth, ...]:
    """Записи для списка: все повторы и разовые с сегодня (и в следующих месяцах) —
    чтобы любую можно было открыть и поправить; сначала повторы, потом разовые."""
    totals: dict[str, float] = {}
    over: dict[str, bool] = {}
    for line in lines:
        for day in line.records:
            totals[day.id] = totals.get(day.id, 0.0) + day.open
            over[day.id] = over.get(day.id, False) or day.over
    shown = sorted(
        (r for r in records if r.repeat != "once" or r.start >= today),
        key=lambda record: (record.repeat == "once", record.start, record.id),
    )
    return tuple(
        RecordMonth(
            record,
            totals.get(record.id, 0.0),
            booked.get(record.id, 0.0),
            over.get(record.id, False),
        )
        for record in shown
    )


def month_forecast(
    frame: pd.DataFrame,
    articles: Sequence[PlanArticle],
    items: Sequence[ListItem],
    today: date,
    rates: Rates,
    currencies: Iterable[str],
    records: Sequence[CalendarRecord] = (),
    carried: Sequence[ListItem] = (),
) -> MonthForecast:
    """Прогноз по валютам карт и всего — по сегодняшним курсам таблицы; carried —
    остатки поступлений прошлого месяца, только в сверку строк."""
    codes = currency_order(currencies)
    first = today.replace(day=1)
    morning = match_receipts(frame, items, first, today - timedelta(days=1), carried)
    now = match_receipts(frame, items, first, today, carried)
    spreads: dict[str, float] = {}
    unpaid: dict[str, float] = {}
    booked: dict[str, float] = {}  # ID записи → её открытое на утро с сегодня
    for code in codes:
        state = _calendar(frame, records, morning, now, today, code)
        spreads[code] = _spread(_book(frame, articles, state, code, today))
        unpaid[code] = _unpaid(items, morning, code)
        for occurrence in state.occurrences:
            key = occurrence.record.id
            booked[key] = booked.get(key, 0.0) + occurrence.morning
    left = _share(spreads, unpaid, rates, codes)
    lines = tuple(
        currency_forecast(
            frame, articles, items, today, code, left[code], records, carried
        )
        for code in codes
    )
    rated = [(line, rate) for line in lines if (rate := rates.to_uzs.get(line.code))]
    numbers = list(range(today.day, month_end(today).day + 1))
    plans = [0.0] * len(numbers)
    paces = [0.0] * len(numbers)
    for line, rate in rated:
        for index, point in enumerate(line.days):
            plans[index] += point.plan * rate
            paces[index] += point.pace * rate
    uzs = tuple(
        Evening(number, plan, pace)
        for number, plan, pace in zip(numbers, plans, paces, strict=True)
    )
    dollar = rates.to_uzs.get("USD")
    usd = (
        tuple(Evening(p.day, p.plan / dollar, p.pace / dollar) for p in uzs)
        if dollar
        else ()
    )
    sums = [[0.0, 0.0, 0.0, 0.0] for _ in range(today.day)]
    for line, rate in rated:
        for index, past in enumerate(line.past):
            for slot, value in enumerate(
                (past.evening, past.change, past.spent, past.earned)
            ):
                sums[index][slot] += value * rate
    past_uzs = tuple(PastDay(index + 1, *values) for index, values in enumerate(sums))
    past_usd = (
        tuple(
            PastDay(
                p.day,
                p.evening / dollar,
                p.change / dollar,
                p.spent / dollar,
                p.earned / dollar,
            )
            for p in past_uzs
        )
        if dollar
        else ()
    )
    return MonthForecast(
        today,
        month_end(today),
        lines,
        frozenset(line.code for line, _ in rated),
        uzs,
        usd,
        dip(numbers, [point.plan for point in uzs]),
        dip(numbers, [point.pace for point in uzs]),
        _month_records(lines, records, booked, today),
        hints(frame, today),
        {code: rate for code, rate in rates.to_uzs.items() if code != "UZS"},
        past_uzs,
        past_usd,
    )
