"""Лимит на день: сколько можно потратить сегодня, чтобы выйти на план месяца.

Тот же расчёт, что в блоке прогноза вкладки месяца (колонки O:S). Остаток на всех
картах на вечер вчера плюс поступления и крупные платежи жёлтого списка, которые
ещё впереди, минус остаток, нужный к концу месяца, — и делим на оставшиеся дни
вместе с сегодняшним. Нужный остаток — по плану (на 1-е число + список − бюджет на
день × дни) или больше, если цель «отложить за месяц» выше плановой экономии.

Заморожено — карты в валюте заморозки (долларовая) без платежей в этой валюте,
которые ещё впереди. Лимит эти деньги не тратит: всё, что было на картах 1-го
числа, входит в нужный к концу месяца остаток, а перевод между своими картами
остаток не меняет.
"""

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd

from app.domain import INCOME_GROUP, Rates
from app.services.analytics_service import (
    SERIAL_ZERO,
    PlanLine,
    expenses,
    money,
    plan_lines,
)

COLUMN_O = 14  # блок прогноза: O название, P день, Q сумма, R валюта, S в сумах
LIST_HEADER = "Поступления"  # «Поступления (+) и крупные платежи (−)»
TABLE_HEADER = "Дата"  # таблица по дням под списком
GOAL_LABEL = "Отложить за месяц"
PAYMENT_MATCH = 0.01  # трата дня совпала с платежом из списка с точностью до 1 %
WEEKDAYS = ("понедельник", "вторник", "среда", "четверг", "пятница", "суббота",
            "воскресенье")  # fmt: skip
MONTHS_PREPOSITIONAL = ("январе", "феврале", "марте", "апреле", "мае", "июне", "июле",
                        "августе", "сентябре", "октябре", "ноябре", "декабре")  # fmt: skip
NEAR_PLAN = 0.8  # с этой доли плана статья жёлтая
MONTHS_GENITIVE = ("января", "февраля", "марта", "апреля", "мая", "июня", "июля",
                   "августа", "сентября", "октября", "ноября", "декабря")  # fmt: skip


@dataclass(frozen=True)
class ForecastItem:
    """Пункт жёлтого списка: поступление (+) или крупный платёж (−)."""

    name: str
    day: int
    amount: float  # в валюте пункта
    currency: str
    uzs: float


@dataclass(frozen=True)
class Forecast:
    items: tuple[ForecastItem, ...]
    savings_goal: float = 0.0  # в сумах, 0 — цели нет


@dataclass(frozen=True)
class DayBudget:
    today: date
    limit: float  # меньше нуля — до плана на конец месяца уже не хватает
    plan_per_day: float
    days_left: int  # вместе с сегодняшним
    balance: float  # на всех картах на вечер вчера, в сумах
    planned_balance: float  # сколько должно было быть по плану на вечер вчера
    yesterday_spent: float
    yesterday_limit: float | None  # None — вчера был прошлый месяц
    spent_today: float
    growth: float  # плановая экономия месяца: остаток на конец − на 1-е число
    goal: float
    frozen: float | None  # в валюте заморозки на вечер вчера; None — не считаем
    frozen_change: float  # с 1-го числа
    frozen_uzs: float
    frozen_currency: str | None
    upcoming: tuple[ForecastItem, ...]
    over_plan: tuple[PlanLine, ...]


def _number(value: Any) -> float | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return None


def _block(row: list[Any]) -> list[Any]:
    cells = list(row[COLUMN_O : COLUMN_O + 5])
    return cells + [""] * (5 - len(cells))


def read_forecast(rows: list[list[Any]]) -> Forecast | None:
    """Жёлтый список и цель накоплений из блока прогноза; None — блока нет."""
    start = next(
        (
            i
            for i, row in enumerate(rows)
            if str(_block(row)[0]).startswith(LIST_HEADER)
        ),
        None,
    )
    if start is None:
        return None
    items: list[ForecastItem] = []
    goal = 0.0
    for row in rows[start + 1 :]:
        name, day, amount, currency, uzs = _block(row)
        if name == TABLE_HEADER:
            break
        if GOAL_LABEL in str(name):
            goal = _number(uzs) or 0.0
            continue
        day, amount, uzs = _number(day), _number(amount), _number(uzs)
        if (
            isinstance(name, str)
            and name.strip()
            and day is not None
            and 1 <= day <= 31
            and amount is not None
            and uzs is not None
        ):
            code = str(currency).strip().upper() or "UZS"
            items.append(ForecastItem(name.strip(), int(day), amount, code, uzs))
    return Forecast(tuple(items), goal)


@dataclass(frozen=True)
class DayPoint:
    """Строка таблицы дней вкладки: остаток на вечер дня по плану и по факту."""

    day: date
    plan: float
    fact: float | None  # None — день ещё не прошёл («#N/A» в таблице)


def read_daily(rows: list[list[Any]]) -> list[DayPoint]:
    """Таблица дней под блоком прогноза (заголовок «Дата» в колонке O)."""
    start = next(
        (i for i, row in enumerate(rows) if _block(row)[0] == TABLE_HEADER), None
    )
    if start is None:
        return []
    points: list[DayPoint] = []
    for row in rows[start + 1 :]:
        day, plan, fact = _block(row)[:3]
        serial, planned = _number(day), _number(plan)
        if serial is None or planned is None:
            break
        points.append(
            DayPoint(SERIAL_ZERO + timedelta(days=int(serial)), planned, _number(fact))
        )
    return points


def _balance(frame: pd.DataFrame, through: date, column: str = "amount_uzs") -> float:
    """Остаток на вечер дня: доход плюс, остальное с обратным знаком колонки D."""
    flow = frame[column].where(frame["group"] == INCOME_GROUP, -frame[column])
    return float(flow[frame["day"] <= through].sum())


def _spent_on(frame: pd.DataFrame, day: date, items: tuple[ForecastItem, ...]) -> float:
    """Траты дня без платежей из списка: квартплата не съедает лимит своего дня."""
    spent = expenses(frame)
    amounts = list(spent.loc[spent["day"] == day, "amount_uzs"])
    for item in items:
        due = -item.uzs
        if due <= 0:
            continue
        match = next((a for a in amounts if abs(a - due) <= PAYMENT_MATCH * due), None)
        if match is not None:
            amounts.remove(match)
    return float(sum(amounts))


def day_budget(
    frame: pd.DataFrame,
    rows: list[list[Any]],
    today: date,
    rates: Rates,
    frozen_currency: str | None,
) -> DayBudget | None:
    """Лимит на сегодня по листу fact и вкладке месяца; None — во вкладке нет списка."""
    forecast = read_forecast(rows)
    if forecast is None:
        return None
    items = forecast.items
    days = calendar.monthrange(today.year, today.month)[1]
    first = today.replace(day=1)
    yesterday = today - timedelta(days=1)

    opening = _balance(frame, first - timedelta(days=1))
    spend, _ = plan_lines(rows)
    payments = sum(item.uzs for item in items if item.uzs < 0)
    per_day = (sum(line.plan for line in spend) + payments) / days
    plan_end = opening + sum(item.uzs for item in items) - per_day * days
    target = max(plan_end, opening + forecast.savings_goal)

    def limit_on(day: date) -> float:
        ahead = sum(item.uzs for item in items if item.day >= day.day)
        left = _balance(frame, day - timedelta(days=1)) + ahead - target
        return left / (days - day.day + 1)

    passed = yesterday.day if yesterday >= first else 0
    planned = (
        opening
        + sum(item.uzs for item in items if item.day <= passed)
        - per_day * passed
    )

    frozen, change, frozen_uzs = None, 0.0, 0.0
    rate = rates.to_uzs.get(frozen_currency) if frozen_currency else None
    if rate:
        own = frame[frame["currency"] == frozen_currency]

        def reserved(from_day: int) -> float:
            """Платежи в валюте заморозки, которые ещё впереди: они не отложены."""
            return -sum(
                item.amount
                for item in items
                if item.currency == frozen_currency
                and item.amount < 0
                and item.day >= from_day
            )

        frozen = _balance(own, yesterday, "amount") - reserved(today.day)
        start = _balance(own, first - timedelta(days=1), "amount") - reserved(1)
        change, frozen_uzs = frozen - start, frozen * rate

    over = sorted(
        (line for line in spend if line.plan > 0 and line.fact > line.plan),
        key=lambda line: line.fact - line.plan,
        reverse=True,
    )
    return DayBudget(
        today=today,
        limit=limit_on(today),
        plan_per_day=per_day,
        days_left=days - today.day + 1,
        balance=_balance(frame, yesterday),
        planned_balance=planned,
        yesterday_spent=_spent_on(frame, yesterday, items),
        yesterday_limit=limit_on(yesterday) if passed else None,
        spent_today=_spent_on(frame, today, items),
        growth=plan_end - opening,
        goal=forecast.savings_goal,
        frozen=frozen,
        frozen_change=change,
        frozen_uzs=frozen_uzs,
        frozen_currency=frozen_currency,
        upcoming=tuple(
            sorted((i for i in items if i.day >= today.day), key=lambda i: i.day)[:2]
        ),
        over_plan=tuple(over[:3]),
    )


def short(value: float) -> str:
    """«8,52 млн» от миллиона, иначе «650 000»."""
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f} млн".replace(".", ",")
    return money(value)


def _pair(fact: float, plan: float) -> str:
    """«4,15 из 0,56 млн» или «690 910 из 500 000»."""
    if max(abs(fact), abs(plan)) >= 1_000_000:
        return f"{fact / 1_000_000:.2f} из {plan / 1_000_000:.2f} млн".replace(".", ",")
    return f"{money(fact)} из {money(plan)}"


def _cents(value: float) -> str:
    """«1 824,85»."""
    return f"{value:,.2f}".replace(",", " ").replace(".", ",")


def _item_amount(item: ForecastItem) -> str:
    """«+2,72 млн», «−650 USD»."""
    sign = "+" if item.amount > 0 else "−"
    if item.currency == "UZS":
        return f"{sign}{short(abs(item.uzs))}"
    return f"{sign}{money(abs(item.amount))} {item.currency}"


def format_day_budget(budget: DayBudget, now: bool = False) -> str:
    """Утреннее сообщение; now=True — для /today: ещё потрачено и осталось сегодня."""
    b = budget
    weekday = WEEKDAYS[b.today.weekday()].capitalize()
    lines = [f"☀️ {weekday}, {b.today.day} {MONTHS_GENITIVE[b.today.month - 1]}"]
    if b.limit > 0:
        lines.append(f"💸 Можно потратить сегодня: {money(b.limit)} сум")
    else:
        lines.append(
            "💸 Лимита на сегодня нет: до плана на конец месяца не хватает "
            f"{short(-b.limit * b.days_left)}"
        )
    if now:
        left = max(b.limit, 0) - b.spent_today
        rest = (
            f"осталось {money(left)}" if left >= 0 else f"сверх лимита {money(-left)}"
        )
        lines.append(f"   потрачено {money(b.spent_today)} — {rest}")
    lines.append(f"   по плану месяца — {money(b.plan_per_day)} в день")

    if b.yesterday_limit is None:
        lines.append(f"Вчера: {money(b.yesterday_spent)}")
    else:
        allowed = max(b.yesterday_limit, 0)
        over = b.yesterday_spent - allowed
        verdict = (
            f"больше на {money(over)}"
            if over > 0
            else f"уложились, запас {money(-over)}"
        )
        lines.append(
            f"Вчера: {money(b.yesterday_spent)} при лимите {money(allowed)} — {verdict}"
        )
        gap = b.balance - b.planned_balance
        where = f"на картах {short(b.balance)}, по плану {short(b.planned_balance)}"
        lines.append(
            f"⚠️ Отстаём от плана на {short(-gap)}: {where}"
            if gap < 0
            else f"✅ Идём лучше плана на {short(gap)}: {where}"
        )

    if b.frozen is not None:
        line = f"🧊 Заморожено: {_cents(b.frozen)} {b.frozen_currency} ≈ {short(b.frozen_uzs)}"
        if abs(b.frozen_change) >= 0.01:
            sign = "+" if b.frozen_change > 0 else "−"
            line += f" (с 1-го числа {sign}{_cents(abs(b.frozen_change))} {b.frozen_currency})"
        lines.append(line)
    if b.goal > 0:
        if b.goal > b.growth:
            cut = (b.goal - b.growth) / b.days_left
            lines.append(
                f"🎯 Цель отложить {short(b.goal)} больше плановой экономии "
                f"{short(b.growth)} — лимит ниже на {money(cut)} в день"
            )
        else:
            lines.append(f"🎯 Цель отложить {short(b.goal)}: план её покрывает")
    if b.over_plan:
        lines.append(
            "🔴 Сверх плана: "
            + " • ".join(
                f"{line.subcategory} {_pair(line.fact, line.plan)}"
                for line in b.over_plan
            )
        )
    if b.upcoming:
        lines.append(
            "📅 Впереди: "
            + " • ".join(
                f"{item.day:02d}.{b.today.month:02d} {item.name} {_item_amount(item)}"
                for item in b.upcoming
            )
        )
    return "\n".join(lines)


def plan_status(
    line: PlanLine | None, subcategory: str, icons: dict[str, str] | None, today: date
) -> str:
    """Строка о статье после записи: 🟢 в плане, 🟡 от 80 %, 🔴 сверх, ⚪ вне плана."""
    icon = (icons or {}).get(subcategory)
    name = f"{icon} {subcategory}" if icon else subcategory
    if line is None:
        return f"⚪ {name}: нет в плане месяца"
    if line.plan <= 0:
        month = MONTHS_PREPOSITIONAL[today.month - 1]
        return f"⚪ {name}: вне плана, в {month} уже {short(line.fact)}"
    if line.fact > line.plan:
        return (
            f"🔴 {name}: {_pair(line.fact, line.plan)} — "
            f"сверх плана на {short(line.fact - line.plan)}"
        )
    share = f"{line.fact / line.plan:.0%}"
    if line.fact >= NEAR_PLAN * line.plan:
        return (
            f"🟡 {name}: {_pair(line.fact, line.plan)} ({share}) — "
            f"осталось {short(line.plan - line.fact)}"
        )
    return f"🟢 {name}: {_pair(line.fact, line.plan)} ({share})"


def today_line(budget: DayBudget) -> str:
    """Сколько ещё можно сегодня — после каждой записи."""
    if budget.limit <= 0:
        return (
            "💸 Лимита на сегодня нет: до плана на конец месяца не хватает "
            f"{short(-budget.limit * budget.days_left)}"
        )
    left = budget.limit - budget.spent_today
    if left < 0:
        return (
            f"💸 Сегодня сверх лимита на {money(-left)} (лимит {money(budget.limit)})"
        )
    return f"💸 На сегодня осталось {money(left)} из {money(budget.limit)}"


def celebrate(budget: DayBudget) -> bool:
    """Вчера (в этом месяце) потратили не больше лимита — утром 🎉."""
    if budget.yesterday_limit is None:
        return False
    return budget.yesterday_spent <= max(budget.yesterday_limit, 0)
