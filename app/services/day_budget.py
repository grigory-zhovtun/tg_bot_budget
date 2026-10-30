"""Лимит на день — по деньгам до зарплаты, план — одной строкой.

Лимит: сумы на вечер вчера (все строки fact в UZS — карты и наличные) плюс авансы
срока минус платежи в сумах по день основной зарплаты, на дни по него включительно:
зарплату могут начислить и вечером, её день живём на то, что есть. Основная
зарплата — открытая к утру строка жёлтого списка (колонки O:S вкладки месяца) с
плюсом в UZS и словом «ЗП» или «зарплата» (без «аванс») в названии, позже сегодня;
после последней в месяце — первая такая строка в следующем месяце; таких строк нет —
срок задаёт аванс, как раньше; нет и его — до конца месяца. Авансы срока — открытые
строки с «аванс» позже сегодня (непришедшая часть) и они же в следующем месяце
(целиком). Страховка: деньги аванса до его дня не тратим — если до аванса денег
меньше, чем на ровный лимит, лимит считается по день аванса без него. Платежи —
открытые строки с минусом в UZS и подписки в сумах вне списка, ещё не списанные в
своём месяце. План — траты дня с 1-го по вчера против плана месяца в день × дни, как
строка у графика «Траты». Ячейка лимита во вкладке месяца считает план до конца
месяца — это другое число.

Заморожено — карты в валюте заморозки (долларовая) без платежей в этой валюте,
которые ещё впереди. Лимит эти деньги не тратит: он считает только сумы, а доллары
входят в него, когда их обменяли на сумы.
"""

import calendar
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, timedelta
from typing import Any

import pandas as pd

from app.domain import INCOME_GROUP, Rates
from app.rich import NBSP, heading, in_currency, para, percent, signed, table, whole
from app.services.analytics_service import (
    MONTHS_RU,
    SERIAL_ZERO,
    PlanLine,
    expenses,
    money,
    plan_lines,
)
from app.services.calendar import CalendarRecord
from app.services.forecast import currency_forecast
from app.services.receipts import NOTHING, Receipt, closed, match_receipts, remaining
from app.services.runway import Runway, uzs_runway

COLUMN_O = 14  # блок прогноза: O название, P день, Q сумма, R валюта, S в сумах
LIST_HEADER = "Поступления"  # «Поступления (+) и крупные платежи (−)»
TABLE_HEADER = "Дата"  # таблица по дням под списком
GOAL_LABEL = "Отложить за месяц"
WEEKDAYS = ("понедельник", "вторник", "среда", "четверг", "пятница", "суббота",
            "воскресенье")  # fmt: skip
MONTHS_PREPOSITIONAL = ("январе", "феврале", "марте", "апреле", "мае", "июне", "июле",
                        "августе", "сентябре", "октябре", "ноябре", "декабре")  # fmt: skip
NEAR_PLAN = 0.8  # с этой доли плана статья жёлтая
# строка списка — основная зарплата, до которой лимит, и аванс, который прибавляется к
# деньгам срока: возврат долга и прочие поступления срок не задают, их деньги попадут
# в сумы, когда придут
MAIN_SALARY = re.compile(r"зарплат|(?<![а-яё])зп(?![а-яё])", re.IGNORECASE)
ADVANCE = re.compile(r"аванс", re.IGNORECASE)
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
    row: int = 0  # строка вкладки; 0 — пункт не из вкладки


@dataclass(frozen=True)
class Forecast:
    items: tuple[ForecastItem, ...]
    savings_goal: float = 0.0  # в сумах, 0 — цели нет


@dataclass(frozen=True)
class DaySpend:
    """День месяца: траты без платежей из списка и лимит, с которым день начался."""

    day: date
    spent: float
    limit: float


@dataclass(frozen=True)
class DayBudget:
    today: date
    limit: (
        float  # в день до зарплаты: (сумы − платежи) / дней; меньше нуля — не хватает
    )
    plan_per_day: float
    days_left: int  # дней лимита с сегодняшним: по день зарплаты или конец месяца
    balance: float  # на всех картах на вечер вчера, в сумах
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
    runway: Runway | None = None  # прогноз по сумам: когда кончатся, сколько обменять
    history: tuple[DaySpend, ...] = ()  # с 1-го числа по сегодняшний день
    ahead: tuple[ForecastItem, ...] = ()  # не пришедшие строки списка: что впереди
    sums: float = 0.0  # сумы на вечер вчера: строки fact в UZS — карты и наличные
    bills: float = 0.0  # платежи в сумах по день зарплаты включительно
    subscriptions: float = 0.0  # из них подписки: формула вкладки их не найдёт
    advances: float = 0.0  # авансы срока: прибавлены к деньгам
    advance_on: date | None = None  # первый из них; None — авансов в сроке нет
    salary_subscriptions: float = 0.0  # подписки по день основной зарплаты — Q5 вкладки
    advance_subscriptions: float | None = None  # по день аванса — R5; None — его нет
    payday: date | None = (
        None  # зарплата в сумах, до которой лимит; None — конец месяца
    )
    payday_name: str = ""  # её строка списка: «Аванс», «ЗП»
    plan_gap: float | None = None  # траты дня с 1-го по вчера минус план; None — 1-е


@dataclass(frozen=True)
class Allowance:
    """Лимит дня по деньгам: сумы минус платежи до зарплаты на дни до неё."""

    limit: float
    sums: float
    bills: float
    subscriptions: float  # часть bills: подписки вне списка
    payday: date | None
    payday_name: str
    days: int
    advances: float = 0.0  # авансы срока: прибавлены к деньгам
    advance_on: date | None = None  # первый из них; None — авансов в сроке нет
    salary_subscriptions: float = 0.0  # подписки по день основной зарплаты — Q5 вкладки
    advance_subscriptions: float | None = None  # по день аванса — R5; None — его нет


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
    for index in range(start + 1, len(rows)):
        name, day, amount, currency, uzs = _block(rows[index])
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
            items.append(
                ForecastItem(name.strip(), int(day), amount, code, uzs, index + 1)
            )
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


def _on(first: date, day: int) -> date:
    """День месяца first; 31-е в коротком месяце — последний день."""
    return first.replace(day=min(day, calendar.monthrange(first.year, first.month)[1]))


def _next_month(first: date) -> date:
    return (first + timedelta(days=32)).replace(day=1)


def _sums(frame: pd.DataFrame, through: date) -> float:
    """Сумы на вечер дня: все строки fact в UZS — карты и наличные."""
    return _balance(frame[frame["currency"] == "UZS"], through, "amount")


def _salaries(
    items: tuple[ForecastItem, ...],
) -> tuple[list[ForecastItem], list[ForecastItem]]:
    """Строки основной зарплаты и аванса в сумах по дням; основной нет — срок задают
    строки аванса, а прибавлять к деньгам нечего."""

    def ordered(found: Callable[[ForecastItem], bool]) -> list[ForecastItem]:
        sums = (item for item in items if item.amount > 0 and item.currency == "UZS")
        return sorted(
            (item for item in sums if found(item)),
            key=lambda item: (item.day, item.row),
        )

    advances = ordered(lambda item: bool(ADVANCE.search(item.name)))
    mains = ordered(
        lambda item: bool(MAIN_SALARY.search(item.name))
        and not ADVANCE.search(item.name)
    )
    return (mains, advances) if mains else (advances, [])


def _payday(
    salaries: Sequence[ForecastItem], receipts: Mapping[int, Receipt], day: date
) -> tuple[date | None, str]:
    """Ближайшая зарплата в сумах позже day: открытая к утру строка salaries; в месяце
    нет — первая из них в следующем месяце; строк нет — (None, ""). Зарплата сегодня
    или опоздавшая — не в счёт, пока деньги не на картах."""
    if not salaries:
        return None, ""
    first = day.replace(day=1)
    for item in salaries:
        due = _on(first, item.day)
        if due > day and not closed(item, receipts.get(item.row, NOTHING)):
            return due, item.name
    return _on(_next_month(first), salaries[0].day), salaries[0].name


def _advances(
    advances: Sequence[ForecastItem],
    receipts: Mapping[int, Receipt],
    day: date,
    until: date,
) -> tuple[float, date | None, str]:
    """Авансы срока: строки аванса этого месяца с днём позже day и раньше until — их
    непришедшая часть, и они же в следующем месяце — целиком; и первый из них. Аванс
    сегодня или опоздавший — не в счёт, пока деньги не на картах, как зарплата."""
    first = day.replace(day=1)
    later = _next_month(first)
    total, found = 0.0, []
    for item in advances:
        due = _on(first, item.day)
        rest = remaining(item, receipts.get(item.row, NOTHING))
        if day < due < until and rest > 0:
            total += rest
            found.append((due, item.name))
        due = _on(later, item.day)
        if due < until:
            total += item.amount
            found.append((due, item.name))
    if not found:
        return 0.0, None, ""
    on, name = min(found)
    return total, on, name


def _list_bills(
    items: tuple[ForecastItem, ...],
    receipts: Mapping[int, Receipt],
    day: date,
    until: date,
) -> float:
    """Платежи списка в сумах с day по день перед until: открытые строки с минусом в
    UZS (и опоздавшие), в части срока в следующем месяце — эти строки целиком."""
    first = day.replace(day=1)
    later = _next_month(first)
    total = 0.0
    for item in items:
        if item.amount >= 0 or item.currency != "UZS":
            continue
        receipt = receipts.get(item.row, NOTHING)
        if _on(first, item.day) < until and not closed(item, receipt):
            total -= remaining(item, receipt)
        if until > later and _on(later, item.day) < until:
            total -= item.amount
    return total


def _subscriptions(series: Sequence[Any], day: date, until: date) -> float:
    """Подписки в UZS с day по день перед until: не отменённые и ещё не списанные в
    своём месяце до day."""
    first = day.replace(day=1)
    later = _next_month(first)
    total = 0.0
    for one in series:
        if one.last.currency != "UZS" or one.lapsed(day):
            continue
        for month in (first, later):
            due = one.expected_on(month)
            charged = any(
                (c.day.year, c.day.month) == (due.year, due.month) and c.day < day
                for c in one.charges
            )
            if day <= due < until and not charged:
                total += one.last.amount
    return total


def _spent_on(frame: pd.DataFrame, day: date, paid: frozenset[int]) -> float:
    """Траты дня без платежей из списка: квартплата не съедает лимит своего дня."""
    spent = expenses(frame)
    spent = spent[(spent["day"] == day) & ~spent.index.isin(list(paid))]
    return float(spent["amount_uzs"].sum())


def _ahead(
    items: tuple[ForecastItem, ...], receipts: Mapping[int, Receipt]
) -> tuple[ForecastItem, ...]:
    """Не закрытые строки списка: сумма — что ещё впереди, по дню."""
    left = []
    for item in items:
        rest = remaining(item, receipts.get(item.row, NOTHING))
        if rest:
            left.append(replace(item, amount=rest, uzs=item.uzs * rest / item.amount))
    return tuple(sorted(left, key=lambda item: item.day))


def day_budget(
    frame: pd.DataFrame,
    rows: list[list[Any]],
    today: date,
    rates: Rates,
    frozen_currency: str | None,
    records: Sequence[CalendarRecord] = (),
) -> DayBudget | None:
    """Лимит на сегодня по листу fact и вкладке месяца; None — во вкладке нет списка.

    Лимит — по деньгам до зарплаты (Allowance): сумы на вечер вчера минус платежи по
    день ближайшей зарплаты в сумах, на дни по него включительно. Строки списка — по
    сверке с fact (services.receipts): к утру дня — для лимита и заморозки; на сейчас —
    для «Впереди» и трат дня. Прогноз по сумам — линия «по темпу» модели прогноза
    (services.forecast) с записями календаря records, как во вкладке «Календарь».
    """
    # recurring сам берёт отсюда MONTHS_PREPOSITIONAL и short
    from app.services.recurring import find_series, outside_list

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
    series = outside_list(find_series(frame, today), items)
    month_end = first.replace(day=days)
    salaries, spare = _salaries(items)

    def allowance_on(day: date) -> Allowance:
        """Лимит дня day: сумы на вечер накануне, авансы, платежи и дни до зарплаты."""
        receipts = match_receipts(frame, items, first, day - timedelta(days=1))
        payday, name = _payday(salaries, receipts, day)
        # день зарплаты — тоже: её могут начислить и вечером
        until = (payday or month_end) + timedelta(days=1)
        sums = _sums(frame, day - timedelta(days=1))
        advances, advance_on, advance_name = _advances(spare, receipts, day, until)
        subscriptions = _subscriptions(series, day, until)
        bills = _list_bills(items, receipts, day, until) + subscriptions
        spread = (until - day).days
        allowance = Allowance(
            (sums + advances - bills) / spread,
            sums,
            bills,
            subscriptions,
            payday,
            name,
            spread,
            advances,
            advance_on,
            subscriptions,
        )
        if advance_on is None:
            return allowance
        # страховка: деньги аванса до его дня не тратим, его день живём на то, что есть
        guard_until = advance_on + timedelta(days=1)
        guard_subscriptions = _subscriptions(series, day, guard_until)
        guard_bills = (
            _list_bills(items, receipts, day, guard_until) + guard_subscriptions
        )
        guard_days = (guard_until - day).days
        guard = (sums - guard_bills) / guard_days
        if guard >= allowance.limit:
            return replace(allowance, advance_subscriptions=guard_subscriptions)
        return Allowance(
            guard,
            sums,
            guard_bills,
            guard_subscriptions,
            advance_on,
            advance_name,
            guard_days,
            salary_subscriptions=subscriptions,
            advance_subscriptions=guard_subscriptions,
        )

    passed = yesterday.day if yesterday >= first else 0
    now_allowance = allowance_on(today)
    morning = _ahead(items, match_receipts(frame, items, first, yesterday))
    now = match_receipts(frame, items, first, today)
    paid = frozenset(index for receipt in now.values() for index in receipt.operations)

    frozen, change, frozen_uzs = None, 0.0, 0.0
    rate = rates.to_uzs.get(frozen_currency) if frozen_currency else None
    if rate:
        own = frame[frame["currency"] == frozen_currency]

        def reserved(lines: tuple[ForecastItem, ...]) -> float:
            """Платежи в валюте заморозки, которые ещё впереди: они не отложены."""
            return -sum(
                item.amount
                for item in lines
                if item.currency == frozen_currency and item.amount < 0
            )

        frozen = _balance(own, yesterday, "amount") - reserved(morning)
        start = _balance(own, first - timedelta(days=1), "amount") - reserved(items)
        change, frozen_uzs = frozen - start, frozen * rate

    over = sorted(
        (line for line in spend if line.plan > 0 and line.fact > line.plan),
        key=lambda line: line.fact - line.plan,
        reverse=True,
    )
    left_now = _ahead(items, now)
    history = tuple(
        DaySpend(day, _spent_on(frame, day, paid), allowance_on(day).limit)
        for day in (first + timedelta(days=k) for k in range(today.day))
    )
    done = sum(day.spent for day in history if day.day < today)
    return DayBudget(
        today=today,
        limit=now_allowance.limit,
        plan_per_day=per_day,
        days_left=now_allowance.days,
        balance=_balance(frame, yesterday),
        yesterday_spent=_spent_on(frame, yesterday, paid),
        yesterday_limit=history[-2].limit if passed else None,
        spent_today=_spent_on(frame, today, paid),
        growth=plan_end - opening,
        goal=forecast.savings_goal,
        frozen=frozen,
        frozen_change=change,
        frozen_uzs=frozen_uzs,
        frozen_currency=frozen_currency,
        upcoming=left_now[:2],
        over_plan=tuple(over[:3]),
        runway=uzs_runway(
            currency_forecast(frame, (), items, today, "UZS", records=records),
            today,
            rates,
        ),
        history=history,
        ahead=left_now,
        sums=now_allowance.sums,
        bills=now_allowance.bills,
        subscriptions=now_allowance.subscriptions,
        advances=now_allowance.advances,
        advance_on=now_allowance.advance_on,
        salary_subscriptions=now_allowance.salary_subscriptions,
        advance_subscriptions=now_allowance.advance_subscriptions,
        payday=now_allowance.payday,
        payday_name=now_allowance.payday_name,
        plan_gap=done - per_day * passed if passed else None,
    )


def short(value: float) -> str:
    """«1,25 млн» от миллиона, иначе «450 000»."""
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f} млн".replace(".", ",")
    return money(value)


def _pair(fact: float, plan: float) -> str:
    """«1,25 из 3,00 млн» или «720 000 из 500 000»."""
    if max(abs(fact), abs(plan)) >= 1_000_000:
        return f"{fact / 1_000_000:.2f} из {plan / 1_000_000:.2f} млн".replace(".", ",")
    return f"{money(fact)} из {money(plan)}"


def _cents(value: float) -> str:
    """«1 234,56»."""
    return f"{value:,.2f}".replace(",", " ").replace(".", ",")


def _item_amount(item: ForecastItem) -> str:
    """«+2,00 млн», «−400 USD»."""
    sign = "+" if item.amount > 0 else "−"
    if item.currency == "UZS":
        return f"{sign}{short(abs(item.uzs))}"
    return f"{sign}{money(abs(item.amount))} {item.currency}"


def _ahead_text(item: ForecastItem, today: date) -> str:
    """«15.10 Аванс +2,00 млн»; опоздавшая строка — «⏳ 30.10 ЗП +10,00 млн»."""
    late = "⏳ " if item.day < today.day else ""
    return f"{late}{item.day:02d}.{today.month:02d} {item.name} {_item_amount(item)}"


def days_text(count: int) -> str:
    """«1 день», «2 дня», «5 дней», «21 день»."""
    if count % 10 == 1 and count % 100 != 11:
        return f"{count} день"
    if 2 <= count % 10 <= 4 and not 12 <= count % 100 <= 14:
        return f"{count} дня"
    return f"{count} дней"


def payday_on(budget: DayBudget) -> str:
    """«15-го» — в этом месяце, «15 ноября» — в следующем."""
    day = budget.payday
    if day is None:
        return "конца месяца"
    if day.month == budget.today.month:
        return f"{day.day}-го"
    return f"{day.day} {MONTHS_GENITIVE[day.month - 1]}"


def until_text(budget: DayBudget) -> str:
    """«до аванса 15-го», «до зарплаты 15 ноября», «до конца месяца»."""
    if budget.payday is None:
        return "до конца месяца"
    word = "аванса" if "аванс" in budget.payday_name.lower() else "зарплаты"
    return f"до {word} {payday_on(budget)}"


def _money_line(budget: DayBudget, amount: Callable[[float], str] = whole) -> str:
    """«на сумовых картах 7 200 000, платежи до 30-го 80 000»."""
    line = f"на сумовых картах {amount(budget.sums)}"
    if round(budget.bills) > 0:
        line += f", платежи до {payday_on(budget)} {amount(budget.bills)}"
    return line


def _plan_text(budget: DayBudget, amount: Callable[[float], str] = whole) -> str | None:
    """Траты дня против плана — как строка у графика «Траты»; 1-го числа — None."""
    if budget.plan_gap is None:
        return None
    period = f"за {days_text(budget.today.day - 1)}"
    gap = round(budget.plan_gap)
    if gap > 0:
        return f"⚠️ Сверх плана на {amount(gap)} {period}"
    if gap < 0:
        return f"✓ Запас {amount(-gap)} {period}"
    return f"✓ Ровно по плану {period}"


def runway_line(runway: Runway, today: date) -> str:
    """«💳 Сумы 2,00 млн — хватит до 18.10 · до 30.10 обменять ≈ $460»."""
    head = f"💳 Сумы {short(runway.balance)}"
    if runway.short <= 0:
        return f"{head} — до конца месяца хватит"
    if runway.last_day is None or runway.last_day < today:
        when = "уже закончились"
    elif runway.last_day == today:
        when = "хватит только на сегодня"
    else:
        when = f"хватит до {runway.last_day:%d.%m}"
    target = f"до {runway.until:%d.%m}" if runway.until else "до конца месяца"
    need = (
        f"обменять ≈ ${money(runway.dollars)}"
        if runway.dollars is not None
        else f"не хватит {short(runway.short)}"
    )
    return f"{head} — {when} · {target} {need}"


def format_day_budget(budget: DayBudget, now: bool = False) -> str:
    """Утреннее сообщение; now=True — для /today: ещё потрачено и осталось сегодня."""
    b = budget
    weekday = WEEKDAYS[b.today.weekday()].capitalize()
    lines = [f"☀️ {weekday}, {b.today.day} {MONTHS_GENITIVE[b.today.month - 1]}"]
    until = until_text(b)
    if b.limit > 0:
        lines.append(f"💸 Можно тратить в день {until}: {money(b.limit)} сум")
    else:
        lines.append(
            f"💸 Лимита на сегодня нет: {until} не хватает "
            f"{money(-b.limit * b.days_left)}"
        )
    if now:
        left = max(b.limit, 0) - b.spent_today
        rest = (
            f"осталось {money(left)}" if left >= 0 else f"сверх лимита {money(-left)}"
        )
        lines.append(f"   потрачено {money(b.spent_today)} — {rest}")
    lines.append(f"   {_money_line(b, money)}")
    plan = _plan_text(b, money)
    if plan is not None:
        lines.append(plan)

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

    if b.frozen is not None:
        line = f"🧊 Заморожено: {_cents(b.frozen)} {b.frozen_currency} ≈ {short(b.frozen_uzs)}"
        if abs(b.frozen_change) >= 0.01:
            sign = "+" if b.frozen_change > 0 else "−"
            line += f" (с 1-го числа {sign}{_cents(abs(b.frozen_change))} {b.frozen_currency})"
        lines.append(line)
    if b.runway is not None:
        lines.append(runway_line(b.runway, b.today))
    if b.goal > 0:
        if b.goal > b.growth:
            lines.append(
                f"🎯 Цель отложить {short(b.goal)} больше плановой экономии "
                f"{short(b.growth)}"
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
            + " • ".join(_ahead_text(item, b.today) for item in b.upcoming)
        )
    return "\n".join(lines)


def _rich_runway(runway: Runway, today: date) -> list[str]:
    """«💳 Сумы …»: хватит ли сумов и сколько долларов обменять."""
    head = heading(f"💳 Сумы {whole(runway.balance)}")
    if runway.short <= 0:
        return [head, para("до конца месяца хватит")]
    if runway.last_day is None or runway.last_day < today:
        when = "уже закончились"
    elif runway.last_day == today:
        when = "хватит только на сегодня"
    else:
        when = f"хватит до {runway.last_day:%d.%m}"
    target = f"до {runway.until:%d.%m}" if runway.until else "до конца месяца"
    need = (
        f"обменять ≈{NBSP}{in_currency(runway.dollars, 'USD')}"
        if runway.dollars is not None
        else f"не хватит {whole(runway.short)}"
    )
    return [head, para(when, f"{target} {need}")]


def rich_day_budget(budget: DayBudget, now: bool = False) -> list[str]:
    """Блоки расширенного сообщения: утро (now=False) или /today — как на макете."""
    b = budget
    weekday = WEEKDAYS[b.today.weekday()].capitalize()
    month = MONTHS_GENITIVE[b.today.month - 1]
    blocks = [para(f"☀️ {weekday}, {b.today.day} {month}")]
    until = until_text(b)
    limit = f"лимит {whole(b.limit)} в день {until}"
    spent = f"потрачено {whole(b.spent_today)}"
    if b.limit <= 0:
        lines = [f"{until} не хватает {whole(-b.limit * b.days_left)}"]
        if now:
            lines.append(f"потрачено сегодня {whole(b.spent_today)}")
        blocks += [heading("💸 Лимита на сегодня нет", 2), para(*lines, _money_line(b))]
    elif not now:
        blocks += [
            heading(f"💸 {whole(b.limit)} сум", 1),
            para(f"можно тратить в день {until}", _money_line(b)),
        ]
    elif b.spent_today <= b.limit:
        blocks += [
            heading(f"💸 {whole(b.limit - b.spent_today)} сум", 1),
            para("осталось на сегодня", limit, spent),
        ]
    else:
        blocks += [
            heading(f"⚠️ Сверх лимита на {whole(b.spent_today - b.limit)}", 2),
            para(limit, spent),
        ]
    plan = _plan_text(b)
    if plan is not None:
        blocks.append(heading(plan))

    if b.yesterday_limit is None:
        blocks.append(heading(f"Вчера {whole(b.yesterday_spent)}"))
    else:
        allowed = max(b.yesterday_limit, 0)
        over = b.yesterday_spent - allowed
        pair = f"{whole(b.yesterday_spent)} из{NBSP}{whole(allowed)}"  # «из» — с числом
        blocks += (
            [heading(f"⚠️ Вчера {pair}"), para(f"больше на {whole(over)}")]
            if over > 0
            else [heading(f"✅ Вчера {pair}"), para(f"уложились, запас {whole(-over)}")]
        )

    if b.runway is not None:
        blocks += _rich_runway(b.runway, b.today)
    if b.frozen is not None:
        code = str(b.frozen_currency)
        lines = [f"≈{NBSP}{whole(b.frozen_uzs)} сум"]
        if abs(b.frozen_change) >= 0.01:
            sign = "+" if b.frozen_change > 0 else "−"
            change = in_currency(abs(b.frozen_change), code, with_cents=True)
            lines.append(f"с 1-го числа {sign}{change}")
        frozen = in_currency(b.frozen, code, with_cents=True)
        blocks += [heading(f"🧊 Заморожено {frozen}"), para(*lines)]
    if b.goal > 0:
        if b.goal > b.growth:
            reason: tuple[str, ...] = (f"больше плановой экономии {whole(b.growth)}",)
        else:
            reason = ("план её покрывает",)
        blocks += [heading(f"🎯 Цель отложить {whole(b.goal)}"), para(*reason)]
    if b.over_plan:
        rows = [
            [line.subcategory, whole(line.fact), whole(line.plan)]
            for line in b.over_plan
        ]
        blocks += [
            heading("🔴 Сверх плана"),
            table(rows, "lrr", ["статья", "факт", "план"]),
        ]
    if b.upcoming:
        # строками: таблица с датой и названием не влезает в ширину телефона
        lines = [
            f"{item.day:02d}.{b.today.month:02d} {item.name}"
            f"{' ⏳' if item.day < b.today.day else ''}: "
            f"{signed(item.amount, item.currency)}"
            for item in b.upcoming
        ]
        blocks += [heading("📅 Впереди"), para(*lines)]
    return blocks


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


def rich_plan_status(line: PlanLine | None, subcategory: str, today: date) -> list[str]:
    """Статья после записи блоками: «🟡 Кофе 1 287 000» и «из 1 500 000 — 86 %…»."""
    name = subcategory[:1].upper() + subcategory[1:]
    if line is None:
        return [heading(f"⚪ {name}"), para("нет в плане месяца")]
    if line.plan <= 0:
        month = MONTHS_RU[today.month - 1]
        return [
            heading(f"⚪ {name} {whole(line.fact)}"),
            para(f"вне плана — всё за {month}"),
        ]
    head = f"{name} {whole(line.fact)}"
    of = f"из {whole(line.plan)}"
    if line.fact > line.plan:
        over = whole(line.fact - line.plan)
        return [heading(f"🔴 {head}"), para(f"{of} — сверх на {over}")]
    share = percent(line.fact / line.plan)
    if line.fact >= NEAR_PLAN * line.plan:
        left = whole(line.plan - line.fact)
        return [heading(f"🟡 {head}"), para(f"{of} — {share}, осталось {left}")]
    return [heading(f"🟢 {head}"), para(f"{of} — {share}")]


def rich_today_line(budget: DayBudget) -> list[str]:
    """Сколько ещё можно сегодня — после записи, блоками."""
    until = until_text(budget)
    if budget.limit <= 0:
        short_by = whole(-budget.limit * budget.days_left)
        return [
            heading("💸 Лимита на сегодня нет"),
            para(f"{until} не хватает {short_by}"),
        ]
    left = budget.limit - budget.spent_today
    if left < 0:
        return [
            heading(f"⚠️ Сверх лимита на {whole(-left)}"),
            para(f"лимит {whole(budget.limit)} в день {until}"),
        ]
    return [
        heading(f"💸 Осталось {whole(left)}"),
        para(f"на сегодня, лимит {whole(budget.limit)} в день {until}"),
    ]


def today_line(budget: DayBudget) -> str:
    """Сколько ещё можно сегодня — после каждой записи."""
    until = until_text(budget)
    if budget.limit <= 0:
        return (
            f"💸 Лимита на сегодня нет: {until} не хватает "
            f"{money(-budget.limit * budget.days_left)}"
        )
    left = budget.limit - budget.spent_today
    if left < 0:
        return (
            f"💸 Сегодня сверх лимита на {money(-left)} "
            f"(лимит {money(budget.limit)} в день {until})"
        )
    return f"💸 На сегодня осталось {money(left)} из {money(budget.limit)}"


def celebrate(budget: DayBudget) -> bool:
    """Вчера (в этом месяце) потратили не больше лимита — утром 🎉."""
    if budget.yesterday_limit is None:
        return False
    return budget.yesterday_spent <= max(budget.yesterday_limit, 0)
