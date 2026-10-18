"""Подушка безопасности и норма сбережений: карточки «Сводки» и строка «Отложите»
в сводке записи поступления.

Подушка — «Заморожено» дневного бюджета (деньги в валюте заморозки минус её
ожидаемые платежи) в месяцах средних трат. Норма сбережений — доля дохода,
которая осталась за месяц: (доход − траты) / доход. Считаются только полные
месяцы: в текущем зарплата могла ещё не прийти, и доля была бы огромным минусом.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Protocol

import pandas as pd

from app import config
from app.domain import INCOME_GROUP, Rates, SheetRow
from app.services.analytics_service import OPENING_BALANCE, expenses, income

if TYPE_CHECKING:
    from app.services.day_budget import DayBudget

AVERAGE_MONTHS = 3  # средние траты для подушки — за столько полных месяцев
RATE_MONTHS = 3  # норма сбережений крупно — за столько последних полных месяцев
CHART_MONTHS = 6  # столбики нормы — по стольким полным месяцам
FULL_FROM_DAY = 5  # траты ведут не с начала месяца (позже 5-го) — он не полный
TINY_SHARE = 10_000.0  # сумов: доля поступления меньше — ни «🛟», ни «🐷»
RATES_AGE = 300.0  # секунд: курс для эквивалента в подсказке — из копии не старше


@dataclass(frozen=True)
class Cushion:
    """Подушка безопасности: «Заморожено» в месяцах средних трат."""

    amount: float  # «Заморожено» в валюте подушки
    currency: str
    uzs: float  # в сумах по курсу таблицы
    rate: float  # курс валюты подушки к суму; 0 — курса нет
    monthly: float  # средние траты в месяц, в сумах
    target_months: int

    @property
    def months(self) -> float | None:
        """На сколько месяцев средних трат хватит; None — трат ещё нет."""
        if self.monthly <= 0:
            return None
        return max(self.uzs, 0.0) / self.monthly

    @property
    def short_uzs(self) -> float:
        """Сколько не хватает до цели, в сумах; 0 — цель достигнута."""
        return max(self.target_months * self.monthly - max(self.uzs, 0.0), 0.0)

    @property
    def short(self) -> float:
        """То же в валюте подушки по курсу таблицы."""
        return self.short_uzs / self.rate if self.rate > 0 else 0.0


@dataclass(frozen=True)
class MonthSaving:
    """Полный месяц: доход без «нач остаток» и траты без переводов, в сумах."""

    month: date  # первое число
    income: float
    spent: float

    @property
    def rate(self) -> float | None:
        """Доля дохода, которая осталась; None — дохода не было."""
        if self.income <= 0:
            return None
        return (self.income - self.spent) / self.income


@dataclass(frozen=True)
class Savings:
    """Норма сбережений по полным месяцам, от старых к новым."""

    months: tuple[MonthSaving, ...]

    @property
    def rate(self) -> float | None:
        """Норма за RATE_MONTHS последних полных месяцев; None — дохода не было."""
        recent = self.months[-RATE_MONTHS:]
        earned = sum(item.income for item in recent)
        if earned <= 0:
            return None
        return (earned - sum(item.spent for item in recent)) / earned


def full_months(today: date, count: int) -> list[date]:
    """Первые числа count полных месяцев перед месяцем today, от старых к новым."""
    found = []
    year, month = today.year, today.month
    for _ in range(count):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        found.append(date(year, month, 1))
    return found[::-1]


def _by_month(rows: pd.DataFrame) -> pd.Series:
    """Суммы в сумах по первым числам месяцев."""
    if rows.empty:
        return pd.Series(dtype=float)
    months = rows["day"].map(lambda day: day.replace(day=1))
    return rows["amount_uzs"].groupby(months).sum()


def monthly_spending(
    frame: pd.DataFrame, today: date, count: int = AVERAGE_MONTHS
) -> float:
    """Средние траты в месяц за count полных месяцев, в сумах.

    Месяцы до первой траты не в счёт, а месяц, где траты начали вести позже
    FULL_FROM_DAY-го, — тоже: иначе учёт, начатый недавно, занижал бы траты
    и завышал подушку.
    """
    spending = expenses(frame)
    if spending.empty:
        return 0.0
    started = min(spending["day"])
    first = started.replace(day=1)
    if started.day > FULL_FROM_DAY:
        first = (first + timedelta(days=32)).replace(day=1)
    months = [month for month in full_months(today, count) if month >= first]
    if not months:
        return 0.0
    spent = _by_month(spending)
    return float(sum(spent.get(month, 0.0) for month in months) / len(months))


def month_savings(
    frame: pd.DataFrame, today: date, count: int = CHART_MONTHS
) -> Savings:
    """Доход и траты count полных месяцев перед месяцем today."""
    spent = _by_month(expenses(frame))
    earned = _by_month(income(frame))
    return Savings(
        tuple(
            MonthSaving(
                month, float(earned.get(month, 0.0)), float(spent.get(month, 0.0))
            )
            for month in full_months(today, count)
        )
    )


def cushion_of(
    budget: DayBudget | None, rates: Rates, monthly: float, target_months: int
) -> Cushion | None:
    """Подушка из «Заморожено» дневного бюджета; None — заморозку не считаем."""
    if budget is None or budget.frozen is None or not budget.frozen_currency:
        return None
    return Cushion(
        budget.frozen,
        budget.frozen_currency,
        budget.frozen_uzs,
        rates.to_uzs.get(budget.frozen_currency, 0.0),
        monthly,
        target_months,
    )


def _amount(value: float, currency: str) -> str:
    """«3 000 000 UZS», «125.00 USD» — как суммы в сводке записи."""
    digits = f"{value:,.0f}" if currency == "UZS" else f"{value:,.2f}"
    return f"{digits.replace(',', ' ')} {currency}"


def _rough(value: float, currency: str) -> str:
    """Эквивалент целыми: «$267», «250 EUR»."""
    digits = f"{value:,.0f}".replace(",", " ")
    return f"${digits}" if currency == "USD" else f"{digits} {currency}"


def incomes(rows: Sequence[SheetRow]) -> list[SheetRow]:
    """Поступления: доходы без «нач остаток» (выравнивание, а не деньги)."""
    return [
        row
        for row in rows
        if row.category == INCOME_GROUP
        and row.subcategory != OPENING_BALANCE
        and row.amount > 0
    ]


def save_lines(
    rows: Sequence[SheetRow],
    share: float,
    cushion: str | None = None,
    rates: Rates | None = None,
) -> list[str]:
    """«🛟 Отложите 10 % в подушку: 3 000 000 UZS (≈ $250)» — к поступлениям.

    «Сначала заплати себе»: долю поступления откладывают сразу, до трат.
    cushion — валюта подушки: сумма в другой валюте получает её эквивалент
    (подушка вырастет, когда их обменяют). share 0 — не напоминать.
    """
    if share <= 0:
        return []
    sums: dict[str, float] = {}
    for row in incomes(rows):
        sums[row.currency] = sums.get(row.currency, 0.0) + row.amount * share
    if not sums:
        return []
    rate = rates.to_uzs.get(cushion, 0.0) if rates and cushion else 0.0

    def part(code: str, value: float) -> str:
        text = _amount(value, code)
        own = rates.to_uzs.get(code, 0.0) if rates else 0.0
        if cushion and code != cushion and rate > 0 and own > 0:
            text += f" (≈ {_rough(value * own / rate, cushion)})"
        return text

    percent = f"{share * 100:g}".replace(".", ",")
    where = " в подушку" if cushion else ""
    parts = " и ".join(part(code, value) for code, value in sums.items())
    return [f"🛟 Отложите {percent} %{where}: {parts}"]


class RatesSource(Protocol):
    def get_rates(self, max_age: float = 0.0) -> Rates: ...


def _has_piggies(gs: RatesSource) -> bool:
    """Есть лист «Копилки»: тогда про процент с поступления скажет «🐷»."""
    reader = getattr(gs, "get_piggy", None)
    if reader is None:
        return False
    try:
        piggies, _ = reader(RATES_AGE)
    except Exception:
        return False
    return bool(piggies)


def save_hint(gs: RatesSource, rows: Sequence[SheetRow]) -> list[str]:
    """Подсказка «Отложите» для записанных строк по настройкам бота — только без
    листа «Копилки» (с ним процент и раскладку называет сообщение «🐷»).

    Курс — из копии листа, и только если среди строк есть поступление.
    """
    came = incomes(rows)
    if config.SAVE_SHARE <= 0 or not came or _has_piggies(gs):
        return []
    rates = gs.get_rates(RATES_AGE) if config.FROZEN_CURRENCY else None
    if rates is not None:
        share = sum(
            (rates.convert(row.amount, row.currency, "UZS") or 0.0) * config.SAVE_SHARE
            for row in came
        )
        if share < TINY_SHARE:
            return []
    return save_lines(rows, config.SAVE_SHARE, config.FROZEN_CURRENCY, rates)
