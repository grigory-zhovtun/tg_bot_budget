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
from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

from app.domain import INCOME_GROUP, Rates, SheetRow
from app.services.analytics_service import OPENING_BALANCE, expenses, income

if TYPE_CHECKING:
    from app.services.day_budget import DayBudget

AVERAGE_MONTHS = 3  # средние траты для подушки — за столько полных месяцев
RATE_MONTHS = 3  # норма сбережений крупно — за столько последних полных месяцев
CHART_MONTHS = 6  # столбики нормы — по стольким полным месяцам


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

    Месяцы до первой строки fact не в счёт: иначе учёт, начатый недавно,
    занижал бы траты и завышал подушку.
    """
    if frame.empty:
        return 0.0
    first = min(frame["day"]).replace(day=1)
    months = [month for month in full_months(today, count) if month >= first]
    if not months:
        return 0.0
    spent = _by_month(expenses(frame))
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


def save_lines(rows: Sequence[SheetRow], share: float) -> list[str]:
    """«🛟 Отложите 10 % в подушку: 3 000 000 UZS» — к сводке записи поступлений.

    «Сначала заплати себе»: долю поступления откладывают сразу, до трат.
    share 0 — не напоминать; «нач остаток» — выравнивание, не поступление.
    """
    if share <= 0:
        return []
    sums: dict[str, float] = {}
    for row in rows:
        if (
            row.category == INCOME_GROUP
            and row.subcategory != OPENING_BALANCE
            and row.amount > 0
        ):
            sums[row.currency] = sums.get(row.currency, 0.0) + row.amount * share
    if not sums:
        return []
    percent = f"{share * 100:g}".replace(".", ",")
    parts = " и ".join(_amount(value, code) for code, value in sums.items())
    return [f"🛟 Отложите {percent} % в подушку: {parts}"]
