"""Подушка безопасности и норма сбережений: «Сводка» и строка «Отложите» после поступления."""

from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from app import config
from app.config import parse_months, parse_share
from app.domain import Rates, SheetRow
from app.services.analytics_service import transactions_frame
from app.services.savings import (
    cushion_of,
    full_months,
    month_savings,
    monthly_spending,
    save_hint,
    save_lines,
)
from tests.test_day_budget import FACT_HEADER, RATES, fact_row

TODAY = date(2026, 10, 17)


def frame(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, *rows], RATES)


def frozen(amount: float | None, uzs: float = 0.0) -> Any:
    """Дневной бюджет с «Заморожено» в долларах — только то, что берёт подушка."""
    return SimpleNamespace(frozen=amount, frozen_currency="USD", frozen_uzs=uzs)


def test_full_months_end_before_this_month() -> None:
    assert full_months(TODAY, 3) == [
        date(2026, 7, 1),
        date(2026, 8, 1),
        date(2026, 9, 1),
    ]
    assert full_months(date(2026, 2, 5), 3) == [
        date(2025, 11, 1),
        date(2025, 12, 1),
        date(2026, 1, 1),
    ]


def test_monthly_spending_is_the_average_of_the_last_full_months() -> None:
    rows = [
        fact_row(date(2026, 6, 10), "🍔 ЕДА", "кафе", 100_000_000),  # раньше трёх
        fact_row(date(2026, 7, 10), "🍔 ЕДА", "кафе", 60_000_000),
        fact_row(date(2026, 8, 10), "🍔 ЕДА", "кафе", 85_000_000),
        fact_row(date(2026, 8, 11), "🍔 ЕДА", "кафе", 5_000_000),
        fact_row(date(2026, 9, 10), "🍔 ЕДА", "кафе", 2_500, "USD"),  # 30 млн
        fact_row(date(2026, 9, 12), "💳 СЧЕТА", "перевод", 7_000_000),  # не трата
        fact_row(date(2026, 9, 15), "💰 ДОХОДЫ", "зарплата", 50_000_000),
        fact_row(TODAY, "🍔 ЕДА", "кафе", 10_000_000),  # месяц ещё не кончился
    ]
    assert monthly_spending(frame(*rows), TODAY) == 60_000_000  # (60 + 90 + 30) / 3


def test_months_before_the_first_row_do_not_lower_the_average() -> None:
    rows = [fact_row(date(2026, 9, 3), "🍔 ЕДА", "кафе", 30_000_000)]
    assert monthly_spending(frame(*rows), TODAY) == 30_000_000
    assert monthly_spending(frame(), TODAY) == 0


def test_a_month_started_late_is_not_a_full_month() -> None:
    # учёт трат начат 20-го: сентябрь не полный, иначе подушка вышла бы втрое больше
    rows = [
        fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 5_000_000),
        fact_row(date(2026, 9, 20), "🍔 ЕДА", "кафе", 10_000_000),
    ]
    assert monthly_spending(frame(*rows), TODAY) == 0


def test_refunds_lower_the_spending_of_their_month() -> None:
    rows = [
        fact_row(date(2026, 9, 3), "🍔 ЕДА", "кафе", 30_000_000),
        fact_row(date(2026, 9, 9), "🍔 ЕДА", "кафе", -6_000_000),
    ]
    assert monthly_spending(frame(*rows), TODAY) == 24_000_000


def test_savings_rate_by_month_and_for_three_months() -> None:
    rows = [
        fact_row(date(2026, 4, 1), "💰 ДОХОДЫ", "зарплата", 40_000_000),
        fact_row(date(2026, 4, 2), "🍔 ЕДА", "кафе", 50_000_000),
        fact_row(date(2026, 7, 1), "💰 ДОХОДЫ", "зарплата", 50_000_000),
        fact_row(date(2026, 7, 2), "🍔 ЕДА", "кафе", 40_000_000),
        fact_row(date(2026, 8, 2), "🍔 ЕДА", "кафе", 20_000_000),  # дохода не было
        fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "зарплата", 5_000, "USD"),  # 60 млн
        fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 99_000_000),
        fact_row(date(2026, 9, 3), "💳 СЧЕТА", "перевод", 9_000_000),
        fact_row(date(2026, 9, 2), "🍔 ЕДА", "кафе", 30_000_000),
        fact_row(TODAY, "💰 ДОХОДЫ", "зарплата", 1_000_000),  # не полный месяц
    ]
    found = month_savings(frame(*rows), TODAY)
    assert [item.month for item in found.months] == [
        date(2026, number, 1) for number in (4, 5, 6, 7, 8, 9)
    ]
    april, may, _, _, august, september = found.months
    assert (april.income, april.spent) == (40_000_000, 50_000_000)
    assert april.rate == pytest.approx(-0.25)
    assert may.rate is None  # ни дохода, ни трат
    assert august.rate is None  # траты без дохода: долю не посчитать
    # «нач остаток» — не доход, перевод между своими картами — не трата
    assert (september.income, september.spent) == (60_000_000, 30_000_000)
    assert september.rate == pytest.approx(0.5)
    # за 3 месяца: (50 + 0 + 60 − 40 − 20 − 30) / (50 + 0 + 60)
    assert found.rate == pytest.approx(20 / 110)


def test_without_income_the_rate_is_unknown() -> None:
    rows = [fact_row(date(2026, 9, 2), "🍔 ЕДА", "кафе", 30_000_000)]
    assert month_savings(frame(*rows), TODAY).rate is None


def test_the_cushion_is_the_frozen_money_in_months_of_spending() -> None:
    found = cushion_of(frozen(1_800, 21_600_000), RATES, 60_000_000, 6)
    assert found is not None
    assert found.months == pytest.approx(0.36)
    assert found.short_uzs == pytest.approx(360_000_000 - 21_600_000)
    assert found.short == pytest.approx((360_000_000 - 21_600_000) / 12_000)


def test_a_cushion_at_its_target_lacks_nothing() -> None:
    found = cushion_of(frozen(40_000, 480_000_000), RATES, 60_000_000, 6)
    assert found is not None
    assert (found.months, found.short_uzs, found.short) == (8, 0, 0)


def test_no_cushion_without_frozen_money() -> None:
    assert cushion_of(None, RATES, 60_000_000, 6) is None
    assert cushion_of(frozen(None), RATES, 60_000_000, 6) is None


def test_without_spending_the_months_are_unknown() -> None:
    found = cushion_of(frozen(1_800, 21_600_000), RATES, 0, 6)
    assert found is not None
    assert (found.months, found.short_uzs) == (None, 0)


def test_without_a_rate_nothing_is_short_in_the_cushion_currency() -> None:
    found = cushion_of(frozen(1_800, 21_600_000), Rates({"UZS": 1.0}), 60e6, 6)
    assert found is not None
    assert (found.short_uzs, found.short) == (360_000_000 - 21_600_000, 0)


def test_payments_bigger_than_the_frozen_money_leave_no_cushion() -> None:
    found = cushion_of(frozen(-200, -2_400_000), RATES, 60_000_000, 6)
    assert found is not None
    assert (found.months, found.short_uzs) == (0, 360_000_000)


def income(amount: float, currency: str = "UZS", sub: str = "зарплата") -> SheetRow:
    return SheetRow(TODAY, "💰 ДОХОДЫ", sub, amount, "", currency, "VISA 9120 UZS")


def test_an_income_suggests_setting_a_share_aside() -> None:
    # подушка в долларах: сумы надо обменять — рядом их эквивалент
    assert save_lines([income(30_000_000)], 0.1, "USD", RATES) == [
        "🛟 Отложите 10 % в подушку: 3 000 000 UZS (≈ $250)"
    ]
    assert save_lines([income(1_000, "USD")], 0.1, "USD", RATES) == [
        "🛟 Отложите 10 % в подушку: 100.00 USD"
    ]


def test_incomes_in_two_currencies_make_one_line() -> None:
    rows = [income(30_000_000), income(1_250, "USD"), income(2_000_000)]
    assert save_lines(rows, 0.1, "USD", RATES) == [
        "🛟 Отложите 10 % в подушку: 3 200 000 UZS (≈ $267) и 125.00 USD"
    ]


def test_without_a_cushion_the_hint_does_not_name_it() -> None:
    assert save_lines([income(30_000_000)], 0.1) == ["🛟 Отложите 10 %: 3 000 000 UZS"]


def test_spending_and_the_opening_balance_suggest_nothing() -> None:
    coffee = SheetRow(TODAY, "🍔 ЕДА", "кофе", 48_000, "", "UZS", "VISA 9120 UZS")
    opening = income(5_000_000, sub="нач остаток")
    assert save_lines([coffee, opening], 0.1, "USD", RATES) == []
    assert save_lines([income(30_000_000)], 0, "USD", RATES) == []  # выключено


def test_refunds_and_money_back_suggest_nothing() -> None:
    refund = SheetRow(TODAY, "🍔 ЕДА", "кафе", -50_000, "", "UZS", "VISA 9120 UZS")
    back = SheetRow(TODAY, "💳 СЧЕТА", "перевод", -1e6, "", "UZS", "VISA 9120 UZS")
    assert save_lines([refund, back, income(-5_000_000)], 0.1, "USD", RATES) == []


class RatesOnly:
    def __init__(self) -> None:
        self.asked: list[float] = []

    def get_rates(self, max_age: float = 0.0) -> Rates:
        self.asked.append(max_age)
        return RATES


def test_the_hint_reads_the_rates_only_for_an_income(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "SAVE_SHARE", 0.1)
    monkeypatch.setattr(config, "FROZEN_CURRENCY", "USD")
    sheets = RatesOnly()
    coffee = SheetRow(TODAY, "🍔 ЕДА", "кофе", 48_000, "", "UZS", "VISA 9120 UZS")
    assert save_hint(sheets, [coffee]) == []
    assert sheets.asked == []  # трата — без лишнего чтения таблицы
    assert save_hint(sheets, [income(30_000_000)]) == [
        "🛟 Отложите 10 % в подушку: 3 000 000 UZS (≈ $250)"
    ]
    assert sheets.asked and sheets.asked[0] > 0  # курс — из копии листа


@pytest.mark.parametrize(
    ("text", "share"),
    [
        ("0.10", 0.1),
        ("0,15", 0.15),
        ("10", 0.1),  # проценты вместо доли
        ("12.5%", 0.125),
        ("off", 0),
        ("", 0),
        ("abc", 0),
        ("-3", 0),
        ("250", 1),
    ],
)
def test_the_share_to_set_aside_is_read_kindly(text: str, share: float) -> None:
    assert parse_share(text) == pytest.approx(share)


@pytest.mark.parametrize(
    ("text", "months"), [("6", 6), ("3", 3), ("", 6), ("abc", 6), ("0", 6)]
)
def test_the_cushion_target_is_read_kindly(text: str, months: int) -> None:
    assert parse_months(text) == months
