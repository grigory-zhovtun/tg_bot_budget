"""Карточка «💳 Сумы»: линия «по темпу» прогноза в сумах."""

from datetime import date, timedelta
from typing import Any

import pytest

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.day_budget import (
    ForecastItem,
    day_budget,
    format_day_budget,
    runway_line,
)
from app.services.forecast import currency_forecast
from app.services.runway import Runway, uzs_runway
from tests.test_day_budget import FACT_HEADER, RATES, fact_row, frame, tab

TODAY = date(2026, 10, 13)
OPENING = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 9_000_000)
ADVANCE = ForecastItem("Аванс", 15, 1_000_000, "UZS", 1_000_000, 12)
SALARY = ForecastItem("ЗП", 30, 20_000_000, "UZS", 20_000_000, 13)
RENT = ForecastItem("Квартплата", 18, -500, "USD", -6_000_000, 14)


def facts(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, *rows], RATES)


def spending(days: int, amount: float, today: date = TODAY) -> list[list[Any]]:
    """Каждый из days дней до today — трата amount с сумовой карты."""
    return [
        fact_row(today - timedelta(days=k), "🍔 ЕДА", "кафе", amount)
        for k in range(1, days + 1)
    ]


def card(
    rows: list[list[Any]],
    items: list[ForecastItem] | None = None,
    today: date = TODAY,
    rates: Rates = RATES,
) -> Runway:
    line = currency_forecast(facts(*rows), (), items or [], today, "UZS")
    return uzs_runway(line, today, rates)


def test_sums_run_out_before_the_salary_and_dollars_fill_the_gap() -> None:
    runway = card([OPENING, *spending(14, 500_000)], [ADVANCE, SALARY, RENT])
    assert (runway.balance, runway.pace) == (2_000_000, 500_000)
    # 13-го 1,5 млн, 15-го +аванс, 18-го ровно 0, 19-го уже минус
    assert runway.last_day == date(2026, 10, 18)
    # ниже всего — 29-го: −5,5 млн; 30-го зарплата закрывает минус
    assert (runway.short, runway.until) == (5_500_000, date(2026, 10, 30))
    assert runway.dollars == 460  # 5,5 млн / 12 000 = 458,3 → вверх до $10


def test_enough_sums_until_the_end_of_the_month() -> None:
    runway = card([OPENING, *spending(14, 100_000)])
    assert (runway.last_day, runway.short, runway.until, runway.dollars) == (
        None,
        0,
        None,
        None,
    )


def test_the_pace_is_ordinary_spending_and_the_balance_has_today() -> None:
    rows = [
        OPENING,
        fact_row(TODAY - timedelta(days=1), "🍔 ЕДА", "кафе", 1_400_000),
        fact_row(TODAY - timedelta(days=2), "🍔 ЕДА", "кафе", -280_000),  # возврат
        fact_row(TODAY - timedelta(days=3), "🍔 ЕДА", "кофе", 50, "USD"),
        # обмен: на сумовую карту пришло 1,2 млн (перевод между своими)
        fact_row(TODAY - timedelta(days=4), "💳 СЧЕТА", "переводы", -1_200_000),
        fact_row(TODAY - timedelta(days=5), "💰 ДОХОДЫ", "зарплата", 3_000_000),
        fact_row(TODAY - timedelta(days=20), "🍔 ЕДА", "кафе", 9_999_999),  # до окна
        fact_row(TODAY, "🍔 ЕДА", "кафе", 777_777),  # сегодня: в остатке, не в темпе
    ]
    runway = card(rows)
    assert runway.pace == (1_400_000 - 280_000) / 14
    assert runway.balance == (
        9_000_000 - 1_400_000 + 280_000 + 1_200_000 + 3_000_000 - 9_999_999 - 777_777
    )


def test_a_list_payment_does_not_speed_up_the_pace() -> None:
    camp = ForecastItem("Лагерь", 10, -3_500_000, "UZS", -3_500_000, 15)
    paid = fact_row(date(2026, 10, 10), "👶 ДЕТИ", "развитие", 3_500_000)
    runway = card([OPENING, *spending(14, 100_000), paid], [camp])
    assert runway.pace == pytest.approx(100_000)
    assert runway.balance == 9_000_000 - 1_400_000 - 3_500_000


def test_todays_spending_takes_only_what_is_left_of_the_days_pace() -> None:
    rows = [OPENING, *spending(14, 500_000)]
    coffee = fact_row(TODAY, "🍔 ЕДА", "кофе", 30_000)
    morning = card(rows, [ADVANCE, SALARY])
    evening = card([*rows, coffee], [ADVANCE, SALARY])
    # 30 000 уже ушли — до вечера по темпу уйдёт только 470 000: картина та же
    assert evening.balance == morning.balance - 30_000
    assert (evening.last_day, evening.short, evening.until) == (
        morning.last_day,
        morning.short,
        morning.until,
    )


def test_the_card_names_the_day_before_the_forecast_goes_below_zero() -> None:
    history = facts(OPENING, *spending(14, 500_000))
    line = currency_forecast(history, (), [ADVANCE, SALARY], TODAY, "UZS")
    runway = uzs_runway(line, TODAY, RATES)
    assert line.pace_dip is not None
    assert runway.last_day == TODAY.replace(day=line.pace_dip.start) - timedelta(days=1)


def test_without_a_dollar_rate_the_gap_stays_in_sums() -> None:
    runway = card(
        [OPENING, *spending(14, 500_000)], [ADVANCE, SALARY], rates=Rates({"UZS": 1.0})
    )
    assert (runway.short, runway.dollars) == (5_500_000, None)


def test_without_a_salary_the_gap_lasts_to_the_end_of_the_month() -> None:
    runway = card([OPENING, *spending(14, 500_000)])
    assert runway.last_day == date(2026, 10, 16)
    assert runway.until is None
    assert runway.short == 500_000 * 19 - 2_000_000  # 13–31 октября, 19 дней


def test_sums_already_gone() -> None:
    opening = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_000_000)
    runway = card([opening, *spending(14, 100_000)], [SALARY])
    assert runway.balance == -400_000
    assert runway.last_day == TODAY - timedelta(days=1)


def test_a_list_day_past_the_month_end_counts_on_its_last_day() -> None:
    today = date(2026, 11, 20)
    opening = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_900_000)
    salary = ForecastItem("ЗП", 31, 5_000_000, "UZS", 5_000_000, 13)  # в ноябре 30
    runway = card([opening, *spending(14, 100_000, today)], [salary], today)
    assert runway.last_day == date(2026, 11, 24)
    assert (runway.short, runway.until) == (500_000, date(2026, 11, 30))


@pytest.mark.parametrize(
    ("runway", "line"),
    [
        (
            Runway(
                2_000_000,
                500_000,
                date(2026, 10, 18),
                5_500_000,
                date(2026, 10, 30),
                460,
            ),
            "💳 Сумы 2,00 млн — хватит до 18.10 · до 30.10 обменять ≈ $460",
        ),
        (
            Runway(7_600_000, 100_000, None, 0, None, None),
            "💳 Сумы 7,60 млн — до конца месяца хватит",
        ),
        (
            Runway(2_000_000, 500_000, date(2026, 10, 16), 7_500_000, None, 630),
            "💳 Сумы 2,00 млн — хватит до 16.10 · до конца месяца обменять ≈ $630",
        ),
        (
            Runway(
                2_000_000,
                500_000,
                date(2026, 10, 18),
                5_500_000,
                date(2026, 10, 30),
                None,
            ),
            "💳 Сумы 2,00 млн — хватит до 18.10 · до 30.10 не хватит 5,50 млн",
        ),
        (
            Runway(
                -400_000,
                100_000,
                date(2026, 10, 12),
                1_900_000,
                date(2026, 10, 30),
                1_160,
            ),
            "💳 Сумы -400 000 — уже закончились · до 30.10 обменять ≈ $1 160",
        ),
        (
            Runway(
                300_000, 500_000, date(2026, 10, 13), 200_000, date(2026, 10, 15), 20
            ),
            "💳 Сумы 300 000 — хватит только на сегодня · до 15.10 обменять ≈ $20",
        ),
    ],
)
def test_the_morning_line(runway: Runway, line: str) -> None:
    assert runway_line(runway, TODAY) == line


def test_the_line_follows_the_frozen_money_in_the_morning_message() -> None:
    budget = day_budget(frame(), tab(), date(2026, 10, 2), RATES, "USD")
    assert budget is not None and budget.runway is not None
    lines = format_day_budget(budget).splitlines()
    frozen = next(i for i, text in enumerate(lines) if text.startswith("🧊"))
    assert lines[frozen + 1] == "💳 Сумы 5,00 млн — до конца месяца хватит"
