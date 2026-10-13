"""Прогноз по сумам: когда закончатся и сколько долларов обменять до зарплаты."""

from datetime import date, timedelta
from typing import Any

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.day_budget import ForecastItem
from app.services.runway import uzs_runway
from tests.test_day_budget import FACT_HEADER, RATES, fact_row

TODAY = date(2026, 10, 13)
OPENING = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 9_000_000)
ADVANCE = ForecastItem("Аванс", 15, 1_000_000, "UZS", 1_000_000)
SALARY = ForecastItem("ЗП", 30, 20_000_000, "UZS", 20_000_000)
RENT = ForecastItem("Квартплата", 18, -500, "USD", -6_000_000)


def facts(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, *rows], RATES)


def spending(days: int, amount: float, today: date = TODAY) -> list[list[Any]]:
    """Каждый из days дней до today — трата amount с сумовой карты."""
    return [
        fact_row(today - timedelta(days=k), "🍔 ЕДА", "кафе", amount)
        for k in range(1, days + 1)
    ]


def test_sums_run_out_before_the_salary_and_dollars_fill_the_gap() -> None:
    runway = uzs_runway(
        facts(OPENING, *spending(14, 500_000)), [ADVANCE, SALARY, RENT], TODAY, RATES
    )
    assert (runway.balance, runway.pace) == (2_000_000, 500_000)
    # 13-го 1,5 млн, 15-го +аванс, 18-го ровно 0, 19-го уже минус
    assert runway.last_day == date(2026, 10, 18)
    # ниже всего — 29-го: −5,5 млн; 30-го зарплата закрывает минус
    assert (runway.short, runway.until) == (5_500_000, date(2026, 10, 30))
    assert runway.dollars == 460  # 5,5 млн / 12 000 = 458,3 → вверх до $10


def test_enough_sums_until_the_end_of_the_month() -> None:
    runway = uzs_runway(facts(OPENING, *spending(14, 100_000)), [], TODAY, RATES)
    assert (runway.last_day, runway.short, runway.until, runway.dollars) == (
        None,
        0,
        None,
        None,
    )


def test_only_spending_from_sums_sets_the_pace() -> None:
    rows = [
        OPENING,
        fact_row(TODAY - timedelta(days=1), "🍔 ЕДА", "кафе", 1_400_000),
        fact_row(TODAY - timedelta(days=2), "🍔 ЕДА", "кафе", -280_000),  # возврат
        fact_row(TODAY - timedelta(days=3), "🍔 ЕДА", "кофе", 50, "USD"),
        # обмен: на сумовую карту пришло 1,2 млн (перевод между своими)
        fact_row(TODAY - timedelta(days=4), "💳 СЧЕТА", "переводы", -1_200_000),
        fact_row(TODAY - timedelta(days=5), "💰 ДОХОДЫ", "зарплата", 3_000_000),
        fact_row(TODAY - timedelta(days=20), "🍔 ЕДА", "кафе", 9_999_999),  # до окна
        fact_row(TODAY, "🍔 ЕДА", "кафе", 777_777),  # сегодня не в темпе и не в остатке
    ]
    runway = uzs_runway(facts(*rows), [], TODAY, RATES)
    assert runway.pace == (1_400_000 - 280_000) / 14
    assert runway.balance == (
        9_000_000 - 1_400_000 + 280_000 + 1_200_000 + 3_000_000 - 9_999_999
    )


def test_without_a_dollar_rate_the_gap_stays_in_sums() -> None:
    runway = uzs_runway(
        facts(OPENING, *spending(14, 500_000)),
        [ADVANCE, SALARY],
        TODAY,
        Rates({"UZS": 1.0}),
    )
    assert (runway.short, runway.dollars) == (5_500_000, None)


def test_without_a_salary_the_gap_lasts_to_the_end_of_the_month() -> None:
    runway = uzs_runway(facts(OPENING, *spending(14, 500_000)), [], TODAY, RATES)
    assert runway.last_day == date(2026, 10, 16)
    assert runway.until is None
    assert runway.short == 500_000 * 19 - 2_000_000  # 13–31 октября, 19 дней


def test_sums_already_gone() -> None:
    opening = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_000_000)
    runway = uzs_runway(facts(opening, *spending(14, 100_000)), [SALARY], TODAY, RATES)
    assert runway.balance == -400_000
    assert runway.last_day == TODAY - timedelta(days=1)


def test_a_list_day_past_the_month_end_counts_on_its_last_day() -> None:
    today = date(2026, 11, 20)
    opening = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_900_000)
    salary = ForecastItem("ЗП", 31, 5_000_000, "UZS", 5_000_000)  # в ноябре 30 дней
    runway = uzs_runway(
        facts(opening, *spending(14, 100_000, today)), [salary], today, RATES
    )
    assert runway.last_day == date(2026, 11, 24)
    assert (runway.short, runway.until) == (500_000, date(2026, 11, 30))
