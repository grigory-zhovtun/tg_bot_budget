"""Сверка жёлтого списка с fact: что пришло и что ушло по строкам."""

from datetime import date
from typing import Any

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.receipts import (
    NOTHING,
    Line,
    Receipt,
    match_receipts,
    receipt_status,
    remaining,
)
from tests.test_day_budget import FACT_HEADER, fact_row

RATES = Rates({"UZS": 1.0, "USD": 12_000.0, "RUB": 150.0})
OCTOBER = date(2026, 10, 1)
ADVANCE = Line(12, 15, 2_000_000, "UZS")
SALARY = Line(13, 30, 10_000_000, "UZS")
RUB_SALARY = Line(14, 28, 100_000, "RUB")
RENT = Line(15, 18, -400, "USD")
LINES = [ADVANCE, SALARY, RUB_SALARY, RENT]


def income(day: int, amount: float, cur: str = "UZS", sub: str = "зарплата") -> Any:
    return fact_row(date(2026, 10, day), "💰 ДОХОДЫ", sub, amount, cur)


def rent(day: int, amount: float) -> Any:
    return fact_row(date(2026, 10, day), "🏚️ ДОМ", "квартплата", amount, "USD")


def received(*rows: Any, through: int = 31) -> dict[int, Receipt]:
    frame = transactions_frame([FACT_HEADER, *rows], RATES)
    return match_receipts(frame, LINES, OCTOBER, date(2026, 10, through))


def test_a_salary_paid_in_parts_adds_up() -> None:
    found = received(income(28, 60_000, "RUB"), income(29, 40_000, "RUB"))
    assert found[RUB_SALARY.row].received == 100_000
    assert found[RUB_SALARY.row].on == date(2026, 10, 29)
    assert remaining(RUB_SALARY, found[RUB_SALARY.row]) == 0


def test_incomes_fill_the_lines_in_the_order_of_their_days() -> None:
    found = received(income(29, 9_900_000), income(17, 2_000_000))
    assert found[SALARY.row].received == 9_900_000  # на день раньше 30-го
    assert found[ADVANCE.row].received == 2_000_000  # аванс опоздал на два дня


def test_an_early_income_counts_toward_the_months_plan() -> None:
    # окна нет: доход месяца — в план месяца, даже за шесть дней до строки
    assert received(income(9, 2_000_000))[ADVANCE.row].received == 2_000_000


def test_half_of_a_salary_a_week_early_counts() -> None:
    found = received(income(21, 47_000, "RUB"))
    assert found[RUB_SALARY.row].received == 47_000
    assert found[RUB_SALARY.row].on == date(2026, 10, 21)
    assert remaining(RUB_SALARY, found[RUB_SALARY.row]) == 53_000


def test_an_income_over_a_line_flows_to_the_next() -> None:
    found = received(income(15, 2_500_000))
    assert found[ADVANCE.row].received == 2_000_000
    assert found[SALARY.row].received == 500_000


def test_after_the_advance_an_income_goes_to_the_salary() -> None:
    found = received(income(15, 2_000_000), income(17, 3_000_000))
    assert found[ADVANCE.row].received == 2_000_000
    assert found[SALARY.row].received == 3_000_000


def test_more_than_the_month_lands_on_the_last_line() -> None:
    found = received(income(28, 150_000, "RUB"))
    assert found[RUB_SALARY.row].received == 150_000


def test_operations_after_through_do_not_count_yet() -> None:
    assert ADVANCE.row not in received(income(16, 2_000_000), through=15)
    assert ADVANCE.row in received(income(16, 2_000_000), through=16)


def test_ninety_percent_closes_a_row_and_less_leaves_the_rest_ahead() -> None:
    almost = received(income(15, 1_800_000))[ADVANCE.row]
    assert remaining(ADVANCE, almost) == 0
    part = received(income(15, 1_700_000))[ADVANCE.row]
    assert remaining(ADVANCE, part) == 300_000
    assert receipt_status(ADVANCE, part, date(2026, 10, 20)) == "partial"


def test_a_payment_is_one_spending_close_to_its_sum() -> None:
    paid = received(rent(17, 20), rent(17, 390), rent(18, 300))[RENT.row]
    assert (paid.received, paid.on, len(paid.operations)) == (
        -390,
        date(2026, 10, 17),
        1,
    )
    assert remaining(RENT, paid) == 0


def test_small_purchases_do_not_close_a_payment() -> None:
    assert RENT.row not in received(rent(18, 20), rent(19, 300))


def test_one_spending_closes_one_payment() -> None:
    twice = [RENT, Line(16, 20, -400, "USD")]
    frame = transactions_frame([FACT_HEADER, rent(18, 400)], RATES)
    found = match_receipts(frame, twice, OCTOBER, date(2026, 10, 31))
    assert list(found) == [RENT.row]  # первой по дню


def test_transfers_refunds_and_the_opening_balance_do_not_count() -> None:
    rows = [
        fact_row(date(2026, 10, 18), "💳 СЧЕТА", "переводы", 400, "USD"),
        rent(18, -400),  # возврат
        income(15, 2_000_000, sub="нач остаток"),
    ]
    assert received(*rows) == {}


# остатки поступлений прошлого месяца (перенос): день 1, строки −1, −2, … по его дням
LEFT_SALARY = Line(-1, 1, 55_000, "RUB")
LEFT_BONUS = Line(-2, 1, 30_000, "RUB")


def carried(*rows: Any, left: tuple[Line, ...] = (LEFT_SALARY,)) -> dict[int, Receipt]:
    frame = transactions_frame([FACT_HEADER, *rows], RATES)
    return match_receipts(frame, LINES, OCTOBER, date(2026, 10, 31), left)


def test_last_months_remainder_is_filled_before_the_months_line() -> None:
    found = carried(income(3, 70_000, "RUB"))
    # перенос — под своей отрицательной строкой, чтобы не совпасть со строкой вкладки
    assert sorted(found) == [LEFT_SALARY.row, RUB_SALARY.row]
    assert found[LEFT_SALARY.row].received == 55_000
    assert remaining(LEFT_SALARY, found[LEFT_SALARY.row]) == 0
    assert found[RUB_SALARY.row].received == 15_000  # строке месяца — что сверх


def test_remainders_are_filled_in_the_order_they_are_given() -> None:
    # все в день 1: порядок — дни прошлого месяца, как их дал перенос
    found = carried(income(3, 60_000, "RUB"), left=(LEFT_SALARY, LEFT_BONUS))
    assert found[LEFT_SALARY.row].received == 55_000
    assert found[LEFT_BONUS.row].received == 5_000
    assert RUB_SALARY.row not in found


def test_a_remainder_waits_only_for_its_own_currency() -> None:
    found = carried(income(15, 2_000_000))
    assert found == received(income(15, 2_000_000))


def test_the_status_follows_the_day_and_what_came() -> None:
    assert receipt_status(ADVANCE, NOTHING, date(2026, 10, 15)) == "future"
    assert receipt_status(ADVANCE, NOTHING, date(2026, 10, 16)) == "waiting"
    paid = received(income(15, 2_000_000))[ADVANCE.row]
    assert receipt_status(ADVANCE, paid, date(2026, 10, 15)) == "done"
