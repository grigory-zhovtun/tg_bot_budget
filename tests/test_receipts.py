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
RENT = Line(15, 18, -650, "USD")
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


def test_an_income_goes_to_the_row_with_the_nearest_day() -> None:
    found = received(income(29, 9_900_000), income(17, 2_000_000))
    assert found[SALARY.row].received == 9_900_000  # на день раньше 30-го
    assert found[ADVANCE.row].received == 2_000_000  # аванс опоздал на два дня


def test_the_window_opens_five_days_before_the_row() -> None:
    assert ADVANCE.row not in received(income(9, 2_000_000))
    assert received(income(10, 2_000_000))[ADVANCE.row].received == 2_000_000


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
    paid = received(rent(17, 20), rent(17, 640), rent(18, 500))[RENT.row]
    assert (paid.received, paid.on, len(paid.operations)) == (
        -640,
        date(2026, 10, 17),
        1,
    )
    assert remaining(RENT, paid) == 0


def test_small_purchases_do_not_close_a_payment() -> None:
    assert RENT.row not in received(rent(18, 20), rent(19, 500))


def test_one_spending_closes_one_payment() -> None:
    twice = [RENT, Line(16, 20, -650, "USD")]
    frame = transactions_frame([FACT_HEADER, rent(18, 650)], RATES)
    found = match_receipts(frame, twice, OCTOBER, date(2026, 10, 31))
    assert list(found) == [RENT.row]  # первой по дню


def test_transfers_refunds_and_the_opening_balance_do_not_count() -> None:
    rows = [
        fact_row(date(2026, 10, 18), "💳 СЧЕТА", "переводы", 650, "USD"),
        rent(18, -650),  # возврат
        income(15, 2_000_000, sub="нач остаток"),
    ]
    assert received(*rows) == {}


def test_the_status_follows_the_day_and_what_came() -> None:
    assert receipt_status(ADVANCE, NOTHING, date(2026, 10, 15)) == "future"
    assert receipt_status(ADVANCE, NOTHING, date(2026, 10, 16)) == "waiting"
    paid = received(income(15, 2_000_000))[ADVANCE.row]
    assert receipt_status(ADVANCE, paid, date(2026, 10, 15)) == "done"
