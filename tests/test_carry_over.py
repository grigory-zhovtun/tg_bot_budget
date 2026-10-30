"""Перенос: поступление прошлого месяца, по которому пришло меньше 90 %, ждёт свой
остаток в этом месяце (спека 1.8). Суммы выдуманные."""

from datetime import date
from typing import Any

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.day_budget import ForecastItem, carried_items
from tests.test_day_budget import FACT_HEADER, OPENING, fact_row, tab

RATES = Rates({"UZS": 1.0, "USD": 12_000.0, "RUB": 150.0})
NOVEMBER = date(2026, 11, 1)


def listed(*items: tuple[str, int, float, str]) -> list[list[Any]]:
    """Вкладка месяца с жёлтым списком: (название, день, сумма, валюта)."""
    rows = tab(items=[])
    for k, (name, day, amount, cur) in enumerate(items):
        rows[11 + k][14:19] = [name, day, amount, cur, amount * RATES.to_uzs[cur]]
    return rows


OCTOBER_TAB = listed(
    ("ЗП Питер", 28, 100_000, "RUB"),
    ("Квартплата", 18, -400, "USD"),
)


def rubles(day: date, amount: float) -> list[Any]:
    return fact_row(day, "💰 ДОХОДЫ", "зарплата", amount, "RUB")


def facts(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, *OPENING, *rows], RATES)


def test_the_unpaid_part_of_last_months_income_waits_this_month() -> None:
    came = facts(rubles(date(2026, 10, 28), 45_000))
    assert carried_items(OCTOBER_TAB, came, NOVEMBER) == (
        ForecastItem("ЗП Питер за октябрь, остаток", 1, 55_000, "RUB", 8_250_000, -1),
    )


def test_ninety_percent_closes_last_months_income() -> None:
    came = facts(rubles(date(2026, 10, 28), 95_000))
    assert carried_items(OCTOBER_TAB, came, NOVEMBER) == ()


def test_payments_do_not_carry_over() -> None:
    # квартплата 18 октября не ушла — платёж в следующий месяц не переносится
    came = facts(rubles(date(2026, 10, 28), 100_000))
    assert carried_items(OCTOBER_TAB, came, NOVEMBER) == ()


def test_an_income_recorded_later_with_last_months_date_lowers_the_rest() -> None:
    # 31 октября записали 2 ноября, после ноябрьского дохода: остаток меньше, а
    # ноябрьский доход прошлому месяцу не засчитывается
    came = facts(
        rubles(date(2026, 10, 28), 45_000),
        rubles(date(2026, 11, 2), 10_000),
        rubles(date(2026, 10, 31), 20_000),
    )
    [rest] = carried_items(OCTOBER_TAB, came, NOVEMBER)
    assert (rest.amount, rest.uzs) == (35_000, 5_250_000)


def test_rows_go_minus_one_minus_two_by_last_months_days() -> None:
    october = listed(
        ("ЗП Питер", 28, 100_000, "RUB"),
        ("Возврат долга", 5, 1_000_000, "UZS"),
    )
    found = carried_items(october, facts(), NOVEMBER)
    assert [(item.name, item.row) for item in found] == [
        ("Возврат долга за октябрь, остаток", -1),
        ("ЗП Питер за октябрь, остаток", -2),
    ]


def test_without_last_months_tab_or_its_list_nothing_carries_over() -> None:
    assert carried_items(None, facts(), NOVEMBER) == ()
    no_list = [row[:14] for row in OCTOBER_TAB]
    assert carried_items(no_list, facts(), NOVEMBER) == ()
