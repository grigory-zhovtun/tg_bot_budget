"""Перенос: поступление прошлого месяца, по которому пришло меньше 90 %, ждёт свой
остаток в этом месяце (спека 1.8). Суммы выдуманные."""

from datetime import date
from typing import Any

import pytest

from app.domain import Rates
from app.services.analytics_service import AnalyticsService, transactions_frame
from app.services.day_budget import (
    DayBudget,
    ForecastItem,
    carried_items,
    day_budget,
    format_day_budget,
)
from app.services.forecast import month_forecast
from app.services.plan import PlanChanges, load_plan
from app.services.plan_advice import gather_advice
from tests.test_day_budget import FACT_HEADER, OPENING, fact_row, tab
from tests.test_day_budget import Sheets as DaySheets

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


NOVEMBER_TAB = listed(
    ("Аванс", 15, 2_000_000, "UZS"),
    ("ЗП Питер", 28, 100_000, "RUB"),
    ("ЗП", 30, 10_000_000, "UZS"),
)
HALF_PAID = rubles(date(2026, 10, 28), 45_000)  # из 100 000 за октябрь


def november(
    today: date, *rows: list[Any], previous: list[list[Any]] | None = OCTOBER_TAB
) -> DayBudget:
    found = day_budget(
        facts(HALF_PAID, *rows), NOVEMBER_TAB, today, RATES, "USD", previous=previous
    )
    assert found is not None
    return found


def test_last_months_remainder_is_ahead_first() -> None:
    day = november(date(2026, 11, 3))
    assert [(item.name, item.amount) for item in day.ahead] == [
        ("ЗП Питер за октябрь, остаток", 55_000),
        ("Аванс", 2_000_000),
        ("ЗП Питер", 100_000),
        ("ЗП", 10_000_000),
    ]
    # день 1 прошёл, а денег нет — «⏳», как у любой строки, чей день прошёл
    assert (
        "📅 Впереди: ⏳ 01.11 ЗП Питер за октябрь, остаток +55 000 RUB • "
        "15.11 Аванс +2,00 млн"
    ) in format_day_budget(day).splitlines()


def test_the_remainder_is_filled_before_this_months_line() -> None:
    day = november(date(2026, 11, 4), rubles(date(2026, 11, 4), 55_000))
    # 55 000 закрыли октябрьский остаток — ноябрьская ЗП ждёт 100 000 целиком
    assert [(item.name, item.amount) for item in day.ahead] == [
        ("Аванс", 2_000_000),
        ("ЗП Питер", 100_000),
        ("ЗП", 10_000_000),
    ]
    alone = november(
        date(2026, 11, 4), rubles(date(2026, 11, 4), 55_000), previous=None
    )
    assert ("ЗП Питер", 45_000) in [(item.name, item.amount) for item in alone.ahead]


OCTOBER_SALARY = listed(("ЗП", 30, 10_000_000, "UZS"))


def test_a_carried_salary_in_sums_does_not_set_the_limits_horizon() -> None:
    # октябрьская ЗП пришла наполовину: остаток с днём 1 — прочее поступление, а не
    # зарплата: 30 ноября, когда ноябрьской ЗП ещё нет, срок — до ЗП 30 декабря
    october = facts(fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 5_000_000))
    day = day_budget(
        october,
        NOVEMBER_TAB,
        date(2026, 11, 30),
        RATES,
        "USD",
        previous=OCTOBER_SALARY,
    )
    assert day is not None
    assert (day.payday, day.payday_name) == (date(2026, 12, 30), "ЗП")
    assert day.ahead[0].name == "ЗП за октябрь, остаток"


def test_this_months_advance_waits_while_last_months_salary_is_short() -> None:
    # 2 млн 10 ноября закрывают остаток октябрьской ЗП, а не аванс 15-го: аванс ещё
    # впереди и входит в деньги срока
    came = facts(
        fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 5_000_000),
        fact_row(date(2026, 11, 10), "💰 ДОХОДЫ", "зарплата", 2_000_000),
    )
    day = day_budget(
        came,
        NOVEMBER_TAB,
        date(2026, 11, 11),
        RATES,
        "USD",
        previous=OCTOBER_SALARY,
    )
    assert day is not None
    assert (day.advances, day.advance_on) == (2_000_000, date(2026, 11, 15))
    assert [(item.name, item.amount) for item in day.ahead][:2] == [
        ("ЗП за октябрь, остаток", 3_000_000),
        ("Аванс", 2_000_000),
    ]


DEBT = listed(("Возврат долга", 5, 1_000_000, "UZS"))
PACE = [
    fact_row(date(2026, 10, 20 + k), "🍔 ЕДА", "кафе", 300_000) for k in range(12)
] + [fact_row(date(2026, 11, k), "🍔 ЕДА", "кафе", 300_000) for k in (1, 2)]
DEBT_BACK = fact_row(date(2026, 11, 2), "💰 ДОХОДЫ", "возврат долга", 1_000_000)


def test_the_sums_card_waits_for_the_whole_advance() -> None:
    # долг вернули 2 ноября — он закрыл октябрьский остаток, аванс 15-го ждём целиком:
    # сумы 1,8 млн, 300 000 в день — к 29-му −6,3 млн и аванс +2 млн
    day = day_budget(
        facts(*PACE, DEBT_BACK),
        NOVEMBER_TAB,
        date(2026, 11, 3),
        RATES,
        "USD",
        previous=DEBT,
    )
    assert day is not None and day.runway is not None
    assert day.runway.short == pytest.approx(4_300_000)


def test_the_calendar_gets_no_new_events_and_waits_for_the_whole_line() -> None:
    came = facts(HALF_PAID, rubles(date(2026, 11, 4), 55_000))
    found = month_forecast(
        came,
        (),
        [
            ForecastItem("ЗП Питер", 28, 100_000, "RUB", 15_000_000, 13),
        ],
        date(2026, 11, 5),
        RATES,
        ["RUB"],
        carried=carried_items(OCTOBER_TAB, came, NOVEMBER),
    )
    [rubles_line] = found.currencies
    assert [(event.name, event.amount) for event in rubles_line.events] == [
        ("ЗП Питер", 100_000)
    ]


class Sheets(DaySheets):
    """Листы в памяти, курсы с рублём."""

    def get_rates(self, max_age: float = 0.0) -> Rates:
        self.ages.append(max_age)
        return RATES

    def get_advisor_profile(self) -> str:
        return ""


FACT = [FACT_HEADER, *OPENING, HALF_PAID]
BOTH = {"fact": FACT, "Oct 26": OCTOBER_TAB, "Nov 26": NOVEMBER_TAB}


def test_the_morning_message_reads_last_months_tab() -> None:
    service = AnalyticsService(Sheets(BOTH))
    assert (
        "📅 Впереди: ⏳ 01.11 ЗП Питер за октябрь, остаток +55 000 RUB • "
        "15.11 Аванс +2,00 млн"
    ) in service.morning_brief(date(2026, 11, 3)).text.splitlines()
    # вкладки октября нет — как раньше: впереди только ноябрьские строки
    alone = AnalyticsService(Sheets({"fact": FACT, "Nov 26": NOVEMBER_TAB}))
    assert "📅 Впереди: 15.11 Аванс +2,00 млн • 28.11 ЗП Питер +100 000 RUB" in (
        alone.morning_brief(date(2026, 11, 3)).text.splitlines()
    )


SHORT_SALARY = [
    FACT_HEADER,
    *OPENING,
    fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 5_000_000),
    fact_row(date(2026, 11, 10), "💰 ДОХОДЫ", "зарплата", 2_000_000),
]


def test_the_reply_to_a_record_counts_the_advance_still_ahead() -> None:
    sheets = Sheets(
        {"fact": SHORT_SALARY, "Oct 26": OCTOBER_SALARY, "Nov 26": NOVEMBER_TAB}
    )
    feedback = AnalyticsService(sheets).write_feedback(
        ["кафе"], today=date(2026, 11, 11)
    )
    # 12 млн сумов и аванс 15-го 2 млн на 20 дней по ЗП 30-го
    assert feedback.lines[-1] == "💸 На сегодня осталось 700 000 из 700 000"


def test_the_summary_screen_reads_last_months_tab() -> None:
    found = AnalyticsService(Sheets(BOTH)).dashboard(date(2026, 11, 3))
    assert found is not None
    assert [(item.name, item.amount) for item in found.upcoming][:2] == [
        ("ЗП Питер за октябрь, остаток", 55_000),
        ("Аванс", 2_000_000),
    ]


PAID_REST = [*FACT, rubles(date(2026, 11, 4), 55_000)]


def test_the_calendar_screen_waits_for_the_whole_line() -> None:
    sheets = Sheets({**BOTH, "fact": PAID_REST})
    found = AnalyticsService(sheets).forecast(date(2026, 11, 5), ["RUB"])
    assert found is not None
    [rubles_line] = found.currencies
    assert [(event.name, event.amount) for event in rubles_line.events] == [
        ("ЗП Питер", 100_000)
    ]


def test_the_month_plan_list_matches_after_last_months_rest() -> None:
    loaded = load_plan(Sheets({**BOTH, "fact": PAID_REST}), "Nov 26", date(2026, 11, 5))
    assert loaded is not None
    assert 13 not in loaded.receipts  # «ЗП Питер» ноября: 55 000 — октябрю
    alone = load_plan(
        Sheets({"fact": PAID_REST, "Nov 26": NOVEMBER_TAB}),
        "Nov 26",
        date(2026, 11, 5),
    )
    assert alone is not None and alone.receipts[13].received == 55_000


def test_the_advisor_sees_the_limit_with_last_months_rest() -> None:
    sheets = Sheets(
        {"fact": SHORT_SALARY, "Oct 26": OCTOBER_SALARY, "Nov 26": NOVEMBER_TAB}
    )
    gathered = gather_advice(
        sheets,
        NOVEMBER,
        date(2026, 11, 11),
        lambda plan: PlanChanges(),
        {"💰 ДОХОДЫ": ["зарплата"]},
    )
    assert gathered.context is not None and gathered.context.budget is not None
    assert gathered.context.budget.advances == 2_000_000
