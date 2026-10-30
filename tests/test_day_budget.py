"""Лимит на день: остаток на картах, жёлтый список вкладки месяца, заморозка, текст."""

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from gspread.exceptions import WorksheetNotFound

from app.domain import Rates
from app.handlers import analytics
from app.rich import Report
from app.services.analytics_service import (
    AnalyticsService,
    money,
    transactions_frame,
)
from app.services.calendar import HEADER as CALENDAR_HEADER
from app.services.calendar import read_calendar
from app.services.day_budget import (
    ForecastItem,
    _advance_days,
    _bill_days,
    _salaries,
    _subscription_days,
    day_budget,
    format_day_budget,
    read_forecast,
    today_line,
)
from app.services.receipts import match_receipts
from app.services.recurring import find_series, outside_list
from tests.test_analytics_service import serial

RATES = Rates({"UZS": 1.0, "USD": 12000.0})
UZS_CARD, USD_CARD = "VISA 9090 UZS", "VISA 3434 USD"
FACT_HEADER = ["Дата", "Категория", "Название", "Сумма", "Баланс", "Комментарий",
               "Валюта", "Источник"]  # fmt: skip


def fact_row(day: date, group: str, sub: str, amount: float, cur: str = "UZS"):
    source = USD_CARD if cur == "USD" else UZS_CARD
    return [serial(day), group, sub, amount, "", "", cur, source]


# На 1 октября: 5 млн на сумовой карте и 1000 USD (12 млн) на долларовой
OPENING = [
    fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 5_000_000),
    fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1000, "USD"),
]
ITEMS = [
    ("Аванс", 15, 2_000_000, "UZS"),
    ("ЗП", 30, 10_000_000, "UZS"),
    ("Квартплата", 18, -400, "USD"),
]


def frame(*rows: list[Any]):
    return transactions_frame([FACT_HEADER, *OPENING, *rows], RATES)


def plan_row(group: str, sub: str, plan: float, fact: float) -> list[Any]:
    """Строка вкладки месяца: план и факт в сумах (H и I) отрицательные."""
    return ["Расходы", group, sub, -plan, -fact, plan - fact, "UZS", -plan, -fact]


def tab(
    items=ITEMS, goal: float | None = None, inventory_fact: float = 0
) -> list[list[Any]]:
    """Вкладка «Oct 26»: план слева (A:I), блок прогноза справа (O:S).

    План расходов 7,9 млн, из них квартплата 4,8 млн из жёлтого списка —
    остальные 3,1 млн делятся на 31 день: 100 000 в день.
    """
    rows: list[list[Any]] = [["Статья", "Категория", "Подкатегория"]]
    rows.append(plan_row("🍔 ЕДА", "кафе", 2_600_000, 0))
    rows.append(plan_row("🏚️ ДОМ", "инвентарь", 500_000, inventory_fact))
    rows.append(plan_row("🏚️ ДОМ", "квартплата", 4_800_000, 0))
    rows += [[] for _ in range(30 - len(rows))]
    rows = [row + [""] * (19 - len(row)) for row in rows]
    rows[1][14:16] = ["Сейчас на картах (факт)", 16_600_000]
    rows[10][14:19] = ["Поступления (+) и крупные платежи (−)", "День", "Сумма",
                       "Валюта", "В сумах"]  # fmt: skip
    for k, (name, day, amount, cur) in enumerate(items):
        rows[11 + k][14:19] = [name, day, amount, cur, amount * RATES.to_uzs[cur]]
    if goal is not None:
        rows[19][14:19] = ["🎯 Отложить за месяц (цель накоплений)", "", goal, "UZS",
                           goal]  # fmt: skip
    rows[20][14:18] = ["Дата", "План", "Факт", "Разница"]
    rows[21][14:18] = [serial(date(2026, 10, 1)), 16_900_000, 16_600_000, -300_000]
    return rows


def budget(today: date, *rows: list[Any], **options: Any):
    return day_budget(frame(*rows), tab(**options), today, RATES, "USD")


def test_forecast_list_and_goal_are_read_from_the_block() -> None:
    forecast = read_forecast(tab(goal=3_000_000))
    assert forecast.items == (
        ForecastItem("Аванс", 15, 2_000_000, "UZS", 2_000_000, 12),
        ForecastItem("ЗП", 30, 10_000_000, "UZS", 10_000_000, 13),
        ForecastItem("Квартплата", 18, -400, "USD", -4_800_000, 14),
    )
    assert forecast.savings_goal == 3_000_000  # таблица по дням ниже — не пункты
    assert read_forecast([["Статья"], ["Расходы", "🍔 ЕДА", "кафе"]]) is None


def charge(day: date, amount: float, comment: str, cur: str = "UZS") -> list[Any]:
    """Списание подписки: по комментарию банка бот узнаёт серию."""
    found = fact_row(day, "🌛 ЕЖЕМЕСЯЧНО", "подписка", amount, cur)
    found[5] = comment
    return found


def test_first_day_limit_lasts_until_the_salary_with_the_advance() -> None:
    first = budget(date(2026, 10, 1))
    # 5 млн сумов и аванс 15-го 2 млн на 30 дней, по день ЗП 30-го: доллары и план
    # не входят
    assert (first.sums, first.bills) == (5_000_000, 0)
    assert (first.payday, first.payday_name, first.days_left) == (
        date(2026, 10, 30),
        "ЗП",
        30,
    )
    assert (first.advances, first.advance_on) == (2_000_000, date(2026, 10, 15))
    assert first.limit == pytest.approx(7_000_000 / 30)
    assert first.plan_per_day == pytest.approx(100_000)
    assert first.yesterday_limit is None  # 30 сентября — прошлый месяц
    assert first.plan_gap is None


def test_spending_lowers_the_limit_until_the_salary() -> None:
    second = budget(
        date(2026, 10, 2), fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000)
    )
    assert second.limit == pytest.approx(6_600_000 / 29)
    assert second.yesterday_spent == pytest.approx(400_000)
    assert second.yesterday_limit == pytest.approx(7_000_000 / 30)
    # траты дня против плана: 400 000 при плане 100 000 в день
    assert second.plan_gap == pytest.approx(300_000)


def test_on_the_advance_day_before_it_comes_the_limit_lasts_until_the_salary() -> None:
    day = budget(date(2026, 10, 15))
    assert (day.payday, day.payday_name, day.days_left) == (
        date(2026, 10, 30),
        "ЗП",
        16,
    )
    # аванс сегодня, денег ещё нет — он в деньгах срока: его день живём на то, что
    # есть (страховка — один сегодняшний день), а лимит не проседает на день
    assert (day.advances, day.advance_on) == (2_000_000, date(2026, 10, 15))
    assert day.limit == pytest.approx(7_000_000 / 16)


def test_the_limit_does_not_dip_on_the_advance_day() -> None:
    cafe = [
        fact_row(date(2026, 10, day), "🍔 ЕДА", "кафе", 200_000) for day in range(1, 15)
    ]
    day = budget(date(2026, 10, 16), *cafe, ADVANCE_PAID)
    # 14-го, 15-го и 16-го — одни и те же деньги: сумы и аванс на дни по ЗП
    assert [round(item.limit) for item in day.history[13:16]] == [
        round((5_000_000 - 2_600_000 + 2_000_000) / 17),
        round((5_000_000 - 2_800_000 + 2_000_000) / 16),
        round((5_000_000 - 2_800_000 + 2_000_000) / 15),
    ]


def test_only_the_rest_of_a_partly_paid_advance_is_added() -> None:
    # миллион аванса пришёл 10-го: 12-го в деньгах срока — только второй миллион
    part = fact_row(date(2026, 10, 10), "💰 ДОХОДЫ", "зарплата", 1_000_000)
    day = budget(date(2026, 10, 12), part)
    assert (day.advances, day.advance_on) == (1_000_000, date(2026, 10, 15))
    assert day.limit == pytest.approx((6_000_000 + 1_000_000) / 19)


def test_the_advance_that_came_counts_from_the_next_morning() -> None:
    day = budget(date(2026, 10, 16), ADVANCE_PAID)
    assert day.limit == pytest.approx(7_000_000 / 15)


def test_after_the_last_salary_the_limit_lasts_until_next_months_salary() -> None:
    salary = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    last = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID, salary)
    # 17 млн сумов и аванс 15 ноября 2 млн на 31 день, по 30 ноября
    assert (last.payday, last.payday_name, last.days_left) == (
        date(2026, 11, 30),
        "ЗП",
        31,
    )
    assert (last.advances, last.advance_on) == (2_000_000, date(2026, 11, 15))
    assert last.limit == pytest.approx(19_000_000 / 31)


def test_without_salaries_in_sums_the_limit_lasts_until_the_month_end() -> None:
    day = budget(date(2026, 10, 10), items=[("Квартплата", 18, -400, "USD")])
    assert (day.payday, day.payday_name, day.days_left) == (None, "", 22)
    assert day.limit == pytest.approx(5_000_000 / 22)


def test_only_a_salary_row_sets_the_limits_horizon() -> None:
    # возврат долга 3-го — поступление, а не зарплата: срок лимита — до ЗП 30-го,
    # и в деньги срока он не входит, пока не пришёл
    refund = [*ITEMS, ("Возврат долга", 3, 1_000_000, "UZS")]
    day = budget(date(2026, 10, 2), items=refund)
    assert (day.payday, day.payday_name) == (date(2026, 10, 30), "ЗП")
    assert day.limit == pytest.approx(7_000_000 / 29)
    salary = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    last = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID, salary, items=refund)
    assert (last.payday, last.payday_name) == (date(2026, 11, 30), "ЗП")


def test_the_guard_keeps_the_advance_money_until_it_comes() -> None:
    # ремонт 10-го на 4,6 млн: до аванса 15-го остаётся 400 000 на 15 дней — меньше,
    # чем ровный лимит до ЗП (2,4 млн на 30 дней), поэтому срок — до аванса
    repair = [*ITEMS, ("Ремонт", 10, -4_600_000, "UZS")]
    day = budget(date(2026, 10, 1), items=repair)
    assert (day.payday, day.payday_name, day.days_left) == (
        date(2026, 10, 15),
        "Аванс",
        15,
    )
    assert (day.advances, day.advance_on, day.bills) == (0, None, 4_600_000)
    assert day.limit == pytest.approx(400_000 / 15)
    # для формулы вкладки: Q — подписки по день ЗП, R — по день аванса, и при страховке
    cloud = [
        charge(date(2026, 8, 20), 50_000, "CLOUD STORAGE"),
        charge(date(2026, 9, 20), 50_000, "CLOUD STORAGE"),
    ]
    paper = [
        charge(date(2026, 8, 5), 30_000, "PAPER PRESS"),
        charge(date(2026, 9, 5), 30_000, "PAPER PRESS"),
    ]
    guarded = budget(date(2026, 10, 1), *cloud, *paper, items=repair)
    assert guarded.payday_name == "Аванс"
    assert (guarded.salary_subscriptions, guarded.advance_subscriptions) == (
        pytest.approx(80_000),
        pytest.approx(30_000),
    )


def test_without_a_salary_row_the_advance_sets_the_horizon() -> None:
    # в списке только аванс — он и срок, как до основной зарплаты, и в деньги не идёт
    day = budget(date(2026, 10, 1), items=[("Аванс", 15, 2_000_000, "UZS")])
    assert (day.payday, day.payday_name, day.days_left) == (
        date(2026, 10, 15),
        "Аванс",
        15,
    )
    assert (day.advances, day.advance_subscriptions) == (0, None)
    assert day.limit == pytest.approx(5_000_000 / 15)


def test_the_day_before_the_salary_covers_the_salary_day_too() -> None:
    # зарплату могут начислить и вечером: сумы делятся на сегодня и на её день
    day = budget(date(2026, 10, 29), ADVANCE_PAID, RENT_PAID)
    assert (day.payday, day.days_left) == (date(2026, 10, 30), 2)
    assert day.limit == pytest.approx(7_000_000 / 2)


def test_payments_on_the_salary_day_come_off_the_limit() -> None:
    # интернет и подписка 30-го спишутся раньше, чем придёт зарплата
    bills = [*ITEMS, ("Интернет", 30, -300_000, "UZS")]
    video = [
        charge(date(2026, 8, 30), 40_000, "VIDEO CLUB"),
        charge(date(2026, 9, 30), 40_000, "VIDEO CLUB"),
    ]
    day = budget(date(2026, 10, 29), ADVANCE_PAID, RENT_PAID, *video, items=bills)
    assert (day.bills, day.subscriptions) == (
        pytest.approx(340_000),
        pytest.approx(40_000),
    )
    assert day.limit == pytest.approx((6_920_000 - 340_000) / 2)


def test_next_months_advance_day_counts_too() -> None:
    salary = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    paper = [
        charge(date(2026, 9, 15), 20_000, "PAPER PRESS"),
        charge(date(2026, 10, 15), 20_000, "PAPER PRESS"),
    ]
    last = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID, salary, *paper)
    # с 31 октября по 30 ноября включительно; подписка 15 ноября — в день аванса:
    # она и в платежах срока, и в платежах страховки
    assert (last.payday, last.days_left) == (date(2026, 11, 30), 31)
    assert last.bills == pytest.approx(20_000)
    assert (last.salary_subscriptions, last.advance_subscriptions) == (
        pytest.approx(20_000),
        pytest.approx(20_000),
    )


BILLS = [*ITEMS, ("Интернет", 10, -300_000, "UZS"), ("Кружок", 20, -500_000, "UZS")]


def test_list_payments_in_sums_before_the_salary_come_off_the_limit() -> None:
    day = budget(date(2026, 10, 5), items=BILLS)
    # интернет 10-го и кружок 20-го — до ЗП 30-го; подписок нет; аванс 15-го — в деньги
    assert (day.bills, day.subscriptions) == (pytest.approx(800_000), 0)
    assert day.limit == pytest.approx((5_000_000 + 2_000_000 - 800_000) / 26)
    paid = budget(
        date(2026, 10, 7),
        fact_row(date(2026, 10, 6), "🏚️ ДОМ", "интернет", 300_000),
        items=BILLS,
    )
    assert paid.bills == pytest.approx(500_000)  # интернет заплатили 6-го — закрыт
    late = budget(date(2026, 10, 12), items=BILLS)
    assert late.bills == pytest.approx(800_000)  # 10-е прошло, а не заплачено


CLOUD = [
    charge(date(2026, 8, 20), 50_000, "CLOUD STORAGE"),
    charge(date(2026, 9, 20), 50_000, "CLOUD STORAGE"),
]


def test_a_subscription_in_sums_before_the_salary_comes_off_the_limit() -> None:
    day = budget(date(2026, 10, 16), ADVANCE_PAID, *CLOUD)
    # 20-го спишется 50 000 — до зарплаты 30-го; сумы — 7 млн минус две оплаты
    assert (day.bills, day.subscriptions) == (
        pytest.approx(50_000),
        pytest.approx(50_000),
    )
    assert day.limit == pytest.approx((6_900_000 - 50_000) / 15)


def test_a_paid_or_a_dollar_subscription_does_not() -> None:
    early = charge(date(2026, 10, 17), 50_000, "CLOUD STORAGE")
    assert budget(date(2026, 10, 18), ADVANCE_PAID, *CLOUD, early).bills == 0
    music = [
        charge(date(2026, 8, 22), 10, "MUSIC PLUS", "USD"),
        charge(date(2026, 9, 22), 10, "MUSIC PLUS", "USD"),
    ]
    assert budget(date(2026, 10, 16), ADVANCE_PAID, *music).bills == 0


def test_after_the_last_salary_next_months_subscriptions_come_off() -> None:
    news = [
        charge(date(2026, 9, 5), 30_000, "NEWS DAILY"),
        charge(date(2026, 10, 5), 30_000, "NEWS DAILY"),
    ]
    salary = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    last = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID, salary, *news)
    assert last.bills == pytest.approx(30_000)  # 5 ноября — до ЗП 30 ноября
    assert last.limit == pytest.approx((16_940_000 + 2_000_000 - 30_000) / 31)


def test_the_horizons_money_has_its_days() -> None:
    bills = [*ITEMS, ("Свет", 20, -300_000, "UZS"), ("Газ", 10, -100_000, "UZS")]
    items = read_forecast(tab(items=bills)).items
    fact = frame(*CLOUD)
    receipts = match_receipts(fact, items, date(2026, 10, 1), date(2026, 10, 15))
    day, until = date(2026, 10, 16), date(2026, 10, 31)
    # свет — в свой день; газ 10-го не оплачен — опоздал, ждём его сегодня
    assert _bill_days(items, receipts, day, until) == [
        (date(2026, 10, 16), 100_000),
        (date(2026, 10, 20), 300_000),
    ]
    series = outside_list(find_series(fact, day), items)
    assert _subscription_days(series, day, until) == [(date(2026, 10, 20), 50_000)]


def test_next_months_rows_come_in_full() -> None:
    items = read_forecast(tab(items=[*ITEMS, ("Свет", 20, -300_000, "UZS")])).items
    _, advances = _salaries(items)
    light = fact_row(date(2026, 10, 20), "🏚️ ДОМ", "свет", 300_000)
    fact = frame(ADVANCE_PAID, RENT_PAID, light)
    receipts = match_receipts(fact, items, date(2026, 10, 1), date(2026, 10, 29))
    day, until = date(2026, 10, 30), date(2026, 12, 1)
    # октябрьский аванс позади; ноябрьский — целиком
    assert _advance_days(advances, receipts, day, until) == [
        (date(2026, 11, 15), 2_000_000, "Аванс")
    ]
    # свет за октябрь оплачен; ноябрьский — целиком, в свой день
    assert _bill_days(items, receipts, day, until) == [(date(2026, 11, 20), 300_000)]


def test_when_the_sums_do_not_cover_the_payments_there_is_no_limit() -> None:
    spree = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 6_000_000)
    assert budget(date(2026, 10, 2), spree).limit == pytest.approx(-1_000_000 / 14)


def test_every_day_of_the_month_keeps_its_spending_and_limit() -> None:
    third = budget(
        date(2026, 10, 3),
        fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000),
        fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 50_000),
        fact_row(date(2026, 10, 3), "🍔 ЕДА", "кафе", 30_000),
    )
    assert [(day.day, day.spent) for day in third.history] == [
        (date(2026, 10, 1), 400_000),
        (date(2026, 10, 2), 50_000),
        (date(2026, 10, 3), 30_000),
    ]
    # каждый день — сумы на вечер накануне и аванс на дни по ЗП 30-го
    assert [day.limit for day in third.history] == pytest.approx(
        [7_000_000 / 30, 6_600_000 / 29, 6_550_000 / 28]
    )
    assert third.plan_gap == pytest.approx(250_000)  # 450 000 при плане 200 000
    # те же числа, что в утреннем сообщении: вчера против лимита и лимит на сегодня
    yesterday, today = third.history[-2:]
    assert (yesterday.spent, yesterday.limit) == (
        third.yesterday_spent,
        third.yesterday_limit,
    )
    assert (today.spent, today.limit) == (third.spent_today, third.limit)


def test_on_the_first_the_history_is_only_today() -> None:
    [today] = budget(date(2026, 10, 1)).history
    assert (today.day, today.spent, today.limit) == (
        date(2026, 10, 1),
        0,
        pytest.approx(7_000_000 / 30),
    )


def test_payment_from_the_list_is_not_an_expense_of_the_day() -> None:
    rent = fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 400, "USD")
    cafe = fact_row(date(2026, 10, 18), "🍔 ЕДА", "кафе", 50_000)
    assert budget(date(2026, 10, 19), rent, cafe).yesterday_spent == pytest.approx(
        50_000
    )


@pytest.mark.parametrize("goal", [6_000_000, 500_000, None])
def test_the_savings_goal_does_not_change_the_limit(goal: float | None) -> None:
    first = budget(date(2026, 10, 1), goal=goal)
    assert first.limit == pytest.approx(7_000_000 / 30)
    assert first.growth == pytest.approx(4_100_000)


def test_frozen_is_the_dollar_card_without_dollar_payments_ahead() -> None:
    calm = budget(date(2026, 10, 10))
    assert (calm.frozen, calm.frozen_change) == (600, 0)  # 1000 − 400 на квартиру
    assert calm.frozen_uzs == pytest.approx(7_200_000)

    # 5 октября 100 USD перевели на сумовую карту: заморозка меньше, лимит тот же
    exchange = [
        fact_row(date(2026, 10, 5), "💳 СЧЕТА", "обмен валюты", 100, "USD"),
        fact_row(date(2026, 10, 5), "💳 СЧЕТА", "обмен валюты", -1_200_000),
    ]
    took = budget(date(2026, 10, 10), *exchange)
    assert (took.frozen, took.frozen_change) == (500, -100)
    # доллары стали сумами: лимит до ЗП вырос — (5 + 1,2 + аванс 2) млн на 21 день
    assert (calm.limit, took.limit) == (
        pytest.approx(7_000_000 / 21),
        pytest.approx(8_200_000 / 21),
    )
    assert "🧊 Заморожено: 500,00 USD ≈ 6,00 млн (с 1-го числа −100,00 USD)" in (
        format_day_budget(took)
    )

    paid = budget(
        date(2026, 10, 19),
        fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 400, "USD"),
    )
    assert paid.frozen == 600  # квартплату заплатили из отложенного на неё

    assert day_budget(frame(), tab(), date(2026, 10, 10), RATES, None).frozen is None


OVERSPENT = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000)


def test_morning_message() -> None:
    text = format_day_budget(
        budget(date(2026, 10, 2), OVERSPENT, inventory_fact=720_000)
    )
    assert text.splitlines() == [
        "☀️ Пятница, 2 октября",
        "💸 Можно тратить в день до зарплаты 30-го: 227 586 сум",
        "   на сумовых картах 4 600 000, аванс 15-го +2 000 000",
        "⚠️ Сверх плана на 300 000 за 1 день",
        "Вчера: 400 000 при лимите 233 333 — больше на 166 667",
        "🧊 Заморожено: 600,00 USD ≈ 7,20 млн",
        "💳 Сумы 4,60 млн — до зарплаты 30-го хватит, останется ≈ 5,77 млн",
        "🔴 Сверх плана: инвентарь 720 000 из 500 000",
        "📅 Впереди: 15.10 Аванс +2,00 млн • 18.10 Квартплата −400 USD",
    ]


def test_today_adds_what_is_spent_and_left() -> None:
    coffee = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 30_000)
    text = format_day_budget(budget(date(2026, 10, 2), OVERSPENT, coffee), now=True)
    assert "   потрачено 30 000 — осталось 197 586" in text.splitlines()


def test_no_limit_left_and_goal_lines() -> None:
    spree = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 6_000_000)
    text = format_day_budget(budget(date(2026, 10, 2), spree))
    assert "💸 Лимита на сегодня нет: до аванса 15-го не хватает 1 000 000" in text

    goal = format_day_budget(budget(date(2026, 10, 1), goal=6_000_000))
    # лимит от цели больше не зависит — про лимит в строке цели ни слова
    assert "🎯 Цель отложить 6,00 млн больше плановой экономии 4,10 млн" in (
        goal.splitlines()
    )
    covered = format_day_budget(budget(date(2026, 10, 1), goal=500_000))
    assert "🎯 Цель отложить 500 000: план её покрывает" in covered
    assert "Вчера: 0" in covered  # 1-е число: ни строки плана, ни «Отстаём»
    assert "Сверх плана на" not in covered and "Отстаём" not in covered


class Sheets:
    def __init__(self, tabs: dict[str, list[list[Any]]]) -> None:
        self.tabs = tabs
        self.ages: list[float] = []  # max_age каждого чтения

    def get_values(self, name: str, max_age: float = 0.0) -> list[list[Any]]:
        self.ages.append(max_age)
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def get_rates(self, max_age: float = 0.0) -> Rates:
        self.ages.append(max_age)
        return RATES

    def get_calendar(self, max_age: float = 60.0) -> list[list[Any]]:
        self.ages.append(max_age)
        return self.tabs.get("Календарь", [])


def test_service_builds_the_message_from_the_sheet() -> None:
    fact = [FACT_HEADER, *OPENING, OVERSPENT]
    service = AnalyticsService(
        Sheets({"fact": fact, "Oct 26": tab(inventory_fact=720_000)})
    )
    assert service.morning_brief(date(2026, 10, 2)).text == format_day_budget(
        budget(date(2026, 10, 2), OVERSPENT, inventory_fact=720_000)
    )
    no_block = [row[:14] for row in tab()]
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": no_block}))
    text = service.morning_brief(date(2026, 10, 2)).text
    assert text.startswith("Во вкладке «Oct 26» нет списка поступлений и платежей")
    assert "📋 План-факт: октябрь 2026" in text
    assert AnalyticsService(Sheets({})).morning_brief(date(2026, 11, 1)).text == (
        "Вкладки «Nov 26» с планом нет — бот создаёт её 1-го числа."
    )


def test_a_one_off_record_keeps_its_payment_out_of_the_sums_pace() -> None:
    paid = fact_row(date(2026, 10, 5), "👶 ДЕТИ", "лагерь", 3_500_000)
    fact = [FACT_HEADER, *OPENING, paid]
    camp =["k7f2qa", "05.10.2026", "нет", "", "👶 ДЕТИ", "лагерь", "UZS", 3_500_000,
            "Лагерь", "Тест"]  # fmt: skip
    plain = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    planned = AnalyticsService(
        Sheets({"fact": fact, "Oct 26": tab(), "Календарь": [CALENDAR_HEADER, camp]})
    )
    day = date(2026, 10, 10)
    # без записи лагерь — обычная трата: темп 250 000 в день, до ЗП сумов не хватит
    assert "💳 Сумы 1,50 млн — хватит до 23.10 · до зарплаты 30-го обменять ≈ $150" in (
        plain.morning_brief(day).text.splitlines()
    )
    # платёж закрыл разовую запись календаря — в темп он не входит
    assert "💳 Сумы 1,50 млн — до зарплаты 30-го хватит, останется ≈ 3,50 млн" in (
        planned.morning_brief(day).text.splitlines()
    )


def test_a_one_off_record_in_sums_moves_the_sums_card() -> None:
    # запланированная разовая трата — отток в свой день, как в линии «по темпу»
    # «Календаря»: иначе «Сумы» скажут «хватит», а «Календарь» покажет минус
    fact = [FACT_HEADER, *OPENING]
    camp = ["k7f2qa", "05.10.2026", "нет", "", "👶 ДЕТИ", "лагерь", "UZS", 6_000_000,
            "Лагерь", "Тест"]  # fmt: skip
    plain = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    planned = AnalyticsService(
        Sheets({"fact": fact, "Oct 26": tab(), "Календарь": [CALENDAR_HEADER, camp]})
    )
    day = date(2026, 10, 2)
    assert "💳 Сумы 5,00 млн — до зарплаты 30-го хватит, останется ≈ 7,00 млн" in (
        plain.morning_brief(day).text.splitlines()
    )
    # 5 млн − лагерь 6 млн 5-го: хватит до 04.10, аванс 15-го закрывает минус
    assert "💳 Сумы 5,00 млн — хватит до 04.10 · до зарплаты 30-го обменять ≈ $90" in (
        planned.morning_brief(day).text.splitlines()
    )


def test_read_error_is_not_reported_as_a_missing_tab() -> None:
    class Flaky(Sheets):
        def get_values(self, name: str, max_age: float = 0.0) -> list[list[Any]]:
            raise ConnectionError("Google is down")

    with pytest.raises(ConnectionError):
        AnalyticsService(Flaky({})).morning_brief(date(2026, 10, 2))


async def test_morning_brief_is_sent_and_errors_stay_in_the_log() -> None:
    bot = SimpleNamespace(send_message=AsyncMock())
    service = SimpleNamespace(morning_message=lambda: (Report("☀️"), False))
    await analytics.send_morning_brief(bot, 42, service)
    bot.send_message.assert_awaited_once_with(42, "☀️")

    def broken() -> tuple[str, bool]:
        raise ConnectionError("Google is down")

    bot = SimpleNamespace(send_message=AsyncMock())
    await analytics.send_morning_brief(bot, 42, SimpleNamespace(morning_message=broken))
    bot.send_message.assert_not_awaited()


async def test_today_command_shows_the_day_so_far() -> None:
    calls: list[dict[str, Any]] = []

    def brief(**options: Any) -> Report:
        calls.append(options)
        return Report("💸")

    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=42),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )
    bot = SimpleNamespace(send_message=AsyncMock())
    context = SimpleNamespace(
        bot=bot, bot_data={"analytics_service": SimpleNamespace(morning_brief=brief)}
    )
    await analytics.today_command(update, context)
    bot.send_message.assert_awaited_once_with(42, "💸")
    assert calls == [{"now": True}]


ADVANCE_PAID = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000)
RENT_PAID = fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 400, "USD")


def test_an_early_salary_is_not_counted_twice() -> None:
    early = fact_row(date(2026, 10, 29), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    day = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, early)
    # зарплата уже на картах: 17 млн сумов и аванс 15 ноября на 32 дня по 30 ноября
    assert day.limit == pytest.approx(19_000_000 / 32)
    assert day.upcoming == ()


def test_half_a_salary_a_week_early_leaves_only_the_rest_ahead() -> None:
    half = fact_row(date(2026, 10, 21), "💰 ДОХОДЫ", "зарплата", 5_000_000)
    day = budget(date(2026, 10, 22), ADVANCE_PAID, RENT_PAID, half)
    # половина уже на картах — впереди только вторая, лимит не считает её дважды
    [salary] = day.upcoming
    assert (salary.name, salary.amount) == ("ЗП", 5_000_000)


def test_a_late_salary_stays_ahead() -> None:
    day = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID)
    # опоздавшую зарплату лимит не ждёт: 7 млн сумов и аванс 15 ноября на 31 день по
    # зарплату 30 ноября
    assert day.limit == pytest.approx(9_000_000 / 31)
    assert "📅 Впереди: ⏳ 30.10 ЗП +10,00 млн" in format_day_budget(day)


def test_a_salary_that_came_today_keeps_the_morning_limit() -> None:
    today = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    day = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, today)
    # лимит с утра: зарплаты на картах ещё нет — до следующей, 30 ноября, с авансом
    assert day.limit == pytest.approx(9_000_000 / 32)
    assert day.upcoming == ()  # а в «Впереди» её уже нет


SALARY_TODAY = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)


def test_on_the_salary_day_the_limit_runs_from_tomorrow() -> None:
    cafe = fact_row(date(2026, 10, 30), "🍔 ЕДА", "кафе", 300_000)  # после зарплаты
    day = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, SALARY_TODAY, cafe)
    assert day.tomorrow is not None
    # с 31 октября по 30 ноября: сумы на сейчас и аванс 15 ноября
    assert (day.tomorrow.payday, day.tomorrow.days) == (date(2026, 11, 30), 31)
    assert day.tomorrow.sums == 16_700_000  # по 30.10 включительно, с кафе
    assert (day.tomorrow.advances, day.tomorrow.advance_on) == (
        2_000_000,
        date(2026, 11, 15),
    )
    assert day.tomorrow.limit == pytest.approx(18_700_000 / 31)
    assert (day.salary_came, day.salary_name) == (10_000_000, "ЗП")
    assert day.limit == pytest.approx(9_000_000 / 32)  # утренний лимит — прежний


def test_a_salary_on_the_last_day_runs_to_next_months_salary() -> None:
    advance = fact_row(date(2026, 11, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000)
    salary = fact_row(date(2026, 11, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    day = budget(date(2026, 11, 30), advance, salary)
    assert day.tomorrow is not None
    # 1 декабря — строки вкладки на те же дни декабря, ещё не пришли: с 1 по 30 декабря
    # и аванс 15 декабря, а не до ЗП 30 января
    assert (day.tomorrow.payday, day.tomorrow.days) == (date(2026, 12, 30), 30)
    assert (day.tomorrow.advances, day.tomorrow.advance_on) == (
        2_000_000,
        date(2026, 12, 15),
    )
    assert day.tomorrow.limit == pytest.approx(19_000_000 / 30)


def test_a_salary_from_yesterday_is_the_morning_limit() -> None:
    assert budget(date(2026, 10, 31), ADVANCE_PAID, SALARY_TODAY).tomorrow is None
    # записали сегодня вчерашней датой: к утру она уже пришла — лимит утра её считает
    early = fact_row(date(2026, 10, 29), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    day = budget(date(2026, 10, 30), ADVANCE_PAID, early)
    assert day.tomorrow is None
    assert day.limit == pytest.approx(19_000_000 / 32)


def test_no_tomorrow_until_the_salary_comes() -> None:
    assert budget(date(2026, 10, 30), ADVANCE_PAID).tomorrow is None
    half = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 5_000_000)
    day = budget(date(2026, 10, 30), ADVANCE_PAID, half)
    assert (day.tomorrow, day.salary_came, day.salary_name) == (None, 0, "")


def test_the_salary_in_two_parts_today_runs_from_tomorrow() -> None:
    parts = [
        fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 6_000_000),
        fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 4_000_000),
    ]
    day = budget(date(2026, 10, 30), ADVANCE_PAID, *parts)
    assert day.tomorrow is not None
    assert day.salary_came == 10_000_000


def test_two_salaries_a_month_run_from_tomorrow_to_the_next_one() -> None:
    twice = [("ЗП", 15, 5_000_000, "UZS"), ("ЗП", 30, 5_000_000, "UZS")]
    paid = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 5_000_000)
    day = budget(date(2026, 10, 15), paid, items=twice)
    assert day.tomorrow is not None
    assert (day.tomorrow.payday, day.tomorrow.days) == (date(2026, 10, 30), 15)
    assert day.salary_came == 5_000_000


def test_without_a_main_salary_the_advance_runs_from_tomorrow() -> None:
    only = [("Аванс", 15, 2_000_000, "UZS")]
    day = budget(date(2026, 10, 15), ADVANCE_PAID, items=only)
    assert day.tomorrow is not None
    assert (day.tomorrow.payday, day.tomorrow.payday_name) == (
        date(2026, 11, 15),
        "Аванс",
    )
    assert (day.salary_came, day.salary_name) == (2_000_000, "Аванс")


AFTER_SALARY = fact_row(date(2026, 10, 30), "🍔 ЕДА", "кафе", 300_000)


def test_today_on_the_salary_day_speaks_of_tomorrow() -> None:
    day = budget(
        date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, SALARY_TODAY, AFTER_SALARY
    )
    # 18,7 млн на 31 день; сверх лимита дня не пишем — утренний лимит устарел
    assert format_day_budget(day, now=True).splitlines()[1:4] == [
        "💸 С завтра можно 603 226 в день до зарплаты 30 ноября",
        "   ✓ пришла ЗП 10 000 000, потрачено сегодня 300 000",
        "   на сумовых картах сейчас 16 700 000, аванс 15 ноября +2 000 000",
    ]
    # утро — прежнее: лимит с утра и сумы на вечер вчера
    assert format_day_budget(day).splitlines()[1:3] == [
        "💸 Можно тратить в день до зарплаты 30 ноября: 281 250 сум",
        "   на сумовых картах 7 000 000, аванс 15 ноября +2 000 000",
    ]


def test_no_limit_from_tomorrow_keeps_the_no_limit_line() -> None:
    spree = fact_row(date(2026, 10, 30), "🍔 ЕДА", "кафе", 20_000_000)
    day = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, SALARY_TODAY, spree)
    # сумы −3 млн: до аванса 15 ноября не хватает 3 млн на 16 дней
    assert format_day_budget(day, now=True).splitlines()[1:4] == [
        "💸 Лимита на сегодня нет: до аванса 15 ноября не хватает 3 000 000",
        "   ✓ пришла ЗП 10 000 000, потрачено сегодня 20 000 000",
        "   на сумовых картах сейчас -3 000 000",
    ]
    assert today_line(day) == (
        "💸 Лимита на сегодня нет: до аванса 15 ноября не хватает 3 000 000"
    )


def test_a_record_on_the_salary_day_says_tomorrows_limit() -> None:
    day = budget(
        date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, SALARY_TODAY, AFTER_SALARY
    )
    assert today_line(day) == "💸 С завтра можно 603 226 в день до зарплаты 30 ноября"
    # в другой день — как раньше
    calm = budget(date(2026, 10, 29), ADVANCE_PAID, RENT_PAID)
    assert today_line(calm).startswith("💸 На сегодня осталось")


def test_a_payment_a_bit_off_its_sum_does_not_eat_the_day() -> None:
    rent = fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 390, "USD")
    cafe = fact_row(date(2026, 10, 18), "🍔 ЕДА", "кафе", 50_000)
    assert budget(date(2026, 10, 19), rent, cafe).yesterday_spent == pytest.approx(
        50_000
    )


def test_rent_paid_before_its_day_leaves_the_frozen_dollars() -> None:
    early = fact_row(date(2026, 10, 16), "🏚️ ДОМ", "квартплата", 400, "USD")
    assert budget(date(2026, 10, 17), early).frozen == 600


def test_sums_wait_for_a_late_salary_today() -> None:
    cafe = [
        fact_row(date(2026, 10, day), "🍔 ЕДА", "кафе", 1_000_000)
        for day in range(17, 31)
    ]
    late = budget(date(2026, 10, 31), ADVANCE_PAID, *cafe)
    # опоздавшую ЗП ждём сегодня: срок «Сумы» кончается сегодня, а сама она не
    # прибавляется — сумы 5 + 2 − 14 = −7 млн и темп 1 млн в день
    assert late.runway is not None
    assert (late.runway.payday, late.runway.short) == (date(2026, 10, 31), 8_000_000)
    # утром в день ЗП, до денег, — тоже до сегодня
    morning = budget(date(2026, 10, 30), ADVANCE_PAID)
    assert morning.runway is not None
    assert morning.runway.payday == date(2026, 10, 30)


def test_after_the_salary_the_sums_run_to_the_next_one() -> None:
    day = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, SALARY_TODAY)
    # ЗП пришла: срок — до ЗП 30 ноября, аванс 15 ноября — в свой день
    assert day.runway is not None
    assert (day.runway.payday, day.runway.short) == (date(2026, 11, 30), 0)
    assert day.runway.left == 17_000_000 + 2_000_000


def test_the_sums_card_takes_the_payments_on_their_days() -> None:
    # интернет 10-го не оплачен — ждём сегодня; кружок и подписка — 20-го
    day = budget(date(2026, 10, 16), ADVANCE_PAID, *CLOUD, items=BILLS)
    assert day.runway is not None
    assert day.runway.left == 6_900_000 - 300_000 - 500_000 - 50_000
    # подписку списали сегодня: она уже в сумах сейчас, второй раз не вычитается
    paid = charge(date(2026, 10, 20), 50_000, "CLOUD STORAGE")
    later = budget(date(2026, 10, 20), ADVANCE_PAID, *CLOUD, paid, items=BILLS)
    assert later.runway is not None
    assert later.runway.left == 6_850_000 - 300_000 - 500_000


def test_a_row_raised_after_it_came_opens_again() -> None:
    raised = [("Аванс", 15, 5_000_000, "UZS"), *ITEMS[1:]]
    day = budget(date(2026, 10, 16), ADVANCE_PAID, items=raised)
    assert [(item.name, item.amount) for item in day.ahead][0] == ("Аванс", 3_000_000)


# --- финальное ревью этапа 1б ---

TWO_SALARIES = [
    ("ЗП", 15, 5_000_000, "UZS"),
    ("ЗП", 30, 5_000_000, "UZS"),
    ("Квартплата", 18, -400, "USD"),
]


def test_two_salary_rows_closed_by_one_payment_add_up() -> None:
    both = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    day = budget(date(2026, 10, 15), both, items=TWO_SALARIES)
    assert day.tomorrow is not None
    assert day.salary_came == 10_000_000


def test_a_late_salary_with_part_of_the_money_does_not_hold_the_sums_today() -> None:
    part = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 4_250_000)  # 85 %
    day = budget(date(2026, 10, 20), part, items=TWO_SALARIES)
    # как лимит: часть пришла — ждём следующую ЗП, а не «сегодня» две недели
    assert day.runway is not None and day.runway.payday == date(2026, 10, 30)
    late = budget(date(2026, 10, 20), items=TWO_SALARIES)  # денег нет вовсе
    assert late.runway is not None and late.runway.payday == date(2026, 10, 20)


DAILY_LUNCH = ["d4ly01", "01.10.2026", "каждый день", "", "🍔 ЕДА", "кафе", "UZS",
               100_000, "Обед", "Тест"]  # fmt: skip


def test_a_repeat_record_paid_today_is_not_taken_off_the_sums_twice() -> None:
    lunches = [
        fact_row(date(2026, 10, day), "🍔 ЕДА", "кафе", 100_000) for day in range(1, 11)
    ]
    today = date(2026, 10, 10)
    plain = day_budget(frame(*lunches), tab(), today, RATES, "USD")
    planned = day_budget(
        frame(*lunches),
        tab(),
        today,
        RATES,
        "USD",
        read_calendar([CALENDAR_HEADER, DAILY_LUNCH]),
    )
    assert plain is not None and planned is not None
    assert plain.runway is not None and planned.runway is not None
    assert planned.runway.left == pytest.approx(plain.runway.left)


CIRCLE = [
    ["k1rz09", "09.10.2026", "нет", "", "👶 ДЕТИ", "кружок", "UZS", 300_000, "Кружок",
     "Тест"],
    ["k1rz10", "10.10.2026", "нет", "", "👶 ДЕТИ", "кружок", "UZS", 300_000, "Кружок",
     "Тест"],
]  # fmt: skip


def test_yesterdays_payment_closes_yesterdays_record_not_todays() -> None:
    # как «Календарь»: кружок 9-го оплачен, кружок 10-го ещё впереди
    paid = fact_row(date(2026, 10, 9), "👶 ДЕТИ", "кружок", 300_000)
    day = day_budget(
        frame(paid),
        tab(),
        date(2026, 10, 10),
        RATES,
        "USD",
        read_calendar([CALENDAR_HEADER, *CIRCLE]),
    )
    assert day is not None and day.runway is not None
    # 5 млн − 300 000 (9-го) − 300 000 (сегодня) + аванс 15-го 2 млн
    assert day.runway.left == pytest.approx(6_400_000)


def test_an_advance_in_place_of_the_salary_says_it_came() -> None:
    items = [("Аванс", 15, 2_000_000, "UZS"), ("Квартплата", 18, -400, "USD")]
    came = budget(date(2026, 10, 15), ADVANCE_PAID, items=items)
    assert came.tomorrow is not None
    assert (
        "   ✓ пришёл аванс 2 000 000, потрачено сегодня 0"
        in format_day_budget(came, now=True).splitlines()
    )


def test_the_salary_reply_says_tomorrows_limit() -> None:
    fact = [FACT_HEADER, *OPENING, ADVANCE_PAID, RENT_PAID, SALARY_TODAY]
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    found = service.write_feedback([], today=date(2026, 10, 30), salary_day_only=True)
    # 17 млн на сумовых картах и аванс 15 ноября 2 млн — на 31 день
    limit = money(19_000_000 / 31)
    assert found.lines == [f"💸 С завтра можно {limit} в день до зарплаты 30 ноября"]
    # в другой день ответ на доход лимита не добавляет
    other = service.write_feedback([], today=date(2026, 10, 29), salary_day_only=True)
    assert other.lines == []
