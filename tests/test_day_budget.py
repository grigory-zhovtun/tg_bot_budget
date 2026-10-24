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
from app.services.analytics_service import AnalyticsService, transactions_frame
from app.services.calendar import HEADER as CALENDAR_HEADER
from app.services.day_budget import (
    ForecastItem,
    day_budget,
    format_day_budget,
    read_forecast,
)
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


def test_first_day_limit_lasts_until_the_advance() -> None:
    first = budget(date(2026, 10, 1))
    # 5 млн сумов на 14 дней до аванса 15-го: доллары и план в лимит не входят
    assert (first.sums, first.bills) == (5_000_000, 0)
    assert (first.payday, first.payday_name, first.days_left) == (
        date(2026, 10, 15),
        "Аванс",
        14,
    )
    assert first.limit == pytest.approx(5_000_000 / 14)
    assert first.plan_per_day == pytest.approx(100_000)
    assert first.yesterday_limit is None  # 30 сентября — прошлый месяц
    assert first.plan_gap is None


def test_spending_lowers_the_limit_until_the_advance() -> None:
    second = budget(
        date(2026, 10, 2), fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000)
    )
    assert second.limit == pytest.approx(4_600_000 / 13)
    assert second.yesterday_spent == pytest.approx(400_000)
    assert second.yesterday_limit == pytest.approx(5_000_000 / 14)
    # траты дня против плана: 400 000 при плане 100 000 в день
    assert second.plan_gap == pytest.approx(300_000)


def test_on_the_advance_day_before_it_comes_the_limit_lasts_until_the_salary() -> None:
    day = budget(date(2026, 10, 15))
    assert (day.payday, day.payday_name, day.days_left) == (
        date(2026, 10, 30),
        "ЗП",
        15,
    )
    assert day.limit == pytest.approx(5_000_000 / 15)


def test_the_advance_that_came_counts_from_the_next_morning() -> None:
    day = budget(date(2026, 10, 16), ADVANCE_PAID)
    assert day.limit == pytest.approx(7_000_000 / 14)


def test_after_the_last_salary_the_limit_lasts_until_next_months_advance() -> None:
    salary = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    last = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID, salary)
    assert (last.payday, last.payday_name, last.days_left) == (
        date(2026, 11, 15),
        "Аванс",
        15,
    )
    assert last.limit == pytest.approx(17_000_000 / 15)


def test_without_salaries_in_sums_the_limit_lasts_until_the_month_end() -> None:
    day = budget(date(2026, 10, 10), items=[("Квартплата", 18, -400, "USD")])
    assert (day.payday, day.payday_name, day.days_left) == (None, "", 22)
    assert day.limit == pytest.approx(5_000_000 / 22)


BILLS = [*ITEMS, ("Интернет", 10, -300_000, "UZS"), ("Кружок", 20, -500_000, "UZS")]


def test_list_payments_in_sums_before_the_salary_come_off_the_limit() -> None:
    day = budget(date(2026, 10, 5), items=BILLS)
    # интернет 10-го — до аванса 15-го, кружок 20-го — после
    assert day.bills == pytest.approx(300_000)
    assert day.limit == pytest.approx(4_700_000 / 10)
    paid = budget(
        date(2026, 10, 7),
        fact_row(date(2026, 10, 6), "🏚️ ДОМ", "интернет", 300_000),
        items=BILLS,
    )
    assert paid.bills == 0  # заплатили 6-го — строка закрыта
    late = budget(date(2026, 10, 12), items=BILLS)
    assert late.bills == pytest.approx(300_000)  # 10-е прошло, а не заплачено


CLOUD = [
    charge(date(2026, 8, 20), 50_000, "CLOUD STORAGE"),
    charge(date(2026, 9, 20), 50_000, "CLOUD STORAGE"),
]


def test_a_subscription_in_sums_before_the_salary_comes_off_the_limit() -> None:
    day = budget(date(2026, 10, 16), ADVANCE_PAID, *CLOUD)
    # 20-го спишется 50 000 — до зарплаты 30-го; сумы — 7 млн минус две оплаты
    assert day.bills == pytest.approx(50_000)
    assert day.limit == pytest.approx((6_900_000 - 50_000) / 14)


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
    assert last.bills == pytest.approx(30_000)  # 5 ноября — до аванса 15-го
    assert last.limit == pytest.approx((16_940_000 - 30_000) / 15)


def test_when_the_sums_do_not_cover_the_payments_there_is_no_limit() -> None:
    spree = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 6_000_000)
    assert budget(date(2026, 10, 2), spree).limit == pytest.approx(-1_000_000 / 13)


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
    # каждый день — сумы на вечер накануне на дни до аванса 15-го
    assert [day.limit for day in third.history] == pytest.approx(
        [5_000_000 / 14, 4_600_000 / 13, 4_550_000 / 12]
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
        pytest.approx(5_000_000 / 14),
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
    assert first.limit == pytest.approx(5_000_000 / 14)
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
    # доллары стали сумами: лимит до аванса вырос — (5 + 1,2) млн на 5 дней
    assert (calm.limit, took.limit) == (
        pytest.approx(5_000_000 / 5),
        pytest.approx(6_200_000 / 5),
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
        "💸 Можно тратить в день до аванса 15-го: 353 846 сум",
        "   на сумовых картах 4 600 000",
        "⚠️ Сверх плана на 300 000 за 1 день",
        "Вчера: 400 000 при лимите 357 143 — больше на 42 857",
        "🧊 Заморожено: 600,00 USD ≈ 7,20 млн",
        "💳 Сумы 4,60 млн — до конца месяца хватит",
        "🔴 Сверх плана: инвентарь 720 000 из 500 000",
        "📅 Впереди: 15.10 Аванс +2,00 млн • 18.10 Квартплата −400 USD",
    ]


def test_today_adds_what_is_spent_and_left() -> None:
    coffee = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 30_000)
    text = format_day_budget(budget(date(2026, 10, 2), OVERSPENT, coffee), now=True)
    assert "   потрачено 30 000 — осталось 323 846" in text.splitlines()


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


def test_a_one_off_record_in_sums_moves_the_sums_card() -> None:
    fact = [FACT_HEADER, *OPENING]
    camp = ["k7f2qa", "05.10.2026", "нет", "", "👶 ДЕТИ", "лагерь", "UZS", 6_000_000,
            "Лагерь", "Тест"]  # fmt: skip
    plain = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    planned = AnalyticsService(
        Sheets({"fact": fact, "Oct 26": tab(), "Календарь": [CALENDAR_HEADER, camp]})
    )
    day = date(2026, 10, 2)
    assert "💳 Сумы 5,00 млн — до конца месяца хватит" in (
        plain.morning_brief(day).text.splitlines()
    )
    # 5 млн − лагерь 6 млн 5-го: хватит до 04.10, аванс 15-го закрывает минус
    assert "💳 Сумы 5,00 млн — хватит до 04.10 · до 15.10 обменять ≈ $90" in (
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
    # зарплата уже на картах: 17 млн сумов на 16 дней до аванса 15 ноября
    assert day.limit == pytest.approx(17_000_000 / 16)
    assert day.upcoming == ()


def test_half_a_salary_a_week_early_leaves_only_the_rest_ahead() -> None:
    half = fact_row(date(2026, 10, 21), "💰 ДОХОДЫ", "зарплата", 5_000_000)
    day = budget(date(2026, 10, 22), ADVANCE_PAID, RENT_PAID, half)
    # половина уже на картах — впереди только вторая, лимит не считает её дважды
    [salary] = day.upcoming
    assert (salary.name, salary.amount) == ("ЗП", 5_000_000)


def test_a_late_salary_stays_ahead() -> None:
    day = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID)
    # опоздавшую зарплату лимит не ждёт: 7 млн сумов на 15 дней до аванса 15 ноября
    assert day.limit == pytest.approx(7_000_000 / 15)
    assert "📅 Впереди: ⏳ 30.10 ЗП +10,00 млн" in format_day_budget(day)


def test_a_salary_that_came_today_keeps_the_morning_limit() -> None:
    today = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    day = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, today)
    # лимит с утра: зарплаты на картах ещё нет — до следующей, аванса 15 ноября
    assert day.limit == pytest.approx(7_000_000 / 16)
    assert day.upcoming == ()  # а в «Впереди» её уже нет


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
    # сумы на вечер 30-го: 5 + 2 − 14 = −7 млн; зарплата 10 млн — сегодня
    assert late.runway is not None and late.runway.short == 0


def test_a_row_raised_after_it_came_opens_again() -> None:
    raised = [("Аванс", 15, 5_000_000, "UZS"), *ITEMS[1:]]
    day = budget(date(2026, 10, 16), ADVANCE_PAID, items=raised)
    assert [(item.name, item.amount) for item in day.ahead][0] == ("Аванс", 3_000_000)
