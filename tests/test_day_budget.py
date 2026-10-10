"""Лимит на день: остаток на картах, жёлтый список вкладки месяца, заморозка, текст."""

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from gspread.exceptions import WorksheetNotFound

from app.domain import Rates
from app.handlers import analytics
from app.services.analytics_service import AnalyticsService, transactions_frame
from app.services.day_budget import (
    ForecastItem,
    day_budget,
    format_day_budget,
    read_forecast,
)
from tests.test_analytics_service import serial

RATES = Rates({"UZS": 1.0, "USD": 12000.0})
UZS_CARD, USD_CARD = "VISA 9120 UZS", "VISA 4058 USD"
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
    ("Квартплата", 18, -650, "USD"),
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

    План расходов 10,9 млн, из них квартплата 7,8 млн из жёлтого списка —
    остальные 3,1 млн делятся на 31 день: 100 000 в день.
    """
    rows: list[list[Any]] = [["Статья", "Категория", "Подкатегория"]]
    rows.append(plan_row("🍔 ЕДА", "кафе", 2_600_000, 0))
    rows.append(plan_row("🏚️ ДОМ", "инвентарь", 500_000, inventory_fact))
    rows.append(plan_row("🏚️ ДОМ", "квартплата", 7_800_000, 0))
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
        ForecastItem("Аванс", 15, 2_000_000, "UZS", 2_000_000),
        ForecastItem("ЗП", 30, 10_000_000, "UZS", 10_000_000),
        ForecastItem("Квартплата", 18, -650, "USD", -7_800_000),
    )
    assert forecast.savings_goal == 3_000_000  # таблица по дням ниже — не пункты
    assert read_forecast([["Статья"], ["Расходы", "🍔 ЕДА", "кафе"]]) is None


def test_first_day_limit_is_the_plan_per_day() -> None:
    first = budget(date(2026, 10, 1))
    assert first.limit == pytest.approx(100_000)
    assert first.plan_per_day == pytest.approx(100_000)
    assert first.days_left == 31
    assert first.yesterday_limit is None  # 30 сентября — прошлый месяц


def test_overspending_lowers_the_limit_for_the_rest_of_the_month() -> None:
    second = budget(
        date(2026, 10, 2), fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000)
    )
    # (16,6 млн + 4,2 млн по списку − 18,1 млн к концу месяца) / 30 дней
    assert second.limit == pytest.approx(90_000)
    assert second.yesterday_spent == pytest.approx(400_000)
    assert second.yesterday_limit == pytest.approx(100_000)
    assert (second.balance, second.planned_balance) == (16_600_000, 16_900_000)


def test_income_due_today_still_counts() -> None:
    # 14 дней без трат: запас 1,4 млн делится на оставшиеся 17 дней
    assert budget(date(2026, 10, 15)).limit == pytest.approx(3_100_000 / 17)


def test_payment_from_the_list_is_not_an_expense_of_the_day() -> None:
    rent = fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 650, "USD")
    cafe = fact_row(date(2026, 10, 18), "🍔 ЕДА", "кафе", 50_000)
    assert budget(date(2026, 10, 19), rent, cafe).yesterday_spent == pytest.approx(
        50_000
    )


def test_last_day_gets_everything_left() -> None:
    month = [
        fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000),
        fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 650, "USD"),
        fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000),
    ]
    last = budget(date(2026, 10, 31), *month)
    assert last.days_left == 1
    assert last.limit == pytest.approx(3_100_000)  # весь бюджет месяца не потрачен


@pytest.mark.parametrize(
    ("goal", "expected"),
    [
        (3_000_000, 1_200_000 / 31),  # цель выше плановой экономии 1,1 млн
        (500_000, 100_000),  # план и так откладывает больше
        (None, 100_000),
    ],
)
def test_savings_goal_above_the_plan_lowers_the_limit(
    goal: float | None, expected: float
) -> None:
    first = budget(date(2026, 10, 1), goal=goal)
    assert first.limit == pytest.approx(expected)
    assert first.growth == pytest.approx(1_100_000)


def test_frozen_is_the_dollar_card_without_dollar_payments_ahead() -> None:
    calm = budget(date(2026, 10, 10))
    assert (calm.frozen, calm.frozen_change) == (350, 0)  # 1000 − 650 на квартиру
    assert calm.frozen_uzs == pytest.approx(4_200_000)

    # 5 октября 100 USD перевели на сумовую карту: заморозка меньше, лимит тот же
    exchange = [
        fact_row(date(2026, 10, 5), "💳 СЧЕТА", "обмен валюты", 100, "USD"),
        fact_row(date(2026, 10, 5), "💳 СЧЕТА", "обмен валюты", -1_200_000),
    ]
    took = budget(date(2026, 10, 10), *exchange)
    assert (took.frozen, took.frozen_change) == (250, -100)
    assert took.limit == pytest.approx(calm.limit)
    assert "🧊 Заморожено: 250,00 USD ≈ 3,00 млн (с 1-го числа −100,00 USD)" in (
        format_day_budget(took)
    )

    paid = budget(
        date(2026, 10, 19),
        fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 650, "USD"),
    )
    assert paid.frozen == 350  # квартплату заплатили из отложенного на неё

    assert day_budget(frame(), tab(), date(2026, 10, 10), RATES, None).frozen is None


OVERSPENT = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000)


def test_morning_message() -> None:
    text = format_day_budget(
        budget(date(2026, 10, 2), OVERSPENT, inventory_fact=690_910)
    )
    assert text.splitlines() == [
        "☀️ Пятница, 2 октября",
        "💸 Можно потратить сегодня: 90 000 сум",
        "   по плану месяца — 100 000 в день",
        "Вчера: 400 000 при лимите 100 000 — больше на 300 000",
        "⚠️ Отстаём от плана на 300 000: на картах 16,60 млн, по плану 16,90 млн",
        "🧊 Заморожено: 350,00 USD ≈ 4,20 млн",
        "🔴 Сверх плана: инвентарь 690 910 из 500 000",
        "📅 Впереди: 15.10 Аванс +2,00 млн • 18.10 Квартплата −650 USD",
    ]


def test_today_adds_what_is_spent_and_left() -> None:
    coffee = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 30_000)
    text = format_day_budget(budget(date(2026, 10, 2), OVERSPENT, coffee), now=True)
    assert "   потрачено 30 000 — осталось 60 000" in text.splitlines()


def test_no_limit_left_and_goal_lines() -> None:
    spree = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 4_000_000)
    text = format_day_budget(budget(date(2026, 10, 2), spree))
    assert (
        "💸 Лимита на сегодня нет: до плана на конец месяца не хватает 900 000" in text
    )

    goal = format_day_budget(budget(date(2026, 10, 1), goal=3_000_000))
    assert (
        "🎯 Цель отложить 3,00 млн больше плановой экономии 1,10 млн — "
        "лимит ниже на 61 290 в день" in goal
    )
    covered = format_day_budget(budget(date(2026, 10, 1), goal=500_000))
    assert "🎯 Цель отложить 500 000: план её покрывает" in covered
    assert "Вчера: 0" in covered and "Отстаём" not in covered  # 1-е число


class Sheets:
    def __init__(self, tabs: dict[str, list[list[Any]]]) -> None:
        self.tabs = tabs

    def get_values(self, name: str) -> list[list[Any]]:
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def get_rates(self) -> Rates:
        return RATES


def test_service_builds_the_message_from_the_sheet() -> None:
    fact = [FACT_HEADER, *OPENING, OVERSPENT]
    service = AnalyticsService(
        Sheets({"fact": fact, "Oct 26": tab(inventory_fact=690_910)})
    )
    assert service.morning_brief(date(2026, 10, 2)) == format_day_budget(
        budget(date(2026, 10, 2), OVERSPENT, inventory_fact=690_910)
    )
    no_block = [row[:14] for row in tab()]
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": no_block}))
    text = service.morning_brief(date(2026, 10, 2))
    assert text.startswith("Во вкладке «Oct 26» нет списка поступлений и платежей")
    assert "📋 План-факт: октябрь 2026" in text
    assert AnalyticsService(Sheets({})).morning_brief(date(2026, 11, 1)) == (
        "Вкладки «Nov 26» с планом нет — бот создаёт её 1-го числа."
    )


def test_read_error_is_not_reported_as_a_missing_tab() -> None:
    class Flaky(Sheets):
        def get_values(self, name: str) -> list[list[Any]]:
            raise ConnectionError("Google is down")

    with pytest.raises(ConnectionError):
        AnalyticsService(Flaky({})).morning_brief(date(2026, 10, 2))


async def test_morning_brief_is_sent_and_errors_stay_in_the_log() -> None:
    bot = SimpleNamespace(send_message=AsyncMock())
    service = SimpleNamespace(morning_brief=lambda: "☀️")
    await analytics.send_morning_brief(bot, 42, service)
    bot.send_message.assert_awaited_once_with(42, "☀️")

    def broken() -> str:
        raise ConnectionError("Google is down")

    bot = SimpleNamespace(send_message=AsyncMock())
    await analytics.send_morning_brief(bot, 42, SimpleNamespace(morning_brief=broken))
    bot.send_message.assert_not_awaited()


async def test_today_command_shows_the_day_so_far() -> None:
    calls: list[dict[str, Any]] = []

    def brief(**options: Any) -> str:
        calls.append(options)
        return "💸"

    update = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
    context = SimpleNamespace(
        bot_data={"analytics_service": SimpleNamespace(morning_brief=brief)}
    )
    await analytics.today_command(update, context)
    update.message.reply_text.assert_awaited_once_with("💸")
    assert calls == [{"now": True}]
