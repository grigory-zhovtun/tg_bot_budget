"""/plan и воскресная сводка: вкладка месяца → понятный текст без AI."""

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

from app.handlers import analytics
from app.services.analytics_service import (
    AnalyticsService,
    PlanLine,
    format_plan_report,
    plan_lines,
)
from tests.test_analytics_service import FACT, RATES

HEADER = ["Статья", "Категория", "Подкатегория", "План", "Факт", "Дельта", "Валюта",
          "Экв. план", "Экв. факт", "Экв. дельта", "Кол-во", "Среднее", 46296]  # fmt: skip


def tab_row(kind: str, group: str, sub: str, cur: str, plan: float, fact: float):
    """Строка вкладки месяца: план и факт уже в сумах (колонки H и I)."""
    return [kind, group, sub, plan, fact, fact - plan, cur, plan, fact, fact - plan]


# Как во вкладке «Oct 26»: расходы и доходы отрицательные
OCTOBER = [
    HEADER,
    tab_row("Расходы", "🏚️ ДОМ", "продукты", "UZS", -11_000_000, -3_084_653),
    tab_row("Расходы", "🏚️ ДОМ", "инвентарь", "UZS", -500_000, -503_700),
    tab_row("Расходы", "🍔 ЕДА", "кафе", "UZS", -1_000_000, -850_000),
    tab_row("Расходы", "🌛 ЕЖЕМЕСЯЧНО", "подписка", "UZS", -1_100_000, -419_725),
    tab_row("Расходы", "🌛 ЕЖЕМЕСЯЧНО", "подписка", "USD", 0, -2_706_593),
    tab_row("Расходы", "🎉 ПРАЗДНИК", "сезонный", "UZS", 0, -314_000),
    tab_row("Расходы", "✈️ ПОЕЗДКИ", "путешествие", "UZS", 0, 0),
    tab_row("Доходы", "💰 ДОХОДЫ", "зарплата", "UZS", -31_320_000, 0),
    ["", "", "", "", "", "", "", -45_000_000, -7_878_671],  # итоговая строка
]


def test_plan_lines_join_currencies_and_flip_the_sign() -> None:
    spend, earn = plan_lines(OCTOBER)
    by_name = {line.name: line for line in spend}
    assert by_name["🌛 ЕЖЕМЕСЯЧНО / подписка"] == PlanLine(
        "🌛 ЕЖЕМЕСЯЧНО", "подписка", plan=1_100_000, fact=3_126_318
    )
    assert by_name["🏚️ ДОМ / продукты"].fact == 3_084_653
    assert earn == [PlanLine("💰 ДОХОДЫ", "зарплата", plan=31_320_000, fact=0)]
    assert len(spend) == 6  # итоговая строка без статьи пропущена


def test_plan_report_shows_pace_overspend_and_what_is_left() -> None:
    text = format_plan_report(OCTOBER, date(2026, 10, 6))
    lines = text.splitlines()
    assert lines[0] == "📋 План-факт: октябрь 2026, прошло 6 из 31 дн. (19%)"
    # 3 084 653 + 503 700 + 850 000 + 3 126 318 + 314 000 = 7 878 671 — 58 % при 19 %
    assert "Расходы: 7 878 671 из 13 600 000 сум (58%) ⚠️ быстрее графика" in text
    assert "Осталось 5 721 329 сум ≈ 220 051 в день"  # 26 дней с сегодняшним in text
    over = lines.index("🔴 Сверх плана:")
    assert (
        lines[over + 1] == "• 🌛 ЕЖЕМЕСЯЧНО / подписка: 3 126 318 из 1 100 000 (284%)"
    )
    assert lines[over + 2] == "• 🏚️ ДОМ / инвентарь: 503 700 из 500 000 (101%)"
    assert "• 🍔 ЕДА / кафе: 850 000 из 1 000 000 (85%)" in text  # почти всё
    assert "⚪ Вне плана:\n• 🎉 ПРАЗДНИК / сезонный: 314 000" in text
    assert "путешествие" not in text
    assert text.endswith("Доходы: 0 из 31 320 000 сум (0%)")


def test_calm_month_and_overspent_month() -> None:
    calm = [HEADER, tab_row("Расходы", "🍔 ЕДА", "кофе", "UZS", -2_000_000, -100_000)]
    text = format_plan_report(calm, date(2026, 10, 15))
    assert "(5%) ✅ в графике" in text
    assert "Сверх плана" not in text and "Доходы" not in text

    spent = [HEADER, tab_row("Расходы", "🍔 ЕДА", "кофе", "UZS", -100_000, -150_000)]
    assert "План превышен на 50 000 сум" in format_plan_report(spent, date(2026, 10, 2))


class Sheets:
    def __init__(self, tabs: dict[str, list[list[Any]]]) -> None:
        self.tabs = tabs

    def get_values(self, name: str) -> list[list[Any]]:
        if name not in self.tabs:
            raise LookupError(name)
        return self.tabs[name]

    def get_rates(self):
        return RATES


def test_missing_month_tab_is_explained() -> None:
    service = AnalyticsService(Sheets({}))
    assert service.plan_report(date(2026, 11, 1)) == (
        "Вкладки «Nov 26» с планом нет — бот создаёт её 1-го числа."
    )


def test_weekly_digest_compares_weeks_and_appends_the_plan() -> None:
    service = AnalyticsService(Sheets({"fact": FACT, "Oct 26": OCTOBER}))
    text = service.weekly_digest(date(2026, 10, 6))
    first, second, third = text.splitlines()[:3]
    assert first == "🗓 Неделя 30.09–06.10"
    assert second.startswith("Потрачено ")
    assert third.startswith("Больше всего: ")
    assert "📋 План-факт: октябрь 2026" in text


async def test_plan_command_replies_with_the_report() -> None:
    service = SimpleNamespace(plan_report=lambda: "📋 План-факт")
    update = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
    context = SimpleNamespace(bot_data={"analytics_service": service})
    await analytics.plan_command(update, context)
    update.message.reply_text.assert_awaited_once_with("📋 План-факт")


async def test_plan_command_reports_errors_briefly() -> None:
    def broken() -> str:
        raise ConnectionError("Google is down")

    update = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
    context = SimpleNamespace(
        bot_data={"analytics_service": SimpleNamespace(plan_report=broken)}
    )
    await analytics.plan_command(update, context)
    assert update.message.reply_text.await_args.args[0] == (
        "Не удалось собрать план-факт: Google is down"
    )


async def test_weekly_digest_is_sent_and_errors_stay_in_the_log() -> None:
    bot = SimpleNamespace(send_message=AsyncMock())
    await analytics.send_weekly_digest(
        bot, 42, SimpleNamespace(weekly_digest=lambda: "🗓")
    )
    bot.send_message.assert_awaited_once_with(42, "🗓")

    def broken() -> str:
        raise ConnectionError("offline")

    bot = SimpleNamespace(send_message=AsyncMock())
    await analytics.send_weekly_digest(bot, 42, SimpleNamespace(weekly_digest=broken))
    bot.send_message.assert_not_awaited()
