"""Недельный отчёт и /plan расширенным сообщением: план-факт крупно и таблицами."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.handlers import analytics
from app.rich import NBSP, RULE, Report, heading, para, percent, table, whole
from app.services.analytics_service import (
    AnalyticsService,
    rich_plan_report,
    week_title,
)
from tests.test_analytics_service import FACT
from tests.test_plan import HEADER, Sheets, tab_row

pytestmark = pytest.mark.usefixtures("rich")


HEAD = ["статья", "факт", "план"]
# выдуманная вкладка месяца: план и факт уже в сумах, как во вкладке — со знаком «−»
FALL = [
    HEADER,
    tab_row("Расходы", "🏚️ ДОМ", "продукты", "UZS", -10_000_000, -2_500_000),
    tab_row("Расходы", "🏚️ ДОМ", "инвентарь", "UZS", -400_000, -410_000),
    tab_row("Расходы", "🌛 ЕЖЕМЕСЯЧНО", "подписка", "UZS", -1_000_000, -2_000_000),
    tab_row("Расходы", "🍔 ЕДА", "кафе", "UZS", -1_000_000, -900_000),
    tab_row("Расходы", "🎉 ПРАЗДНИК", "сезонный", "UZS", 0, -250_000),
    tab_row("Доходы", "💰 ДОХОДЫ", "зарплата", "UZS", -30_000_000, 0),
]


def test_plan_blocks_follow_the_mockup() -> None:
    # 6 060 000 из 12 400 000 — 49 % при 19 % месяца; осталось 6 340 000 на 26 дней
    assert rich_plan_report(FALL, date(2026, 10, 6)) == [
        heading("📋 Октябрь: 6 из 31 дня", 2),
        heading(f"⚠️ Расходы {whole(6_060_000)}"),
        para(
            f"из {whole(12_400_000)} — {percent(0.49)}",
            f"быстрее графика: прошло {percent(0.19)} месяца",
        ),
        heading(f"Осталось {whole(6_340_000)}"),
        para(f"≈{NBSP}{whole(243_846)} в день"),
        heading("🔴 Сверх плана"),
        table(
            [
                ["подписка", whole(2_000_000), whole(1_000_000)],
                ["инвентарь", whole(410_000), whole(400_000)],
            ],
            "lrr",
            HEAD,
        ),
        heading(f"🟡 Почти всё (от {percent(0.8)})"),
        table([["кафе", whole(900_000), whole(1_000_000)]], "lrr", HEAD),
        heading("⚪ Вне плана"),
        table([["🎉 сезонный", whole(250_000)]], "lr"),
        heading(f"💰 Доходы {whole(0)}"),
        para(f"из {whole(30_000_000)} — {percent(0)} плана"),
    ]


def test_days_word_and_a_month_without_a_plan() -> None:
    november = rich_plan_report(FALL, date(2026, 11, 3))
    assert november[0] == heading("📋 Ноябрь: 3 из 30 дней", 2)
    empty = [HEADER, tab_row("Расходы", "🍔 ЕДА", "кафе", "UZS", 0, -50_000)]
    blocks = rich_plan_report(empty, date(2026, 10, 6))
    assert blocks[1:3] == [
        heading(f"Расходы {whole(50_000)}"),
        para("плана на месяц нет"),
    ]
    assert not any("Осталось" in block or "превышен" in block for block in blocks)


def test_week_titles() -> None:
    assert week_title(date(2026, 10, 12), date(2026, 10, 18)) == "12–18 октября"
    assert week_title(date(2026, 9, 28), date(2026, 10, 4)) == "28 сентября – 4 октября"


def test_weekly_report_keeps_the_text_and_adds_the_blocks() -> None:
    service = AnalyticsService(Sheets({"fact": FACT, "Oct 26": FALL}))
    report = service.weekly_digest(date(2026, 10, 6))
    assert report.text.splitlines()[0] == "🗓 Неделя 30.09–06.10"
    assert report.html.startswith(para("🗓 Неделя 30 сентября – 6 октября") + "<h1>")
    assert report.html.count(RULE) == 2
    assert "".join(rich_plan_report(FALL, date(2026, 10, 6))) in report.html
    reminder = "📸 Пришлите скрины главных экранов банков — сверю остатки по картам."
    assert report.html.endswith(para(reminder))


async def test_plan_command_and_the_weekly_job_send_reports() -> None:
    bot = SimpleNamespace(do_api_request=AsyncMock(), send_message=AsyncMock())
    service = SimpleNamespace(
        plan_report=lambda: Report("📋", "<p>📋</p>"),
        weekly_digest=lambda: Report("🗓", "<p>🗓</p>"),
    )
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=5),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )
    context = SimpleNamespace(bot=bot, bot_data={"analytics_service": service})
    await analytics.plan_command(update, context)
    await analytics.send_weekly_digest(bot, 9, service)
    sent = [call.kwargs["api_kwargs"] for call in bot.do_api_request.await_args_list]
    assert [(a["chat_id"], a["rich_message"]["html"]) for a in sent] == [
        (5, "<p>📋</p>"),
        (9, "<p>🗓</p>"),
    ]
