"""Сигнал в момент записи: статус статьи месяца и сколько осталось на сегодня."""

from dataclasses import replace
from datetime import date
from typing import Any

import pytest

from app.handlers import messages
from app.services.analytics_service import AnalyticsService, PlanLine
from app.services.analytics_service import Feedback as Found
from app.services.day_budget import plan_status, today_line
from tests.test_day_budget import FACT_HEADER, OPENING, OVERSPENT, Sheets, budget, tab
from tests.test_messages import make_chat, manual_state, summary

ICONS = {"кофе": "☕", "хобби": "🎸"}
OCTOBER = date(2026, 10, 2)


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (
            PlanLine("🍔 ЕДА", "кофе", 3_000_000, 1_250_000),
            "🟢 ☕ кофе: 1,25 из 3,00 млн (42%)",
        ),
        (
            PlanLine("🍔 ЕДА", "кофе", 3_000_000, 2_700_000),
            "🟡 ☕ кофе: 2,70 из 3,00 млн (90%) — осталось 300 000",
        ),
        (
            PlanLine("🍔 ЕДА", "кофе", 400_000, 450_000),
            "🔴 ☕ кофе: 450 000 из 400 000 — сверх плана на 50 000",
        ),
        (
            PlanLine("⛱️ ОТДЫХ", "хобби", 0, 350_000),
            "⚪ 🎸 хобби: вне плана, в октябре уже 350 000",
        ),
        (None, "⚪ ☕ кофе: нет в плане месяца"),
    ],
)
def test_plan_status_of_the_written_subcategory(
    line: PlanLine | None, expected: str
) -> None:
    assert (
        plan_status(
            line,
            "хобби" if line and line.subcategory == "хобби" else "кофе",
            ICONS,
            OCTOBER,
        )
        == expected
    )


def test_today_line_left_over_and_no_limit() -> None:
    day = budget(date(2026, 10, 2), OVERSPENT)  # 4,6 млн и аванс 2 млн на 29 дней
    assert today_line(day) == "💸 На сегодня осталось 227 586 из 227 586"
    spent = replace(day, spent_today=400_000)
    assert today_line(spent) == (
        "💸 Сегодня сверх лимита на 172 414 (лимит 227 586 в день до зарплаты 30-го)"
    )
    broke = replace(day, limit=-30_000)
    assert today_line(broke) == (
        "💸 Лимита на сегодня нет: до зарплаты 30-го не хватает 870 000"
    )


def test_service_reads_the_month_tab_after_the_write() -> None:
    fact = [FACT_HEADER, *OPENING, OVERSPENT]
    service = AnalyticsService(
        Sheets({"fact": fact, "Oct 26": tab(inventory_fact=720_000)})
    )
    lines = service.write_feedback(
        ["инвентарь", "кафе", "такси"], {"кафе": "🍽️"}, OCTOBER
    ).lines
    assert lines == [
        "🔴 инвентарь: 720 000 из 500 000 — сверх плана на 220 000",
        "🟢 🍽️ кафе: 0,00 из 2,60 млн (0%)",  # обе суммы в одних единицах
        "⚪ такси: нет в плане месяца",
        "💸 На сегодня осталось 227 586 из 227 586",
    ]
    assert AnalyticsService(Sheets({})).write_feedback(
        ["кафе"], {}, date(2026, 11, 1)
    ) == Found([], [])


class Feedback:
    def __init__(self, fail: bool = False) -> None:
        self.asked: list[list[str]] = []
        self.fail = fail

    def write_feedback(self, subcategories: list[str], icons: Any) -> Found:
        self.asked.append(subcategories)
        if self.fail:
            raise ConnectionError("Google is down")
        lines = ["🟢 ☕ кофе: 48 000 из 400 000 (12%)", "💸 На сегодня осталось 1 из 2"]
        return Found(lines, [])


async def test_summary_gets_the_plan_lines_under_the_record() -> None:
    update, context, _ = make_chat("48000 латте", manual_state())
    feedback = Feedback()
    context.bot_data["analytics_service"] = feedback
    await messages.text_handler(update, context)
    assert feedback.asked == [["кофе"]]
    lines = summary(update).splitlines()
    assert lines[0].startswith("✅ 48 000 UZS • 🍔 ЕДА (кофе)")
    assert lines[1:] == [
        "🟢 ☕ кофе: 48 000 из 400 000 (12%)",
        "💸 На сегодня осталось 1 из 2",
    ]


async def test_no_plan_lines_for_income_and_transfers_or_on_errors() -> None:
    update, context, _ = make_chat(
        "+20000 перевод", manual_state("💳 СЧЕТА", "переводы")
    )
    feedback = Feedback()
    context.bot_data["analytics_service"] = feedback
    await messages.text_handler(update, context)
    assert feedback.asked == []  # переводы между картами — не трата

    update, context, _ = make_chat("48000 латте", manual_state())
    context.bot_data["analytics_service"] = Feedback(fail=True)
    await messages.text_handler(update, context)
    assert summary(update).startswith("✅ 48 000 UZS")  # запись важнее подсказки
