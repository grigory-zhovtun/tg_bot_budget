"""Утренняя сводка и /today расширенным сообщением: блоки по макету, числа целиком."""

from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app import config
from app.handlers import analytics
from app.rich import NBSP, Report, heading, in_currency, para, signed, table, whole
from app.services.analytics_service import AnalyticsService
from app.services.day_budget import format_day_budget, rich_day_budget
from app.services.recurring import find_series, rich_morning_lines
from app.services.runway import Runway
from tests.test_day_budget import (
    FACT_HEADER,
    OPENING,
    OVERSPENT,
    Sheets,
    budget,
    fact_row,
    tab,
)
from tests.test_recurring import frame as history
from tests.test_recurring import row

pytestmark = pytest.mark.usefixtures("rich")


# как ITEMS старых тестов, но аренда выдуманная
LIST = [
    ("Аванс", 15, 2_000_000, "UZS"),
    ("ЗП", 30, 10_000_000, "UZS"),
    ("Аренда & Ко", 18, -420, "USD"),
]
HEAD = ["статья", "факт", "план"]
NOTES = [row(date(2026, month, 14), "Cloud Notes", 52_000) for month in (8, 9)]
MUSIC = [
    row(day, "Music Box", 41_000)
    for day in (
        date(2026, 8, 3),
        date(2026, 9, 3),
        date(2026, 10, 3),
        date(2026, 10, 4),
    )
]


def test_morning_blocks_follow_the_mockup() -> None:
    b = budget(date(2026, 10, 2), OVERSPENT, inventory_fact=640_000, items=LIST)
    blocks = rich_day_budget(b)
    assert blocks[:7] == [
        para("☀️ Пятница, 2 октября"),
        heading(f"💸 {whole(b.limit)} сум", 1),
        para(
            "можно тратить в день до аванса 15-го", f"на сумовых картах {whole(b.sums)}"
        ),
        heading(f"⚠️ Сверх плана на {whole(400_000 - b.plan_per_day)} за 1 день"),
        heading(f"⚠️ Вчера {whole(b.yesterday_spent)} из {whole(b.yesterday_limit)}"),
        para(f"больше на {whole(b.yesterday_spent - b.yesterday_limit)}"),
        heading(f"💳 Сумы {whole(b.runway.balance)}"),
    ]
    assert not any("Отстаём" in block or "по плану месяца" in block for block in blocks)
    assert para("до конца месяца хватит") in blocks
    frozen = in_currency(b.frozen, "USD", with_cents=True)
    assert heading(f"🧊 Заморожено {frozen}") in blocks
    over = [["инвентарь", whole(640_000), whole(500_000)]]
    assert table(over, "lrr", HEAD) in blocks
    # короткий список строками: таблица с датой и названием не влезает в телефон
    assert blocks[-2:] == [
        heading("📅 Впереди"),
        para(
            f"15.10 Аванс: {signed(2_000_000, 'UZS')}",
            f"18.10 Аренда & Ко: {signed(-420, 'USD')}",
        ),
    ]
    assert "Аренда &amp; Ко" in "".join(blocks)


def test_today_shows_what_is_left_or_the_overspend() -> None:
    coffee = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 30_000)
    b = budget(date(2026, 10, 2), OVERSPENT, coffee, items=LIST)
    assert rich_day_budget(b, now=True)[1:3] == [
        heading(f"💸 {whole(b.limit - 30_000)} сум", 1),
        para(
            "осталось на сегодня",
            f"лимит {whole(b.limit)} в день до аванса 15-го",
            f"потрачено {whole(30_000)}",
        ),
    ]
    spree = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 10 * b.limit)
    over = budget(date(2026, 10, 2), OVERSPENT, spree, items=LIST)
    assert rich_day_budget(over, now=True)[1:3] == [
        heading(f"⚠️ Сверх лимита на {whole(over.spent_today - over.limit)}", 2),
        para(
            f"лимит {whole(over.limit)} в день до аванса 15-го",
            f"потрачено {whole(over.spent_today)}",
        ),
    ]


def test_the_limit_names_its_salary_and_the_payments_before_it() -> None:
    bills = [*LIST, ("Интернет", 10, -300_000, "UZS")]
    b = budget(date(2026, 10, 5), items=bills)
    assert rich_day_budget(b)[2] == para(
        "можно тратить в день до аванса 15-го",
        f"на сумовых картах {whole(5_000_000)}, платежи до 15-го {whole(300_000)}",
    )
    paid = [
        fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000),
        fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000),
    ]
    last = budget(date(2026, 10, 31), *paid, items=LIST)
    assert rich_day_budget(last)[2] == para(
        "можно тратить в день до аванса 15 ноября",
        f"на сумовых картах {whole(17_000_000)}",
    )
    rent = budget(date(2026, 10, 10), items=[("Аренда & Ко", 18, -420, "USD")])
    assert rich_day_budget(rent)[2] == para(
        "можно тратить в день до конца месяца", f"на сумовых картах {whole(5_000_000)}"
    )
    calm = budget(date(2026, 10, 3), items=LIST)
    spare = whole(2 * calm.plan_per_day)  # два дня без трат
    assert heading(f"✓ Запас {spare} за 2 дня") in rich_day_budget(calm)


def test_no_limit_goal_and_the_first_day() -> None:
    spree = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 8_000_000)
    b = budget(date(2026, 10, 2), spree, items=LIST)
    assert rich_day_budget(b)[1:3] == [
        heading("💸 Лимита на сегодня нет", 2),
        para(
            f"до аванса 15-го не хватает {whole(3_000_000)}",
            f"на сумовых картах {whole(-3_000_000)}",
        ),
    ]
    first = budget(date(2026, 10, 1), goal=500_000, items=LIST)
    blocks = rich_day_budget(first)
    assert heading(f"Вчера {whole(0)}") in blocks
    assert not any("Отстаём" in block or "Сверх плана на" in block for block in blocks)
    assert heading(f"🎯 Цель отложить {whole(500_000)}") in blocks
    assert para("план её покрывает") in blocks


def test_runway_and_frozen_variants() -> None:
    b = budget(date(2026, 10, 2), items=LIST)
    short = Runway(5_000_000, 0, date(2026, 10, 23), 1_000_000, date(2026, 10, 30), 370)
    blocks = rich_day_budget(replace(b, runway=short, frozen_change=-117.45))
    exchange = f"до 30.10 обменять ≈{NBSP}{in_currency(370, 'USD')}"
    assert para("хватит до 23.10", exchange) in blocks
    change = in_currency(117.45, "USD", with_cents=True)
    assert (
        para(f"≈{NBSP}{whole(b.frozen_uzs)} сум", f"с 1-го числа −{change}") in blocks
    )


def test_due_subscriptions_are_short_lines() -> None:
    found = find_series(history(*NOTES), date(2026, 10, 13))
    assert rich_morning_lines(found, date(2026, 10, 13)) == [
        heading("🔁 Завтра спишется"),
        para(f"Cloud Notes: ≈{NBSP}{whole(52_000)}"),
    ]
    twice = find_series(history(*MUSIC), date(2026, 10, 5))
    assert rich_morning_lines(twice, date(2026, 10, 5)) == [
        para("⚠️ Music Box списан второй раз за октябрь: 03.10, 04.10")
    ]


def test_service_report_carries_both_views() -> None:
    fact = [FACT_HEADER, *OPENING, OVERSPENT]
    rows = tab(inventory_fact=640_000, items=LIST)
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": rows}))
    report = service.morning_brief(date(2026, 10, 2))
    b = budget(date(2026, 10, 2), OVERSPENT, inventory_fact=640_000, items=LIST)
    assert report.text == format_day_budget(b)
    assert report.html == "".join(rich_day_budget(b))
    assert AnalyticsService(Sheets({})).morning_brief(date(2026, 11, 1)).html is None


async def test_morning_brief_goes_rich_with_the_party(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "CELEBRATE_EFFECT_ID", "42")
    bot = SimpleNamespace(do_api_request=AsyncMock(), send_message=AsyncMock())
    report = Report("☀️", "<p>☀️</p>")
    service = SimpleNamespace(morning_message=lambda: (report, True))
    await analytics.send_morning_brief(bot, 7, service)
    api = bot.do_api_request.await_args.kwargs["api_kwargs"]
    assert (api["chat_id"], api["message_effect_id"]) == (7, "42")
    bot.send_message.assert_not_awaited()


async def test_today_command_sends_the_report_to_the_chat() -> None:
    calls: list[dict[str, Any]] = []

    def brief(**options: Any) -> Report:
        calls.append(options)
        return Report("💸", "<p>💸</p>")

    bot = SimpleNamespace(do_api_request=AsyncMock(), send_message=AsyncMock())
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=5),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )
    service = SimpleNamespace(morning_brief=brief)
    context = SimpleNamespace(bot=bot, bot_data={"analytics_service": service})
    await analytics.today_command(update, context)
    assert bot.do_api_request.await_args.kwargs["api_kwargs"]["chat_id"] == 5
    assert calls == [{"now": True}]


async def test_fonts_command_sends_each_variant_as_its_own_message() -> None:
    # временная проверка шрифтов: крупные числа разными способами, каждый — отдельно,
    # чтобы отказ Telegram в одном варианте не скрыл остальные
    bot = SimpleNamespace(do_api_request=AsyncMock(), send_message=AsyncMock())
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=5),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )
    context = SimpleNamespace(bot=bot, bot_data={})
    await analytics.fonts_command(update, context)
    sent = [
        call.kwargs["api_kwargs"]["rich_message"]["html"]
        for call in bot.do_api_request.await_args_list
    ]
    assert len(sent) == 6
    number = whole(1_234_567_890)
    assert sent[0] == (
        f"<p>Вариант 6 — моноширинный блок (выбранный)</p><pre>💸 {number} сум</pre>"
    )
    # «математические» моноширинные цифры: 𝟷 𝟸𝟹𝟺 𝟻𝟼𝟽 𝟾𝟿𝟶
    mono = number.translate({ord(str(d)): chr(0x1D7F6 + d) for d in range(10)})
    assert f"<h1>💸 {mono} сум</h1>" in sent[1]
    assert "<pre>⚠️ Сверх плана на" in sent[4]
