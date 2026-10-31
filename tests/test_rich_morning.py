"""Утренняя сводка и /today расширенным сообщением: блоки по макету, числа целиком."""

from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app import config
from app.handlers import analytics
from app.rich import (
    NBSP,
    Report,
    heading,
    in_currency,
    money_text,
    para,
    signed,
    table,
    whole,
)
from app.services.analytics_service import AnalyticsService
from app.services.day_budget import format_day_budget, rich_day_budget
from app.services.recurring import find_series, rich_morning_lines
from app.services.runway import Runway
from tests.test_day_budget import (
    ADVANCE_PAID,
    FACT_HEADER,
    ITEMS,
    OPENING,
    OVERSPENT,
    RENT_PAID,
    SALARY_TODAY,
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


def usd(amount: float) -> str:
    return money_text(amount, "USD")


def test_morning_blocks_follow_the_mockup() -> None:
    b = budget(date(2026, 10, 2), OVERSPENT, inventory_fact=640_000, items=LIST)
    blocks = rich_day_budget(b)
    assert blocks[:7] == [
        para("☀️ Пятница, 2 октября"),
        heading(f"💸 {whole(b.limit)} сум", 1),
        para(
            "можно тратить в день до зарплаты 30-го",
            f"на сумовых картах {whole(b.sums)}, аванс 15-го +{whole(2_000_000)}",
        ),
        heading(f"⚠️ Сверх плана на {whole(400_000 - b.plan_per_day)} за 1 день"),
        # «из» — с числом: блок переносит строку перед ним, а не после
        heading(
            f"⚠️ Вчера {whole(b.yesterday_spent)} из{NBSP}{whole(b.yesterday_limit)}"
        ),
        para(f"больше на {whole(b.yesterday_spent - b.yesterday_limit)}"),
        heading(f"💳 Сумы сейчас {whole(b.runway.balance)}"),
    ]
    assert not any("Отстаём" in block or "по плану месяца" in block for block in blocks)
    left = f"останется ≈{NBSP}{whole(b.runway.left)}"
    assert para("✓ до зарплаты 30-го хватит", left) in blocks
    at = blocks.index(heading(f"🧊 Заморожено {usd(b.frozen)}"))
    assert blocks[at + 1] == para(
        "на платежи до зарплаты 30-го:", f"Аренда & Ко 18-го — {usd(420)}"
    )
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
            f"лимит {whole(b.limit)} в день до зарплаты 30-го",
            f"потрачено {whole(30_000)}",
        ),
    ]
    spree = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 10 * b.limit)
    over = budget(date(2026, 10, 2), OVERSPENT, spree, items=LIST)
    assert rich_day_budget(over, now=True)[1:3] == [
        heading(f"⚠️ Сверх лимита на {whole(over.spent_today - over.limit)}", 2),
        para(
            f"лимит {whole(over.limit)} в день до зарплаты 30-го",
            f"потрачено {whole(over.spent_today)}",
        ),
    ]


SALARY_PAID = [
    fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000),
    fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000),
]


def test_today_on_the_salary_day_counts_from_tomorrow() -> None:
    cafe = fact_row(date(2026, 10, 30), "🍔 ЕДА", "кафе", 300_000)
    b = budget(date(2026, 10, 30), *SALARY_PAID, cafe, items=LIST)
    assert b.tomorrow is not None
    assert rich_day_budget(b, now=True)[1:3] == [
        heading(f"💸 {whole(b.tomorrow.limit)} сум", 1),
        para(
            "можно в день с завтра",
            "до зарплаты 30 ноября, 31 день",
            f"✓ пришла ЗП {whole(10_000_000)}",
            f"потрачено сегодня {whole(300_000)}",
            f"на сумовых картах сейчас {whole(16_700_000)}, "
            f"аванс 15 ноября +{whole(2_000_000)}",
        ),
    ]
    # утро — прежнее: лимит с утра, сумы на вечер вчера
    assert rich_day_budget(b)[1] == heading(f"💸 {whole(b.limit)} сум", 1)


def test_no_limit_from_tomorrow_on_the_salary_day() -> None:
    spree = fact_row(date(2026, 10, 30), "🍔 ЕДА", "кафе", 20_000_000)
    b = budget(date(2026, 10, 30), *SALARY_PAID, spree, items=LIST)
    # сумы −3 млн: и до аванса 15 ноября денег нет — лимита нет, сверх лимита не пишем
    assert rich_day_budget(b, now=True)[1:3] == [
        heading("💸 Лимита на сегодня нет", 2),
        para(
            f"до аванса 15 ноября не хватает {whole(3_000_000)}",
            f"✓ пришла ЗП {whole(10_000_000)}",
            f"потрачено сегодня {whole(20_000_000)}",
            f"на сумовых картах сейчас {whole(-3_000_000)}",
        ),
    ]


def test_the_limit_names_its_salary_and_the_payments_before_it() -> None:
    bills = [*LIST, ("Интернет", 10, -300_000, "UZS")]
    b = budget(date(2026, 10, 5), items=bills)
    assert rich_day_budget(b)[2] == para(
        "можно тратить в день до зарплаты 30-го",
        f"на сумовых картах {whole(5_000_000)}, аванс 15-го +{whole(2_000_000)}, "
        f"платежи до 30-го {whole(300_000)}",
    )
    paid = [
        fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000),
        fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000),
    ]
    last = budget(date(2026, 10, 31), *paid, items=LIST)
    assert rich_day_budget(last)[2] == para(
        "можно тратить в день до зарплаты 30 ноября",
        f"на сумовых картах {whole(17_000_000)}, аванс 15 ноября +{whole(2_000_000)}",
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
    payday = date(2026, 10, 30)
    short = Runway(
        5_000_000, 0, date(2026, 10, 23), 1_000_000, payday, 370, -1e6, payday, "ЗП"
    )
    blocks = rich_day_budget(replace(b, runway=short))
    # заголовок — «сейчас»: сумы с утра уже могли измениться
    assert heading(f"💳 Сумы сейчас {whole(5_000_000)}") in blocks
    exchange = f"до зарплаты 30-го обменять ≈{NBSP}{in_currency(370, 'USD')}"
    assert para("хватит до 23.10", exchange) in blocks
    enough = replace(
        short, last_day=None, short=0, until=None, dollars=None, left=400_000
    )
    today = replace(enough, payday=b.today)
    assert para(
        "✓ до зарплаты сегодня хватит", f"останется ≈{NBSP}{whole(400_000)}"
    ) in (rich_day_budget(replace(b, runway=today)))
    # «с 1-го числа ±X» убрана: она смешивала обмен, взносы и платежи
    assert not any("с 1-го числа" in block for block in blocks)


def test_the_morning_says_what_the_frozen_money_keeps() -> None:
    b = replace(budget(date(2026, 10, 10)), piggies=500)
    keeps = f"подушка {usd(100)}, копилки {usd(500)}"
    lines = format_day_budget(b).splitlines()
    at = lines.index(f"🧊 Заморожено {usd(600)} — не трогаем: {keeps}")
    assert lines[at + 1 : at + 3] == [
        "   на платежи до зарплаты 30-го:",
        f"   Квартплата 18-го — {usd(400)}",
    ]
    blocks = rich_day_budget(b)
    at = blocks.index(heading(f"🧊 Заморожено {usd(600)}"))
    assert blocks[at + 1] == para(
        f"не трогаем: {keeps}",
        "на платежи до зарплаты 30-го:",
        f"Квартплата 18-го — {usd(400)}",
    )
    assert not any("с 1-го числа" in block for block in blocks)


def test_a_payment_of_next_month_names_its_month() -> None:
    b = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID, SALARY_TODAY)
    assert f"   Квартплата 18 ноября — {usd(400)}" in format_day_budget(b).splitlines()


def test_the_frozen_line_says_what_to_do_when_it_does_not_add_up() -> None:
    # 1000 $ на карте, квартплата 18-го 400 $: копилки 1200 $ больше всей карты
    over = replace(budget(date(2026, 10, 10)), piggies=1_200)
    assert (
        f"   ⚠️ копилки больше карты на {usd(200)} — отложите разницу или "
        "поправьте копилки"
    ) in format_day_budget(over).splitlines()
    # копилки 700 $ меньше карты, но с квартплатой не помещаются — долларов мало
    tight = format_day_budget(replace(budget(date(2026, 10, 10)), piggies=700))
    assert (
        f"   ⚠️ долларов не хватает на платежи — купите {usd(100)} до 18-го"
    ) in tight.splitlines()
    assert "копилки больше карты" not in tight
    short = budget(date(2026, 10, 10), items=[*ITEMS, ("Курсы", 25, -1_000, "USD")])
    lines = format_day_budget(short).splitlines()
    at = lines.index(f"🧊 Заморожено {usd(-400)}")  # копилок не читали: без разбивки
    assert lines[at + 1 : at + 5] == [
        f"   ⚠️ долларов не хватает на платежи — купите {usd(400)} до 25-го",
        "   на платежи до зарплаты 30-го:",
        f"   Квартплата 18-го — {usd(400)}",
        f"   Курсы 25-го — {usd(1_000)}",
    ]


def test_a_payment_that_is_already_late_says_to_buy_today() -> None:
    # курсы 8-го на 1200 $ не оплачены: долларов не хватает уже сейчас
    late = [*ITEMS, ("Курсы", 8, -1_200, "USD")]
    lines = format_day_budget(budget(date(2026, 10, 10), items=late)).splitlines()
    assert f"   ⚠️ долларов не хватает на платежи — купите {usd(600)} сегодня" in lines


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
