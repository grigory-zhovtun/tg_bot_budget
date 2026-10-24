"""Ответы после записи расширенным сообщением: статьи месяца, «на сегодня», запись."""

from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import ReplyKeyboardMarkup

from app import config
from app.domain import SheetRow
from app.handlers import analytics, messages, undo
from app.handlers.common import notify_others, show_main_menu
from app.handlers.messages import record_blocks
from app.rich import (
    Report,
    heading,
    in_currency,
    money_text,
    note_report,
    para,
    percent,
    table,
    whole,
)
from app.services.analytics_service import AnalyticsService, Feedback, PlanLine
from app.services.day_budget import (
    plan_status,
    rich_plan_status,
    rich_today_line,
    today_line,
)
from app.services.recurring import find_series, rich_subs
from tests.test_day_budget import FACT_HEADER, OPENING, Sheets, budget, fact_row, tab
from tests.test_messages import FakeSheets, make_chat, manual_state
from tests.test_recurring import frame as history
from tests.test_recurring import row as charge

pytestmark = pytest.mark.usefixtures("rich")


TODAY = date(2026, 10, 19)
# выдуманный список месяца, как ITEMS старых тестов
LIST = [
    ("Аванс", 15, 2_000_000, "UZS"),
    ("ЗП", 30, 10_000_000, "UZS"),
    ("Аренда", 18, -420, "USD"),
]


def test_plan_status_blocks() -> None:
    taxi = PlanLine("🚌 ТРАНСПОРТ", "такси", 900_000, 365_000)
    assert rich_plan_status(taxi, "такси", TODAY) == [
        heading(f"🟢 Такси {whole(365_000)}"),
        para(f"из {whole(900_000)} — {percent(365_000 / 900_000)}"),
    ]
    coffee = PlanLine("🍔 ЕДА", "кофе", 1_500_000, 1_287_000)
    assert rich_plan_status(coffee, "кофе", TODAY) == [
        heading(f"🟡 Кофе {whole(1_287_000)}"),
        para(f"из {whole(1_500_000)} — {percent(0.858)}, осталось {whole(213_000)}"),
    ]
    food = PlanLine("🏚️ ДОМ", "продукты", 9_000_000, 9_380_000)
    assert rich_plan_status(food, "продукты", TODAY) == [
        heading(f"🔴 Продукты {whole(9_380_000)}"),
        para(f"из {whole(9_000_000)} — сверх на {whole(380_000)}"),
    ]
    season = PlanLine("🎉 ПРАЗДНИК", "сезонный", 0, 2_650_000)
    assert rich_plan_status(season, "сезонный", TODAY) == [
        heading(f"⚪ Сезонный {whole(2_650_000)}"),
        para("вне плана — всё за октябрь"),
    ]
    assert rich_plan_status(None, "сезонный", TODAY) == [
        heading("⚪ Сезонный"),
        para("нет в плане месяца"),
    ]


def test_today_line_blocks() -> None:
    b = replace(budget(date(2026, 10, 2), items=LIST), limit=350_000)
    assert rich_today_line(replace(b, spent_today=48_000)) == [
        heading(f"💸 Осталось {whole(302_000)}"),
        para(f"на сегодня, лимит {whole(350_000)} в день до аванса 15-го"),
    ]
    assert rich_today_line(replace(b, spent_today=400_000)) == [
        heading(f"⚠️ Сверх лимита на {whole(50_000)}"),
        para(f"лимит {whole(350_000)} в день до аванса 15-го"),
    ]
    short = replace(b, limit=-30_000, days_left=13)
    assert rich_today_line(short) == [
        heading("💸 Лимита на сегодня нет"),
        para(f"до аванса 15-го не хватает {whole(390_000)}"),
    ]


def test_write_feedback_keeps_the_lines_and_adds_blocks() -> None:
    cafe = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 120_000)
    fact = [FACT_HEADER, *OPENING, cafe]
    rows = tab(items=LIST)
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": rows}))
    found = service.write_feedback(["кафе"], today=date(2026, 10, 2))
    b = budget(date(2026, 10, 2), cafe, items=LIST)
    line = PlanLine("🍔 ЕДА", "кафе", 2_600_000, 0)
    assert found == Feedback(
        [plan_status(line, "кафе", None, date(2026, 10, 2)), today_line(b)],
        [*rich_plan_status(line, "кафе", date(2026, 10, 2)), *rich_today_line(b)],
    )


CARD = "VISA 1111 UZS"


def spend(
    amount: float,
    sub: str = "кофе",
    group: str = "🍔 ЕДА",
    currency: str = "UZS",
    source: str = CARD,
    balance: float | None = None,
) -> SheetRow:
    return SheetRow(TODAY, group, sub, amount, "A & B", currency, source, balance)


def test_one_record_is_a_big_amount_with_the_card_and_the_check() -> None:
    row = spend(48_000, balance=4_205_330)
    found = record_blocks(
        [row], ["🆕"], {CARD: 4_205_330}, {CARD: 4_205_330}, [CARD], {"кофе": "☕"}
    )
    assert found == [
        heading(f"✅ {whole(48_000)}", 2),
        para(
            "☕ кофе · 🍔 ЕДА",
            f"{CARD}: {whole(4_205_330)}",
            "✓ сходится с банком",
            "🆕 A & B",
        ),
    ]


def test_income_and_currency_headings() -> None:
    pay = SheetRow(TODAY, "💰 ДОХОДЫ", "зарплата", 31_000_000, "", "UZS", CARD)
    assert record_blocks([pay], [None], {}, {}, [], None)[0] == heading(
        f"✅ +{whole(31_000_000)}", 2
    )
    usd = spend(380, currency="USD", source="VISA 2222 USD")
    assert record_blocks([usd], [None], {}, {}, [], None) == [
        heading(f"✅ {in_currency(380, 'USD')}", 2),
        para("кофе · 🍔 ЕДА", "VISA 2222 USD"),
    ]
    cents = spend(12.5, currency="USD", source="VISA 2222 USD")
    assert record_blocks([cents], [None], {}, {}, [], None)[0] == heading(
        f"✅ {in_currency(12.5, 'USD', with_cents=True)}", 2
    )


def test_a_batch_is_a_table_with_the_total() -> None:
    rows = [
        spend(48_000),
        spend(31_000, "такси", "🚌 ТРАНСПОРТ"),
        spend(57_400, "продукты", "🏚️ ДОМ", balance=4_116_930),
    ]
    found = record_blocks(
        rows,
        [None, None, "❓"],
        {CARD: 4_116_930},
        {CARD: 4_116_930},
        [CARD],
        {"кофе": "☕"},
    )
    assert found == [
        heading(f"✅ 3 траты — {whole(136_400)}", 2),
        table(
            [
                [whole(48_000), "☕ кофе"],
                [whole(31_000), "такси"],
                [whole(57_400), "продукты ❓"],
            ],
            "rl",
        ),
        para(f"{CARD}: {whole(4_116_930)}", "✓ сходится с банком", "❓ A & B"),
    ]


def test_a_batch_with_an_income_has_no_total() -> None:
    pay = SheetRow(TODAY, "💰 ДОХОДЫ", "зарплата", 1_000_000, "", "UZS", CARD)
    found = record_blocks([spend(48_000), pay], [None, None], {}, {}, [], None)
    assert found[0] == heading("✅ Записал 2 строки", 2)


def test_a_mismatch_with_the_bank_is_spelled_out() -> None:
    row = spend(48_000, balance=4_205_330)
    found = record_blocks(
        [row], [None], {CARD: 4_205_330}, {CARD: 4_180_000}, [CARD], None
    )
    assert found[1] == para(
        "кофе · 🍔 ЕДА",
        f"{CARD}: {whole(4_205_330)}",
        f"⚠️ в банке {whole(4_205_330)}",
        f"в таблице {whole(4_180_000)}",
        f"разница {whole(25_330)}",
    )


async def test_the_reply_goes_rich_with_the_card_keyboard_and_is_remembered() -> None:
    update, context, _ = make_chat("x", manual_state())
    context.bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=777))
    await show_main_menu(update.effective_chat, context, Report("x", "<p>x</p>"))
    call = context.bot.do_api_request.await_args
    assert call.args == ("sendRichMessage",)
    assert isinstance(call.kwargs["api_kwargs"]["reply_markup"], ReplyKeyboardMarkup)
    assert 777 in context.user_data["bot_messages"]


async def test_a_typed_record_replies_rich() -> None:
    update, context, _ = make_chat("48000 латте", manual_state())
    context.bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=778))
    await messages.text_handler(update, context)
    api = context.bot.do_api_request.await_args.kwargs["api_kwargs"]
    assert api["rich_message"]["html"].startswith(heading(f"✅ {whole(48_000)}", 2))


async def test_a_duplicate_warning_is_a_heading() -> None:
    update, context, _ = make_chat("x", {})
    context.bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=779))
    await messages._report_duplicate(update, context, {"rows": (12, 13), "at": 1.79e9})
    api = context.bot.do_api_request.await_args.kwargs["api_kwargs"]
    assert api["rich_message"]["html"].startswith(heading("⚠️ Это уже записано"))


async def test_a_note_to_the_other_user_is_a_heading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({1, 2}))
    bot = SimpleNamespace(do_api_request=AsyncMock(), send_message=AsyncMock())
    text = "📅 Жена — новая запись: 21.10 Кружок 180 000 UZS"
    await notify_others(bot, 1, text)
    api = bot.do_api_request.await_args.kwargs["api_kwargs"]
    assert api["chat_id"] == 2
    assert api["rich_message"]["html"] == note_report(text).html


SUBS_DAY = date(2026, 10, 10)
SUBS = [
    *[charge(date(2026, m, 1), "Web Host", 99_000) for m in (8, 9, 10)],
    *[charge(day, "Music Box", 41_000) for day in (date(2026, 8, 3),
      date(2026, 9, 3), date(2026, 10, 3), date(2026, 10, 4))],
    *[charge(date(2026, m, 5), "Cloud Box", 30_000) for m in (8, 9)],
    *[charge(date(2026, m, 12), "Code Pal", 10, cur="USD") for m in (8, 9)],
    *[charge(date(2026, m, 14), "Cloud Notes", 52_000) for m in (8, 9)],
    *[charge(date(2026, m, 20), "Old Club", 70_000) for m in (6, 7)],
]  # fmt: skip


def test_subs_go_by_groups() -> None:
    found = find_series(history(*SUBS), SUBS_DAY)
    usd = f"{money_text(10, 'USD')} ({whole(120_000)})"
    assert rich_subs(found, SUBS_DAY) == [
        heading(f"🔁 ≈ {whole(342_000)} в месяц", 2),
        para("регулярные списания", f"из них подписки ≈ {whole(342_000)}"),
        heading("⏳ Ждём в октябре"),
        para(f"12 Code Pal — {usd}", f"14 Cloud Notes — {whole(52_000)}"),
        heading("✅ Списано"),
        para(f"01 Web Host — {whole(99_000)}"),
        heading("⚠️ Дважды за октябрь"),
        para(f"Music Box — {whole(41_000)}", "03.10, 04.10"),
        heading("❓ Не было в октябре"),
        para(f"05 Cloud Box — {whole(30_000)}"),
        heading("Давно не было"),
        para("Old Club — последнее 20.07"),
    ]


async def test_subs_and_undo_commands_reply_rich(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = SimpleNamespace(do_api_request=AsyncMock(), send_message=AsyncMock())
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=5),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )
    service = SimpleNamespace(subs_report=lambda: Report("🔁", "<p>🔁</p>"))
    context = SimpleNamespace(bot=bot, bot_data={"analytics_service": service})
    await analytics.subs_command(update, context)
    api = bot.do_api_request.await_args.kwargs["api_kwargs"]
    assert (api["chat_id"], api["rich_message"]["html"]) == (5, "<p>🔁</p>")

    async def undone(context: object) -> str:
        return "↩️ Удалил из таблицы: строка 12."

    monkeypatch.setattr(undo, "undo_last", undone)
    await undo.undo(update, context)
    api = bot.do_api_request.await_args.kwargs["api_kwargs"]
    assert (
        api["rich_message"]["html"]
        == note_report("↩️ Удалил из таблицы: строка 12.").html
    )
    update.message.reply_text.assert_not_awaited()


async def test_a_failed_write_still_replies_with_the_card_keyboard() -> None:
    class BrokenSheets(FakeSheets):
        def append_transactions(self, rows: list[object]) -> tuple[int, int]:
            raise ConnectionError("Google is down")

    update, context, _ = make_chat("48000 латте", manual_state())
    context.bot_data["gs_service"] = BrokenSheets()
    context.bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=780))
    await messages.text_handler(update, context)
    api = context.bot.do_api_request.await_args.kwargs["api_kwargs"]
    assert api["rich_message"]["html"] == para(
        "❌ Не записал в Google Таблицу (1 шт.), попробуйте ещё раз"
    )
    assert isinstance(api["reply_markup"], ReplyKeyboardMarkup)
