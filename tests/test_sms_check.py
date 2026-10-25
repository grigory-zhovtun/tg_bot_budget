"""Проверка строк SMS с пометкой: в меню под сводкой и по очереди («🔎 Проверить»)."""

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

from telegram.error import BadRequest

from app.domain import SheetRow
from app.handlers import fix, last_write, messages
from app.handlers.common import LAST_WRITE
from tests.test_fix import (
    CATEGORIES,
    SUBCATEGORIES,
    FixSheets,
    HistoryAI,
    callbacks,
    command,
    press,
    row,
)
from tests.test_messages import make_chat, menu_after_summary, sent_message

ROWS = [
    row("🍔 ЕДА", "кафе", comment="OOO SHAVI CAFE"),
    row(comment="IP OOO ANGLESEY FOOD"),
    row(comment="перевод: VISA MC UZS 2 UZCARD OB"),
]
MENU = "💳 VISA 9090 UZS — выберите категорию"


def checking(rows: list[SheetRow], checks: dict[int, str]) -> SimpleNamespace:
    """Последняя запись, где часть строк ждёт проверки; меню — сообщение 77."""
    return SimpleNamespace(
        bot_data={
            "gs_service": FixSheets(),
            "ai_service": HistoryAI(),
            "categories": CATEGORIES,
            "subcategories": SUBCATEGORIES,
        },
        user_data={
            "source": "VISA 9090 UZS",
            LAST_WRITE: {
                "first": 4190,
                "last": 4189 + len(rows),
                "rows": list(rows),
                "fingerprint": None,
                "prints": [],
                "checks": dict(checks),
                "menu": (1, 77),
            },
        },
        bot=SimpleNamespace(edit_message_reply_markup=AsyncMock()),
    )


async def open_check(context: SimpleNamespace) -> tuple[str, Any]:
    """Нажать «🔎 Проверить» под сводкой: проверка приходит новым сообщением."""
    reply = AsyncMock(side_effect=lambda *a, **k: sent_message())
    query = SimpleNamespace(
        data="last:check",
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=SimpleNamespace(reply_text=reply),
    )
    await last_write.button(SimpleNamespace(callback_query=query), context)
    query.edit_message_text.assert_not_awaited()  # меню под сводкой остаётся
    call = reply.await_args
    return call.args[0], call.kwargs.get("reply_markup")


async def test_one_flagged_row_is_checked_right_under_the_summary() -> None:
    update, context, _ = make_chat("", {})
    new = SheetRow(
        day=date(2026, 10, 14),
        category="🍔 ЕДА",
        subcategory="кафе",
        amount=48000.0,
        comment="OOO SHAVI CAFE>TASHKENT",
        currency="UZS",
        source="VISA 9090 UZS",
    )
    await messages.save_rows(
        context, update.effective_chat, [new], [], None, flags=["🆕"]
    )
    call = menu_after_summary(update)
    assert call.args[0] == (
        "🆕 OOO SHAVI CAFE>TASHKENT • 48 000 UZS • VISA 9090 UZS\n"
        "ИИ предлагает: 🍔 ЕДА / кафе. Верно?"
    )
    keyboard = call.kwargs["reply_markup"].inline_keyboard
    assert [(b.text, b.callback_data) for b in keyboard[0]] == [("✓ Верно", "fix:ok")]
    assert [(b.text, b.callback_data) for b in keyboard[-1]] == [
        ("↩️ Отменить запись", "last:undo")
    ]
    assert context.user_data[fix.STATE]["mode"] == "inline"


async def test_ok_keeps_the_category_and_brings_the_menu_back() -> None:
    context = checking(ROWS[:1], {0: "🆕"})
    fix.inline_check(context)
    text, markup = await press(context, "fix:ok")
    assert text == (
        "👍 Оставил: 48 000 UZS • OOO SHAVI CAFE • 🍔 ЕДА (кафе) • VISA 9090 UZS\n"
        + MENU
    )
    assert callbacks(markup)[0] == ["last:fix", "last:undo"]  # «🔎» больше нет
    assert context.user_data[LAST_WRITE]["checks"] == {}
    assert fix.STATE not in context.user_data
    assert context.bot_data["gs_service"].calls == []


async def test_check_under_the_card_leaves_only_the_verdict() -> None:
    context = checking(ROWS[:1], {0: "🆕"})
    context.user_data[LAST_WRITE]["on_card"] = True
    fix.inline_check(context)
    text, markup = await press(context, "fix:ok")
    assert text == (
        "👍 Оставил: 48 000 UZS • OOO SHAVI CAFE • 🍔 ЕДА (кафе) • VISA 9090 UZS"
    )
    assert markup is None  # кнопки записи — на самой карточке


async def test_group_choice_fixes_the_row_and_brings_the_menu_back() -> None:
    context = checking(ROWS[2:], {0: "❓"})
    text, _ = fix.inline_check(context)
    assert text == (
        "❓ перевод: VISA MC UZS 2 UZCARD OB • 48 000 UZS • VISA 9090 UZS\n"
        "Записал как: 🚧 РАЗНОЕ / неучтенка. Что это?"
    )
    await press(context, "fix:c:0")
    text, markup = await press(context, "fix:s:1")
    [(number, target, *changes)] = context.bot_data["gs_service"].calls
    assert (number, target, changes) == (4190, ROWS[2], ["🍔 ЕДА", "кафе", 48000.0])
    assert text == (
        "✅ Исправил (строка 4190): 48 000 UZS • перевод: VISA MC UZS 2 UZCARD OB • "
        "🍔 ЕДА (кафе) • VISA 9090 UZS\n" + MENU
    )
    assert callbacks(markup)[0] == ["last:fix", "last:undo"]
    assert context.bot_data["ai_service"].forgotten == 1
    assert context.user_data[LAST_WRITE]["checks"] == {}


async def test_back_from_subgroups_returns_to_the_check() -> None:
    context = checking(ROWS[2:], {0: "❓"})
    fix.inline_check(context)
    await press(context, "fix:c:0")
    text, markup = await press(context, "fix:b")
    assert text.endswith("Что это?")
    assert callbacks(markup)[0] == ["fix:ok"]


async def test_cancel_in_subgroups_leaves_the_category_and_the_check() -> None:
    context = checking(ROWS[2:], {0: "❓"})
    fix.inline_check(context)
    await press(context, "fix:c:0")
    text, markup = await press(context, "fix:x")
    assert text == "Категорию оставил как есть.\n" + MENU
    assert markup.inline_keyboard[0][0].text == "🔎 Проверить (1)"


async def test_check_button_goes_through_the_rows_one_by_one() -> None:
    context = checking(ROWS, {0: "🆕", 2: "❓"})
    text, markup = await open_check(context)
    assert text.startswith("🔎 1 из 2. 🆕 OOO SHAVI CAFE • 48 000 UZS • VISA 9090 UZS")
    assert callbacks(markup)[0] == ["fix:ok"] and callbacks(markup)[-1] == ["fix:x"]

    text, _ = await press(context, "fix:ok")
    assert text.startswith("👍 Оставил: 48 000 UZS • OOO SHAVI CAFE")
    assert "\n\n🔎 2 из 2. ❓ перевод: VISA MC UZS 2 UZCARD OB" in text

    await press(context, "fix:c:0")
    text, markup = await press(context, "fix:s:0")
    assert text.startswith("✅ Исправил (строка 4192)")
    assert text.endswith("✅ Проверено 2, исправлено 1.")
    assert markup is None
    assert context.user_data[LAST_WRITE]["checks"] == {}
    [call] = context.bot.edit_message_reply_markup.await_args_list
    assert (call.kwargs["chat_id"], call.kwargs["message_id"]) == (1, 77)
    assert callbacks(call.kwargs["reply_markup"])[0] == ["last:fix", "last:undo"]


async def test_stop_leaves_the_rest_for_later() -> None:
    context = checking(ROWS, {0: "🆕", 2: "❓"})
    await open_check(context)
    await press(context, "fix:ok")
    text, _ = await press(context, "fix:x")
    assert text == "Проверку остановил: проверено 1 из 2."
    assert context.user_data[LAST_WRITE]["checks"] == {2: "❓"}
    menu = context.bot.edit_message_reply_markup.await_args.kwargs["reply_markup"]
    assert menu.inline_keyboard[0][0].text == "🔎 Проверить (1)"
    text, _ = await open_check(context)
    assert text.startswith("🔎 1 из 1. ❓")


async def test_a_stopped_check_updates_the_buttons_on_the_card() -> None:
    context = checking(ROWS, {0: "🆕", 2: "❓"})
    context.user_data[LAST_WRITE]["on_card"] = True
    await open_check(context)
    await press(context, "fix:ok")
    await press(context, "fix:x")
    call = context.bot.edit_message_reply_markup.await_args
    assert (call.kwargs["chat_id"], call.kwargs["message_id"]) == (1, 77)
    assert callbacks(call.kwargs["reply_markup"]) == [
        ["card:check"],
        ["card:fix", "card:undo"],
    ]


async def test_nothing_left_to_check() -> None:
    text, markup = await open_check(checking(ROWS, {}))
    assert text == "Проверять нечего: все строки этой записи уже проверены."
    assert markup is None


async def test_check_buttons_of_an_older_write_are_stale() -> None:
    context = checking(ROWS[:1], {0: "🆕"})
    fix.inline_check(context)
    context.user_data[LAST_WRITE] = {
        **context.user_data[LAST_WRITE],
        "first": 4191,
        "rows": [row(comment="AI: KORZINKA")],
    }
    text, _ = await press(context, "fix:ok")
    assert text == fix.STALE
    assert context.bot_data["gs_service"].calls == []


async def test_fixing_a_flagged_row_with_fix_clears_its_check() -> None:
    context = checking(ROWS, {2: "❓"})
    await command(context)
    await press(context, "fix:r:2")
    await press(context, "fix:c:0")
    await press(context, "fix:s:0")
    assert context.user_data[LAST_WRITE]["checks"] == {}


async def test_lost_menu_message_does_not_break_the_check() -> None:
    context = checking(ROWS[:1], {0: "🆕"})
    context.bot.edit_message_reply_markup = AsyncMock(
        side_effect=BadRequest("Message to edit not found")
    )
    await open_check(context)
    text, _ = await press(context, "fix:ok")
    assert text.endswith("✅ Проверено 1, исправлено 0.")


async def test_undo_no_brings_the_check_button_back() -> None:
    context = checking(ROWS[:1], {0: "🆕"})
    query = SimpleNamespace(
        data="last:undo:no", answer=AsyncMock(), edit_message_text=AsyncMock()
    )
    await last_write.button(SimpleNamespace(callback_query=query), context)
    markup = query.edit_message_text.await_args.kwargs["reply_markup"]
    assert markup.inline_keyboard[0][0].text == "🔎 Проверить (1)"
