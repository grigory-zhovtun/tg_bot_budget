"""Кнопки под сводкой записи: «✏️ Исправить запись» и «↩️ Отменить запись»."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

from app.handlers import fix, last_write, messages
from app.handlers.common import LAST_WRITE
from tests.test_messages import (
    make_chat,
    manual_state,
    menu_after_summary,
    sent_message,
    summary,
)

ACTIONS = [
    ("✏️ Исправить запись", "last:fix", "primary"),
    ("↩️ Отменить запись", "last:undo", "danger"),
]


async def written() -> tuple[SimpleNamespace, SimpleNamespace, Any]:
    """Ручная запись «48000 латте» → сводка и меню категорий под ней."""
    update, context, sheets = make_chat("48000 латте", manual_state())
    context.bot_data["icons"] = {"кофе": "☕"}
    await messages.text_handler(update, context)
    return update, context, sheets


def menu(update: SimpleNamespace) -> Any:
    """Второе сообщение после записи: подпись и кнопки категорий."""
    return menu_after_summary(update)


def buttons(markup: Any) -> list[list[tuple[str, str, str | None]]]:
    return [
        [(b.text, b.callback_data, b.style) for b in row]
        for row in markup.inline_keyboard
    ]


async def press(context: SimpleNamespace, data: str) -> SimpleNamespace:
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=SimpleNamespace(
            reply_text=AsyncMock(side_effect=lambda *a, **k: sent_message())
        ),
    )
    await last_write.button(SimpleNamespace(callback_query=query), context)
    return query


async def test_record_gets_fix_and_undo_buttons_above_the_categories() -> None:
    update, _, _ = await written()
    call = menu(update)
    assert call.args[0] == "💳 VISA 9120 UZS — выберите категорию"
    rows = buttons(call.kwargs["reply_markup"])
    assert rows[0] == ACTIONS
    assert rows[1][0][1].startswith("cat_")
    assert "/undo" not in summary(update) and "/fix" not in summary(update)


async def test_undo_asks_first_and_no_brings_the_menu_back() -> None:
    _, context, sheets = await written()
    query = await press(context, "last:undo")
    call = query.edit_message_text.await_args
    assert call.args[0] == (
        "↩️ Удалить строку 4169: 48 000 UZS • 🍔 ЕДА (☕ кофе) • VISA 9120 UZS?"
    )
    assert buttons(call.kwargs["reply_markup"]) == [
        [("🗑 Да, удалить", "last:undo:yes", "danger"), ("Нет", "last:undo:no", None)]
    ]
    assert not hasattr(sheets, "undone")  # пока ничего не удалено

    query = await press(context, "last:undo:no")
    call = query.edit_message_text.await_args
    assert call.args[0] == "💳 VISA 9120 UZS — выберите категорию"
    assert buttons(call.kwargs["reply_markup"])[0] == ACTIONS


async def test_undo_yes_deletes_the_rows_and_drops_the_buttons() -> None:
    _, context, sheets = await written()
    query = await press(context, "last:undo:yes")
    first, last, _ = sheets.undone
    assert (first, last) == (4169, 4169)
    call = query.edit_message_text.await_args
    assert call.args[0] == (
        "↩️ Удалил из таблицы: строка 4169.\n💳 VISA 9120 UZS — выберите категорию"
    )
    rows = buttons(call.kwargs["reply_markup"])
    assert all(data.startswith("cat_") for row in rows for _, data, _ in row)
    assert LAST_WRITE not in context.user_data


async def test_buttons_after_the_record_is_gone() -> None:
    _, context, _ = await written()
    context.user_data.pop(LAST_WRITE)
    query = await press(context, "last:undo")
    call = query.edit_message_text.await_args
    assert call.args[0].startswith("Последней записи уже нет")
    assert buttons(call.kwargs["reply_markup"])[0][0][1].startswith("cat_")


async def test_fix_opens_the_same_dialog_as_the_command() -> None:
    _, context, _ = await written()
    query = await press(context, "last:fix")
    call = query.message.reply_text.await_args
    assert call.args[0].startswith("✏️ 48 000 UZS • латте • 🍔 ЕДА (кофе)")
    assert call.kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "fix:c:0"
    assert context.user_data[fix.STATE]["row"] == 0
    query.edit_message_text.assert_not_awaited()  # меню категорий остаётся
