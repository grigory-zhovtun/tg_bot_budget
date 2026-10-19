"""Сводка записи из SMS: пометки 🆕/❓, строки «⏭», «🔎 Проверить», отпечатки SMS."""

from datetime import date
from typing import Any

from app.domain import SheetRow
from app.handlers import messages, undo
from app.handlers.common import LAST_WRITE, SEEN_INPUTS
from app.utils.keyboards import categories_menu
from tests.test_messages import FakeSheets, make_chat, menu_after_summary, summary


def sheet_row(amount: float, category: str, sub: str, comment: str) -> SheetRow:
    return SheetRow(
        day=date(2026, 10, 14),
        category=category,
        subcategory=sub,
        amount=amount,
        comment=comment,
        currency="UZS",
        source="VISA 9090 UZS",
    )


ROWS = [
    sheet_row(48000.0, "🍔 ЕДА", "кафе", "OOO SHAVI CAFE>TASHKENT"),
    sheet_row(25000.0, "🍔 ЕДА", "кофе", "VSMCUZSKB to QRonlin; ≈ 2 USD по курсу"),
    sheet_row(30000.0, "🏚️ ДОМ", "продукты", "IP OOO ANGLESEY FOOD"),
]


async def write(**kwargs: Any) -> tuple[Any, Any, FakeSheets]:
    update, context, sheets = make_chat("", {})
    await messages.save_rows(
        context, update.effective_chat, ROWS, [], "text:message", **kwargs
    )
    return update, context, sheets


async def test_flagged_rows_show_the_name_and_notes_close_the_summary() -> None:
    update, _, _ = await write(
        flags=["🆕", "❓", None], notes=["⏭ Пропустил: код подтверждения"]
    )
    lines = summary(update).splitlines()
    assert lines[0] == (
        "✅ 48 000 UZS • 🍔 ЕДА (кафе) • VISA 9090 UZS • 🆕 OOO SHAVI CAFE>TASHKENT"
    )
    assert lines[1].endswith("• VISA 9090 UZS • ❓ VSMCUZSKB to QRonlin")
    assert lines[2] == "✅ 30 000 UZS • 🏚️ ДОМ (продукты) • VISA 9090 UZS"
    assert lines[-1] == "⏭ Пропустил: код подтверждения"


async def test_check_button_and_the_menu_message_are_remembered() -> None:
    update, context, _ = await write(flags=["🆕", "❓", None])
    last = context.user_data[LAST_WRITE]
    assert last["checks"] == {0: "🆕", 1: "❓"}
    markup = menu_after_summary(update).kwargs["reply_markup"]
    first = markup.inline_keyboard[0][0]
    assert (first.text, first.callback_data) == ("🔎 Проверить (2)", "last:check")
    assert [b.callback_data for b in markup.inline_keyboard[1]] == [
        "last:fix",
        "last:undo",
    ]
    chat_id, message_id = last["menu"]
    assert chat_id == 1 and isinstance(message_id, int)


async def test_without_flags_the_menu_is_as_before() -> None:
    update, context, _ = await write()
    markup = menu_after_summary(update).kwargs["reply_markup"]
    assert markup.inline_keyboard[0][0].callback_data == "last:fix"
    assert context.user_data[LAST_WRITE]["checks"] == {}


async def test_each_sms_is_remembered_with_its_row() -> None:
    _, context, _ = await write(prints=["text:a", "text:b", None])
    seen = context.user_data[SEEN_INPUTS]
    assert seen["text:message"]["rows"] == (4169, 4171)
    assert (seen["text:a"]["rows"], seen["text:b"]["rows"]) == (
        (4169, 4169),
        (4170, 4170),
    )
    assert context.user_data[LAST_WRITE]["prints"] == ["text:a", "text:b"]


async def test_undo_forgets_every_sms_of_the_write() -> None:
    _, context, _ = await write(prints=["text:a", "text:b", None])
    await undo.undo_last(context)
    assert not {"text:message", "text:a", "text:b"} & set(
        context.user_data[SEEN_INPUTS]
    )


async def test_failed_write_keeps_no_checks_and_no_prints() -> None:
    class BrokenSheets(FakeSheets):
        def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
            raise ConnectionError("Google is down")

    update, context, _ = make_chat("", {})
    context.bot_data["gs_service"] = BrokenSheets()
    result = await messages.save_rows(
        context,
        update.effective_chat,
        ROWS,
        [],
        None,
        flags=["🆕", None, None],
        prints=["text:a", None, None],
    )
    assert result.written == 0
    assert LAST_WRITE not in context.user_data
    assert "text:a" not in context.user_data.get(SEEN_INPUTS, {})


def test_check_button_needs_a_write_to_check() -> None:
    plain = categories_menu(["🍔 ЕДА"], check=2)
    assert all(
        not b.callback_data.startswith("last:")
        for row in plain.inline_keyboard
        for b in row
    )
