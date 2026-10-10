"""Кнопки под сводкой записи: «✏️ Исправить запись» и «↩️ Отменить запись».

Кнопки стоят в сообщении с группами сразу под сводкой (клавиатура карт живёт на
самой сводке). «Исправить» открывает тот же диалог, что /fix, отдельным
сообщением; «Отменить» сначала спрашивает в этом же сообщении — промахнуться по
кнопке легче, чем набрать /undo.
"""

from telegram import InlineKeyboardMarkup, Update
from telegram.constants import KeyboardButtonStyle
from telegram.ext import ContextTypes

from app.domain import SheetRow, format_amount
from app.handlers import fix
from app.handlers.common import LAST_WRITE, track_message
from app.handlers.undo import undo_last
from app.utils.keyboards import (
    LAST_FIX,
    LAST_UNDO,
    LAST_UNDO_NO,
    LAST_UNDO_YES,
    action,
    categories_menu,
    category_prompt,
    with_icon,
)


def describe_write(last: dict, icons: dict[str, str] | None) -> str:
    """«строку 4169: 48 000 UZS • 🍔 ЕДА (☕ кофе) • VISA 9120 UZS» или «строки 4169–4171»."""
    rows: list[SheetRow] = last["rows"]
    if len(rows) == 1:
        row = rows[0]
        return (
            f"строку {last['first']}: {format_amount(row)} • {row.category} "
            f"({with_icon(row.subcategory, icons)}) • {row.source}"
        )
    return f"строки {last['first']}–{last['last']} ({len(rows)} шт.)"


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    categories = context.bot_data.get("categories", [])
    source = context.user_data.get("source") or context.bot_data.get("last_source")
    prompt = category_prompt(source) if source else "Категория:"

    if query.data == LAST_FIX:
        # диалог — новым сообщением, меню категорий под сводкой остаётся
        track_message(context, await fix.start_fix(query.message.reply_text, context))
        return

    last = context.user_data.get(LAST_WRITE)
    if not last:
        await query.edit_message_text(
            f"Последней записи уже нет.\n{prompt}",
            reply_markup=categories_menu(categories),
        )
        return
    if query.data == LAST_UNDO:
        confirm = InlineKeyboardMarkup(
            [
                [
                    action("🗑 Да, удалить", LAST_UNDO_YES, KeyboardButtonStyle.DANGER),
                    action("Нет", LAST_UNDO_NO),
                ]
            ]
        )
        icons = context.bot_data.get("icons")
        await query.edit_message_text(
            f"↩️ Удалить {describe_write(last, icons)}?", reply_markup=confirm
        )
    elif query.data == LAST_UNDO_NO:
        await query.edit_message_text(
            prompt, reply_markup=categories_menu(categories, with_actions=True)
        )
    elif query.data == LAST_UNDO_YES:
        result = await undo_last(context)
        await query.edit_message_text(
            f"{result}\n{prompt}", reply_markup=categories_menu(categories)
        )
