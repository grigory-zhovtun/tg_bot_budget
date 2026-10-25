"""Кнопки записи: «✏️ Исправить», «↩️ Отменить» и «🔎 Проверить».

Клавиатуры групп и карт выключены (/keyboards) — кнопки стоят на самой сводке
(card:…), иначе — в сообщении под ней (last:…), над группами, если они включены
(клавиатура карт живёт на самой сводке). «Исправить» открывает тот же диалог, что
/fix, отдельным сообщением; «Проверить» — строки SMS с пометкой по одной, тоже
отдельным сообщением; «Отменить» сначала спрашивает — промахнуться по кнопке легче,
чем набрать /undo: в меню — в этом же сообщении, на сводке — сменой кнопок, текст
сводки остаётся.
"""

import logging

from telegram import CallbackQuery, InlineKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from app.chat_state import state_of
from app.domain import SheetRow, format_amount
from app.handlers import fix
from app.handlers.analytics import send_report
from app.handlers.common import LAST_WRITE, premium_icons, track_message
from app.handlers.undo import undo_rows
from app.rich import note_report
from app.utils.keyboards import (
    CARD,
    categories_menu,
    menu_prompt,
    record_actions,
    undo_confirm,
    with_icon,
)

logger = logging.getLogger(__name__)

GONE = "Последней записи уже нет"
STALE = "Кнопки работают только у последней записи"


def describe_write(last: dict, icons: dict[str, str] | None) -> str:
    """«строку 4169: 48 000 UZS • 🍔 ЕДА (☕ кофе) • VISA 9090 UZS» или «строки 4169–4171»."""
    rows: list[SheetRow] = last["rows"]
    if len(rows) == 1:
        row = rows[0]
        return (
            f"строку {last['first']}: {format_amount(row)} • {row.category} "
            f"({with_icon(row.subcategory, icons)}) • {row.source}"
        )
    return f"строки {last['first']}–{last['last']} ({len(rows)} шт.)"


def _shown(markup: InlineKeyboardMarkup) -> InlineKeyboardMarkup | None:
    """Пустые кнопки Telegram не нужны: без них кнопки снимаются."""
    return markup if markup.inline_keyboard else None


async def _dialog(
    query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, step: str
) -> None:
    """«Исправить» и «Проверить»: диалог — новым сообщением, кнопки записи остаются."""
    start = fix.start_fix if step == "fix" else fix.start_check
    track_message(context, await start(query.message.reply_text, context))


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    step = query.data.split(":", 1)[1]  # fix, check, undo, undo:yes, undo:no
    if query.data.startswith(CARD):
        await _card_button(update, query, context, step)
        return
    if step in ("fix", "check"):
        await query.answer()
        await _dialog(query, context, step)
        return

    await query.answer()
    last = context.user_data.get(LAST_WRITE)
    groups = (
        last.get("groups", True)
        if last
        else state_of(context.bot_data, update.effective_chat.id).groups
    )
    categories = context.bot_data.get("categories", [])
    source = context.user_data.get("source") or context.bot_data.get("last_source")
    prompt = menu_prompt(source, groups)
    custom = premium_icons(context)
    if not last:
        await query.edit_message_text(
            f"{GONE}.\n{prompt}",
            reply_markup=_shown(
                categories_menu(categories, custom=custom, groups=groups)
            ),
        )
        return
    if step == "undo":
        icons = context.bot_data.get("icons")
        await query.edit_message_text(
            f"↩️ Удалить {describe_write(last, icons)}?", reply_markup=undo_confirm()
        )
    elif step == "undo:no":
        pending = len(last.get("checks", {}))
        menu = categories_menu(categories, True, custom, pending, groups=groups)
        await query.edit_message_text(prompt, reply_markup=menu)
    elif step == "undo:yes":
        result, _ = await undo_rows(context)
        menu = categories_menu(categories, custom=custom, groups=groups)
        await query.edit_message_text(f"{result}\n{prompt}", reply_markup=_shown(menu))


async def _set_buttons(
    query: CallbackQuery, markup: InlineKeyboardMarkup | None
) -> bool:
    """Сменить кнопки сообщения; Telegram отказал — False (кнопки те же — не отказ)."""
    try:
        await query.edit_message_reply_markup(reply_markup=markup)
    except TelegramError as error:
        if "not modified" in str(error).lower():
            return True
        logger.warning("The buttons of a summary were not changed: %s", error)
        return False
    return True


async def _card_button(
    update: Update, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, step: str
) -> None:
    """Кнопки на самой сводке: меняются только кнопки, текст сводки остаётся.

    Работают только у сводки последней записи: на старой «Отменить» удалила бы
    чужие строки — последнюю запись, а не ту, что на карточке.
    """
    last = context.user_data.get(LAST_WRITE)
    card = (query.message.chat_id, query.message.message_id)
    if not last or last.get("menu") != card:
        # бота перезапускали или карточка не последняя — кнопки снимаем
        await query.answer(STALE if last else GONE, show_alert=True)
        await _set_buttons(query, None)
        return
    await query.answer()
    if step in ("fix", "check"):
        await _dialog(query, context, step)
        return
    actions = InlineKeyboardMarkup(
        record_actions(len(last.get("checks", {})), on_card=True)
    )
    if step == "undo":
        if not await _set_buttons(query, undo_confirm(True)):
            # кнопки сводки не поменялись — спрашиваем, как в меню, сообщением
            icons = context.bot_data.get("icons")
            asked = await query.message.reply_text(
                f"↩️ Удалить {describe_write(last, icons)}?",
                reply_markup=undo_confirm(),
            )
            track_message(context, asked)
        return
    if step == "undo:no":
        await _set_buttons(query, actions)
        return
    result, deleted = await undo_rows(context)
    if deleted:  # строк больше нет — и сводки о них тоже
        try:
            await query.message.delete()
        except TelegramError as error:  # уже убрана утром — не страшно
            logger.debug("The card after undo was not deleted: %s", error)
    else:  # не вышло: можно повторить — кнопки снова, нельзя — без кнопок
        again = actions if context.user_data.get(LAST_WRITE) else None
        await _set_buttons(query, again)
    chat = update.effective_chat
    sent = await send_report(
        context.bot, chat.id, note_report(result), send=chat.send_message
    )
    for message in sent.messages:
        track_message(context, message)
