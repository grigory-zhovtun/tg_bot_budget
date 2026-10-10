"""/undo — удалить строки последней записи, если в таблице их ещё не меняли."""

import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

from app.errors import user_message
from app.handlers.common import LAST_WRITE, SEEN_INPUTS
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)


async def undo_last(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Удалить строки последней записи, если в таблице их не меняли; текст ответа."""
    last = context.user_data.get(LAST_WRITE)
    if not last:
        return "Нечего отменять: после запуска бота записей не было."
    first, end = last["first"], last["last"]
    span = f"строка {first}" if first == end else f"строки {first}–{end}"
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    try:
        deleted = await asyncio.to_thread(
            gs_service.delete_rows_if_match, first, end, last["rows"]
        )
    except Exception as error:
        logger.exception("Undo failed")
        return f"Не удалось отменить: {user_message(error)}"

    context.user_data.pop(LAST_WRITE, None)
    if not deleted:
        return f"В таблице {span} уже изменились — удалите их вручную, если нужно."
    if fingerprint := last.get("fingerprint"):
        # то же SMS после отмены можно прислать снова
        context.user_data.get(SEEN_INPUTS, {}).pop(fingerprint, None)
    logger.info("Undo: deleted %s", span)
    return f"↩️ Удалил из таблицы: {span}."


async def undo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(await undo_last(context))
