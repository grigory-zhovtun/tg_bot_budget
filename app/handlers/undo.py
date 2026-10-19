"""/undo — удалить строки последней записи, если в таблице их ещё не меняли."""

import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

from app.errors import user_message
from app.handlers.analytics import send_report
from app.handlers.common import LAST_WRITE, SEEN_INPUTS
from app.handlers.receipts import mark_dirty
from app.rich import note_report
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

UNDO_LOCK = "undo_lock"  # bot_data: одна отмена за раз — чат и Mini App вместе


async def undo_last(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Удалить строки последней записи, если в таблице их не меняли; текст ответа.

    Проверка и удаление — два запроса к таблице, а удаление сдвигает строки ниже.
    Поэтому отмены идут по одной (чат обрабатывает сообщения по очереди, Mini App —
    нет), а последняя запись снимается до запроса: повторная отмена её не найдёт.
    """
    lock: asyncio.Lock = context.bot_data.setdefault(UNDO_LOCK, asyncio.Lock())
    async with lock:
        last = context.user_data.pop(LAST_WRITE, None)
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
            context.user_data[LAST_WRITE] = last  # можно повторить
            return f"Не удалось отменить: {user_message(error)}"

    if not deleted:
        return f"В таблице {span} уже изменились — удалите их вручную, если нужно."
    mark_dirty(context.bot_data)
    seen = context.user_data.get(SEEN_INPUTS, {})
    for fingerprint in [last.get("fingerprint"), *last.get("prints", [])]:
        if fingerprint:  # те же SMS после отмены можно прислать снова
            seen.pop(fingerprint, None)
    logger.info("Undo: deleted %s", span)
    return f"↩️ Удалил из таблицы: {span}."


async def undo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = await undo_last(context)
    await send_report(
        context.bot,
        update.effective_chat.id,
        note_report(text),
        send=update.message.reply_text,
    )
