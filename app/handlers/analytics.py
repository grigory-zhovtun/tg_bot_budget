import asyncio
import logging
from collections.abc import Awaitable, Callable
from functools import partial
from io import BytesIO
from typing import Any

from telegram import Bot, Message, Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from app.errors import user_message

logger = logging.getLogger(__name__)

CHUNK_SIZE = 4000  # лимит Telegram — 4096 символов

Send = Callable[..., Awaitable[Message]]


def split_text(text: str, limit: int = CHUNK_SIZE) -> list[str]:
    """Куски не длиннее limit по границам строк — так не рвётся Markdown-разметка."""
    chunks: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        while len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        if len(current) + len(line) > limit:
            chunks.append(current)
            current = ""
        current += line
    if current:
        chunks.append(current)
    return chunks


async def send_markdown(send: Send, text: str) -> None:
    """Каждый кусок с Markdown; если разметка сломана — этот же кусок без неё.

    Раньше при ошибке в середине весь текст отправлялся заново, и начало дублировалось.
    """
    for chunk in split_text(text, CHUNK_SIZE):
        try:
            await send(chunk, parse_mode="Markdown")
        except BadRequest:
            await send(chunk, parse_mode=None)


async def _send_charts(send_photo: Send, charts: list[tuple[str, BytesIO]]) -> None:
    for caption, chart in charts:
        with chart:
            await send_photo(photo=chart, caption=caption)


async def _delete_quietly(message: Message) -> None:
    try:
        await message.delete()
    except BadRequest:
        logger.debug("Status message already gone")


async def advice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /advice command: AI analysis of the transaction history."""
    ai_service = context.bot_data.get("ai_service")
    analytics_service = context.bot_data.get("analytics_service")
    if not ai_service or not analytics_service:
        await update.message.reply_text("AI сервис не доступен.")
        return

    status_msg = await update.message.reply_text(
        "🤖 Анализирую ваши финансы... Это займет пару секунд."
    )
    try:
        numbers = await asyncio.to_thread(analytics_service.advice_context)
        advice_text = await ai_service.analyze_finances(numbers)
    except Exception as e:
        logger.exception("Advice failed")
        await status_msg.edit_text(f"Произошла ошибка при анализе: {user_message(e)}")
        return

    await _delete_quietly(status_msg)
    await send_markdown(update.message.reply_text, advice_text)


async def analytics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /analytics command: 3-day report with charts."""
    analytics_service = context.bot_data.get("analytics_service")
    if not analytics_service:
        await update.message.reply_text("Сервис аналитики не доступен.")
        return

    status_msg = await update.message.reply_text("📊 Формирую аналитику за 3 дня...")
    try:
        report_text, charts = await asyncio.to_thread(
            analytics_service.generate_3day_report
        )
    except Exception as e:
        logger.exception("Analytics failed")
        await status_msg.edit_text(
            f"Ошибка при формировании аналитики: {user_message(e)}"
        )
        return

    await _delete_quietly(status_msg)
    await send_markdown(update.message.reply_text, report_text)
    await _send_charts(update.message.reply_photo, charts)


async def send_daily_analytics(bot: Bot, chat_id: int, analytics_service: Any) -> None:
    """Ежедневный отчёт (JobQueue). Ошибки только в лог — пользователь ничего не ждёт."""
    try:
        report_text, charts = await asyncio.to_thread(
            analytics_service.generate_3day_report
        )
        await send_markdown(partial(bot.send_message, chat_id), report_text)
        await _send_charts(partial(bot.send_photo, chat_id), charts)
        logger.info("Daily analytics sent to chat %s", chat_id)
    except Exception:
        logger.exception("Failed to send daily analytics")
