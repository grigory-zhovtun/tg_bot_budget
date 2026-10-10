import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from functools import partial
from io import BytesIO
from typing import Any

from telegram import Bot, Message, Update
from telegram.error import BadRequest, TelegramError
from telegram.ext import ContextTypes

from app import config
from app.errors import user_message

logger = logging.getLogger(__name__)

CHUNK_SIZE = 4000  # лимит Telegram — 4096 символов
DRAFT_EVERY_SECONDS = 1.0  # как часто обновлять черновик /advice

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
    """/advice: выводы Gemini по цифрам месяца, текст появляется по мере генерации."""
    ai_service = context.bot_data.get("ai_service")
    analytics_service = context.bot_data.get("analytics_service")
    if not ai_service or not analytics_service:
        await update.message.reply_text("AI сервис не доступен.")
        return
    try:
        numbers = await asyncio.to_thread(analytics_service.advice_context)
    except Exception as e:
        logger.exception("Advice numbers failed")
        await update.message.reply_text(
            f"Произошла ошибка при анализе: {user_message(e)}"
        )
        return

    chat_id, draft_id = update.effective_chat.id, update.message.message_id
    text = ""
    try:
        await _draft(context.bot, chat_id, draft_id, "")  # «Думаю…»
        shown = 0.0
        async for text in ai_service.stream_analysis(numbers):
            if time.monotonic() - shown >= DRAFT_EVERY_SECONDS:
                await _draft(context.bot, chat_id, draft_id, text[:CHUNK_SIZE])
                shown = time.monotonic()
    except Exception:
        logger.warning(
            "Streaming advice failed, asking without streaming", exc_info=True
        )
        text = ""
    if not text:
        try:
            text = await ai_service.analyze_finances(numbers)
        except Exception as e:
            logger.exception("Advice failed")
            await update.message.reply_text(
                f"Произошла ошибка при анализе: {user_message(e)}"
            )
            return
    if len(text) > CHUNK_SIZE:
        text = text[: CHUNK_SIZE - 100] + "..."
    await send_markdown(update.message.reply_text, text)


async def _draft(bot: Bot, chat_id: int, draft_id: int, text: str) -> None:
    """Черновик ответа в чате; если клиент их не умеет — просто ждём итог."""
    try:
        await bot.send_message_draft(chat_id, draft_id, text)
    except TelegramError as error:
        logger.debug("Draft not shown: %s", error)


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


async def plan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/plan: план-факт месяца без AI — сколько потрачено и сколько осталось."""
    analytics_service = context.bot_data.get("analytics_service")
    try:
        text = await asyncio.to_thread(analytics_service.plan_report)
    except Exception as e:
        logger.exception("Plan report failed")
        await update.message.reply_text(
            f"Не удалось собрать план-факт: {user_message(e)}"
        )
        return
    await update.message.reply_text(text)


async def send_morning_brief(bot: Bot, chat_id: int, analytics_service: Any) -> None:
    """Утреннее сообщение с лимитом (JobQueue); 🎉, если вчера уложились. Ошибки — в лог."""
    try:
        text, party = await asyncio.to_thread(analytics_service.morning_message)
        effect = config.CELEBRATE_EFFECT_ID if party else None
        try:
            await bot.send_message(chat_id, text, message_effect_id=effect)
        except BadRequest:
            if effect is None:
                raise
            logger.warning("Message effect %s rejected, sending without it", effect)
            await bot.send_message(chat_id, text)
        logger.info("Morning brief sent%s", " with the party effect" if effect else "")
    except Exception:
        logger.exception("Failed to send the morning brief")


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/today: лимит на сегодня, сколько уже потрачено и сколько осталось."""
    analytics_service = context.bot_data.get("analytics_service")
    try:
        text = await asyncio.to_thread(analytics_service.morning_brief, now=True)
    except Exception as e:
        logger.exception("Today brief failed")
        await update.message.reply_text(
            f"Не удалось посчитать лимит: {user_message(e)}"
        )
        return
    await update.message.reply_text(text)


async def send_weekly_digest(bot: Bot, chat_id: int, analytics_service: Any) -> None:
    """Воскресная сводка (JobQueue). Ошибки только в лог."""
    try:
        text = await asyncio.to_thread(analytics_service.weekly_digest)
        for chunk in split_text(text):
            await bot.send_message(chat_id, chunk)
        logger.info("Weekly digest sent")
    except Exception:
        logger.exception("Failed to send the weekly digest")
