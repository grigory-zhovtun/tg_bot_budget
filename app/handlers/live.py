"""Живой статус в чате: реакции на сообщение и «Думаю…» (Bot API 7.0 и 9.3).

SMS и скрины остаются в чате: пока Gemini разбирает — 👀, записал — 👍, не
понял — 🤔. Вместо служебного «🔍» Telegram показывает «Думаю…» (черновик
сообщения живёт 30 секунд, поэтому обновляем его, пока ждём ответа).
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from telegram.error import TelegramError
from telegram.ext import ContextTypes

from app.handlers.common import track_message

logger = logging.getLogger(__name__)

DRAFT_REFRESH_SECONDS = 20


async def react(message: Any, emoji: str) -> None:
    """Реакция бота на сообщение; если в чате реакции выключены — не страшно."""
    try:
        await message.set_reaction(emoji)
    except TelegramError as error:
        logger.debug("Reaction %s was not set: %s", emoji, error)


@asynccontextmanager
async def thinking(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, draft_id: int
) -> AsyncIterator[None]:
    """«Думаю…» в чате, пока идёт разбор; старым клиентам — прежний «🔍»."""
    bot = context.bot
    try:
        await bot.send_message_draft(chat_id, draft_id, "")
    except TelegramError as error:
        logger.debug("Message drafts are not available: %s", error)
        track_message(context, await bot.send_message(chat_id, "🔍"))
        yield
        return

    async def refresh() -> None:
        while True:
            await asyncio.sleep(DRAFT_REFRESH_SECONDS)
            try:
                await bot.send_message_draft(chat_id, draft_id, "")
            except TelegramError:
                return

    task = asyncio.create_task(refresh())
    try:
        yield
    finally:
        task.cancel()
