"""Безопасные тексты ошибок для чата и общий обработчик ошибок приложения."""

import logging
import re

import httpx
from google.genai import errors as genai_errors
from telegram.error import Conflict
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

_SECRETS = [
    re.compile(r"\d{6,}:[A-Za-z0-9_-]{30,}"),  # токен Telegram-бота
    re.compile(r"AIza[0-9A-Za-z_-]{20,}"),  # ключ Google API
    re.compile(r"(?i)(key|token|secret)=[^&\s]+"),  # параметры URL
]
MAX_LENGTH = 200


def user_message(error: BaseException) -> str:
    """Первая строка ошибки без секретов, не длиннее MAX_LENGTH символов."""
    if isinstance(error, genai_errors.APIError) and error.code == 429:
        return (
            "Gemini: лимит запросов на сегодня исчерпан — попробуйте позже "
            "или внесите операцию кнопками"
        )
    if isinstance(error, genai_errors.APIError) and error.code == 503:
        return "Gemini сейчас перегружен, попробуйте через минуту"
    if isinstance(error, httpx.TimeoutException):
        return "Gemini не ответил вовремя, попробуйте ещё раз"
    text = (str(error).strip().splitlines() or [type(error).__name__])[0]
    for pattern in _SECRETS:
        text = pattern.sub("•••", text)
    if len(text) > MAX_LENGTH:
        text = text[: MAX_LENGTH - 1] + "…"
    return text or type(error).__name__


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ошибки вне обработчиков: сеть, Telegram, деплой."""
    if isinstance(context.error, Conflict):
        # Во время деплоя Render минуту держит старый и новый инстансы вместе
        logger.warning("Another instance is polling Telegram (normal during deploy)")
        return
    logger.error("Unhandled error", exc_info=context.error)
