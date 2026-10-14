"""Колонка «Пришло к утру»: сверка ночью и после изменений днём.

Сегодняшние операции колонку не меняют (она «к утру»), поэтому днём сверка нужна
только после записей задним числом и правок списка: их помечает mark_dirty, а
задача раз в INTERVAL_SECONDS сверяет. Ночью (month_tab_job) — всегда.
"""

import asyncio
import logging
from datetime import date
from typing import Any

from telegram.ext import ContextTypes

from app import config
from app.domain import local_today
from app.services.receipts_sync import sync_receipts

logger = logging.getLogger(__name__)

DIRTY = "receipts_dirty"  # bot_data: в fact или в списке что-то поменялось
LOCK = "receipts_lock"  # bot_data: одна сверка за раз
INTERVAL_SECONDS = 120


def mark_dirty(bot_data: dict[str, Any]) -> None:
    """Попросить сверку: задача выполнит её не позже чем через INTERVAL_SECONDS."""
    bot_data[DIRTY] = True


async def sync_now(bot_data: dict[str, Any], today: date) -> int:
    """Сверка под замком; ошибка — в лог, колонку поправит следующая сверка."""
    lock: asyncio.Lock = bot_data.setdefault(LOCK, asyncio.Lock())
    async with lock:
        try:
            written = await asyncio.to_thread(
                sync_receipts, bot_data["gs_service"], today
            )
        except Exception:
            logger.exception("Receipts sync failed")
            return 0
    if written:
        logger.info("Receipts: %d cell(s) of the list updated", written)
    return written


async def receipts_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Раз в INTERVAL_SECONDS: сверка, если после прошлой что-то поменяли."""
    if context.bot_data.pop(DIRTY, False):
        await sync_now(context.bot_data, local_today(config.ANALYTICS_TIMEZONE))
