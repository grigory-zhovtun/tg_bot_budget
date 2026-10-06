"""Вкладка план-факта нового месяца: создаётся сама, владелец получает сообщение."""

import asyncio
import logging

from telegram.ext import ContextTypes

from app import config
from app.domain import local_today
from app.services.google_sheets import GoogleSheetsService, MonthTab

logger = logging.getLogger(__name__)


def owner_chat_id() -> int | None:
    """Куда писать владельцу: чат ежедневного отчёта или первый разрешённый id."""
    if config.ANALYTICS_CHAT_ID:
        try:
            return int(config.ANALYTICS_CHAT_ID)
        except ValueError:
            logger.warning("ANALYTICS_CHAT_ID is not a number")
    return min(config.ALLOWED_USER_IDS) if config.ALLOWED_USER_IDS else None


def describe(tab: MonthTab) -> str | None:
    """Текст для владельца или None, если ничего не поменялось."""
    if tab.created:
        lines = [
            f"📅 Новый месяц: создал вкладку «{tab.title}», "
            f"план скопирован с «{tab.source}»."
        ]
        if tab.hidden:
            lines.append(f"Скрыл прошлые месяцы: {', '.join(tab.hidden)}.")
        lines.append("Поправьте план во вкладке, если в этом месяце он другой.")
        return "\n".join(lines)
    if tab.dates_fixed:
        return f"📅 Поправил даты месяца во вкладке «{tab.title}» (ячейки M1:M2)."
    return None


async def month_tab_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Раз в сутки и при запуске: вкладка текущего месяца есть и считает свой месяц."""
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    today = local_today(config.ANALYTICS_TIMEZONE)
    try:
        tab = await asyncio.to_thread(gs_service.ensure_month_tab, today)
    except Exception:
        logger.exception("Month tab check failed")
        return
    text = describe(tab)
    if text is None:
        return
    logger.info("Month tab %s: created=%s", tab.title, tab.created)
    chat_id = owner_chat_id()
    if chat_id is None:
        return
    try:
        await context.bot.send_message(chat_id, text)
    except Exception:
        logger.exception("Could not tell the owner about the month tab")
