"""Вкладка план-факта нового месяца: создаётся сама, владелец получает сообщение."""

import asyncio
import logging
from datetime import date

from telegram import Bot
from telegram.ext import ContextTypes

from app import config
from app.domain import local_today
from app.handlers.receipts import sync_now
from app.services.google_sheets import GoogleSheetsService, MonthTab
from app.services.piggy_sync import bot_catalog, goal_cells

logger = logging.getLogger(__name__)


def owner_chat_id() -> int | None:
    """Куда писать владельцу: чат ежедневного отчёта или первый ID из ALLOWED_USER_IDS."""
    if config.ANALYTICS_CHAT_ID:
        try:
            return int(config.ANALYTICS_CHAT_ID)
        except ValueError:
            logger.warning("ANALYTICS_CHAT_ID is not a number")
    return config.OWNER_ID


def describe(tab: MonthTab, today: date) -> str | None:
    """Текст для владельца или None, если ничего не поменялось."""
    hidden = f"Скрыл прошлые месяцы: {', '.join(tab.hidden)}."
    if tab.created:
        lines = [
            f"📅 Новый месяц: создал вкладку «{tab.title}», "
            f"план скопирован с «{tab.source}»."
        ]
        if tab.hidden:
            lines.append(hidden)
        lines.append("Поправьте план во вкладке, если в этом месяце он другой.")
        return "\n".join(lines)
    if tab.revealed:
        lines = [
            (
                (
                    f"📅 Новый месяц: вкладка «{tab.title}» с планом, составленным "
                    "заранее, стала текущей."
                )
                if today.day == 1
                else f"📅 Вкладка «{tab.title}» была скрыта — показал её: это текущий месяц."
            )
        ]
        if tab.hidden:
            lines.append(hidden)
        return "\n".join(lines)
    if tab.hidden:
        return f"📅 Новый месяц: вкладка «{tab.title}» уже открыта.\n{hidden}"
    if tab.dates_fixed:
        return f"📅 Поправил даты месяца во вкладке «{tab.title}» (ячейки M1:M2)."
    return None


async def tell_owner(bot: Bot, text: str) -> None:
    """Сообщение владельцу о вкладке месяца; сбой отправки — только в лог."""
    chat_id = owner_chat_id()
    if chat_id is None:
        return
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        logger.exception("Could not tell the owner about the month tab")


async def month_tab_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Раз в сутки и при запуске: вкладка текущего месяца есть и считает свой месяц."""
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    today = local_today(config.ANALYTICS_TIMEZONE)
    try:
        # новая вкладка получает «Отложить за месяц» по копилкам вместе с датами
        goal = goal_cells(gs_service, today, *bot_catalog(context.bot_data))
        tab = await asyncio.to_thread(gs_service.ensure_month_tab, today, goal)
    except Exception:
        logger.exception("Month tab check failed")
        return
    await sync_now(context.bot_data, today)  # 1-го числа — уже по новой вкладке
    text = describe(tab, today)
    if text is None:
        return
    logger.info("Month tab %s: created=%s", tab.title, tab.created)
    await tell_owner(context.bot, text)
