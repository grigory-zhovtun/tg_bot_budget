import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

from app.errors import user_message
from app.handlers.common import start
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)


async def reboot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reloads categories and sources from Google Sheets."""
    logger.info("User %s requested reboot.", update.effective_user.id)

    gs_service: GoogleSheetsService = context.bot_data.get("gs_service")
    if not gs_service:
        await update.message.reply_text(
            "Ошибка: Сервис Google Sheets не инициализирован."
        )
        return

    await update.message.reply_text("Обновление данных из Google Sheets...")

    try:
        gs_service.reload()
        categories, subcategories, sources = await asyncio.to_thread(
            gs_service.get_categories_and_sources
        )
        icons = await asyncio.to_thread(gs_service.get_icons)
    except Exception as e:
        logger.exception("Reboot failed")
        await update.message.reply_text(f"Ошибка при обновлении: {user_message(e)}")
        return

    context.bot_data["categories"] = categories
    context.bot_data["subcategories"] = subcategories
    context.bot_data["sources"] = sources
    context.bot_data["icons"] = icons
    logger.info("Loaded %d sources and %d categories.", len(sources), len(categories))

    # Verify source consistency for the user
    current_source = context.user_data.get("source")
    if current_source and current_source not in sources:
        context.user_data["source"] = sources[0] if sources else None
        await update.message.reply_text(
            f"Ваш текущий источник '{current_source}' больше не существует. Сброс."
        )

    await update.message.reply_text(
        f"Данные успешно обновлены.\nКатегорий: {len(categories)}\n"
        f"Источников: {len(sources)}"
    )

    # Restart internal logic to refresh keyboards
    await start(update, context)
