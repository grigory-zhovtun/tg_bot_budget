from telegram import Update
from telegram.ext import ContextTypes
import logging
from app.services.google_sheets import GoogleSheetsService
from app.handlers.common import start

logger = logging.getLogger(__name__)

async def reboot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reloads categories and sources from Google Sheets."""
    logger.info(f"User {update.effective_user.first_name} requested reboot.")
    
    gs_service: GoogleSheetsService = context.bot_data.get("gs_service")
    if not gs_service:
        await update.message.reply_text("Ошибка: Сервис Google Sheets не инициализирован.")
        return

    await update.message.reply_text("Обновление данных из Google Sheets...")
    
    try:
        categories, subcategories, sources = gs_service.get_categories_and_sources()
        
        context.bot_data["categories"] = categories
        context.bot_data["subcategories"] = subcategories
        context.bot_data["sources"] = sources
        
        logger.info(f"Loaded {len(sources)} sources and {len(categories)} categories.")
        
        # Verify source consistency for the user
        current_source = context.user_data.get('source')
        if current_source and current_source not in sources:
            context.user_data['source'] = sources[0] if sources else None
            await update.message.reply_text(f"Ваш текущий источник '{current_source}' больше не существует. Сброс.")

        await update.message.reply_text(f"Данные успешно обновлены.\nКатегорий: {len(categories)}\nИсточников: {len(sources)}")
        
        # Restart internal logic to refresh keyboards
        await start(update, context)
        
    except Exception as e:
        logger.error(f"Reboot failed: {e}")
        await update.message.reply_text(f"Ошибка при обновлении: {e}")
