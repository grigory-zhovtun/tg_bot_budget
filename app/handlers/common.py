from telegram import Update
from telegram.ext import ContextTypes
import logging
from app.utils.keyboards import generate_sources_keyboard, generate_categories_keyboard
from app import config

logger = logging.getLogger(__name__)

def get_currency_from_source(source_name: str) -> str:
    if source_name and len(source_name) >= 3:
        return source_name[-3:].upper()
    return config.FALLBACK_CURRENCY

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts the bot and shows the source selection."""
    user = update.effective_user
    logger.info(f"User {user.first_name} (ID: {user.id}) started the bot.")
    
    # Data should be loaded in bot_data
    sources = context.bot_data.get("sources", [])
    
    if not sources:
        await update.message.reply_text(
            "Список источников пуст. Пожалуйста, выполните /reboot после настройки Google Таблицы."
        )
        return

    current_source = context.user_data.get('source')
    
    # Auto-select first if not set
    if not current_source and sources:
        # In original code logic: if not current_source and SOURCES... current_source = None
        # But specifically logic was: if not current_source and SOURCES: current_source = None (to force selection?)
        # Let's stick to the prompt to select source.
        pass

    derived_currency = config.FALLBACK_CURRENCY
    welcome_text = f'Привет, {user.first_name}!'

    if current_source:
        derived_currency = get_currency_from_source(current_source)
        welcome_text += f'\nВыбери действие:'
        welcome_text += f'\nТекущий источник: {current_source} (Валюта: {derived_currency})'
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=generate_sources_keyboard(sources, current_source)
        )
        await update.message.reply_text(
            'Выбери категорию или отправь сообщение с тратой (текст/фото):',
            reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
        )
    else:
        welcome_text += f'\nИсточник не выбран. Сначала выберите источник:'
        await update.message.reply_text(
            welcome_text,
            reply_markup=generate_sources_keyboard(sources, current_source)
        )
