from telegram import Update, Message
from telegram.ext import ContextTypes
import logging
from app.utils.keyboards import generate_sources_keyboard, generate_categories_keyboard
from app import config

logger = logging.getLogger(__name__)


def track_message(context: ContextTypes.DEFAULT_TYPE, message: Message):
    """Track a bot message for later cleanup."""
    if 'bot_messages' not in context.user_data:
        context.user_data['bot_messages'] = []
    context.user_data['bot_messages'].append(message.message_id)


async def clear_tracked_messages(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Delete all tracked bot messages."""
    message_ids = context.user_data.get('bot_messages', [])
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            logger.debug(f"Could not delete message {msg_id}: {e}")
    context.user_data['bot_messages'] = []


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, success_message: str = None):
    """Show the main menu (source selection + categories)."""
    sources = context.bot_data.get("sources", [])
    current_source = context.user_data.get('source')

    # Build welcome text
    if success_message:
        welcome_text = success_message + "\n\n"
    else:
        welcome_text = ""

    if current_source:
        derived_currency = get_currency_from_source(current_source)
        welcome_text += f"Текущий источник: {current_source} (Валюта: {derived_currency})"
    else:
        welcome_text += "Выберите источник:"

    # Send source keyboard
    msg1 = await update.effective_message.reply_text(
        welcome_text,
        reply_markup=generate_sources_keyboard(sources, current_source)
    )
    track_message(context, msg1)

    # Send categories keyboard if source is selected
    if current_source:
        msg2 = await update.effective_message.reply_text(
            "Выбери категорию или отправь описание траты:",
            reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
        )
        track_message(context, msg2)

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

    # Clear any previous tracked messages on /start
    chat_id = update.effective_chat.id
    await clear_tracked_messages(context, chat_id)

    if current_source:
        derived_currency = get_currency_from_source(current_source)
        welcome_text += f'\nВыбери действие:'
        welcome_text += f'\nТекущий источник: {current_source} (Валюта: {derived_currency})'

        msg1 = await update.message.reply_text(
            welcome_text,
            reply_markup=generate_sources_keyboard(sources, current_source)
        )
        track_message(context, msg1)
        msg2 = await update.message.reply_text(
            'Выбери категорию или отправь сообщение с тратой (текст/фото):',
            reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
        )
        track_message(context, msg2)
    else:
        welcome_text += f'\nИсточник не выбран. Сначала выберите источник:'
        msg = await update.message.reply_text(
            welcome_text,
            reply_markup=generate_sources_keyboard(sources, current_source)
        )
        track_message(context, msg)
