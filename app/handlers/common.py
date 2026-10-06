import logging

from telegram import Message, Update
from telegram.ext import ContextTypes

from app import config
from app.utils.keyboards import generate_categories_keyboard, generate_sources_keyboard

logger = logging.getLogger(__name__)

# user_data: последняя запись (для /undo) и уже записанные SMS/файлы
LAST_WRITE = "last_write"
SEEN_INPUTS = "seen_inputs"
CHOOSE_SOURCE = "Выберите карту:"


def track_message(context: ContextTypes.DEFAULT_TYPE, message: Message):
    """Track a bot message for later cleanup."""
    if "bot_messages" not in context.user_data:
        context.user_data["bot_messages"] = []
    context.user_data["bot_messages"].append(message.message_id)


async def clear_tracked_messages(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Delete all tracked bot messages."""
    message_ids = context.user_data.get("bot_messages", [])
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            logger.debug(f"Could not delete message {msg_id}: {e}")
    context.user_data["bot_messages"] = []


async def show_main_menu(
    update: Update, context: ContextTypes.DEFAULT_TYPE, success_message: str = None
):
    """Show the main menu (source selection + categories)."""
    sources = context.bot_data.get("sources", [])
    current_source = context.user_data.get("source")

    # Пустой или «невидимый» текст Telegram отвергает (Message_empty)
    text = success_message or (
        f"💳 {current_source}" if current_source else CHOOSE_SOURCE
    )
    msg1 = await update.effective_message.reply_text(
        text, reply_markup=generate_sources_keyboard(sources, current_source)
    )
    track_message(context, msg1)

    # Send categories keyboard if source is selected
    if current_source:
        msg2 = await update.effective_message.reply_text(
            "Категория:",
            reply_markup=generate_categories_keyboard(
                context.bot_data.get("categories", [])
            ),
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

    sources = context.bot_data.get("sources", [])

    if not sources:
        await update.message.reply_text("Источники не найдены. /reboot")
        return

    current_source = context.user_data.get("source")

    # Clear any previous tracked messages on /start
    chat_id = update.effective_chat.id
    await clear_tracked_messages(context, chat_id)

    # Show keyboards
    msg1 = await update.message.reply_text(
        f"👋 {user.first_name}",
        reply_markup=generate_sources_keyboard(sources, current_source),
    )
    track_message(context, msg1)

    if current_source:
        msg2 = await update.message.reply_text(
            "Категория:",
            reply_markup=generate_categories_keyboard(
                context.bot_data.get("categories", [])
            ),
        )
        track_message(context, msg2)
