import hashlib
import logging
import re
import time
from functools import partial
from typing import Any

from telegram import (
    Bot,
    Chat,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import ContextTypes

from app import config
from app.custom_icons import EMPTY, CustomIcons
from app.handlers.analytics import send_report
from app.rich import Report, note_report
from app.utils.keyboards import (
    categories_menu,
    category_prompt,
    generate_categories_keyboard,
    generate_sources_keyboard,
)
from app.web.auth import launch_token

logger = logging.getLogger(__name__)

# user_data: последняя запись (для /undo) и уже записанные SMS/файлы
LAST_WRITE = "last_write"
SEEN_INPUTS = "seen_inputs"
CHOOSE_SOURCE = "Выберите карту:"
SEEN_TTL = 7 * 24 * 3600
MIN_SMS_LENGTH = 20  # короче — это ручной ввод вроде «48000 латте», повтор законен


def input_fingerprint(
    text: str | None, file_unique_id: str | None = None
) -> str | None:
    """Отпечаток SMS или файла: тот же текст или тот же файл Telegram — тот же ключ."""
    if file_unique_id:
        return f"file:{file_unique_id}"
    normalized = re.sub(r"\s+", " ", text or "").strip().lower()
    if len(normalized) < MIN_SMS_LENGTH:
        return None
    return "text:" + hashlib.sha256(normalized.encode()).hexdigest()[:16]


def seen_input(
    context: ContextTypes.DEFAULT_TYPE, fingerprint: str | None
) -> dict | None:
    """Запись о том, что этот вход уже записан в таблицу (не старше недели)."""
    if fingerprint is None:
        return None
    entry = context.user_data.get(SEEN_INPUTS, {}).get(fingerprint)
    if entry and time.time() - entry["at"] < SEEN_TTL:
        return entry
    return None


def premium_icons(context: ContextTypes.DEFAULT_TYPE) -> CustomIcons:
    """Картинки для кнопок — только пока у владельца бота Telegram Premium."""
    found = context.bot_data.get("custom_icons")
    if found is None or not context.bot_data.get("premium"):
        return EMPTY
    return found


def app_url(user_id: int) -> str | None:
    """Ссылка на Mini App с личным токеном; None — страницы нет (локальный опрос)."""
    if not config.WEBAPP_URL:
        return None
    return f"{config.WEBAPP_URL}?launch={launch_token(user_id, config.TELEGRAM_TOKEN)}"


def sources_keyboard(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, current: str | None = None
) -> ReplyKeyboardMarkup:
    """Клавиатура карт с кнопкой Mini App: токен в ссылке обновляется с клавиатурой."""
    return generate_sources_keyboard(
        context.bot_data.get("sources", []), current, app_url(chat_id)
    )


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
    chat: Chat,
    context: ContextTypes.DEFAULT_TYPE,
    success_message: Report | str | None = None,
    actions: bool = False,
    check: int = 0,
    menu: tuple[str, InlineKeyboardMarkup] | None = None,
) -> Message | None:
    """Сводка (или выбранная карта) с клавиатурой карт, под ней — группы.

    chat — куда отправить: чат сообщения пользователя или, для Mini App, его личный
    чат с ботом. check — сколько строк ждут проверки («🔎 Проверить»), menu — своё
    сообщение вместо групп (проверка единственной строки). Возвращает сообщение
    с группами: проверка потом обновляет его кнопки.
    """
    current_source = context.user_data.get("source")

    if isinstance(success_message, Report):
        report = success_message
    else:  # пустой или «невидимый» текст Telegram отвергает (Message_empty)
        report = Report(
            success_message
            or (f"💳 {current_source}" if current_source else CHOOSE_SOURCE)
        )
    sent = await send_report(
        context.bot,
        chat.id,
        report,
        reply_markup=sources_keyboard(context, chat.id, current_source),
        send=chat.send_message,
    )
    for message in sent.messages:
        track_message(context, message)

    if not current_source:
        return None
    prompt, markup = menu or (
        category_prompt(current_source),
        categories_menu(
            context.bot_data.get("categories", []),
            with_actions=actions,
            custom=premium_icons(context),
            check=check,
        ),
    )
    msg2 = await chat.send_message(prompt, reply_markup=markup)
    track_message(context, msg2)
    return msg2


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

    # После перезапуска user_data пуст — берём карту последней записи
    current_source = context.user_data.get("source") or context.bot_data.get(
        "last_source"
    )
    if current_source:
        context.user_data["source"] = current_source

    # Clear any previous tracked messages on /start
    chat_id = update.effective_chat.id
    await clear_tracked_messages(context, chat_id)

    # Show keyboards
    msg1 = await update.message.reply_text(
        f"👋 {user.first_name}",
        reply_markup=sources_keyboard(
            context, update.effective_chat.id, current_source
        ),
    )
    track_message(context, msg1)

    if current_source:
        msg2 = await update.message.reply_text(
            category_prompt(current_source),
            reply_markup=generate_categories_keyboard(
                context.bot_data.get("categories", []), premium_icons(context)
            ),
        )
        track_message(context, msg2)


async def _note(bot: Bot, user_id: int, text: str, **options: Any) -> Message:
    """Запасной текст заметки — bot.send_message(chat_id=…, text=…), как раньше."""
    return await bot.send_message(chat_id=user_id, text=text, **options)


async def notify_others(bot: Bot, author_id: int, text: str) -> None:
    """Сообщение всем пользователям, кроме автора; сбой отправки — только в лог."""
    for user_id in sorted(config.ALLOWED_USER_IDS - {author_id}):
        try:
            await send_report(
                bot, user_id, note_report(text), send=partial(_note, bot, user_id)
            )
        except Exception:
            logger.warning("Could not tell user_id=%s about the change", user_id)
