"""Утренняя уборка чатов и первое сообщение после обновления.

В 8:00 в чате каждого из ALLOWED_USER_IDS удаляется вчерашнее с прошлой уборки:
номера от «уборка с №» (system!Q:U) до последнего вчерашнего по журналу сообщений
(app/message_log.py). Пришедшее после полуночи остаётся до следующего утра. Не
удаляются «не удалять №» (сводки недели, снятие клавиатуры карт) и сообщение, на
котором держится клавиатура карт: удалить его — значит убрать клавиатуру с экрана.
Удалить сообщение бот может только в первые 48 часов — уборка идёт каждый день.

Номера сообщений у бота общие для всех его личных чатов (растут со временем), так
что в диапазоне одного чата есть и номера других: их Telegram пропускает, а то, что
другие чаты хранят, уборка не трогает и в этом чате — на всякий случай.
"""

import logging
from collections.abc import Sequence
from datetime import date
from typing import Any

from telegram import ReplyKeyboardRemove, Update
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from app import config
from app.chat_state import finish_sweep, keep_messages, state_of, update_state
from app.domain import local_today
from app.handlers.analytics import Sent, send_report
from app.handlers.common import ANCHOR, CHOOSE_SOURCE, app_url
from app.message_log import MessageLog
from app.rich import Report, heading, para
from app.utils.keyboards import app_markup, generate_sources_keyboard

logger = logging.getLogger(__name__)

LOG = "message_log"  # bot_data: MessageLog
CHUNK = 100  # deleteMessages берёт до 100 номеров
NOTICE_TITLE = "⌨️ Клавиатуры скрыты"
NOTICE = (
    "Чтобы чат был чище, кнопки групп и карт больше не показываются.",
    "Включить — в меню: /keyboards. Приложение — /app.",
)


async def observe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Пришедшее сообщение — в журнал: номер и время (для утренней уборки)."""
    message = update.message
    log: MessageLog | None = context.bot_data.get(LOG)
    if message is not None and log is not None:
        log.record(message.chat_id, message.message_id, message.date)


def _cards(application: Any, chat_id: int) -> Any:
    """Клавиатура карт чата с выбранной картой и кнопкой приложения."""
    bot_data = application.bot_data
    source = application.user_data.get(chat_id, {}).get("source") or bot_data.get(
        "last_source"
    )
    keyboard = generate_sources_keyboard(
        bot_data.get("sources", []), source, app_url(chat_id)
    )
    return source, keyboard


def brief_markup(application: Any, chat_id: int) -> Any:
    """Утренней сводке — клавиатура карт (включены) или кнопка приложения."""
    if state_of(application.bot_data, chat_id).cards:
        return _cards(application, chat_id)[1]
    return app_markup(config.WEBAPP_URL) if config.WEBAPP_URL else None


def anchor_brief(application: Any, chat_id: int, sent: Sent | None) -> None:
    """Сводка с клавиатурой карт — теперь клавиатура держится на ней."""
    if sent is not None and state_of(application.bot_data, chat_id).cards:
        # user_data PTB — только для чтения, словарь пользователя создаёт обращение
        application.user_data[chat_id][ANCHOR] = sent.messages[-1].message_id


async def keep(application: Any, chat_id: int, ids: Sequence[int]) -> None:
    """Не удалять эти сообщения утром (сводка недели)."""
    await keep_messages(application.bot_data, chat_id, ids)


async def _delete(bot: Any, chat_id: int, numbers: list[int]) -> None:
    """Пачками по 100; пачку, которую Telegram не принял, — по одному."""
    for start in range(0, len(numbers), CHUNK):
        chunk = numbers[start : start + CHUNK]
        try:
            await bot.delete_messages(chat_id, chunk)
        except TelegramError as error:
            logger.warning(
                "Chat %s: a batch of %d was refused (%s), deleting one by one",
                chat_id,
                len(chunk),
                error,
            )
            for number in chunk:
                try:
                    await bot.delete_message(chat_id, number)
                except TelegramError as one:
                    logger.debug("Chat %s: message %s stays: %s", chat_id, number, one)


def _kept_elsewhere(application: Any, chat_id: int) -> set[int]:
    """Что хранят другие чаты: «не удалять №» и сообщения с клавиатурой карт."""
    kept: set[int] = set()
    for other, state in application.bot_data.get("chats", {}).items():
        if other == chat_id:
            continue
        kept.update(state.keep)
        anchor = application.user_data.get(other, {}).get(ANCHOR)
        if state.cards and anchor is not None:
            kept.add(anchor)
    return kept


async def clean(application: Any, chat_id: int, today: date) -> None:
    """Убрать в чате вчерашнее и более раннее с прошлой уборки."""
    bot_data = application.bot_data
    last = bot_data[LOG].last_before(chat_id, today)
    if last is None:  # после перезапуска вчера в чате ничего не было
        logger.info("Chat %s: nothing known from yesterday, no sweep", chat_id)
        return
    state = state_of(bot_data, chat_id)
    if state.start is None:  # уборки ещё не было — запоминаем, откуда начать
        await update_state(bot_data, chat_id, start=last + 1)
        return
    user_data = application.user_data.get(chat_id, {})
    spared = set(state.keep) | _kept_elsewhere(application, chat_id)
    if state.cards:
        anchor = user_data.get(ANCHOR)
        if anchor is None or anchor <= last:
            # клавиатура держится на вчерашнем (или неизвестном) сообщении: сначала
            # новое с клавиатурой, потом уборка — иначе вчерашние копились бы
            anchor = await _new_anchor(application, chat_id) or anchor
        if anchor is not None:
            spared.add(anchor)
    numbers = [n for n in range(state.start, last + 1) if n not in spared]
    await _delete(application.bot, chat_id, numbers)
    await finish_sweep(bot_data, chat_id, last)
    if "bot_messages" in user_data:  # удалённые незачем удалять снова
        user_data["bot_messages"] = [n for n in user_data["bot_messages"] if n > last]
    logger.info("Chat %s: swept %d messages up to %d", chat_id, len(numbers), last)


async def _new_anchor(application: Any, chat_id: int) -> int | None:
    """Без звука — «💳 карта» с клавиатурой карт; не отправилось — None."""
    source, keyboard = _cards(application, chat_id)
    try:
        message = await application.bot.send_message(
            chat_id,
            f"💳 {source}" if source else CHOOSE_SOURCE,
            reply_markup=keyboard,
            disable_notification=True,
        )
    except TelegramError as error:
        logger.warning(
            "Chat %s: the cards keyboard was not sent again: %s", chat_id, error
        )
        return None
    application.user_data[chat_id][ANCHOR] = message.message_id
    return message.message_id


async def sweep(application: Any, today: date | None = None) -> None:
    """Утренняя уборка всех чатов; сбой в одном не мешает остальным."""
    day = today or local_today(config.ANALYTICS_TIMEZONE)
    for chat_id in sorted(config.ALLOWED_USER_IDS):
        try:
            await clean(application, chat_id, day)
        except Exception:
            logger.exception("Sweeping chat %s failed", chat_id)


async def first_run(context: ContextTypes.DEFAULT_TYPE) -> None:
    """После обновления — кому ещё не писали: «⌨️ Клавиатуры скрыты». Сообщение
    убирает старую клавиатуру карт с экрана и само остаётся (удалить его — значит,
    приложение Telegram может вернуть старую клавиатуру): уборка начинается со
    следующего номера. Блок system!Q:U не прочитался — молчим: неизвестно, кому
    сообщение уже ушло."""
    application = context.application
    bot_data = application.bot_data
    if not bot_data.get("chats_loaded"):
        return
    report = Report(
        "\n".join([NOTICE_TITLE, *NOTICE]), heading(NOTICE_TITLE) + para(*NOTICE)
    )
    told: list[int] = []
    last = 0
    for chat_id in sorted(config.ALLOWED_USER_IDS):
        if chat_id in bot_data.get("chats", {}):
            continue
        try:
            sent = await send_report(
                context.bot, chat_id, report, reply_markup=ReplyKeyboardRemove()
            )
        except Exception:
            logger.exception(
                "Could not tell chat %s about the hidden keyboards", chat_id
            )
            continue
        told.append(chat_id)
        last = max(last, sent.messages[-1].message_id)
    # номера общие для всех чатов: уборка каждого начинается после всех сообщений
    for chat_id in told:
        await update_state(bot_data, chat_id, start=last + 1)
