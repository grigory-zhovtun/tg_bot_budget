"""Доступ к боту только для разрешённых пользователей Telegram."""

import logging
from collections.abc import Awaitable, Callable

from telegram import Chat, Update
from telegram.ext import ApplicationHandlerStop, ContextTypes

logger = logging.getLogger(__name__)

Gatekeeper = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]


def parse_user_ids(*sources: str | None) -> frozenset[int]:
    """ID из первого непустого источника: «123, 456» или «123 456».

    Нечисловые части пропускаются с предупреждением, чтобы опечатка в переменной
    окружения не роняла бота при старте.
    """
    for raw in sources:
        if not raw or not raw.strip():
            continue
        ids: set[int] = set()
        for part in raw.replace(",", " ").split():
            try:
                ids.add(int(part))
            except ValueError:
                logger.warning("Skipping invalid Telegram user id: %r", part)
        return frozenset(ids)
    return frozenset()


def first_user_id(*sources: str | None) -> int | None:
    """Владелец бота — первый ID первого непустого источника («123, 456» → 123).

    Порядок важен: владельцем считается тот, кто записан первым, а не наименьший ID.
    """
    for raw in sources:
        if not raw or not raw.strip():
            continue
        for part in raw.replace(",", " ").split():
            try:
                return int(part)
            except ValueError:
                continue
        return None
    return None


def make_gatekeeper(allowed: frozenset[int]) -> Gatekeeper:
    """Обработчик для группы -1: пропускает только `allowed`, остальное обрывает.

    Чужому пользователю бот один раз (до перезапуска) показывает его ID — так
    владелец может добавить его в ALLOWED_USER_IDS, не заглядывая в логи.
    """
    notified: set[int] = set()

    async def gatekeeper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        if user is not None and user.id in allowed:
            return

        logger.warning("Access denied for user_id=%s", user.id if user else None)
        if update.callback_query is not None:
            await update.callback_query.answer()
        chat = update.effective_chat
        if (
            user is not None
            and chat is not None
            and chat.type == Chat.PRIVATE
            and user.id not in notified
        ):
            notified.add(user.id)
            await chat.send_message(
                f"⛔ Доступ к боту закрыт. Ваш Telegram ID: {user.id}"
            )
        raise ApplicationHandlerStop

    return gatekeeper
