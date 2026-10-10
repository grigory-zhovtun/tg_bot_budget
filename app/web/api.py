"""JSON API Mini App: справочник для ввода, запись траты, отмена последней записи.

Запись идёт той же функцией, что ручной ввод в чате (save_rows): в чат приходит
обычная сводка с «Исправить»/«Отменить», а /undo и /fix видят эту запись. Повтор с
тем же entry_id не пишет вторую строку.
"""

import logging
from datetime import date, timedelta
from typing import Any

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from telegram import Bot, Chat
from telegram.ext import Application, CallbackContext

from app import config
from app.custom_icons import leading_emoji
from app.domain import currency_of, entry_row, local_today
from app.handlers.messages import save_rows, seen_input
from app.handlers.undo import undo_last
from app.web.auth import WebUser, authenticate
from app.web.schemas import (
    BootstrapOut,
    ErrorEnvelope,
    ErrorOut,
    ExpenseIn,
    ExpenseOut,
    GroupOut,
    MessageOut,
    RowsOut,
    SourceOut,
    SubcategoryOut,
    UserOut,
)

logger = logging.getLogger(__name__)

MAX_DAYS_BACK = 31
PENDING = "app_pending"  # user_data: entry_id записей, которые сейчас пишутся
FIELD_MESSAGES = {
    "amount": "Сумма — больше нуля, до двух знаков после запятой",
    "comment": "Комментарий — до 200 символов",
    "day": "Дата — не раньше чем 31 день назад и не позже сегодня",
    "source": "Такой карты нет — обновите приложение",
    "category": "Такой группы нет — обновите приложение",
    "subcategory": "Такой подкатегории нет — обновите приложение",
}
SHEETS_DOWN = "Таблица не ответила — попробуйте ещё раз"


def error_response(
    status: int, code: str, message: str, fields: dict[str, str] | None = None
) -> JSONResponse:
    body = ErrorEnvelope(error=ErrorOut(code=code, message=message, fields=fields))
    return JSONResponse(
        body.model_dump(mode="json", exclude_none=True), status_code=status
    )


def validation_error(fields: set[str]) -> JSONResponse:
    named = {
        name: FIELD_MESSAGES.get(name, "Повторите запись") for name in sorted(fields)
    }
    return error_response(422, "validation", "Проверьте введённые данные", named)


def current_user(request: Request) -> WebUser:
    """Пользователь запроса; AuthError превращает в 401/403 обработчик сервера."""
    user = authenticate(
        request.headers.get("Authorization"),
        config.TELEGRAM_TOKEN,
        config.ALLOWED_USER_IDS,
    )
    request.state.user_id = user.id  # для лога ошибок
    return user


def user_context(application: Application, user_id: int) -> CallbackContext:
    """Контекст PTB пользователя: те же user_data, что у его чата с ботом."""
    return CallbackContext(application, chat_id=user_id, user_id=user_id)


def private_chat(bot: Bot, user_id: int) -> Chat:
    """Личный чат пользователя с ботом — туда save_rows отправит сводку."""
    chat = Chat(id=user_id, type=Chat.PRIVATE)
    chat.set_bot(bot)
    return chat


def groups_out(bot_data: dict[str, Any]) -> list[GroupOut]:
    icons: dict[str, str] = bot_data.get("icons", {})
    subcategories: dict[str, list[str]] = bot_data.get("subcategories", {})
    groups = []
    for name in bot_data.get("categories", []):
        emoji, title = leading_emoji(name)
        subs = [
            SubcategoryOut(name=sub, icon=icons.get(sub, ""))
            for sub in subcategories.get(name, [])
        ]
        groups.append(GroupOut(name=name, emoji=emoji, title=title, subcategories=subs))
    return groups


def check_entry(entry: ExpenseIn, bot_data: dict[str, Any], today: date) -> set[str]:
    """Поля, которых нет в справочнике бота, и дата вне окна."""
    bad: set[str] = set()
    if entry.source not in bot_data.get("sources", []):
        bad.add("source")
    if entry.category not in bot_data.get("categories", []):
        bad.add("category")
    elif entry.subcategory not in bot_data.get("subcategories", {}).get(
        entry.category, []
    ):
        bad.add("subcategory")
    if entry.day is not None and not (
        today - timedelta(days=MAX_DAYS_BACK) <= entry.day <= today
    ):
        bad.add("day")
    return bad


def routes(application: Application) -> list[Route]:
    """Маршруты /api/*; application — приложение PTB (данные бота и пользователей)."""

    async def bootstrap(request: Request) -> JSONResponse:
        user = current_user(request)
        context = user_context(application, user.id)
        sources: list[str] = context.bot_data.get("sources", [])
        chosen = context.user_data.get("source") or context.bot_data.get("last_source")
        body = BootstrapOut(
            user=UserOut(id=user.id, first_name=user.first_name),
            bot_username=application.bot.username or "",
            today=local_today(config.ANALYTICS_TIMEZONE),
            default_source=chosen if chosen in sources else next(iter(sources), None),
            sources=[SourceOut(name=s, currency=currency_of(s)) for s in sources],
            groups=groups_out(context.bot_data),
        )
        return JSONResponse(body.model_dump(mode="json"))

    async def add_expense(request: Request) -> JSONResponse:
        user = current_user(request)
        try:
            entry = ExpenseIn.model_validate_json(await request.body())
        except ValidationError as error:
            return validation_error(
                {str(e["loc"][0]) if e["loc"] else "body" for e in error.errors()}
            )
        context = user_context(application, user.id)
        today = local_today(config.ANALYTICS_TIMEZONE)
        if bad := check_entry(entry, context.bot_data, today):
            return validation_error(bad)

        fingerprint = f"app:{entry.entry_id}"
        if seen := seen_input(context, fingerprint):
            first, last = seen["rows"]
            same = ExpenseOut(
                status="duplicate", rows=RowsOut(first=first, last=last), lines=[]
            )
            return JSONResponse(same.model_dump(mode="json"))
        pending: set[str] = context.user_data.setdefault(PENDING, set())
        if fingerprint in pending:
            return error_response(409, "in_progress", "Эта запись ещё сохраняется")
        pending.add(fingerprint)
        try:
            row = entry_row(
                float(entry.amount),
                False,
                entry.comment,
                entry.source,
                entry.category,
                entry.subcategory,
                entry.day or today,
            )
            chat = private_chat(application.bot, user.id)
            result = await save_rows(context, chat, [row], [], fingerprint)
        finally:
            pending.discard(fingerprint)
        if result.first is None or result.last is None:
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        logger.info(
            "Mini App write by user_id=%s: rows %s–%s",
            user.id,
            result.first,
            result.last,
        )
        done = ExpenseOut(
            status="written",
            rows=RowsOut(first=result.first, last=result.last),
            lines=list(result.lines),
        )
        return JSONResponse(done.model_dump(mode="json"))

    async def undo_expense(request: Request) -> JSONResponse:
        user = current_user(request)
        message = await undo_last(user_context(application, user.id))
        return JSONResponse(MessageOut(message=message).model_dump(mode="json"))

    return [
        Route("/bootstrap", bootstrap),
        Route("/expenses", add_expense, methods=["POST"]),
        Route("/expenses/undo", undo_expense, methods=["POST"]),
    ]
