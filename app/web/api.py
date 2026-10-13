"""JSON API Mini App: справочник для ввода, запись траты, отмена последней записи.

Запись идёт той же функцией, что ручной ввод в чате (save_rows): в чат приходит
обычная сводка с «Исправить»/«Отменить», а /undo и /fix видят эту запись. Повтор с
тем же entry_id не пишет вторую строку.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any

from gspread.exceptions import WorksheetNotFound
from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from telegram import Bot, Chat
from telegram.ext import Application, CallbackContext

from app import config
from app.custom_icons import leading_emoji
from app.domain import currency_of, entry_row, local_today, month_title
from app.handlers.messages import save_rows, seen_input
from app.handlers.undo import undo_last
from app.services.dashboard import Dashboard
from app.services.plan import (
    ArticleChange,
    EventChange,
    EventValue,
    GoalChange,
    LoadedPlan,
    PlanChanges,
    change_note,
    load_plan,
    save_plan,
    shift_month,
)
from app.web.auth import WebUser, authenticate
from app.web.schemas import (
    BootstrapOut,
    DashboardOut,
    DayPointOut,
    ErrorEnvelope,
    ErrorOut,
    ExpenseIn,
    ExpenseOut,
    ForecastItemOut,
    FrozenOut,
    GroupOut,
    GroupPlanOut,
    MessageOut,
    PlanArticleOut,
    PlanConflictOut,
    PlanEventOut,
    PlanGoalOut,
    PlanHistoryOut,
    PlanItemOut,
    PlanOut,
    PlanSaveIn,
    RowsOut,
    RunwayOut,
    SourceOut,
    SubcategoryOut,
    SubscriptionOut,
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
    "month": "Можно открыть только текущий или следующий месяц",
    "rows": "Статьи не подходят — обновите план",
    "events": "Строка списка не подходит — проверьте название, день, сумму и валюту",
    "goal": "Цель не подходит — проверьте сумму и валюту",
    "changes": "Нет изменений — нечего сохранять",
}
SHEETS_DOWN = "Таблица не ответила — попробуйте ещё раз"
PLAN_LOCK = "plan_lock"  # bot_data: одно сохранение плана за раз — проверка и запись
CONFLICT = "Пока вы правили, план поменяли — проверьте подсвеченные строки"


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


def dashboard_out(
    found: Dashboard | None, today: date, icons: dict[str, str]
) -> DashboardOut:
    """Сводка для экрана; без вкладки месяца — только статус, без прогноза — группы."""
    month = month_title(today)
    if found is None:
        return DashboardOut(status="no_month_tab", month=month, today=today)
    groups = [
        GroupPlanOut(
            name=group.name,
            plan=group.plan,
            fact=group.fact,
            items=[
                PlanItemOut(
                    name=line.subcategory,
                    icon=icons.get(line.subcategory, ""),
                    plan=line.plan,
                    fact=line.fact,
                )
                for line in group.items
            ],
        )
        for group in found.groups
    ]
    budget = found.budget
    if budget is None:
        return DashboardOut(
            status="no_forecast", month=month, today=today, groups=groups
        )
    runway = None
    if budget.runway is not None:
        r = budget.runway
        runway = RunwayOut(
            balance=r.balance,
            pace=r.pace,
            last_day=r.last_day,
            short=r.short,
            until=r.until,
            dollars=r.dollars,
        )
    frozen = None
    if budget.frozen is not None and budget.frozen_currency:
        frozen = FrozenOut(
            amount=budget.frozen,
            currency=budget.frozen_currency,
            uzs=budget.frozen_uzs,
            change=budget.frozen_change,
        )
    return DashboardOut(
        status="ok",
        month=month,
        today=today,
        limit=budget.limit,
        spent_today=budget.spent_today,
        # как /today: без лимита (отстаём от плана) всё потраченное — сверх него
        left_today=max(budget.limit, 0) - budget.spent_today,
        plan_per_day=budget.plan_per_day,
        days_left=budget.days_left,
        balance=budget.balance,
        planned_balance=budget.planned_balance,
        frozen=frozen,
        groups=groups,
        daily=[
            DayPointOut(day=point.day, plan=point.plan, fact=point.fact)
            for point in found.daily
        ],
        upcoming=[
            ForecastItemOut(
                name=item.name,
                day=item.day,
                amount=item.amount,
                currency=item.currency,
                uzs=item.uzs,
            )
            for item in found.upcoming
        ],
        subscriptions=[
            SubscriptionOut(
                name=sub.name,
                day=sub.day,
                amount=sub.amount,
                currency=sub.currency,
                uzs=sub.uzs,
                state=sub.state,
            )
            for sub in found.subscriptions
        ],
        runway=runway,
    )


def plan_month(text: str | None, today: date) -> date | None:
    """«2026-10» → 01.10.2026, если это текущий или следующий месяц."""
    try:
        first = datetime.strptime(text or "", "%Y-%m").date()
    except ValueError:
        return None
    current = today.replace(day=1)
    return first if first in (current, shift_month(current, 1)) else None


def plan_out(
    first: date, today: date, loaded: LoadedPlan | None, icons: dict[str, str]
) -> PlanOut:
    """План месяца для экрана; без вкладки — только статус."""
    month, title = f"{first:%Y-%m}", month_title(first)
    if loaded is None:
        status = "no_month_tab" if first == today.replace(day=1) else "not_created"
        return PlanOut(month=month, title=title, status=status, today=today)
    history, plan = loaded.history, loaded.plan
    rows = []
    for article in plan.articles:
        average, last, current = history.of(article.key)
        rows.append(
            PlanArticleOut(
                key=article.key,
                kind=article.kind,
                group=article.group,
                subcategory=article.subcategory,
                icon=icons.get(article.subcategory, ""),
                currency=article.currency,
                plan=article.plan,
                average=average,
                last=last,
                current=current,
                editable=article.editable,
            )
        )
    events = None
    if plan.events is not None:
        events = [
            PlanEventOut(
                slot=e.slot,
                name=e.name,
                day=e.day,
                amount=e.amount,
                currency=e.currency,
            )
            for e in plan.events
        ]
    goal = None
    if plan.goal is not None:
        goal = PlanGoalOut(amount=plan.goal.amount, currency=plan.goal.currency)
    return PlanOut(
        month=month,
        title=title,
        status="ok",
        today=today,
        rates=dict(loaded.rates.to_uzs),
        history=PlanHistoryOut(
            average=[f"{m:%Y-%m}" for m in history.average_months],
            last=f"{history.last_month:%Y-%m}",
            current=f"{history.current_month:%Y-%m}",
            current_days=history.current_days,
        ),
        rows=rows,
        events=events,
        goal=goal,
    )


def plan_changes(body: PlanSaveIn) -> PlanChanges:
    """Тело запроса → правки для services.plan (суммы — float, как в таблице)."""
    goal = None
    if body.goal is not None:
        goal = GoalChange(
            (body.goal.base.amount, body.goal.base.currency),
            (float(body.goal.value.amount), body.goal.value.currency),
        )
    return PlanChanges(
        articles=tuple(ArticleChange(c.key, c.base, float(c.plan)) for c in body.rows),
        events=tuple(
            EventChange(
                c.slot,
                (c.base.name, c.base.day, c.base.amount, c.base.currency),
                (
                    None
                    if c.value is None
                    else EventValue(
                        c.value.name,
                        c.value.day,
                        float(c.value.amount),
                        c.value.currency,
                    )
                ),
            )
            for c in body.events
        ),
        goal=goal,
    )


async def author_name(bot: Bot, user: WebUser) -> str | None:
    """Имя для уведомления: из initData, иначе из чата с ботом."""
    if user.first_name:
        return user.first_name
    try:
        chat = await bot.get_chat(user.id)
    except Exception:
        logger.warning("No name for user_id=%s", user.id)
        return None
    return chat.first_name or None


async def notify_others(bot: Bot, author_id: int, text: str) -> None:
    """Сообщение всем пользователям, кроме автора; сбой отправки — только в лог."""
    for user_id in sorted(config.ALLOWED_USER_IDS - {author_id}):
        try:
            await bot.send_message(chat_id=user_id, text=text)
        except Exception:
            logger.warning("Could not tell user_id=%s about the plan", user_id)


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

    async def dashboard(request: Request) -> JSONResponse:
        user = current_user(request)
        today = local_today(config.ANALYTICS_TIMEZONE)
        analytics = application.bot_data["analytics_service"]
        try:
            found = await asyncio.to_thread(analytics.dashboard, today)
        except Exception:
            logger.exception("Dashboard failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        icons: dict[str, str] = application.bot_data.get("icons", {})
        return JSONResponse(dashboard_out(found, today, icons).model_dump(mode="json"))

    async def month_plan(request: Request) -> JSONResponse:
        user = current_user(request)
        today = local_today(config.ANALYTICS_TIMEZONE)
        first = plan_month(request.query_params.get("month"), today)
        if first is None:
            return validation_error({"month"})
        gs = application.bot_data["gs_service"]
        try:
            loaded = await asyncio.to_thread(load_plan, gs, month_title(first), today)
        except Exception:
            logger.exception("Plan failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        icons: dict[str, str] = application.bot_data.get("icons", {})
        body = plan_out(first, today, loaded, icons)
        return JSONResponse(body.model_dump(mode="json"))

    async def save_month_plan(request: Request) -> JSONResponse:
        user = current_user(request)
        try:
            body = PlanSaveIn.model_validate_json(await request.body())
        except ValidationError as error:
            return validation_error(
                {str(e["loc"][0]) if e["loc"] else "body" for e in error.errors()}
            )
        today = local_today(config.ANALYTICS_TIMEZONE)
        first = plan_month(body.month, today)
        if first is None:
            return validation_error({"month"})
        changes = plan_changes(body)
        if changes.count() == 0:
            return validation_error({"changes"})
        title = month_title(first)
        gs = application.bot_data["gs_service"]
        lock: asyncio.Lock = application.bot_data.setdefault(PLAN_LOCK, asyncio.Lock())
        try:
            async with lock:
                outcome = await asyncio.to_thread(save_plan, gs, title, changes)
                loaded = None
                if not outcome.invalid:
                    loaded = await asyncio.to_thread(load_plan, gs, title, today)
        except WorksheetNotFound:
            return validation_error({"month"})
        except Exception:
            logger.exception("Plan save failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if outcome.invalid:
            return validation_error(set(outcome.invalid))
        icons: dict[str, str] = application.bot_data.get("icons", {})
        fresh = plan_out(first, today, loaded, icons)
        if outcome.conflicts:
            conflict = PlanConflictOut(
                error=ErrorOut(code="plan_changed", message=CONFLICT),
                plan=fresh,
                conflicts=list(outcome.conflicts),
            )
            return JSONResponse(conflict.model_dump(mode="json"), status_code=409)
        fresh.saved = changes.count()
        logger.info(
            "Plan %s: %s changes by user_id=%s", title, changes.count(), user.id
        )
        name = await author_name(application.bot, user)
        note = change_note(outcome.plan, changes, outcome.rates, first, name, icons)
        await notify_others(application.bot, user.id, note)
        return JSONResponse(fresh.model_dump(mode="json"))

    async def create_next_plan(request: Request) -> JSONResponse:
        user = current_user(request)
        today = local_today(config.ANALYTICS_TIMEZONE)
        first = shift_month(today.replace(day=1), 1)
        gs = application.bot_data["gs_service"]
        lock: asyncio.Lock = application.bot_data.setdefault(PLAN_LOCK, asyncio.Lock())
        try:
            async with lock:
                tab = await asyncio.to_thread(gs.ensure_next_month_tab, today)
                loaded = await asyncio.to_thread(
                    load_plan, gs, month_title(first), today
                )
        except Exception:
            logger.exception("Next month plan failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if tab.created:
            logger.info("Plan tab %s created by user_id=%s", tab.title, user.id)
        icons: dict[str, str] = application.bot_data.get("icons", {})
        body = plan_out(first, today, loaded, icons)
        return JSONResponse(body.model_dump(mode="json"))

    return [
        Route("/bootstrap", bootstrap),
        Route("/expenses", add_expense, methods=["POST"]),
        Route("/expenses/undo", undo_expense, methods=["POST"]),
        Route("/dashboard", dashboard),
        Route("/plan", month_plan),
        Route("/plan", save_month_plan, methods=["POST"]),
        Route("/plan/next", create_next_plan, methods=["POST"]),
    ]
