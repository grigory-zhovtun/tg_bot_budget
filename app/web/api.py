"""JSON API Mini App: справочник для ввода, запись траты, отмена последней записи.

Запись идёт той же функцией, что ручной ввод в чате (save_rows): в чат приходит
обычная сводка с «Исправить»/«Отменить», а /undo и /fix видят эту запись. Повтор с
тем же entry_id не пишет вторую строку.
"""

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import asdict
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
from app.domain import INCOME_GROUP, currency_of, entry_row, local_today, month_title
from app.handlers.messages import save_rows, seen_input
from app.handlers.month import describe, tell_owner
from app.handlers.receipts import mark_dirty
from app.handlers.undo import undo_last
from app.services.analytics_service import TRANSFERS_GROUP, SavingsTab, money
from app.services.calendar import CalendarRecord, new_id, read_calendar, record_row
from app.services.cards import CardBalance
from app.services.dashboard import Dashboard
from app.services.forecast import DayArticle, Dip, Evening, MonthForecast, PastDay
from app.services.piggy import (
    CLOSED,
    NOT_FROM,
    SAVED,
    SPENT,
    WRITE_OFF,
    YEARLY,
    Piggy,
    PiggyState,
    distribute,
    goal_of,
)
from app.services.plan import (
    ArticleChange,
    Catalog,
    Event,
    EventChange,
    EventValue,
    GoalChange,
    LoadedPlan,
    MonthPlan,
    NewArticle,
    PlanChanges,
    addable,
    change_note,
    kind_of,
    load_plan,
    save_plan,
    shift_month,
)
from app.services.plan_advice import advice_numbers, checked_points, gather_advice
from app.services.receipts import NOTHING, Line, Receipt, receipt_status
from app.services.savings import Cushion, Savings
from app.web.auth import WebUser, authenticate
from app.web.schemas import (
    AdviceApplyOut,
    AdvicePointOut,
    AmountOut,
    BootstrapOut,
    CalendarIn,
    CalendarRecordOut,
    CardOut,
    CategoryLineOut,
    CategoryOut,
    CushionOut,
    DashboardOut,
    DayArticleOut,
    DayPointOut,
    DaySpendOut,
    EnoughOut,
    ErrorEnvelope,
    ErrorOut,
    ExcludeIn,
    ExpenseIn,
    ExpenseOut,
    ForecastArticleOut,
    ForecastCurrencyOut,
    ForecastDayOut,
    ForecastDipOut,
    ForecastDipsOut,
    ForecastEventOut,
    ForecastItemOut,
    ForecastOut,
    ForecastPastOut,
    ForecastRecordOut,
    ForecastTotalOut,
    FrozenOut,
    GroupOut,
    GroupPlanOut,
    HintOut,
    MessageOut,
    MonthPlanOut,
    MoveIn,
    MoveOut,
    PiggyIn,
    PiggyOut,
    PlanAddableOut,
    PlanAdviceIn,
    PlanAdviceOut,
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
    SaveIn,
    SavingMonthOut,
    SavingsGoalOut,
    SavingsMonthOut,
    SavingsOut,
    SavingsScreenOut,
    SourceOut,
    SpendOut,
    SpentGroupOut,
    SpentItemOut,
    SubcategoryOut,
    SubscriptionOut,
    UntrackedOut,
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
    "new_rows": "Новую статью не добавить — обновите план",
    "changes": "Нет изменений — нечего сохранять",
    "date": "Дата — не раньше сегодня и не дальше чем через год",
    "group": "Такой статьи нет — обновите приложение",
    "currency": "Такой валюты нет у карт",
    "weekdays": "Выберите дни недели",
    "name": "Название — до 60 символов",
    "repeat": "Такого повтора нет — обновите приложение",
}
SHEETS_DOWN = "Таблица не ответила — попробуйте ещё раз"
PLAN_LOCK = "plan_lock"  # bot_data: одно сохранение плана за раз — проверка и запись
CONFLICT = "Пока вы правили, план поменяли — проверьте подсвеченные строки"
NO_SPACE = "Во вкладке нет места для новых статей"
ADVISOR_DOWN = "Советник сейчас недоступен"
RECORD_GONE = "Запись уже удалили — обновите прогноз"
CLOSED_GROUPS = frozenset({INCOME_GROUP, TRANSFERS_GROUP})  # в календаре только траты
MAX_AHEAD_DAYS = 366  # запись — не дальше чем через год
CALENDAR_LOCK = "calendar_lock"  # bot_data: в «Календарь» пишет один запрос за раз
CALENDAR_ENTRIES = "calendar_entries"  # user_data: entry_id уже записанных записей
ENTRY_LIMIT = 200
# Обе модели с повторами — до двух минут; приложение ждёт советника 100 с
ADVICE_SECONDS = 60
# секунд: экраны Mini App считаются из копии листов не старше — переход между
# вкладками не ждёт Google. Запись бота копии сбрасывает, а ?fresh=1 («Обновить»
# и первое открытие приложения) читает таблицу заново
SCREEN_COPY_AGE = 120.0
PIGGY_LOCK = "piggy_lock"  # bot_data: в листы копилок пишет один запрос за раз
PIGGY_ENTRIES = "piggy_entries"  # user_data: entry_id уже записанных правок копилок
PIGGY_YEARS = 5  # срок копилки — не дальше чем через 5 лет
PIGGY_FIELDS = {
    "name": "Название — от 1 до 40 символов",
    "target": "Цель — больше нуля, до двух знаков после запятой",
    "currency": "Такой валюты нет у карт",
    "deadline": "Срок — не раньше сегодня и не дальше чем через 5 лет",
    "repeat": "Каждый год — только со сроком",
    "group": "Такой статьи нет — обновите приложение",
    "subcategory": "Такой подкатегории нет — обновите приложение",
    "amount": "Сумма — больше нуля",
    "kind": "Такого движения нет — обновите приложение",
    "key": "Трата не подходит — обновите «Накопления»",
}
PIGGY_GONE = "Копилку уже закрыли — обновите «Накопления»"
SPEND_GONE = "Трата изменилась — обновите «Накопления»"
CURRENCY_LOCKED = (
    "Валюту не поменять: в копилке есть движения. Закройте её и заведите новую"
)
KIND_TEXT = {"saved": SAVED, "spent": SPENT, "write_off": WRITE_OFF}


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


def screen_age(request: Request) -> float:
    """Сколько секунд может быть копии листов: ?fresh=1 — нисколько."""
    return 0.0 if request.query_params.get("fresh") == "1" else SCREEN_COPY_AGE


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


def card_out(card: CardBalance, today: date) -> CardOut:
    """Карта для экрана: имя без валюты — она у суммы."""
    name = card.name
    if name.upper().endswith(f" {card.currency}"):
        name = name[: -len(card.currency)].strip()
    return CardOut(
        name=name,
        currency=card.currency,
        balance=card.balance,
        uzs=card.uzs,
        bank=card.bank,
        checked=card.checked.date() if card.checked else None,
        matches=card.matches,
        stale=card.stale(today),
    )


def savings_out(savings: Savings | None) -> SavingsOut | None:
    if savings is None:
        return None
    return SavingsOut(
        rate=None if savings.rate is None else round(savings.rate, 4),
        months=[
            SavingMonthOut(
                month=f"{item.month:%Y-%m}",
                income=float(round(item.income)),
                spent=float(round(item.spent)),
                rate=None if item.rate is None else round(item.rate, 4),
            )
            for item in savings.months
        ],
    )


def cushion_out(cushion: Cushion | None) -> CushionOut | None:
    if cushion is None:
        return None
    months = cushion.months
    return CushionOut(
        amount=_rounded(cushion.amount, cushion.currency),
        currency=cushion.currency,
        uzs=float(round(cushion.uzs)),
        monthly=float(round(cushion.monthly)),
        months=None if months is None else round(months, 2),
        target_months=cushion.target_months,
        short_uzs=float(round(cushion.short_uzs)),
        short=_rounded(cushion.short, cushion.currency),
    )


def amount_text(value: float) -> str:
    """«12 000 000», «210», «210,50» — для заметок и ошибок."""
    return money(value) if value == int(value) else f"{value:.2f}".replace(".", ",")


def piggy_text(
    name: str,
    target: float | None,
    currency: str,
    deadline: date | None,
    yearly: bool,
) -> str:
    """«Новый год 12 000 000 UZS к 20.12, каждый год», «Машина, без срока»."""
    text = name if target is None else f"{name} {amount_text(target)} {currency}"
    if deadline is None:
        return f"{text}, без срока"
    text += f" к {deadline:%d.%m}"
    return f"{text}, каждый год" if yearly else text


def piggy_note(action: str, author: str, text: str) -> str:
    """«🐷 Иван — новая копилка: …»; без имени — «🐷 Новая копилка: …»."""
    head = f"{author} — {action}" if author else action[:1].upper() + action[1:]
    return f"🐷 {head}: {text}"


def piggy_error(fields: set[str] | dict[str, str]) -> JSONResponse:
    named = (
        fields
        if isinstance(fields, dict)
        else {
            name: PIGGY_FIELDS.get(name, "Повторите запись") for name in sorted(fields)
        }
    )
    return error_response(422, "validation", "Проверьте введённые данные", named)


def _years_later(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year + years)
    except ValueError:  # 29 февраля
        return day.replace(year=day.year + years, day=28)


def check_piggy(
    body: PiggyIn, bot_data: Mapping[str, Any], today: date, current: Piggy | None
) -> set[str]:
    """Поля копилки, которые не подходят: валюта карт, срок с сегодня и не дальше
    5 лет (у правки можно прежний), «каждый год» только со сроком, статья трат."""
    bad: set[str] = set()
    cards = {currency_of(source) for source in bot_data.get("sources", [])}
    if body.currency.upper() not in cards:
        bad.add("currency")
    if body.deadline is not None:
        kept = current is not None and current.deadline == body.deadline
        latest = _years_later(today, PIGGY_YEARS)
        if not kept and not today <= body.deadline <= latest:
            bad.add("deadline")
    elif body.repeat == "yearly":
        bad.add("repeat")
    if body.group or body.subcategory:
        groups: list[str] = bot_data.get("categories", [])
        subcategories: dict[str, list[str]] = bot_data.get("subcategories", {})
        if body.group not in groups or body.group in CLOSED_GROUPS:
            bad.add("group")
        elif body.subcategory not in subcategories.get(body.group or "", []):
            bad.add("subcategory")
    return bad


def piggy_cells(body: PiggyIn, author: str) -> dict[str, Any]:
    """Колонки владельца B–H и M; формулы и колонки бота не трогаются."""
    repeat = YEARLY if body.repeat == "yearly" else "один раз"
    return {
        "B": body.name,
        "C": float(body.target) if body.target is not None else "",
        "D": body.currency.upper(),
        "E": f"{body.deadline:%d.%m.%Y}" if body.deadline else "",
        "F": repeat if body.deadline else "",
        "G": body.group or "",
        "H": body.subcategory or "",
        "M": author,
    }


def piggy_row(piggy_id: str, body: PiggyIn, author: str) -> list[Any]:
    cells = piggy_cells(body, author)
    return [piggy_id, *(cells[c] for c in "BCDEFGH"), "", "", "", "", author, ""]


def move_row(
    day: date, piggy: Piggy, amount: float, kind: str, author: str, key: str = ""
) -> list[Any]:
    """Строка журнала копилок: сумма в валюте копилки со знаком, округлённая, как
    сумма валюты (без хвостов вида 66.77000000000001 и без «-0»)."""
    rounded = _rounded(amount, piggy.currency) or 0.0
    return [f"{day:%d.%m.%Y}", piggy.id, piggy.name, rounded, piggy.currency, kind,
            author, key]  # fmt: skip


def piggy_out(state: PiggyState) -> PiggyOut:
    piggy, balance, code = state.piggy, state.balance, state.piggy.currency
    return PiggyOut(
        id=piggy.id,
        row=piggy.row,
        name=piggy.name,
        target=piggy.target,
        currency=code,
        deadline=piggy.deadline,
        repeat="yearly" if piggy.yearly else "once",
        group=piggy.group,
        subcategory=piggy.subcategory,
        author=piggy.author,
        saved=_rounded(balance.saved, code),
        spent=_rounded(balance.spent, code),
        balance=_rounded(balance.left, code),
        collected=_rounded(balance.collected, code),
        this_month=state.this_month,
        left_this_month=_rounded(state.left_this_month, code),
        status=state.status,  # type: ignore[arg-type]
        short=_rounded(state.short, code),
        leftover=_rounded(state.leftover, code),
        error=piggy.error,
        history=[
            MoveOut(day=move.day, amount=move.amount, kind=move.kind, who=move.who)
            for move in state.history
        ],
        spends=[
            SpendOut(
                key=item.key,
                day=item.day,
                group=item.group,
                subcategory=item.subcategory,
                amount=_rounded(item.amount, code),
            )
            for item in balance.spends
        ],
    )


def thousands(value: float) -> float:
    """Сумы до тысячи — как пишем «Отложить за месяц» в таблицу."""
    return float(round(value / 1000) * 1000)


def savings_screen_out(found: SavingsTab | None, today: date) -> SavingsScreenOut:
    """«Накопления» для экрана; без вкладки месяца — только статус."""
    if found is None:
        return SavingsScreenOut(status="no_month_tab", today=today)
    screen, savings, rates = found
    first = screen.plan[0] if screen.plan else None
    month = None
    if first is not None:
        left = sum(
            state.left_this_month * rates.to_uzs.get(state.piggy.currency, 0.0)
            for state in screen.piggies
            if state.piggy.deadline is not None
        )
        month = SavingsMonthOut(
            month=f"{first.month:%Y-%m}",
            piggy=float(round(first.total)),
            piggy_left=float(round(left)),
            share=float(round(first.cushion + sum(first.goals.values()))),
            share_to=screen.share_to,  # type: ignore[arg-type]
            goals=[
                AmountOut(id=key, amount=float(round(value)))
                for key, value in first.goals.items()
            ],
        )
    cushion = cushion_out(screen.cushion)
    if cushion is not None:
        cushion = cushion.model_copy(
            update={
                "frozen": (
                    None
                    if screen.frozen is None
                    else _rounded(screen.frozen, cushion.currency)
                ),
                "piggies": _rounded(screen.envelopes, cushion.currency),
                "over": _rounded(screen.over, cushion.currency),
            }
        )
    found_goal = goal_of(screen, first.month) if first is not None else None
    goal = None
    if found_goal is not None and any(
        state.status != "broken" for state in screen.piggies
    ):
        goal = SavingsGoalOut(
            suggested=thousands(found_goal.total),
            in_plan=None if screen.in_plan is None else float(round(screen.in_plan)),
            piggies=float(round(found_goal.piggies)),
            share=float(round(found_goal.share)),
            spent=float(round(found_goal.spent)),
        )
    return SavingsScreenOut(
        status="ok",
        today=today,
        month=month,
        goal=goal,
        cushion=cushion,
        piggies=[piggy_out(state) for state in screen.piggies],
        plan=[
            MonthPlanOut(
                month=f"{item.month:%Y-%m}",
                total=float(round(item.total)),
                items=[
                    AmountOut(id=key, amount=float(round(value)))
                    for key, value in item.items.items()
                ],
                cushion=float(round(item.cushion)),
                goals=[
                    AmountOut(id=key, amount=float(round(value)))
                    for key, value in item.goals.items()
                ],
                ready=list(item.ready),
            )
            for item in screen.plan
        ],
        enough=(
            None
            if screen.enough is None
            else EnoughOut(
                **{
                    key: float(round(value))
                    for key, value in asdict(screen.enough).items()
                }
            )
        ),
        savings=savings_out(savings),
        rates=dict(rates.to_uzs),
    )


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
    spent = [
        SpentGroupOut(
            name=group.name,
            amount=group.amount,
            items=[
                SpentItemOut(
                    name=item.subcategory,
                    icon=icons.get(item.subcategory, ""),
                    amount=item.amount,
                )
                for item in group.items
            ],
        )
        for group in found.spent
    ]
    untracked = [
        UntrackedOut(
            group=item.group,
            subcategory=item.subcategory,
            currency=item.currency,
            amount=item.amount,
        )
        for item in found.untracked
    ]
    cards = [card_out(card, today) for card in found.cards]
    joined = [
        CategoryOut(
            name=group.name,
            plan=group.plan,
            fact=group.fact,
            items=[
                CategoryLineOut(
                    name=line.subcategory,
                    icon=icons.get(line.subcategory, ""),
                    plan=line.plan,
                    fact=line.fact,
                )
                for line in group.items
            ],
        )
        for group in found.categories
    ]
    budget = found.budget
    if budget is None:
        return DashboardOut(
            status="no_forecast",
            month=month,
            today=today,
            groups=groups,
            spent=spent,
            untracked=untracked,
            cards=cards,
            categories=joined,
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
        spending=[
            DaySpendOut(day=day.day, spent=day.spent, limit=day.limit)
            for day in budget.history
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
        spent=spent,
        untracked=untracked,
        cards=cards,
        categories=joined,
    )


WHOLE_CURRENCIES = frozenset({"UZS", "RUB"})  # остальные валюты — до центов


def _rounded(value: float, code: str) -> float:
    return float(round(value)) if code in WHOLE_CURRENCIES else round(value, 2)


def _days_out(days: Sequence[Evening], code: str) -> list[ForecastDayOut]:
    return [
        ForecastDayOut(
            day=point.day,
            plan=_rounded(point.plan, code),
            pace=_rounded(point.pace, code),
        )
        for point in days
    ]


def _article_out(
    article: DayArticle, code: str, icons: Mapping[str, str]
) -> DayArticleOut:
    return DayArticleOut(
        group=article.group,
        subcategory=article.subcategory,
        icon=icons.get(article.subcategory, ""),
        amount=_rounded(article.amount, code),
        uzs=float(round(article.uzs)),
        income=article.income,
    )


def _past_out(
    days: Sequence[PastDay], code: str, icons: Mapping[str, str]
) -> list[ForecastPastOut]:
    return [
        ForecastPastOut(
            day=point.day,
            evening=_rounded(point.evening, code),
            change=_rounded(point.change, code),
            spent=_rounded(point.spent, code),
            earned=_rounded(point.earned, code),
            articles=[_article_out(item, code, icons) for item in point.articles],
        )
        for point in days
    ]


def _dips_out(plan: Dip | None, pace: Dip | None) -> ForecastDipsOut:
    def one(dip: Dip | None) -> ForecastDipOut | None:
        return None if dip is None else ForecastDipOut(start=dip.start, until=dip.until)

    return ForecastDipsOut(plan=one(plan), pace=one(pace))


def forecast_out(
    found: MonthForecast | None,
    today: date,
    icons: Mapping[str, str] | None = None,
) -> ForecastOut:
    """Прогноз для экрана; без вкладки месяца — только статус.
    icons — значки подкатегорий для статей дня."""
    icons = icons or {}
    if found is None:
        return ForecastOut(status="no_month_tab", month=month_title(today), today=today)
    currencies = [
        ForecastCurrencyOut(
            code=line.code,
            now=_rounded(line.now, line.code),
            per_day_plan=_rounded(line.per_day_plan, line.code),
            per_day_pace=_rounded(line.per_day_pace, line.code),
            spent_today=_rounded(line.spent_today, line.code),
            in_total=line.code in found.counted,
            events=[
                ForecastEventOut(
                    day=event.day,
                    name=event.name,
                    amount=_rounded(event.amount, line.code),
                    late=event.late,
                )
                for event in line.events
            ],
            days=_days_out(line.days, line.code),
            below_zero=_dips_out(line.plan_dip, line.pace_dip),
            records=_records_out(line.records, line.code),
            past=_past_out(line.past, line.code, icons),
        )
        for line in found.currencies
    ]
    return ForecastOut(
        status="ok",
        month=month_title(found.today),
        today=found.today,
        end=found.end,
        currencies=currencies,
        total=ForecastTotalOut(
            uzs=_days_out(found.total_uzs, "UZS"),
            usd=_days_out(found.total_usd, "USD"),
            below_zero=_dips_out(found.plan_dip, found.pace_dip),
            past_uzs=_past_out(found.past_uzs, "UZS", icons),
            past_usd=_past_out(found.past_usd, "USD", icons),
        ),
        rates=dict(found.rates),
        calendar=[
            CalendarRecordOut(
                id=item.record.id,
                date=item.record.start,
                repeat=item.record.repeat,
                weekdays=sorted(item.record.weekdays),
                group=item.record.group,
                subcategory=item.record.subcategory,
                currency=item.record.currency,
                amount=_rounded(item.record.amount, item.record.currency),
                name=item.record.name,
                author=item.record.author,
                month_total=_rounded(item.month_total, item.record.currency),
                booked=_rounded(item.booked, item.record.currency),
                over=item.over,
            )
            for item in found.calendar
        ],
        articles=[
            ForecastArticleOut(
                group=article.group,
                subcategory=article.subcategory,
                currency=article.currency,
                plan=_rounded(article.plan, article.currency),
                rest=_rounded(article.rest, article.currency),
                booked=_rounded(article.booked, article.currency),
            )
            for line in found.currencies
            for article in line.articles
        ],
        hints={
            key: HintOut(typical=hint.typical, last=hint.last, per_day=hint.per_day)
            for key, hint in found.hints.items()
        },
    )


def _records_out(records: Sequence[Any], code: str) -> list[ForecastRecordOut]:
    found = []
    for day in records:
        left = _rounded(day.open, code)
        found.append(
            ForecastRecordOut(
                id=day.id,
                day=day.day,
                name=day.name,
                group=day.group,
                subcategory=day.subcategory,
                repeat=day.repeat,
                amount=_rounded(day.amount, code),
                open=left,
                done=left == 0,
                over=day.over,
                spent=_rounded(day.spent, code),
            )
        )
    return found


def check_record(
    body: CalendarIn,
    bot_data: Mapping[str, Any],
    today: date,
    current: CalendarRecord | None,
) -> set[str]:
    """Поля записи, которые не подходят: статья трат справочника, валюта карт,
    дата с сегодня и не дальше года (у правки повтора — можно прежнюю), дни недели."""
    bad: set[str] = set()
    groups: list[str] = bot_data.get("categories", [])
    subcategories: dict[str, list[str]] = bot_data.get("subcategories", {})
    if body.group not in groups or body.group in CLOSED_GROUPS:
        bad.add("group")
    elif body.subcategory not in subcategories.get(body.group, []):
        bad.add("subcategory")
    cards = {currency_of(source) for source in bot_data.get("sources", [])}
    if body.currency not in cards:
        bad.add("currency")
    kept = current is not None and body.repeat != "once" and body.date == current.start
    latest = today + timedelta(days=MAX_AHEAD_DAYS)
    if not kept and not today <= body.date <= latest:
        bad.add("date")
    if body.repeat == "weekdays":
        if not body.weekdays or any(day not in range(1, 8) for day in body.weekdays):
            bad.add("weekdays")
    elif body.weekdays:
        bad.add("weekdays")
    return bad


def record_from(body: CalendarIn, record_id: str, author: str) -> CalendarRecord:
    return CalendarRecord(
        id=record_id,
        start=body.date,
        repeat=body.repeat,
        weekdays=frozenset(body.weekdays),
        group=body.group,
        subcategory=body.subcategory,
        currency=body.currency,
        amount=float(body.amount),
        name=body.name,
        author=author,
    )


def find_record(gs: Any, record_id: str) -> CalendarRecord | None:
    return next(
        (r for r in read_calendar(gs.get_calendar()) if r.id == record_id), None
    )


def add_record_row(gs: Any, body: CalendarIn, author: str) -> CalendarRecord:
    """Новая запись с кодом, которого ещё нет на листе."""
    taken = {record.id for record in read_calendar(gs.get_calendar())}
    record = record_from(body, new_id(taken), author)
    gs.add_calendar_row(record_row(record))
    return record


def record_note(action: str, author: str, record: CalendarRecord) -> str:
    """«📅 Иван — новая запись: 17.10 Детский центр 320 000 UZS (ДЕТИ / развитие)»;
    без имени — «📅 Новая запись: …». Как у плана: имя через тире, без рода."""
    if record.repeat == "daily":
        when = f"каждый день с {record.start:%d.%m}"
    elif record.repeat == "weekdays":
        days = " ".join(
            ("пн", "вт", "ср", "чт", "пт", "сб", "вс")[day - 1]
            for day in sorted(record.weekdays)
        )
        when = f"{days} с {record.start:%d.%m}"
    elif record.repeat == "monthly":
        when = f"{record.start.day}-го каждого месяца"
    else:
        when = f"{record.start:%d.%m}"
    value = (
        money(record.amount)
        if record.amount == int(record.amount)
        else f"{record.amount:.2f}".replace(".", ",")
    )
    _, group = leading_emoji(record.group)
    head = f"{author} — {action}" if author else action[:1].upper() + action[1:]
    return (
        f"📅 {head}: {when} {record.title} {value} "
        f"{record.currency} ({group} / {record.subcategory})"
    )


def plan_month(text: str | None, today: date) -> date | None:
    """«2026-10» → 01.10.2026, если это текущий или следующий месяц."""
    try:
        first = datetime.strptime(text or "", "%Y-%m").date()
    except ValueError:
        return None
    current = today.replace(day=1)
    return first if first in (current, shift_month(current, 1)) else None


def event_out(
    event: Event, receipts: Mapping[int, Receipt] | None, today: date
) -> PlanEventOut:
    """Строка списка для экрана; receipts — сверка текущего месяца, None — следующий."""
    status, received, on = "future", None, None
    if receipts is not None and event.day is not None and event.amount is not None:
        line = Line(event.row, event.day, event.amount, event.currency or "UZS")
        receipt = receipts.get(event.row, NOTHING)
        status = receipt_status(line, receipt, today)
        if receipt.received:
            received, on = receipt.received, receipt.on
    return PlanEventOut(
        slot=event.slot,
        name=event.name,
        day=event.day,
        amount=event.amount,
        currency=event.currency,
        received=received,
        received_on=on,
        status=status,
    )


def plan_out(
    first: date,
    today: date,
    loaded: LoadedPlan | None,
    icons: dict[str, str],
    catalog: Catalog,
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
    current = first == today.replace(day=1)
    events = None
    if plan.events is not None:
        receipts = loaded.receipts if current else None
        events = [event_out(e, receipts, today) for e in plan.events]
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
        addable=[
            PlanAddableOut(
                kind=kind_of(item.group),
                group=item.group,
                subcategory=item.subcategory,
                icon=icons.get(item.subcategory, ""),
                currency=item.currency,
                average=item.average,
                last=item.last,
                current=item.current,
            )
            for item in addable(plan, catalog, loaded.rates.to_uzs.keys(), history)
        ],
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
        added=tuple(
            NewArticle(n.group, n.subcategory, n.currency, float(n.plan))
            for n in body.new_rows
        ),
    )


def advice_changes(body: PlanAdviceIn, plan: MonthPlan) -> PlanChanges:
    """Черновик без base → правки поверх вкладки: база — то, что в ней сейчас."""
    articles = {a.key: a.plan for a in plan.articles}
    events = {e.slot: e.cells() for e in plan.events or ()}
    goal = None
    if body.goal is not None:
        base = (plan.goal.amount, plan.goal.currency) if plan.goal else (0.0, "UZS")
        goal = GoalChange(base, (float(body.goal.amount), body.goal.currency))
    return PlanChanges(
        articles=tuple(
            ArticleChange(r.key, articles.get(r.key, 0.0), float(r.plan))
            for r in body.rows
        ),
        events=tuple(
            EventChange(
                e.slot,
                events.get(e.slot, (None, None, None, None)),
                (
                    None
                    if e.value is None
                    else EventValue(
                        e.value.name,
                        e.value.day,
                        float(e.value.amount),
                        e.value.currency,
                    )
                ),
            )
            for e in body.events
        ),
        goal=goal,
        added=tuple(
            NewArticle(n.group, n.subcategory, n.currency, float(n.plan))
            for n in body.new_rows
        ),
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
            found = await asyncio.to_thread(
                analytics.dashboard, today, screen_age(request)
            )
        except Exception:
            logger.exception("Dashboard failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        icons: dict[str, str] = application.bot_data.get("icons", {})
        return JSONResponse(dashboard_out(found, today, icons).model_dump(mode="json"))

    async def fresh_forecast(user_id: int, max_age: float = 0.0) -> JSONResponse:
        """Прогноз ответом: после записи — по таблице заново, экрану — из копии."""
        today = local_today(config.ANALYTICS_TIMEZONE)
        analytics = application.bot_data["analytics_service"]
        sources: list[str] = application.bot_data.get("sources", [])
        # валюты карт бота; без справочника карт — хотя бы сумы
        codes = [currency_of(source) for source in sources] or ["UZS"]
        try:
            found = await asyncio.to_thread(analytics.forecast, today, codes, max_age)
        except Exception:
            logger.exception("Forecast failed for user_id=%s", user_id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        icons: dict[str, str] = application.bot_data.get("icons", {})
        body = forecast_out(found, today, icons)
        return JSONResponse(body.model_dump(mode="json"))

    async def forecast(request: Request) -> JSONResponse:
        user = current_user(request)
        return await fresh_forecast(user.id, screen_age(request))

    async def calendar_body(request: Request) -> CalendarIn | JSONResponse:
        try:
            return CalendarIn.model_validate_json(await request.body())
        except ValidationError as error:
            return validation_error(
                {str(e["loc"][0]) if e["loc"] else "body" for e in error.errors()}
            )

    def calendar_lock() -> asyncio.Lock:
        """Записи в «Календарь» — по одной: правка и удаление находят строку по ID,
        а пишут по её номеру; чужое удаление выше между ними сдвинуло бы строки,
        и правка легла бы на другую запись."""
        return application.bot_data.setdefault(CALENDAR_LOCK, asyncio.Lock())

    async def add_record(request: Request) -> JSONResponse:
        user = current_user(request)
        logger.info("Calendar add by user_id=%s", user.id)
        body = await calendar_body(request)
        if isinstance(body, JSONResponse):
            return body
        today = local_today(config.ANALYTICS_TIMEZONE)
        if bad := check_record(body, application.bot_data, today, None):
            return validation_error(bad)
        entries: dict[str, None] = user_context(
            application, user.id
        ).user_data.setdefault(CALENDAR_ENTRIES, {})
        key = str(body.entry_id) if body.entry_id else None
        if key is not None and key in entries:
            return await fresh_forecast(user.id)  # повтор нажатия: строка уже есть
        if key is not None:
            entries[key] = None
            while len(entries) > ENTRY_LIMIT:
                entries.pop(next(iter(entries)))
        author = await author_name(application.bot, user) or ""
        gs = application.bot_data["gs_service"]
        try:
            async with calendar_lock():
                record = await asyncio.to_thread(add_record_row, gs, body, author)
        except Exception:
            if key is not None:
                entries.pop(key, None)
            logger.exception("Calendar write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        logger.info("Calendar record %s added by user_id=%s", record.id, user.id)
        await notify_others(
            application.bot, user.id, record_note("новая запись", author, record)
        )
        return await fresh_forecast(user.id)

    async def update_record(request: Request) -> JSONResponse:
        user = current_user(request)
        logger.info("Calendar update by user_id=%s", user.id)
        record_id = request.path_params["record_id"]
        body = await calendar_body(request)
        if isinstance(body, JSONResponse):
            return body
        gs = application.bot_data["gs_service"]
        try:
            current = await asyncio.to_thread(find_record, gs, record_id)
        except Exception:
            logger.exception("Calendar read failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if current is None:
            return error_response(404, "record_not_found", RECORD_GONE)
        today = local_today(config.ANALYTICS_TIMEZONE)
        if bad := check_record(body, application.bot_data, today, current):
            return validation_error(bad)
        author = await author_name(application.bot, user) or ""
        record = record_from(body, record_id, author)
        try:
            async with calendar_lock():
                found = await asyncio.to_thread(
                    gs.update_calendar_row, record_id, record_row(record)
                )
        except Exception:
            logger.exception("Calendar write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if not found:
            return error_response(404, "record_not_found", RECORD_GONE)
        await notify_others(
            application.bot, user.id, record_note("запись изменена", author, record)
        )
        return await fresh_forecast(user.id)

    async def delete_record(request: Request) -> JSONResponse:
        user = current_user(request)
        logger.info("Calendar delete by user_id=%s", user.id)
        record_id = request.path_params["record_id"]
        gs = application.bot_data["gs_service"]
        try:
            async with calendar_lock():
                current = await asyncio.to_thread(find_record, gs, record_id)
                deleted = current is not None and await asyncio.to_thread(
                    gs.delete_calendar_row, record_id
                )
        except Exception:
            logger.exception("Calendar write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if current is None or not deleted:
            return error_response(404, "record_not_found", RECORD_GONE)
        author = await author_name(application.bot, user) or ""
        await notify_others(
            application.bot, user.id, record_note("запись удалена", author, current)
        )
        return await fresh_forecast(user.id)

    def piggy_lock() -> asyncio.Lock:
        """Записи в листы копилок — по одной: первая копилка создаёт листы, а ID
        новой берётся из прочитанного списка."""
        return application.bot_data.setdefault(PIGGY_LOCK, asyncio.Lock())

    def catalog() -> tuple[set[str], list[str], dict[str, list[str]]]:
        bot_data = application.bot_data
        cards = {currency_of(source) for source in bot_data.get("sources", [])}
        return (
            cards or {"UZS"},
            bot_data.get("categories", []),
            bot_data.get("subcategories", {}),
        )

    async def read_savings(max_age: float) -> SavingsTab | None:
        today = local_today(config.ANALYTICS_TIMEZONE)
        analytics = application.bot_data["analytics_service"]
        return await asyncio.to_thread(analytics.savings, today, *catalog(), max_age)

    async def fresh_savings(user_id: int, max_age: float = 0.0) -> JSONResponse:
        """«Накопления» ответом: после записи — по таблице заново, экрану — из копии."""
        today = local_today(config.ANALYTICS_TIMEZONE)
        try:
            found = await read_savings(max_age)
        except Exception:
            logger.exception("Savings failed for user_id=%s", user_id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        return JSONResponse(savings_screen_out(found, today).model_dump(mode="json"))

    async def savings(request: Request) -> JSONResponse:
        user = current_user(request)
        return await fresh_savings(user.id, screen_age(request))

    async def piggy_body(request: Request, model: Any) -> Any:
        try:
            return model.model_validate_json(await request.body() or b"{}")
        except ValidationError as error:
            return piggy_error(
                {str(e["loc"][0]) if e["loc"] else "body" for e in error.errors()}
            )

    def first_time(user_id: int, entry_id: Any) -> tuple[dict[str, None], str | None]:
        """Ключ правки для повтора нажатия; None — ключа нет или правка уже была."""
        entries: dict[str, None] = user_context(
            application, user_id
        ).user_data.setdefault(PIGGY_ENTRIES, {})
        return entries, None if entry_id is None else str(entry_id)

    def remember(entries: dict[str, None], key: str | None) -> None:
        if key is not None:
            entries[key] = None
            while len(entries) > ENTRY_LIMIT:
                entries.pop(next(iter(entries)))

    def state_of(found: SavingsTab | None, piggy_id: str) -> PiggyState | None:
        if found is None:
            return None
        return next(
            (item for item in found.screen.piggies if item.piggy.id == piggy_id), None
        )

    async def written(user_id: int, author: str, note: str) -> JSONResponse:
        """После записи: фоновая запись «Потрачено» и взноса, заметка второму."""
        mark_dirty(application.bot_data)
        await notify_others(application.bot, user_id, piggy_note_text(author, note))
        return await fresh_savings(user_id)

    def piggy_note_text(author: str, note: str) -> str:
        action, text = note.split("|", 1)
        return piggy_note(action, author, text)

    async def add_piggy(request: Request) -> JSONResponse:
        user = current_user(request)
        logger.info("Piggy add by user_id=%s", user.id)
        body = await piggy_body(request, PiggyIn)
        if isinstance(body, JSONResponse):
            return body
        today = local_today(config.ANALYTICS_TIMEZONE)
        if bad := check_piggy(body, application.bot_data, today, None):
            return piggy_error(bad)
        entries, key = first_time(user.id, body.entry_id)
        if key is not None and key in entries:
            return await fresh_savings(user.id)  # повтор нажатия: строка уже есть
        remember(entries, key)
        author = await author_name(application.bot, user) or ""
        gs = application.bot_data["gs_service"]
        try:
            async with piggy_lock():
                rows, _ = await asyncio.to_thread(gs.get_piggy)
                taken = {str(row[0]).strip() for row in rows[1:] if row}
                piggy_id = new_id(taken)
                await asyncio.to_thread(gs.add_piggy, piggy_row(piggy_id, body, author))
        except Exception:
            if key is not None:
                entries.pop(key, None)
            logger.exception("Piggy write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        logger.info("Piggy %s added by user_id=%s", piggy_id, user.id)
        text = piggy_text(
            body.name,
            None if body.target is None else float(body.target),
            body.currency.upper(),
            body.deadline,
            body.repeat == "yearly",
        )
        return await written(user.id, author, f"новая копилка|{text}")

    async def update_piggy(request: Request) -> JSONResponse:
        user = current_user(request)
        piggy_id = request.path_params["piggy_id"]
        logger.info("Piggy %s update by user_id=%s", piggy_id, user.id)
        body = await piggy_body(request, PiggyIn)
        if isinstance(body, JSONResponse):
            return body
        try:
            current = state_of(await read_savings(SCREEN_COPY_AGE), piggy_id)
        except Exception:
            logger.exception("Savings failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if current is None:
            return error_response(404, "piggy_not_found", PIGGY_GONE)
        today = local_today(config.ANALYTICS_TIMEZONE)
        if bad := check_piggy(body, application.bot_data, today, current.piggy):
            return piggy_error(bad)
        if body.currency.upper() != current.piggy.currency and current.history:
            return piggy_error({"currency": CURRENCY_LOCKED})
        author = await author_name(application.bot, user) or ""
        gs = application.bot_data["gs_service"]
        try:
            async with piggy_lock():
                found = await asyncio.to_thread(
                    gs.update_piggy, piggy_id, piggy_cells(body, author)
                )
        except Exception:
            logger.exception("Piggy write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if not found:
            return error_response(404, "piggy_not_found", PIGGY_GONE)
        text = piggy_text(
            body.name,
            None if body.target is None else float(body.target),
            body.currency.upper(),
            body.deadline,
            body.repeat == "yearly",
        )
        return await written(user.id, author, f"копилка изменена|{text}")

    async def close_piggy(request: Request) -> JSONResponse:
        user = current_user(request)
        piggy_id = request.path_params["piggy_id"]
        logger.info("Piggy %s close by user_id=%s", piggy_id, user.id)
        today = local_today(config.ANALYTICS_TIMEZONE)
        author = await author_name(application.bot, user) or ""
        gs = application.bot_data["gs_service"]
        try:
            async with piggy_lock():
                current = state_of(await read_savings(SCREEN_COPY_AGE), piggy_id)
                found = current is not None
                if current is not None:
                    left = current.balance.left
                    if left > 0:  # остаток возвращается в подушку
                        await asyncio.to_thread(
                            gs.add_piggy_moves,
                            [move_row(today, current.piggy, -left, CLOSED, author)],
                        )
                    found = await asyncio.to_thread(
                        gs.update_piggy, piggy_id, {"N": f"{today:%d.%m.%Y}"}
                    )
        except Exception:
            logger.exception("Piggy write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if current is None or not found:
            return error_response(404, "piggy_not_found", PIGGY_GONE)
        name = current.piggy.name
        return await written(
            user.id, author, f"копилка закрыта|{name} (остаток — в подушку)"
        )

    async def move_piggy(request: Request) -> JSONResponse:
        user = current_user(request)
        piggy_id = request.path_params["piggy_id"]
        body = await piggy_body(request, MoveIn)
        if isinstance(body, JSONResponse):
            return body
        entries, key = first_time(user.id, body.entry_id)
        if key is not None and key in entries:
            return await fresh_savings(user.id)
        today = local_today(config.ANALYTICS_TIMEZONE)
        author = await author_name(application.bot, user) or ""
        gs = application.bot_data["gs_service"]
        amount, too_much = float(body.amount), None
        try:
            async with piggy_lock():
                current = state_of(await read_savings(SCREEN_COPY_AGE), piggy_id)
                if current is not None:
                    left = current.balance.left
                    if body.kind != "saved" and amount > left + 1e-9:
                        too_much = left  # остаток копилки не уходит ниже нуля
                    else:
                        signed = amount if body.kind == "saved" else -amount
                        await asyncio.to_thread(
                            gs.add_piggy_moves,
                            [
                                move_row(
                                    today,
                                    current.piggy,
                                    signed,
                                    KIND_TEXT[body.kind],
                                    author,
                                )
                            ],
                        )
                        remember(entries, key)
        except Exception:
            logger.exception("Piggy write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if current is None:
            return error_response(404, "piggy_not_found", PIGGY_GONE)
        code = current.piggy.currency
        if too_much is not None:
            return piggy_error(
                {"amount": f"В копилке только {amount_text(too_much)} {code}"}
            )
        text = f"{current.piggy.name} {amount_text(amount)} {code}"
        return await written(user.id, author, f"{KIND_TEXT[body.kind]}|{text}")

    async def save_piggies(request: Request) -> JSONResponse:
        user = current_user(request)
        body = await piggy_body(request, SaveIn)
        if isinstance(body, JSONResponse):
            return body
        currency = body.currency.upper()
        if currency not in catalog()[0]:
            return piggy_error({"currency"})
        entries, key = first_time(user.id, body.entry_id)
        if key is not None and key in entries:
            return await fresh_savings(user.id)
        today = local_today(config.ANALYTICS_TIMEZONE)
        author = await author_name(application.bot, user) or ""
        gs = application.bot_data["gs_service"]
        chosen: list[tuple[Piggy, float]] = []
        try:
            async with piggy_lock():
                found = await read_savings(SCREEN_COPY_AGE)
                if found is not None:
                    named = {item.piggy.id: item.piggy for item in found.screen.piggies}
                    chosen = [
                        (named[piggy_id], amount)
                        for piggy_id, amount in distribute(
                            found.screen, float(body.amount), currency, found.rates
                        )
                        if amount > 0
                    ]
                if chosen:
                    await asyncio.to_thread(
                        gs.add_piggy_moves,
                        [move_row(today, piggy, amount, SAVED, author)
                         for piggy, amount in chosen],  # fmt: skip
                    )
                remember(entries, key)
        except Exception:
            logger.exception("Piggy write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if not chosen:  # всё — подушке: в журнал писать нечего
            return await fresh_savings(user.id)
        text = ", ".join(
            f"{piggy.name} {amount_text(amount)} {piggy.currency}"
            for piggy, amount in chosen
        )
        return await written(user.id, author, f"{SAVED}|{text}")

    async def exclude_spend(request: Request) -> JSONResponse:
        user = current_user(request)
        piggy_id = request.path_params["piggy_id"]
        body = await piggy_body(request, ExcludeIn)
        if isinstance(body, JSONResponse):
            return body
        today = local_today(config.ANALYTICS_TIMEZONE)
        author = await author_name(application.bot, user) or ""
        gs = application.bot_data["gs_service"]
        spend, marked = None, False
        try:
            async with piggy_lock():
                current = state_of(await read_savings(SCREEN_COPY_AGE), piggy_id)
                if current is not None:
                    spend = next(
                        (s for s in current.balance.spends if s.key == body.key), None
                    )
                    if spend is not None:
                        await asyncio.to_thread(
                            gs.add_piggy_moves,
                            [move_row(today, current.piggy, 0, NOT_FROM, author,
                                      body.key)],  # fmt: skip
                        )
                    else:  # второе нажатие: отметка уже в журнале
                        _, journal = await asyncio.to_thread(gs.get_piggy)
                        marked = any(
                            len(row) > 7
                            and str(row[5]).strip().lower() == NOT_FROM
                            and str(row[7]) == body.key
                            for row in journal[1:]
                        )
        except Exception:
            logger.exception("Piggy write failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if current is None:
            return error_response(404, "piggy_not_found", PIGGY_GONE)
        if spend is None:
            if marked:
                return await fresh_savings(user.id)
            return error_response(404, "spend_not_found", SPEND_GONE)
        _, group = leading_emoji(spend.group)
        text = (
            f"{spend.day:%d.%m} {group} / {spend.subcategory} "
            f"{amount_text(spend.amount)}"
        )
        return await written(user.id, author, f"{NOT_FROM}|{text}")

    async def month_plan(request: Request) -> JSONResponse:
        user = current_user(request)
        today = local_today(config.ANALYTICS_TIMEZONE)
        first = plan_month(request.query_params.get("month"), today)
        if first is None:
            return validation_error({"month"})
        gs = application.bot_data["gs_service"]
        try:
            loaded = await asyncio.to_thread(
                load_plan, gs, month_title(first), today, screen_age(request)
            )
        except Exception:
            logger.exception("Plan failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        icons: dict[str, str] = application.bot_data.get("icons", {})
        catalog: Catalog = application.bot_data.get("subcategories", {})
        body = plan_out(first, today, loaded, icons, catalog)
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
        catalog: Catalog = application.bot_data.get("subcategories", {})
        lock: asyncio.Lock = application.bot_data.setdefault(PLAN_LOCK, asyncio.Lock())
        try:
            async with lock:
                outcome = await asyncio.to_thread(
                    save_plan, gs, title, changes, catalog
                )
                loaded = None
                if not outcome.invalid:
                    loaded = await asyncio.to_thread(load_plan, gs, title, today)
        except WorksheetNotFound:
            return validation_error({"month"})
        except Exception:
            logger.exception("Plan save failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if outcome.invalid:
            if "space" in outcome.invalid:
                return error_response(
                    422, "validation", NO_SPACE, {"new_rows": NO_SPACE}
                )
            return validation_error(set(outcome.invalid))
        icons: dict[str, str] = application.bot_data.get("icons", {})
        fresh = plan_out(first, today, loaded, icons, catalog)
        if outcome.conflicts:
            conflict = PlanConflictOut(
                error=ErrorOut(code="plan_changed", message=CONFLICT),
                plan=fresh,
                conflicts=list(outcome.conflicts),
            )
            return JSONResponse(conflict.model_dump(mode="json"), status_code=409)
        fresh.saved = changes.count()
        mark_dirty(application.bot_data)  # строку списка могли поменять
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
                current = await asyncio.to_thread(gs.ensure_month_tab, today)
                tab = await asyncio.to_thread(gs.ensure_next_month_tab, today)
                loaded = await asyncio.to_thread(
                    load_plan, gs, month_title(first), today
                )
        except Exception:
            logger.exception("Next month plan failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if text := describe(current, today):
            # вкладку текущего месяца сделала кнопка раньше ночной задачи
            await tell_owner(application.bot, text)
        if tab.created:
            logger.info("Plan tab %s created by user_id=%s", tab.title, user.id)
        icons: dict[str, str] = application.bot_data.get("icons", {})
        catalog: Catalog = application.bot_data.get("subcategories", {})
        body = plan_out(first, today, loaded, icons, catalog)
        return JSONResponse(body.model_dump(mode="json"))

    async def plan_advice(request: Request) -> JSONResponse:
        user = current_user(request)
        try:
            body = PlanAdviceIn.model_validate_json(await request.body())
        except ValidationError as error:
            return validation_error(
                {str(e["loc"][0]) if e["loc"] else "body" for e in error.errors()}
            )
        today = local_today(config.ANALYTICS_TIMEZONE)
        first = plan_month(body.month, today)
        if first is None:
            return validation_error({"month"})
        ai = application.bot_data.get("ai_service")
        if ai is None or not ai.enabled:
            return error_response(503, "advisor_unavailable", ADVISOR_DOWN)
        gs = application.bot_data["gs_service"]
        catalog: Catalog = application.bot_data.get("subcategories", {})
        try:
            gathered = await asyncio.to_thread(
                gather_advice,
                gs,
                first,
                today,
                lambda plan: advice_changes(body, plan),
                catalog,
            )
        except WorksheetNotFound:
            return validation_error({"month"})
        except Exception:
            logger.exception("Plan advice: the sheet failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        if gathered.context is None:
            return validation_error(set(gathered.invalid))
        try:
            points = await asyncio.wait_for(
                ai.plan_advice(advice_numbers(gathered.context)), ADVICE_SECONDS
            )
        except Exception:
            logger.exception("Plan advice failed for user_id=%s", user.id)
            return error_response(503, "advisor_unavailable", ADVISOR_DOWN)
        tab = {a.key for a in gathered.plan.articles if a.editable}
        editable = {a.key: a for a in gathered.context.draft.articles if a.key in tab}
        checked = checked_points(points, editable)
        logger.info("Plan advice: %d point(s) for user_id=%s", len(checked), user.id)
        answer = PlanAdviceOut(
            points=[
                AdvicePointOut(
                    text=p.text,
                    apply=(
                        None
                        if p.key is None or p.plan is None
                        else AdviceApplyOut(key=p.key, plan=p.plan)
                    ),
                )
                for p in checked
            ]
        )
        return JSONResponse(answer.model_dump(mode="json"))

    return [
        Route("/bootstrap", bootstrap),
        Route("/expenses", add_expense, methods=["POST"]),
        Route("/expenses/undo", undo_expense, methods=["POST"]),
        Route("/dashboard", dashboard),
        Route("/forecast", forecast),
        Route("/calendar", add_record, methods=["POST"]),
        Route("/calendar/{record_id}", update_record, methods=["PUT"]),
        Route("/calendar/{record_id}", delete_record, methods=["DELETE"]),
        Route("/savings", savings),
        Route("/piggy", add_piggy, methods=["POST"]),
        Route("/piggy/save", save_piggies, methods=["POST"]),
        Route("/piggy/{piggy_id}", update_piggy, methods=["PUT"]),
        Route("/piggy/{piggy_id}/close", close_piggy, methods=["POST"]),
        Route("/piggy/{piggy_id}/moves", move_piggy, methods=["POST"]),
        Route("/piggy/{piggy_id}/exclude", exclude_spend, methods=["POST"]),
        Route("/plan", month_plan),
        Route("/plan", save_month_plan, methods=["POST"]),
        Route("/plan/next", create_next_plan, methods=["POST"]),
        Route("/plan/advice", plan_advice, methods=["POST"]),
    ]
