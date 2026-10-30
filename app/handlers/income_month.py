"""К какому месяцу поступление: после записи дохода не в сумах без пометки «за
<месяц>», когда прошлый месяц ещё ждёт денег в этой валюте, бот спрашивает «Эти 55 000 ₽
— за октябрь?». Кнопка пишет пометку в комментарий строки fact, и сверка
(services.receipts.income_month) относит доход к названному месяцу — без пометки он
месяцу своей даты. В сумах вопроса нет: месяц ЗП пишет банк («Зарплата за октябрь»).
"""

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import replace
from datetime import date

from telegram import CallbackQuery, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from app import config
from app.domain import INCOME_GROUP, SheetRow, local_today
from app.handlers.analytics import send_report
from app.handlers.common import LAST_WRITE
from app.handlers.receipts import mark_dirty
from app.rich import Report, heading, money_text, para
from app.services.analytics_service import MONTHS_RU
from app.services.receipts import MONTH_MARK
from app.utils.keyboards import action

logger = logging.getLogger(__name__)

PREFIX = "mon:"
QUESTIONS = "month_questions"  # user_data: номер строки fact → записанная строка
REST_SUFFIX = ", остаток"


def _asks(row: SheetRow, today: date) -> bool:
    """Доход не в сумах, без пометки месяца, с датой этого месяца."""
    return (
        row.category == INCOME_GROUP
        and row.amount > 0
        and row.currency != "UZS"
        and MONTH_MARK.search(row.comment) is None
        and (row.day.year, row.day.month) == (today.year, today.month)
    )


def _mark(month: date) -> str:
    return f"за {MONTHS_RU[month.month - 1]}"


async def _edit(query: CallbackQuery, text: str) -> None:
    """Ответ на кнопку вместо вопроса; повторное нажатие той же кнопки — не ошибка."""
    try:
        await query.edit_message_text(text)
    except BadRequest as error:
        if "not modified" not in str(error).lower():
            raise


async def month_question(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    rows: Sequence[SheetRow],
    first: int | None,
) -> None:
    """Вопрос по каждому такому доходу записи (first — номер первой строки в fact);
    прошлый месяц в его валюте ничего не ждёт — вопроса нет. Сбой — только в лог."""
    today = local_today(config.ANALYTICS_TIMEZONE)
    asked = [
        (first + index, row)
        for index, row in enumerate(rows)
        if first is not None and _asks(row, today)
    ]
    analytics = context.bot_data.get("analytics_service")
    if not asked or analytics is None:
        return
    try:
        rest = await asyncio.to_thread(analytics.last_months_rest, today)
        last = (today.replace(day=1) - date.resolution).replace(day=1)
        month, this = _mark(last), _mark(today)
        questions = context.user_data.setdefault(QUESTIONS, {})
        for number, row in asked:
            waiting = [item for item in rest if item.currency == row.currency]
            if not waiting:
                continue
            amount = money_text(row.amount, row.currency)
            names = [
                f"{item.name.removesuffix(f' {month}{REST_SUFFIX}')} "
                f"{money_text(item.amount, item.currency)}"
                for item in waiting
            ]
            head = f"📅 Эти {amount} — {month}?"
            text = f"{head}\n{month.capitalize()} ещё ждём: " + "; ".join(names)
            html = heading(head, 2) + para(f"{month} ещё ждём:", *names)
            questions[number] = row
            buttons = [
                action(month.capitalize(), f"{PREFIX}{number}:{last:%Y-%m}"),
                action(this.capitalize(), f"{PREFIX}{number}:{today:%Y-%m}"),
            ]
            await send_report(
                context.bot,
                chat_id,
                Report(text, html),
                reply_markup=InlineKeyboardMarkup([buttons]),
            )
    except Exception:
        logger.exception("Month question failed for chat_id=%s", chat_id)


async def month_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """mon:<строка fact>:<ГГГГ-ММ> — пометить доход этим месяцем."""
    query = update.callback_query
    await query.answer()
    _, number_text, month_text = (query.data or "").split(":")
    number = int(number_text)
    month = date.fromisoformat(f"{month_text}-01")
    mark = _mark(month)
    row: SheetRow | None = context.user_data.get(QUESTIONS, {}).get(number)
    if row is None:
        await _edit(query, f"Вопрос устарел — допишите в комментарий строки «{mark}»")
        return
    comment = f"{row.comment} {mark}".strip()
    gs = context.bot_data["gs_service"]
    if not await asyncio.to_thread(gs.comment_row, number, row, comment):
        await _edit(
            query,
            "Строку уже поменяли — пометку не поставил. "
            f"Допишите в комментарий строки «{mark}»",
        )
        return
    context.user_data[QUESTIONS].pop(number, None)
    marked = replace(row, comment=comment)
    last = context.user_data.get(LAST_WRITE)
    if last and last["first"] <= number <= last["last"]:
        # /undo и /fix сверяют строку с записанной — пусть видят новый комментарий
        last["rows"][number - last["first"]] = marked
    mark_dirty(context.bot_data)  # «Пришло к утру» — по новой пометке
    await _edit(query, f"✓ {money_text(row.amount, row.currency)} — {mark}")
