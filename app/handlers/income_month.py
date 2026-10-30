"""К какому месяцу поступление: после записи дохода без пометки «за <месяц>», когда
прошлый месяц ещё ждёт денег в этой валюте, бот спрашивает «Эти 55 000 ₽ — за
октябрь?». Кнопка пишет пометку в комментарий строки fact, и сверка
(services.receipts.income_month) относит доход к названному месяцу — без пометки он
месяцу своей даты. В сумах — только о зарплате и авансе, когда прошлый месяц ждёт
свою ЗП или аванс: сумовых доходов много, а пометку банк пишет только в выписке.
Вопрос идёт фоном (ask_month): его чтения Google ответ на запись не ждёт.
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
from app.handlers.analytics import edit_report, send_report
from app.handlers.common import LAST_WRITE
from app.handlers.receipts import mark_dirty
from app.rich import Report, heading, money_text, note_report, para
from app.services.analytics_service import MONTHS_RU, OPENING_BALANCE
from app.services.day_budget import ADVANCE, MAIN_SALARY, ForecastItem
from app.services.receipts import MONTH_MARK
from app.utils.keyboards import action

logger = logging.getLogger(__name__)

PREFIX = "mon:"
QUESTIONS = "month_questions"  # user_data: строка fact → записанная строка или пометка
REST_SUFFIX = ", остаток"
TASKS: set[asyncio.Task[None]] = set()  # фоновые вопросы — пока не отправлены


def _salary(name: str) -> bool:
    return bool(MAIN_SALARY.search(name) or ADVANCE.search(name))


def _asks(row: SheetRow, today: date) -> bool:
    """Доход без пометки месяца, с датой этого месяца; в сумах — только зарплата."""
    return (
        row.category == INCOME_GROUP
        and row.amount > 0
        and row.subcategory != OPENING_BALANCE
        and (row.currency != "UZS" or _salary(row.subcategory))
        and MONTH_MARK.search(row.comment) is None
        and (row.day.year, row.day.month) == (today.year, today.month)
    )


def _mark(month: date) -> str:
    return f"за {MONTHS_RU[month.month - 1]}"


def _waits_for(row: SheetRow, rest: Sequence[ForecastItem], month: str) -> list[str]:
    """Что прошлый месяц ждёт в валюте дохода — «ЗП Питер 55 000 ₽»; в сумах — его
    зарплата и аванс."""
    names = []
    for item in rest:
        name = item.name.removesuffix(f" {month}{REST_SUFFIX}")
        if item.currency == row.currency and (row.currency != "UZS" or _salary(name)):
            names.append(f"{name} {money_text(item.amount, item.currency)}")
    return names


def ask_month(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    rows: Sequence[SheetRow],
    first: int | None,
) -> None:
    """Вопрос фоном — только если в записи есть такой доход."""
    today = local_today(config.ANALYTICS_TIMEZONE)
    if first is None or not any(_asks(row, today) for row in rows):
        return
    task = asyncio.create_task(month_question(context, chat_id, rows, first))
    TASKS.add(task)
    task.add_done_callback(TASKS.discard)


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
            names = _waits_for(row, rest, month)
            if not names:
                continue
            head = f"📅 Эти {money_text(row.amount, row.currency)} — {month}?"
            waiting = f"{month.capitalize()} ещё ждём:"
            text = f"{head}\n{waiting} " + "; ".join(names)
            questions[number] = row
            buttons = [
                action(month.capitalize(), f"{PREFIX}{number}:{last:%Y-%m}"),
                action(this.capitalize(), f"{PREFIX}{number}:{today:%Y-%m}"),
            ]
            await send_report(
                context.bot,
                chat_id,
                Report(text, heading(head, 2) + para(waiting, *names)),
                reply_markup=InlineKeyboardMarkup([buttons]),
            )
    except Exception:
        logger.exception("Month question failed for chat_id=%s", chat_id)


async def _answer(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    """Ответ на кнопку вместо вопроса — крупно; то же нажатие ещё раз — не ошибка."""
    try:
        await edit_report(
            context.bot,
            query.message.chat_id,
            query.message.message_id,
            note_report(text),
            reply_markup=reply_markup,
            edit=query.edit_message_text,
        )
    except BadRequest as error:
        if "not modified" not in str(error).lower():
            raise


async def _refused(
    context: ContextTypes.DEFAULT_TYPE, query: CallbackQuery, number: int, mark: str
) -> None:
    """Пометку не записали: строки нет, в ней уже есть пометка или её поправили."""
    gs = context.bot_data["gs_service"]
    try:
        cells = await asyncio.to_thread(gs.fact_row_cells, number)
    except Exception:
        logger.exception("Reading fact row %s failed", number)
        cells = []
    if cells is None:
        await _answer(context, query, f"Строки {number} уже нет — пометку не поставил")
        return
    comment = str(cells[5]) if len(cells) > 5 else ""
    found = MONTH_MARK.search(comment)
    if found is not None:
        context.user_data.get(QUESTIONS, {}).pop(number, None)
        await _answer(
            context,
            query,
            f"✓ В строке {number} уже стоит «за {found.group(1).lower()}»",
        )
        return
    await _answer(
        context,
        query,
        f"Строку {number} уже поменяли — пометку не поставил. "
        f"Допишите в комментарий строки «{mark}»",
    )


async def month_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """mon:<строка fact>:<ГГГГ-ММ> — пометить доход этим месяцем."""
    query = update.callback_query
    await query.answer()
    _, number_text, month_text = (query.data or "").split(":")
    number = int(number_text)
    mark = _mark(date.fromisoformat(f"{month_text}-01"))
    questions = context.user_data.setdefault(QUESTIONS, {})
    stored = questions.get(number)
    if stored is None:
        await _answer(
            context, query, f"Вопрос устарел — допишите в комментарий строки «{mark}»"
        )
        return
    if isinstance(stored, str):  # второе нажатие: пометка уже стоит
        await _answer(context, query, f"✓ Уже отмечено: строка {number} — {stored}")
        return
    row: SheetRow = stored
    comment = f"{row.comment} {mark}".strip()
    gs = context.bot_data["gs_service"]
    try:
        written = await asyncio.to_thread(gs.comment_row, number, row, comment)
    except Exception:
        logger.exception("Marking fact row %s failed", number)
        await _answer(
            context,
            query,
            "Таблица не ответила — нажмите кнопку ещё раз",
            reply_markup=query.message.reply_markup,
        )
        return
    if not written:
        await _refused(context, query, number, mark)
        return
    questions[number] = mark
    last = context.user_data.get(LAST_WRITE)
    if last and last["first"] <= number <= last["last"]:
        # /undo и /fix сверяют строку с записанной — пусть видят новый комментарий
        last["rows"][number - last["first"]] = replace(row, comment=comment)
    mark_dirty(context.bot_data)  # «Пришло к утру» — по новой пометке
    await _answer(context, query, f"✓ {money_text(row.amount, row.currency)} — {mark}")
