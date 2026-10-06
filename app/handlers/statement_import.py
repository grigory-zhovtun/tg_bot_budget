"""Импорт выписок Капиталбанка: PDF → сводка с кнопками → запись в fact.

Выписки нескольких карт удобно присылать вместе: бот ждёт несколько секунд
после последнего PDF и разбирает их разом — так переводы между своими картами
находят пару. Всё про один импорт показывается в одном сообщении.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import date

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from app import config
from app.domain import INCOME_GROUP, Catalog, currency_of
from app.errors import user_message
from app.handlers.common import LAST_WRITE
from app.services.google_sheets import GoogleSheetsService
from app.statements import (
    TEMPORARY_MARK,
    ImportPlan,
    ImportRow,
    Statement,
    plan_import,
    short,
)

logger = logging.getLogger(__name__)

BASKET = "statement_import"
SEQUENCE = "statement_import_seq"
PREVIEW_DELAY = 4  # секунды: дождаться остальных PDF, отправленных вместе
BASKET_TTL = 30 * 60
AI_TIMEOUT = 60
MAX_REVIEW_LINES = 15


@dataclass
class Basket:
    """Выписки одного импорта; version меняется с каждой новой выпиской."""

    statements: list[Statement]
    version: int
    created: float = field(default_factory=time.monotonic)
    message_id: int | None = None
    ai_categories: dict[str, tuple[str, str]] = field(default_factory=dict)
    ai_asked: set[str] = field(default_factory=set)


def _catalog(context: ContextTypes.DEFAULT_TYPE) -> Catalog:
    return Catalog(
        categories=context.bot_data.get("categories", []),
        subcategories=context.bot_data.get("subcategories", {}),
        sources=context.bot_data.get("sources", []),
    )


def _money(value: float, currency: str) -> str:
    digits = f"{abs(value):,.2f}" if currency == "USD" else f"{abs(value):,.0f}"
    return digits.replace(",", " ")


def _signed(value: float, currency: str) -> str:
    return f"{'+' if value > 0 else '−'}{_money(value, currency)}"


def _day(value: date) -> str:
    return value.strftime("%d.%m.%Y")


def _review_line(item: ImportRow) -> str:
    row = item.row
    comment = row.comment.removeprefix("AI: ")[:45]
    sign = "+" if row.category == INCOME_GROUP or row.amount < 0 else ""
    return (
        f"{row.day:%d.%m} {short(row.source)} {sign}{_money(row.amount, row.currency)}"
        f" — {item.review}: {comment}"
    )


def format_plan(plan: ImportPlan) -> str:
    lines = ["📄 Выписки Капиталбанка: " + ", ".join(plan.sources)]
    if plan.period:
        start, end = plan.period
        lines.append(f"Период {start:%d.%m}–{_day(end)}, операций: {plan.total}")
    lines.append("")
    if plan.covered:
        lines.append(f"• загружены раньше: {plan.covered}")
    if plan.already:
        lines.append(f"• уже есть в таблице: {plan.already}")
    lines.append(f"• новых строк: {len(plan.rows)}")
    if plan.adjustments:
        lines.append(f"• уточню сумму по выписке: {len(plan.adjustments)}")
    for fact in plan.removals:
        lines.append(
            f"• удалю временную строку {fact.number} "
            f"({_money(fact.amount, currency_of(fact.source))}, {short(fact.source)})"
        )

    effect = plan.balance_effect()
    if effect:
        lines += ["", "Остаток по таблице изменится:"]
        lines += [
            f"{source}: {_signed(value, currency_of(source))}"
            for source, value in sorted(effect.items())
        ]

    review = plan.review()
    if review:
        lines += ["", f"Проверьте после записи ({len(review)}):"]
        lines += [_review_line(item) for item in review[:MAX_REVIEW_LINES]]
        if len(review) > MAX_REVIEW_LINES:
            lines.append(f"…и ещё {len(review) - MAX_REVIEW_LINES}")
    lines += [f"\n⚠️ {warning}" for warning in plan.warnings]
    return "\n".join(lines)


def _buttons(version: int, rows: int) -> InlineKeyboardMarkup:
    label = f"✅ Записать ({rows})" if rows else "✅ Применить"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(label, callback_data=f"import:ok:{version}"),
                InlineKeyboardButton("❌ Отмена", callback_data=f"import:no:{version}"),
            ]
        ]
    )


async def _show(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    basket: Basket,
    text: str,
    markup: InlineKeyboardMarkup | None = None,
) -> None:
    """Обновить сообщение импорта; если его удалили — прислать новое."""
    if basket.message_id is not None:
        try:
            await context.bot.edit_message_text(
                text, chat_id=chat_id, message_id=basket.message_id, reply_markup=markup
            )
            return
        except BadRequest as error:
            if "not modified" in str(error).lower():
                return
            logger.info("Import message is gone, sending a new one: %s", error)
    message = await context.bot.send_message(chat_id, text, reply_markup=markup)
    basket.message_id = message.message_id


def _untrack(context: ContextTypes.DEFAULT_TYPE, message: Message) -> None:
    """Сообщение импорта не должно исчезать при очистке чата другими командами."""
    tracked = context.user_data.get("bot_messages", [])
    if message.message_id in tracked:
        tracked.remove(message.message_id)


async def receive_statement(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    statement: Statement,
    status: Message,
) -> None:
    """Добавить выписку к текущему импорту и через пару секунд показать сводку."""
    chat_id = update.effective_chat.id
    basket: Basket | None = context.user_data.get(BASKET)
    if basket is None or time.monotonic() - basket.created > BASKET_TTL:
        basket = Basket(statements=[], version=0)
        context.user_data[BASKET] = basket
    context.user_data[SEQUENCE] = context.user_data.get(SEQUENCE, 0) + 1
    basket.version = context.user_data[SEQUENCE]
    basket.statements.append(statement)

    _untrack(context, status)
    if basket.message_id is None:
        basket.message_id = status.message_id
    else:
        try:
            await status.delete()
        except Exception as error:
            logger.debug("Could not delete status message: %s", error)

    names = ", ".join(sorted({s.source for s in basket.statements}))
    await _show(
        context,
        chat_id,
        basket,
        f"📄 Выписки: {names}\nОпераций: "
        f"{sum(len(s.txns) for s in basket.statements)}. Готовлю сводку…",
    )

    name = f"statement-preview-{chat_id}"
    for job in context.job_queue.get_jobs_by_name(name):
        job.schedule_removal()
    context.job_queue.run_once(
        _preview_job,
        PREVIEW_DELAY,
        chat_id=chat_id,
        user_id=update.effective_user.id,
        name=name,
        data=basket.version,
    )


async def _build_plan(context: ContextTypes.DEFAULT_TYPE, basket: Basket) -> ImportPlan:
    """Свежие данные таблицы → план; новые магазины один раз спросить у Gemini."""
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    catalog = _catalog(context)
    fact = await asyncio.to_thread(gs_service.get_values, config.FACT_SHEET_NAME)
    marks = await asyncio.to_thread(gs_service.get_statement_marks)
    plan = plan_import(basket.statements, catalog, fact, marks, basket.ai_categories)

    ai_service = context.bot_data.get("ai_service")
    unknown = [name for name in plan.unknown_merchants() if name not in basket.ai_asked]
    if not unknown or ai_service is None or not ai_service.enabled:
        return plan
    basket.ai_asked.update(unknown)
    try:
        found = await asyncio.wait_for(
            ai_service.categorize_merchants(unknown, catalog), AI_TIMEOUT
        )
    except Exception as error:  # AI только помогает: без него магазины в «РАЗНОЕ»
        logger.warning("Merchant categorization skipped: %s", user_message(error))
        return plan
    if not found:
        return plan
    basket.ai_categories.update(found)
    return plan_import(basket.statements, catalog, fact, marks, basket.ai_categories)


async def _preview_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = context.job.chat_id
    basket: Basket | None = context.user_data.get(BASKET)
    if basket is None or basket.version != context.job.data:
        return  # пришла ещё выписка или импорт уже завершён
    try:
        plan = await _build_plan(context, basket)
    except Exception as error:
        logger.exception("Statement import preview failed")
        if basket.version != context.job.data:
            return  # пришла ещё выписка: следующий запуск попробует снова
        context.user_data.pop(BASKET, None)
        await _show(
            context, chat_id, basket, f"❌ Не разобрал выписки: {user_message(error)}"
        )
        return
    if (
        context.user_data.get(BASKET) is not basket
        or basket.version != context.job.data
    ):
        return  # пока считали, пришла ещё выписка — сводку покажет следующий запуск

    if plan.has_changes:
        await _show(
            context,
            chat_id,
            basket,
            format_plan(plan),
            _buttons(basket.version, len(plan.rows)),
        )
        return

    # Записывать нечего: только отметить, по какой день выписки загружены
    context.user_data.pop(BASKET, None)
    text = format_plan(plan) + "\n\n✅ Всё из выписок уже есть в таблице."
    if plan.marks:
        gs_service: GoogleSheetsService = context.bot_data["gs_service"]
        try:
            await asyncio.to_thread(gs_service.set_statement_marks, plan.marks)
            text += "\nОтметил в system: " + _marks_text(plan.marks)
        except Exception:
            logger.exception("Writing statement marks failed")
    await _show(context, chat_id, basket, text)


def _marks_text(marks: dict[str, date]) -> str:
    return ", ".join(
        f"{short(source)} по {_day(day)}" for source, day in sorted(marks.items())
    )


def apply_plan(gs_service: GoogleSheetsService, plan: ImportPlan) -> list[str]:
    """Записать план в таблицу (синхронно, в отдельном потоке); строки отчёта.

    Порядок важен: сначала правки и удаления по номерам строк, потом дописывание
    в конец, отметки — последними. Если что-то упадёт посередине, повторный
    импорт тех же выписок доделает остальное и ничего не задвоит.
    """
    report: list[str] = []
    if plan.adjustments:
        gs_service.update_fact_amounts(
            [
                (adj.fact.number, adj.amount, f"{adj.fact.comment}; сумма по выписке")
                for adj in plan.adjustments
            ]
        )
        report.append(f"• уточнил сумм по выписке: {len(plan.adjustments)}")
    if plan.removals:
        deleted = gs_service.delete_fact_rows(
            [fact.number for fact in plan.removals], must_contain=TEMPORARY_MARK
        )
        report.append(f"• удалил временных строк: {len(deleted)}")
    if plan.rows:
        first, last = gs_service.append_transactions([item.row for item in plan.rows])
        report.insert(0, f"• записал строк: {len(plan.rows)} (строки {first}–{last})")
    if plan.marks:
        gs_service.set_statement_marks(plan.marks)
        report.append("• отметил в system: " + _marks_text(plan.marks))
    return report


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки сводки: import:ok:<версия> — записать, import:no:<версия> — отмена."""
    query = update.callback_query
    _, action, version = (query.data or "").split(":", 2)
    basket: Basket | None = context.user_data.get(BASKET)
    if basket is None:
        await query.answer()
        await query.edit_message_text("Эта сводка устарела — пришлите выписки ещё раз.")
        return
    if str(basket.version) != version:
        await query.answer("Пришла ещё выписка, сводка обновляется…")
        return
    await query.answer()

    context.user_data.pop(BASKET, None)  # второе нажатие уже ничего не сделает
    if action != "ok":
        await query.edit_message_text("❌ Импорт выписок отменён, таблица не менялась.")
        return

    chat_id = update.effective_chat.id
    await _show(context, chat_id, basket, "⏳ Записываю в таблицу…")
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    try:
        plan = await _build_plan(context, basket)  # таблица могла измениться
        report = await asyncio.to_thread(apply_plan, gs_service, plan)
    except Exception as error:
        logger.exception("Statement import failed")
        await _show(
            context,
            chat_id,
            basket,
            f"❌ Не записал: {user_message(error)}\n"
            "Пришлите выписки ещё раз — уже записанное бот не повторит.",
        )
        return

    # строки сдвинулись (удаление временных) — /undo для прошлой записи уже неверен
    context.user_data.pop(LAST_WRITE, None)
    lines = ["✅ Выписки загружены", *report]
    try:
        balances = await asyncio.to_thread(gs_service.get_table_balances)
    except Exception:
        logger.exception("Reading table balances failed")
        balances = {}
    shown = [s for s in plan.sources if s in balances]
    if shown:
        lines += ["", "Остаток по таблице — сверьте с приложением банка:"]
        lines += [
            f"{source}: {_money(balances[source], currency_of(source))}"
            for source in shown
        ]
    review = plan.review()
    if review:
        lines += ["", f"Проверьте категории ({len(review)}):"]
        lines += [_review_line(item) for item in review[:MAX_REVIEW_LINES]]
    await _show(context, chat_id, basket, "\n".join(lines))
    logger.info(
        "Statement import: %d row(s), %d adjusted, %d removed",
        len(plan.rows),
        len(plan.adjustments),
        len(plan.removals),
    )
