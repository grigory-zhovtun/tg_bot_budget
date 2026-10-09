"""/fix — поменять группу и подгруппу последней записи (AI ошибся с категорией).

Сценарий: /fix → [какую операцию, если их несколько] → группа → подгруппа.
Кнопки ссылаются на номера в списках справочника (callback_data ≤ 64 байт),
перед записью строка сверяется с таблицей, как в /undo.
"""

import asyncio
import dataclasses
import logging

from telegram import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from app.domain import INCOME_GROUP, SheetRow, format_amount
from app.errors import user_message
from app.handlers.common import LAST_WRITE
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

# user_data: {"write": снимок последней записи, "row": индекс строки, "category": группа}
STATE = "fix_state"
CANCEL = InlineKeyboardButton("✖️ Отмена", callback_data="fix:x")
STALE = "Кнопки устарели — наберите /fix ещё раз."


def describe(row: SheetRow) -> str:
    comment = row.comment.removeprefix("AI: ").strip()
    note = f" • {comment[:40]}" if comment else ""
    return f"{format_amount(row)}{note} • {row.category} ({row.subcategory}) • {row.source}"


def amount_for(row: SheetRow, category: str) -> float | None:
    """Сумма D в новой группе так, чтобы деньги шли в ту же сторону.

    Доход пишется плюсом, прочий приход — минусом, расход — плюсом. Приход
    между доходом и прочими статьями меняет знак; списание доходом стать
    не может — None.
    """
    was_income, now_income = row.category == INCOME_GROUP, category == INCOME_GROUP
    if was_income == now_income:
        return row.amount
    if now_income:
        return -row.amount if row.amount < 0 else None
    return -row.amount


def _grid(
    buttons: list[InlineKeyboardButton], per_row: int
) -> list[list[InlineKeyboardButton]]:
    return [buttons[i : i + per_row] for i in range(0, len(buttons), per_row)]


def categories_keyboard(categories: list[str]) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(name, callback_data=f"fix:c:{index}")
        for index, name in enumerate(categories)
    ]
    return InlineKeyboardMarkup([*_grid(buttons, 3), [CANCEL]])


def subcategories_keyboard(subcategories: list[str]) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(name, callback_data=f"fix:s:{index}")
        for index, name in enumerate(subcategories)
    ]
    back = InlineKeyboardButton("⬅️ Назад", callback_data="fix:b")
    return InlineKeyboardMarkup([*_grid(buttons, 2), [back, CANCEL]])


def rows_keyboard(rows: list[SheetRow]) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                f"{i + 1}) {format_amount(row)} • {row.subcategory}",
                callback_data=f"fix:r:{i}",
            )
        ]
        for i, row in enumerate(rows)
    ]
    return InlineKeyboardMarkup([*buttons, [CANCEL]])


def _snapshot(last: dict) -> tuple:
    """Какая запись сейчас последняя: кнопки старой записи не должны править новую."""
    return last["first"], tuple(last["rows"])


def _index(rest: list[str], items: list) -> int | None:
    """Номер из кнопки, если он ещё указывает на элемент списка."""
    if len(rest) == 1 and rest[0].isdigit() and int(rest[0]) < len(items):
        return int(rest[0])
    return None


async def _edit(
    query: CallbackQuery, text: str, markup: InlineKeyboardMarkup | None = None
) -> None:
    """Обновить сообщение /fix; повторное нажатие той же кнопки — не ошибка."""
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except BadRequest as error:
        if "not modified" not in str(error).lower():
            raise


async def fix_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    last = context.user_data.get(LAST_WRITE)
    if not last:
        await update.message.reply_text(
            "Нечего исправлять: последней записи нет — она забывается после "
            "перезапуска бота, /undo и импорта выписки."
        )
        return
    rows: list[SheetRow] = last["rows"]
    state = {"write": _snapshot(last)}
    context.user_data[STATE] = state
    if len(rows) == 1:
        state["row"] = 0
        await update.message.reply_text(
            f"✏️ {describe(rows[0])}\nВыберите группу:",
            reply_markup=categories_keyboard(context.bot_data.get("categories", [])),
        )
        return
    listing = "\n".join(f"{i + 1}) {describe(row)}" for i, row in enumerate(rows))
    await update.message.reply_text(
        f"✏️ Какую операцию исправить?\n{listing}", reply_markup=rows_keyboard(rows)
    )


async def fix_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки /fix: fix:r:<i> — операция, fix:c:<i> — группа, fix:s:<i> — подгруппа."""
    query = update.callback_query
    _, action, *rest = (query.data or "").split(":")
    await query.answer()
    if action == "x":
        context.user_data.pop(STATE, None)
        await _edit(query, "Исправление отменено.")
        return
    last = context.user_data.get(LAST_WRITE)
    state = context.user_data.get(STATE)
    if not last or not state or state["write"] != _snapshot(last):
        context.user_data.pop(STATE, None)
        await _edit(query, STALE)
        return
    rows: list[SheetRow] = last["rows"]
    categories: list[str] = context.bot_data.get("categories", [])
    subcategories: dict[str, list[str]] = context.bot_data.get("subcategories", {})

    if action == "r":
        index = _index(rest, rows)
        if index is None:
            await _edit(query, STALE)
            return
        state["row"] = index
        await _edit(
            query,
            f"✏️ {describe(rows[index])}\nВыберите группу:",
            categories_keyboard(categories),
        )
        return
    if "row" not in state:
        await _edit(query, STALE)
        return
    row = rows[state["row"]]
    if action == "b":
        state.pop("category", None)
        await _edit(
            query,
            f"✏️ {describe(row)}\nВыберите группу:",
            categories_keyboard(categories),
        )
        return
    if action == "c":
        index = _index(rest, categories)
        if index is None:
            await _edit(query, STALE)
            return
        category = state["category"] = categories[index]
        await _edit(
            query,
            f"✏️ {describe(row)}\n{category} — выберите подгруппу:",
            subcategories_keyboard(subcategories.get(category, [])),
        )
        return
    if action != "s" or "category" not in state:
        await _edit(query, STALE)
        return

    category = state["category"]
    names = subcategories.get(category, [])
    index = _index(rest, names)
    if index is None:
        await _edit(query, STALE)
        return
    number = last["first"] + state["row"]
    amount = amount_for(row, category)
    if amount is None:
        context.user_data.pop(STATE, None)
        await _edit(
            query,
            "Это списание — в доходы его не перенести. Если на самом деле это "
            f"поступление, поправьте строку {number} в таблице или удалите "
            "запись: /undo",
        )
        return
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    try:
        changed = await asyncio.to_thread(
            gs_service.recategorize_row, number, row, category, names[index], amount
        )
    except Exception as error:
        logger.exception("Recategorization failed")
        await _edit(query, f"❌ Не исправил: {user_message(error)}. /fix — ещё раз")
        return
    context.user_data.pop(STATE, None)
    if not changed:
        await _edit(
            query, f"Строка {number} уже изменена в таблице — поправьте её там."
        )
        return
    fixed = dataclasses.replace(
        row, category=category, subcategory=names[index], amount=amount
    )
    rows[state["row"]] = fixed  # /undo и следующий /fix сверяют уже новую категорию
    if ai_service := context.bot_data.get("ai_service"):
        ai_service.forget_history()  # магазин сразу пойдёт в исправленную категорию
    logger.info("Row %s recategorized", number)
    await _edit(query, f"✅ Исправил (строка {number}): {describe(fixed)}")
