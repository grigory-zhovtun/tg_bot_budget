"""/fix — поменять группу и подгруппу последней записи (AI ошибся с категорией).

Сценарий: /fix → [какую операцию, если их несколько] → группа → подгруппа.
Кнопки ссылаются на номера в списках справочника (callback_data ≤ 64 байт),
перед записью строка сверяется с таблицей, как в /undo.

Тот же диалог проверяет строки SMS с пометкой 🆕 или ❓: единственную — прямо в
меню под сводкой («inline»), несколько — по очереди отдельным сообщением
(«queue», кнопка «🔎 Проверить»). «✓ Верно» оставляет категорию как есть.
"""

import asyncio
import dataclasses
import logging
from collections.abc import Awaitable, Callable

from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)
from telegram.constants import KeyboardButtonStyle
from telegram.error import BadRequest, TelegramError
from telegram.ext import ContextTypes

from app.custom_icons import CustomIcons
from app.domain import (
    FALLBACK_CATEGORY,
    INCOME_GROUP,
    SheetRow,
    format_amount,
    row_name,
)
from app.errors import user_message
from app.handlers.common import LAST_WRITE, premium_icons
from app.handlers.income_month import ask_month
from app.handlers.piggy import income_prompt
from app.handlers.receipts import mark_dirty
from app.services.google_sheets import GoogleSheetsService
from app.services.savings import save_hint
from app.sms_rows import NEW
from app.utils.keyboards import (
    LAST_UNDO,
    action,
    categories_menu,
    group_button,
    menu_prompt,
    record_actions,
    subcategory_button,
)

logger = logging.getLogger(__name__)

# user_data: {"write": снимок последней записи, "row": индекс строки, "category":
# группа; у проверки ещё "mode", а в очереди — "queue", "done", "fixed", "total"}
STATE = "fix_state"
INLINE, QUEUE = "inline", "queue"
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


def _groups(
    categories: list[str], custom: CustomIcons | None
) -> list[list[InlineKeyboardButton]]:
    buttons = [
        group_button(name, f"fix:c:{index}", custom)
        for index, name in enumerate(categories)
    ]
    return _grid(buttons, 2)


def categories_keyboard(
    categories: list[str], custom: CustomIcons | None = None
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([*_groups(categories, custom), [CANCEL]])


def subcategories_keyboard(
    subcategories: list[str],
    icons: dict[str, str] | None = None,
    custom: CustomIcons | None = None,
) -> InlineKeyboardMarkup:
    buttons = [
        subcategory_button(name, f"fix:s:{index}", icons, custom)
        for index, name in enumerate(subcategories)
    ]
    back = InlineKeyboardButton("⬅️ Назад", callback_data="fix:b")
    return InlineKeyboardMarkup([*_grid(buttons, 2), [back, CANCEL]])


def check_keyboard(
    categories: list[str], custom: CustomIcons | None, mode: str
) -> InlineKeyboardMarkup:
    """[✓ Верно], группы по две, внизу «↩️ Отменить запись» или «⏹ Хватит»."""
    if mode == INLINE:
        bottom = action("↩️ Отменить запись", LAST_UNDO, KeyboardButtonStyle.DANGER)
    else:
        bottom = InlineKeyboardButton("⏹ Хватит", callback_data="fix:x")
    ok = action("✓ Верно", "fix:ok", KeyboardButtonStyle.SUCCESS)
    return InlineKeyboardMarkup([[ok], *_groups(categories, custom), [bottom]])


def check_text(last: dict, index: int, prefix: str = "") -> str:
    """«🆕 OOO SHAVI CAFE • 48 000 UZS • VISA 9090 UZS» и что с категорией."""
    row: SheetRow = last["rows"][index]
    flag = last["checks"].get(index, "")
    guessed = row.category != FALLBACK_CATEGORY
    lead = "ИИ предлагает" if flag == NEW and guessed else "Записал как"
    question = "Верно?" if guessed else "Что это?"
    return (
        f"{prefix}{flag} {row_name(row)} • {format_amount(row)} • {row.source}\n"
        f"{lead}: {row.category} / {row.subcategory}. {question}"
    )


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


def _menu(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    """Кнопки последней записи: на сводке или в меню под ней — с группами, если они
    включены (так их показала запись)."""
    last = context.user_data.get(LAST_WRITE)
    check = len(last.get("checks", {})) if last else 0
    if last and last.get("on_card"):
        return InlineKeyboardMarkup(record_actions(check, on_card=True))
    return categories_menu(
        context.bot_data.get("categories", []),
        last is not None,
        premium_icons(context),
        check=check,
        groups=last.get("groups", True) if last else True,
    )


def inline_check(
    context: ContextTypes.DEFAULT_TYPE,
) -> tuple[str, InlineKeyboardMarkup]:
    """Проверка единственной строки записи: текст и кнопки меню под сводкой."""
    last = context.user_data[LAST_WRITE]
    index = min(last["checks"])
    context.user_data[STATE] = {"write": _snapshot(last), "row": index, "mode": INLINE}
    categories = context.bot_data.get("categories", [])
    return check_text(last, index), check_keyboard(
        categories, premium_icons(context), INLINE
    )


def _queue_prompt(
    context: ContextTypes.DEFAULT_TYPE, state: dict, last: dict
) -> tuple[str, InlineKeyboardMarkup]:
    prefix = f"🔎 {state['done'] + 1} из {state['total']}. "
    categories = context.bot_data.get("categories", [])
    return check_text(last, state["row"], prefix), check_keyboard(
        categories, premium_icons(context), QUEUE
    )


async def start_check(
    reply: Callable[..., Awaitable[Message]], context: ContextTypes.DEFAULT_TYPE
) -> Message:
    """«🔎 Проверить»: строки с пометкой по одной, новым сообщением."""
    last = context.user_data.get(LAST_WRITE)
    pending = sorted(last.get("checks", {})) if last else []
    if not pending:
        context.user_data.pop(STATE, None)
        return await reply("Проверять нечего: все строки этой записи уже проверены.")
    state = {
        "write": _snapshot(last),
        "row": pending[0],
        "mode": QUEUE,
        "queue": pending,
        "done": 0,
        "fixed": 0,
        "total": len(pending),
    }
    context.user_data[STATE] = state
    text, markup = _queue_prompt(context, state, last)
    return await reply(text, reply_markup=markup)


async def start_fix(
    reply: Callable[..., Awaitable[Message]], context: ContextTypes.DEFAULT_TYPE
) -> Message:
    """Начало диалога /fix (команда или кнопка под сводкой); reply шлёт сообщение."""
    last = context.user_data.get(LAST_WRITE)
    if not last:
        return await reply(
            "Нечего исправлять: последней записи нет — она забывается после "
            "перезапуска бота, /undo и импорта выписки."
        )
    rows: list[SheetRow] = last["rows"]
    state = {"write": _snapshot(last)}
    context.user_data[STATE] = state
    if len(rows) == 1:
        state["row"] = 0
        return await reply(
            f"✏️ {describe(rows[0])}\nВыберите группу:",
            reply_markup=categories_keyboard(
                context.bot_data.get("categories", []), premium_icons(context)
            ),
        )
    listing = "\n".join(f"{i + 1}) {describe(row)}" for i, row in enumerate(rows))
    return await reply(
        f"✏️ Какую операцию исправить?\n{listing}", reply_markup=rows_keyboard(rows)
    )


async def fix_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_fix(update.message.reply_text, context)


async def _inline_done(
    query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    """Проверка в меню закончилась: под текстом снова меню записи; если кнопки
    записи на самой сводке — в сообщении проверки остаётся только итог."""
    context.user_data.pop(STATE, None)
    last = context.user_data.get(LAST_WRITE)
    if last and last.get("on_card"):
        await _edit(query, text)
        return
    source = context.user_data.get("source") or context.bot_data.get("last_source")
    prompt = menu_prompt(source, last.get("groups", True) if last else True)
    await _edit(query, f"{text}\n{prompt}", _menu(context))


async def _refresh_menu(context: ContextTypes.DEFAULT_TYPE) -> None:
    """После проверки по очереди кнопка «🔎 Проверить» в меню под сводкой обновляется."""
    last = context.user_data.get(LAST_WRITE)
    if not last or not last.get("menu"):
        return
    chat_id, message_id = last["menu"]
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id, message_id=message_id, reply_markup=_menu(context)
        )
    except TelegramError as error:  # меню уже удалено или не изменилось
        logger.debug("Menu after the check was not updated: %s", error)


async def _checked(
    query: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    state: dict,
    last: dict,
    verdict: str,
    fixed: bool,
) -> None:
    """Строка проверена: в меню — снова группы, в очереди — следующая строка."""
    last.get("checks", {}).pop(state["row"], None)
    if state["mode"] == INLINE:
        await _inline_done(query, context, verdict)
        return
    state["done"] += 1
    state["fixed"] += int(fixed)
    state["queue"] = [i for i in state["queue"] if i in last.get("checks", {})]
    state.pop("category", None)
    if state["queue"]:
        state["row"] = state["queue"][0]
        state["write"] = _snapshot(last)  # строки могли поменяться — новый снимок
        text, markup = _queue_prompt(context, state, last)
        await _edit(query, f"{verdict}\n\n{text}", markup)
        return
    context.user_data.pop(STATE, None)
    summary = f"✅ Проверено {state['done']}, исправлено {state['fixed']}."
    await _edit(query, f"{verdict}\n{summary}")
    await _refresh_menu(context)


async def _stop(
    query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, state: dict, text: str
) -> None:
    """Исправить не вышло: в меню — текст и снова группы, иначе — только текст."""
    if state.get("mode") == INLINE:
        await _inline_done(query, context, text)
        return
    context.user_data.pop(STATE, None)
    await _edit(query, text)


async def _cancel(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    """«✖️ Отмена» и «⏹ Хватит»: в меню — снова группы, в очереди — итог."""
    state = context.user_data.get(STATE) or {}
    last = context.user_data.get(LAST_WRITE)
    current = bool(last) and state.get("write") == _snapshot(last)
    if current and state.get("mode") == INLINE:
        await _inline_done(query, context, "Категорию оставил как есть.")
        return
    context.user_data.pop(STATE, None)
    if current and state.get("mode") == QUEUE:
        done, total = state["done"], state["total"]
        await _edit(query, f"Проверку остановил: проверено {done} из {total}.")
        await _refresh_menu(context)
        return
    await _edit(query, "Исправление отменено.")


async def fix_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки /fix и проверки: fix:r:<i> — операция, fix:c:<i> — группа,
    fix:s:<i> — подгруппа, fix:ok — «✓ Верно», fix:b — назад, fix:x — отмена."""
    query = update.callback_query
    _, step, *rest = (query.data or "").split(":")
    await query.answer()
    if step == "x":
        await _cancel(query, context)
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

    if step == "r":
        index = _index(rest, rows)
        if index is None:
            await _edit(query, STALE)
            return
        state["row"] = index
        await _edit(
            query,
            f"✏️ {describe(rows[index])}\nВыберите группу:",
            categories_keyboard(categories, premium_icons(context)),
        )
        return
    if "row" not in state:
        await _edit(query, STALE)
        return
    row = rows[state["row"]]
    if step == "ok":
        if "mode" not in state:
            await _edit(query, STALE)
            return
        await _checked(
            query, context, state, last, f"👍 Оставил: {describe(row)}", False
        )
        return
    if step == "b":
        state.pop("category", None)
        if state.get("mode") == INLINE:
            await _edit(
                query,
                check_text(last, state["row"]),
                check_keyboard(categories, premium_icons(context), INLINE),
            )
        elif state.get("mode") == QUEUE:
            await _edit(query, *_queue_prompt(context, state, last))
        else:
            await _edit(
                query,
                f"✏️ {describe(row)}\nВыберите группу:",
                categories_keyboard(categories, premium_icons(context)),
            )
        return
    if step == "c":
        index = _index(rest, categories)
        if index is None:
            await _edit(query, STALE)
            return
        category = state["category"] = categories[index]
        await _edit(
            query,
            f"✏️ {describe(row)}\n{category} — выберите подгруппу:",
            subcategories_keyboard(
                subcategories.get(category, []),
                context.bot_data.get("icons"),
                premium_icons(context),
            ),
        )
        return
    if step != "s" or "category" not in state:
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
        await _stop(
            query,
            context,
            state,
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
        await _stop(
            query,
            context,
            state,
            f"❌ Не исправил: {user_message(error)}. /fix — ещё раз",
        )
        return
    if not changed:
        await _stop(
            query,
            context,
            state,
            f"Строка {number} уже изменена в таблице — поправьте её там.",
        )
        return
    fixed = dataclasses.replace(
        row, category=category, subcategory=names[index], amount=amount
    )
    rows[state["row"]] = fixed  # /undo и следующий /fix сверяют уже новую категорию
    if ai_service := context.bot_data.get("ai_service"):
        ai_service.forget_history()  # магазин сразу пойдёт в исправленную категорию
    mark_dirty(context.bot_data)  # доход стал тратой или наоборот — сверка списка
    logger.info("Row %s recategorized", number)
    verdict = f"✅ Исправил (строка {number}): {describe(fixed)}"
    moved_in = row.category != INCOME_GROUP and category == INCOME_GROUP
    if moved_in:
        # зарплата по SMS приходит переводом с ❓ — «отложите» звучит здесь
        hint = await asyncio.to_thread(save_hint, gs_service, [fixed])
        verdict = "\n".join([verdict, *hint])
    if "mode" in state:
        await _checked(query, context, state, last, verdict, True)
    else:
        last.get("checks", {}).pop(state["row"], None)  # исправленная строка проверена
        context.user_data.pop(STATE, None)
        await _edit(query, verdict)
    chat = getattr(update, "effective_chat", None)
    if moved_in and chat is not None:
        # опоздавшая часть ЗП по SMS приходит переводом — месяц спрашиваем здесь
        ask_month(context, chat.id, [fixed], number)
        await income_prompt(context, chat.id, chat.id, [fixed])  # «🐷» под ответом
