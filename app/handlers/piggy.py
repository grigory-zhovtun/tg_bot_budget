"""Копилки в чате (спека 7): «🐷» после поступления и «Отложил всё».

Кнопки не опираются на память бота (после перезапуска её нет): всё считается по
таблице заново под тем же замком, что правки копилок из Mini App. Хелперы строки
журнала и заметки — общие с `app/web/api.py`.
"""

import asyncio
import logging
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.ext import ContextTypes

from app import config
from app.domain import SheetRow, local_today
from app.handlers.common import app_url, notify_others
from app.handlers.receipts import mark_dirty
from app.services.analytics_service import money
from app.services.piggy import SAVED, Piggy, PiggyState, Screen
from app.services.piggy_sync import bot_catalog
from app.services.savings import incomes
from app.utils.keyboards import action

logger = logging.getLogger(__name__)

PIGGY_LOCK = "piggy_lock"  # bot_data: в листы копилок пишет один запрос за раз
ALL = "piggy:all:"  # «Отложил всё» месяца подсказки: piggy:all:2026-10
WHOLE_CURRENCIES = frozenset({"UZS", "RUB"})  # остальные валюты — до центов
# «по плану ноября», «за октябрь», «в ноябре»
MONTHS_OF = ("января", "февраля", "марта", "апреля", "мая", "июня", "июля",
             "августа", "сентября", "октября", "ноября", "декабря")  # fmt: skip
MONTHS_NAMED = ("январь", "февраль", "март", "апрель", "май", "июнь", "июль",
                "август", "сентябрь", "октябрь", "ноябрь", "декабрь")  # fmt: skip
MONTHS_IN = ("январе", "феврале", "марте", "апреле", "мае", "июне", "июле",
             "августе", "сентябре", "октябре", "ноябре", "декабре")  # fmt: skip


def amount_text(value: float) -> str:
    """«12 000 000», «210», «210,50» — для заметок и ошибок."""
    return money(value) if value == int(value) else f"{value:.2f}".replace(".", ",")


def rounded(value: float, currency: str) -> float:
    return float(round(value)) if currency in WHOLE_CURRENCIES else round(value, 2)


def move_row(
    day: date, piggy: Piggy, amount: float, kind: str, author: str, key: str = ""
) -> list[Any]:
    """Строка журнала копилок: сумма в валюте копилки со знаком, округлённая, как
    сумма валюты (без хвостов вида 66.77000000000001 и без «-0»)."""
    amount = rounded(amount, piggy.currency) or 0.0
    return [f"{day:%d.%m.%Y}", piggy.id, piggy.name, amount, piggy.currency, kind,
            author, key]  # fmt: skip


def piggy_note(action_name: str, author: str, text: str) -> str:
    """«🐷 Иван — новая копилка: …»; без имени — «🐷 Новая копилка: …»."""
    head = (
        f"{author} — {action_name}"
        if author
        else action_name[:1].upper() + action_name[1:]
    )
    return f"🐷 {head}: {text}"


def piggy_lock(bot_data: dict[str, Any]) -> asyncio.Lock:
    """Записи в листы копилок — по одной, и из чата, и из Mini App."""
    return bot_data.setdefault(PIGGY_LOCK, asyncio.Lock())


async def _screen(bot_data: Mapping[str, Any]) -> Screen | None:
    """«Накопления» по таблице заново; нет сервиса или вкладки месяца — None."""
    analytics = bot_data.get("analytics_service")
    if analytics is None:
        return None
    today = local_today(config.ANALYTICS_TIMEZONE)
    found = await asyncio.to_thread(
        analytics.savings, today, *bot_catalog(bot_data), 0.0
    )
    return None if found is None else found.screen


def _left(screen: Screen | None) -> list[PiggyState]:
    """Копилки и долгие цели, у которых «осталось в этом месяце» больше нуля."""
    if screen is None:
        return []
    return [
        state
        for state in screen.piggies
        if state.status != "broken" and state.left_this_month > 0
    ]


def _sums(items: Sequence[PiggyState], quoted: bool) -> str:
    """«Новый год» 6 000 000 UZS, «Отпуск» 210 USD — что осталось отложить."""
    return ", ".join(
        f"{'«' if quoted else ''}{state.piggy.name}{'»' if quoted else ''} "
        f"{amount_text(state.left_this_month)} {state.piggy.currency}"
        for state in items
    )


def _part_button(user_id: int) -> InlineKeyboardButton | None:
    """«Отложил часть…» — окно «Отложил…» приложения; без страницы — нет кнопки."""
    url = app_url(user_id)
    if url is None:
        return None
    return InlineKeyboardButton(
        "Отложил часть…", web_app=WebAppInfo(f"{url}&open=save")
    )


async def income_prompt(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_id: int,
    rows: Sequence[SheetRow],
) -> None:
    """«🐷 В копилки по плану ноября осталось: …» после поступления (спека 7.1);
    отложить нечего — сообщения нет, сбой — только в лог."""
    if not incomes(rows):
        return
    try:
        items = _left(await _screen(context.bot_data))
        if not items:
            return
        today = local_today(config.ANALYTICS_TIMEZONE)
        buttons = [action("Отложил всё", f"{ALL}{today:%Y-%m}")]
        if part := _part_button(user_id):
            buttons.append(part)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🐷 В копилки по плану {MONTHS_OF[today.month - 1]} осталось: "
            + _sums(items, quoted=True),
            reply_markup=InlineKeyboardMarkup([buttons]),
        )
    except Exception:
        logger.exception("Piggy prompt failed for chat_id=%s", chat_id)


async def piggy_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки «🐷»: piggy:all:<месяц> — «Отложил всё»."""
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if data.startswith(ALL):
        await _set_all_aside(update, context, data.removeprefix(ALL))


async def _set_all_aside(
    update: Update, context: ContextTypes.DEFAULT_TYPE, month: str
) -> None:
    """«Отложил всё»: «осталось в этом месяце» — в журнал; второй раз — ничего."""
    query, user = update.callback_query, update.effective_user
    today = local_today(config.ANALYTICS_TIMEZONE)
    if month != f"{today:%Y-%m}":
        try:
            named = MONTHS_NAMED[int(month[5:7]) - 1]
        except (ValueError, IndexError):
            named = "прошлый месяц"
        part = _part_button(user.id)
        await query.edit_message_text(
            f"Подсказка за {named} устарела: в {MONTHS_IN[today.month - 1]} свой план",
            reply_markup=InlineKeyboardMarkup([[part]]) if part else None,
        )
        return
    author = user.first_name or ""
    try:
        async with piggy_lock(context.bot_data):
            items = _left(await _screen(context.bot_data))
            if items:
                await asyncio.to_thread(
                    context.bot_data["gs_service"].add_piggy_moves,
                    [move_row(today, state.piggy, state.left_this_month, SAVED, author)
                     for state in items],  # fmt: skip
                )
    except Exception:
        logger.exception("Piggy set-aside failed for user_id=%s", user.id)
        message = getattr(query, "message", None)
        await query.edit_message_text(
            "Не записал — таблица не ответила. Нажмите ещё раз.",
            reply_markup=getattr(message, "reply_markup", None),
        )
        return
    if not items:
        await query.edit_message_text("Уже записано", reply_markup=None)
        return
    mark_dirty(context.bot_data)
    await query.edit_message_text(
        f"🐷 Записал: {_sums(items, quoted=True)}", reply_markup=None
    )
    note = piggy_note(SAVED, author, _sums(items, quoted=False))
    await notify_others(context.bot, user.id, note)
