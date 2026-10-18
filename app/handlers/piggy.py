"""Копилки в чате (спека 7): «🐷» после поступления и «Отложил всё», «🐷 Из
копилки» после траты и «Не из копилки».

Кнопки не опираются на память бота (после перезапуска её нет): всё считается по
таблице заново под тем же замком, что правки копилок из Mini App. Хелперы строки
журнала и заметки — общие с `app/web/api.py`.
"""

import asyncio
import logging
import math
import time
from collections.abc import Sequence
from datetime import date
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.ext import ContextTypes

from app import config
from app.custom_icons import leading_emoji
from app.domain import INCOME_GROUP, SheetRow, local_today
from app.handlers.common import app_url, notify_others
from app.handlers.receipts import mark_dirty
from app.services.analytics_service import SavingsTab, money
from app.services.piggy import (
    NOT_FROM,
    SAVED,
    Piggy,
    PiggyState,
    Screen,
    fact_key,
    fingerprint,
    income_cushion,
    row_prefix,
)
from app.services.piggy_sync import bot_catalog
from app.services.savings import TINY_SHARE, incomes
from app.utils.keyboards import action

logger = logging.getLogger(__name__)

PIGGY_LOCK = "piggy_lock"  # bot_data: в листы копилок пишет один запрос за раз
ALL = "piggy:all:"  # «Отложил всё» месяца подсказки: piggy:all:2026-10
NO = "piggy:no:"  # «Не из копилки»: piggy:no:<ID>:<отпечаток ключа строки fact>
ARTICLES = "piggy_articles"  # bot_data: (время, статьи открытых копилок)
ARTICLES_AGE = 600.0  # статьи копилок из памяти — не дольше 10 минут
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


async def _savings(bot_data: dict[str, Any]) -> SavingsTab | None:
    """«Накопления» по таблице заново; нет сервиса или вкладки месяца — None.
    Заодно запоминает статьи открытых копилок."""
    analytics = bot_data.get("analytics_service")
    if analytics is None:
        return None
    today = local_today(config.ANALYTICS_TIMEZONE)
    found: SavingsTab | None = await asyncio.to_thread(
        analytics.savings, today, *bot_catalog(bot_data), 0.0
    )
    if found is not None:
        bot_data[ARTICLES] = (
            time.monotonic(),
            {
                (state.piggy.group, state.piggy.subcategory)
                for state in found.screen.piggies
                if state.status != "broken"
                and state.piggy.group
                and state.piggy.subcategory
            },
        )
    return found


async def _screen(bot_data: dict[str, Any]) -> Screen | None:
    found = await _savings(bot_data)
    return None if found is None else found.screen


async def piggy_articles(bot_data: dict[str, Any], gs: Any) -> set[tuple[str, str]]:
    """Статьи открытых копилок: обычная трата не читает таблицу — пары живут в
    памяти и обновляются любым расчётом «Накоплений», сами — раз в 10 минут."""
    known = bot_data.get(ARTICLES)
    if known is not None and time.monotonic() - known[0] < ARTICLES_AGE:
        return known[1]
    rows, _ = await asyncio.to_thread(gs.get_piggy, ARTICLES_AGE)
    found = {
        (str(row[6]).strip(), str(row[7]).strip())
        for row in rows[1:]
        if len(row) > 7
        and str(row[0]).strip()
        and str(row[6]).strip()
        and str(row[7]).strip()
        and not (len(row) > 13 and str(row[13]).strip())
    }
    bot_data[ARTICLES] = (time.monotonic(), found)
    return found


def _left(screen: Screen | None) -> list[PiggyState]:
    """Копилки и долгие цели, у которых «отложить сейчас» больше нуля."""
    if screen is None:
        return []
    return [
        state for state in screen.piggies if state.status != "broken" and state.now > 0
    ]


def _sums(items: Sequence[PiggyState], quoted: bool) -> str:
    """«Новый год» 6 000 000 UZS, «Отпуск» 210 USD — что отложить сейчас."""
    return ", ".join(
        f"{'«' if quoted else ''}{state.piggy.name}{'»' if quoted else ''} "
        f"{amount_text(state.now)} {state.piggy.currency}"
        for state in items
    )


def _up_money(value: float, currency: str) -> float:
    """Вверх: сумы — до тысячи, остальное — до целых (как суммы плана)."""
    step = 1000 if currency == "UZS" else 1
    return float(math.ceil(value / step - 1e-6) * step) if value > 0 else 0.0


def _percent_text(share: float) -> str:
    return f"{share * 100:g}".replace(".", ",")


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
    """«🐷 С поступления … — N %: …» после поступления (спека процента 7.1):
    сколько отложить с него, что отложить сейчас по копилкам и часть подушки
    именно этого поступления; копилкам ничего не нужно — «всё в подушку» без
    кнопок; доля меньше 10 000 сумов — сообщения нет; сбой — только в лог."""
    came = incomes(rows)
    if not came:
        return
    try:
        found = await _savings(context.bot_data)
        if found is None:
            return
        screen, rates = found.screen, found.rates
        amount = sum(
            rates.convert(row.amount, row.currency, "UZS") or 0.0 for row in came
        )
        if screen.percent * amount < TINY_SHARE:
            return
        percent = _percent_text(screen.percent)
        currencies = {row.currency for row in came}
        if len(came) == 1 or len(currencies) == 1:
            code = came[0].currency
            total = sum(row.amount for row in came)
            share = _up_money(total * screen.percent, code)
            head = (
                f"🐷 С поступления {amount_text(total)} {code} — {percent} %: "
                f"{amount_text(share)} {code}"
            )
            if code != "UZS":
                head += (
                    f" (≈ {amount_text(_up_money(amount * screen.percent, 'UZS'))} UZS)"
                )
        else:
            head = (
                f"🐷 С поступлений {amount_text(_up_money(amount, 'UZS'))} UZS — "
                f"{percent} %: {amount_text(_up_money(amount * screen.percent, 'UZS'))} UZS"
            )
        items = _left(screen)
        if not items:
            await context.bot.send_message(
                chat_id=chat_id, text=f"{head} — всё в подушку"
            )
            return
        today = local_today(config.ANALYTICS_TIMEZONE)
        text = f"{head}. За {MONTHS_NAMED[today.month - 1]} отложить: " + _sums(
            items, quoted=True
        )
        cushion = round(income_cushion(screen, amount), -3)
        if cushion > 0:
            text += f" · в подушку с этого поступления {amount_text(cushion)} UZS"
        buttons = [action("Отложил всё", f"{ALL}{today:%Y-%m}")]
        if part := _part_button(user_id):
            buttons.append(part)
        await context.bot.send_message(
            chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup([buttons])
        )
    except Exception:
        logger.exception("Piggy prompt failed for chat_id=%s", chat_id)


def _written_keys(screen: Screen, rows: Sequence[SheetRow]) -> set[str]:
    """Ключи трат копилок, которые дали записанные строки: та же строка раньше в
    тот же день (тот же ключ с «#2») — не наша, берём новейшие. Комментарий Sheets
    мог поменять при записи — тогда ищем по ключу без него."""
    order: list[str] = []  # ключи трат копилок по порядку строк fact
    for state in screen.piggies:
        for spend in state.balance.spends:
            if spend.key not in order:
                order.append(spend.key)
    wanted: dict[str, tuple[str, int]] = {}  # ключ строки → (без комментария, сколько)
    for row in rows:
        head = row_prefix(row.day, row.category, row.subcategory, row.amount,
                          row.currency, row.source)  # fmt: skip
        exact = fact_key(row.day, row.category, row.subcategory, row.amount,
                         row.currency, row.source, row.comment)  # fmt: skip
        count = wanted.get(exact, (head, 0))[1]
        wanted[exact] = (head, count + 1)
    chosen: set[str] = set()
    for exact, (head, count) in wanted.items():
        same = [key for key in order if key == exact or key.startswith(f"{exact}#")]
        if not same:  # комментарий в таблице другой — по ключу без комментария
            same = [key for key in order if key.startswith(f"{head}|")]
        chosen.update(same[-count:])
    return chosen


def _spend_lines(
    found: SavingsTab, rows: Sequence[SheetRow], button: bool = True
) -> tuple[list[str], InlineKeyboardButton | None]:
    """«🐷 Из копилки «Новый год»: −2 400 000 UZS, осталось 3 600 000» по копилке;
    одна строка записи — с кнопкой «Не из копилки»."""
    chosen = _written_keys(found.screen, rows)
    taken: list[tuple[PiggyState, float]] = []
    keys: list[str] = []
    for state in found.screen.piggies:
        if state.status == "broken":
            continue
        mine = [spend for spend in state.balance.spends if spend.key in chosen]
        if mine:
            taken.append((state, sum(spend.amount for spend in mine)))
            keys += [spend.key for spend in mine if spend.key not in keys]
    if not taken:
        return [], None
    over = False
    if len(rows) == 1:  # всё, что было: остаток траты ушёл в обычные
        row = rows[0]
        covered = sum(
            found.rates.convert(amount, state.piggy.currency, row.currency) or 0.0
            for state, amount in taken
        )
        over = covered < row.amount - 0.5
    lines = []
    for index, (state, amount) in enumerate(taken):
        code = state.piggy.currency
        name, sum_text = state.piggy.name, amount_text(rounded(amount, code))
        if over and index == len(taken) - 1:
            lines.append(
                f"🐷 Из копилки «{name}»: {sum_text} {code} — всё, что было, "
                "остальное — обычные траты"
            )
        else:
            left = amount_text(rounded(max(state.balance.left, 0.0), code))
            lines.append(f"🐷 Из копилки «{name}»: −{sum_text} {code}, осталось {left}")
    found_button = None
    if button and len(rows) == 1 and keys:
        found_button = action(
            "Не из копилки", f"{NO}{taken[0][0].piggy.id}:{fingerprint(keys[0])}"
        )
    return lines, found_button


async def spend_note(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    rows: Sequence[SheetRow],
    button: bool = True,
) -> list[str]:
    """«🐷 Из копилки…» после траты по статье копилки (спека 7.2): сообщение в чат
    и те же строки для ответа приложению; трата другой статьи таблицу не читает.
    button=False — выписка: по строке на копилку без «Не из копилки»."""
    spent = [row for row in rows if row.category != INCOME_GROUP and row.amount > 0]
    if not spent:
        return []
    try:
        articles = await piggy_articles(
            context.bot_data, context.bot_data["gs_service"]
        )
        touched = [row for row in spent if (row.category, row.subcategory) in articles]
        if not touched:
            return []
        found = await _savings(context.bot_data)
        if found is None:
            return []
        lines, found_button = _spend_lines(found, touched, button)
        if not lines:
            return []
        await context.bot.send_message(
            chat_id=chat_id,
            text="\n".join(lines),
            reply_markup=(
                InlineKeyboardMarkup([[found_button]]) if found_button else None
            ),
        )
        return lines
    except Exception:
        logger.exception("Piggy spending note failed for chat_id=%s", chat_id)
        return []


async def piggy_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки «🐷»: piggy:all:<месяц> — «Отложил всё», piggy:no:<ID>:<отпечаток> —
    «Не из копилки»."""
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if data.startswith(ALL):
        await _set_all_aside(update, context, data.removeprefix(ALL))
    elif data.startswith(NO):
        piggy_id, _, mark = data.removeprefix(NO).partition(":")
        await _not_from(update, context, piggy_id, mark)


async def _not_from(
    update: Update, context: ContextTypes.DEFAULT_TYPE, piggy_id: str, mark: str
) -> None:
    """«Не из копилки»: строка журнала с ключом траты; fact не меняется."""
    query, user = update.callback_query, update.effective_user
    today = local_today(config.ANALYTICS_TIMEZONE)
    author = user.first_name or ""
    gs = context.bot_data["gs_service"]
    spend, marked = None, False
    try:
        async with piggy_lock(context.bot_data):
            screen = await _screen(context.bot_data)
            state = next(
                (item for item in screen.piggies if item.piggy.id == piggy_id), None
            ) if screen is not None else None  # fmt: skip
            if state is not None:
                spend = next(
                    (item for item in state.balance.spends
                     if fingerprint(item.key) == mark),
                    None,
                )  # fmt: skip
            if state is not None and spend is not None:
                await asyncio.to_thread(
                    gs.add_piggy_moves,
                    [move_row(today, state.piggy, 0, NOT_FROM, author, spend.key)],
                )
            else:
                _, journal = await asyncio.to_thread(gs.get_piggy, 0.0)
                marked = any(
                    len(row) > 7
                    and str(row[5]).strip().lower() == NOT_FROM
                    and fingerprint(str(row[7])) == mark
                    for row in journal[1:]
                )
    except Exception:
        logger.exception("Not from the piggy failed for user_id=%s", user.id)
        message = getattr(query, "message", None)
        await query.edit_message_text(
            "Не записал — таблица не ответила. Нажмите ещё раз.",
            reply_markup=getattr(message, "reply_markup", None),
        )
        return
    if spend is None:
        text = (
            "Уже учтено" if marked else "Запись изменилась — отметьте в «Накоплениях»"
        )
        await query.edit_message_text(text, reply_markup=None)
        return
    mark_dirty(context.bot_data)
    await query.edit_message_text("🐷 Не из копилки — учтено", reply_markup=None)
    _, group = leading_emoji(spend.group)
    text = (
        f"{spend.day:%d.%m} {group} / {spend.subcategory} {amount_text(spend.amount)}"
    )
    await notify_others(context.bot, user.id, piggy_note(NOT_FROM, author, text))


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
                    [move_row(today, state.piggy, state.now, SAVED, author)
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
