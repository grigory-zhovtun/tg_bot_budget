"""Остатки карт со скриншотов банковских приложений: «Проверка», сверка, выравнивание.

Скрин главного экрана (список карт с остатками) пишет остаток банка в колонку
«Проверка» блока остатков и время в «Сверено», а в ответ показывает, сходится
ли таблица с банком. Для расхождений — кнопки «Выровнять»: строка выравнивания
в «РАЗНОЕ / неучтенка» добавляется только по нажатию, разница считается заново.
"""

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from app import config
from app.domain import (
    FALLBACK_CATEGORY,
    FALLBACK_SUBCATEGORY,
    SheetRow,
    currency_of,
    local_today,
    parse_number,
)
from app.errors import user_message
from app.handlers.common import LAST_WRITE
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

PENDING = "pending_alignment"  # user_data: источник → (остаток банка, время сверки)


@dataclass(frozen=True)
class BankBalance:
    source: str
    balance: float


@dataclass
class Matched:
    balances: list[BankBalance]
    unknown_cards: list[str]  # 4 цифры карт, которых нет в system
    wrong_currency: list[str]  # «VISA 9120 UZS: на скрине USD»


def tolerance(source: str) -> float:
    """Разница, которую считаем совпадением: цент у долларов, сум/рубль у остальных."""
    return 0.01 if currency_of(source) == "USD" else 1.0


def same(diff: float, source: str) -> bool:
    return abs(diff) <= tolerance(source) + 1e-9


def money(value: float, source: str) -> str:
    pattern = "{:,.2f}" if currency_of(source) == "USD" else "{:,.0f}"
    return pattern.format(value).replace(",", " ")


def match_cards(
    items: list[dict], sources: list[str], ignored: frozenset[str]
) -> Matched:
    """Карты со скрина → источники из system по последним 4 цифрам."""
    result = Matched([], [], [])
    seen: set[str] = set()
    for item in items:
        digits = re.findall(r"\d{4}", str(item.get("card") or ""))
        balance = parse_number(item.get("balance"))
        if not digits or balance is None:
            continue  # скрытый остаток или не карта
        card = digits[-1]
        if card in ignored or card in seen:
            continue
        seen.add(card)
        matches = [s for s in sources if card in s]
        if len(matches) != 1:
            result.unknown_cards.append(card)
            continue
        source = matches[0]
        currency = str(item.get("currency") or "").strip().upper()
        if currency and currency != currency_of(source):
            result.wrong_currency.append(f"{source}: на скрине {currency}")
            continue
        result.balances.append(BankBalance(source, balance))
    return result


def _line(bank: BankBalance, table: float | None) -> tuple[str, float | None]:
    """Строка ответа и разница «таблица − банк» (None — сходится или не с чем сравнить)."""
    if table is None:
        return f"❔ {bank.source}: банк {money(bank.balance, bank.source)}", None
    diff = round(table - bank.balance, 2)
    if same(diff, bank.source):
        return f"✅ {bank.source}: {money(bank.balance, bank.source)} — сходится", None
    where = "больше" if diff > 0 else "меньше"
    return (
        f"⚠️ {bank.source}: банк {money(bank.balance, bank.source)}, "
        f"в таблице {money(table, bank.source)} — в таблице {where} "
        f"на {money(abs(diff), bank.source)}",
        diff,
    )


async def report_screen_balances(
    update: Update, context: ContextTypes.DEFAULT_TYPE, items: list[dict]
) -> None:
    """Остатки со скрина → «Проверка» и «Сверено», ответ со сверкой и кнопками."""
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    matched = match_cards(
        items, context.bot_data.get("sources", []), config.IGNORED_CARDS
    )
    now = datetime.now(ZoneInfo(config.ANALYTICS_TIMEZONE))
    stamp = now.strftime("%d.%m %H:%M")
    lines = [f"💳 Остатки банка на {stamp}"]
    buttons: list[list[InlineKeyboardButton]] = []
    pending: dict[str, tuple[float, str]] = {}

    if matched.balances:
        try:
            await asyncio.to_thread(
                gs_service.update_balances,
                {b.source: b.balance for b in matched.balances},
                now,
            )
            table = await asyncio.to_thread(gs_service.get_table_balances)
        except Exception as error:
            logger.exception("Writing screen balances failed")
            await update.effective_chat.send_message(
                f"❌ Не записал остатки: {user_message(error)}"
            )
            return
        for bank in matched.balances:
            line, diff = _line(bank, table.get(bank.source))
            lines.append(line)
            if diff is not None:
                pending[bank.source] = (bank.balance, stamp)
                label = f"Выровнять {bank.source}: {'−' if diff > 0 else '+'}{money(abs(diff), bank.source)}"
                buttons.append(
                    [InlineKeyboardButton(label, callback_data=f"align:{bank.source}")]
                )
    else:
        lines.append("Не нашёл на скрине остатков карт из таблицы.")

    lines += [
        f"ℹ️ Карты {card} нет в таблице (лист system, колонка F)"
        for card in matched.unknown_cards
    ]
    lines += [f"⚠️ Валюта не совпала — {text}" for text in matched.wrong_currency]
    if buttons:
        lines.append(
            "\nВыровнять — добавить строку в «🚧 РАЗНОЕ / неучтенка» на разницу: "
            "так делают, когда незаписанные траты уже не найти."
        )
    context.user_data[PENDING] = pending
    # сверка остаётся в чате: очистка служебных сообщений её не трогает
    await update.effective_chat.send_message(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
    )
    logger.info(
        "Screen balances: %d card(s), %d mismatch(es)",
        len(matched.balances),
        len(pending),
    )


async def align_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопка «Выровнять <карта>»: строка выравнивания на текущую разницу."""
    query = update.callback_query
    source = (query.data or "").removeprefix("align:")
    pending: dict[str, tuple[float, str]] = context.user_data.get(PENDING, {})
    if source not in pending:
        await query.answer("Сверка устарела — пришлите скрин ещё раз")
        return
    await query.answer()
    bank, stamp = pending.pop(source)
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    try:
        # таблица могла измениться после скрина (пришли SMS) — разница на сейчас
        table = (await asyncio.to_thread(gs_service.get_table_balances)).get(source)
        if table is None:
            raise ValueError(f"нет остатка «{source}» в блоке остатков")
        diff = round(table - bank, 2)
        if same(diff, source):
            text = f"✅ {source}: уже сходится с банком"
        else:
            row = SheetRow(
                day=local_today(config.ANALYTICS_TIMEZONE),
                category=FALLBACK_CATEGORY,
                subcategory=FALLBACK_SUBCATEGORY,
                amount=diff,
                comment=f"AI: выравнивание к остатку банка {money(bank, source)} ({stamp})",
                currency=currency_of(source),
                source=source,
            )
            first, last = await asyncio.to_thread(gs_service.append_transactions, [row])
            context.user_data[LAST_WRITE] = {
                "first": first,
                "last": last,
                "rows": [row],
                "fingerprint": None,
            }
            text = (
                f"✅ {source}: добавил выравнивание {money(diff, source)} "
                f"(строка {first}), теперь сходится с банком. ↩️ /undo — отменить"
            )
    except Exception as error:
        logger.exception("Alignment failed")
        pending[source] = (bank, stamp)  # можно нажать ещё раз
        await query.message.reply_text(f"❌ Не выровнял: {user_message(error)}")
        return

    # убрать нажатую кнопку, остальные оставить
    keyboard = (
        query.message.reply_markup.inline_keyboard if query.message.reply_markup else []
    )
    rest = [row for row in keyboard if row and row[0].callback_data != query.data]
    await query.edit_message_reply_markup(InlineKeyboardMarkup(rest) if rest else None)
    await query.message.reply_text(text)
