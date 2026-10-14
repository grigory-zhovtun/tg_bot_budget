"""SMS знакомых видов в чате → строки fact без ИИ (спека разбора SMS правилами).

prepare разбирает текст, отбрасывает уже записанные SMS, берёт категории из
памяти по сумме и истории магазинов, у новых магазинов спрашивает ИИ одним
запросом. None — текст не узнан: его, как раньше, разбирает ИИ.
"""

import asyncio
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any

from telegram.constants import ReactionEmoji
from telegram.ext import ContextTypes

from app import config
from app.domain import (
    Catalog,
    MerchantBook,
    ParsedTransaction,
    SheetRow,
    Skipped,
    build_row,
    local_today,
)
from app.handlers import live
from app.handlers.common import input_fingerprint, seen_input
from app.sms import Sms, SmsBatch, parse_sms
from app.sms_rows import CHECK, NEW, AmountMemory, new_merchants, sms_rows

logger = logging.getLogger(__name__)

SUGGEST_SECONDS = 30  # подсказка ИИ для новых магазинов; дольше — пишем без неё
SHOWN_ROWS = 5  # сколько номеров строк показать в «Уже записано раньше»


@dataclass(frozen=True)
class PreparedSms:
    """Сообщение из SMS, готовое к save_rows."""

    rows: list[SheetRow]
    flags: list[str | None]  # 🆕, ❓ или None у каждой строки
    prints: list[str | None]  # отпечаток SMS каждой строки; у комиссии — None
    skipped: list[Skipped]  # карта не найдена, нет курса
    notes: list[str]  # «⏭ Пропустил…», «⏭ Уже записано раньше…»

    @property
    def quiet(self) -> bool:
        """В сообщении были только коды и повторы — это не ошибка."""
        return not self.rows and not self.skipped


async def prepare(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, message: Any, text: str
) -> PreparedSms | None:
    """Текст SMS → строки для save_rows; None — правила текст не узнали."""
    try:
        batch = parse_sms(text)
    except Exception:
        logger.exception("SMS rules failed")
        return None
    if batch is None:
        logger.info("SMS rules: not recognized, AI parses")
        return None
    await live.react(message, ReactionEmoji.EYES)
    try:
        return await _prepare(context, chat_id, message, batch)
    except Exception:
        logger.exception("SMS rules failed")
        return None


def _fresh(
    context: ContextTypes.DEFAULT_TYPE, operations: list[Sms]
) -> tuple[list[Sms], dict[Sms, str | None], list[int]]:
    """Новые SMS и их отпечатки; повтор внутри сообщения и записанные раньше — мимо."""
    fresh: list[Sms] = []
    prints: dict[Sms, str | None] = {}
    repeated: list[int] = []  # строки fact, где эти SMS уже записаны
    here: set[str | None] = set()
    for sms in operations:
        fingerprint = input_fingerprint(sms.text)
        if fingerprint in here:
            continue
        here.add(fingerprint)
        if entry := seen_input(context, fingerprint):
            repeated.append(entry["rows"][0])
            continue
        fresh.append(sms)
        prints[sms] = fingerprint
    return fresh, prints, repeated


def _notes(skipped: list[str], repeated: list[int]) -> list[str]:
    """«⏭ Пропустил: код подтверждения ×2» и «⏭ Уже записано раньше: …»."""
    notes: list[str] = []
    if skipped:
        parts = [
            f"{reason} ×{count}" if count > 1 else reason
            for reason, count in Counter(skipped).items()
        ]
        notes.append("⏭ Пропустил: " + ", ".join(parts))
    if repeated:
        numbers = sorted(set(repeated))
        shown = ", ".join(map(str, numbers[:SHOWN_ROWS]))
        if len(numbers) > SHOWN_ROWS:
            shown += "…"
        where = f"строка {shown}" if len(numbers) == 1 else f"строки {shown}"
        notes.append(f"⏭ Уже записано раньше: {len(repeated)} SMS ({where})")
    return notes


async def _suggestions(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message: Any,
    names: list[str],
    catalog: Catalog,
) -> dict[str, tuple[str, str]]:
    """Категории новых магазинов одним запросом; ошибка или 30 секунд — без них."""
    ai = context.bot_data.get("ai_service")
    if not names or ai is None or not getattr(ai, "enabled", False):
        return {}
    try:
        async with live.thinking(context, chat_id, message.message_id):
            return await asyncio.wait_for(
                ai.categorize_merchants(names, catalog), SUGGEST_SECONDS
            )
    except Exception:
        logger.warning("SMS rules: no merchant suggestions", exc_info=True)
        return {}


async def _prepare(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, message: Any, batch: SmsBatch
) -> PreparedSms:
    fresh, prints, repeated = _fresh(context, batch.operations)
    catalog = Catalog(
        categories=context.bot_data.get("categories", []),
        subcategories=context.bot_data.get("subcategories", {}),
        sources=context.bot_data.get("sources", []),
    )
    ai = context.bot_data.get("ai_service")
    book, fact = MerchantBook([]), []
    if ai is not None:
        book, fact = await asyncio.to_thread(ai.merchant_history)
    rates = await asyncio.to_thread(context.bot_data["gs_service"].get_rates)
    today = local_today(config.ANALYTICS_TIMEZONE)
    memory = AmountMemory.from_sheet(fact, today)
    names = new_merchants(fresh, catalog.sources, book, memory)
    suggestions = await _suggestions(context, chat_id, message, names, catalog)
    planned, skipped = sms_rows(
        fresh, catalog.sources, book, memory, suggestions, rates
    )
    rows: list[SheetRow] = []
    flags: list[str | None] = []
    row_prints: list[str | None] = []
    for position in planned:
        parsed = ParsedTransaction.model_validate(position.item)
        built = build_row(parsed, catalog, rates, today, None)
        if isinstance(built, Skipped):
            skipped.append(built)
            continue
        rows.append(built)
        flags.append(position.flag)
        row_prints.append(prints.get(position.sms) if position.sms else None)
    logger.info(
        "SMS rules: %d SMS → %d rows (%d new, %d to check, %d skipped, %d repeated)",
        len(batch.operations),
        len(rows),
        flags.count(NEW),
        flags.count(CHECK),
        len(batch.skipped) + len(skipped),
        len(repeated),
    )
    return PreparedSms(
        rows, flags, row_prints, skipped, _notes(batch.skipped, repeated)
    )
