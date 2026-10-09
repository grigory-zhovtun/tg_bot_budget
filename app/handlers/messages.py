import asyncio
import hashlib
import logging
import re
import tempfile
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from telegram import File, Message, Update
from telegram.ext import ContextTypes

from app import config
from app.domain import (
    Catalog,
    ParsedTransaction,
    SheetRow,
    Skipped,
    build_row,
    format_amount,
    local_today,
    manual_row,
)
from app.errors import user_message
from app.handlers import balances, statement_import
from app.handlers.common import (
    CHOOSE_SOURCE,
    LAST_WRITE,
    SEEN_INPUTS,
    clear_tracked_messages,
    show_main_menu,
    track_message,
)
from app.services.google_sheets import GoogleSheetsService
from app.statements import is_kapitalbank_statement, parse_statement, pdf_text
from app.utils.keyboards import (
    generate_categories_keyboard,
    generate_sources_keyboard,
)

logger = logging.getLogger(__name__)

SEEN_LIMIT = 200  # сколько последних SMS/файлов помнить
SEEN_TTL = 7 * 24 * 3600
MIN_SMS_LENGTH = 20  # короче — это ручной ввод вроде «48000 латте», повтор законен


@asynccontextmanager
async def _downloaded(file: File, suffix: str) -> AsyncIterator[Path]:
    """Скачать файл Telegram во временную папку, которая удалится после блока.

    Имя файла от пользователя в путь не попадает — только расширение.
    """
    with tempfile.TemporaryDirectory(prefix="tg-budget-") as folder:
        path = Path(folder) / f"upload{suffix}"
        await file.download_to_drive(path)
        yield path


async def _delete_quietly(message: Message | None) -> None:
    """Убрать сообщение пользователя из чата; если не вышло — не страшно."""
    if message is None:
        return
    try:
        await message.delete()
    except Exception as error:  # нет прав, сообщение уже удалено и т. п.
        logger.debug("Could not delete message: %s", error)


def _catalog(context: ContextTypes.DEFAULT_TYPE) -> Catalog:
    return Catalog(
        categories=context.bot_data.get("categories", []),
        subcategories=context.bot_data.get("subcategories", {}),
        sources=context.bot_data.get("sources", []),
    )


def input_fingerprint(
    text: str | None, file_unique_id: str | None = None
) -> str | None:
    """Отпечаток SMS или файла: тот же текст или тот же файл Telegram — тот же ключ."""
    if file_unique_id:
        return f"file:{file_unique_id}"
    normalized = re.sub(r"\s+", " ", text or "").strip().lower()
    if len(normalized) < MIN_SMS_LENGTH:
        return None
    return "text:" + hashlib.sha256(normalized.encode()).hexdigest()[:16]


def _seen(context: ContextTypes.DEFAULT_TYPE, fingerprint: str | None) -> dict | None:
    """Запись о том, что этот вход уже записан в таблицу (не старше недели)."""
    if fingerprint is None:
        return None
    entry = context.user_data.get(SEEN_INPUTS, {}).get(fingerprint)
    if entry and time.time() - entry["at"] < SEEN_TTL:
        return entry
    return None


def _remember(
    context: ContextTypes.DEFAULT_TYPE, fingerprint: str, first: int, last: int
) -> None:
    seen: dict[str, dict] = context.user_data.setdefault(SEEN_INPUTS, {})
    seen[fingerprint] = {"at": time.time(), "rows": (first, last)}
    while len(seen) > SEEN_LIMIT:
        seen.pop(next(iter(seen)))


async def _report_duplicate(
    update: Update, context: ContextTypes.DEFAULT_TYPE, entry: dict
) -> None:
    await _delete_quietly(update.message)
    first, last = entry["rows"]
    rows = f"строка {first}" if first == last else f"строки {first}–{last}"
    tz = ZoneInfo(config.ANALYTICS_TIMEZONE)
    when = datetime.fromtimestamp(entry["at"], tz).strftime("%d.%m %H:%M")
    message = await update.effective_chat.send_message(
        f"⚠️ Это уже записано ({rows}, {when}), повтор не записываю.\n"
        "Если запись была ошибочной — /undo."
    )
    track_message(context, message)


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text messages and photos (Source selection, Manual Entry, or AI Parsing)."""

    # Check if this is a Photo message
    is_photo = bool(update.message.photo)
    msg_text = update.message.caption if is_photo else update.message.text

    # If photo but no caption, treat text as empty string (still proceed to AI if photo exists)
    if is_photo and not msg_text:
        msg_text = ""

    if not msg_text and not is_photo:
        return  # Ignore empty updates

    sources = context.bot_data.get("sources", [])

    # 1. Check if text is a Source Selection (Only if text exists)
    if msg_text:
        clean_text = (
            msg_text.replace("✅ ", "") if msg_text.startswith("✅ ") else msg_text
        )
        if clean_text in sources:
            context.user_data["source"] = clean_text
            await _delete_quietly(update.message)
            # Клавиатура карт держится на сообщении, а пустой или «невидимый»
            # текст Telegram отвергает (Message_empty) — показываем выбранную карту
            msg1 = await update.effective_chat.send_message(
                f"💳 {clean_text}",
                reply_markup=generate_sources_keyboard(sources, clean_text),
            )
            track_message(context, msg1)
            msg2 = await update.effective_chat.send_message(
                "Категория:",
                reply_markup=generate_categories_keyboard(
                    context.bot_data.get("categories", [])
                ),
            )
            track_message(context, msg2)
            return

        # 2. Check for "Back"
        if msg_text == "⬅️ Назад":
            context.user_data.pop("category", None)
            context.user_data.pop("subcategory", None)
            await _delete_quietly(update.message)
            current_source = context.user_data.get("source")
            if current_source:
                msg = await update.effective_chat.send_message(
                    "Категория:",
                    reply_markup=generate_categories_keyboard(
                        context.bot_data.get("categories", [])
                    ),
                )
                track_message(context, msg)
            else:
                msg = await update.effective_chat.send_message(
                    CHOOSE_SOURCE, reply_markup=generate_sources_keyboard(sources)
                )
                track_message(context, msg)
            return

    # 3. Manual entry: source + category + subcategory are chosen, text is "amount comment"
    current_source = context.user_data.get("source")
    category = context.user_data.get("category")
    subcategory = context.user_data.get("subcategory")

    if current_source and category and subcategory and msg_text and not is_photo:
        row = manual_row(
            msg_text,
            current_source,
            category,
            subcategory,
            local_today(config.ANALYTICS_TIMEZONE),
        )
        if row is not None:
            await _delete_quietly(update.message)
            await _save_rows(update, context, [row], [])
            return

    # 4. AI Parsing (Fallthrough)
    ai_service = context.bot_data.get("ai_service")
    if not ai_service or not config.GEMINI_API_KEY:
        await update.message.reply_text(
            "AI сервис не настроен. Пожалуйста, используйте кнопки для ручного ввода."
        )
        return

    fingerprint = input_fingerprint(
        msg_text, update.message.photo[-1].file_unique_id if is_photo else None
    )
    if entry := _seen(context, fingerprint):
        await _report_duplicate(update, context, entry)
        return

    # Photo: Telegram always sends JPEG; read it into memory and drop the file
    photo = None
    if is_photo:
        photo_file = await update.message.photo[-1].get_file()
        async with _downloaded(photo_file, ".jpg") as path:
            photo = path.read_bytes()

    # Delete user's message (text/SMS/photo) to keep chat clean
    await _delete_quietly(update.message)

    analyzing_msg = await update.effective_chat.send_message("🔍")
    track_message(context, analyzing_msg)

    try:
        catalog = _catalog(context)
        known = {
            "known_categories": catalog.categories,
            "known_sources": catalog.sources,
            "known_subcategories": catalog.subcategories,
        }
        if photo is not None:
            screen = await ai_service.parse_screenshot(
                photo, caption=msg_text or None, **known
            )
            if screen["kind"] == "balances" and screen["balances"]:
                # главный экран банка: остатки карт, операции на нём не записываем
                await _delete_quietly(analyzing_msg)
                await balances.report_screen_balances(
                    update, context, screen["balances"]
                )
                return
            # Скриншот операций: нижняя транзакция → первая запись
            items = screen["transactions"][::-1]
        else:
            # SMS: верхняя транзакция → первая запись
            result = await ai_service.parse_transaction(user_input=msg_text, **known)
            items = result if isinstance(result, list) else [result]

        rows, skipped = await _to_rows(items, context, current_source)
        await _save_rows(update, context, rows, skipped, fingerprint)

    except Exception as e:
        logger.exception("AI parsing failed")
        await update.effective_chat.send_message(f"Ошибка AI: {user_message(e)}")


async def _to_rows(
    items: Sequence[object],
    context: ContextTypes.DEFAULT_TYPE,
    default_source: str | None,
) -> tuple[list[SheetRow], list[Skipped]]:
    """Ответ AI → строки fact по правилам таблицы плюс то, что записать нельзя."""
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    catalog = _catalog(context)
    rates = await asyncio.to_thread(gs_service.get_rates)
    now = local_today(config.ANALYTICS_TIMEZONE)

    rows: list[SheetRow] = []
    skipped: list[Skipped] = []
    for item in items:
        try:
            parsed = ParsedTransaction.model_validate(item)
        except ValidationError:
            comment = item.get("comment") if isinstance(item, dict) else None
            skipped.append(Skipped("не нашёл сумму", str(comment or item)[:60]))
            continue
        result = build_row(parsed, catalog, rates, now, default_source)
        (skipped if isinstance(result, Skipped) else rows).append(result)
    return rows, skipped


def _balance_check(source: str, bank: float, table: float) -> str:
    """Сверка остатка из SMS с остатком по таблице (колонка J блока остатков)."""
    currency = source.strip()[-3:].upper()
    tolerance = 0.01 if currency == "USD" else 1
    if abs(bank - table) < tolerance:
        return f"🟰 {source}: остаток сходится с банком"
    digits = "{:,.2f}" if currency == "USD" else "{:,.0f}"

    def money(value: float) -> str:
        return digits.format(value).replace(",", " ")

    return (
        f"⚠️ {source}: в банке {money(bank)}, в таблице {money(table)} "
        f"(разница {money(bank - table)})"
    )


async def _save_rows(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    rows: Sequence[SheetRow],
    skipped: Sequence[Skipped],
    fingerprint: str | None = None,
) -> None:
    """Записать строки в fact одним запросом, обновить остатки и показать сводку."""
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    lines: list[str] = []
    if rows:
        try:
            first, last = await asyncio.to_thread(gs_service.append_transactions, rows)
        except Exception:
            logger.exception("Writing %d rows failed", len(rows))
            lines.append(
                f"❌ Не записал в Google Таблицу ({len(rows)} шт.), попробуйте ещё раз"
            )
            rows = []
        else:
            context.user_data[LAST_WRITE] = {
                "first": first,
                "last": last,
                "rows": list(rows),
                "fingerprint": fingerprint,
            }
            if fingerprint:
                _remember(context, fingerprint, first, last)

    updated: list[str] = []
    balances = {row.source: row.balance for row in rows if row.balance is not None}
    if balances:
        try:
            checked_at = datetime.now(ZoneInfo(config.ANALYTICS_TIMEZONE))
            updated = await asyncio.to_thread(
                gs_service.update_balances, balances, checked_at
            )
        except Exception:
            logger.exception("Updating card balances failed")

    for row in rows:
        line = f"✅ {format_amount(row)} • {row.category} ({row.subcategory}) • {row.source}"
        if row.balance is not None and row.source in updated:
            line += f" | 💳 {row.balance:,.0f}".replace(",", " ")
        lines.append(line)
    if rows:
        context.user_data["source"] = rows[-1].source

    if updated:
        try:
            table = await asyncio.to_thread(gs_service.get_table_balances)
        except Exception:
            logger.exception("Reading table balances failed")
            table = {}
        lines += [
            _balance_check(source, balances[source], table[source])
            for source in updated
            if source in table
        ]

    lines += [f"⚠️ Не записал: {s.comment} — {s.reason}" for s in skipped]
    if not lines:
        lines.append("Не удалось распознать транзакции.")
    if rows:
        lines.append("✏️ Не та категория? /fix  ↩️ Удалить запись: /undo")

    # Clear specific manual selection state
    context.user_data.pop("category", None)
    context.user_data.pop("subcategory", None)
    await clear_tracked_messages(context, update.effective_chat.id)
    await show_main_menu(update, context, "\n".join(lines))


async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles document uploads (PDF, Excel, CSV)."""
    document = update.message.document
    if not document:
        return

    file_name = (document.file_name or "").lower()
    mime_type = document.mime_type or ""

    # Determine file type
    is_pdf = file_name.endswith(".pdf") or "pdf" in mime_type
    is_excel = (
        file_name.endswith((".xlsx", ".xls"))
        or "spreadsheet" in mime_type
        or "excel" in mime_type
    )
    is_csv = file_name.endswith(".csv") or "csv" in mime_type

    if not (is_pdf or is_excel or is_csv):
        await update.message.reply_text(
            "❌ Поддерживаются только PDF, Excel (.xlsx/.xls) и CSV файлы."
        )
        return

    ai_service = context.bot_data.get("ai_service")
    await _delete_quietly(update.message)

    analyzing_msg = await update.effective_chat.send_message("📄")
    track_message(context, analyzing_msg)

    try:
        file = await document.get_file()
        attachment = None
        async with _downloaded(file, Path(file_name).suffix) as path:
            if is_pdf:
                # Gemini читает PDF сам: сканы, таблицы, все страницы
                attachment = path.read_bytes()
                extracted_text = "Выписка или документ с операциями во вложении (PDF)"
            else:
                extracted_text = _table_text(path, is_excel=is_excel)

        if attachment is not None:
            # Выписку Капиталбанка бот разбирает сам, без AI и без лимитов модели
            text = await asyncio.to_thread(pdf_text, attachment)
            if is_kapitalbank_statement(text):
                statement = parse_statement(text, _catalog(context).sources)
                await statement_import.receive_statement(
                    update, context, statement, analyzing_msg
                )
                return

        if not ai_service or not config.GEMINI_API_KEY:
            await update.effective_chat.send_message("AI сервис не настроен.")
            return
        fingerprint = input_fingerprint(None, document.file_unique_id)
        if entry := _seen(context, fingerprint):
            await _report_duplicate(update, context, entry)
            return

        if not extracted_text.strip():
            await update.effective_chat.send_message(
                "❌ Не удалось извлечь данные из файла."
            )
            return

        catalog = _catalog(context)
        result = await ai_service.parse_transaction(
            user_input=extracted_text,
            attachment=attachment,
            mime_type="application/pdf" if attachment else None,
            known_categories=catalog.categories,
            known_sources=catalog.sources,
            known_subcategories=catalog.subcategories,
        )
        items = result if isinstance(result, list) else [result]
        rows, skipped = await _to_rows(items, context, context.user_data.get("source"))
        await _save_rows(update, context, rows, skipped, fingerprint)

    except Exception as e:
        logger.exception("Document processing failed")
        await update.effective_chat.send_message(
            f"❌ Ошибка обработки файла: {user_message(e)}"
        )


def _table_text(path: Path, *, is_excel: bool) -> str:
    """Таблица (Excel/CSV) текстом для AI, до 500 строк."""
    import pandas as pd

    frame = pd.read_excel(path) if is_excel else pd.read_csv(path)
    return frame.head(500).to_string()
