import logging
import tempfile
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path

import PIL.Image
from pydantic import ValidationError
from telegram import File, Message, Update
from telegram.ext import ContextTypes

from app import config
from app.domain import (
    INCOME_GROUP,
    Catalog,
    ParsedTransaction,
    SheetRow,
    Skipped,
    build_row,
    local_today,
    manual_row,
)
from app.handlers.common import (
    clear_tracked_messages,
    show_main_menu,
    track_message,
)
from app.services.google_sheets import GoogleSheetsService
from app.utils.keyboards import (
    generate_categories_keyboard,
    generate_sources_keyboard,
)

logger = logging.getLogger(__name__)

BALANCE_FORMULA = (
    '=СУММЕСЛИМН($D$2:D{r}; $H$2:H{r}; $H{r}; $G$2:G{r}; $G{r}; $B$2:B{r}; "💰 ДОХОДЫ")'
    " - "
    'СУММЕСЛИМН($D$2:D{r}; $H$2:H{r}; $H{r}; $G$2:G{r}; $G{r}; $B$2:B{r}; "<>💰 ДОХОДЫ")'
)


@asynccontextmanager
async def _downloaded(file: File, suffix: str) -> AsyncIterator[Path]:
    """Скачать файл Telegram во временную папку, которая удалится после блока.

    Имя файла от пользователя в путь не попадает — только расширение.
    """
    with tempfile.TemporaryDirectory(prefix="tg-budget-") as folder:
        path = Path(folder) / f"upload{suffix}"
        await file.download_to_drive(path)
        yield path


def _load_image(path: Path) -> PIL.Image.Image:
    """Прочитать картинку в память, чтобы файл можно было сразу удалить."""
    image = PIL.Image.open(path)
    image.load()
    return image


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
            # Silent update - just show keyboards
            msg1 = await update.effective_chat.send_message(
                "ㅤ",  # Invisible character for minimal text
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
                    "ㅤ", reply_markup=generate_sources_keyboard(sources)
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

    # Prepare Image if photo
    image_part = None
    if is_photo:
        photo_file = await update.message.photo[-1].get_file()
        async with _downloaded(photo_file, ".jpg") as path:
            image_part = _load_image(path)

    # Delete user's message (text/SMS/photo) to keep chat clean
    await _delete_quietly(update.message)

    analyzing_msg = await update.effective_chat.send_message("🔍")
    track_message(context, analyzing_msg)

    try:
        catalog = _catalog(context)
        result = await ai_service.parse_transaction(
            user_input=msg_text or "Image Input",
            image_part=image_part,
            known_categories=catalog.categories,
            known_sources=catalog.sources,
            known_subcategories=catalog.subcategories,
        )
        items = result if isinstance(result, list) else [result]

        # Для скриншотов разворачиваем порядок: нижняя транзакция → первая запись
        # Для SMS оставляем как есть: верхняя транзакция → первая запись
        if is_photo and len(items) > 1:
            items = items[::-1]

        rows, skipped = _to_rows(items, context, current_source)
        await _save_rows(update, context, rows, skipped)

    except Exception as e:
        logger.exception("AI parsing failed")
        await update.effective_chat.send_message(f"Ошибка AI: {e}")
    finally:
        if image_part is not None:
            image_part.close()


def _to_rows(
    items: Sequence[object],
    context: ContextTypes.DEFAULT_TYPE,
    default_source: str | None,
) -> tuple[list[SheetRow], list[Skipped]]:
    """Ответ AI → строки fact по правилам таблицы плюс то, что записать нельзя."""
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    catalog = _catalog(context)
    rates = gs_service.get_rates()
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


def _format_amount(row: SheetRow) -> str:
    """«48 000 UZS», «+1 000 000 UZS», «12.21 USD»."""
    incoming = row.category == INCOME_GROUP or row.amount < 0
    value = abs(row.amount)
    digits = f"{value:,.0f}" if row.currency == "UZS" else f"{value:,.2f}"
    return f"{'+' if incoming else ''}{digits.replace(',', ' ')} {row.currency}"


def _card_number(row: SheetRow) -> str | None:
    if row.card_identifier:
        return row.card_identifier
    return next((part for part in row.source.split() if part.isdigit()), None)


async def _save_rows(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    rows: Sequence[SheetRow],
    skipped: Sequence[Skipped],
) -> None:
    """Записать строки в fact, обновить остатки карт и показать одну сводку."""
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    lines: list[str] = []
    failed = 0
    for row in rows:
        next_row = gs_service.get_last_row_index() + 1
        values = [
            row.date_text,
            row.category,
            row.subcategory,
            row.amount,
            BALANCE_FORMULA.format(r=next_row),
            row.comment,
            row.currency,
            row.source,
        ]
        if not gs_service.add_transaction(values):
            failed += 1
            continue
        context.user_data["source"] = row.source
        line = f"✅ {_format_amount(row)} • {row.category} ({row.subcategory}) • {row.source}"
        card = _card_number(row)
        cell = config.CARD_BALANCE_CELLS.get(card) if card else None
        if row.balance is not None and cell:
            if gs_service.update_cell(config.FACT_SHEET_NAME, cell, row.balance):
                line += f" | 💳 {row.balance:,.0f}".replace(",", " ")
                logger.info("Updated balance for card %s", card)
        lines.append(line)

    lines += [f"⚠️ Не записал: {s.comment} — {s.reason}" for s in skipped]
    if failed:
        lines.append(f"❌ Ошибка записи в Google Таблицу: {failed} шт.")
    if not lines:
        lines.append("Не удалось распознать транзакции.")

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
    if not ai_service or not config.GEMINI_API_KEY:
        await update.message.reply_text("AI сервис не настроен.")
        return

    await _delete_quietly(update.message)

    analyzing_msg = await update.effective_chat.send_message("📄")
    track_message(context, analyzing_msg)

    try:
        file = await document.get_file()
        async with _downloaded(file, Path(file_name).suffix) as path:
            extracted_text = _extract_text(path, is_pdf=is_pdf, is_excel=is_excel)

        if not extracted_text.strip():
            await update.effective_chat.send_message(
                "❌ Не удалось извлечь данные из файла."
            )
            return

        catalog = _catalog(context)
        result = await ai_service.parse_transaction(
            user_input=extracted_text,
            known_categories=catalog.categories,
            known_sources=catalog.sources,
            known_subcategories=catalog.subcategories,
        )
        items = result if isinstance(result, list) else [result]
        rows, skipped = _to_rows(items, context, context.user_data.get("source"))
        await _save_rows(update, context, rows, skipped)

    except Exception as e:
        logger.exception("Document processing failed")
        await update.effective_chat.send_message(f"❌ Ошибка обработки файла: {e}")


def _extract_text(path: Path, *, is_pdf: bool, is_excel: bool) -> str:
    """Текст документа для AI: PDF — первые 10 страниц, таблицы — 500 строк."""
    if is_pdf:
        from pypdf import PdfReader  # PyPDF2 заброшен, CVE-2023-36464

        reader = PdfReader(path)
        return "".join(page.extract_text() or "" for page in reader.pages[:10])

    import pandas as pd

    frame = pd.read_excel(path) if is_excel else pd.read_csv(path)
    return frame.head(500).to_string()
