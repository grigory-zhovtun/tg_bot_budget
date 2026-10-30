import asyncio
import logging
import tempfile
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from telegram import Chat, File, Message, Update
from telegram.constants import ReactionEmoji
from telegram.ext import ContextTypes

from app import config
from app.chat_state import state_of
from app.domain import (
    INCOME_GROUP,
    Catalog,
    ParsedTransaction,
    SheetRow,
    Skipped,
    build_row,
    format_amount,
    local_today,
    manual_row,
    row_name,
)
from app.errors import user_message
from app.handlers import balances, fix, icons, live, statement_import
from app.handlers import sms as sms_chat
from app.handlers.analytics import send_report
from app.handlers.common import (
    ANCHOR,
    CHOOSE_SOURCE,
    LAST_WRITE,
    SEEN_INPUTS,
    clear_tracked_messages,
    input_fingerprint,
    premium_icons,
    seen_input,
    show_main_menu,
    sources_keyboard,
    track_message,
)
from app.handlers.income_month import month_question
from app.handlers.piggy import income_prompt, spend_note
from app.handlers.receipts import mark_dirty
from app.rich import Report, cents, heading, money_text, para, table, whole
from app.services.analytics_service import TRANSFERS_GROUP, Feedback
from app.services.google_sheets import GoogleSheetsService
from app.services.savings import save_hint
from app.statements import is_kapitalbank_statement, parse_statement, pdf_text
from app.utils.keyboards import (
    category_prompt,
    generate_categories_keyboard,
    with_icon,
)

logger = logging.getLogger(__name__)

SEEN_LIMIT = 1000  # сколько последних SMS/файлов помнить: в одной вставке до 50 SMS


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
    text = (
        f"⚠️ Это уже записано ({rows}, {when}), повтор не записываю.\n"
        "Если запись была ошибочной — /undo."
    )
    html = heading("⚠️ Это уже записано") + para(
        f"{rows}, {when}",
        "повтор не записываю",
        "если запись была ошибочной — /undo",
    )
    chat = update.effective_chat
    sent = await send_report(
        context.bot, chat.id, Report(text, html), send=chat.send_message
    )
    for message in sent.messages:
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

    # после /icons — кастомные эмодзи из набора для картинок на кнопках
    if await icons.receive_pack(update, context):
        return

    sources = context.bot_data.get("sources", [])

    # 1. Check if text is a Source Selection (Only if text exists)
    if msg_text:
        clean_text = (
            msg_text.replace("✅ ", "") if msg_text.startswith("✅ ") else msg_text
        )
        if clean_text in sources:
            context.user_data["source"] = clean_text
            await _delete_quietly(update.message)
            chat = update.effective_chat
            keys = state_of(context.bot_data, chat.id)
            # Клавиатура карт держится на сообщении, а пустой или «невидимый»
            # текст Telegram отвергает (Message_empty) — показываем выбранную карту
            msg1 = await chat.send_message(
                f"💳 {clean_text}",
                reply_markup=(
                    sources_keyboard(context, chat.id, clean_text)
                    if keys.cards
                    else None
                ),
            )
            track_message(context, msg1)
            if keys.cards:
                context.user_data[ANCHOR] = msg1.message_id
            if keys.groups:
                msg2 = await chat.send_message(
                    category_prompt(clean_text),
                    reply_markup=generate_categories_keyboard(
                        context.bot_data.get("categories", []), premium_icons(context)
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
            keys = state_of(context.bot_data, update.effective_chat.id)
            if current_source and keys.groups:
                msg = await update.effective_chat.send_message(
                    category_prompt(current_source),
                    reply_markup=generate_categories_keyboard(
                        context.bot_data.get("categories", []), premium_icons(context)
                    ),
                )
                track_message(context, msg)
            elif not current_source and keys.cards:
                msg = await update.effective_chat.send_message(
                    CHOOSE_SOURCE,
                    reply_markup=sources_keyboard(context, update.effective_chat.id),
                )
                track_message(context, msg)
                context.user_data[ANCHOR] = msg.message_id
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
            await save_rows(context, update.effective_chat, [row], [])
            return

    # 4. Уже записанные SMS или скрин второй раз не пишем
    fingerprint = input_fingerprint(
        msg_text, update.message.photo[-1].file_unique_id if is_photo else None
    )
    if entry := seen_input(context, fingerprint):
        await _report_duplicate(update, context, entry)
        return

    # 5. SMS знакомых видов — правилами; ИИ только подсказывает новые магазины
    if not is_photo:
        prepared = await sms_chat.prepare(
            context, update.effective_chat.id, update.message, msg_text
        )
        if prepared is not None:
            await _save_sms(update, context, prepared, fingerprint)
            return

    # 6. AI Parsing (Fallthrough)
    ai_service = context.bot_data.get("ai_service")
    if not ai_service or not config.GEMINI_API_KEY:
        await update.message.reply_text(
            "AI сервис не настроен. Пожалуйста, используйте кнопки для ручного ввода."
        )
        return

    # Photo: Telegram always sends JPEG; read it into memory and drop the file
    photo = None
    if is_photo:
        photo_file = await update.message.photo[-1].get_file()
        async with _downloaded(photo_file, ".jpg") as path:
            photo = path.read_bytes()

    # Пока разбор — 👀 и «Думаю…»; записано всё — сообщение уходит из чата, иначе
    # остаётся с реакцией: 👍 — записано не всё, 🤔 — ничего
    message = update.message
    await live.react(message, ReactionEmoji.EYES)
    try:
        catalog = _catalog(context)
        known = {
            "known_categories": catalog.categories,
            "known_sources": catalog.sources,
            "known_subcategories": catalog.subcategories,
        }
        async with live.thinking(context, update.effective_chat.id, message.message_id):
            if photo is not None:
                screen = await ai_service.parse_screenshot(
                    photo, caption=msg_text or None, **known
                )
            else:
                result = await ai_service.parse_transaction(
                    user_input=msg_text, **known
                )

        if photo is not None and screen["kind"] == "balances" and screen["balances"]:
            # главный экран банка: остатки карт, операции на нём не записываем
            await balances.report_screen_balances(update, context, screen["balances"])
            await _delete_quietly(message)  # остатки сверены — скрин не нужен
            return
        if photo is not None:
            # Скриншот операций: нижняя транзакция → первая запись
            items = screen["transactions"][::-1]
        else:
            # SMS: верхняя транзакция → первая запись
            items = result if isinstance(result, list) else [result]

        rows, skipped = await _to_rows(items, context, current_source)
        result = await save_rows(
            context, update.effective_chat, rows, skipped, fingerprint
        )
        if result.written == len(rows) > 0 and not skipped:
            await _delete_quietly(message)  # что записано — видно в карточке
        else:
            await live.react(
                message,
                (
                    ReactionEmoji.THUMBS_UP
                    if result.written
                    else ReactionEmoji.THINKING_FACE
                ),
            )

    except Exception as e:
        logger.exception("AI parsing failed")
        await live.react(message, ReactionEmoji.THINKING_FACE)
        await update.effective_chat.send_message(f"Ошибка AI: {user_message(e)}")


async def _save_sms(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    prepared: sms_chat.PreparedSms,
    fingerprint: str | None,
) -> None:
    """Запись сообщения из SMS: сводка с пометками; SMS уходит из чата, если записано
    всё (или в нём были только коды и повторы), иначе остаётся с реакцией."""
    result = await save_rows(
        context,
        update.effective_chat,
        prepared.rows,
        prepared.skipped,
        fingerprint,
        flags=prepared.flags,
        prints=prepared.prints,
        notes=prepared.notes,
    )
    ai_service = context.bot_data.get("ai_service")
    if result.written and any(prepared.flags) and ai_service is not None:
        ai_service.forget_history()  # новые магазины и суммы — в историю сразу
    if result.written == len(prepared.rows) and not prepared.skipped:
        await _delete_quietly(update.message)  # что записано — видно в карточке
        return
    done = result.written or prepared.quiet
    await live.react(
        update.message,
        ReactionEmoji.THUMBS_UP if done else ReactionEmoji.THINKING_FACE,
    )


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


def _incoming(row: SheetRow) -> bool:
    """Поступление или возврат — со знаком «+», как в format_amount."""
    return row.category == INCOME_GROUP or row.amount < 0


def _signed_money(row: SheetRow) -> str:
    return ("+" if _incoming(row) else "") + money_text(abs(row.amount), row.currency)


def _card_money(value: float, source: str) -> str:
    """Остаток карты: долларовой — с центами, остальных — целиком (как сверка)."""
    return cents(value) if source.strip()[-3:].upper() == "USD" else whole(value)


def _check_lines(source: str, bank: float, table_value: float) -> list[str]:
    """Сверка остатка из SMS с таблицей — строками абзаца."""
    tolerance = 0.01 if source.strip()[-3:].upper() == "USD" else 1
    if abs(bank - table_value) < tolerance:
        return ["✓ сходится с банком"]
    return [  # короткими строками: одной не влезает в ширину телефона
        f"⚠️ в банке {_card_money(bank, source)}",
        f"в таблице {_card_money(table_value, source)}",
        f"разница {_card_money(bank - table_value, source)}",
    ]


def _plural(count: int, one: str, few: str, many: str) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return one
    if 2 <= count % 10 <= 4 and not 12 <= count % 100 <= 14:
        return few
    return many


def record_blocks(
    rows: Sequence[SheetRow],
    flags: Sequence[str | None],
    balances: dict[str, float],
    table_balances: dict[str, float],
    updated: Sequence[str],
    icons: dict[str, str] | None,
) -> list[str]:
    """Записанные строки крупно: одна — сумма заголовком, несколько — таблицей.

    Под ними — карта с остатком, если бот его обновил, сверка с банком и строки
    с пометкой проверки (🆕/❓).
    """

    def flag(index: int) -> str | None:
        return flags[index] if index < len(flags) else None

    def card_lines(source: str) -> list[str]:
        if source not in updated:
            return [source]
        lines = [f"{source}: {_card_money(balances[source], source)}"]
        if source in table_balances:
            lines += _check_lines(source, balances[source], table_balances[source])
        return lines

    marks = [
        f"{flag(index)} {row_name(row)}"
        for index, row in enumerate(rows)
        if flag(index)
    ]
    if len(rows) == 1:
        row = rows[0]
        return [
            heading(f"✅ {_signed_money(row)}", 2),
            para(
                f"{with_icon(row.subcategory, icons)} · {row.category}",
                *card_lines(row.source),
                *marks,
            ),
        ]
    count = len(rows)
    spends = len({row.currency for row in rows}) == 1 and all(
        not _incoming(row) and row.category != TRANSFERS_GROUP for row in rows
    )
    if spends:
        total = money_text(sum(row.amount for row in rows), rows[0].currency)
        head = f"✅ {count} {_plural(count, 'трата', 'траты', 'трат')} — {total}"
    else:
        head = f"✅ Записал {count} {_plural(count, 'строку', 'строки', 'строк')}"
    cells = [
        [
            _signed_money(row),
            with_icon(row.subcategory, icons)
            + (f" {flag(index)}" if flag(index) else ""),
        ]
        for index, row in enumerate(rows)
    ]
    sources = list(dict.fromkeys(row.source for row in rows))
    lines = [line for source in sources for line in card_lines(source)]
    return [heading(head, 2), table(cells, "rl"), para(*lines, *marks)]


async def _plan_feedback(
    context: ContextTypes.DEFAULT_TYPE, rows: Sequence[SheetRow]
) -> Feedback:
    """Статус статей месяца и остаток на сегодня; ошибка не мешает записи."""
    analytics = context.bot_data.get("analytics_service")
    today = local_today(config.ANALYTICS_TIMEZONE)
    spent = [
        row.subcategory
        for row in rows
        if row.category not in (INCOME_GROUP, TRANSFERS_GROUP)
        and (row.day.year, row.day.month) == (today.year, today.month)
    ]
    if analytics is None or not spent:
        return Feedback([], [])
    try:
        return await asyncio.to_thread(
            analytics.write_feedback,
            list(dict.fromkeys(spent)),
            context.bot_data.get("icons"),
        )
    except Exception:
        logger.exception("Plan feedback failed")
        return Feedback([], [])


@dataclass(frozen=True)
class SaveResult:
    """Итог записи: сколько строк записано, их номера в fact и строки сводки."""

    written: int
    first: int | None
    last: int | None
    lines: tuple[str, ...]


async def save_rows(
    context: ContextTypes.DEFAULT_TYPE,
    chat: Chat,
    rows: Sequence[SheetRow],
    skipped: Sequence[Skipped],
    fingerprint: str | None = None,
    *,
    flags: Sequence[str | None] = (),
    prints: Sequence[str | None] = (),
    notes: Sequence[str] = (),
    notify_failure: bool = True,
) -> SaveResult:
    """Записать строки в fact, обновить остатки, показать сводку в chat.

    Общая для чата и Mini App: под сводкой — «✏️ Исправить запись» и «↩️ Отменить
    запись», последняя запись и отпечаток входа — в user_data (для /undo и повторов).
    Для SMS: flags — пометка каждой строки (🆕, ❓ или None), prints — отпечаток SMS
    каждой строки (повтор этой SMS не запишется), notes — строки «⏭» в конце сводки.
    notify_failure=False — таблица не приняла строки: в чат ничего, ответ — пустой
    (Mini App повторит запись сам).
    """
    gs_service: GoogleSheetsService = context.bot_data["gs_service"]
    keys = state_of(context.bot_data, chat.id)
    lines: list[str] = []
    failed: list[str] = []  # «❌ Не записал…» — в расширенном виде абзацем
    first: int | None = None
    last: int | None = None
    if rows:
        try:
            first, last = await asyncio.to_thread(gs_service.append_transactions, rows)
        except Exception:
            logger.exception("Writing %d rows failed", len(rows))
            lines.append(
                f"❌ Не записал в Google Таблицу ({len(rows)} шт.), попробуйте ещё раз"
            )
            failed.append(lines[-1])
            if not notify_failure:
                return SaveResult(0, None, None, tuple(lines))
            rows = []
        else:
            context.user_data[LAST_WRITE] = {
                "first": first,
                "last": last,
                "rows": list(rows),
                "fingerprint": fingerprint,
                "prints": [p for p in prints if p],
                # строки с пометкой, которые ещё не проверены: номер → 🆕 или ❓
                "checks": {i: flag for i, flag in enumerate(flags) if flag},
                # кнопки записи — на сводке (клавиатуры выключены) или в меню под ней
                "on_card": not (keys.groups or keys.cards),
                "groups": keys.groups,
            }
            mark_dirty(context.bot_data)  # «Пришло к утру» сверит задача receipts
            if fingerprint:
                _remember(context, fingerprint, first, last)
            for offset, sms_print in enumerate(prints):
                if sms_print:
                    _remember(context, sms_print, first + offset, first + offset)

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

    for index, row in enumerate(rows):
        sub = with_icon(row.subcategory, context.bot_data.get("icons"))
        line = f"✅ {format_amount(row)} • {row.category} ({sub}) • {row.source}"
        if index < len(flags) and flags[index]:
            line += f" • {flags[index]} {row_name(row)}"
        if row.balance is not None and row.source in updated:
            line += f" | 💳 {row.balance:,.0f}".replace(",", " ")
        lines.append(line)
    if rows:
        context.user_data["source"] = rows[-1].source
        context.bot_data["last_source"] = rows[-1].source  # переживёт сброс user_data

    table_balances: dict[str, float] = {}
    if updated:
        try:
            table_balances = await asyncio.to_thread(gs_service.get_table_balances)
        except Exception:
            logger.exception("Reading table balances failed")
        lines += [
            _balance_check(source, balances[source], table_balances[source])
            for source in updated
            if source in table_balances
        ]

    feedback = await _plan_feedback(context, rows)
    lines += feedback.lines
    after = await asyncio.to_thread(save_hint, gs_service, rows)  # «отложите»
    after += [f"⚠️ Не записал: {s.comment} — {s.reason}" for s in skipped]
    after += notes
    lines += after
    if not lines:
        lines.append("Не удалось распознать транзакции.")
        after.append(lines[0])
    blocks = (
        record_blocks(
            rows,
            flags,
            balances,
            table_balances,
            updated,
            context.bot_data.get("icons"),
        )
        if rows
        else []
    )
    blocks += feedback.blocks
    if failed or after:
        blocks.append(para(*failed, *after))
    report = Report("\n".join(lines), "".join(blocks))

    # Clear specific manual selection state
    context.user_data.pop("category", None)
    context.user_data.pop("subcategory", None)
    await clear_tracked_messages(context, chat.id)
    # после записи под сводкой — «✏️ Исправить запись» и «↩️ Отменить запись»,
    # а если есть строки с пометкой — «🔎 Проверить»
    checks = context.user_data[LAST_WRITE]["checks"] if rows else {}
    # единственную строку с пометкой проверяем прямо в меню под сводкой — если
    # включены группы; без них вопрос с группами только по «🔎 Проверить»
    single = len(rows) == 1 and checks and keys.groups
    check_now = fix.inline_check(context) if single else None
    menu = await show_main_menu(
        chat,
        context,
        report,
        actions=bool(rows),
        check=len(checks),
        menu=check_now,
    )
    if rows and menu is not None:
        context.user_data[LAST_WRITE]["menu"] = (chat.id, menu.message_id)
    # «🐷» — отдельным сообщением под меню: копилки, что отложить с поступления
    # к какому месяцу доход не в сумах — до «🐷»: от ответа зависит его месяц
    await month_question(context, chat.id, rows, first)
    await income_prompt(context, chat.id, chat.id, rows)
    # «🐷 Из копилки…» — тоже своим сообщением; приложению — строкой ответа
    lines += await spend_note(context, chat.id, rows)
    return SaveResult(len(rows), first, last, tuple(lines))


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
        if entry := seen_input(context, fingerprint):
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
        await save_rows(context, update.effective_chat, rows, skipped, fingerprint)

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
