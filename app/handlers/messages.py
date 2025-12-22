from telegram import Update
from telegram.ext import ContextTypes
import logging
from datetime import datetime
from app import config
from app.services.google_sheets import GoogleSheetsService
from app.handlers.common import get_currency_from_source, track_message, clear_tracked_messages, show_main_menu
from app.utils.keyboards import generate_sources_keyboard, generate_categories_keyboard, generate_subcategories_keyboard

logger = logging.getLogger(__name__)

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text messages and photos (Source selection, Manual Entry, or AI Parsing)."""
    
    # Check if this is a Photo message
    is_photo = bool(update.message.photo)
    msg_text = update.message.caption if is_photo else update.message.text
    
    # If photo but no caption, treat text as empty string (still proceed to AI if photo exists)
    if is_photo and not msg_text:
        msg_text = ""
        
    if not msg_text and not is_photo:
        return # Ignore empty updates

    sources = context.bot_data.get("sources", [])
    
    # 1. Check if text is a Source Selection (Only if text exists)
    if msg_text:
        clean_text = msg_text.replace("✅ ", "") if msg_text.startswith("✅ ") else msg_text
        if clean_text in sources:
            context.user_data['source'] = clean_text
            derived_currency = get_currency_from_source(clean_text)
            resp_text = f"Источник '{clean_text}' выбран (Валюта: {derived_currency})."
            msg1 = await update.message.reply_text(
                 resp_text,
                 reply_markup=generate_sources_keyboard(sources, clean_text)
            )
            track_message(context, msg1)
            msg2 = await update.message.reply_text(
                "Выбери категорию или отправь описание траты:",
                reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
            )
            track_message(context, msg2)
            return

        # 2. Check for "Back"
        if msg_text == "⬅️ Назад":
            context.user_data.pop('category', None)
            context.user_data.pop('subcategory', None)
            current_source = context.user_data.get('source')
            if current_source:
                 msg = await update.message.reply_text(
                    f"Источник: {current_source}. Выбери категорию:",
                    reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
                )
                 track_message(context, msg)
            else:
                 msg = await update.message.reply_text(
                    "Источник не выбран.",
                    reply_markup=generate_sources_keyboard(sources)
                )
                 track_message(context, msg)
            return

    # 3. Decision Logic: Manual Entry (Strict) vs AI Parsing
    # Manual Entry requires: Source + Category + Subcategory + Text matches "float string" pattern.
    current_source = context.user_data.get('source')
    category = context.user_data.get('category')
    subcategory = context.user_data.get('subcategory')
    
    is_manual_candidate = (current_source and category and subcategory and msg_text and not is_photo)
    manual_success = False
    
    if is_manual_candidate:
        try:
            parts = msg_text.split(' ', 1)
            amount = float(parts[0].replace(',', '.'))
            comment = parts[1] if len(parts) > 1 else ""
            
            # Execute Manual Entry logic
            await _save_transaction(update, context, current_source, category, subcategory, amount, comment)
            manual_success = True
        except ValueError:
            # Not a simple number, so fall through to AI
            manual_success = False
    
    if manual_success:
        return

    # 4. AI Parsing (Fallthrough)
    ai_service = context.bot_data.get("ai_service")
    if not ai_service or not config.GEMINI_API_KEY:
        if is_manual_candidate:
             await update.message.reply_text("Неверный формат суммы для ручного ввода. AI сервис недоступен.")
        else:
             await update.message.reply_text("AI сервис не настроен. Пожалуйста, используйте кнопки для ручного ввода.")
        return

    # Prepare Image if photo
    image_part = None
    if is_photo:
        photo_file = await update.message.photo[-1].get_file()
        file_path = f"/tmp/{photo_file.file_id}.jpg"
        await photo_file.download_to_drive(file_path)
        
        # We need to read it back for genai. Or pass PIL image.
        import PIL.Image
        image_part = PIL.Image.open(file_path)

    analyzing_msg = await update.message.reply_text("🔍 Анализирую...")
    track_message(context, analyzing_msg)

    try:
        known_cats = context.bot_data.get("categories", [])
        known_srcs = context.bot_data.get("sources", [])
        
        result = await ai_service.parse_transaction(
            user_input=msg_text or "Image Input",
            image_part=image_part,
            known_categories=known_cats,
            known_sources=known_srcs
        )
        
        # Logic to handle results
        # Needs: amount, category, subcategory, comment, source
        ai_amount = result.get('amount')
        ai_cat = result.get('category')
        ai_sub = result.get('subcategory')
        ai_comment = result.get('comment') or "AI Recognized"
        ai_source = result.get('source') or current_source # Use inferred source or fallback to selected

        if not ai_amount:
            await update.message.reply_text("Не удалось определить сумму транзакции.")
            return

        if not ai_source:
            await update.message.reply_text("Не удалось определить источник. Выберите источник вручную и повторите.")
            return

        # Auto-save or Confirm?
        # User requested convenience. Let's Auto-save if confident (all fields present).
        
        # Check defaults if missing
        if not ai_cat or ai_cat not in known_cats:
            ai_cat = "Прочее" # Fallback
            ai_sub = "AI (Не распознано)"
        if not ai_sub:
             ai_sub = "Общее"

        await _save_transaction(update, context, ai_source, ai_cat, ai_sub, ai_amount, ai_comment)

    except Exception as e:
        logger.error(f"AI Error: {e}")
        await update.message.reply_text(f"Ошибка AI: {e}")


async def _save_transaction(update, context, source, category, subcategory, amount, comment):
    """Helper to save to Google Sheets."""
    gs_service: GoogleSheetsService = context.bot_data.get("gs_service")
    last_row = gs_service.get_last_row_index()
    next_row = last_row + 1
    
    balance_formula_ru = (
            f'=СУММЕСЛИМН($D$2:D{next_row}; $H$2:H{next_row}; $H{next_row}; $G$2:G{next_row}; $G{next_row}; $B$2:B{next_row}; "💰 ДОХОДЫ")'
            f' - '
            f'СУММЕСЛИМН($D$2:D{next_row}; $H$2:H{next_row}; $H{next_row}; $G$2:G{next_row}; $G{next_row}; $B$2:B{next_row}; "<>💰 ДОХОДЫ")'
    )
    
    currency = get_currency_from_source(source)
    
    row_data = [
        datetime.now().strftime('%d.%m.%Y'),
        category.upper(),
        subcategory,
        amount,
        balance_formula_ru,
        comment,
        currency,
        source
    ]
    
    if gs_service.add_transaction(row_data):
        # Clear specific manual selection state
        context.user_data.pop('category', None)
        context.user_data.pop('subcategory', None)
        context.user_data['source'] = source

        # Clear previous bot messages
        chat_id = update.effective_chat.id
        await clear_tracked_messages(context, chat_id)

        # Show success message with main menu
        success_msg = f"✅ Записано:\n{amount} {currency} - {category} ({subcategory})\n{comment}\n(Источник: {source})"
        await show_main_menu(update, context, success_msg)
    else:
        await update.message.reply_text("❌ Ошибка при записи в Google Таблицу.")
