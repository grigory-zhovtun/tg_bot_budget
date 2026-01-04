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
            # Delete user's source selection message to keep chat clean
            try:
                await update.message.delete()
            except Exception:
                pass
            # Silent update - just show keyboards
            msg1 = await update.effective_chat.send_message(
                "ㅤ",  # Invisible character for minimal text
                reply_markup=generate_sources_keyboard(sources, clean_text)
            )
            track_message(context, msg1)
            msg2 = await update.effective_chat.send_message(
                "Категория:",
                reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
            )
            track_message(context, msg2)
            return

        # 2. Check for "Back"
        if msg_text == "⬅️ Назад":
            context.user_data.pop('category', None)
            context.user_data.pop('subcategory', None)
            # Delete user's back message
            try:
                await update.message.delete()
            except Exception:
                pass
            current_source = context.user_data.get('source')
            if current_source:
                msg = await update.effective_chat.send_message(
                    "Категория:",
                    reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
                )
                track_message(context, msg)
            else:
                msg = await update.effective_chat.send_message(
                    "ㅤ",
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

        # Handle both single transaction (dict) and multiple transactions (list)
        transactions = result if isinstance(result, list) else [result]

        if not transactions:
            await update.message.reply_text("Не удалось распознать транзакции.")
            return

        # Process each transaction
        for txn in transactions:
            ai_amount = txn.get('amount')
            ai_cat = txn.get('category')
            ai_sub = txn.get('subcategory')
            ai_comment = txn.get('comment') or "AI Recognized"
            ai_source = txn.get('source') or current_source
            ai_date = txn.get('date')  # Date extracted by AI from receipt/screenshot
            ai_balance = txn.get('balance')  # Card balance if mentioned
            ai_card_id = txn.get('card_identifier')  # Last 4 digits of card

            if not ai_amount:
                continue  # Skip transactions without amount

            if not ai_source:
                await update.message.reply_text(f"Пропущено (нет источника): {ai_comment}")
                continue

            # Check defaults if missing
            if not ai_cat or ai_cat not in known_cats:
                ai_cat = "Прочее"
                ai_sub = "AI (Не распознано)"
            if not ai_sub:
                ai_sub = "Общее"

            await _save_transaction(update, context, ai_source, ai_cat, ai_sub, ai_amount, ai_comment, ai_date, ai_balance, ai_card_id)

    except Exception as e:
        logger.error(f"AI Error: {e}")
        await update.message.reply_text(f"Ошибка AI: {e}")


async def _save_transaction(update, context, source, category, subcategory, amount, comment, date_str=None, balance=None, card_identifier=None):
    """Helper to save to Google Sheets.

    Args:
        date_str: Optional date string in DD.MM.YYYY format. If None, uses today's date.
        balance: Optional card balance to update in the sheet.
        card_identifier: Last 4 digits of card number for balance update.
    """
    gs_service: GoogleSheetsService = context.bot_data.get("gs_service")
    last_row = gs_service.get_last_row_index()
    next_row = last_row + 1

    balance_formula_ru = (
            f'=СУММЕСЛИМН($D$2:D{next_row}; $H$2:H{next_row}; $H{next_row}; $G$2:G{next_row}; $G{next_row}; $B$2:B{next_row}; "💰 ДОХОДЫ")'
            f' - '
            f'СУММЕСЛИМН($D$2:D{next_row}; $H$2:H{next_row}; $H{next_row}; $G$2:G{next_row}; $G{next_row}; $B$2:B{next_row}; "<>💰 ДОХОДЫ")'
    )

    currency = get_currency_from_source(source)

    # Use provided date or fallback to today
    transaction_date = date_str if date_str else datetime.now().strftime('%d.%m.%Y')

    # Ensure amount is positive (remove minus sign from expenses)
    positive_amount = abs(float(amount))

    row_data = [
        transaction_date,
        category.upper(),
        subcategory,
        positive_amount,
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

        # Update card balance if provided
        balance_msg = ""
        if balance is not None and card_identifier:
            cell = config.CARD_BALANCE_CELLS.get(card_identifier)
            if cell:
                if gs_service.update_cell(config.FACT_SHEET_NAME, cell, balance):
                    balance_msg = f" | 💳 {balance:,.0f}"
                    logger.info(f"Updated balance for card {card_identifier}: {balance}")

        # Clear previous bot messages
        chat_id = update.effective_chat.id
        await clear_tracked_messages(context, chat_id)

        # Concise success message: amount Category (subcategory) • Source
        success_msg = f"✅ {positive_amount:,.0f} {currency} • {category} ({subcategory}) • {source}{balance_msg}"
        await show_main_menu(update, context, success_msg)
    else:
        await update.message.reply_text("❌ Ошибка при записи в Google Таблицу.")


async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles document uploads (PDF, Excel, CSV)."""
    document = update.message.document
    if not document:
        return

    file_name = document.file_name.lower()
    mime_type = document.mime_type or ""

    # Determine file type
    is_pdf = file_name.endswith('.pdf') or 'pdf' in mime_type
    is_excel = file_name.endswith(('.xlsx', '.xls')) or 'spreadsheet' in mime_type or 'excel' in mime_type
    is_csv = file_name.endswith('.csv') or 'csv' in mime_type

    if not (is_pdf or is_excel or is_csv):
        await update.message.reply_text("❌ Поддерживаются только PDF, Excel (.xlsx/.xls) и CSV файлы.")
        return

    ai_service = context.bot_data.get("ai_service")
    if not ai_service or not config.GEMINI_API_KEY:
        await update.message.reply_text("AI сервис не настроен.")
        return

    analyzing_msg = await update.message.reply_text("📄 Загружаю и анализирую файл...")
    track_message(context, analyzing_msg)

    try:
        # Download file
        file = await document.get_file()
        file_path = f"/tmp/{document.file_id}_{file_name}"
        await file.download_to_drive(file_path)

        # Extract content based on file type
        extracted_text = ""
        image_part = None

        if is_pdf:
            # For PDF, pass directly to Gemini as image (it handles PDFs well)
            import PIL.Image
            try:
                # Try to use pdf2image if available
                from pdf2image import convert_from_path
                images = convert_from_path(file_path, first_page=1, last_page=5)  # Limit to first 5 pages
                if images:
                    image_part = images[0]  # Use first page as image
                    extracted_text = "PDF document with transaction history"
            except ImportError:
                # Fallback: try to extract text with PyPDF2
                try:
                    import PyPDF2
                    with open(file_path, 'rb') as f:
                        reader = PyPDF2.PdfReader(f)
                        for page in reader.pages[:10]:  # Limit to 10 pages
                            extracted_text += page.extract_text() or ""
                except ImportError:
                    await update.message.reply_text("❌ Для обработки PDF установите pdf2image или PyPDF2.")
                    return

        elif is_excel:
            try:
                import pandas as pd
                df = pd.read_excel(file_path)
                # Convert to string representation
                extracted_text = df.head(500).to_string()  # Limit rows
            except ImportError:
                await update.message.reply_text("❌ Для обработки Excel установите pandas и openpyxl.")
                return

        elif is_csv:
            try:
                import pandas as pd
                df = pd.read_csv(file_path)
                extracted_text = df.head(500).to_string()
            except ImportError:
                # Fallback without pandas
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    extracted_text = f.read()[:50000]  # Limit size

        if not extracted_text and not image_part:
            await update.message.reply_text("❌ Не удалось извлечь данные из файла.")
            return

        # Send to AI for parsing
        current_source = context.user_data.get('source')
        known_cats = context.bot_data.get("categories", [])
        known_srcs = context.bot_data.get("sources", [])

        result = await ai_service.parse_transaction(
            user_input=extracted_text or "Document with transactions",
            image_part=image_part,
            known_categories=known_cats,
            known_sources=known_srcs
        )

        # Handle results (same logic as text_handler)
        transactions = result if isinstance(result, list) else [result]

        if not transactions:
            await update.message.reply_text("Не удалось распознать транзакции в файле.")
            return

        saved_count = 0
        for txn in transactions:
            ai_amount = txn.get('amount')
            ai_cat = txn.get('category')
            ai_sub = txn.get('subcategory')
            ai_comment = txn.get('comment') or "From document"
            ai_source = txn.get('source') or current_source
            ai_date = txn.get('date')  # Date extracted by AI from document
            ai_balance = txn.get('balance')  # Card balance if mentioned
            ai_card_id = txn.get('card_identifier')  # Last 4 digits of card

            if not ai_amount:
                continue

            if not ai_source:
                continue

            if not ai_cat or ai_cat not in known_cats:
                ai_cat = "Прочее"
                ai_sub = "AI (Не распознано)"
            if not ai_sub:
                ai_sub = "Общее"

            await _save_transaction(update, context, ai_source, ai_cat, ai_sub, ai_amount, ai_comment, ai_date, ai_balance, ai_card_id)
            saved_count += 1

        if saved_count == 0:
            await update.message.reply_text("❌ Не удалось сохранить транзакции. Проверьте, что выбран источник.")

    except Exception as e:
        logger.error(f"Document processing error: {e}")
        await update.message.reply_text(f"❌ Ошибка обработки файла: {e}")
