from telegram import Update
from telegram.ext import ContextTypes
import logging
from datetime import datetime
from app import config
from app.services.google_sheets import GoogleSheetsService
from app.handlers.common import get_currency_from_source
from app.utils.keyboards import generate_sources_keyboard, generate_categories_keyboard, generate_subcategories_keyboard

logger = logging.getLogger(__name__)

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text messages (Source selection or Transaction entry)."""
    msg_text = update.message.text
    sources = context.bot_data.get("sources", [])
    
    # 1. Check if text is a Source Selection
    selected_source_name = None
    # Normalize check (handle ✅ if present)
    clean_text = msg_text.replace("✅ ", "") if msg_text.startswith("✅ ") else msg_text
    
    if clean_text in sources:
        selected_source_name = clean_text
    
    if selected_source_name:
        context.user_data['source'] = selected_source_name
        derived_currency = get_currency_from_source(selected_source_name)
        category = context.user_data.get('category')
        
        resp_text = f"Источник '{selected_source_name}' выбран (Валюта: {derived_currency})."
        
        await update.message.reply_text(
             resp_text,
             reply_markup=generate_sources_keyboard(sources, selected_source_name)
        )
        # Always encourage selecting category next
        await update.message.reply_text(
            "Выбери категорию:",
            reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
        )
        return

    # 2. Check for "Back"
    if msg_text == "⬅️ Назад":
        context.user_data.pop('category', None)
        context.user_data.pop('subcategory', None)
        current_source = context.user_data.get('source')
        if current_source:
             await update.message.reply_text(
                f"Источник: {current_source}. Выбери категорию:",
                reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
            )
        else:
             await update.message.reply_text(
                "Источник не выбран.",
                reply_markup=generate_sources_keyboard(sources)
            )
        return

    # 3. Manual Transaction Entry (Amount + Comment)
    current_source = context.user_data.get('source')
    category = context.user_data.get('category')
    subcategory = context.user_data.get('subcategory')
    
    # Validation
    errors = []
    if not current_source: errors.append("- Источник")
    if not category: errors.append("- Категория")
    if not subcategory: errors.append("- Подкатегория")
    
    if errors:
        # Placeholder for AI or error
        await update.message.reply_text(
            "Для ручного ввода выберите сначала:\n" + "\n".join(errors) + 
            "\n\n(Скоро здесь заработает AI!)",
            reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
        )
        return

    # Try to parse Amount + Comment
    try:
        parts = msg_text.split(' ', 1)
        amount_str = parts[0].replace(',', '.')
        amount = float(amount_str)
        comment = parts[1] if len(parts) > 1 else ""
        
        gs_service: GoogleSheetsService = context.bot_data.get("gs_service")
        
        last_row = gs_service.get_last_row_index()
        next_row = last_row + 1
        
        # Russian formula as per original requirement
        balance_formula_ru = (
             f'=СУММЕСЛИМН($D$2:D{next_row}; $H$2:H{next_row}; $H{next_row}; $G$2:G{next_row}; $G{next_row}; $B$2:B{next_row}; "💰 ДОХОДЫ")'
             f' - '
             f'СУММЕСЛИМН($D$2:D{next_row}; $H$2:H{next_row}; $H{next_row}; $G$2:G{next_row}; $G{next_row}; $B$2:B{next_row}; "<>💰 ДОХОДЫ")'
        )

        currency = get_currency_from_source(current_source)
        
        row_data = [
            datetime.now().strftime('%d.%m.%Y'),
            category.upper(),
            subcategory,
            amount,
            balance_formula_ru,
            comment,
            currency,
            current_source
        ]
        
        success = gs_service.add_transaction(row_data)
        
        if success:
            await update.message.reply_text(
                f"✅ Записано:\n{amount} {currency} - {category} ({subcategory})\n{comment}",
                reply_markup=generate_sources_keyboard(sources, current_source)
            )
            await update.message.reply_text(
                "Выбери категорию для следующей:",
                reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
            )
        else:
             await update.message.reply_text("❌ Ошибка при записи в Google Таблицу.")
        
    except ValueError:
        await update.message.reply_text(
             "Неверный формат суммы. Введите число и комментарий.\nПример: 15000 Обед"
        )
