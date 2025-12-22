from telegram import Update
from telegram.ext import ContextTypes
import logging
from app import config
from app.handlers.common import get_currency_from_source, track_message
from app.utils.keyboards import generate_subcategories_keyboard, generate_categories_keyboard, generate_sources_keyboard

logger = logging.getLogger(__name__)

async def transaction_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles callback queries from inline keyboards."""
    query = update.callback_query
    await query.answer()
    data = query.data

    # User state
    selected_source = context.user_data.get('source')
    derived_currency = get_currency_from_source(selected_source)

    if data.startswith("cat_"):
        selected_category = data[4:]
        context.user_data['category'] = selected_category
        
        if not selected_source:
             # If source somehow lost or not selected (though UI tries to prevent)
             if query.message:
                msg = await query.message.reply_text(
                    text="Сначала выберите ИСТОЧНИК.\nЗатем выберите категорию.",
                    reply_markup=generate_sources_keyboard(context.bot_data.get("sources", []))
                )
                track_message(context, msg)
             return

        # Fetch subcategories from bot_data
        subcategories = context.bot_data.get("subcategories", {})
        
        await query.edit_message_text(
            text=f"Источник: {selected_source} (Валюта: {derived_currency})\nКатегория: {selected_category}\n\nВыберите подкатегорию:",
            reply_markup=generate_subcategories_keyboard(subcategories, selected_category)
        )

    elif data in ["back_to_categories"]:
        context.user_data.pop('category', None)
        context.user_data.pop('subcategory', None)

        msg = "Выбери категорию:"
        if selected_source:
            msg = f"Источник: {selected_source} (Валюта: {derived_currency})\n{msg}"

        await query.edit_message_text(
            text=msg,
            reply_markup=generate_categories_keyboard(context.bot_data.get("categories", []))
        )

    elif data.startswith("sub_"):
        subcategory_name = data[4:]
        context.user_data['subcategory'] = subcategory_name
        category = context.user_data.get('category', 'Не выбрана')
        
        prompt_text = (f"Источник: {selected_source}\n"
                       f"Категория: {category}\n"
                       f"Подкатегория: {subcategory_name}\n"
                       f"Валюта: {derived_currency}\n\n"
                       f"ВНЕСИТЕ СУММУ И КОММЕНТАРИЙ (ЧЕРЕЗ ПРОБЕЛ):")
        
        # We keep the subcategories keyboard to allow "Back" functionality logic to remain simple
        # or we could show a "Cancel" button. Original showed subcategories.
        subcategories = context.bot_data.get("subcategories", {})
        await query.edit_message_text(
            text=prompt_text,
            reply_markup=generate_subcategories_keyboard(subcategories, category)
        )
