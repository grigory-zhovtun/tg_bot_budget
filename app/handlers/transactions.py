import logging

from telegram import Update
from telegram.ext import ContextTypes

from app.handlers.common import track_message
from app.utils.keyboards import (
    amount_prompt,
    category_prompt,
    generate_categories_keyboard,
    generate_sources_keyboard,
    generate_subcategories_keyboard,
    subcategory_prompt,
)

logger = logging.getLogger(__name__)


async def transaction_button_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """Кнопки ручного ввода: группа → подгруппа → сумма текстом."""
    query = update.callback_query
    await query.answer()
    data = query.data
    icons = context.bot_data.get("icons", {})
    subcategories = context.bot_data.get("subcategories", {})

    # После перезапуска user_data пуст — берём карту последней записи
    source = context.user_data.get("source") or context.bot_data.get("last_source")
    if source:
        context.user_data["source"] = source

    if data.startswith("cat_"):
        category = data[4:]
        context.user_data["category"] = category
        if not source:
            if query.message:
                msg = await query.message.reply_text(
                    text="⚠️ Выберите источник",
                    reply_markup=generate_sources_keyboard(
                        context.bot_data.get("sources", [])
                    ),
                )
                track_message(context, msg)
            return
        await query.edit_message_text(
            text=subcategory_prompt(source, category),
            reply_markup=generate_subcategories_keyboard(
                subcategories, category, icons
            ),
        )

    elif data == "back_to_categories":
        context.user_data.pop("category", None)
        context.user_data.pop("subcategory", None)
        await query.edit_message_text(
            text=category_prompt(source) if source else "Категория:",
            reply_markup=generate_categories_keyboard(
                context.bot_data.get("categories", [])
            ),
        )

    elif data.startswith("sub_"):
        subcategory = data[4:]
        context.user_data["subcategory"] = subcategory
        category = context.user_data.get("category", "")
        await query.edit_message_text(
            text=amount_prompt(source or "?", category, subcategory, icons),
            reply_markup=generate_subcategories_keyboard(
                subcategories, category, icons
            ),
        )
