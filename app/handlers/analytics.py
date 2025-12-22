from telegram import Update
from telegram.ext import ContextTypes
import logging

logger = logging.getLogger(__name__)

async def advice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for /advice command. 
    Triggers AI analysis of transaction history.
    """
    ai_service = context.bot_data.get("ai_service")
    
    if not ai_service:
        await update.message.reply_text("AI сервис не доступен.")
        return

    # Notify user that process started (it might take a few seconds)
    status_msg = await update.message.reply_text("🤖 Анализирую ваши финансы... Это займет пару секунд.")
    
    try:
        # Call AI service
        advice_text = await ai_service.analyze_finances()
        
        # Delete status message and send report
        await status_msg.delete()
        await update.message.reply_text(advice_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Advice handling error: {e}")
        await status_msg.edit_text(f"Произошла ошибка при анализе: {e}")
