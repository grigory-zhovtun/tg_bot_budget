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
        
        # Delete status message
        await status_msg.delete()
        
        # Try sending with Markdown
        # Try sending with Markdown
        try:
            # Split message if too long (Telegram limit 4096)
            # We use a slightly smaller chunk size to be safe
            chunk_size = 4000
            for i in range(0, len(advice_text), chunk_size):
                chunk = advice_text[i:i + chunk_size]
                await update.message.reply_text(chunk, parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Markdown parsing failed: {e}. Sending plain text.")
            # Fallback to plain text if Markdown fails
            for i in range(0, len(advice_text), chunk_size):
                chunk = advice_text[i:i + chunk_size]
                await update.message.reply_text(chunk, parse_mode=None)
        
    except Exception as e:
        logger.error(f"Advice handling error: {e}")
        # If status_msg still exists/accessible, edit it
        try:
            await status_msg.edit_text(f"Произошла ошибка при анализе: {e}")
        except:
            await update.message.reply_text(f"Произошла ошибка при анализе: {e}")
