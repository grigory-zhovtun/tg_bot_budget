from telegram import Update, InputMediaPhoto
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


async def analytics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for /analytics command.
    Sends 3-day analytics with charts.
    """
    analytics_service = context.bot_data.get("analytics_service")

    if not analytics_service:
        await update.message.reply_text("Сервис аналитики не доступен.")
        return

    status_msg = await update.message.reply_text("📊 Формирую аналитику за 3 дня...")

    try:
        # Generate report with charts
        report_text, charts = analytics_service.generate_3day_report()

        # Delete status message
        await status_msg.delete()

        # Send text report
        try:
            await update.message.reply_text(report_text, parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(report_text, parse_mode=None)

        # Send charts
        if charts:
            for i, chart_buf in enumerate(charts):
                caption = "📈 Расходы по категориям" if i == 0 else "📊 Динамика по дням"
                await update.message.reply_photo(photo=chart_buf, caption=caption)
                chart_buf.close()

    except Exception as e:
        logger.error(f"Analytics error: {e}")
        try:
            await status_msg.edit_text(f"Ошибка при формировании аналитики: {e}")
        except:
            await update.message.reply_text(f"Ошибка при формировании аналитики: {e}")


async def send_daily_analytics(bot, chat_id: int, analytics_service):
    """
    Send daily analytics to a specific chat.
    Called by scheduler.
    """
    try:
        report_text, charts = analytics_service.generate_3day_report()

        # Send text report
        try:
            await bot.send_message(chat_id=chat_id, text=report_text, parse_mode="Markdown")
        except Exception:
            await bot.send_message(chat_id=chat_id, text=report_text, parse_mode=None)

        # Send charts
        if charts:
            for i, chart_buf in enumerate(charts):
                caption = "📈 Расходы по категориям" if i == 0 else "📊 Динамика по дням"
                await bot.send_photo(chat_id=chat_id, photo=chart_buf, caption=caption)
                chart_buf.close()

        logger.info(f"Daily analytics sent to chat {chat_id}")

    except Exception as e:
        logger.error(f"Failed to send daily analytics: {e}")
