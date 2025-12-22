import logging
import sys
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from app import config
from app.services.google_sheets import GoogleSheetsService
from app.handlers import common, admin, transactions, messages, analytics

# Setup Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def main():
    if not config.TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN is not set.")
        sys.exit(1)

    # Initialize Service
    try:
        gs_service = GoogleSheetsService()
        categories, subcategories, sources = gs_service.get_categories_and_sources()
    except Exception as e:
        logger.critical(f"Failed to initialize Google Sheets Service: {e}")
        # We might want to exit or let it run with empty data (waiting for reboot)
        logger.warning("Starting bot with empty data. Please fix credentials and /reboot.")
        gs_service = None # Logic should handle None?
        sys.exit(1)

    # Build App
    app_builder = ApplicationBuilder().token(config.TELEGRAM_TOKEN)
    
    async def post_init(application):
        await application.bot.set_my_commands([
            ("start", "Начать работу 🚀"),
            ("advice", "Финансовый совет 🧠"),
            ("reboot", "Обновить настройки 🔄")
        ])

    app = app_builder.post_init(post_init).build()

    # Inject Dependencies
    app.bot_data["gs_service"] = gs_service
    app.bot_data["categories"] = categories
    app.bot_data["subcategories"] = subcategories
    app.bot_data["sources"] = sources
    
    # AI Service
    from app.services.ai_service import GeminiService
    ai_service = GeminiService(gs_service)
    app.bot_data["ai_service"] = ai_service
    
    logger.info(f"Loaded {len(sources)} sources and {len(categories)} categories.")
    if config.GEMINI_API_KEY:
        logger.info("AI Service enabled.")


    # Handlers
    app.add_handler(CommandHandler("start", common.start))
    app.add_handler(CommandHandler("reboot", admin.reboot))
    app.add_handler(CommandHandler("advice", analytics.advice_command))
    
    app.add_handler(CallbackQueryHandler(transactions.transaction_button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, messages.text_handler))

    # Run
    if config.LOCAL_RUN:
        logger.info("Starting polling (Local Run)...")
        app.run_polling()
    else:
        # Webhook mode
        if not config.WEBHOOK_URL:
            logger.warning("WEBHOOK_URL not set in production. Falling back to polling.")
            app.run_polling()
        else:
            logger.info(f"Starting webhook on port {config.PORT}...")
            app.run_webhook(
                listen="0.0.0.0",
                port=config.PORT,
                url_path=config.TELEGRAM_TOKEN,
                webhook_url=f"{config.WEBHOOK_URL}/{config.TELEGRAM_TOKEN}"
            )

if __name__ == '__main__':
    main()
