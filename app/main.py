"""Точка входа: сборка Telegram-приложения, обработчики, ежедневный отчёт."""

import logging
import secrets
import sys
from datetime import time as dtime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

from app import config
from app.auth import make_gatekeeper
from app.errors import on_error
from app.handlers import (
    admin,
    analytics,
    common,
    messages,
    statement_import,
    transactions,
    undo,
)
from app.services.ai_service import GeminiService
from app.services.analytics_service import AnalyticsService
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

COMMANDS = [
    ("start", "Начать работу 🚀"),
    ("analytics", "Аналитика за 3 дня 📊"),
    ("advice", "Финансовый совет 🧠"),
    ("undo", "Отменить последнюю запись ↩️"),
    ("reboot", "Обновить настройки 🔄"),
]
WEBHOOK_PATH = "telegram"


def setup_logging() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    # httpx на уровне INFO пишет полный URL запроса к Telegram, а в нём токен бота
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def _post_init(application: Application) -> None:
    await application.bot.set_my_commands(COMMANDS)
    # Проверка Gemini — в фоне: при перегрузке модели она идёт минуту и больше,
    # а бот всё это время не отвечал бы
    application.job_queue.run_once(_self_check, 1, name="gemini_self_check")


async def _self_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot_data["ai_service"].self_check()


async def _daily_analytics(context: ContextTypes.DEFAULT_TYPE) -> None:
    await analytics.send_daily_analytics(
        bot=context.bot,
        chat_id=context.job.chat_id,
        analytics_service=context.bot_data["analytics_service"],
    )


def _schedule_daily_report(app: Application) -> None:
    """Ежедневный отчёт в ANALYTICS_CHAT_ID через встроенный JobQueue."""
    if not config.ANALYTICS_CHAT_ID:
        logger.info("ANALYTICS_CHAT_ID not set. Daily analytics disabled.")
        return
    try:
        hour, minute = map(int, config.ANALYTICS_TIME.split(":"))
        when = dtime(hour, minute, tzinfo=ZoneInfo(config.ANALYTICS_TIMEZONE))
        chat_id = int(config.ANALYTICS_CHAT_ID)
    except (ValueError, ZoneInfoNotFoundError):
        logger.exception("Bad daily analytics settings, report disabled")
        return
    app.job_queue.run_daily(
        _daily_analytics, time=when, chat_id=chat_id, name="daily_analytics"
    )
    logger.info(
        "Daily analytics scheduled for %s (%s)",
        config.ANALYTICS_TIME,
        config.ANALYTICS_TIMEZONE,
    )


def build_application(
    gs_service: GoogleSheetsService,
    categories: list[str],
    subcategories: dict[str, list[str]],
    sources: list[str],
) -> Application:
    """Собрать приложение без сетевых вызовов: зависимости, доступ, обработчики."""
    app = (
        ApplicationBuilder().token(config.TELEGRAM_TOKEN).post_init(_post_init).build()
    )
    app.bot_data.update(
        gs_service=gs_service,
        categories=categories,
        subcategories=subcategories,
        sources=sources,
        ai_service=GeminiService(gs_service),
        analytics_service=AnalyticsService(gs_service),
    )

    # Access control: runs before every other handler (group -1)
    if config.ALLOWED_USER_IDS:
        logger.info("Access allowed for user ids: %s", sorted(config.ALLOWED_USER_IDS))
    else:
        logger.warning(
            "ALLOWED_USER_IDS and ANALYTICS_CHAT_ID are empty: the bot answers nobody"
        )
    app.add_handler(TypeHandler(Update, make_gatekeeper(config.ALLOWED_USER_IDS)), -1)

    app.add_handler(CommandHandler("start", common.start))
    app.add_handler(CommandHandler("reboot", admin.reboot))
    app.add_handler(CommandHandler("undo", undo.undo))
    app.add_handler(CommandHandler("advice", analytics.advice_command))
    app.add_handler(CommandHandler("analytics", analytics.analytics_command))
    # Кнопки импорта выписок — раньше общего обработчика кнопок без фильтра
    app.add_handler(CallbackQueryHandler(statement_import.button, pattern=r"^import:"))
    app.add_handler(CallbackQueryHandler(transactions.transaction_button_handler))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, messages.text_handler)
    )
    app.add_handler(MessageHandler(filters.PHOTO, messages.text_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, messages.document_handler))

    app.add_error_handler(on_error)
    _schedule_daily_report(app)
    return app


def main() -> None:
    setup_logging()
    try:
        gs_service = GoogleSheetsService()
        categories, subcategories, sources = gs_service.get_categories_and_sources()
    except Exception:
        logger.critical("Failed to initialize Google Sheets Service", exc_info=True)
        sys.exit(1)

    app = build_application(gs_service, categories, subcategories, sources)
    logger.info("Loaded %d sources and %d categories.", len(sources), len(categories))
    logger.info("AI Service %s.", "enabled" if config.GEMINI_API_KEY else "disabled")

    if config.LOCAL_RUN or not config.WEBHOOK_URL:
        logger.info("Starting polling...")
        app.run_polling(allowed_updates=Update.ALL_TYPES)
        return

    # Webhook: путь не содержит токен, Telegram подписывает запросы секретом
    logger.info("Starting webhook on port %s...", config.PORT)
    app.run_webhook(
        listen="0.0.0.0",
        port=config.PORT,
        url_path=WEBHOOK_PATH,
        webhook_url=f"{config.WEBHOOK_URL.rstrip('/')}/{WEBHOOK_PATH}",
        secret_token=config.WEBHOOK_SECRET or secrets.token_urlsafe(32),
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
