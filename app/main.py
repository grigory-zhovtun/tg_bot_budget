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
    balances,
    common,
    fix,
    last_write,
    messages,
    month,
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
    ("plan", "План-факт месяца 📋"),
    ("today", "Лимит на сегодня 💸"),
    ("advice", "Финансовый совет 🧠"),
    ("fix", "Исправить категорию последней записи ✏️"),
    ("undo", "Отменить последнюю запись ↩️"),
    ("reboot", "Обновить настройки 🔄"),
]
WEBHOOK_PATH = "telegram"
SUNDAY = 0  # PTB 20+: дни недели 0–6 = воскресенье–суббота


def setup_logging() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    # httpx на уровне INFO пишет полный URL запроса к Telegram, а в нём токен бота
    logging.getLogger("httpx").setLevel(logging.WARNING)
    # apscheduler на INFO пишет 4 строки на каждый запуск задачи
    logging.getLogger("apscheduler").setLevel(logging.WARNING)


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


def _schedule_month_tab(app: Application) -> None:
    """Вкладка месяца: проверка в 00:05 по времени владельца и сразу после запуска."""
    try:
        zone = ZoneInfo(config.ANALYTICS_TIMEZONE)
    except ZoneInfoNotFoundError:
        logger.warning("Unknown ANALYTICS_TIMEZONE, month tab check uses UTC")
        zone = ZoneInfo("UTC")
    app.job_queue.run_daily(
        month.month_tab_job, time=dtime(0, 5, tzinfo=zone), name="month_tab"
    )
    app.job_queue.run_once(month.month_tab_job, 30, name="month_tab_on_start")


async def _weekly_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = month.owner_chat_id()
    if chat_id is not None:
        await analytics.send_weekly_digest(
            context.bot, chat_id, context.bot_data["analytics_service"]
        )


def _schedule_weekly_digest(app: Application) -> None:
    """Сводка недели по воскресеньям; WEEKLY_DIGEST_TIME=off — выключить."""
    if config.WEEKLY_DIGEST_TIME.strip().lower() == "off":
        logger.info("Weekly digest disabled")
        return
    try:
        hour, minute = map(int, config.WEEKLY_DIGEST_TIME.split(":"))
        when = dtime(hour, minute, tzinfo=ZoneInfo(config.ANALYTICS_TIMEZONE))
    except (ValueError, ZoneInfoNotFoundError):
        logger.exception("Bad WEEKLY_DIGEST_TIME, weekly digest disabled")
        return
    app.job_queue.run_daily(
        _weekly_digest, time=when, days=(SUNDAY,), name="weekly_digest"
    )


async def _morning_brief(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = month.owner_chat_id()
    if chat_id is not None:
        await analytics.send_morning_brief(
            context.bot, chat_id, context.bot_data["analytics_service"]
        )


def _schedule_morning_brief(app: Application) -> None:
    """Утреннее сообщение с лимитом на день; MORNING_TIME=off — выключить."""
    if config.MORNING_TIME.strip().lower() == "off":
        logger.info("Morning brief disabled")
        return
    try:
        hour, minute = map(int, config.MORNING_TIME.split(":"))
        when = dtime(hour, minute, tzinfo=ZoneInfo(config.ANALYTICS_TIMEZONE))
    except (ValueError, ZoneInfoNotFoundError):
        logger.exception("Bad MORNING_TIME, morning brief disabled")
        return
    app.job_queue.run_daily(_morning_brief, time=when, name="morning_brief")
    logger.info(
        "Morning brief scheduled for %s (%s)",
        config.MORNING_TIME,
        config.ANALYTICS_TIMEZONE,
    )


def build_application(
    gs_service: GoogleSheetsService,
    categories: list[str],
    subcategories: dict[str, list[str]],
    sources: list[str],
    icons: dict[str, str] | None = None,
    last_source: str | None = None,
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
        icons=icons or {},
        last_source=last_source,
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
    app.add_handler(CommandHandler("fix", fix.fix_command))
    app.add_handler(CommandHandler("advice", analytics.advice_command))
    app.add_handler(CommandHandler("analytics", analytics.analytics_command))
    app.add_handler(CommandHandler("plan", analytics.plan_command))
    app.add_handler(CommandHandler("today", analytics.today_command))
    # Кнопки импорта, выравнивания, /fix и под сводкой — раньше общего обработчика
    app.add_handler(CallbackQueryHandler(balances.align_button, pattern=r"^align:"))
    app.add_handler(CallbackQueryHandler(fix.fix_button, pattern=r"^fix:"))
    app.add_handler(CallbackQueryHandler(last_write.button, pattern=r"^last:"))
    app.add_handler(CallbackQueryHandler(statement_import.button, pattern=r"^import:"))
    app.add_handler(CallbackQueryHandler(transactions.transaction_button_handler))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, messages.text_handler)
    )
    app.add_handler(MessageHandler(filters.PHOTO, messages.text_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, messages.document_handler))

    app.add_error_handler(on_error)
    _schedule_daily_report(app)
    _schedule_month_tab(app)
    _schedule_weekly_digest(app)
    _schedule_morning_brief(app)
    return app


def main() -> None:
    setup_logging()
    try:
        gs_service = GoogleSheetsService()
        categories, subcategories, sources = gs_service.get_categories_and_sources()
    except Exception:
        logger.critical("Failed to initialize Google Sheets Service", exc_info=True)
        sys.exit(1)

    try:
        icons = gs_service.get_icons()
        last_source = gs_service.last_source(sources)
    except Exception:
        # не критично: кнопки без иконок, карту спросим у пользователя
        logger.exception("Could not read icons or the last card")
        icons, last_source = {}, None

    app = build_application(
        gs_service, categories, subcategories, sources, icons, last_source
    )
    logger.info(
        "Loaded %d sources, %d categories and %d icons.",
        len(sources),
        len(categories),
        len(icons),
    )
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
