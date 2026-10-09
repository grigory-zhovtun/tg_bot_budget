import os
import re
from pathlib import Path

from dotenv import load_dotenv

from app.auth import parse_user_ids

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN is not set in environment variables.")

# Google Sheets
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
if not SPREADSHEET_ID:
    raise ValueError("SPREADSHEET_ID is not set in environment variables.")

GOOGLE_SERVICE_ACCOUNT_EMAIL = os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL")
GOOGLE_PRIVATE_KEY = os.getenv("GOOGLE_PRIVATE_KEY")
GOOGLE_APPLICATION_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")

if not GOOGLE_APPLICATION_CREDENTIALS_PATH and not (
    GOOGLE_SERVICE_ACCOUNT_EMAIL and GOOGLE_PRIVATE_KEY
):
    raise ValueError(
        "Google credentials are missing. Set GOOGLE_APPLICATION_CREDENTIALS_PATH or (GOOGLE_SERVICE_ACCOUNT_EMAIL and GOOGLE_PRIVATE_KEY)."
    )

if GOOGLE_PRIVATE_KEY:
    GOOGLE_PRIVATE_KEY = GOOGLE_PRIVATE_KEY.replace("\\n", "\n")

# Sheet Names
FACT_SHEET_NAME = "fact"
SYSTEM_SHEET_NAME = "system"

# Defaults
DEFAULT_CURRENCY = "UZS"
FALLBACK_CURRENCY = "XXX"

# Webhook (for Render/Production)
# Render задаёт RENDER_EXTERNAL_URL только web-сервисам; у воркера его нет → polling
WEBHOOK_URL = os.getenv("WEBHOOK_URL") or os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
PORT = int(os.getenv("PORT", "8443"))
LOCAL_RUN = os.getenv("LOCAL_RUN", "False").lower() == "true"

# AI
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# Псевдоним на актуальную Flash-модель; конкретную версию видно в логах
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-flash-latest")
# Запасные модели, если основная перегружена (503/429): «a,b»
GEMINI_FALLBACK_MODELS = [
    m.strip()
    for m in os.getenv("GEMINI_FALLBACK_MODELS", "gemini-flash-lite-latest").split(",")
    if m.strip()
]
# We don't raise error immediately to allow bot to start if key is missing (feature flag logic),
# but for this specific request, it's critical.
# However, user might deploy first then add key.

# Daily Analytics
# Chat ID for daily analytics reports (your Telegram user ID)
ANALYTICS_CHAT_ID = os.getenv("ANALYTICS_CHAT_ID")
# Time for daily report (24h format, e.g., "07:00" for 7 AM)
ANALYTICS_TIME = os.getenv("ANALYTICS_TIME", "07:00")
# Timezone for scheduling (e.g., "Asia/Tashkent", "Europe/Moscow")
ANALYTICS_TIMEZONE = os.getenv("ANALYTICS_TIMEZONE", "Asia/Tashkent")
# Карты, которые бот не сверяет по скринам (чужие, детские): «2513, 1111»
IGNORED_CARDS = frozenset(
    d for d in re.findall(r"\d{4}", os.getenv("IGNORED_CARDS", ""))
)
# Воскресная сводка владельцу: «HH:MM» по ANALYTICS_TIMEZONE, «off» — выключить
WEEKLY_DIGEST_TIME = os.getenv("WEEKLY_DIGEST_TIME", "20:00")

# Access control: Telegram user IDs allowed to use the bot ("123, 456").
# Falls back to ANALYTICS_CHAT_ID (the owner's private chat id == user id).
# Empty → the bot answers nobody.
ALLOWED_USER_IDS = parse_user_ids(os.getenv("ALLOWED_USER_IDS"), ANALYTICS_CHAT_ID)
