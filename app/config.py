import os
from pathlib import Path
from dotenv import load_dotenv

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

if not GOOGLE_APPLICATION_CREDENTIALS_PATH and not (GOOGLE_SERVICE_ACCOUNT_EMAIL and GOOGLE_PRIVATE_KEY):
    raise ValueError("Google credentials are missing. Set GOOGLE_APPLICATION_CREDENTIALS_PATH or (GOOGLE_SERVICE_ACCOUNT_EMAIL and GOOGLE_PRIVATE_KEY).")

if GOOGLE_PRIVATE_KEY:
    GOOGLE_PRIVATE_KEY = GOOGLE_PRIVATE_KEY.replace('\\n', '\n')

# Sheet Names
FACT_SHEET_NAME = "fact"
SYSTEM_SHEET_NAME = "system"

# Defaults
DEFAULT_CURRENCY = "UZS"
FALLBACK_CURRENCY = "XXX"

# Webhook (for Render/Production)
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "8443"))
LOCAL_RUN = os.getenv("LOCAL_RUN", "False").lower() == "true"

# AI
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# We don't raise error immediately to allow bot to start if key is missing (feature flag logic), 
# but for this specific request, it's critical. 
# However, user might deploy first then add key.
