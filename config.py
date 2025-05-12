import os

# Telegram Bot Configuration
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

# Google Sheets Configuration
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_SERVICE_ACCOUNT_EMAIL = os.environ.get("GOOGLE_SERVICE_ACCOUNT_EMAIL")
# GOOGLE_PRIVATE_KEY будет загружен и обработан в bot.py из .env или переменных окружения
GOOGLE_PRIVATE_KEY = os.environ.get("GOOGLE_PRIVATE_KEY")

# Sheet Names (остаются как есть, это не токены)
FACT_SHEET_NAME = "fact"
SYSTEM_SHEET_NAME = "system"

# Card Information Cell Addresses (остаются как есть)
CARD1_CELL = "K2"
CARD2_CELL = "L2"
DEFAULT_CURRENCY = "UZS"