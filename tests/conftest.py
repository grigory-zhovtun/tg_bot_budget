"""Общие настройки тестов: фиктивное окружение до импорта app.config."""

import os

os.environ.setdefault("TELEGRAM_TOKEN", "123456:TEST")
os.environ.setdefault("SPREADSHEET_ID", "test-spreadsheet")
os.environ.setdefault(
    "GOOGLE_SERVICE_ACCOUNT_EMAIL", "bot@test.iam.gserviceaccount.com"
)
os.environ.setdefault("GOOGLE_PRIVATE_KEY", "test-key")
