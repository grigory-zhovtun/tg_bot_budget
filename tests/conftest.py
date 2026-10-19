"""Общие настройки тестов: фиктивное окружение до импорта app.config."""

import os

os.environ.setdefault("TELEGRAM_TOKEN", "123456:TEST")
os.environ.setdefault("SPREADSHEET_ID", "test-spreadsheet")
os.environ.setdefault(
    "GOOGLE_SERVICE_ACCOUNT_EMAIL", "bot@test.iam.gserviceaccount.com"
)
os.environ.setdefault("GOOGLE_PRIVATE_KEY", "test-key")


import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def plain_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    """По умолчанию — обычный текст: старые проверки читают текст сообщений.
    Расширенный вид включают тесты с фикстурой rich."""
    from app import config

    monkeypatch.setattr(config, "RICH_MESSAGES", False)


@pytest.fixture
def rich(monkeypatch: pytest.MonkeyPatch) -> None:
    from app import config

    monkeypatch.setattr(config, "RICH_MESSAGES", True)
