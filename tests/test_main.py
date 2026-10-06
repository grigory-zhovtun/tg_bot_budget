"""Сборка приложения без сети: доступ, обработчики, ежедневный отчёт."""

import logging

import pytest
from telegram.ext import CommandHandler, TypeHandler

from app import config, main
from tests.test_messages import CATEGORIES, SOURCES, SUBCATEGORIES, FakeSheets


def build(monkeypatch: pytest.MonkeyPatch, **settings: object):
    defaults = {
        "ANALYTICS_CHAT_ID": "42",
        "ANALYTICS_TIME": "07:00",
        "ANALYTICS_TIMEZONE": "Asia/Tashkent",
        "ALLOWED_USER_IDS": frozenset({42}),
    }
    for name, value in {**defaults, **settings}.items():
        monkeypatch.setattr(config, name, value)
    return main.build_application(FakeSheets(), CATEGORIES, SUBCATEGORIES, SOURCES)


def test_gate_runs_before_all_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    app = build(monkeypatch)
    [gate] = app.handlers[-1]
    assert isinstance(gate, TypeHandler)
    commands = {
        command
        for handler in app.handlers[0]
        if isinstance(handler, CommandHandler)
        for command in handler.commands
    }
    assert commands == {"start", "reboot", "advice", "analytics", "undo"}


def test_daily_report_is_scheduled_in_owner_timezone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = build(monkeypatch)
    [job] = app.job_queue.get_jobs_by_name("daily_analytics")
    assert job.chat_id == 42
    trigger = str(job.trigger)
    assert "hour='7'" in trigger and "minute='0'" in trigger
    assert str(job.trigger.timezone) == "Asia/Tashkent"


@pytest.mark.parametrize(
    "settings",
    [
        {"ANALYTICS_CHAT_ID": None},
        {"ANALYTICS_TIME": "7am"},
        {"ANALYTICS_TIMEZONE": "Mars/Base"},
    ],
)
def test_bad_or_missing_report_settings_do_not_break_start(
    monkeypatch: pytest.MonkeyPatch, settings: dict
) -> None:
    app = build(monkeypatch, **settings)
    assert app.job_queue.get_jobs_by_name("daily_analytics") == ()


def test_httpx_logs_no_request_urls() -> None:
    main.setup_logging()
    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING
