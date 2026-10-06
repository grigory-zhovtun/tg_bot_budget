"""Сборка приложения без сети: доступ, обработчики, ежедневный отчёт."""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


async def test_gemini_self_check_does_not_delay_the_start() -> None:
    scheduled: list[tuple[object, float, str]] = []
    ai = SimpleNamespace(self_check=AsyncMock())
    application = SimpleNamespace(
        bot=SimpleNamespace(set_my_commands=AsyncMock()),
        bot_data={"ai_service": ai},
        job_queue=SimpleNamespace(
            run_once=lambda cb, when, name: scheduled.append((cb, when, name))
        ),
    )

    await main._post_init(application)

    application.bot.set_my_commands.assert_awaited_once_with(main.COMMANDS)
    ai.self_check.assert_not_awaited()
    [(callback, when, name)] = scheduled
    assert (when, name) == (1, "gemini_self_check")
    await callback(SimpleNamespace(bot_data=application.bot_data))
    ai.self_check.assert_awaited_once()
