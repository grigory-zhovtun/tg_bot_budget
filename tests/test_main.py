"""Сборка приложения без сети: доступ, обработчики, ежедневный отчёт."""

import logging
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from telegram.ext import CommandHandler, TypeHandler

from app import config, main
from app.web.auth import webhook_secret
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
    assert commands == {
        "start",
        "reboot",
        "advice",
        "analytics",
        "undo",
        "fix",
        "plan",
        "today",
        "icons",
        "subs",
    }


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
    assert logging.getLogger("apscheduler").getEffectiveLevel() >= logging.WARNING


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
    (callback, when, name), packs = scheduled
    assert (when, name) == (1, "gemini_self_check")
    assert packs[1:] == (2, "emoji_packs")  # картинки на кнопках — тоже в фоне
    await callback(SimpleNamespace(bot_data=application.bot_data))
    ai.self_check.assert_awaited_once()


def test_month_tab_is_checked_daily_and_on_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = build(monkeypatch)
    jobs = {job.name: job for job in app.job_queue.jobs()}
    daily = jobs["month_tab"]
    assert daily.trigger.timezone.key == "Asia/Tashkent"
    assert (
        daily.trigger.fields[5].expressions[0].first,
        daily.trigger.fields[6].expressions[0].first,
    ) == (0, 5)
    assert "month_tab_on_start" in jobs


def test_weekly_digest_runs_on_sunday_evening(monkeypatch: pytest.MonkeyPatch) -> None:
    app = build(monkeypatch, WEEKLY_DIGEST_TIME="20:00")
    job = {job.name: job for job in app.job_queue.jobs()}["weekly_digest"]
    fields = {field.name: str(field) for field in job.trigger.fields}
    assert (fields["day_of_week"], fields["hour"], fields["minute"]) == (
        "sun",
        "20",
        "0",
    )


def test_weekly_digest_can_be_switched_off(monkeypatch: pytest.MonkeyPatch) -> None:
    app = build(monkeypatch, WEEKLY_DIGEST_TIME="off")
    assert "weekly_digest" not in {job.name for job in app.job_queue.jobs()}


def test_morning_brief_runs_every_morning(monkeypatch: pytest.MonkeyPatch) -> None:
    app = build(monkeypatch, MORNING_TIME="08:00")
    job = {job.name: job for job in app.job_queue.jobs()}["morning_brief"]
    fields = {field.name: str(field) for field in job.trigger.fields}
    assert len(fields["day_of_week"].split(",")) == 7  # каждый день
    assert (fields["hour"], fields["minute"]) == ("8", "0")
    assert job.trigger.timezone.key == "Asia/Tashkent"


@pytest.mark.parametrize("value", ["off", "8am"])
def test_morning_brief_can_be_switched_off(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    app = build(monkeypatch, MORNING_TIME=value)
    assert "morning_brief" not in {job.name for job in app.job_queue.jobs()}


async def test_morning_brief_goes_to_the_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[int] = []

    async def send(bot: object, chat_id: int, service: object) -> None:
        sent.append(chat_id)

    monkeypatch.setattr(main.analytics, "send_morning_brief", send)
    monkeypatch.setattr(config, "ANALYTICS_CHAT_ID", None)
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({7}))
    context = SimpleNamespace(bot=object(), bot_data={"analytics_service": object()})
    await main._morning_brief(context)
    assert sent == [7]


def test_icons_and_last_card_reach_the_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in {"ALLOWED_USER_IDS": frozenset({42})}.items():
        monkeypatch.setattr(config, name, value)
    app = main.build_application(
        FakeSheets(),
        CATEGORIES,
        SUBCATEGORIES,
        SOURCES,
        icons={"кофе": "☕"},
        last_source="VISA 9120 UZS",
    )
    assert app.bot_data["icons"] == {"кофе": "☕"}
    assert app.bot_data["last_source"] == "VISA 9120 UZS"


def test_webhook_mode_has_no_updater(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({42}))
    web = main.build_application(
        FakeSheets(), CATEGORIES, SUBCATEGORIES, SOURCES, webhook=True
    )
    polling = main.build_application(FakeSheets(), CATEGORIES, SUBCATEGORIES, SOURCES)
    assert web.updater is None  # обновления приходят на /telegram
    assert polling.updater is not None


@pytest.mark.parametrize(
    ("url", "local", "expected"),
    [
        ("https://budget.onrender.com", False, True),
        ("https://budget.onrender.com", True, False),
        (None, False, False),
    ],
)
def test_web_mode_needs_an_external_url(
    monkeypatch: pytest.MonkeyPatch, url: str | None, local: bool, expected: bool
) -> None:
    monkeypatch.setattr(config, "WEBHOOK_URL", url)
    monkeypatch.setattr(config, "LOCAL_RUN", local)
    assert main.web_mode() is expected


def test_polling_never_takes_over_a_webhook(caplog: pytest.LogCaptureFixture) -> None:
    app, wait = SimpleNamespace(run_polling=Mock()), Mock()
    main.start_polling(app, "https://budget.onrender.com/telegram", False, wait)
    app.run_polling.assert_not_called()  # PTB при старте опроса снял бы вебхук
    wait.assert_called_once()
    assert "polling is off" in caplog.text


@pytest.mark.parametrize(
    ("url", "force"), [("", False), ("https://budget.onrender.com/telegram", True)]
)
def test_polling_starts_without_a_webhook_or_when_forced(url: str, force: bool) -> None:
    app, wait = SimpleNamespace(run_polling=Mock()), Mock()
    main.start_polling(app, url, force, wait)
    app.run_polling.assert_called_once()
    wait.assert_not_called()


def test_serve_runs_one_uvicorn_process_without_access_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}
    monkeypatch.setattr(
        main.uvicorn, "run", lambda web, **options: calls.update(web=web, **options)
    )
    hooks: dict[str, object] = {}

    def lifespan(app: object, url: str, secret: str, post_init: object) -> None:
        hooks.update(url=url, secret=secret, post_init=post_init)

    monkeypatch.setattr(main.server, "telegram_lifespan", lifespan)
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://budget.onrender.com/")
    monkeypatch.setattr(config, "WEBHOOK_SECRET", None)
    monkeypatch.setattr(config, "PORT", 10000)
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({42}))
    app = main.build_application(
        FakeSheets(), CATEGORIES, SUBCATEGORIES, SOURCES, webhook=True
    )
    main.serve(app)
    assert (calls["host"], calls["port"]) == ("0.0.0.0", 10000)
    assert (calls["access_log"], calls["log_config"]) == (False, None)
    # uvicorn без workers читает WEB_CONCURRENCY и при >1 не стартует с объектом app
    assert calls["workers"] == 1
    assert hooks["url"] == "https://budget.onrender.com/telegram"
    # секрет из токена: у двух экземпляров во время деплоя он один и тот же
    assert hooks["secret"] == webhook_secret(config.TELEGRAM_TOKEN)
    assert hooks["post_init"] is main._post_init


def test_build_script_skips_without_the_webapp(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "build_webapp.sh"
    script.parent.mkdir()
    script.write_text(Path("scripts/build_webapp.sh").read_text())
    result = subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0
    assert "No webapp" in result.stdout
