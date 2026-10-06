from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, Conflict

from app.errors import MAX_LENGTH, on_error, user_message
from app.handlers import analytics

FAKE_TOKEN = "1234567:" + "A" * 35


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ValueError("bad amount"), "bad amount"),
        (ConnectionError(f"POST https://api.telegram.org/bot{FAKE_TOKEN}/x"), None),
        (
            RuntimeError("API key AIza" + "B" * 30 + " not valid"),
            "API key ••• not valid",
        ),
        (RuntimeError("https://x/?key=secret123&a=1"), "https://x/?•••&a=1"),
        (RuntimeError("first line\nsecond line"), "first line"),
        (RuntimeError(""), "RuntimeError"),
    ],
)
def test_user_message_hides_secrets(error: Exception, expected: str | None) -> None:
    text = user_message(error)
    assert FAKE_TOKEN not in text
    if expected is not None:
        assert text == expected


def test_user_message_is_short() -> None:
    assert len(user_message(RuntimeError("x" * 1000))) == MAX_LENGTH


async def test_conflict_during_deploy_is_only_a_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    await on_error(
        None, SimpleNamespace(error=Conflict("terminated by other getUpdates"))
    )
    assert [r.levelname for r in caplog.records] == ["WARNING"]


async def test_other_errors_are_logged_with_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    await on_error(None, SimpleNamespace(error=RuntimeError("boom")))
    [record] = caplog.records
    assert record.levelname == "ERROR" and record.exc_info


@pytest.mark.parametrize(
    ("text", "limit", "expected"),
    [
        ("a\nb\nc\n", 4, ["a\nb\n", "c\n"]),
        ("abcdef", 4, ["abcd", "ef"]),
        ("", 4, []),
        ("short", 4000, ["short"]),
    ],
)
def test_split_text(text: str, limit: int, expected: list[str]) -> None:
    assert analytics.split_text(text, limit) == expected


async def test_broken_markdown_falls_back_per_chunk_without_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(analytics, "CHUNK_SIZE", 10)
    sent: list[tuple[str, str | None]] = []

    async def send(text: str, parse_mode: str | None = None) -> None:
        if parse_mode == "Markdown" and "*" in text:
            raise BadRequest("Can't parse entities")
        sent.append((text, parse_mode))

    await analytics.send_markdown(send, "line one\n*bold\nline 3\n")
    assert sent == [
        ("line one\n", "Markdown"),
        ("*bold\n", None),
        ("line 3\n", "Markdown"),
    ]


class FakeAnalytics:
    def __init__(self) -> None:
        self.charts = [BytesIO(b"pie"), BytesIO(b"bars")]

    def generate_3day_report(self) -> tuple[str, list[BytesIO]]:
        return "📊 *Отчёт*", self.charts


async def test_analytics_command_sends_report_and_closes_charts() -> None:
    service = FakeAnalytics()
    status = SimpleNamespace(delete=AsyncMock(), edit_text=AsyncMock())
    message = SimpleNamespace(
        reply_text=AsyncMock(return_value=status), reply_photo=AsyncMock()
    )
    update = SimpleNamespace(message=message)
    context = SimpleNamespace(bot_data={"analytics_service": service})

    await analytics.analytics_command(update, context)

    status.delete.assert_awaited_once()
    assert message.reply_text.await_args_list[-1].args == ("📊 *Отчёт*",)
    captions = [c.kwargs["caption"] for c in message.reply_photo.await_args_list]
    assert captions == list(analytics.CAPTIONS)
    assert all(chart.closed for chart in service.charts)


async def test_analytics_failure_is_shown_in_status_message() -> None:
    class Broken:
        def generate_3day_report(self):
            raise RuntimeError("Google is down")

    status = SimpleNamespace(delete=AsyncMock(), edit_text=AsyncMock())
    message = SimpleNamespace(reply_text=AsyncMock(return_value=status))
    await analytics.analytics_command(
        SimpleNamespace(message=message),
        SimpleNamespace(bot_data={"analytics_service": Broken()}),
    )
    assert "Google is down" in status.edit_text.await_args.args[0]


async def test_daily_report_goes_to_owner_chat() -> None:
    bot = SimpleNamespace(send_message=AsyncMock(), send_photo=AsyncMock())
    await analytics.send_daily_analytics(bot, 42, FakeAnalytics())
    assert bot.send_message.await_args.args == (42, "📊 *Отчёт*")
    assert bot.send_photo.await_count == 2
