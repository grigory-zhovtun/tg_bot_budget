"""Отправка сводки: расширенным сообщением, а при отказе Telegram — обычным текстом."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, EndPointNotFound, TimedOut

from app import config
from app.handlers.analytics import send_report
from app.rich import Report

REPORT = Report("x", "<p>x</p>")
RICH = {"html": "<p>x</p>", "skip_entity_detection": True}


def fake_bot(rich=None, text=None) -> SimpleNamespace:
    return SimpleNamespace(
        do_api_request=AsyncMock(side_effect=rich),
        send_message=AsyncMock(side_effect=text),
    )


async def test_rich_goes_first_with_the_effect() -> None:
    bot = fake_bot()
    assert await send_report(bot, 7, REPORT, "42") == "rich"
    bot.do_api_request.assert_awaited_once_with(
        "sendRichMessage",
        api_kwargs={"chat_id": 7, "rich_message": RICH, "message_effect_id": "42"},
    )
    bot.send_message.assert_not_awaited()


async def test_a_refused_effect_is_dropped_before_the_text() -> None:
    bot = fake_bot(rich=[BadRequest("EFFECT_ID_INVALID"), None])
    assert await send_report(bot, 7, REPORT, "42") == "rich"
    assert bot.do_api_request.await_args.kwargs["api_kwargs"] == {
        "chat_id": 7,
        "rich_message": RICH,
    }


async def test_refused_rich_falls_back_to_the_text_with_the_effect() -> None:
    bot = fake_bot(rich=[BadRequest("bad"), BadRequest("bad")])
    assert await send_report(bot, 7, REPORT, "42") == "text"
    bot.send_message.assert_awaited_once_with(7, "x", message_effect_id="42")


async def test_no_method_without_effect_sends_the_text_once() -> None:
    bot = fake_bot(rich=[EndPointNotFound("Not Found")])
    assert await send_report(bot, 7, REPORT) == "text"
    assert bot.do_api_request.await_count == 1
    bot.send_message.assert_awaited_once_with(7, "x")


async def test_text_only_reports_and_the_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = fake_bot()
    assert await send_report(bot, 7, Report("x")) == "text"
    monkeypatch.setattr(config, "RICH_MESSAGES", False)
    assert await send_report(bot, 7, REPORT) == "text"
    bot.do_api_request.assert_not_awaited()
    assert bot.send_message.await_count == 2


async def test_a_timeout_is_not_sent_twice() -> None:
    bot = fake_bot(rich=[TimedOut()])
    with pytest.raises(TimedOut):
        await send_report(bot, 7, REPORT, "42")
    bot.send_message.assert_not_awaited()


async def test_long_text_is_split_and_only_the_first_part_has_the_effect() -> None:
    bot = fake_bot(rich=[BadRequest("bad"), BadRequest("bad")])
    long = Report("а" * 3000 + "\n" + "б" * 3000, "<p>x</p>")
    await send_report(bot, 7, long, "42")
    first, second = bot.send_message.await_args_list
    assert first.kwargs == {"message_effect_id": "42"} and second.kwargs == {}


async def test_the_last_refusal_is_raised() -> None:
    bot = fake_bot(rich=[BadRequest("bad")], text=[BadRequest("bad")])
    with pytest.raises(BadRequest):
        await send_report(bot, 7, REPORT)
