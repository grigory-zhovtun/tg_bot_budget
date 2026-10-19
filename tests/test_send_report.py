"""Отправка сводки: расширенным сообщением, а при отказе Telegram — обычным текстом."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import Message
from telegram.error import BadRequest, EndPointNotFound, TimedOut

from app import config
from app.handlers.analytics import Sent, edit_report, send_report
from app.rich import Report

pytestmark = pytest.mark.usefixtures("rich")


REPORT = Report("x", "<p>x</p>")
RICH = {"html": "<p>x</p>", "skip_entity_detection": True}


def fake_bot(rich=None, text=None) -> SimpleNamespace:
    return SimpleNamespace(
        do_api_request=AsyncMock(side_effect=rich),
        send_message=AsyncMock(side_effect=text),
    )


async def test_rich_goes_first_with_the_effect() -> None:
    bot = fake_bot()
    assert (await send_report(bot, 7, REPORT, "42")).kind == "rich"
    bot.do_api_request.assert_awaited_once_with(
        "sendRichMessage",
        api_kwargs={"chat_id": 7, "rich_message": RICH, "message_effect_id": "42"},
        return_type=Message,
    )
    bot.send_message.assert_not_awaited()


async def test_a_refused_effect_is_dropped_before_the_text() -> None:
    bot = fake_bot(rich=[BadRequest("EFFECT_ID_INVALID"), None])
    assert (await send_report(bot, 7, REPORT, "42")).kind == "rich"
    assert bot.do_api_request.await_args.kwargs["api_kwargs"] == {
        "chat_id": 7,
        "rich_message": RICH,
    }


async def test_refused_rich_falls_back_to_the_text_with_the_effect() -> None:
    bot = fake_bot(rich=[BadRequest("bad"), BadRequest("bad")])
    assert (await send_report(bot, 7, REPORT, "42")).kind == "text"
    bot.send_message.assert_awaited_once_with(7, "x", message_effect_id="42")


async def test_no_method_without_effect_sends_the_text_once() -> None:
    bot = fake_bot(rich=[EndPointNotFound("Not Found")])
    assert (await send_report(bot, 7, REPORT)).kind == "text"
    assert bot.do_api_request.await_count == 1
    bot.send_message.assert_awaited_once_with(7, "x")


async def test_text_only_reports_and_the_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = fake_bot()
    assert (await send_report(bot, 7, Report("x"))).kind == "text"
    monkeypatch.setattr(config, "RICH_MESSAGES", False)
    assert (await send_report(bot, 7, REPORT)).kind == "text"
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


async def test_rich_carries_the_keyboard_and_returns_the_message() -> None:
    message, markup = object(), object()
    bot = fake_bot(rich=[message])
    sent = await send_report(bot, 7, REPORT, reply_markup=markup)
    assert sent == Sent("rich", (message,))
    assert bot.do_api_request.await_args.kwargs["api_kwargs"]["reply_markup"] is markup


async def test_text_keeps_the_keyboard_on_the_last_part() -> None:
    first, second, markup = object(), object(), object()
    bot = fake_bot(rich=[BadRequest("bad"), BadRequest("bad")], text=[first, second])
    long = Report("а" * 3000 + "\n" + "б" * 3000, "<p>x</p>")
    sent = await send_report(bot, 7, long, "42", reply_markup=markup)
    one, two = bot.send_message.await_args_list
    assert one.kwargs == {"message_effect_id": "42"}
    assert two.kwargs == {"reply_markup": markup}
    assert sent == Sent("text", (first, second))


async def test_edit_tries_rich_then_text() -> None:
    bot = SimpleNamespace(
        do_api_request=AsyncMock(side_effect=[BadRequest("bad")]),
        edit_message_text=AsyncMock(),
    )
    assert await edit_report(bot, 7, 99, REPORT) == "text"
    assert bot.do_api_request.await_args.kwargs["api_kwargs"] == {
        "chat_id": 7,
        "message_id": 99,
        "rich_message": RICH,
    }
    bot.edit_message_text.assert_awaited_once_with(chat_id=7, message_id=99, text="x")


async def test_edit_keeps_the_keyboard_when_given() -> None:
    markup = object()
    bot = SimpleNamespace(do_api_request=AsyncMock(), edit_message_text=AsyncMock())
    assert await edit_report(bot, 7, 99, REPORT, reply_markup=markup) == "rich"
    assert bot.do_api_request.await_args.kwargs["api_kwargs"]["reply_markup"] is markup
    bot.edit_message_text.assert_not_awaited()
