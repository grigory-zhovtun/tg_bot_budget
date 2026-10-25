"""/keyboards включает и выключает клавиатуры групп и карт, /app открывает приложение."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from telegram import InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove

from app import config
from app.chat_state import ChatState
from app.handlers import settings
from app.handlers.common import ANCHOR
from tests.test_messages import CATEGORIES, SOURCES, sent_message

OFF = ChatState()


class Sheets:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.written: list[tuple[int, ChatState]] = []

    def set_chat_state(self, chat_id: int, state: ChatState) -> None:
        if self.fail:
            raise ConnectionError("quota")
        self.written.append((chat_id, state))


def chat(
    state: ChatState = OFF, fail: bool = False
) -> tuple[SimpleNamespace, list[SimpleNamespace]]:
    sent: list[SimpleNamespace] = []

    def send(*args: Any, **kwargs: Any) -> SimpleNamespace:
        sent.append(sent_message())
        return sent[-1]

    context = SimpleNamespace(
        bot_data={
            "gs_service": Sheets(fail),
            "chats": {1: state},
            "categories": CATEGORIES,
            "sources": SOURCES,
            "last_source": "VISA 9090 UZS",
        },
        user_data={},
        bot=SimpleNamespace(),
        chat=SimpleNamespace(id=1, send_message=AsyncMock(side_effect=send)),
    )
    return context, sent


def labels(markup: Any) -> list[list[tuple[str, str, str | None]]]:
    return [
        [(b.text, b.callback_data, b.style) for b in row]
        for row in markup.inline_keyboard
    ]


async def open_settings(context: SimpleNamespace) -> Any:
    update = SimpleNamespace(effective_chat=context.chat, message=SimpleNamespace())
    await settings.keyboards_command(update, context)
    return context.chat.send_message.await_args


async def tap(context: SimpleNamespace, data: str) -> SimpleNamespace:
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=SimpleNamespace(message_id=300, chat_id=1),
    )
    update = SimpleNamespace(callback_query=query, effective_chat=context.chat)
    await settings.keyboards_button(update, context)
    query.answer.assert_awaited_once()
    return query


async def test_keyboards_shows_the_state_and_what_a_tap_does() -> None:
    context, _ = chat()
    call = await open_settings(context)
    assert call.args[0] == (
        "⌨️ Клавиатуры\n⬜ Группы под записью: выключены\n"
        "⬜ Карты внизу экрана: выключены"
    )
    assert labels(call.kwargs["reply_markup"]) == [
        [("Включить группы", "kb:groups:on", "success")],
        [("Включить карты", "kb:cards:on", "success")],
    ]


async def test_switching_groups_on_saves_it_and_shows_the_groups() -> None:
    context, _ = chat()
    query = await tap(context, "kb:groups:on")
    assert context.bot_data["gs_service"].written == [(1, ChatState(groups=True))]
    edit = query.edit_message_text.await_args
    assert "✅ Группы под записью: включены" in edit.args[0]
    assert labels(edit.kwargs["reply_markup"])[0] == [
        ("Выключить группы", "kb:groups:off", None)
    ]
    menu = context.chat.send_message.await_args
    assert menu.args[0] == "💳 VISA 9090 UZS — выберите категорию"
    assert (
        menu.kwargs["reply_markup"]
        .inline_keyboard[0][0]
        .callback_data.startswith("cat_")
    )


async def test_switching_cards_on_sends_the_cards_keyboard() -> None:
    context, sent = chat(ChatState(groups=True))
    await tap(context, "kb:cards:on")
    assert context.bot_data["chats"][1] == ChatState(groups=True, cards=True)
    call = context.chat.send_message.await_args
    assert call.args[0] == "💳 VISA 9090 UZS"
    assert isinstance(call.kwargs["reply_markup"], ReplyKeyboardMarkup)
    assert context.user_data[ANCHOR] == sent[-1].message_id


async def test_switching_cards_off_hides_the_keyboard_and_keeps_that_message() -> None:
    context, sent = chat(ChatState(cards=True, start=90, keep=(95,)))
    context.user_data[ANCHOR] = 99
    await tap(context, "kb:cards:off")
    call = context.chat.send_message.await_args
    assert call.args[0] == "⌨️ Карты скрыты"
    assert isinstance(call.kwargs["reply_markup"], ReplyKeyboardRemove)
    # удалить его нельзя: приложение Telegram могло бы вернуть старую клавиатуру
    hidden = sent[-1].message_id
    assert context.bot_data["chats"][1] == ChatState(start=90, keep=(95, hidden))
    assert ANCHOR not in context.user_data


async def test_switching_groups_off_sends_nothing() -> None:
    context, _ = chat(ChatState(groups=True))
    query = await tap(context, "kb:groups:off")
    assert context.bot_data["chats"][1] == ChatState()
    assert (
        "⬜ Группы под записью: выключены" in query.edit_message_text.await_args.args[0]
    )
    context.chat.send_message.assert_not_awaited()


async def test_a_failed_save_keeps_the_setting_and_says_so() -> None:
    context, _ = chat(fail=True)
    query = await tap(context, "kb:cards:on")
    assert context.bot_data["chats"][1] == ChatState()
    edit = query.edit_message_text.await_args
    assert edit.args[0].endswith("⚠️ Не сохранил, попробуйте ещё раз")
    assert labels(edit.kwargs["reply_markup"])[1][0][0] == "Включить карты"
    context.chat.send_message.assert_not_awaited()


async def test_app_opens_the_page_with_the_telegram_sign_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "WEBAPP_URL", "https://example.test/app/")
    context, _ = chat()
    update = SimpleNamespace(effective_chat=context.chat, message=SimpleNamespace())
    await settings.app_command(update, context)
    call = context.chat.send_message.await_args
    assert call.args[0] == "📱 Приложение бюджета"
    markup: InlineKeyboardMarkup = call.kwargs["reply_markup"]
    [[button]] = markup.inline_keyboard
    assert button.text == "📱 Открыть приложение"
    assert button.web_app.url == "https://example.test/app/"  # без токена: initData


async def test_app_without_the_page(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "WEBAPP_URL", None)
    context, _ = chat()
    update = SimpleNamespace(effective_chat=context.chat, message=SimpleNamespace())
    await settings.app_command(update, context)
    assert context.chat.send_message.await_args.args[0] == "Приложение не настроено"
