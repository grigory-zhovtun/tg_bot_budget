"""Утренняя уборка: вчерашнее с прошлой уборки, кроме сводок недели и клавиатуры."""

from collections import defaultdict
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.error import BadRequest

from app import config
from app.chat_state import ChatState, keep_messages
from app.handlers import cleanup
from app.handlers.common import ANCHOR
from app.message_log import MessageLog
from tests.test_messages import SOURCES, sent_message

TODAY = date(2026, 10, 26)
TASHKENT = ZoneInfo("Asia/Tashkent")


def at(day: int, hour: int) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=TASHKENT)


class Sheets:
    def __init__(self) -> None:
        self.written: list[tuple[int, ChatState]] = []

    def set_chat_state(self, chat_id: int, state: ChatState) -> None:
        self.written.append((chat_id, state))


def application(
    states: dict[int, ChatState], log: MessageLog | None = None
) -> SimpleNamespace:
    def send(*args: Any, **kwargs: Any) -> SimpleNamespace:
        return sent_message()

    return SimpleNamespace(
        bot_data={
            "gs_service": Sheets(),
            "chats": dict(states),
            "chats_loaded": True,
            "message_log": log or MessageLog("Asia/Tashkent"),
            "sources": SOURCES,
            "last_source": "VISA 9090 UZS",
        },
        user_data=defaultdict(dict),  # как у PTB: словарь появляется при обращении
        bot=SimpleNamespace(
            delete_messages=AsyncMock(),
            delete_message=AsyncMock(),
            send_message=AsyncMock(side_effect=send),
        ),
    )


def deleted(app: SimpleNamespace, chat_id: int = 1) -> list[int]:
    return [
        number
        for call in app.bot.delete_messages.await_args_list
        if call.args[0] == chat_id
        for number in call.args[1]
    ]


def logged(*entries: tuple[int, int, datetime]) -> MessageLog:
    log = MessageLog("Asia/Tashkent")
    for chat_id, number, when in entries:
        log.record(chat_id, number, when)
    return log


@pytest.fixture(autouse=True)
def two_users(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({1, 2}))


async def test_yesterday_goes_from_the_start_and_the_start_moves() -> None:
    log = logged((1, 140, at(25, 21)), (1, 141, at(26, 0)), (1, 142, at(26, 7)))
    app = application({1: ChatState(start=100, keep=(103,))}, log)
    app.user_data[1] = {"bot_messages": [130, 142]}
    await cleanup.sweep(app, TODAY)
    assert deleted(app) == [n for n in range(100, 141) if n != 103]
    # после полуночи — ночное «📅 Новый месяц», карточка в 7:00 — остаются
    assert 141 not in deleted(app) and 142 not in deleted(app)
    assert app.bot_data["chats"][1] == ChatState(start=141)
    assert app.bot_data["gs_service"].written == [(1, ChatState(start=141))]
    assert app.user_data[1]["bot_messages"] == [142]


async def test_a_big_backlog_goes_in_batches_of_100() -> None:
    app = application({1: ChatState(start=1)}, logged((1, 250, at(25, 12))))
    await cleanup.sweep(app, TODAY)
    sizes = [len(call.args[1]) for call in app.bot.delete_messages.await_args_list]
    assert sizes == [100, 100, 50]


async def test_nothing_known_from_yesterday_leaves_the_chat() -> None:
    app = application({1: ChatState(start=100)}, logged((1, 141, at(26, 7))))
    await cleanup.sweep(app, TODAY)
    app.bot.delete_messages.assert_not_awaited()
    assert app.bot_data["chats"][1] == ChatState(start=100)


async def test_without_a_start_it_only_remembers_where_to_begin() -> None:
    app = application({}, logged((2, 60, at(25, 12))))
    await cleanup.sweep(app, TODAY)
    app.bot.delete_messages.assert_not_awaited()
    assert app.bot_data["chats"][2] == ChatState(start=61)


async def test_todays_cards_keyboard_message_stays_without_a_new_one() -> None:
    app = application(
        {1: ChatState(cards=True, start=100)}, logged((1, 130, at(25, 9)))
    )
    app.user_data[1] = {ANCHOR: 135}  # утренняя сводка с клавиатурой — после полуночи
    await cleanup.sweep(app, TODAY)
    assert deleted(app) == list(range(100, 131))
    app.bot.send_message.assert_not_awaited()


async def test_yesterdays_cards_keyboard_message_is_replaced_first() -> None:
    app = application({2: ChatState(cards=True, start=50)}, logged((2, 60, at(25, 9))))
    app.user_data[2] = {ANCHOR: 55}
    order: list[str] = []

    def anchor(*args: Any, **kwargs: Any) -> SimpleNamespace:
        order.append("anchor")
        return sent_message()

    async def delete(chat_id: int, numbers: list[int]) -> None:
        order.append("delete")

    app.bot.send_message.side_effect = anchor
    app.bot.delete_messages.side_effect = delete
    await cleanup.sweep(app, TODAY)
    assert order == ["anchor", "delete"]  # клавиатура не пропадает с экрана
    assert 55 in deleted(app, 2)  # вчерашняя карточка не копится
    assert app.bot.send_message.await_args.kwargs["disable_notification"] is True
    assert app.user_data[2][ANCHOR] > 60


async def test_a_keep_added_during_the_sweep_survives() -> None:
    app = application({1: ChatState(start=100)}, logged((1, 140, at(25, 21))))

    async def busy(chat_id: int, numbers: list[int]) -> None:
        await keep_messages(app.bot_data, 1, [150])  # «⌨️ Карты скрыты» в 8:00:01

    app.bot.delete_messages.side_effect = busy
    await cleanup.sweep(app, TODAY)
    assert app.bot_data["chats"][1] == ChatState(start=141, keep=(150,))


async def test_an_unknown_cards_anchor_is_sent_again_quietly() -> None:
    app = application({2: ChatState(cards=True, start=50)}, logged((2, 60, at(25, 9))))
    await cleanup.sweep(app, TODAY)
    assert deleted(app, 2) == list(range(50, 61))
    call = app.bot.send_message.await_args
    assert call.args[:2] == (2, "💳 VISA 9090 UZS")
    assert isinstance(call.kwargs["reply_markup"], ReplyKeyboardMarkup)
    assert call.kwargs["disable_notification"] is True
    assert app.user_data[2][ANCHOR] > 60


async def test_a_refused_batch_goes_one_by_one_and_the_next_chat_is_swept() -> None:
    log = logged((1, 103, at(25, 9)), (2, 62, at(25, 9)))
    app = application({1: ChatState(start=100), 2: ChatState(start=60)}, log)

    async def refuse(chat_id: int, numbers: list[int]) -> None:
        if chat_id == 1:
            raise BadRequest("Message can't be deleted")

    app.bot.delete_messages.side_effect = refuse
    app.bot.delete_message.side_effect = [None, BadRequest("old"), None, None]
    await cleanup.sweep(app, TODAY)
    one_by_one = [call.args for call in app.bot.delete_message.await_args_list]
    assert one_by_one == [(1, 100), (1, 101), (1, 102), (1, 103)]
    assert deleted(app, 2) == [60, 61, 62]
    assert app.bot_data["chats"][1].start == 104
    assert app.bot_data["chats"][2].start == 63


async def test_messages_kept_in_another_chat_are_spared_too() -> None:
    # номера сообщений у бота общие для всех личных чатов: чужая сводка недели,
    # «⌨️ Карты скрыты» или клавиатура карт могут оказаться в диапазоне этого чата
    states = {
        1: ChatState(start=100),
        2: ChatState(cards=True, start=200, keep=(110,)),
    }
    app = application(states, logged((1, 130, at(25, 21))))
    app.user_data[2] = {ANCHOR: 120}
    await cleanup.clean(app, 1, TODAY)
    assert 110 not in deleted(app) and 120 not in deleted(app)
    assert 111 in deleted(app)


async def test_the_weekly_digest_is_kept_through_the_sweep() -> None:
    app = application({1: ChatState(start=100)}, logged((1, 152, at(25, 22))))
    await cleanup.keep(app, 1, [150])
    assert app.bot_data["gs_service"].written[-1] == (
        1,
        ChatState(start=100, keep=(150,)),
    )
    await cleanup.sweep(app, TODAY)
    assert 150 not in deleted(app) and 151 in deleted(app)
    assert app.bot_data["chats"][1] == ChatState(start=153)  # дальше — не тронет


async def test_incoming_messages_go_to_the_log() -> None:
    log = MessageLog("Asia/Tashkent")
    message = SimpleNamespace(chat_id=1, message_id=77, date=at(25, 9))
    context = SimpleNamespace(bot_data={"message_log": log})
    await cleanup.observe(SimpleNamespace(message=message), context)
    await cleanup.observe(SimpleNamespace(message=None), context)  # нажатие кнопки
    assert log.last_before(1, TODAY) == 77


async def test_first_run_hides_the_old_keyboard_once() -> None:
    app = application({2: ChatState(groups=True, start=10)})
    sent: list[SimpleNamespace] = []

    def send(*args: Any, **kwargs: Any) -> SimpleNamespace:
        sent.append(sent_message())
        return sent[-1]

    app.bot.send_message.side_effect = send
    await cleanup.first_run(SimpleNamespace(application=app, bot=app.bot))
    [call] = app.bot.send_message.await_args_list
    assert call.args[0] == 1  # у второго строка уже есть
    assert call.args[1] == (
        "⌨️ Клавиатуры скрыты\n"
        "Чтобы чат был чище, кнопки групп и карт больше не показываются.\n"
        "Включить — в меню: /keyboards. Приложение — /app."
    )
    assert isinstance(call.kwargs["reply_markup"], ReplyKeyboardRemove)
    # само сообщение остаётся: уборка начинается со следующего номера
    assert app.bot_data["chats"][1] == ChatState(start=sent[0].message_id + 1)

    await cleanup.first_run(SimpleNamespace(application=app, bot=app.bot))
    assert len(app.bot.send_message.await_args_list) == 1


async def test_first_run_starts_every_new_chat_after_all_the_notes() -> None:
    app = application({})
    sent: list[SimpleNamespace] = []

    def send(*args: Any, **kwargs: Any) -> SimpleNamespace:
        sent.append(sent_message())
        return sent[-1]

    app.bot.send_message.side_effect = send
    await cleanup.first_run(SimpleNamespace(application=app, bot=app.bot))
    assert [call.args[0] for call in app.bot.send_message.await_args_list] == [1, 2]
    # номера общие для всех чатов: уборка ни одного чата не начнётся раньше
    # чьего-то первого сообщения — иначе оно попало бы в чужой диапазон
    after = sent[-1].message_id + 1
    assert app.bot_data["chats"] == {
        1: ChatState(start=after),
        2: ChatState(start=after),
    }


async def test_first_run_waits_for_the_sheet() -> None:
    app = application({})
    app.bot_data["chats_loaded"] = False
    await cleanup.first_run(SimpleNamespace(application=app, bot=app.bot))
    app.bot.send_message.assert_not_awaited()
