"""Чистый чат: клавиатуры групп и карт — по настройкам /keyboards, кнопки записи —
на самой карточке, когда обе выключены."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.error import BadRequest

from app.chat_state import ChatState
from app.handlers import common, last_write, messages
from app.handlers.common import ANCHOR, LAST_WRITE
from tests.test_messages import (
    again,
    chat_messages,
    make_chat,
    manual_state,
    sent_message,
)

OFF = ChatState()
CARD_ACTIONS = [
    ("✏️ Исправить запись", "card:fix"),
    ("↩️ Отменить запись", "card:undo"),
]
MENU_ACTIONS = [
    ("✏️ Исправить запись", "last:fix"),
    ("↩️ Отменить запись", "last:undo"),
]


def chat(
    state: ChatState = OFF,
    text: str = "48000 латте",
    user_data: dict[str, Any] | None = None,
) -> tuple[SimpleNamespace, SimpleNamespace, Any, list[SimpleNamespace]]:
    """Чат 1 с настройками state; sent — сообщения бота по порядку (с номерами)."""
    update, context, sheets = make_chat(
        text, manual_state() if user_data is None else user_data
    )
    context.bot_data["chats"] = {1: state}
    sent: list[SimpleNamespace] = []

    def send(*args: Any, **kwargs: Any) -> SimpleNamespace:
        sent.append(sent_message())
        return sent[-1]

    update.effective_chat.send_message.side_effect = send
    update.message.reply_text.side_effect = send
    return update, context, sheets, sent


async def record(state: ChatState = OFF) -> tuple[Any, Any, Any, list[Any]]:
    update, context, sheets, sent = chat(state)
    await messages.text_handler(update, context)
    return update, context, sheets, sent


def buttons(markup: Any) -> list[list[tuple[str, str]]]:
    return [[(b.text, b.callback_data) for b in row] for row in markup.inline_keyboard]


async def press(
    context: SimpleNamespace, data: str, message_id: int = 500, refuse: bool = False
) -> tuple[SimpleNamespace, SimpleNamespace]:
    """Нажатие кнопки записи на сообщении message_id; update.effective_chat — куда
    бот пишет ответ; refuse — Telegram не меняет кнопки этого сообщения."""
    markup_edit = AsyncMock(side_effect=BadRequest("Message can't be edited"))
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        edit_message_reply_markup=markup_edit if refuse else AsyncMock(),
        message=SimpleNamespace(
            message_id=message_id,
            chat_id=1,
            delete=AsyncMock(),
            reply_text=AsyncMock(side_effect=lambda *a, **k: sent_message()),
        ),
    )
    update = SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(
            id=1, send_message=AsyncMock(side_effect=lambda *a, **k: sent_message())
        ),
    )
    await last_write.button(update, context)
    return update, query


def new_markup(query: SimpleNamespace) -> Any:
    return query.edit_message_reply_markup.await_args.kwargs["reply_markup"]


async def test_without_keyboards_the_record_buttons_sit_on_the_card() -> None:
    update, context, _, sent = await record()
    [card] = chat_messages(update)  # одно сообщение: ни карт, ни групп
    assert card.args[0].startswith("✅ 48 000 UZS • 🍔 ЕДА (кофе)")
    assert buttons(card.kwargs["reply_markup"]) == [CARD_ACTIONS]
    last = context.user_data[LAST_WRITE]
    assert last["on_card"] is True and last["groups"] is False
    assert last["menu"] == (1, sent[0].message_id)
    assert ANCHOR not in context.user_data


async def test_several_flagged_rows_put_the_check_button_on_the_card() -> None:
    update, context, _, _ = chat(user_data={"source": "VISA 9090 UZS"})
    await common.show_main_menu(update.effective_chat, context, "✅", True, check=2)
    [card] = chat_messages(update)
    assert buttons(card.kwargs["reply_markup"]) == [
        [("🔎 Проверить (2)", "card:check")],
        CARD_ACTIONS,
    ]


async def test_one_flagged_row_is_asked_below_and_the_card_keeps_the_actions() -> None:
    update, context, _, sent = chat(user_data={"source": "VISA 9090 UZS"})
    check = InlineKeyboardMarkup([[InlineKeyboardButton("✓ Верно", callback_data="x")]])
    menu = await common.show_main_menu(
        update.effective_chat, context, "✅", True, check=1, menu=("❓ кафе?", check)
    )
    card, question = chat_messages(update)
    assert buttons(card.kwargs["reply_markup"]) == [CARD_ACTIONS]  # без «🔎»
    assert question.args[0] == "❓ кафе?"
    assert menu is sent[0]  # кнопки записи — на карточке


async def test_cards_only_the_card_carries_the_keyboard_and_the_menu_the_actions() -> (
    None
):
    update, context, _, sent = await record(ChatState(cards=True))
    card, menu = chat_messages(update)
    assert isinstance(card.kwargs["reply_markup"], ReplyKeyboardMarkup)
    assert context.user_data[ANCHOR] == sent[0].message_id
    assert menu.args[0] == "💳 VISA 9090 UZS"
    assert buttons(menu.kwargs["reply_markup"]) == [MENU_ACTIONS]
    assert context.user_data[LAST_WRITE]["on_card"] is False


async def test_groups_only_the_menu_asks_for_a_category() -> None:
    update, context, _, _ = await record(ChatState(groups=True))
    card, menu = chat_messages(update)
    assert card.kwargs.get("reply_markup") is None  # клавиатуры карт нет
    assert menu.args[0] == "💳 VISA 9090 UZS — выберите категорию"
    rows = buttons(menu.kwargs["reply_markup"])
    assert rows[0] == MENU_ACTIONS and rows[1][0][1].startswith("cat_")
    assert ANCHOR not in context.user_data


async def test_the_wifes_settings_leave_the_owners_chat_alone() -> None:
    update, context, _, _ = chat()
    context.bot_data["chats"][2] = ChatState(groups=True, cards=True)
    await messages.text_handler(update, context)
    [card] = chat_messages(update)
    assert buttons(card.kwargs["reply_markup"]) == [CARD_ACTIONS]


async def test_undo_on_the_card_swaps_only_the_buttons() -> None:
    _, context, sheets, sent = await record()
    card = sent[0].message_id
    _, query = await press(context, "card:undo", card)
    assert buttons(new_markup(query)) == [
        [("🗑 Да, удалить запись", "card:undo:yes"), ("Нет", "card:undo:no")]
    ]
    query.edit_message_text.assert_not_awaited()  # текст карточки цел
    assert not hasattr(sheets, "undone")

    _, query = await press(context, "card:undo:no", card)
    assert buttons(new_markup(query)) == [CARD_ACTIONS]


async def test_undo_yes_on_the_card_deletes_the_card_and_says_so() -> None:
    _, context, sheets, sent = await record()
    update, query = await press(context, "card:undo:yes", sent[0].message_id)
    assert sheets.undone[:2] == (4169, 4169)
    query.message.delete.assert_awaited_once()
    note = update.effective_chat.send_message.await_args
    assert note.args[0] == "↩️ Удалил из таблицы: строка 4169."
    assert LAST_WRITE not in context.user_data


async def test_undo_that_changed_rows_block_drops_the_buttons() -> None:
    _, context, sheets, sent = await record()
    sheets.undo_allowed = False
    update, query = await press(context, "card:undo:yes", sent[0].message_id)
    query.message.delete.assert_not_awaited()
    assert new_markup(query) is None  # отменять больше нечего
    note = update.effective_chat.send_message.await_args.args[0]
    assert note.startswith("В таблице строка 4169 уже изменились")


async def test_card_buttons_after_a_restart_only_say_so() -> None:
    _, context, _, _ = await record()
    context.user_data.pop(LAST_WRITE)
    _, query = await press(context, "card:undo")
    query.answer.assert_awaited_once_with("Последней записи уже нет", show_alert=True)
    assert new_markup(query) is None
    query.edit_message_text.assert_not_awaited()


async def test_card_fix_opens_the_same_dialog_as_the_command() -> None:
    _, context, _, sent = await record()
    _, query = await press(context, "card:fix", sent[0].message_id)
    text = query.message.reply_text.await_args.args[0]
    assert text.startswith("✏️ 48 000 UZS • латте • 🍔 ЕДА (кофе)")
    query.edit_message_reply_markup.assert_not_awaited()


async def test_buttons_on_an_older_card_touch_nothing() -> None:
    update, context, sheets, sent = await record()
    older = sent[0].message_id
    context.user_data.update(manual_state())
    second, _ = again(update, context, "52000 капучино")
    await messages.text_handler(second, context)
    assert context.user_data[LAST_WRITE]["first"] == 4170
    for data in ("card:undo:yes", "card:undo", "card:fix", "card:check"):
        _, query = await press(context, data, older)
        query.answer.assert_awaited_once_with(
            "Кнопки работают только у последней записи", show_alert=True
        )
        assert new_markup(query) is None
        query.message.reply_text.assert_not_awaited()
        query.message.delete.assert_not_awaited()
    assert not hasattr(sheets, "undone")  # последняя запись цела


async def test_when_telegram_keeps_the_buttons_undo_asks_in_a_message() -> None:
    _, context, sheets, sent = await record()
    _, query = await press(context, "card:undo", sent[0].message_id, refuse=True)
    call = query.message.reply_text.await_args
    assert call.args[0] == (
        "↩️ Удалить строку 4169: 48 000 UZS • 🍔 ЕДА (кофе) • VISA 9090 UZS?"
    )
    assert buttons(call.kwargs["reply_markup"]) == [
        [("🗑 Да, удалить", "last:undo:yes"), ("Нет", "last:undo:no")]
    ]
    assert not hasattr(sheets, "undone")


async def test_no_after_undo_without_groups_brings_back_only_the_actions() -> None:
    _, context, _, _ = await record(ChatState(cards=True))
    _, query = await press(context, "last:undo:no")
    call = query.edit_message_text.await_args
    assert call.args[0] == "💳 VISA 9090 UZS"
    assert buttons(call.kwargs["reply_markup"]) == [MENU_ACTIONS]


async def test_start_without_keyboards_says_where_they_are() -> None:
    update, context, _, _ = chat(user_data={"source": "VISA 9090 UZS"})
    update.effective_user = SimpleNamespace(id=1, first_name="Аня")
    await common.start(update, context)
    [hello] = update.message.reply_text.await_args_list
    assert hello.args[0] == (
        "👋 Аня\nПришлите SMS банка, скриншот или выписку — запишу.\n"
        "Клавиатуры — /keyboards, приложение — /app."
    )
    assert hello.kwargs.get("reply_markup") is None
    assert ANCHOR not in context.user_data


async def test_start_with_both_keyboards_shows_the_cards_and_the_groups() -> None:
    state = ChatState(groups=True, cards=True)
    update, context, _, sent = chat(state, user_data={"source": "VISA 9090 UZS"})
    update.effective_user = SimpleNamespace(id=1, first_name="Аня")
    await common.start(update, context)
    hello, menu = update.message.reply_text.await_args_list
    assert isinstance(hello.kwargs["reply_markup"], ReplyKeyboardMarkup)
    assert context.user_data[ANCHOR] == sent[0].message_id
    assert menu.args[0] == "💳 VISA 9090 UZS — выберите категорию"


async def test_picking_a_card_without_groups_shows_no_groups() -> None:
    update, context, _, sent = chat(
        ChatState(cards=True), text="UZCARD 5656 UZS", user_data={}
    )
    await messages.text_handler(update, context)
    [card] = chat_messages(update)
    assert card.args[0] == "💳 UZCARD 5656 UZS"
    assert isinstance(card.kwargs["reply_markup"], ReplyKeyboardMarkup)
    assert context.user_data[ANCHOR] == sent[0].message_id
