"""Кнопки: иконки подкатегорий, по две в ряд, подписи с картой, цвет у действий."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from telegram.constants import KeyboardButtonStyle

from app.handlers import transactions
from app.utils import keyboards
from tests.test_messages import SOURCES, SUBCATEGORIES

ICONS = {"кофе": "☕", "кафе": "🍽️"}


def labels(markup) -> list[list[str]]:
    return [[button.text for button in row] for row in markup.inline_keyboard]


def test_subcategories_get_icons_and_keep_their_callback_names() -> None:
    markup = keyboards.generate_subcategories_keyboard(
        {"🍔 ЕДА": ["кофе", "кафе", "ресторан"]}, "🍔 ЕДА", ICONS
    )
    assert labels(markup) == [["☕ кофе", "🍽️ кафе"], ["ресторан"], ["⬅️ Назад"]]
    assert [b.callback_data for b in markup.inline_keyboard[0]] == [
        "sub_кофе",
        "sub_кафе",
    ]


def test_categories_go_two_per_row() -> None:
    markup = keyboards.generate_categories_keyboard(["🏚️ ДОМ", "🍔 ЕДА", "🚧 РАЗНОЕ"])
    assert labels(markup) == [["🏚️ ДОМ", "🍔 ЕДА"], ["🚧 РАЗНОЕ"]]


def test_prompts_name_the_card_and_the_choice() -> None:
    # кнопки под сообщением шириной с само сообщение: короткое «Подкатегория:» их сужало
    assert keyboards.category_prompt("VISA 9120 UZS") == (
        "💳 VISA 9120 UZS — выберите категорию"
    )
    assert keyboards.subcategory_prompt("VISA 9120 UZS", "🍔 ЕДА") == (
        "💳 VISA 9120 UZS · 🍔 ЕДА — выберите подкатегорию"
    )
    assert keyboards.amount_prompt("VISA 9120 UZS", "🍔 ЕДА", "кофе", ICONS) == (
        "💳 VISA 9120 UZS · 🍔 ЕДА · ☕ кофе\n"
        "Введите сумму и комментарий, например «48000 латте»"
    )
    assert keyboards.with_icon("такси", ICONS) == "такси"


def tap(
    data: str, user_data: dict, **bot_data
) -> tuple[SimpleNamespace, SimpleNamespace]:
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=SimpleNamespace(reply_text=AsyncMock(), chat_id=1),
    )
    context = SimpleNamespace(
        bot_data={"subcategories": SUBCATEGORIES, "icons": ICONS, **bot_data},
        user_data=user_data,
    )
    return SimpleNamespace(callback_query=query), context


async def test_category_without_a_card_takes_the_card_of_the_last_record() -> None:
    update, context = tap("cat_🍔 ЕДА", {}, last_source="VISA 9120 UZS")
    await transactions.transaction_button_handler(update, context)

    query = update.callback_query
    query.message.reply_text.assert_not_awaited()  # без «⚠️ Выберите источник»
    assert context.user_data == {"category": "🍔 ЕДА", "source": "VISA 9120 UZS"}
    call = query.edit_message_text.await_args
    assert call.kwargs["text"] == "💳 VISA 9120 UZS · 🍔 ЕДА — выберите подкатегорию"
    assert labels(call.kwargs["reply_markup"])[0] == ["☕ кофе", "🍽️ кафе"]


async def test_category_with_no_card_at_all_still_asks_for_one() -> None:
    update, context = tap("cat_🍔 ЕДА", {}, sources=["VISA 9120 UZS"])
    await transactions.transaction_button_handler(update, context)
    assert update.callback_query.message.reply_text.await_args.kwargs["text"] == (
        "⚠️ Выберите источник"
    )


async def test_subcategory_asks_for_the_amount_with_an_example() -> None:
    state = {"source": "VISA 9120 UZS", "category": "🍔 ЕДА"}
    update, context = tap("sub_кофе", state)
    await transactions.transaction_button_handler(update, context)
    assert context.user_data["subcategory"] == "кофе"
    assert update.callback_query.edit_message_text.await_args.kwargs["text"] == (
        keyboards.amount_prompt("VISA 9120 UZS", "🍔 ЕДА", "кофе", ICONS)
    )


def test_action_buttons_are_coloured() -> None:
    assert keyboards.action("✅ Записать", "import:ok:1", "success").style == (
        KeyboardButtonStyle.SUCCESS
    )


def test_app_button_is_the_last_row_of_the_cards_keyboard() -> None:
    url = "https://budget.onrender.com/app/?launch=42.1.sig"
    rows = keyboards.generate_sources_keyboard(SOURCES, "VISA 9120 UZS", url).keyboard
    [button] = rows[-1]
    assert (button.text, button.web_app.url) == (keyboards.APP_BUTTON, url)
    assert button.style == KeyboardButtonStyle.PRIMARY
    assert rows[0][0].text == "✅ VISA 9120 UZS"


def test_no_app_button_without_the_page() -> None:
    rows = keyboards.generate_sources_keyboard(SOURCES).keyboard
    assert all(button.web_app is None for row in rows for button in row)
