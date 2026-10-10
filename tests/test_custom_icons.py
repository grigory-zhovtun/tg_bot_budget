"""Картинки на кнопках (кастомные эмодзи, Bot API 9.4): наборы, подбор, Premium."""

from types import SimpleNamespace
from typing import Any

from telegram import MessageEntity

from app import config, main
from app.custom_icons import EMPTY, CustomIcons, leading_emoji, match, pack_map
from app.handlers import icons, messages
from app.handlers.common import premium_icons
from app.utils import keyboards
from tests.test_google_sheets import RangeWorksheet, make_service
from tests.test_messages import make_chat

ICONS = {"кофе": "☕", "кафе": "🍽️", "такси": "🚕"}
GROUPS = ["🍔 ЕДА", "🚗 ТРАНСПОРТ", "КНИГИ"]


def sticker(emoji: str, custom_id: str, set_name: str = "FoodPack") -> SimpleNamespace:
    return SimpleNamespace(emoji=emoji, custom_emoji_id=custom_id, set_name=set_name)


FOOD = [
    sticker("☕", "101"),
    sticker("🍽", "102"),
    sticker("🍔", "103"),
    sticker("☕", "999"),
]


def test_pack_is_matched_by_the_plain_emoji() -> None:
    assert leading_emoji("🍔 ЕДА") == ("🍔", "ЕДА")
    assert leading_emoji("КНИГИ") == ("", "КНИГИ")
    food = pack_map(FOOD)
    assert food == {"☕": "101", "🍽": "102", "🍔": "103"}  # первый в наборе выигрывает
    transport = pack_map([sticker("🚕", "201", "Cars"), sticker("🚗", "202", "Cars")])
    found = match([food, transport], ICONS, GROUPS)
    # 🍽️ в таблице с вариантным селектором — в наборе без него
    assert found.subcategories == {"кофе": "101", "кафе": "102", "такси": "201"}
    assert found.groups == {"🍔 ЕДА": "103", "🚗 ТРАНСПОРТ": "202"}


def test_buttons_show_pictures_instead_of_plain_emoji() -> None:
    custom = CustomIcons({"кофе": "101"}, {"🍔 ЕДА": "103"})
    subs = keyboards.generate_subcategories_keyboard(
        {"🍔 ЕДА": ["кофе", "кафе"]}, "🍔 ЕДА", ICONS, custom
    )
    coffee, cafe = subs.inline_keyboard[0]
    assert (coffee.text, coffee.icon_custom_emoji_id, coffee.callback_data) == (
        "кофе",
        "101",
        "sub_кофе",
    )
    assert (cafe.text, cafe.icon_custom_emoji_id) == ("🍽️ кафе", None)

    groups = keyboards.generate_categories_keyboard(["🍔 ЕДА", "КНИГИ"], custom)
    food, books = groups.inline_keyboard[0]
    assert (food.text, food.icon_custom_emoji_id, food.callback_data) == (
        "ЕДА",
        "103",
        "cat_🍔 ЕДА",
    )
    assert (books.text, books.icon_custom_emoji_id) == ("КНИГИ", None)


def test_pictures_only_while_the_owner_has_premium() -> None:
    custom = CustomIcons({"кофе": "101"}, {})
    context = SimpleNamespace(bot_data={"custom_icons": custom, "premium": True})
    assert premium_icons(context) is custom
    context.bot_data["premium"] = False
    assert premium_icons(context) == EMPTY
    assert premium_icons(SimpleNamespace(bot_data={})) == EMPTY


async def test_owner_premium_is_tracked_from_updates(monkeypatch) -> None:
    monkeypatch.setattr(config, "ANALYTICS_CHAT_ID", None)
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({7, 9}))
    monkeypatch.setattr(config, "OWNER_ID", 7)
    context = SimpleNamespace(bot_data={})
    owner = SimpleNamespace(effective_user=SimpleNamespace(id=7, is_premium=True))
    await main._track_premium(owner, context)
    assert context.bot_data["premium"] is True
    other = SimpleNamespace(effective_user=SimpleNamespace(id=9, is_premium=False))
    await main._track_premium(other, context)
    assert context.bot_data["premium"] is True  # важен Premium владельца бота


class PackSheets:
    def __init__(self, packs: list[str]) -> None:
        self.packs = list(packs)

    def get_emoji_packs(self) -> list[str]:
        return list(self.packs)

    def set_emoji_packs(self, packs: list[str]) -> None:
        self.packs = list(packs)


def bot(sets: dict[str, list[SimpleNamespace]]) -> SimpleNamespace:
    async def get_custom_emoji_stickers(ids: list[str]) -> list[SimpleNamespace]:
        everything = [s for stickers in sets.values() for s in stickers]
        return [next(s for s in everything if s.custom_emoji_id == i) for i in ids]

    async def get_sticker_set(name: str) -> SimpleNamespace:
        return SimpleNamespace(name=name, title=f"{name} title", stickers=sets[name])

    return SimpleNamespace(
        get_custom_emoji_stickers=get_custom_emoji_stickers,
        get_sticker_set=get_sticker_set,
    )


def emoji_message(update: SimpleNamespace, *custom_ids: str) -> None:
    update.message.text = "☕" * len(custom_ids)
    update.message.entities = [
        MessageEntity(MessageEntity.CUSTOM_EMOJI, i, 1, custom_emoji_id=cid)
        for i, cid in enumerate(custom_ids)
    ]


async def test_icons_command_then_any_emoji_of_a_pack_connects_the_whole_pack() -> None:
    update, context, _ = make_chat("/icons", {})
    sheets = PackSheets([])
    context.bot_data.update(
        gs_service=sheets,
        icons=ICONS,
        categories=GROUPS,
        premium=True,
    )
    context.bot = bot({"FoodPack": FOOD, "Cars": [sticker("🚕", "201", "Cars")]})

    await icons.icons_command(update, context)
    assert update.message.reply_text.await_args.args[0].startswith(
        "Пришлите кастомный эмодзи из набора"
    )

    emoji_message(update, "101", "201")
    await messages.text_handler(update, context)

    assert sheets.packs == ["FoodPack", "Cars"]
    found = context.bot_data["custom_icons"]
    assert found.subcategories == {"кофе": "101", "кафе": "102", "такси": "201"}
    reply = update.message.reply_text.await_args.args[0]
    assert reply.startswith("✅ Подключил: FoodPack title, Cars title")
    assert "картинки у 3 из 3 подкатегорий и 1 из 3 групп" in reply
    assert icons.MODE not in context.user_data

    # без /icons те же эмодзи — обычное сообщение, а не набор
    update.message.reply_text.reset_mock()
    context.bot_data["ai_service"] = None
    await messages.text_handler(update, context)
    assert sheets.packs == ["FoodPack", "Cars"]


async def test_a_plain_message_after_icons_is_handled_as_usual() -> None:
    update, context, _ = make_chat("/icons", {})
    context.bot_data.update(gs_service=PackSheets([]), premium=True)
    await icons.icons_command(update, context)
    update.message.entities = []
    assert await icons.receive_pack(update, context) is False
    assert icons.MODE not in context.user_data  # режим только на одно сообщение


async def test_icons_off_returns_plain_emoji() -> None:
    update, context, _ = make_chat("/icons off", {})
    sheets = PackSheets(["FoodPack"])
    context.bot_data.update(
        gs_service=sheets, custom_icons=CustomIcons({"кофе": "101"}, {})
    )
    await icons.icons_command(update, context)
    assert sheets.packs == []
    assert context.bot_data["custom_icons"] == EMPTY
    assert "обычные эмодзи" in update.message.reply_text.await_args.args[0]


async def test_packs_are_loaded_on_start() -> None:
    bot_data: dict[str, Any] = {"icons": ICONS, "categories": GROUPS}
    found, titles = await icons.load_custom_icons(
        bot({"FoodPack": FOOD}), bot_data, ["FoodPack", "Gone"]
    )
    assert titles == ["FoodPack title"]  # пропавший набор пропускаем
    assert bot_data["custom_icons"] is found and found.groups == {"🍔 ЕДА": "103"}


def test_packs_live_in_column_j_of_system() -> None:
    ws = RangeWorksheet({"J2:J": [["FoodPack"], [" Cars "], [""]]})
    service = make_service(ws)
    assert service.get_emoji_packs() == ["FoodPack", "Cars"]
    service.set_emoji_packs(["Cars"])
    [body] = service.sheet.values_batch
    assert body["data"] == [
        {"range": "system!J1:J4", "values": [["наборы эмодзи"], ["Cars"], [""], [""]]}
    ]
