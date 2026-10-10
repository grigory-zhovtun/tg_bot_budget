"""/icons — картинки на кнопках из наборов кастомных эмодзи.

После /icons владелец присылает любой кастомный эмодзи из понравившегося набора
(можно несколько из разных наборов одним сообщением); бот подключает набор
целиком и подбирает картинки по обычным эмодзи. «/icons off» — выключить.
Список наборов лежит в system!J, при старте бот загружает их заново.
"""

import asyncio
import logging

from telegram import Bot, MessageEntity, Update
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from app.custom_icons import EMPTY, CustomIcons, match, pack_map
from app.errors import user_message

logger = logging.getLogger(__name__)

MODE = "icons_mode"  # user_data: ждём сообщение с кастомными эмодзи


async def load_custom_icons(
    bot: Bot, bot_data: dict, packs: list[str]
) -> tuple[CustomIcons, list[str]]:
    """Скачать наборы и подобрать картинки; пропавший набор пропускаем."""
    maps, titles = [], []
    for name in packs:
        try:
            sticker_set = await bot.get_sticker_set(name)
        except (TelegramError, KeyError) as error:
            logger.warning("Emoji pack %s is unavailable: %s", name, error)
            continue
        maps.append(pack_map(sticker_set.stickers))
        titles.append(sticker_set.title)
    found = match(maps, bot_data.get("icons", {}), bot_data.get("categories", []))
    bot_data["custom_icons"] = found
    bot_data["emoji_packs"] = packs
    return found, titles


def coverage(found: CustomIcons, bot_data: dict) -> str:
    total_subs = len(bot_data.get("icons", {}))
    total_groups = len(bot_data.get("categories", []))
    return (
        f"картинки у {len(found.subcategories)} из {total_subs} подкатегорий "
        f"и {len(found.groups)} из {total_groups} групп"
    )


async def icons_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    gs_service = context.bot_data["gs_service"]
    args = (update.message.text or "").split()[1:]
    if args and args[0].lower() == "off":
        try:
            await asyncio.to_thread(gs_service.set_emoji_packs, [])
        except Exception as error:
            logger.exception("Could not clear emoji packs")
            await update.message.reply_text(f"Не выключил: {user_message(error)}")
            return
        context.bot_data["emoji_packs"] = []
        context.bot_data["custom_icons"] = EMPTY
        await update.message.reply_text(
            "Картинки на кнопках выключены — снова обычные эмодзи."
        )
        return

    context.user_data[MODE] = True
    lines = [
        "Пришлите кастомный эмодзи из набора, который нравится (можно несколько из "
        "разных наборов одним сообщением). Я подключу набор целиком и подберу "
        "картинки по обычным эмодзи из колонки D листа system и названий групп."
    ]
    if context.bot_data.get("emoji_packs"):
        found = context.bot_data.get("custom_icons", EMPTY)
        count = len(context.bot_data["emoji_packs"])
        lines.append(
            f"Сейчас подключено наборов: {count}, {coverage(found, context.bot_data)}. "
            "«/icons off» — выключить."
        )
    if not context.bot_data.get("premium"):
        lines.append(
            "⚠️ Картинки на кнопках видны, только пока у вас Telegram Premium."
        )
    await update.message.reply_text("\n".join(lines))


async def receive_pack(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Сообщение после /icons с кастомными эмодзи; True — оно обработано здесь."""
    if not context.user_data.get(MODE):
        return False
    entities = getattr(update.message, "entities", None) or []
    ids = [
        entity.custom_emoji_id
        for entity in entities
        if entity.type == MessageEntity.CUSTOM_EMOJI and entity.custom_emoji_id
    ]
    context.user_data.pop(MODE, None)  # ждём только следующее сообщение
    if not ids:
        return False  # обычное сообщение (SMS и т. п.) — разбираем как всегда
    gs_service = context.bot_data["gs_service"]
    try:
        stickers = await context.bot.get_custom_emoji_stickers(ids)
        packs = list(context.bot_data.get("emoji_packs") or [])
        for sticker in stickers:
            if sticker.set_name and sticker.set_name not in packs:
                packs.append(sticker.set_name)
        found, titles = await load_custom_icons(context.bot, context.bot_data, packs)
        await asyncio.to_thread(gs_service.set_emoji_packs, packs)
    except Exception as error:
        logger.exception("Could not connect emoji packs")
        await update.message.reply_text(f"❌ Не подключил: {user_message(error)}")
        return True
    missing = sorted(set(context.bot_data.get("icons", {})) - set(found.subcategories))
    text = f"✅ Подключил: {', '.join(titles)}. Теперь {coverage(found, context.bot_data)}."
    if missing:
        text += f"\nБез картинки (обычный эмодзи): {', '.join(missing[:15])}"
        text += "…" if len(missing) > 15 else ""
        text += "\nДобавьте ещё набор: /icons"
    logger.info("Emoji packs %s: %s", packs, coverage(found, context.bot_data))
    await update.message.reply_text(text)
    return True
