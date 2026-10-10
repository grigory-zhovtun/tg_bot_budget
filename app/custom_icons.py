"""Картинки на кнопках: кастомные эмодзи из наборов Telegram (Bot API 9.4).

Каждый кастомный эмодзи набора привязан к обычному эмодзи. Поэтому иконка
подкатегории из system!D («☕») и эмодзи в начале названия группы («🍔 ЕДА»)
сами находят свою картинку в подключённых наборах — коды вводить не нужно.
Показывать такие кнопки бот может, только пока у владельца Telegram Premium.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

VARIATION_SELECTOR = "️"


@dataclass(frozen=True)
class CustomIcons:
    subcategories: dict[str, str]  # подкатегория → custom_emoji_id
    groups: dict[str, str]  # группа («🍔 ЕДА») → custom_emoji_id


EMPTY = CustomIcons({}, {})


def normalize(emoji: str) -> str:
    """«🍽️» и «🍽» — один эмодзи: в наборах селектор варианта обычно не ставят."""
    return emoji.replace(VARIATION_SELECTOR, "").strip()


def leading_emoji(name: str) -> tuple[str, str]:
    """«🍔 ЕДА» → («🍔», «ЕДА»); без эмодзи в начале → («», name)."""
    head, _, rest = name.partition(" ")
    if rest and not any(char.isalnum() for char in head):
        return head, rest
    return "", name


def pack_map(stickers: Iterable[Any]) -> dict[str, str]:
    """Эмодзи набора → custom_emoji_id; при повторах выигрывает первый в наборе."""
    found: dict[str, str] = {}
    for sticker in stickers:
        if sticker.custom_emoji_id and sticker.emoji:
            found.setdefault(normalize(sticker.emoji), sticker.custom_emoji_id)
    return found


def match(
    maps: list[dict[str, str]], icons: dict[str, str], groups: list[str]
) -> CustomIcons:
    """Картинки для подкатегорий и групп; наборы по порядку подключения."""

    def find(emoji: str) -> str | None:
        key = normalize(emoji)
        return next((found[key] for found in maps if key in found), None)

    subcategories = {name: cid for name, emoji in icons.items() if (cid := find(emoji))}
    by_group = {}
    for group in groups:
        emoji, _ = leading_emoji(group)
        if emoji and (cid := find(emoji)):
            by_group[group] = cid
    return CustomIcons(subcategories, by_group)
