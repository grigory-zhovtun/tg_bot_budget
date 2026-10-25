"""Настройки чата и начало уборки: блок system!Q:U таблицы.

Память бота стирается при каждом обновлении, а история чата — у пользователя,
поэтому включённые клавиатуры и номер, с которого начинается утренняя уборка,
хранятся в таблице: строка на пользователя (ID чата = ID пользователя).
"""

import asyncio
import dataclasses
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

CHAT_HEADER = ["чат бота: ID", "группы", "карты", "уборка с №", "не удалять №"]
YES, NO = "да", "нет"
STATE_LOCK = "chat_state_lock"  # bot_data: записи блока по одной — уборка и кнопки


@dataclass(frozen=True)
class ChatState:
    groups: bool = False  # кнопки групп и подгрупп под записью
    cards: bool = False  # клавиатура карт внизу экрана
    start: int | None = None  # «уборка с №»: первый неубранный номер сообщения
    keep: tuple[int, ...] = ()  # «не удалять №»: сводки недели, снятие клавиатуры


def parse_number(value: Any) -> int | None:
    """«1234567890», 8, «1 234 567 890» → число; пусто или текст — None."""
    text = re.sub(r"[\s,\u00a0]", "", str(value))
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def _numbers(value: Any) -> tuple[int, ...]:
    parts = (parse_number(part) for part in re.split(r"[\s,]+", str(value)))
    return tuple(number for number in parts if number is not None)


def parse_chat_rows(rows: Sequence[Sequence[Any]]) -> dict[int, ChatState]:
    """Строки блока под заголовком → состояние по ID; строка без ID пропускается."""
    states: dict[int, ChatState] = {}
    for row in rows:
        cells = [*row, "", "", "", "", ""]
        chat_id = parse_number(cells[0])
        if chat_id is None:
            continue
        states[chat_id] = ChatState(
            groups=str(cells[1]).strip().lower() == YES,
            cards=str(cells[2]).strip().lower() == YES,
            start=parse_number(cells[3]),
            keep=_numbers(cells[4]),
        )
    return states


def chat_row(chat_id: int, state: ChatState) -> list[str]:
    """Строка блока текстом: таблица не превратит ID в «1,29E+09»."""
    return [
        str(chat_id),
        YES if state.groups else NO,
        YES if state.cards else NO,
        "" if state.start is None else str(state.start),
        " ".join(str(number) for number in state.keep),
    ]


def state_of(bot_data: dict[str, Any], chat_id: int) -> ChatState:
    """Состояние чата; строки нет — обе клавиатуры выключены."""
    return bot_data.get("chats", {}).get(chat_id, ChatState())


async def update_state(
    bot_data: dict[str, Any], chat_id: int, **changes: Any
) -> ChatState | None:
    """Поменять поля состояния и записать в таблицу; не записалось — None.

    Записи идут по одной: уборка и кнопки /keyboards не затрут поля друг друга.
    """
    lock: asyncio.Lock = bot_data.setdefault(STATE_LOCK, asyncio.Lock())
    async with lock:
        new = dataclasses.replace(state_of(bot_data, chat_id), **changes)
        try:
            await asyncio.to_thread(bot_data["gs_service"].set_chat_state, chat_id, new)
        except Exception:
            logger.exception("Could not save the chat state of %s", chat_id)
            return None
        bot_data.setdefault("chats", {})[chat_id] = new
        return new
