"""Настройки чата и начало уборки — в блоке system!Q:U таблицы."""

from typing import Any

from app.chat_state import (
    CHAT_HEADER,
    ChatState,
    chat_row,
    parse_chat_rows,
    state_of,
    update_state,
)
from tests.test_google_sheets import RangeWorksheet, make_service


def test_rows_are_read_by_id() -> None:
    rows: list[list[Any]] = [
        ["7", "да", "нет", "120", "130 131"],
        [8, "Да", "да", "", "140, 141"],
        ["9", "", "", "x"],
        ["не ID", "да", "да"],
        [],
        ["1 234 567 890", "нет", "да"],
    ]
    assert parse_chat_rows(rows) == {
        7: ChatState(groups=True, cards=False, start=120, keep=(130, 131)),
        8: ChatState(groups=True, cards=True, start=None, keep=(140, 141)),
        9: ChatState(),
        1234567890: ChatState(cards=True),
    }


def test_row_is_written_as_text() -> None:
    state = ChatState(groups=True, cards=False, start=120, keep=(130, 131))
    assert chat_row(7, state) == ["7", "да", "нет", "120", "130 131"]
    assert chat_row(8, ChatState()) == ["8", "нет", "нет", "", ""]


def test_block_lives_in_q_to_u_of_system() -> None:
    block = [CHAT_HEADER, ["7", "да", "нет", "120", ""], ["", ""], ["8", "нет"]]
    service = make_service(RangeWorksheet({"Q1:U40": block}))
    assert service.get_chat_states() == {
        7: ChatState(groups=True, start=120),
        8: ChatState(),
    }

    service.set_chat_state(8, ChatState(cards=True, start=200))
    service.set_chat_state(9, ChatState())
    first, second = service.sheet.values_batch
    assert first == {
        "valueInputOption": "RAW",
        "data": [
            {"range": "system!Q1:U1", "values": [CHAT_HEADER]},
            {"range": "system!Q4:U4", "values": [["8", "нет", "да", "200", ""]]},
        ],
    }
    # нет строки с этим ID — первая пустая под заголовком
    assert second["data"][1] == {
        "range": "system!Q3:U3",
        "values": [["9", "нет", "нет", "", ""]],
    }


def test_empty_block_starts_under_the_header() -> None:
    service = make_service(RangeWorksheet({}))
    assert service.get_chat_states() == {}
    service.set_chat_state(7, ChatState(groups=True))
    [body] = service.sheet.values_batch
    assert body["data"][1]["range"] == "system!Q2:U2"


def test_state_of_defaults_to_off() -> None:
    bot_data = {"chats": {7: ChatState(groups=True)}}
    assert state_of(bot_data, 7) == ChatState(groups=True)
    assert state_of(bot_data, 8) == ChatState()
    assert state_of({}, 7) == ChatState()


class Sheets:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.written: list[tuple[int, ChatState]] = []

    def set_chat_state(self, chat_id: int, state: ChatState) -> None:
        if self.fail:
            raise ConnectionError("quota")
        self.written.append((chat_id, state))


async def test_update_changes_only_the_given_fields() -> None:
    sheets = Sheets()
    bot_data: dict[str, Any] = {
        "gs_service": sheets,
        "chats": {7: ChatState(cards=True, start=120)},
    }
    new = await update_state(bot_data, 7, groups=True)
    assert new == ChatState(groups=True, cards=True, start=120)
    assert sheets.written == [(7, new)]
    assert state_of(bot_data, 7) == new
    assert await update_state(bot_data, 8, start=5) == ChatState(start=5)


async def test_update_keeps_the_old_state_when_the_sheet_fails() -> None:
    bot_data: dict[str, Any] = {
        "gs_service": Sheets(fail=True),
        "chats": {7: ChatState(cards=True)},
    }
    assert await update_state(bot_data, 7, groups=True) is None
    assert state_of(bot_data, 7) == ChatState(cards=True)
