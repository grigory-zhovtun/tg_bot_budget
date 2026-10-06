from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import Chat
from telegram.ext import ApplicationHandlerStop

from app.auth import make_gatekeeper, parse_user_ids


@pytest.mark.parametrize(
    ("sources", "expected"),
    [
        (("123", None), {123}),
        (("", "456"), {456}),
        ((None, "  "), set()),
        ((None, None), set()),
        (("1, 2 3", "9"), {1, 2, 3}),
        (("abc 5", None), {5}),
        (("-1001", None), {-1001}),
    ],
)
def test_parse_user_ids(sources: tuple[str | None, ...], expected: set[int]) -> None:
    assert parse_user_ids(*sources) == frozenset(expected)


def make_update(user_id: int | None, *, callback: bool = False) -> SimpleNamespace:
    user = SimpleNamespace(id=user_id) if user_id is not None else None
    chat = SimpleNamespace(type=Chat.PRIVATE, send_message=AsyncMock())
    query = SimpleNamespace(answer=AsyncMock()) if callback else None
    return SimpleNamespace(
        effective_user=user, effective_chat=chat, callback_query=query
    )


async def test_allowed_user_passes() -> None:
    gate = make_gatekeeper(frozenset({42}))
    update = make_update(42)
    await gate(update, SimpleNamespace())
    update.effective_chat.send_message.assert_not_awaited()


async def test_stranger_is_stopped_and_told_id_once() -> None:
    gate = make_gatekeeper(frozenset({42}))
    first, second = make_update(7), make_update(7)
    for update in (first, second):
        with pytest.raises(ApplicationHandlerStop):
            await gate(update, SimpleNamespace())
    first.effective_chat.send_message.assert_awaited_once()
    assert "7" in first.effective_chat.send_message.await_args.args[0]
    second.effective_chat.send_message.assert_not_awaited()


async def test_stranger_button_press_is_answered_and_stopped() -> None:
    gate = make_gatekeeper(frozenset({42}))
    update = make_update(7, callback=True)
    with pytest.raises(ApplicationHandlerStop):
        await gate(update, SimpleNamespace())
    update.callback_query.answer.assert_awaited_once()


async def test_empty_allowlist_blocks_everyone() -> None:
    gate = make_gatekeeper(frozenset())
    with pytest.raises(ApplicationHandlerStop):
        await gate(make_update(42), SimpleNamespace())


async def test_update_without_user_is_stopped() -> None:
    gate = make_gatekeeper(frozenset({42}))
    update = make_update(None)
    with pytest.raises(ApplicationHandlerStop):
        await gate(update, SimpleNamespace())
    update.effective_chat.send_message.assert_not_awaited()
