"""/fix: смена группы и подгруппы последней записи кнопками."""

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest

from app.domain import INCOME_GROUP, SheetRow
from app.handlers import fix
from app.handlers.common import LAST_WRITE

CATEGORIES = ["🍔 ЕДА", INCOME_GROUP, "🚧 РАЗНОЕ"]
SUBCATEGORIES = {
    "🍔 ЕДА": ["продукты", "кафе"],
    INCOME_GROUP: ["зарплата", "прочее"],
    "🚧 РАЗНОЕ": ["неучтенка"],
}


def row(
    category: str = "🚧 РАЗНОЕ",
    subcategory: str = "неучтенка",
    amount: float = 48000.0,
    comment: str = "AI: TEZKOR",
) -> SheetRow:
    return SheetRow(
        day=date(2026, 10, 9),
        category=category,
        subcategory=subcategory,
        amount=amount,
        comment=comment,
        currency="UZS",
        source="VISA 9120 UZS",
    )


class FixSheets:
    def __init__(self, changed: bool = True) -> None:
        self.changed = changed
        self.calls: list[tuple[Any, ...]] = []

    def recategorize_row(self, *args: Any) -> bool:
        self.calls.append(args)
        if isinstance(self.changed, Exception):
            raise self.changed
        return self.changed


class HistoryAI:
    def __init__(self) -> None:
        self.forgotten = 0

    def forget_history(self) -> None:
        self.forgotten += 1


def chat(
    rows: list[SheetRow] | None, sheets: FixSheets | None = None
) -> SimpleNamespace:
    user_data: dict[str, Any] = {}
    if rows is not None:
        user_data[LAST_WRITE] = {
            "first": 4190,
            "last": 4189 + len(rows),
            "rows": rows,
            "fingerprint": None,
        }
    return SimpleNamespace(
        bot_data={
            "gs_service": sheets or FixSheets(),
            "ai_service": HistoryAI(),
            "categories": CATEGORIES,
            "subcategories": SUBCATEGORIES,
        },
        user_data=user_data,
    )


async def command(context: SimpleNamespace) -> tuple[str, Any]:
    update = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
    await fix.fix_command(update, context)
    call = update.message.reply_text.await_args
    return call.args[0], call.kwargs.get("reply_markup")


async def press(context: SimpleNamespace, data: str) -> tuple[str, Any]:
    query = SimpleNamespace(
        data=data, answer=AsyncMock(), edit_message_text=AsyncMock()
    )
    await fix.fix_button(SimpleNamespace(callback_query=query), context)
    query.answer.assert_awaited_once()
    call = query.edit_message_text.await_args
    return call.args[0], call.kwargs.get("reply_markup")


def callbacks(markup: Any) -> list[list[str]]:
    return [
        [button.callback_data for button in line] for line in markup.inline_keyboard
    ]


async def test_single_row_goes_straight_to_groups_and_is_fixed() -> None:
    context = chat([row()])
    text, markup = await command(context)
    assert text == (
        "✏️ 48 000 UZS • TEZKOR • 🚧 РАЗНОЕ (неучтенка) • VISA 9120 UZS\nВыберите группу:"
    )
    assert callbacks(markup) == [["fix:c:0", "fix:c:1"], ["fix:c:2"], ["fix:x"]]

    text, markup = await press(context, "fix:c:0")
    assert text.endswith("🍔 ЕДА — выберите подгруппу:")
    assert [b.text for b in markup.inline_keyboard[0]] == ["продукты", "кафе"]
    assert callbacks(markup) == [["fix:s:0", "fix:s:1"], ["fix:b", "fix:x"]]

    text, markup = await press(context, "fix:s:1")
    sheets = context.bot_data["gs_service"]
    assert sheets.calls == [(4190, row(), "🍔 ЕДА", "кафе", 48000.0)]
    assert (
        text
        == "✅ Исправил (строка 4190): 48 000 UZS • TEZKOR • 🍔 ЕДА (кафе) • VISA 9120 UZS"
    )
    assert markup is None
    # /undo и повторный /fix сверяют строку уже с новой категорией
    assert context.user_data[LAST_WRITE]["rows"] == [row("🍔 ЕДА", "кафе")]
    assert context.bot_data["ai_service"].forgotten == 1
    assert fix.STATE not in context.user_data


async def test_several_rows_ask_which_one() -> None:
    rows = [row(comment="кофе"), row(comment="такси"), row(comment="суши")]
    context = chat(rows)
    text, markup = await command(context)
    assert text.splitlines()[0] == "✏️ Какую операцию исправить?"
    assert "3) 48 000 UZS • суши • 🚧 РАЗНОЕ (неучтенка) • VISA 9120 UZS" in text
    assert callbacks(markup) == [["fix:r:0"], ["fix:r:1"], ["fix:r:2"], ["fix:x"]]
    assert markup.inline_keyboard[2][0].text == "3) 48 000 UZS • неучтенка"

    text, _ = await press(context, "fix:r:2")
    assert text.startswith("✏️ 48 000 UZS • суши")
    await press(context, "fix:c:0")
    await press(context, "fix:s:1")
    [(number, target, *_)] = context.bot_data["gs_service"].calls
    assert (number, target) == (4192, row(comment="суши"))
    assert rows[2] == row("🍔 ЕДА", "кафе", comment="суши")
    assert rows[:2] == [row(comment="кофе"), row(comment="такси")]


async def test_nothing_to_fix() -> None:
    text, markup = await command(chat(None))
    assert text.startswith("Нечего исправлять: последней записи нет")
    assert markup is None


async def test_row_changed_in_the_sheet_is_left_alone() -> None:
    context = chat([row()], FixSheets(changed=False))
    await command(context)
    await press(context, "fix:c:0")
    text, _ = await press(context, "fix:s:0")
    assert text == "Строка 4190 уже изменена в таблице — поправьте её там."
    assert context.user_data[LAST_WRITE]["rows"] == [row()]
    assert context.bot_data["ai_service"].forgotten == 0


async def test_sheet_error_is_reported() -> None:
    context = chat([row()], FixSheets(changed=ConnectionError("timed out")))
    await command(context)
    await press(context, "fix:c:0")
    text, _ = await press(context, "fix:s:0")
    assert text == "❌ Не исправил: timed out. /fix — ещё раз"
    assert context.user_data[LAST_WRITE]["rows"] == [row()]


async def test_back_and_cancel() -> None:
    context = chat([row()])
    await command(context)
    await press(context, "fix:c:0")
    text, markup = await press(context, "fix:b")
    assert text.endswith("Выберите группу:")
    assert "category" not in context.user_data[fix.STATE]
    assert callbacks(markup)[0] == ["fix:c:0", "fix:c:1"]

    text, markup = await press(context, "fix:x")
    assert (text, markup) == ("Исправление отменено.", None)
    assert fix.STATE not in context.user_data
    assert context.bot_data["gs_service"].calls == []


async def test_buttons_of_an_older_write_do_not_touch_the_new_one() -> None:
    context = chat([row()])
    await command(context)
    # пока выбирали группу, пришло новое SMS
    context.user_data[LAST_WRITE] = {
        "first": 4191,
        "last": 4191,
        "rows": [row(comment="AI: KORZINKA")],
        "fingerprint": None,
    }
    text, _ = await press(context, "fix:c:0")
    assert text == fix.STALE
    assert fix.STATE not in context.user_data

    context = chat([row()])
    await command(context)
    for data in ("fix:c:9", "fix:c:", "fix:r:0x"):
        text, _ = await press(context, data)
        assert text == fix.STALE


async def test_pressing_the_same_button_twice_is_quiet() -> None:
    context = chat([row()])
    await command(context)
    query = SimpleNamespace(
        data="fix:c:0",
        answer=AsyncMock(),
        edit_message_text=AsyncMock(
            side_effect=BadRequest(
                "Message is not modified: specified new message content ..."
            )
        ),
    )
    await fix.fix_button(SimpleNamespace(callback_query=query), context)
    assert context.user_data[fix.STATE]["category"] == "🍔 ЕДА"


@pytest.mark.parametrize(
    ("before", "category", "expected"),
    [
        (row(amount=48000.0), "🍔 ЕДА", 48000.0),  # расход остаётся расходом
        (row(amount=-324.0), INCOME_GROUP, 324.0),  # приход → доход: знак меняется
        (row(INCOME_GROUP, "зарплата", 1000.0), "🚧 РАЗНОЕ", -1000.0),
        (row(INCOME_GROUP, "зарплата", 1000.0), INCOME_GROUP, 1000.0),
        (row(amount=48000.0), INCOME_GROUP, None),  # списание доходом не бывает
    ],
)
def test_money_keeps_its_direction(
    before: SheetRow, category: str, expected: float | None
) -> None:
    assert fix.amount_for(before, category) == expected


async def test_income_row_moved_out_of_income_flips_the_sign() -> None:
    context = chat([row(INCOME_GROUP, "прочее", 250000.0, "AI: возврат")])
    await command(context)
    await press(context, "fix:c:2")
    text, _ = await press(context, "fix:s:0")
    [call] = context.bot_data["gs_service"].calls
    assert call[2:] == ("🚧 РАЗНОЕ", "неучтенка", -250000.0)
    assert text.startswith(
        "✅ Исправил (строка 4190): +250 000 UZS • возврат • 🚧 РАЗНОЕ"
    )


async def test_expense_cannot_become_income() -> None:
    context = chat([row()])
    await command(context)
    await press(context, "fix:c:1")
    text, _ = await press(context, "fix:s:0")
    assert text.startswith("Это списание — в доходы его не перенести.")
    assert "строку 4190" in text
    assert context.bot_data["gs_service"].calls == []
