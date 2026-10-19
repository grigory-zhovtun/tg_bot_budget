"""Остатки со скриншотов: сопоставление карт, сверка, кнопки «Выровнять»."""

from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app import config
from app.handlers import balances, messages
from app.handlers.common import LAST_WRITE
from app.services.google_sheets import row_values
from tests.test_messages import FakeAI, make_chat

SOURCES = ["VISA 9090 UZS", "UZCARD 5656 UZS", "HUMO 7007 UZS", "VISA 3434 USD"]

# Как вернёт Gemini для главного экрана банка (выдуманные остатки)
SCREEN = [
    {"card": "3434", "balance": 3333.33, "currency": "USD", "name": "VISA USD"},
    {"card": "•• 5656", "balance": 444444.14, "currency": "UZS", "name": "Uzcard"},
    {"card": "7007", "balance": 555555.71, "currency": "UZS", "name": "Humo"},
    {"card": "9090", "balance": 7700000, "currency": "UZS", "name": "VISA UZS"},
    {"card": "1357", "balance": None, "currency": "UZS", "name": "скрыт"},
    {"card": "7777", "balance": 1000, "currency": "UZS", "name": "новая"},
]


def test_cards_are_matched_by_last_digits() -> None:
    items = [*SCREEN, {"card": "9090", "balance": 5, "currency": "USD"}]
    matched = balances.match_cards(items, SOURCES, frozenset({"7777"}))
    assert [(b.source, b.balance) for b in matched.balances] == [
        ("VISA 3434 USD", 3333.33),
        ("UZCARD 5656 UZS", 444444.14),
        ("HUMO 7007 UZS", 555555.71),
        ("VISA 9090 UZS", 7700000.0),
    ]
    assert matched.unknown_cards == []  # 7777 в списке исключений, 1357 скрыта
    assert matched.wrong_currency == []  # повтор карты 9090 не учитывается

    matched = balances.match_cards(
        [
            {"card": "7777", "balance": 1},
            {"card": "9090", "balance": 5, "currency": "USD"},
        ],
        SOURCES,
        frozenset(),
    )
    assert matched.unknown_cards == ["7777"]
    assert matched.wrong_currency == ["VISA 9090 UZS: на скрине USD"]


class BalanceSheets:
    """Блок остатков: остатки по таблице, запись «Проверки» и строк выравнивания."""

    def __init__(self, table: dict[str, float]) -> None:
        self.table = dict(table)
        self.written: dict[str, float] = {}
        self.checked_at: datetime | None = None
        self.appended: list[list[Any]] = []

    def update_balances(
        self, values: dict[str, float], checked_at: Any = None
    ) -> list[str]:
        self.written.update(values)
        self.checked_at = checked_at
        return list(values)

    def get_table_balances(self) -> dict[str, float]:
        return dict(self.table)

    def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
        self.appended += [row_values(row) for row in rows]
        return 4190, 4189 + len(rows)


def chat(
    table: dict[str, float],
) -> tuple[SimpleNamespace, SimpleNamespace, BalanceSheets]:
    sheets = BalanceSheets(table)
    sent: list[SimpleNamespace] = []

    async def send(text: str, **kwargs: Any) -> SimpleNamespace:
        message = SimpleNamespace(text=text, reply_markup=kwargs.get("reply_markup"))
        sent.append(message)
        return message

    update = SimpleNamespace(effective_chat=SimpleNamespace(id=1, send_message=send))
    context = SimpleNamespace(
        bot_data={"gs_service": sheets, "sources": SOURCES}, user_data={}
    )
    update.sent = sent
    return update, context, sheets


TABLE = {
    "VISA 9090 UZS": 8255555.72,  # в таблице больше: не записаны траты
    "UZCARD 5656 UZS": 444000.14,  # в таблице меньше
    "HUMO 7007 UZS": 555555.71,
    "VISA 3434 USD": 3333.34,  # цент разницы — сходится
}


@pytest.fixture(autouse=True)
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "IGNORED_CARDS", frozenset({"1357"}))
    monkeypatch.setattr(config, "GEMINI_API_KEY", "test")


async def test_screen_balances_are_written_and_compared() -> None:
    update, context, sheets = chat(TABLE)
    await balances.report_screen_balances(update, context, SCREEN)

    assert sheets.written == {
        "VISA 3434 USD": 3333.33,
        "UZCARD 5656 UZS": 444444.14,
        "HUMO 7007 UZS": 555555.71,
        "VISA 9090 UZS": 7700000.0,
    }
    assert isinstance(sheets.checked_at, datetime)  # «Сверено»: время отправки
    [report] = update.sent
    lines = report.text.splitlines()
    assert lines[0].startswith("💳 Остатки банка на ")
    assert "✅ VISA 3434 USD: 3 333.33 — сходится" in lines
    assert "✅ HUMO 7007 UZS: 555 556 — сходится" in lines
    assert (
        "⚠️ VISA 9090 UZS: банк 7 700 000, в таблице 8 255 556 — в таблице больше на 555 556"
        in lines
    )
    assert (
        "⚠️ UZCARD 5656 UZS: банк 444 444, в таблице 444 000 — в таблице меньше на 444"
        in lines
    )
    assert "ℹ️ Карты 7777 нет в таблице (лист system, колонка F)" in lines
    buttons = [row[0] for row in report.reply_markup.inline_keyboard]
    assert [(b.text, b.callback_data) for b in buttons] == [
        ("Выровнять UZCARD 5656 UZS: +444", "align:UZCARD 5656 UZS"),
        ("Выровнять VISA 9090 UZS: −555 556", "align:VISA 9090 UZS"),
    ]
    assert {b.style for b in buttons} == {"primary"}
    assert set(context.user_data[balances.PENDING]) == {
        "UZCARD 5656 UZS",
        "VISA 9090 UZS",
    }


async def test_nothing_recognised_is_said_plainly() -> None:
    update, context, sheets = chat(TABLE)
    await balances.report_screen_balances(
        update, context, [{"card": "1357", "balance": 5}]
    )
    assert update.sent[0].text.endswith("Не нашёл на скрине остатков карт из таблицы.")
    assert sheets.written == {} and update.sent[0].reply_markup is None


def press(context: SimpleNamespace, data: str, keyboard: Any) -> SimpleNamespace:
    message = SimpleNamespace(reply_markup=keyboard, reply_text=AsyncMock())
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        message=message,
        edit_message_reply_markup=AsyncMock(),
    )
    return SimpleNamespace(callback_query=query)


async def test_align_uses_the_difference_at_press_time() -> None:
    update, context, sheets = chat(TABLE)
    await balances.report_screen_balances(update, context, SCREEN)
    keyboard = update.sent[0].reply_markup
    sheets.table["VISA 9090 UZS"] = 7750000  # после скрина записали SMS на 505 556

    pressed = press(context, "align:VISA 9090 UZS", keyboard)
    await balances.align_button(pressed, context)

    [row] = sheets.appended
    assert row[1:4] == [
        "🚧 РАЗНОЕ",
        "неучтенка",
        50000.0,
    ]  # расход: банк меньше таблицы
    assert row[5].startswith("AI: выравнивание к остатку банка 7 700 000 (")
    assert row[6:] == ["UZS", "VISA 9090 UZS"]
    assert context.user_data[LAST_WRITE]["first"] == 4190  # /undo сработает
    reply = pressed.callback_query.message.reply_text.await_args.args[0]
    assert reply.startswith(
        "✅ VISA 9090 UZS: добавил выравнивание 50 000 (строка 4190)"
    )
    [rest] = pressed.callback_query.edit_message_reply_markup.await_args.args
    assert [r[0].callback_data for r in rest.inline_keyboard] == [
        "align:UZCARD 5656 UZS"
    ]

    # таблица меньше банка → выравнивание приходом (минус в колонке D)
    pressed = press(context, "align:UZCARD 5656 UZS", rest)
    await balances.align_button(pressed, context)
    assert sheets.appended[-1][3] == -444.0


async def test_align_when_already_equal_or_stale() -> None:
    update, context, sheets = chat(TABLE)
    await balances.report_screen_balances(update, context, SCREEN)
    sheets.table["VISA 9090 UZS"] = 7700000  # SMS всё дописали
    pressed = press(context, "align:VISA 9090 UZS", update.sent[0].reply_markup)
    await balances.align_button(pressed, context)
    assert sheets.appended == []
    assert pressed.callback_query.message.reply_text.await_args.args[0] == (
        "✅ VISA 9090 UZS: уже сходится с банком"
    )

    again = press(context, "align:VISA 9090 UZS", None)
    await balances.align_button(again, context)
    again.callback_query.answer.assert_awaited_once_with(
        "Сверка устарела — пришлите скрин ещё раз"
    )


async def test_bank_main_screen_photo_goes_to_the_balance_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reported = AsyncMock()
    monkeypatch.setattr(balances, "report_screen_balances", reported)
    ai = FakeAI([])
    ai.screen = {
        "kind": "balances",
        "balances": SCREEN,
        "transactions": [{"amount": 189}],
    }
    update, context, sheets = make_chat("", {}, ai)
    photo_file = SimpleNamespace(
        download_to_drive=AsyncMock(side_effect=lambda p: p.write_bytes(b"\xff\xd8"))
    )
    update.message.photo = [
        SimpleNamespace(
            file_unique_id="screen-1", get_file=AsyncMock(return_value=photo_file)
        )
    ]

    await messages.text_handler(update, context)

    reported.assert_awaited_once()
    assert reported.await_args.args[2] == SCREEN
    assert sheets.rows == []  # операции с экрана остатков не записываются
