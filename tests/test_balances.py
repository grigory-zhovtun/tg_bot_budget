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

SOURCES = ["VISA 9120 UZS", "UZCARD 5837 UZS", "HUMO 6845 UZS", "VISA 4058 USD"]

# Как вернёт Gemini для главного экрана банка (выдуманные остатки)
SCREEN = [
    {"card": "4058", "balance": 2474.85, "currency": "USD", "name": "VISA USD"},
    {"card": "•• 5837", "balance": 253324.14, "currency": "UZS", "name": "Uzcard"},
    {"card": "6845", "balance": 256724.71, "currency": "UZS", "name": "Humo"},
    {"card": "9120", "balance": 11251471, "currency": "UZS", "name": "VISA UZS"},
    {"card": "2513", "balance": None, "currency": "UZS", "name": "скрыт"},
    {"card": "7777", "balance": 1000, "currency": "UZS", "name": "новая"},
]


def test_cards_are_matched_by_last_digits() -> None:
    items = [*SCREEN, {"card": "9120", "balance": 5, "currency": "USD"}]
    matched = balances.match_cards(items, SOURCES, frozenset({"7777"}))
    assert [(b.source, b.balance) for b in matched.balances] == [
        ("VISA 4058 USD", 2474.85),
        ("UZCARD 5837 UZS", 253324.14),
        ("HUMO 6845 UZS", 256724.71),
        ("VISA 9120 UZS", 11251471.0),
    ]
    assert matched.unknown_cards == []  # 7777 в списке исключений, 2513 скрыта
    assert matched.wrong_currency == []  # повтор карты 9120 не учитывается

    matched = balances.match_cards(
        [
            {"card": "7777", "balance": 1},
            {"card": "9120", "balance": 5, "currency": "USD"},
        ],
        SOURCES,
        frozenset(),
    )
    assert matched.unknown_cards == ["7777"]
    assert matched.wrong_currency == ["VISA 9120 UZS: на скрине USD"]


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
    "VISA 9120 UZS": 11783733.72,  # в таблице больше: не записаны траты
    "UZCARD 5837 UZS": 253000.14,  # в таблице меньше
    "HUMO 6845 UZS": 256724.71,
    "VISA 4058 USD": 2474.86,  # цент разницы — сходится
}


@pytest.fixture(autouse=True)
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "IGNORED_CARDS", frozenset({"2513"}))
    monkeypatch.setattr(config, "GEMINI_API_KEY", "test")


async def test_screen_balances_are_written_and_compared() -> None:
    update, context, sheets = chat(TABLE)
    await balances.report_screen_balances(update, context, SCREEN)

    assert sheets.written == {
        "VISA 4058 USD": 2474.85,
        "UZCARD 5837 UZS": 253324.14,
        "HUMO 6845 UZS": 256724.71,
        "VISA 9120 UZS": 11251471.0,
    }
    assert isinstance(sheets.checked_at, datetime)  # «Сверено»: время отправки
    [report] = update.sent
    lines = report.text.splitlines()
    assert lines[0].startswith("💳 Остатки банка на ")
    assert "✅ VISA 4058 USD: 2 474.85 — сходится" in lines
    assert "✅ HUMO 6845 UZS: 256 725 — сходится" in lines
    assert (
        "⚠️ VISA 9120 UZS: банк 11 251 471, в таблице 11 783 734 — в таблице больше на 532 263"
        in lines
    )
    assert (
        "⚠️ UZCARD 5837 UZS: банк 253 324, в таблице 253 000 — в таблице меньше на 324"
        in lines
    )
    assert "ℹ️ Карты 7777 нет в таблице (лист system, колонка F)" in lines
    buttons = [row[0] for row in report.reply_markup.inline_keyboard]
    assert [(b.text, b.callback_data) for b in buttons] == [
        ("Выровнять UZCARD 5837 UZS: +324", "align:UZCARD 5837 UZS"),
        ("Выровнять VISA 9120 UZS: −532 263", "align:VISA 9120 UZS"),
    ]
    assert {b.style for b in buttons} == {"primary"}
    assert set(context.user_data[balances.PENDING]) == {
        "UZCARD 5837 UZS",
        "VISA 9120 UZS",
    }


async def test_nothing_recognised_is_said_plainly() -> None:
    update, context, sheets = chat(TABLE)
    await balances.report_screen_balances(
        update, context, [{"card": "2513", "balance": 5}]
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
    sheets.table["VISA 9120 UZS"] = 11300000  # после скрина записали SMS на 483 734

    pressed = press(context, "align:VISA 9120 UZS", keyboard)
    await balances.align_button(pressed, context)

    [row] = sheets.appended
    assert row[1:4] == [
        "🚧 РАЗНОЕ",
        "неучтенка",
        48529.0,
    ]  # расход: банк меньше таблицы
    assert row[5].startswith("AI: выравнивание к остатку банка 11 251 471 (")
    assert row[6:] == ["UZS", "VISA 9120 UZS"]
    assert context.user_data[LAST_WRITE]["first"] == 4190  # /undo сработает
    reply = pressed.callback_query.message.reply_text.await_args.args[0]
    assert reply.startswith(
        "✅ VISA 9120 UZS: добавил выравнивание 48 529 (строка 4190)"
    )
    [rest] = pressed.callback_query.edit_message_reply_markup.await_args.args
    assert [r[0].callback_data for r in rest.inline_keyboard] == [
        "align:UZCARD 5837 UZS"
    ]

    # таблица меньше банка → выравнивание приходом (минус в колонке D)
    pressed = press(context, "align:UZCARD 5837 UZS", rest)
    await balances.align_button(pressed, context)
    assert sheets.appended[-1][3] == -324.0


async def test_align_when_already_equal_or_stale() -> None:
    update, context, sheets = chat(TABLE)
    await balances.report_screen_balances(update, context, SCREEN)
    sheets.table["VISA 9120 UZS"] = 11251471  # SMS всё дописали
    pressed = press(context, "align:VISA 9120 UZS", update.sent[0].reply_markup)
    await balances.align_button(pressed, context)
    assert sheets.appended == []
    assert pressed.callback_query.message.reply_text.await_args.args[0] == (
        "✅ VISA 9120 UZS: уже сходится с банком"
    )

    again = press(context, "align:VISA 9120 UZS", None)
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
