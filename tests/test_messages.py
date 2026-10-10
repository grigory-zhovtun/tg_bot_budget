"""Сценарии чата целиком: ручной ввод и разбор SMS → строки листа fact."""

from itertools import count
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from telegram import ReplyKeyboardMarkup

from app import config
from app.domain import Rates, entry_row, local_today
from app.handlers import messages, undo
from app.services.google_sheets import BALANCE_FORMULA, row_values
from app.web.auth import check_launch_token

CATEGORIES = ["🏚️ ДОМ", "🍔 ЕДА", "🌛 ЕЖЕМЕСЯЧНО", "🚧 РАЗНОЕ", "💳 СЧЕТА", "💰 ДОХОДЫ"]
SUBCATEGORIES = {
    "🏚️ ДОМ": ["продукты"],
    "🍔 ЕДА": ["кофе", "кафе"],
    "🌛 ЕЖЕМЕСЯЧНО": ["подписка"],
    "🚧 РАЗНОЕ": ["неучтенка"],
    "💳 СЧЕТА": ["переводы"],
    "💰 ДОХОДЫ": ["зарплата"],
}
SOURCES = ["VISA 9120 UZS", "UZCARD 5837 UZS", "VISA 4058 USD"]


class FakeSheets:
    """Таблица в памяти с тем же интерфейсом, что GoogleSheetsService."""

    BALANCE_CELLS = {"VISA 9120 UZS": "N2", "UZCARD 5837 UZS": "N3"}

    def __init__(self) -> None:
        self.rows: list[list[Any]] = []
        self.cells: dict[str, Any] = {}
        self.last_row = 4168
        self.appends = 0

    def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
        self.appends += 1
        self.rows += [row_values(row) for row in rows]
        first = self.last_row + 1
        self.last_row += len(rows)
        return first, self.last_row

    def update_balances(
        self, balances: dict[str, float], checked_at: Any = None
    ) -> list[str]:
        self.checked_at = checked_at
        known = {k: v for k, v in balances.items() if k in self.BALANCE_CELLS}
        self.cells.update(
            {f"fact!{self.BALANCE_CELLS[k]}": v for k, v in known.items()}
        )
        return list(known)

    def get_rates(self) -> Rates:
        return Rates({"UZS": 1.0, "USD": 12000.0, "RUB": 150.0})

    table_balances: dict[str, float] = {}
    undo_allowed = True

    def get_table_balances(self) -> dict[str, float]:
        return self.table_balances

    def delete_rows_if_match(self, first: int, last: int, rows: list[Any]) -> bool:
        self.undone = (first, last, rows)
        return self.undo_allowed


class FakeAI:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.calls = 0

    async def parse_transaction(self, **kwargs: Any) -> Any:
        self.calls += 1
        assert kwargs["known_subcategories"] == SUBCATEGORIES
        return self.result

    screen: dict[str, Any] | None = None  # ответ на фото; по умолчанию — операции

    async def parse_screenshot(self, image: bytes, **kwargs: Any) -> dict[str, Any]:
        self.calls += 1
        assert kwargs["known_subcategories"] == SUBCATEGORIES
        return self.screen or {
            "kind": "transactions",
            "balances": [],
            "transactions": self.result,
        }


ids = count(100)


def sent_message() -> SimpleNamespace:
    return SimpleNamespace(message_id=next(ids))


def make_chat(text: str, user_data: dict[str, Any], ai: FakeAI | None = None):
    sheets = FakeSheets()
    reply = AsyncMock(side_effect=lambda *a, **k: sent_message())
    message = SimpleNamespace(
        message_id=next(ids),
        photo=[],
        text=text,
        caption=None,
        delete=AsyncMock(),
        reply_text=reply,
        set_reaction=AsyncMock(),
    )
    update = SimpleNamespace(
        message=message,
        effective_message=message,
        effective_chat=SimpleNamespace(
            id=1, send_message=AsyncMock(side_effect=lambda *a, **k: sent_message())
        ),
    )
    context = SimpleNamespace(
        bot_data={
            "gs_service": sheets,
            "ai_service": ai,
            "categories": CATEGORIES,
            "subcategories": SUBCATEGORIES,
            "sources": SOURCES,
        },
        user_data=user_data,
        bot=SimpleNamespace(
            delete_message=AsyncMock(),
            send_message_draft=AsyncMock(),
            send_message=AsyncMock(side_effect=lambda *a, **k: sent_message()),
        ),
    )
    return update, context, sheets


def chat_messages(update: SimpleNamespace) -> list[Any]:
    """Сообщения бота в чат (chat.send_message) по порядку."""
    return update.effective_chat.send_message.await_args_list


def summary_call(update: SimpleNamespace) -> Any:
    """Сводка записи — первое сообщение с клавиатурой карт."""
    return next(
        call
        for call in chat_messages(update)
        if isinstance(call.kwargs.get("reply_markup"), ReplyKeyboardMarkup)
    )


def summary(update: SimpleNamespace) -> str:
    return summary_call(update).args[0]


def menu_after_summary(update: SimpleNamespace) -> Any:
    """Сообщение с группами сразу под сводкой."""
    calls = chat_messages(update)
    return calls[calls.index(summary_call(update)) + 1]


@pytest.fixture(autouse=True)
def gemini_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "GEMINI_API_KEY", "test")


def manual_state(category: str = "🍔 ЕДА", sub: str = "кофе") -> dict[str, Any]:
    return {"source": "VISA 9120 UZS", "category": category, "subcategory": sub}


async def test_manual_expense_row() -> None:
    update, context, sheets = make_chat("48000 латте", manual_state())
    await messages.text_handler(update, context)

    today = local_today(config.ANALYTICS_TIMEZONE).strftime("%d.%m.%Y")
    [row] = sheets.rows
    assert row[:4] == [today, "🍔 ЕДА", "кофе", 48000.0]
    assert row[4] == BALANCE_FORMULA
    assert row[5:] == ["латте", "UZS", "VISA 9120 UZS"]
    assert summary(update).startswith("✅ 48 000 UZS • 🍔 ЕДА (кофе)")
    assert "category" not in context.user_data
    update.message.delete.assert_awaited()


async def test_manual_incoming_with_plus_is_negative() -> None:
    state = manual_state("💳 СЧЕТА", "переводы")
    update, context, sheets = make_chat("+1 000 000 с HUMO", state)
    await messages.text_handler(update, context)
    assert sheets.rows[0][3] == -1000000.0
    assert summary(update).startswith("✅ +1 000 000 UZS")


async def test_pasted_sms_in_manual_state_goes_to_ai() -> None:
    ai = FakeAI([])
    update, context, sheets = make_chat(
        "12.10.2026 Oplata 50000 UZS", manual_state(), ai
    )
    await messages.text_handler(update, context)
    assert ai.calls == 1
    assert sheets.rows == []
    assert "Не удалось распознать" in summary(update)


async def test_sms_in_usd_on_uzs_card_is_converted_and_balance_updated() -> None:
    ai = FakeAI(
        [
            {
                "amount": 12.21,
                "currency": "USD",
                "source": "*9120",
                "category": "🌛 ЕЖЕМЕСЯЧНО",
                "subcategory": "подписка",
                "comment": "JetBrains",
                "date": "05.10.2026",
                "balance": "15 000 000,50",
                "card_identifier": "9120",
                "direction": "expense",
            }
        ]
    )
    update, context, sheets = make_chat("Pokupka JetBrains 12.21 USD", {}, ai)
    await messages.text_handler(update, context)

    [row] = sheets.rows
    assert row[0] == "05.10.2026"
    assert row[3] == 146520.0
    assert row[5] == "JetBrains; ≈ 12.21 USD по курсу таблицы"
    assert row[6:] == ["UZS", "VISA 9120 UZS"]
    assert sheets.cells == {"fact!N2": 15000000.5}
    assert "💳 15 000 000" in summary(update)
    assert context.user_data["source"] == "VISA 9120 UZS"


async def test_bad_items_are_reported_not_written() -> None:
    ai = FakeAI(
        [
            {"amount": None, "comment": "без суммы"},
            {
                "amount": 30,
                "currency": "SGD",
                "source": "VISA 4058 USD",
                "comment": "Airalo",
            },
            {"amount": 28000, "source": "UZCARD 5837 UZS", "category": "Прочее"},
        ]
    )
    update, context, sheets = make_chat("три операции", {}, ai)
    await messages.text_handler(update, context)

    [row] = sheets.rows
    assert row[1:4] == ["🚧 РАЗНОЕ", "неучтенка", 28000.0]
    text = summary(update)
    assert "⚠️ Не записал: без суммы — не нашёл сумму" in text
    assert "⚠️ Не записал: Airalo — нет курса SGD → USD" in text


async def test_several_rows_are_written_in_one_request() -> None:
    ai = FakeAI(
        [
            {
                "amount": 10000,
                "source": "VISA 9120 UZS",
                "category": "🍔 ЕДА",
                "subcategory": "кофе",
            },
            {
                "amount": 20000,
                "source": "VISA 9120 UZS",
                "category": "🍔 ЕДА",
                "subcategory": "кафе",
            },
        ]
    )
    update, context, sheets = make_chat("две покупки", {}, ai)
    await messages.text_handler(update, context)
    assert sheets.appends == 1
    assert [row[3] for row in sheets.rows] == [10000.0, 20000.0]
    assert all(row[4] == BALANCE_FORMULA for row in sheets.rows)
    assert summary(update).count("✅") == 2


async def test_sheet_failure_is_reported_without_crash() -> None:
    class BrokenSheets(FakeSheets):
        def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
            raise ConnectionError("Google is down")

    update, context, _ = make_chat("48000 латте", manual_state())
    context.bot_data["gs_service"] = BrokenSheets()
    await messages.text_handler(update, context)
    assert summary(update).startswith("❌ Не записал в Google Таблицу (1 шт.)")


SMS = "Pokupka: OOO SHAVI CAFE, 48000.00 UZS, 06.10.2026 12:00, karta *9120"
COFFEE = {
    "amount": 48000,
    "source": "VISA 9120 UZS",
    "category": "🍔 ЕДА",
    "subcategory": "кофе",
    "comment": "Shavi",
}


def again(update: SimpleNamespace, context: SimpleNamespace, text: str):
    """Тот же чат, новое сообщение."""
    fresh, _, _ = make_chat(text, {})
    fresh.effective_chat.send_message = update.effective_chat.send_message
    return fresh, context


def test_fingerprints() -> None:
    assert messages.input_fingerprint("48000 латте") is None  # ручной ввод
    assert messages.input_fingerprint(None, "AgADx") == "file:AgADx"
    same = messages.input_fingerprint("  " + SMS.upper().replace(" ", "  ") + "\n")
    assert same == messages.input_fingerprint(SMS)
    assert messages.input_fingerprint(SMS + " ") != messages.input_fingerprint(
        SMS.replace("48000", "48001")
    )


async def test_same_sms_twice_is_written_once() -> None:
    ai = FakeAI([COFFEE])
    update, context, sheets = make_chat(SMS, {}, ai)
    await messages.text_handler(update, context)
    assert len(sheets.rows) == 1
    menu = menu_after_summary(update)
    first_row = menu.kwargs["reply_markup"].inline_keyboard[0]
    assert [b.callback_data for b in first_row] == ["last:fix", "last:undo"]

    second, _ = again(update, context, SMS)
    await messages.text_handler(second, context)
    assert ai.calls == 1 and len(sheets.rows) == 1
    warning = update.effective_chat.send_message.await_args.args[0]
    assert warning.startswith("⚠️ Это уже записано (строка 4169,")
    second.message.delete.assert_awaited()


async def test_manual_entries_may_repeat() -> None:
    update, context, sheets = make_chat("48000 латте", manual_state())
    await messages.text_handler(update, context)
    context.user_data.update(manual_state())
    second, _ = again(update, context, "48000 латте")
    await messages.text_handler(second, context)
    assert len(sheets.rows) == 2


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        (15_000_000.5, "🟰 VISA 9120 UZS: остаток сходится с банком"),
        (
            14_900_000.0,
            "⚠️ VISA 9120 UZS: в банке 15 000 000, в таблице 14 900 000 "
            "(разница 100 000)",
        ),
    ],
)
async def test_bank_balance_is_compared_with_the_table(
    table: float, expected: str
) -> None:
    ai = FakeAI([{**COFFEE, "balance": "15 000 000,50"}])
    update, context, sheets = make_chat(SMS, {}, ai)
    sheets.table_balances = {"VISA 9120 UZS": table}
    await messages.text_handler(update, context)
    assert expected in summary(update)


async def test_undo_deletes_the_last_write_and_forgets_the_sms() -> None:
    ai = FakeAI([COFFEE])
    update, context, sheets = make_chat(SMS, {}, ai)
    await messages.text_handler(update, context)

    command, _ = again(update, context, "/undo")
    await undo.undo(command, context)
    first, last, rows = sheets.undone
    assert (first, last, [r.amount for r in rows]) == (4169, 4169, [48000.0])
    assert command.message.reply_text.await_args.args[0] == (
        "↩️ Удалил из таблицы: строка 4169."
    )

    # после отмены то же SMS снова записывается
    resent, _ = again(update, context, SMS)
    await messages.text_handler(resent, context)
    assert ai.calls == 2

    nothing, _ = again(update, context, "/undo")
    context.user_data.pop("last_write")
    await undo.undo(nothing, context)
    assert "Нечего отменять" in nothing.message.reply_text.await_args.args[0]


async def test_undo_keeps_rows_changed_in_the_sheet() -> None:
    update, context, sheets = make_chat("48000 латте", manual_state())
    await messages.text_handler(update, context)
    sheets.undo_allowed = False
    command, _ = again(update, context, "/undo")
    await undo.undo(command, context)
    assert "уже изменились" in command.message.reply_text.await_args.args[0]
    assert "last_write" not in context.user_data


# Символы, которые выглядят как пробел: сообщение из них Telegram считает пустым
INVISIBLE = "ㅤ⠀​‌‍⁠﻿"


def visible(text: str) -> bool:
    return bool(text.strip(" \n\t" + INVISIBLE))


async def test_source_button_confirms_the_card_and_shows_categories() -> None:
    update, context, _ = make_chat("VISA 9120 UZS", {})
    await messages.text_handler(update, context)

    first, second = update.effective_chat.send_message.await_args_list
    assert first.args[0] == "💳 VISA 9120 UZS"
    assert second.args[0] == "💳 VISA 9120 UZS — выберите категорию"
    assert context.user_data["source"] == "VISA 9120 UZS"
    keyboard = first.kwargs["reply_markup"].keyboard
    assert any(button.text == "✅ VISA 9120 UZS" for row in keyboard for button in row)


async def test_summary_shows_the_subcategory_icon() -> None:
    update, context, _ = make_chat("48000 латте", manual_state())
    context.bot_data["icons"] = {"кофе": "☕"}
    await messages.text_handler(update, context)
    assert "🍔 ЕДА (☕ кофе) • VISA 9120 UZS" in summary(update)
    assert (
        context.bot_data["last_source"] == "VISA 9120 UZS"
    )  # карта на случай перезапуска


async def test_back_without_a_card_asks_to_choose_one() -> None:
    update, context, _ = make_chat("⬅️ Назад", {})
    await messages.text_handler(update, context)
    [call] = update.effective_chat.send_message.await_args_list
    assert call.args[0] == "Выберите карту:"


@pytest.mark.parametrize(
    ("user_data", "expected"),
    [({"source": "UZCARD 5837 UZS"}, "💳 UZCARD 5837 UZS"), ({}, "Выберите карту:")],
)
async def test_menu_without_a_summary_still_has_text(
    user_data: dict[str, Any], expected: str
) -> None:
    from app.handlers.common import show_main_menu

    update, context, _ = make_chat("", user_data)
    await show_main_menu(update.effective_chat, context)
    assert chat_messages(update)[0].args[0] == expected


def test_no_invisible_message_texts_in_the_code() -> None:
    """«ㅤ» вместо текста: Telegram отвечает Message_empty и кнопка молчит."""
    from pathlib import Path

    for path in Path("app").rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        found = [hex(ord(ch)) for ch in INVISIBLE if ch in source]
        assert not found, f"{path}: {found}"
    assert not visible("ㅤ") and visible("💳 VISA 9120 UZS")


def coffee_row() -> Any:
    today = local_today(config.ANALYTICS_TIMEZONE)
    return entry_row(48000.0, False, "латте", "VISA 9120 UZS", "🍔 ЕДА", "кофе", today)


async def test_save_rows_reports_rows_and_answers_in_the_given_chat() -> None:
    update, context, _ = make_chat("", {})
    result = await messages.save_rows(
        context, update.effective_chat, [coffee_row()], [], "app:1"
    )
    assert (result.written, result.first, result.last) == (1, 4169, 4169)
    assert result.lines[0].startswith("✅ 48 000 UZS • 🍔 ЕДА (кофе)")
    assert summary(update) == "\n".join(result.lines)
    assert context.user_data["seen_inputs"]["app:1"]["rows"] == (4169, 4169)
    assert context.user_data["last_write"]["fingerprint"] == "app:1"


async def test_save_rows_failure_is_reported_and_not_remembered() -> None:
    class BrokenSheets(FakeSheets):
        def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
            raise ConnectionError("Google is down")

    update, context, _ = make_chat("", {})
    context.bot_data["gs_service"] = BrokenSheets()
    result = await messages.save_rows(
        context, update.effective_chat, [coffee_row()], [], "app:2"
    )
    assert (result.written, result.first, result.last) == (0, None, None)
    assert "app:2" not in context.user_data.get("seen_inputs", {})
    assert summary(update).startswith("❌ Не записал в Google Таблицу")


async def test_cards_keyboard_opens_the_mini_app_with_a_fresh_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "WEBAPP_URL", "https://budget.onrender.com/app/")
    update, context, _ = make_chat("48000 латте", manual_state())
    await messages.text_handler(update, context)
    [button] = summary_call(update).kwargs["reply_markup"].keyboard[-1]
    prefix = "https://budget.onrender.com/app/?launch="
    assert button.web_app.url.startswith(prefix)
    token = button.web_app.url.removeprefix(prefix)
    assert check_launch_token(token, config.TELEGRAM_TOKEN).id == 1  # чат make_chat
