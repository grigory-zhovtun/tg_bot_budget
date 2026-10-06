"""Сценарии чата целиком: ручной ввод и разбор SMS → строки листа fact."""

from itertools import count
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app import config
from app.domain import Rates, local_today
from app.handlers import messages
from app.services.google_sheets import BALANCE_FORMULA, row_values

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

    def update_balances(self, balances: dict[str, float]) -> list[str]:
        known = {k: v for k, v in balances.items() if k in self.BALANCE_CELLS}
        self.cells.update(
            {f"fact!{self.BALANCE_CELLS[k]}": v for k, v in known.items()}
        )
        return list(known)

    def get_rates(self) -> Rates:
        return Rates({"UZS": 1.0, "USD": 12000.0, "RUB": 150.0})


class FakeAI:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.calls = 0

    async def parse_transaction(self, **kwargs: Any) -> Any:
        self.calls += 1
        assert kwargs["known_subcategories"] == SUBCATEGORIES
        return self.result


ids = count(100)


def sent_message() -> SimpleNamespace:
    return SimpleNamespace(message_id=next(ids))


def make_chat(text: str, user_data: dict[str, Any], ai: FakeAI | None = None):
    sheets = FakeSheets()
    reply = AsyncMock(side_effect=lambda *a, **k: sent_message())
    message = SimpleNamespace(
        photo=[], text=text, caption=None, delete=AsyncMock(), reply_text=reply
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
        bot=SimpleNamespace(delete_message=AsyncMock()),
    )
    return update, context, sheets


def summary(update: SimpleNamespace) -> str:
    return update.effective_message.reply_text.await_args_list[0].args[0]


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
