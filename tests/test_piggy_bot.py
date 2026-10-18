"""Копилки в чате (спека 7): «🐷» после поступления и «Отложил всё»."""

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app import config
from app.domain import SheetRow
from app.handlers import piggy
from tests.test_web_piggy import PiggyAnalytics, PiggySheets

TODAY = date(2026, 10, 17)
OWNER, WIFE = 42, 77
NEW_YEAR = ["p3k9za", "Новый год", 12000000, "UZS", "20.12.2026", "каждый год",
            "🎉 ПРАЗДНИК", "сезонный", "", "", "", "", "Иван", ""]  # fmt: skip
CATALOG = {
    "sources": ["VISA 1111 UZS"],
    "categories": ["🎉 ПРАЗДНИК", "💰 ДОХОДЫ"],
    "subcategories": {"🎉 ПРАЗДНИК": ["сезонный"], "💰 ДОХОДЫ": ["зарплата"]},
}


@pytest.fixture(autouse=True)
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(piggy, "local_today", lambda zone: TODAY)
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({OWNER, WIFE}))
    monkeypatch.setattr(config, "WEBAPP_URL", "https://budget.example/app/")


def context_with(sheets: PiggySheets) -> SimpleNamespace:
    return SimpleNamespace(
        bot_data={
            "gs_service": sheets,
            "analytics_service": PiggyAnalytics(sheets),
            **CATALOG,
        },
        bot=SimpleNamespace(send_message=AsyncMock()),
        user_data={},
    )


def salary(amount: float = 17_000_000) -> SheetRow:
    return SheetRow(TODAY, "💰 ДОХОДЫ", "зарплата", amount, "", "UZS", "VISA 1111 UZS")


def gifts() -> SheetRow:
    return SheetRow(
        TODAY, "🎉 ПРАЗДНИК", "сезонный", 300_000, "", "UZS", "VISA 1111 UZS"
    )


def press(data: str) -> tuple[Any, Any]:
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    update = SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=OWNER, first_name="Иван"),
        effective_chat=SimpleNamespace(id=OWNER),
    )
    return update, query


def told_wife(context: SimpleNamespace) -> list[str]:
    return [
        call.kwargs["text"]
        for call in context.bot.send_message.await_args_list
        if call.kwargs.get("chat_id") == WIFE
    ]


async def test_an_income_asks_to_set_the_piggies_part_aside() -> None:
    context = context_with(PiggySheets([NEW_YEAR], []))
    await piggy.income_prompt(context, OWNER, OWNER, [salary()])
    call = context.bot.send_message.await_args
    assert call.kwargs["chat_id"] == OWNER
    assert call.kwargs["text"] == (
        "🐷 В копилки по плану октября осталось: «Новый год» 6 000 000 UZS"
    )
    [[saved, part]] = call.kwargs["reply_markup"].inline_keyboard
    assert (saved.text, saved.callback_data) == ("Отложил всё", "piggy:all:2026-10")
    assert part.text == "Отложил часть…"
    assert part.web_app.url.startswith("https://budget.example/app/?launch=")
    assert part.web_app.url.endswith("&open=save")


@pytest.mark.parametrize(
    "moves",
    [
        # уже отложено за месяц — откладывать нечего
        [["05.10.2026", "p3k9za", "Новый год", 6000000, "UZS", "отложил", "Иван", ""]],
    ],
)
async def test_nothing_left_says_nothing(moves: list[list[Any]]) -> None:
    context = context_with(PiggySheets([NEW_YEAR], moves))
    await piggy.income_prompt(context, OWNER, OWNER, [salary()])
    context.bot.send_message.assert_not_awaited()


async def test_a_spending_is_not_an_income() -> None:
    context = context_with(PiggySheets([NEW_YEAR], []))
    await piggy.income_prompt(context, OWNER, OWNER, [gifts()])
    context.bot.send_message.assert_not_awaited()


async def test_a_failed_count_says_nothing() -> None:
    context = context_with(PiggySheets([NEW_YEAR], []))
    context.bot_data["analytics_service"].savings = None  # расчёт упадёт
    await piggy.income_prompt(context, OWNER, OWNER, [salary()])
    context.bot.send_message.assert_not_awaited()


async def test_set_all_aside_writes_the_month_part_once() -> None:
    sheets = PiggySheets([NEW_YEAR], [])
    context = context_with(sheets)
    update, query = press("piggy:all:2026-10")
    await piggy.piggy_button(update, context)
    assert sheets.journal is not None
    assert sheets.journal[-1] == ["17.10.2026", "p3k9za", "Новый год", 6_000_000,
                                  "UZS", "отложил", "Иван", ""]  # fmt: skip
    query.edit_message_text.assert_awaited_with(
        "🐷 Записал: «Новый год» 6 000 000 UZS", reply_markup=None
    )
    assert told_wife(context) == ["🐷 Иван — отложил: Новый год 6 000 000 UZS"]
    rows = len(sheets.journal)
    update, query = press("piggy:all:2026-10")
    await piggy.piggy_button(update, context)  # второй раз — ничего не задваивает
    assert len(sheets.journal) == rows
    query.edit_message_text.assert_awaited_with("Уже записано", reply_markup=None)


async def test_last_months_button_is_out_of_date() -> None:
    sheets = PiggySheets([NEW_YEAR], [])
    context = context_with(sheets)
    update, query = press("piggy:all:2026-09")
    await piggy.piggy_button(update, context)
    call = query.edit_message_text.await_args
    assert call.args[0] == "Подсказка за сентябрь устарела: в октябре свой план"
    [[part]] = call.kwargs["reply_markup"].inline_keyboard
    assert part.text == "Отложил часть…" and part.web_app.url.endswith("&open=save")
    assert sheets.journal is not None and len(sheets.journal) == 1  # только шапка
