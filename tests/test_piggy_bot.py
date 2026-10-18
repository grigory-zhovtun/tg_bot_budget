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


def salary(amount: float = 20_000_000) -> SheetRow:
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


# --- «🐷 Из копилки» после траты и «Не из копилки» (спека 7.2) ---

from app.services.piggy import fact_key, fingerprint  # noqa: E402
from tests.test_day_budget import serial  # noqa: E402

SHOPPING = date(2026, 12, 18)  # окно «Нового года» к 20.12 открыто


def saved_in(piggy_id: str, name: str, amount: float) -> list[Any]:
    return ["30.10.2026", piggy_id, name, amount, "UZS", "отложил", "Иван", ""]


def fact(amount: float, sub: str = "сезонный", group: str = "🎉 ПРАЗДНИК") -> list[Any]:
    return [serial(SHOPPING), group, sub, amount, "", "", "UZS", "VISA 1111 UZS"]


def bought(
    amount: float, sub: str = "сезонный", group: str = "🎉 ПРАЗДНИК"
) -> SheetRow:
    return SheetRow(SHOPPING, group, sub, amount, "", "UZS", "VISA 1111 UZS")


def key_of(amount: float) -> str:
    return fact_key(SHOPPING, "🎉 ПРАЗДНИК", "сезонный", amount, "UZS",
                    "VISA 1111 UZS", "")  # fmt: skip


class CountedSheets(PiggySheets):
    reads = 0

    def get_piggy(self, max_age: float = 0.0) -> tuple[list[Any], list[Any]]:
        self.reads += 1
        return super().get_piggy(max_age)


def shopping(
    piggies: list[list[Any]], moves: list[list[Any]], *spent: list[Any]
) -> SimpleNamespace:
    sheets = CountedSheets(piggies, moves)
    return SimpleNamespace(
        bot_data={
            "gs_service": sheets,
            "analytics_service": PiggyAnalytics(sheets, *spent),
            **CATALOG,
        },
        bot=SimpleNamespace(send_message=AsyncMock()),
        user_data={},
    )


@pytest.fixture
def december(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(piggy, "local_today", lambda zone: SHOPPING)


@pytest.mark.usefixtures("december")
async def test_a_spending_from_the_piggy_says_what_is_left() -> None:
    context = shopping(
        [NEW_YEAR], [saved_in("p3k9za", "Новый год", 6_000_000)], fact(2_400_000)
    )
    lines = await piggy.spend_note(context, OWNER, [bought(2_400_000)])
    assert lines == ["🐷 Из копилки «Новый год»: −2 400 000 UZS, осталось 3 600 000"]
    call = context.bot.send_message.await_args
    assert (call.kwargs["chat_id"], call.kwargs["text"]) == (OWNER, lines[0])
    [[button]] = call.kwargs["reply_markup"].inline_keyboard
    assert (button.text, button.callback_data) == (
        "Не из копилки",
        f"piggy:no:p3k9za:{fingerprint(key_of(2_400_000))}",
    )
    assert len(button.callback_data.encode()) <= 64


@pytest.mark.usefixtures("december")
async def test_a_spending_over_the_balance_takes_what_there_was() -> None:
    context = shopping(
        [NEW_YEAR], [saved_in("p3k9za", "Новый год", 600_000)], fact(2_400_000)
    )
    lines = await piggy.spend_note(context, OWNER, [bought(2_400_000)])
    assert lines == [
        "🐷 Из копилки «Новый год»: 600 000 UZS — всё, что было, "
        "остальное — обычные траты"
    ]


@pytest.mark.usefixtures("december")
async def test_two_piggies_of_one_article_share_one_message() -> None:
    tree = ["k7tree", "Ёлка", 5000000, "UZS", "31.12.2026", "каждый год",
            "🎉 ПРАЗДНИК", "сезонный", "", "", "", "", "Иван", ""]  # fmt: skip
    context = shopping(
        [NEW_YEAR, tree],
        [
            saved_in("p3k9za", "Новый год", 1_000_000),
            saved_in("k7tree", "Ёлка", 5_000_000),
        ],
        fact(2_400_000),
    )
    lines = await piggy.spend_note(context, OWNER, [bought(2_400_000)])
    assert lines == [
        "🐷 Из копилки «Новый год»: −1 000 000 UZS, осталось 0",
        "🐷 Из копилки «Ёлка»: −1 400 000 UZS, осталось 3 600 000",
    ]
    call = context.bot.send_message.await_args
    assert call.kwargs["text"] == "\n".join(lines)


@pytest.mark.usefixtures("december")
async def test_several_rows_get_a_line_per_piggy_without_a_button() -> None:
    context = shopping(
        [NEW_YEAR],
        [saved_in("p3k9za", "Новый год", 6_000_000)],
        fact(1_000_000),
        fact(500_000),
    )
    lines = await piggy.spend_note(context, OWNER, [bought(1_000_000), bought(500_000)])
    assert lines == ["🐷 Из копилки «Новый год»: −1 500 000 UZS, осталось 4 500 000"]
    assert context.bot.send_message.await_args.kwargs["reply_markup"] is None


@pytest.mark.usefixtures("december")
async def test_an_ordinary_spending_reads_nothing_more() -> None:
    context = shopping(
        [NEW_YEAR],
        [saved_in("p3k9za", "Новый год", 6_000_000)],
        fact(48_000, "кафе", "🍔 ЕДА"),
    )
    for _ in range(2):
        assert (
            await piggy.spend_note(context, OWNER, [bought(48_000, "кафе", "🍔 ЕДА")])
            == []
        )
    sheets = context.bot_data["gs_service"]
    assert sheets.reads == 1  # статьи копилок — один раз, потом из памяти
    assert context.bot_data["analytics_service"].ages == []
    context.bot.send_message.assert_not_awaited()


@pytest.mark.usefixtures("december")
async def test_not_from_the_piggy_is_marked_once() -> None:
    context = shopping(
        [NEW_YEAR], [saved_in("p3k9za", "Новый год", 6_000_000)], fact(2_400_000)
    )
    sheets = context.bot_data["gs_service"]
    data = f"piggy:no:p3k9za:{fingerprint(key_of(2_400_000))}"
    update, query = press(data)
    await piggy.piggy_button(update, context)
    assert sheets.journal[-1] == ["18.12.2026", "p3k9za", "Новый год", 0.0, "UZS",
                                  "не из копилки", "Иван", key_of(2_400_000)]  # fmt: skip
    query.edit_message_text.assert_awaited_with(
        "🐷 Не из копилки — учтено", reply_markup=None
    )
    assert told_wife(context) == [
        "🐷 Иван — не из копилки: 18.12 ПРАЗДНИК / сезонный 2 400 000"
    ]
    rows = len(sheets.journal)
    update, query = press(data)
    await piggy.piggy_button(update, context)
    assert len(sheets.journal) == rows
    query.edit_message_text.assert_awaited_with("Уже учтено", reply_markup=None)


@pytest.mark.usefixtures("december")
async def test_a_changed_row_is_not_marked() -> None:
    context = shopping(
        [NEW_YEAR], [saved_in("p3k9za", "Новый год", 6_000_000)], fact(2_400_000)
    )
    sheets = context.bot_data["gs_service"]
    rows = len(sheets.journal)
    update, query = press(f"piggy:no:p3k9za:{fingerprint(key_of(2_500_000))}")
    await piggy.piggy_button(update, context)
    assert len(sheets.journal) == rows
    query.edit_message_text.assert_awaited_with(
        "Запись изменилась — отметьте в «Накоплениях»", reply_markup=None
    )


@pytest.mark.usefixtures("december")
async def test_not_from_takes_the_spending_off_both_piggies() -> None:
    tree = ["k7tree", "Ёлка", 5000000, "UZS", "31.12.2026", "каждый год",
            "🎉 ПРАЗДНИК", "сезонный", "", "", "", "", "Иван", ""]  # fmt: skip
    context = shopping(
        [NEW_YEAR, tree],
        [
            saved_in("p3k9za", "Новый год", 1_000_000),
            saved_in("k7tree", "Ёлка", 5_000_000),
        ],
        fact(2_400_000),
    )
    update, _ = press(f"piggy:no:p3k9za:{fingerprint(key_of(2_400_000))}")
    await piggy.piggy_button(update, context)
    screen = (
        context.bot_data["analytics_service"]
        .savings(SHOPPING, *[{"UZS"}, CATALOG["categories"], CATALOG["subcategories"]])
        .screen
    )
    assert all(state.balance.spends == () for state in screen.piggies)


# --- финальное ревью этапа 3 ---


@pytest.mark.usefixtures("december")
async def test_the_same_spending_twice_counts_only_the_new_row() -> None:
    # утром уже купили то же на ту же сумму — сообщение о новой строке
    context = shopping(
        [NEW_YEAR],
        [saved_in("p3k9za", "Новый год", 6_000_000)],
        fact(1_000_000),
        fact(1_000_000),
    )
    lines = await piggy.spend_note(context, OWNER, [bought(1_000_000)])
    assert lines == ["🐷 Из копилки «Новый год»: −1 000 000 UZS, осталось 4 000 000"]
    [[button]] = context.bot.send_message.await_args.kwargs[
        "reply_markup"
    ].inline_keyboard
    assert button.callback_data == (
        f"piggy:no:p3k9za:{fingerprint(key_of(1_000_000) + '#2')}"
    )


@pytest.mark.usefixtures("december")
async def test_a_statement_spending_gets_no_button() -> None:
    context = shopping(
        [NEW_YEAR], [saved_in("p3k9za", "Новый год", 6_000_000)], fact(2_400_000)
    )
    lines = await piggy.spend_note(context, OWNER, [bought(2_400_000)], button=False)
    assert lines == ["🐷 Из копилки «Новый год»: −2 400 000 UZS, осталось 3 600 000"]
    assert context.bot.send_message.await_args.kwargs["reply_markup"] is None


async def test_set_all_aside_after_a_part_writes_the_rest() -> None:
    # часть отложили в приложении — «Отложил всё» дописывает только остаток
    sheets = PiggySheets(
        [NEW_YEAR],
        [["05.10.2026", "p3k9za", "Новый год", 2000000, "UZS", "отложил", "Иван", ""]],
    )
    context = context_with(sheets)
    update, query = press("piggy:all:2026-10")
    await piggy.piggy_button(update, context)
    assert sheets.journal is not None
    assert sheets.journal[-1][3] == 4_000_000
    query.edit_message_text.assert_awaited_with(
        "🐷 Записал: «Новый год» 4 000 000 UZS", reply_markup=None
    )
