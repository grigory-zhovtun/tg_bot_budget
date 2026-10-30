"""Вопрос «Эти … — за октябрь?» после поступления не в сумах и пометка месяца
кнопкой (спека 1.8). Суммы и карты выдуманные."""

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.domain import SheetRow
from app.handlers import income_month
from app.handlers.common import LAST_WRITE
from app.services.day_budget import ForecastItem

OWNER = 42
TODAY = date(2026, 11, 4)
REST = (ForecastItem("ЗП Питер за октябрь, остаток", 1, 55_000, "RUB", 8_250_000, -1),)


@pytest.fixture(autouse=True)
def november(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(income_month, "local_today", lambda zone: TODAY)


def income(
    comment: str = "",
    currency: str = "RUB",
    day: date = TODAY,
    category: str = "💰 ДОХОДЫ",
) -> SheetRow:
    return SheetRow(
        day, category, "зарплата", 55_000.0, comment, currency, f"VISA 7070 {currency}"
    )


class Analytics:
    """Остатки прошлого месяца — как их посчитал бы AnalyticsService."""

    def __init__(self, rest: tuple[ForecastItem, ...] = REST) -> None:
        self.rest = rest
        self.days: list[date] = []

    def last_months_rest(self, today: date) -> tuple[ForecastItem, ...]:
        self.days.append(today)
        return self.rest


class Sheets:
    def __init__(self, unchanged: bool = True) -> None:
        self.unchanged = unchanged
        self.comments: list[tuple[int, SheetRow, str]] = []

    def comment_row(self, number: int, row: SheetRow, comment: str) -> bool:
        self.comments.append((number, row, comment))
        return self.unchanged


def context_with(
    analytics: Analytics | None = None, sheets: Sheets | None = None
) -> SimpleNamespace:
    return SimpleNamespace(
        bot_data={
            "analytics_service": analytics or Analytics(),
            "gs_service": sheets or Sheets(),
        },
        bot=SimpleNamespace(send_message=AsyncMock()),
        user_data={},
    )


async def test_a_ruble_income_without_a_mark_asks_which_month() -> None:
    context = context_with()
    await income_month.month_question(context, OWNER, [income()], 4170)
    call = context.bot.send_message.await_args
    chat_id, sent = call.args  # запасной текст — bot.send_message(chat_id, text)
    assert chat_id == OWNER
    assert sent.replace("\xa0", " ") == (
        "📅 Эти 55 000 ₽ — за октябрь?\nЗа октябрь ещё ждём: ЗП Питер 55 000 ₽"
    )
    [[october, november]] = call.kwargs["reply_markup"].inline_keyboard
    assert (october.text, october.callback_data) == ("За октябрь", "mon:4170:2026-10")
    assert (november.text, november.callback_data) == ("За ноябрь", "mon:4170:2026-11")


@pytest.mark.parametrize(
    "row",
    [
        income(currency="UZS"),  # сумы: месяц ЗП пишет банк («за октябрь»)
        income(comment="ЗП за октябрь"),  # пометка уже есть
        income(day=date(2026, 10, 31)),  # записана с октябрьской датой — и так октябрю
        income(category="🍔 ЕДА"),  # не поступление
    ],
)
async def test_no_question_when_the_month_is_already_clear(row: SheetRow) -> None:
    context = context_with()
    await income_month.month_question(context, OWNER, [row], 4170)
    context.bot.send_message.assert_not_awaited()


async def test_no_question_when_last_month_waits_for_nothing_in_that_currency() -> None:
    dollars = (
        ForecastItem("Фриланс за октябрь, остаток", 1, 300, "USD", 3_600_000, -1),
    )
    context = context_with(Analytics(dollars))
    await income_month.month_question(context, OWNER, [income()], 4170)
    context.bot.send_message.assert_not_awaited()


async def press(context: SimpleNamespace, data: str) -> str:
    query = SimpleNamespace(
        data=data, answer=AsyncMock(), edit_message_text=AsyncMock()
    )
    await income_month.month_button(SimpleNamespace(callback_query=query), context)
    query.answer.assert_awaited_once()
    return str(query.edit_message_text.await_args.args[0]).replace("\xa0", " ")


async def test_the_button_marks_the_row_and_the_last_write() -> None:
    sheets = Sheets()
    context = context_with(sheets=sheets)
    row = income(comment="перевод")
    context.user_data[LAST_WRITE] = {"first": 4170, "last": 4170, "rows": [row]}
    await income_month.month_question(context, OWNER, [row], 4170)
    assert await press(context, "mon:4170:2026-10") == "✓ 55 000 ₽ — за октябрь"
    assert sheets.comments == [(4170, row, "перевод за октябрь")]
    # /undo и /fix сверяют строку с записанной — в ней теперь новый комментарий
    assert context.user_data[LAST_WRITE]["rows"][0].comment == "перевод за октябрь"


async def test_the_button_writes_this_month_too() -> None:
    sheets = Sheets()
    context = context_with(sheets=sheets)
    await income_month.month_question(context, OWNER, [income()], 4170)
    assert await press(context, "mon:4170:2026-11") == "✓ 55 000 ₽ — за ноябрь"
    assert sheets.comments[0][2] == "за ноябрь"


async def test_a_row_changed_by_hand_is_left_alone() -> None:
    context = context_with(sheets=Sheets(unchanged=False))
    await income_month.month_question(context, OWNER, [income()], 4170)
    assert await press(context, "mon:4170:2026-10") == (
        "Строку уже поменяли — пометку не поставил. "
        "Допишите в комментарий строки «за октябрь»"
    )


async def test_an_old_question_says_what_to_do() -> None:
    context = context_with()
    assert await press(context, "mon:4170:2026-10") == (
        "Вопрос устарел — допишите в комментарий строки «за октябрь»"
    )


async def test_a_failed_question_does_not_break_the_write() -> None:
    class Broken(Analytics):
        def last_months_rest(self, today: date) -> Any:
            raise ConnectionError("Google is down")

    context = context_with(Broken())
    await income_month.month_question(context, OWNER, [income()], 4170)
    context.bot.send_message.assert_not_awaited()
