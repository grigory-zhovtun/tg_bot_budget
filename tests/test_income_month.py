"""Вопрос «Эти … — за октябрь?» после поступления и пометка месяца кнопкой (спека
1.8). Суммы и карты выдуманные."""

import asyncio
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
SUMS_SALARY = (
    ForecastItem("ЗП за октябрь, остаток", 1, 2_000_000, "UZS", 2_000_000, -1),
)
DEBT = (
    ForecastItem("Возврат долга за октябрь, остаток", 1, 900_000, "UZS", 900_000, -1),
)
WRITTEN = [46330, "💰 ДОХОДЫ", "зарплата", 55000, "", "", "RUB", "VISA 7070 RUB"]


@pytest.fixture(autouse=True)
def november(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(income_month, "local_today", lambda zone: TODAY)


def income(
    comment: str = "",
    currency: str = "RUB",
    day: date = TODAY,
    category: str = "💰 ДОХОДЫ",
    subcategory: str = "зарплата",
) -> SheetRow:
    return SheetRow(
        day, category, subcategory, 55_000.0, comment, currency, f"VISA 7070 {currency}"
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
    """fact в памяти: comment_row пишет, если строка та же; cells — что в ней сейчас."""

    def __init__(
        self,
        unchanged: bool = True,
        cells: list[Any] | None = WRITTEN,
        error: Exception | None = None,
    ) -> None:
        self.unchanged = unchanged
        self.cells = cells
        self.error = error
        self.comments: list[tuple[int, SheetRow, str]] = []

    def comment_row(self, number: int, row: SheetRow, comment: str) -> bool:
        if self.error is not None:
            raise self.error
        self.comments.append((number, row, comment))
        return self.unchanged

    def fact_row_cells(self, number: int) -> list[Any] | None:
        return self.cells


def context_with(
    analytics: Analytics | None = None, sheets: Sheets | None = None
) -> SimpleNamespace:
    return SimpleNamespace(
        bot_data={
            "analytics_service": analytics or Analytics(),
            "gs_service": sheets or Sheets(),
        },
        bot=SimpleNamespace(send_message=AsyncMock(), do_api_request=AsyncMock()),
        user_data={},
    )


def sent(context: SimpleNamespace) -> str:
    """Запасной текст вопроса — bot.send_message(chat_id, text)."""
    chat_id, text = context.bot.send_message.await_args.args
    assert chat_id == OWNER
    return str(text).replace("\xa0", " ")


async def test_a_ruble_income_without_a_mark_asks_which_month() -> None:
    context = context_with()
    await income_month.month_question(context, OWNER, [income()], 4170)
    assert sent(context) == (
        "📅 Эти 55 000 ₽ — за октябрь?\nЗа октябрь ещё ждём: ЗП Питер 55 000 ₽"
    )
    markup = context.bot.send_message.await_args.kwargs["reply_markup"]
    [[october, november]] = markup.inline_keyboard
    assert (october.text, october.callback_data) == ("За октябрь", "mon:4170:2026-10")
    assert (november.text, november.callback_data) == ("За ноябрь", "mon:4170:2026-11")


@pytest.mark.usefixtures("rich")
async def test_the_rich_question_starts_with_a_capital() -> None:
    context = context_with()
    await income_month.month_question(context, OWNER, [income()], 4170)
    method, kwargs = (
        context.bot.do_api_request.await_args.args[0],
        context.bot.do_api_request.await_args.kwargs,
    )
    assert method == "sendRichMessage"
    assert "За октябрь ещё ждём:" in str(kwargs["api_kwargs"]["rich_message"])


@pytest.mark.parametrize(
    "row",
    [
        income(comment="ЗП за октябрь"),  # пометка уже есть
        income(day=date(2026, 10, 31)),  # записана с октябрьской датой — и так октябрю
        income(category="🍔 ЕДА"),  # не поступление
        income(subcategory="нач остаток"),  # служебная строка, не доход
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


async def test_a_salary_in_sums_asks_when_last_months_salary_waits() -> None:
    # сумовая ЗП по SMS приходит без «за <месяц>»: банк пишет пометку только в выписке
    context = context_with(Analytics(SUMS_SALARY))
    await income_month.month_question(context, OWNER, [income(currency="UZS")], 4170)
    assert sent(context).startswith("📅 Эти 55 000 — за октябрь?")


@pytest.mark.parametrize(
    ("row", "rest"),
    [
        (income(currency="UZS", subcategory="кэшбэк"), SUMS_SALARY),  # не зарплата
        (income(currency="UZS"), DEBT),  # прошлый месяц ждёт не зарплату
    ],
)
async def test_other_incomes_in_sums_are_not_asked_about(
    row: SheetRow, rest: tuple[ForecastItem, ...]
) -> None:
    context = context_with(Analytics(rest))
    await income_month.month_question(context, OWNER, [row], 4170)
    context.bot.send_message.assert_not_awaited()


async def test_the_question_does_not_hold_up_the_write() -> None:
    # чтения Google для вопроса — фоном: ответ на «Записать» их не ждёт
    context = context_with()
    income_month.ask_month(context, OWNER, [income()], 4170)
    context.bot.send_message.assert_not_awaited()
    await asyncio.gather(*income_month.TASKS)
    assert sent(context).startswith("📅 Эти 55 000 ₽ — за октябрь?")


async def test_a_write_without_such_incomes_reads_nothing() -> None:
    analytics = Analytics()
    income_month.ask_month(context_with(analytics), OWNER, [income("за октябрь")], 1)
    await asyncio.gather(*income_month.TASKS)
    assert analytics.days == []


async def press(context: SimpleNamespace, data: str) -> tuple[str, Any]:
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=SimpleNamespace(chat_id=OWNER, message_id=7, reply_markup="buttons"),
    )
    await income_month.month_button(SimpleNamespace(callback_query=query), context)
    query.answer.assert_awaited_once()
    call = query.edit_message_text.await_args
    return str(call.args[0]).replace("\xa0", " "), call.kwargs.get("reply_markup")


async def test_the_button_marks_the_row_and_the_last_write() -> None:
    sheets = Sheets()
    context = context_with(sheets=sheets)
    row = income(comment="перевод")
    context.user_data[LAST_WRITE] = {"first": 4170, "last": 4170, "rows": [row]}
    await income_month.month_question(context, OWNER, [row], 4170)
    text, _ = await press(context, "mon:4170:2026-10")
    assert text == "✓ 55 000 ₽ — за октябрь"
    assert sheets.comments == [(4170, row, "перевод за октябрь")]
    # /undo и /fix сверяют строку с записанной — в ней теперь новый комментарий
    assert context.user_data[LAST_WRITE]["rows"][0].comment == "перевод за октябрь"


async def test_the_button_writes_this_month_too() -> None:
    sheets = Sheets()
    context = context_with(sheets=sheets)
    await income_month.month_question(context, OWNER, [income()], 4170)
    assert (await press(context, "mon:4170:2026-11"))[0] == "✓ 55 000 ₽ — за ноябрь"
    assert sheets.comments[0][2] == "за ноябрь"


async def test_a_second_tap_says_the_mark_is_already_there() -> None:
    sheets = Sheets()
    context = context_with(sheets=sheets)
    await income_month.month_question(context, OWNER, [income()], 4170)
    await press(context, "mon:4170:2026-10")
    assert (await press(context, "mon:4170:2026-11"))[0] == (
        "✓ Уже отмечено: строка 4170 — за октябрь"
    )
    assert len(sheets.comments) == 1


async def test_a_row_marked_by_hand_names_its_mark() -> None:
    marked = [*WRITTEN[:5], "ЗП за октябрь", *WRITTEN[6:]]
    context = context_with(sheets=Sheets(unchanged=False, cells=marked))
    await income_month.month_question(context, OWNER, [income()], 4170)
    assert (await press(context, "mon:4170:2026-11"))[0] == (
        "✓ В строке 4170 уже стоит «за октябрь»"
    )


async def test_a_row_changed_by_hand_is_left_alone() -> None:
    edited = [*WRITTEN[:5], "премия", *WRITTEN[6:]]
    context = context_with(sheets=Sheets(unchanged=False, cells=edited))
    await income_month.month_question(context, OWNER, [income()], 4170)
    assert (await press(context, "mon:4170:2026-10"))[0] == (
        "Строку 4170 уже поменяли — пометку не поставил. "
        "Допишите в комментарий строки «за октябрь»"
    )


async def test_a_row_that_is_gone_says_so() -> None:
    context = context_with(sheets=Sheets(unchanged=False, cells=None))  # /undo
    await income_month.month_question(context, OWNER, [income()], 4170)
    assert (await press(context, "mon:4170:2026-10"))[0] == (
        "Строки 4170 уже нет — пометку не поставил"
    )


async def test_a_sheet_that_does_not_answer_keeps_the_buttons() -> None:
    context = context_with(sheets=Sheets(error=ConnectionError("Google is down")))
    await income_month.month_question(context, OWNER, [income()], 4170)
    text, markup = await press(context, "mon:4170:2026-10")
    assert text == "Таблица не ответила — нажмите кнопку ещё раз"
    assert markup == "buttons"


async def test_an_old_question_says_what_to_do() -> None:
    context = context_with()
    assert (await press(context, "mon:4170:2026-10"))[0] == (
        "Вопрос устарел — допишите в комментарий строки «за октябрь»"
    )


async def test_a_failed_question_does_not_break_the_write() -> None:
    class Broken(Analytics):
        def last_months_rest(self, today: date) -> Any:
            raise ConnectionError("Google is down")

    context = context_with(Broken())
    await income_month.month_question(context, OWNER, [income()], 4170)
    context.bot.send_message.assert_not_awaited()
