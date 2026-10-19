"""«🐷» расширенным сообщением: сколько отложить, из какой копилки ушло, правки."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from telegram import InlineKeyboardMarkup

from app.domain import SheetRow
from app.handlers import piggy
from app.rich import NBSP, heading, in_currency, note_report, para, table, whole
from tests.test_piggy_bot import (  # noqa: F401 — фикстуры settings и december
    ADVANCE,
    NEW_YEAR,
    OWNER,
    TODAY,
    bought,
    came,
    context_with,
    december,
    fact,
    press,
    salary,
    saved_in,
    settings,
    shopping,
)
from tests.test_web_piggy import PiggySheets

pytestmark = pytest.mark.usefixtures("rich", "settings")

# та же копилка, что «Новый год», но с «&» в названии
TRIP = ["t7m2qa", "Отпуск & море", *NEW_YEAR[2:]]


def rich_bot(context: SimpleNamespace) -> AsyncMock:
    context.bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=9))
    return context.bot.do_api_request


def sent_html(api: AsyncMock, method: str = "sendRichMessage") -> dict[str, Any]:
    calls = [c for c in api.await_args_list if c.args == (method,)]
    assert calls, f"no {method}"
    return calls[-1].kwargs["api_kwargs"]


async def test_an_income_says_how_much_to_set_aside_and_where() -> None:
    context = context_with(
        PiggySheets([TRIP], [], percent=10), ADVANCE, came(17, 8_000_000)
    )
    api = rich_bot(context)
    await piggy.income_prompt(context, OWNER, OWNER, [salary(8_000_000)])
    sent = sent_html(api)
    assert sent["rich_message"]["html"] == (
        heading(f"🐷 Отложить {whole(800_000)}", 2)
        + para(f"10{NBSP}% с поступления {whole(8_000_000)}")
        + table([["Отпуск & море", whole(1_200_000)]], "lr", ["копилка", "сумма"])
    )
    assert isinstance(sent["reply_markup"], InlineKeyboardMarkup)


async def test_a_dollar_income_is_set_aside_in_dollars() -> None:
    context = context_with(
        PiggySheets([TRIP], [], percent=10), ADVANCE, came(17, 8_000_000)
    )
    api = rich_bot(context)
    pay = SheetRow(TODAY, "💰 ДОХОДЫ", "зарплата", 1_000, "", "USD", "VISA 2222 USD")
    await piggy.income_prompt(context, OWNER, OWNER, [pay])
    assert sent_html(api)["rich_message"]["html"].startswith(
        heading(f"🐷 Отложить {in_currency(100, 'USD')}", 2)
        + para(
            f"10{NBSP}% с поступления {in_currency(1_000, 'USD')}",
            f"≈ {whole(1_200_000)} сум",
        )
    )


async def test_all_to_the_cushion_has_no_buttons() -> None:
    once = [*NEW_YEAR[:5], "один раз", *NEW_YEAR[6:]]
    collected = ["30.09.2026", "p3k9za", "Новый год", 12000000, "UZS", "отложил",
                 "Иван", ""]  # fmt: skip
    context = context_with(
        PiggySheets([once], [collected], percent=10), came(17, 12_000_000)
    )
    api = rich_bot(context)
    await piggy.income_prompt(context, OWNER, OWNER, [salary(12_000_000)])
    sent = sent_html(api)
    assert sent["rich_message"]["html"] == heading(
        f"🐷 Отложить {whole(1_200_000)}", 2
    ) + para(f"10{NBSP}% с поступления {whole(12_000_000)}", "всё в подушку")
    assert "reply_markup" not in sent


@pytest.mark.usefixtures("december")
async def test_a_spending_says_what_is_left_in_big_letters() -> None:
    context = shopping(
        [NEW_YEAR], [saved_in("p3k9za", "Новый год", 6_000_000)], fact(2_400_000)
    )
    api = rich_bot(context)
    await piggy.spend_note(context, OWNER, [bought(2_400_000)])
    sent = sent_html(api)
    assert sent["rich_message"]["html"] == heading(
        f"🐷 Из копилки −{whole(2_400_000)}"
    ) + para(f"«Новый год» — осталось {whole(3_600_000)}")
    assert isinstance(sent["reply_markup"], InlineKeyboardMarkup)


async def test_set_all_aside_edits_the_message_in_big_letters() -> None:
    context = context_with(PiggySheets([NEW_YEAR], [], percent=10), ADVANCE)
    api = rich_bot(context)
    update, query = press("piggy:all:2026-10")
    await piggy.piggy_button(update, context)
    edit = sent_html(api, "editMessageText")
    assert (edit["chat_id"], edit["message_id"]) == (OWNER, 5)
    assert edit["rich_message"]["html"] == (
        note_report("🐷 Записал: «Новый год» 400 000 UZS").html
    )
    query.edit_message_text.assert_not_awaited()
