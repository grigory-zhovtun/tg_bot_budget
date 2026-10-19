"""SMS в чате: правила вместо ИИ, подсказка для новых магазинов, повторы, сбои."""

import asyncio
from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest
from telegram import ReplyKeyboardMarkup

from app import config
from app.domain import Catalog, MerchantBook
from app.handlers import messages
from app.handlers import sms as sms_chat
from tests.test_messages import again, make_chat, menu_after_summary, summary
from tests.test_sms import CODE_SMS, PURCHASE, QR

HEADER = ["Дата", "Категория", "Название", "Сумма", "Баланс", "Комментарий"]
SHAVI_LUNCH = ["01.10.2026", "🍔 ЕДА", "кафе", 90000, "", "OOO SHAVI CAFE", "UZS"]


class RulesAI:
    """ИИ для правил: история fact и подсказки категорий; разбирать SMS нельзя."""

    enabled = True

    def __init__(
        self,
        fact: list[list[Any]] | None = None,
        answer: dict[str, tuple[str, str]] | None = None,
        delay: float = 0.0,
    ) -> None:
        self.fact = [HEADER, *(fact or [])]
        self.answer = answer or {}
        self.delay = delay
        self.asked: list[list[str]] = []
        self.forgotten = 0
        self.calls = 0  # разбор текста ИИ

    def merchant_history(self) -> tuple[MerchantBook, list[list[Any]]]:
        return MerchantBook.from_sheet(self.fact), self.fact

    async def categorize_merchants(
        self, names: list[str], catalog: Catalog
    ) -> dict[str, tuple[str, str]]:
        self.asked.append(names)
        await asyncio.sleep(self.delay)
        return {name: self.answer[name] for name in names if name in self.answer}

    def forget_history(self) -> None:
        self.forgotten += 1

    async def parse_transaction(self, **kwargs: Any) -> list[Any]:
        self.calls += 1
        return []


@pytest.fixture(autouse=True)
def sms_day(monkeypatch: pytest.MonkeyPatch) -> None:
    """Даты образцов SMS — «сегодня», иначе старые даты станут сегодняшними."""
    monkeypatch.setattr(config, "GEMINI_API_KEY", "test")
    monkeypatch.setattr(sms_chat, "local_today", lambda timezone: date(2026, 10, 14))


def reactions(update: SimpleNamespace) -> list[str]:
    return [call.args[0] for call in update.message.set_reaction.await_args_list]


def last_summary(update: SimpleNamespace) -> str:
    calls = update.effective_chat.send_message.await_args_list
    return [
        call.args[0]
        for call in calls
        if isinstance(call.kwargs.get("reply_markup"), ReplyKeyboardMarkup)
    ][-1]


async def test_known_merchant_is_written_without_ai() -> None:
    ai = RulesAI(fact=[SHAVI_LUNCH])
    update, context, sheets = make_chat(PURCHASE, {}, ai)
    await messages.text_handler(update, context)
    [row] = sheets.rows
    assert row[:4] == ["14.10.2026", "🍔 ЕДА", "кафе", 48000.0]
    assert row[5:] == ["OOO SHAVI CAFE>TASHKENT", "UZS", "VISA 9090 UZS"]
    assert (ai.calls, ai.asked, ai.forgotten) == (0, [], 0)
    assert sheets.cells == {"fact!N2": 5000000.0}
    assert summary(update).startswith("✅ 48 000 UZS • 🍔 ЕДА (кафе) • VISA 9090 UZS |")
    assert reactions(update) == ["👀", "👍"]


async def test_new_merchant_gets_one_ai_question_and_the_check() -> None:
    ai = RulesAI(answer={"OOO SHAVI CAFE": ("🍔 ЕДА", "кафе")})
    update, context, sheets = make_chat(PURCHASE, {}, ai)
    await messages.text_handler(update, context)
    assert ai.asked == [["OOO SHAVI CAFE"]]
    assert sheets.rows[0][1:3] == ["🍔 ЕДА", "кафе"]
    assert "• 🆕 OOO SHAVI CAFE>TASHKENT" in summary(update)
    assert menu_after_summary(update).args[0] == (
        "🆕 OOO SHAVI CAFE>TASHKENT • 48 000 UZS • VISA 9090 UZS\n"
        "ИИ предлагает: 🍔 ЕДА / кафе. Верно?"
    )
    assert ai.forgotten == 1 and ai.calls == 0


async def test_several_sms_with_a_code_are_written_in_time_order() -> None:
    ai = RulesAI()
    update, context, sheets = make_chat(f"{PURCHASE}\n{CODE_SMS}\n{QR}", {}, ai)
    await messages.text_handler(update, context)
    assert [row[3] for row in sheets.rows] == [25000.0, 48000.0]  # QR раньше
    assert [row[1:3] for row in sheets.rows] == [
        ["🍔 ЕДА", "кофе"],
        ["🚧 РАЗНОЕ", "неучтенка"],
    ]
    assert summary(update).endswith("⏭ Пропустил: код подтверждения")
    markup = menu_after_summary(update).kwargs["reply_markup"]
    assert markup.inline_keyboard[0][0].text == "🔎 Проверить (2)"


async def test_sms_written_before_is_not_written_again() -> None:
    ai = RulesAI(fact=[SHAVI_LUNCH])
    update, context, sheets = make_chat(PURCHASE, {}, ai)
    await messages.text_handler(update, context)
    second, _ = again(update, context, f"{QR}\n{PURCHASE}")
    await messages.text_handler(second, context)
    assert [row[3] for row in sheets.rows] == [48000.0, 25000.0]
    assert last_summary(second).endswith("⏭ Уже записано раньше: 1 SMS (строка 4169)")


async def test_same_sms_twice_in_one_message_is_written_once() -> None:
    update, context, sheets = make_chat(f"{QR}\n{QR}", {}, RulesAI())
    await messages.text_handler(update, context)
    assert len(sheets.rows) == 1


async def test_sms_with_own_words_goes_to_ai() -> None:
    ai = RulesAI()
    update, context, sheets = make_chat(f"{PURCHASE} струны", {}, ai)
    await messages.text_handler(update, context)
    assert ai.calls == 1 and ai.asked == []


async def test_slow_ai_does_not_stop_the_write(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sms_chat, "SUGGEST_SECONDS", 0.01)
    ai = RulesAI(answer={"OOO SHAVI CAFE": ("🍔 ЕДА", "кафе")}, delay=1)
    update, context, sheets = make_chat(PURCHASE, {}, ai)
    await messages.text_handler(update, context)
    assert sheets.rows[0][1:3] == ["🚧 РАЗНОЕ", "неучтенка"]
    assert "• 🆕 OOO SHAVI CAFE>TASHKENT" in summary(update)


async def test_rules_work_without_gemini(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "GEMINI_API_KEY", None)
    update, context, sheets = make_chat(QR, {}, None)
    await messages.text_handler(update, context)
    assert sheets.rows[0][1:4] == ["🍔 ЕДА", "кофе", 25000.0]


async def test_only_codes_are_skipped_quietly() -> None:
    update, context, sheets = make_chat(CODE_SMS, {}, RulesAI())
    await messages.text_handler(update, context)
    assert sheets.rows == []
    assert summary(update) == "⏭ Пропустил: код подтверждения"
    assert reactions(update) == ["👀", "👍"]


async def test_unknown_card_is_reported() -> None:
    text = PURCHASE.replace("*9090", "*1234")
    update, context, sheets = make_chat(text, {}, RulesAI())
    await messages.text_handler(update, context)
    assert sheets.rows == []
    assert summary(update) == (
        "⚠️ Не записал: OOO SHAVI CAFE — карта *1234 не найдена в листе system"
    )
    assert reactions(update) == ["👀", "🤔"]


async def test_rules_failure_falls_back_to_ai(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(text: str) -> None:
        raise RuntimeError("bug")

    monkeypatch.setattr(sms_chat, "parse_sms", broken)
    ai = RulesAI()
    update, context, _ = make_chat(PURCHASE, {}, ai)
    await messages.text_handler(update, context)
    assert ai.calls == 1
