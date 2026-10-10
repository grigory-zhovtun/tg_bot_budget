"""Живой статус: реакции на SMS, «Думаю…» вместо «🔍», /advice по ходу, 🎉 утром."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, TelegramError

from app import config
from app.handlers import analytics, live, messages
from app.services.analytics_service import AnalyticsService
from app.services.day_budget import celebrate
from tests.test_day_budget import FACT_HEADER, OPENING, Sheets, budget, fact_row, tab
from tests.test_messages import COFFEE, SMS, FakeAI, make_chat


@pytest.fixture(autouse=True)
def gemini_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "GEMINI_API_KEY", "test")


def reactions(update: SimpleNamespace) -> list[str]:
    return [call.args[0] for call in update.message.set_reaction.await_args_list]


async def test_sms_gets_eyes_then_thumbs_up_and_stays_in_the_chat() -> None:
    update, context, sheets = make_chat(SMS, {}, FakeAI([COFFEE]))
    await messages.text_handler(update, context)
    assert len(sheets.rows) == 1
    assert reactions(update) == ["👀", "👍"]
    update.message.delete.assert_not_awaited()  # SMS остаётся с реакцией
    draft = context.bot.send_message_draft.await_args
    assert draft.args[0] == 1 and draft.args[2] == ""  # «Думаю…»
    sent = [call.args[0] for call in update.effective_chat.send_message.await_args_list]
    assert "🔍" not in sent


async def test_nothing_recognised_gets_a_thinking_face() -> None:
    update, context, sheets = make_chat("привет", {}, FakeAI([]))
    await messages.text_handler(update, context)
    assert sheets.rows == []
    assert reactions(update) == ["👀", "🤔"]


async def test_old_clients_get_the_magnifier_instead_of_the_draft() -> None:
    update, context, _ = make_chat(SMS, {}, FakeAI([COFFEE]))
    context.bot.send_message_draft = AsyncMock(side_effect=TelegramError("no drafts"))
    await messages.text_handler(update, context)
    context.bot.send_message.assert_awaited_once_with(1, "🔍")


async def test_live_helpers_never_break_the_flow() -> None:
    message = SimpleNamespace(set_reaction=AsyncMock(side_effect=TelegramError("off")))
    await live.react(message, "👍")  # реакции выключены в чате — не страшно


class StreamingAI:
    def __init__(self, chunks: list[str], fail: bool = False) -> None:
        self.chunks, self.fail = chunks, fail
        self.full_calls = 0

    async def stream_analysis(self, numbers: str):
        for text in self.chunks:
            yield text
        if self.fail:
            raise ConnectionError("stream broke")

    async def analyze_finances(self, numbers: str) -> str:
        self.full_calls += 1
        return "📊 полный ответ"


def advice_chat(ai: StreamingAI) -> tuple[SimpleNamespace, SimpleNamespace]:
    update = SimpleNamespace(
        message=SimpleNamespace(message_id=55, reply_text=AsyncMock()),
        effective_chat=SimpleNamespace(id=1),
    )
    context = SimpleNamespace(
        bot_data={
            "ai_service": ai,
            "analytics_service": SimpleNamespace(advice_context=lambda: "цифры"),
        },
        bot=SimpleNamespace(send_message_draft=AsyncMock()),
    )
    return update, context


async def test_advice_is_typed_as_it_is_generated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(analytics, "DRAFT_EVERY_SECONDS", 0)
    ai = StreamingAI(["📊 Ана", "📊 Анализ готов"])
    update, context = advice_chat(ai)
    await analytics.advice_command(update, context)
    drafts = [call.args[2] for call in context.bot.send_message_draft.await_args_list]
    assert drafts == ["", "📊 Ана", "📊 Анализ готов"]
    assert update.message.reply_text.await_args.args[0] == "📊 Анализ готов"
    assert ai.full_calls == 0


async def test_advice_falls_back_when_streaming_breaks() -> None:
    ai = StreamingAI(["📊 Ана"], fail=True)
    update, context = advice_chat(ai)
    await analytics.advice_command(update, context)
    assert ai.full_calls == 1
    assert update.message.reply_text.await_args.args[0] == "📊 полный ответ"


def test_celebrate_when_yesterday_stayed_within_the_limit() -> None:
    calm = budget(
        date(2026, 10, 2), fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 50_000)
    )
    assert celebrate(calm) is True
    spree = budget(
        date(2026, 10, 2), fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000)
    )
    assert celebrate(spree) is False
    assert celebrate(budget(date(2026, 10, 1))) is False  # вчера — прошлый месяц


async def test_morning_message_gets_the_party_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "CELEBRATE_EFFECT_ID", "42")
    bot = SimpleNamespace(send_message=AsyncMock())
    service = SimpleNamespace(morning_message=lambda: ("☀️", True))
    await analytics.send_morning_brief(bot, 7, service)
    bot.send_message.assert_awaited_once_with(7, "☀️", message_effect_id="42")

    bot = SimpleNamespace(
        send_message=AsyncMock(side_effect=[BadRequest("EFFECT_ID_INVALID"), None])
    )
    await analytics.send_morning_brief(bot, 7, service)
    assert bot.send_message.await_args_list[-1].args == (7, "☀️")  # без эффекта


def test_service_tells_whether_to_celebrate() -> None:
    fact = [
        FACT_HEADER,
        *OPENING,
        fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 50_000),
    ]
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    text, party = service.morning_message(date(2026, 10, 2))
    assert party is True and text.startswith("☀️ Пятница, 2 октября")
