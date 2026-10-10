"""Регулярные списания: подписки и платежи раз в месяц по истории fact."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.domain import Rates
from app.handlers import analytics
from app.services.analytics_service import AnalyticsService, transactions_frame
from app.services.recurring import (
    find_series,
    format_subs,
    morning_lines,
    recurring_key,
)
from tests.test_analytics_service import serial
from tests.test_day_budget import FACT_HEADER, OPENING, Sheets, tab

RATES = Rates({"UZS": 1.0, "USD": 12000.0})
TODAY = date(2026, 10, 10)


def row(
    day: date, comment: str, amount: float, sub: str = "подписка", cur: str = "UZS"
):
    group = "🌛 ЕЖЕМЕСЯЧНО" if sub == "подписка" else "🏚️ ДОМ"
    source = "VISA 4058 USD" if cur == "USD" else "VISA 9120 UZS"
    return [serial(day), group, sub, amount, "", f"AI: {comment}", cur, source]


def frame(*rows):
    return transactions_frame([FACT_HEADER, *rows], RATES)


@pytest.mark.parametrize(
    ("comment", "key"),
    [
        ("Anthropic* Claude SUB", "anthropic claude"),
        ("Anthropic Claude Sub", "anthropic claude"),
        ("GOOGLE *ChatGPT; ≈ 22.39 USD по курсу", "google chatgpt"),
        ("Google - .4 Chatgpt", "google chatgpt"),
        ("Google *google One", "google one"),
        ("Apple.com/Bill, 3.49 USD (сумма)", "apple com"),
        ("Yandex Plus 3ds no", "yandex plus"),
        ("", ""),
    ],
)
def test_merchant_names_are_normalised(comment: str, key: str) -> None:
    assert recurring_key(comment) == key


CLAUDE = [
    row(date(2026, 7, 3), "Anthropic Claude Sub", 112, cur="USD"),
    row(date(2026, 8, 2), "Anthropic* Claude SUB", 112, cur="USD"),
    row(date(2026, 9, 2), "Anthropic* Claude SUB", 112, cur="USD"),
    row(date(2026, 10, 2), "Anthropic* Claude SUB", 112, cur="USD"),
    row(date(2026, 10, 4), "Anthropic* Claude SUB", 118.04, cur="USD"),
]
APPLE = [
    row(date(2026, 8, 2), "Apple.com/bill", 41_932),
    row(date(2026, 8, 31), "Apple.com/bill", 166_131),
    row(date(2026, 9, 4), "Apple.com/bill", 41_287),
    row(date(2026, 9, 30), "Apple.com/bill", 165_712),
    row(date(2026, 10, 4), "Apple.com/bill", 41_165),
]
RENDER = [  # июнь выбился по дню — смотрим последние три списания
    row(date(2026, 6, 8), "Render.Com", 313_430),
    row(date(2026, 7, 3), "Render.com (хостинг)", 312_650),
    row(date(2026, 8, 4), "Render.com", 313_170),
    row(date(2026, 9, 1), "Render.com", 353_756),
    row(date(2026, 10, 1), "Render.com", 378_560),
]
GOOGLE_ONE = [
    row(date(2026, 8, 14), "Google *google One", 239_980),
    row(date(2026, 9, 14), "Google *google One", 235_882),
]
OLD = [  # отменили: последнее списание больше 40 дней назад
    row(date(2026, 6, 20), "Telegram Premium", 35_990),
    row(date(2026, 7, 20), "Telegram Premium", 35_990),
]
SHOP = [  # обычный магазин: часто и на разные суммы
    row(date(2026, 9, day), "Korzinka", 50_000 + day, "продукты")
    for day in (3, 5, 9, 12)
] + [row(date(2026, 10, day), "Korzinka", 50_000 + day, "продукты") for day in (3, 6)]


CAFE = [  # кафе раз в месяц на похожую сумму, но кроме этого — ещё визиты
    row(date(2026, month, 23), "Ooo Shavi Cafe", 237_000, "кафе") for month in (8, 9)
] + [
    row(date(2026, 9, day), "Ooo Shavi Cafe", amount, "кафе")
    for day, amount in ((3, 48_000), (11, 95_000), (17, 61_000))
]
GENERIC = [  # общие комментарии — не магазин
    row(date(2026, month, 4), comment, 60_600, "неучтенка")
    for month in (8, 9, 10)
    for comment in ("перевод на чужую карту UZCARD", "без мерчанта")
]


def series_by_name(found):
    return {(s.key, round(s.last.uzs)): s for s in found}


def test_monthly_charges_are_found_and_shops_are_not() -> None:
    found = find_series(
        frame(*CLAUDE, *APPLE, *RENDER, *GOOGLE_ONE, *OLD, *SHOP, *CAFE, *GENERIC),
        TODAY,
    )
    by_key = series_by_name(found)
    assert set(by_key) == {
        ("anthropic claude", 1_416_480),
        ("apple com", 41_165),
        ("apple com", 165_712),
        ("render com", 378_560),
        ("google one", 235_882),
        ("telegram premium", 35_990),
    }
    assert by_key[("apple com", 41_165)].day == 4
    assert by_key[("apple com", 165_712)].day == 30
    assert by_key[("render com", 378_560)].day == 1
    assert by_key[("telegram premium", 35_990)].lapsed(TODAY)
    claude = by_key[("anthropic claude", 1_416_480)]
    assert [c.day for c in claude.charged_in(TODAY)] == [
        date(2026, 10, 2),
        date(2026, 10, 4),
    ]


def test_subs_report_lists_by_day_with_totals_and_warnings() -> None:
    found = find_series(frame(*CLAUDE, *APPLE, *RENDER, *GOOGLE_ONE, *OLD), TODAY)
    text = format_subs(found, TODAY, subscriptions_plan=1_100_000)
    lines = text.splitlines()
    assert lines[0] == "🔁 Регулярные списания ≈ 2,24 млн в месяц"
    assert lines[1] == "из них подписки ≈ 2,24 млн при плане 1,10 млн"
    assert "• 01 — Render.com: 378 560" in lines
    assert (
        "• 02 — Anthropic* Claude SUB: 118,04 USD ≈ 1,42 млн "
        "⚠️ в октябре дважды: 02.10, 04.10"
    ) in lines
    assert "• 14 — Google *google One: 235 882 (ждём 14.10)" in lines
    assert lines[-1] == "Давно не было (отменили?): Telegram Premium — последнее 20.07"


def test_morning_mentions_charges_due_today_and_a_double_charge_yesterday() -> None:
    found = find_series(frame(*CLAUDE, *GOOGLE_ONE), date(2026, 10, 13))
    assert morning_lines(found, date(2026, 10, 13)) == [
        "🔁 Завтра спишется: Google *google One ≈ 235 882"
    ]
    assert morning_lines(found, date(2026, 10, 14)) == [
        "🔁 Сегодня спишется: Google *google One ≈ 235 882"
    ]
    assert morning_lines(
        find_series(frame(*CLAUDE), date(2026, 10, 5)), date(2026, 10, 5)
    ) == ["⚠️ Anthropic* Claude SUB списан второй раз за октябрь: 02.10, 04.10"]


def test_service_and_command_show_the_report() -> None:
    fact = [FACT_HEADER, *OPENING, *GOOGLE_ONE]
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    assert service.subs_report(TODAY).startswith(
        "🔁 Регулярные списания ≈ 235 882 в месяц"
    )
    brief = service.morning_brief(date(2026, 10, 13))
    assert brief.splitlines()[-1] == "🔁 Завтра спишется: Google *google One ≈ 235 882"
    empty = AnalyticsService(Sheets({"fact": [FACT_HEADER, *OPENING], "Oct 26": tab()}))
    assert (
        empty.subs_report(TODAY)
        == "Регулярных списаний пока не нашёл — нужно хотя бы два месяца истории."
    )


async def test_subs_command_replies() -> None:
    update = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
    context = SimpleNamespace(
        bot_data={"analytics_service": SimpleNamespace(subs_report=lambda: "🔁")}
    )
    await analytics.subs_command(update, context)
    update.message.reply_text.assert_awaited_once_with("🔁")


@pytest.mark.parametrize(("month", "word"), [(3, "в марте"), (8, "в августе")])
def test_double_charge_names_the_month_properly(month: int, word: str) -> None:
    charges = [
        row(date(2026, month - 1, 2), "Anthropic Claude Sub", 112, cur="USD"),
        row(date(2026, month, 2), "Anthropic Claude Sub", 112, cur="USD"),
        row(date(2026, month, 4), "Anthropic Claude Sub", 112, cur="USD"),
    ]
    today = date(2026, month, 10)
    assert f"⚠️ {word} дважды: 02.{month:02d}, 04.{month:02d}" in format_subs(
        find_series(frame(*charges), today), today
    )


def test_missed_charge_this_month_is_noted() -> None:
    jetbrains = [
        row(date(2026, 8, 4), "Jetbrains", 146_520),
        row(date(2026, 9, 4), "Jetbrains - .55", 144_444),
    ]
    text = format_subs(find_series(frame(*jetbrains), TODAY), TODAY)
    assert "• 04 — Jetbrains - .55: 144 444 (в октябре не было)" in text
