"""Данные экрана «Сводка»: те же расчёты, что /today и /subs, и таблица дней."""

from datetime import date

import pytest

from app.services.analytics_service import AnalyticsService
from app.services.cards import CardBalance
from app.services.dashboard import build_dashboard, subscriptions
from app.services.day_budget import DayPoint, day_budget, read_daily
from tests.test_analytics_service import serial
from tests.test_day_budget import (
    FACT_HEADER,
    OPENING,
    RATES,
    Sheets,
    fact_row,
    frame,
    plan_row,
    tab,
)
from tests.test_recurring import APPLE, CLAUDE, GOOGLE_ONE, OLD, RENDER, row
from tests.test_recurring import frame as history

TODAY = date(2026, 10, 2)
CAFE = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000)


def test_daily_table_is_read_until_it_ends() -> None:
    assert read_daily(tab()) == [DayPoint(date(2026, 10, 1), 16_900_000, 16_600_000)]
    assert read_daily([["Статья"]]) == []


def test_days_ahead_have_no_fact() -> None:
    rows = tab()
    rows[22][14:17] = [serial(date(2026, 10, 2)), 16_800_000, "#N/A ()"]
    assert read_daily(rows)[-1] == DayPoint(date(2026, 10, 2), 16_800_000, None)


def test_dashboard_limit_is_the_today_limit() -> None:
    fact = frame(CAFE)
    found = build_dashboard(fact, tab(), TODAY, RATES, "USD")
    assert found.status == "ok"
    assert found.budget == day_budget(fact, tab(), TODAY, RATES, "USD")


def test_groups_sum_their_lines_in_sheet_order() -> None:
    found = build_dashboard(frame(), tab(inventory_fact=600_000), TODAY, RATES, "USD")
    assert [(g.name, g.plan, g.fact) for g in found.groups] == [
        ("🍔 ЕДА", 2_600_000, 0),
        ("🏚️ ДОМ", 8_300_000, 600_000),
    ]
    assert [line.subcategory for line in found.groups[1].items] == [
        "инвентарь",
        "квартплата",
    ]


def test_every_upcoming_item_is_listed_by_day() -> None:
    found = build_dashboard(frame(), tab(), date(2026, 10, 16), RATES, "USD")
    # аванс 15-го не пришёл — он впереди, первым
    assert [item.name for item in found.upcoming] == ["Аванс", "Квартплата", "ЗП"]
    paid = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000)
    found = build_dashboard(frame(paid), tab(), date(2026, 10, 16), RATES, "USD")
    assert [item.name for item in found.upcoming] == ["Квартплата", "ЗП"]


def test_without_the_forecast_block_only_groups_remain() -> None:
    rows = [
        ["Статья", "Категория", "Подкатегория"],
        plan_row("🍔 ЕДА", "кафе", 100, 50),
    ]
    found = build_dashboard(frame(), rows, TODAY, RATES, "USD")
    assert (found.status, found.budget, found.daily) == ("no_forecast", None, ())
    assert [(g.name, g.plan, g.fact) for g in found.groups] == [("🍔 ЕДА", 100, 50)]


def test_subscription_states_follow_this_month() -> None:
    gym = [
        row(date(2026, 8, 5), "Fitness Club", 300_000),
        row(date(2026, 9, 5), "Fitness Club", 300_000),
    ]
    found = subscriptions(
        history(*CLAUDE, *APPLE, *RENDER, *GOOGLE_ONE, *OLD, *gym), date(2026, 10, 10)
    )
    assert {(s.name, round(s.uzs)): s.state for s in found} == {
        ("Render.com", 378_560): "charged",
        ("Anthropic* Claude SUB", 1_416_480): "twice",
        ("Apple.com/bill", 41_165): "charged",
        ("Apple.com/bill", 165_712): "expected",
        ("Google *google One", 235_882): "expected",
        ("Fitness Club", 300_000): "missed",
    }  # отменённая подписка (последнее списание > 40 дней) не показывается


def test_service_reads_the_month_tab_and_the_fact() -> None:
    fact = [FACT_HEADER, *OPENING, CAFE]
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    found = service.dashboard(TODAY)
    assert found is not None
    assert found.budget == day_budget(frame(CAFE), tab(), TODAY, RATES, "USD")
    assert AnalyticsService(Sheets({"fact": fact})).dashboard(TODAY) is None


def test_spending_by_group_counts_this_month_from_the_fact_rows() -> None:
    found = build_dashboard(
        frame(
            fact_row(date(2026, 9, 30), "🍔 ЕДА", "кафе", 900_000),  # прошлый месяц
            CAFE,
            fact_row(date(2026, 10, 1), "🍔 ЕДА", "кофе", 30_000),
            fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", -100_000),  # возврат
            fact_row(date(2026, 10, 2), "🏚️ ДОМ", "квартплата", 500, "USD"),
            fact_row(date(2026, 10, 2), "💰 ДОХОДЫ", "зарплата", 5_000_000),
            fact_row(date(2026, 10, 2), "💳 СЧЕТА", "перевод", 1_000_000),
            fact_row(date(2026, 10, 3), "🚌 ТРАНСПОРТ", "такси", 50_000),  # завтра
        ),
        tab(),
        TODAY,
        RATES,
        "USD",
    )
    assert [(g.name, g.amount) for g in found.spent] == [
        ("🏚️ ДОМ", 6_000_000),
        ("🍔 ЕДА", 330_000),
    ]
    assert [(i.subcategory, i.amount) for i in found.spent[1].items] == [
        ("кафе", 300_000),
        ("кофе", 30_000),
    ]


def test_a_group_left_with_refunds_only_is_not_in_the_chart() -> None:
    refund = fact_row(date(2026, 10, 1), "🚑 ЗДОРОВЬЕ", "аптека", -20_000)
    found = build_dashboard(frame(CAFE, refund), tab(), TODAY, RATES, "USD")
    assert [g.name for g in found.spent] == ["🍔 ЕДА"]


def test_spending_on_an_article_missing_from_the_month_tab_is_reported() -> None:
    usd_cafe = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 3.5, "USD")
    taxi = fact_row(date(2026, 10, 2), "🚌 ТРАНСПОРТ", "такси", 50_000)
    found = build_dashboard(frame(CAFE, usd_cafe, taxi), tab(), TODAY, RATES, "USD")
    # вкладка считает факт по паре (подкатегория, валюта): «кафе» у неё только
    # в сумах, «такси» нет вовсе — в план-факт эти траты не попали
    assert [
        (u.group, u.subcategory, u.currency, u.amount) for u in found.untracked
    ] == [
        ("🚌 ТРАНСПОРТ", "такси", "UZS", 50_000),
        ("🍔 ЕДА", "кафе", "USD", pytest.approx(42_000)),
    ]


def test_without_the_forecast_block_the_chart_and_cards_stay() -> None:
    rows = [
        ["Статья", "Категория", "Подкатегория"],
        plan_row("🍔 ЕДА", "кафе", 100, 50),
    ]
    cards = (CardBalance("VISA 1111 UZS", "UZS", 100.0, 100.0, None, None),)
    found = build_dashboard(frame(CAFE), rows, TODAY, RATES, "USD", cards=cards)
    assert found.status == "no_forecast"
    assert [g.name for g in found.spent] == ["🍔 ЕДА"]
    assert found.cards == cards


def test_service_takes_card_balances_from_the_same_fact_read() -> None:
    fact = [list(row) for row in (FACT_HEADER, *OPENING, CAFE)]
    fact[0] += ["Счет", "Текущий баланс"]
    fact[1] += ["VISA 1111 UZS", 4_600_000]
    fact[2] += ["VISA 2222 USD", 1000]
    # у фейка нет get_table_balances: лишнего запроса к таблице сервис не делает
    found = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()})).dashboard(TODAY)
    assert found is not None
    assert [(c.name, c.balance, c.uzs) for c in found.cards] == [
        ("VISA 1111 UZS", 4_600_000, 4_600_000),
        ("VISA 2222 USD", 1000, 12_000_000),
    ]
