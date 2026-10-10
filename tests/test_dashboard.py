"""Данные экрана «Сводка»: те же расчёты, что /today и /subs, и таблица дней."""

from datetime import date

from app.services.analytics_service import AnalyticsService
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
