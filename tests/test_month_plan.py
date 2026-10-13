"""Вкладка «План»: план месяца из вкладки, подсказки, правки, запись."""

from datetime import date
from typing import Any

import pytest

from app.services.analytics_service import transactions_frame
from app.services.plan import Event, Goal, article_history, read_plan, shift_month
from tests.test_day_budget import FACT_HEADER, RATES, fact_row

HEADER = ["Статья", "Категория", "Подкатегория", "План", "Факт", "Дельта", "Валюта"]
EVENTS = [("Аванс", 15, 3_000_000, "UZS"), ("Квартплата", 18, -500, "USD")]


def article(
    kind: str, group: str, sub: str, plan: float, currency: str = "UZS"
) -> list[Any]:
    """Строка статьи: D — план со знаком минус, G — валюта."""
    return [kind, group, sub, -plan, 0, plan, currency]


def month_tab(
    articles: list[list[Any]],
    events: list[tuple[str, int, float, str]] = EVENTS,
    goal: tuple[float, str] | None = (0, "UZS"),
) -> list[list[Any]]:
    """Вкладка месяца: статьи слева (A:G), блок прогноза справа (O:R) в тех же строках.

    Заголовок списка — строка 11, восемь строк списка — 12–19, цель — 20, таблица
    дней — с 21, как во вкладках таблицы.
    """
    rows: list[list[Any]] = [HEADER, *articles]
    rows += [[] for _ in range(30 - len(rows))]
    rows = [list(row) + [""] * (19 - len(row)) for row in rows]
    rows[10][14:19] = [
        "Поступления (+) и крупные платежи (−)",
        "День",
        "Сумма",
        "Валюта",
        "В сумах",
    ]
    for k, (name, day, amount, currency) in enumerate(events):
        rows[11 + k][14:18] = [name, day, amount, currency]
    if goal is not None:
        rows[19][14:18] = ["🎯 Отложить за месяц (цель накоплений)", "", *goal]
    rows[20][14:18] = ["Дата", "План", "Факт", "Разница"]
    return rows


def test_articles_are_pairs_of_subcategory_and_currency() -> None:
    plan = read_plan(
        month_tab(
            [
                article("Расходы", "🍔 ЕДА", "кофе", 1_500_000),
                article("Расходы", "🍔 ЕДА", "кофе", 20, "USD"),
                article("Доходы", "💰 ДОХОДЫ", "зарплата", 10_000_000),
            ]
        )
    )
    assert [(a.key, a.kind, a.group, a.plan, a.row) for a in plan.articles] == [
        ("кофе|UZS", "spend", "🍔 ЕДА", 1_500_000, 2),
        ("кофе|USD", "spend", "🍔 ЕДА", 20, 3),
        ("зарплата|UZS", "income", "💰 ДОХОДЫ", 10_000_000, 4),
    ]
    assert all(a.editable for a in plan.articles)


def test_header_totals_and_blank_rows_are_not_articles() -> None:
    rows = month_tab([article("Расходы", "🍔 ЕДА", "кафе", 100)])
    rows[2][7:9] = [-100, -40]  # итоговая строка: только суммы H и I
    assert [a.key for a in read_plan(rows).articles] == ["кафе|UZS"]


def test_empty_plan_cell_is_zero_and_currency_defaults_to_sums() -> None:
    rows = month_tab([["Расходы", "🍔 ЕДА", "кафе", "", "", "", ""]])
    [only] = read_plan(rows).articles
    assert (only.key, only.plan) == ("кафе|UZS", 0.0)


def test_a_pair_met_twice_is_read_only() -> None:
    rows = month_tab(
        [
            article("Расходы", "🍔 ЕДА", "кафе", 100),
            article("Расходы", "🍔 ЕДА", "кафе", 200),
        ]
    )
    assert [a.editable for a in read_plan(rows).articles] == [False, False]


def test_events_and_goal_come_from_the_forecast_block() -> None:
    plan = read_plan(month_tab([], goal=(2_000_000, "UZS")))
    assert plan.events is not None and len(plan.events) == 8
    assert plan.events[0] == Event(0, 12, "Аванс", 15, 3_000_000, "UZS")
    assert plan.events[1] == Event(1, 13, "Квартплата", 18, -500, "USD")
    assert plan.events[2] == Event(2, 14, None, None, None, None)
    assert plan.goal == Goal(20, 2_000_000, "UZS")


def test_without_the_forecast_block_events_and_goal_are_unknown() -> None:
    plan = read_plan([HEADER, article("Расходы", "🍔 ЕДА", "кафе", 100)])
    assert (plan.events, plan.goal) == (None, None)


def facts(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, *rows], RATES)


def test_history_is_three_full_months_the_last_one_and_this_month() -> None:
    frame = facts(
        fact_row(date(2026, 6, 30), "🍔 ЕДА", "кофе", 5_000_000),  # до окна
        fact_row(date(2026, 7, 5), "🍔 ЕДА", "кофе", 300_000),
        fact_row(date(2026, 8, 5), "🍔 ЕДА", "кофе", 600_000),
        fact_row(date(2026, 9, 5), "🍔 ЕДА", "кофе", 900_000),
        fact_row(date(2026, 9, 6), "🍔 ЕДА", "кофе", -100_000),  # возврат
        fact_row(date(2026, 10, 2), "🍔 ЕДА", "кофе", 250_000),
        fact_row(date(2026, 10, 12), "🍔 ЕДА", "кофе", 7_000_000),  # после сегодня
    )
    history = article_history(frame, date(2026, 10, 11))
    assert history.average_months == (
        date(2026, 7, 1),
        date(2026, 8, 1),
        date(2026, 9, 1),
    )
    assert (history.last_month, history.current_month, history.current_days) == (
        date(2026, 9, 1),
        date(2026, 10, 1),
        11,
    )
    average, last, current = history.of("кофе|UZS")
    assert average == pytest.approx(1_700_000 / 3)
    assert (last, current) == (800_000, 250_000)


def test_months_without_records_are_zero_and_currencies_apart() -> None:
    frame = facts(
        fact_row(date(2026, 9, 5), "🍔 ЕДА", "кофе", 900_000),
        fact_row(date(2026, 9, 5), "🍔 ЕДА", "кофе", 30, "USD"),
    )
    history = article_history(frame, date(2026, 10, 11))
    assert history.of("кофе|UZS") == (300_000, 900_000, 0)
    assert history.of("кофе|USD") == (10, 30, 0)
    assert history.of("кафе|UZS") == (0, 0, 0)


def test_months_go_back_across_the_new_year() -> None:
    assert shift_month(date(2027, 1, 1), -3) == date(2026, 10, 1)
    assert shift_month(date(2026, 12, 1), 1) == date(2027, 1, 1)
    history = article_history(facts(), date(2027, 1, 3))
    assert history.average_months == (
        date(2026, 10, 1),
        date(2026, 11, 1),
        date(2026, 12, 1),
    )
