"""Вкладка «План»: план месяца из вкладки, подсказки, правки, запись."""

from datetime import date
from typing import Any

import pytest

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.plan import (
    ArticleChange,
    Event,
    EventCells,
    EventChange,
    EventValue,
    Goal,
    GoalChange,
    PlanChanges,
    applied,
    article_history,
    cell_updates,
    change_note,
    conflicts,
    read_plan,
    shift_month,
    totals,
    unknown,
)
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


TAB = month_tab(
    [
        article("Расходы", "🍔 ЕДА", "кофе", 1_500_000),
        article("Расходы", "🏚️ ДОМ", "квартплата", 500, "USD"),
        article("Доходы", "💰 ДОХОДЫ", "зарплата", 20_000_000),
    ],
    goal=(1_000_000, "UZS"),
)
CURRENCIES = {"UZS", "USD", "RUB"}
EMPTY: EventCells = (None, None, None, None)
ADVANCE: EventCells = ("Аванс", 15, 3_000_000.0, "UZS")
RENT: EventCells = ("Квартплата", 18, -500.0, "USD")


def test_changes_must_point_to_editable_rows_and_known_currencies() -> None:
    plan = read_plan(TAB)
    good = PlanChanges(
        articles=(ArticleChange("кофе|UZS", 1_500_000, 1_800_000),),
        events=(EventChange(2, EMPTY, EventValue("Премия", 25, 1_000_000, "UZS")),),
        goal=GoalChange((1_000_000, "UZS"), (2_000_000, "UZS")),
    )
    assert unknown(plan, good, CURRENCIES) == set()
    bad = PlanChanges(
        articles=(ArticleChange("кальян|UZS", 0, 1),),
        events=(EventChange(30, EMPTY, None),),
        goal=GoalChange((1_000_000, "UZS"), (2_000_000, "EUR")),
    )
    assert unknown(plan, bad, CURRENCIES) == {"rows", "events", "goal"}


def test_the_same_row_twice_in_one_save_is_refused() -> None:
    twice = PlanChanges(
        articles=(
            ArticleChange("кофе|UZS", 1_500_000, 1),
            ArticleChange("кофе|UZS", 1_500_000, 2),
        )
    )
    assert unknown(read_plan(TAB), twice, CURRENCIES) == {"rows"}


def test_read_only_articles_cannot_be_changed() -> None:
    rows = month_tab(
        [
            article("Расходы", "🍔 ЕДА", "кафе", 1),
            article("Расходы", "🍔 ЕДА", "кафе", 2),
        ]
    )
    change = PlanChanges(articles=(ArticleChange("кафе|UZS", 1, 5),))
    assert unknown(read_plan(rows), change, CURRENCIES) == {"rows"}


def test_nothing_conflicts_while_the_tab_is_as_the_user_saw_it() -> None:
    changes = PlanChanges(
        articles=(ArticleChange("кофе|UZS", 1_500_000, 1_800_000),),
        events=(EventChange(0, ADVANCE, None), EventChange(2, EMPTY, None)),
        goal=GoalChange((1_000_000, "UZS"), (0, "UZS")),
    )
    assert conflicts(read_plan(TAB), changes) == []


def test_changes_made_after_opening_are_conflicts() -> None:
    changes = PlanChanges(
        articles=(ArticleChange("кофе|UZS", 1_400_000, 1_800_000),),
        events=(EventChange(0, ("Аванс", 16, 3_000_000.0, "UZS"), None),),
        goal=GoalChange((900_000, "UZS"), (0, "UZS")),
    )
    assert conflicts(read_plan(TAB), changes) == ["кофе|UZS", "event:0", "goal"]


def test_only_the_changed_cells_are_written() -> None:
    changes = PlanChanges(
        articles=(ArticleChange("квартплата|USD", 500, 650.5),),
        events=(
            EventChange(1, RENT, None),
            EventChange(2, EMPTY, EventValue("Премия", 25, 1_000_000, "UZS")),
        ),
        goal=GoalChange((1_000_000, "UZS"), (2_000_000, "UZS")),
    )
    assert cell_updates("Oct 26", read_plan(TAB), changes) == [
        {"range": "'Oct 26'!D3", "values": [[-650.5]]},
        {"range": "'Oct 26'!O13:R13", "values": [["", "", "", ""]]},
        {"range": "'Oct 26'!O14:R14", "values": [["Премия", 25, 1_000_000, "UZS"]]},
        {"range": "'Oct 26'!Q20:R20", "values": [[2_000_000, "UZS"]]},
    ]


def test_zero_plan_is_written_as_zero_not_minus_zero() -> None:
    changes = PlanChanges(articles=(ArticleChange("кофе|UZS", 1_500_000, 0),))
    [cell] = cell_updates("Oct 26", read_plan(TAB), changes)
    assert str(cell["values"][0][0]) == "0.0"


def test_free_money_is_incomes_minus_spending_minus_goal() -> None:
    rates = Rates({"UZS": 1.0, "USD": 12_000.0})
    plan = read_plan(TAB)
    before = totals(plan, rates)
    assert (before.incomes, before.spend, before.goal, before.income_rows) == (
        3_000_000,
        7_500_000,
        1_000_000,
        20_000_000,
    )
    assert before.free == -5_500_000
    changed = applied(
        plan,
        PlanChanges(
            articles=(ArticleChange("кофе|UZS", 1_500_000, 500_000),),
            events=(EventChange(2, EMPTY, EventValue("ЗП", 30, 10_000_000, "UZS")),),
            goal=GoalChange((1_000_000, "UZS"), (0, "UZS")),
        ),
    )
    assert totals(changed, rates).free == 13_000_000 - 6_500_000


def test_the_other_user_gets_what_changed() -> None:
    rates = Rates({"UZS": 1.0, "USD": 12_000.0})
    changes = PlanChanges(
        articles=(
            ArticleChange("кофе|UZS", 1_500_000, 1_800_000),
            ArticleChange("квартплата|USD", 500, 650.5),
        ),
        events=(
            EventChange(1, RENT, None),
            EventChange(2, EMPTY, EventValue("Премия", 25, 1_000_000, "UZS")),
        ),
        goal=GoalChange((1_000_000, "UZS"), (2_000_000, "UZS")),
    )
    text = change_note(
        read_plan(TAB), changes, rates, date(2026, 10, 1), "Иван", {"кофе": "☕"}
    )
    assert text.splitlines() == [
        "✏️ Иван — правки в плане на октябрь:",
        "☕ кофе: 1 500 000 → 1 800 000",
        "квартплата USD: 500 → 650,50",
        "📅 18 · Квартплата −500 USD → удалено",
        "📅 — → 25 · Премия +1 000 000",
        "🎯 Отложить: 1 000 000 → 2 000 000",
        "Свободно: -5,50 млн → -7,61 млн",
    ]


def test_long_lists_are_cut_and_the_author_may_be_unknown() -> None:
    rows = month_tab(
        [article("Расходы", "🍔 ЕДА", f"статья {n}", 100) for n in range(12)]
    )
    changes = PlanChanges(
        articles=tuple(ArticleChange(f"статья {n}|UZS", 100, 200) for n in range(12))
    )
    lines = change_note(
        read_plan(rows), changes, Rates({"UZS": 1.0}), date(2026, 11, 1), None, {}
    ).splitlines()
    assert lines[0] == "✏️ Правки в плане на ноябрь:"
    assert lines[11] == "… и ещё 2"
    assert len(lines) == 13
