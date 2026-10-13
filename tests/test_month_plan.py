"""Вкладка «План»: план месяца из вкладки, подсказки, правки, запись."""

from typing import Any

from app.services.plan import Event, Goal, read_plan

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
