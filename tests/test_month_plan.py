"""Вкладка «План»: план месяца из вкладки, подсказки, правки, запись."""

import re
import time
from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from gspread.exceptions import WorksheetNotFound

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.google_sheets import GoogleSheetsService
from app.services.plan import (
    Addable,
    ArticleChange,
    Event,
    EventCells,
    EventChange,
    EventValue,
    Goal,
    GoalChange,
    NewArticle,
    PlanChanges,
    addable,
    applied,
    article_history,
    catalog_pairs,
    cell_updates,
    change_note,
    conflicts,
    insert_requests,
    kind_of,
    load_plan,
    read_plan,
    save_plan,
    shift_month,
    totals,
    totals_row,
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


CATALOG = {
    "🍔 ЕДА": ["кофе", "ресторан"],
    "🏚️ ДОМ": ["квартплата"],
    "💳 СЧЕТА": ["переводы"],
    "💰 ДОХОДЫ": ["зарплата", "нач остаток"],
}
RESTAURANT = NewArticle("🍔 ЕДА", "ресторан", "UZS", 500_000)


def test_articles_come_from_the_catalog_without_transfers_and_opening_balance() -> None:
    assert catalog_pairs(CATALOG) == [
        ("🍔 ЕДА", "кофе"),
        ("🍔 ЕДА", "ресторан"),
        ("🏚️ ДОМ", "квартплата"),
        ("💰 ДОХОДЫ", "зарплата"),
    ]
    assert (kind_of("💰 ДОХОДЫ"), kind_of("🍔 ЕДА")) == ("income", "spend")


def test_addable_pairs_are_what_the_tab_lacks_with_their_history() -> None:
    history = article_history(
        facts(fact_row(date(2026, 9, 5), "🍔 ЕДА", "ресторан", 600_000)),
        date(2026, 10, 11),
    )
    found = addable(read_plan(TAB), CATALOG, CURRENCIES, history)
    assert [(item.subcategory, item.currency) for item in found] == [
        ("кофе", "RUB"),
        ("кофе", "USD"),
        ("ресторан", "UZS"),
        ("ресторан", "RUB"),
        ("ресторан", "USD"),
        ("квартплата", "UZS"),
        ("квартплата", "RUB"),
        ("зарплата", "RUB"),
        ("зарплата", "USD"),
    ]
    assert found[2] == Addable("🍔 ЕДА", "ресторан", "UZS", 200_000, 600_000, 0)


def test_new_articles_must_come_from_the_catalog_in_a_known_currency() -> None:
    plan = read_plan(TAB)

    def check(*added: NewArticle) -> set[str]:
        return unknown(plan, PlanChanges(added=added), CURRENCIES, CATALOG)

    assert check(RESTAURANT) == set()
    assert check(replace(RESTAURANT, subcategory="суши")) == {"new_rows"}
    assert check(replace(RESTAURANT, group="🏚️ ДОМ")) == {"new_rows"}
    assert check(replace(RESTAURANT, currency="EUR")) == {"new_rows"}
    assert check(NewArticle("💳 СЧЕТА", "переводы", "UZS", 1)) == {"new_rows"}
    assert check(NewArticle("💰 ДОХОДЫ", "нач остаток", "UZS", 1)) == {"new_rows"}
    assert check(RESTAURANT, replace(RESTAURANT, plan=1)) == {"new_rows"}  # пара дважды
    assert unknown(plan, PlanChanges(added=(RESTAURANT,)), CURRENCIES) == {"new_rows"}
    empty = read_plan(month_tab([]))  # копировать формулы не с чего
    assert unknown(empty, PlanChanges(added=(RESTAURANT,)), CURRENCIES, CATALOG) == {
        "new_rows"
    }


def test_new_rows_stay_where_p9_counts_the_plan() -> None:
    full = read_plan(
        month_tab([article("Расходы", "🍔 ЕДА", f"статья {n}", 1) for n in range(58)])
    )
    assert max(a.row for a in full.articles) == 59
    coffee = NewArticle("🍔 ЕДА", "кофе", "RUB", 5)
    assert unknown(full, PlanChanges(added=(RESTAURANT,)), CURRENCIES, CATALOG) == set()
    two = PlanChanges(added=(RESTAURANT, coffee))
    assert unknown(full, two, CURRENCIES, CATALOG) == {"space"}


def test_a_pair_added_meanwhile_is_a_conflict() -> None:
    coffee = NewArticle("🍔 ЕДА", "кофе", "UZS", 900_000)  # кофе в сумах уже во вкладке
    changes = PlanChanges(added=(coffee, RESTAURANT))
    assert conflicts(read_plan(TAB), changes) == ["кофе|UZS"]
    assert changes.count() == 2


def test_the_note_lists_new_articles_and_free_money_counts_them() -> None:
    changes = PlanChanges(added=(RESTAURANT, NewArticle("🍔 ЕДА", "кофе", "USD", 20)))
    text = change_note(
        read_plan(TAB),
        changes,
        Rates({"UZS": 1.0, "USD": 12_000.0}),
        date(2026, 10, 1),
        "Иван",
        {"ресторан": "🍷"},
    )
    assert text.splitlines() == [
        "✏️ Иван — правки в плане на октябрь:",
        "＋ 🍷 ресторан: 500 000",
        "＋ кофе USD: 20",
        "Свободно: -5,50 млн → -6,24 млн",
    ]


class PlanSheets:
    """Вкладки в памяти: get_values, get_rates; write_cells меняет те же строки."""

    def __init__(
        self, tabs: dict[str, list[list[Any]]], fact: list[list[Any]] | None = None
    ) -> None:
        self.tabs = {name: [list(row) for row in rows] for name, rows in tabs.items()}
        self.tabs["fact"] = fact or [FACT_HEADER]
        self.writes: list[list[dict[str, Any]]] = []
        self.applied: list[list[dict[str, Any]]] = []
        self.profile = ""  # блок «О нас» листа system
        self.delay = 0.0  # пауза между чтением вкладки и записью — окно гонки
        self.ages: list[float] = []  # max_age каждого чтения

    def get_values(self, name: str, max_age: float = 0.0) -> list[list[Any]]:
        self.ages.append(max_age)
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return [list(row) for row in self.tabs[name]]

    def get_advisor_profile(self) -> str:
        return self.profile

    def get_calendar(self, max_age: float = 60.0) -> list[list[Any]]:
        self.ages.append(max_age)
        return []

    def get_rates(self, max_age: float = 0.0) -> Rates:
        self.ages.append(max_age)
        time.sleep(
            self.delay
        )  # save_plan: вкладка уже прочитана, запись ещё не сделана
        return Rates({"UZS": 1.0, "USD": 12_000.0, "RUB": 150.0})

    def write_cells(self, data: list[dict[str, Any]]) -> None:
        self.writes.append(data)
        for item in data:
            found = re.fullmatch(r"'(.+)'!([A-Z])(\d+)(?::[A-Z]\d+)?", item["range"])
            assert found, item["range"]
            title, column, row = found.group(1), found.group(2), int(found.group(3))
            cells = self.tabs[title][row - 1]
            for offset, value in enumerate(item["values"][0]):
                cells[ord(column) - ord("A") + offset] = value

    def sheet_id(self, name: str) -> int:
        return list(self.tabs).index(name)

    def get_formulas(self, name: str, cells: str) -> list[list[Any]]:
        found = re.fullmatch(r"H(\d+):H(\d+)", cells)
        assert found, cells
        rows = self.tabs[name]
        first, last = int(found.group(1)), int(found.group(2))
        return [[rows[n - 1][7]] for n in range(first, min(last, len(rows)) + 1)]

    def apply_requests(self, requests: list[dict[str, Any]]) -> None:
        """insertRange, copyPaste и updateCells — как таблица, только A–L."""
        self.applied.append(requests)
        for request in requests:
            [(kind, body)] = request.items()
            grid = body.get("range") or body["destination"]
            rows = self.tabs[list(self.tabs)[grid["sheetId"]]]
            row, left = grid["startRowIndex"], grid["startColumnIndex"]
            if kind == "insertRange":
                width = grid["endColumnIndex"]
                rows.append([""] * len(rows[0]))
                for index in range(len(rows) - 1, row, -1):
                    rows[index][:width] = rows[index - 1][:width]
                rows[row][:width] = [""] * width
            elif kind == "copyPaste":
                width = grid["endColumnIndex"]
                source = body["source"]["startRowIndex"]
                rows[row][:width] = list(rows[source][:width])
            else:
                for offset, cell in enumerate(body["rows"][0]["values"]):
                    [value] = cell["userEnteredValue"].values()
                    rows[row][left + offset] = value


def test_load_plan_reads_the_tab_rates_and_history() -> None:
    sheets = PlanSheets(
        {"Oct 26": TAB},
        [FACT_HEADER, fact_row(date(2026, 9, 5), "🍔 ЕДА", "кофе", 900_000)],
    )
    loaded = load_plan(sheets, "Oct 26", date(2026, 10, 11))
    assert loaded is not None
    assert [a.key for a in loaded.plan.articles] == [
        "кофе|UZS",
        "квартплата|USD",
        "зарплата|UZS",
    ]
    assert loaded.rates.to_uzs["USD"] == 12_000
    assert loaded.history.of("кофе|UZS") == (300_000, 900_000, 0)


def test_load_plan_without_the_tab_is_none() -> None:
    assert load_plan(PlanSheets({}), "Nov 26", date(2026, 10, 11)) is None


def test_load_plan_may_take_copies_of_the_sheets() -> None:
    sheets = PlanSheets({"Oct 26": TAB})
    assert load_plan(sheets, "Oct 26", date(2026, 10, 11), max_age=120) is not None
    # вкладка, курсы и fact; «Календарь» плану не нужен
    assert sheets.ages == [120, 120, 120]


def test_save_plan_writes_and_the_tab_shows_the_new_plan() -> None:
    sheets = PlanSheets({"Oct 26": TAB})
    changes = PlanChanges(articles=(ArticleChange("кофе|UZS", 1_500_000, 1_800_000),))
    outcome = save_plan(sheets, "Oct 26", changes)
    assert (outcome.invalid, outcome.conflicts) == (frozenset(), ())
    assert outcome.plan.articles[0].plan == 1_500_000  # вкладка до записи
    assert read_plan(sheets.get_values("Oct 26")).articles[0].plan == 1_800_000


def tab_with_totals() -> list[list[Any]]:
    """TAB с итоговой строкой 5 под статьями 2–4."""
    rows = [list(row) for row in TAB]
    rows[4][7:9] = ["=SUM(H2:H4)", "=SUM(I2:I4)"]
    return rows


def grid(row: int, first: int = 0, last: int = 12) -> dict[str, int]:
    return {
        "sheetId": 7,
        "startRowIndex": row - 1,
        "endRowIndex": row,
        "startColumnIndex": first,
        "endColumnIndex": last,
    }


def cells(row: int, first: int, *values: dict[str, Any]) -> dict[str, Any]:
    return {
        "updateCells": {
            "range": grid(row, first, first + len(values)),
            "rows": [{"values": [{"userEnteredValue": value} for value in values]}],
            "fields": "userEnteredValue",
        }
    }


def test_a_new_row_shifts_only_the_article_columns() -> None:
    bonus = NewArticle("💰 ДОХОДЫ", "премия", "UZS", 1_000_000)
    assert insert_requests(7, read_plan(TAB), (bonus,), 5) == [
        {"insertRange": {"range": grid(5), "shiftDimension": "ROWS"}},
        {
            "copyPaste": {
                "source": grid(4),
                "destination": grid(5),
                "pasteType": "PASTE_NORMAL",
            }
        },
        cells(
            5,
            0,
            {"stringValue": "Доходы"},
            {"stringValue": "💰 ДОХОДЫ"},
            {"stringValue": "премия"},
            {"numberValue": -1_000_000.0},
        ),
        cells(5, 6, {"stringValue": "UZS"}),
        # строка сразу под последней статьёй: SUM(H2:H4) её бы не увидел
        cells(6, 7, {"formulaValue": "=SUM(H2:H5)"}, {"formulaValue": "=SUM(I2:I5)"}),
    ]


def test_an_article_of_a_group_without_rows_goes_after_its_kind() -> None:
    misc = NewArticle("🚧 РАЗНОЕ", "неучтенка", "UZS", 1)
    [insert, *_] = insert_requests(7, read_plan(TAB), (misc,), None)
    assert insert["insertRange"]["range"] == grid(4)  # под последним расходом


def test_totals_row_is_the_sum_formula_under_the_articles() -> None:
    assert totals_row([["=SUM(H2:H40)"]], 41) == 41
    assert totals_row([[""], ["= sum(h2:h40)"]], 41) == 42
    assert totals_row([[123], []], 41) is None
    assert totals_row([["=SUM(H$2:H$40)"]], 41) == 41


def test_two_new_rows_of_one_group_stand_one_under_another() -> None:
    coffee_rub = NewArticle("🍔 ЕДА", "кофе", "RUB", 5)
    requests = insert_requests(7, read_plan(TAB), (RESTAURANT, coffee_rub), None)
    inserts = [r["insertRange"]["range"] for r in requests if "insertRange" in r]
    copies = [r["copyPaste"]["source"] for r in requests if "copyPaste" in r]
    assert inserts == [grid(3), grid(4)]  # кофе — 2, ресторан — 3, кофе RUB — 4
    assert copies == [grid(2), grid(3)]  # формулы второй — со строки первой


def test_a_tab_without_a_totals_row_is_told_in_the_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sheets = PlanSheets({"Oct 26": TAB})  # итоговой строки нет
    outcome = save_plan(sheets, "Oct 26", PlanChanges(added=(RESTAURANT,)), CATALOG)
    assert (outcome.invalid, outcome.conflicts) == (frozenset(), ())
    [requests] = sheets.applied
    assert not any("=SUM(" in str(r) for r in requests)
    assert "No totals row under the articles of Oct 26" in caplog.text


def test_new_rows_are_inserted_and_the_sums_go_to_the_shifted_rows() -> None:
    sheets = PlanSheets({"Oct 26": tab_with_totals()})
    changes = PlanChanges(
        articles=(ArticleChange("зарплата|UZS", 20_000_000, 21_000_000),),
        added=(RESTAURANT, NewArticle("💰 ДОХОДЫ", "зарплата", "USD", 300)),
    )
    outcome = save_plan(sheets, "Oct 26", changes, CATALOG)
    assert (outcome.invalid, outcome.conflicts) == (frozenset(), ())
    tab = sheets.get_values("Oct 26")
    assert [(a.key, a.row, a.plan) for a in read_plan(tab).articles] == [
        ("кофе|UZS", 2, 1_500_000),
        ("ресторан|UZS", 3, 500_000),
        ("квартплата|USD", 4, 500),
        ("зарплата|UZS", 5, 21_000_000),
        ("зарплата|USD", 6, 300),
    ]
    assert tab[6][7:9] == ["=SUM(H2:H6)", "=SUM(I2:I6)"]  # итог — строка 7
    assert tab[11][14:18] == ["Аванс", 15, 3_000_000, "UZS"]  # прогноз на месте
    assert len(sheets.applied) == 1  # обе вставки — один batch_update
    assert sheets.writes == [[{"range": "'Oct 26'!D5", "values": [[-21_000_000.0]]}]]


def test_a_lost_reply_does_not_insert_twice() -> None:
    class Lost(PlanSheets):
        def apply_requests(self, requests: list[dict[str, Any]]) -> None:
            super().apply_requests(requests)
            raise ConnectionError("the reply was lost")  # вставка прошла, ответа нет

    sheets = Lost({"Oct 26": tab_with_totals()})
    changes = PlanChanges(added=(RESTAURANT,))
    with pytest.raises(ConnectionError):
        save_plan(sheets, "Oct 26", changes, CATALOG)
    again = save_plan(sheets, "Oct 26", changes, CATALOG)
    assert again.conflicts == ("ресторан|UZS",)
    assert len(sheets.applied) == 1


def test_a_row_inserted_above_by_hand_still_gets_the_write() -> None:
    sheets = PlanSheets({"Oct 26": TAB})
    tab = sheets.tabs["Oct 26"]
    tab.insert(1, ["Расходы", "🚌 ТРАНСПОРТ", "такси", -1, 0, 1, "UZS"] + [""] * 12)
    changes = PlanChanges(articles=(ArticleChange("кофе|UZS", 1_500_000, 1_800_000),))
    save_plan(sheets, "Oct 26", changes)
    assert sheets.writes[0][0]["range"] == "'Oct 26'!D3"  # кофе съехал на строку 3


def test_save_plan_with_a_conflict_writes_nothing() -> None:
    sheets = PlanSheets({"Oct 26": TAB})
    changes = PlanChanges(articles=(ArticleChange("кофе|UZS", 1_000_000, 1_800_000),))
    outcome = save_plan(sheets, "Oct 26", changes)
    assert outcome.conflicts == ("кофе|UZS",)
    assert sheets.writes == []


def test_save_plan_with_unknown_rows_writes_nothing() -> None:
    sheets = PlanSheets({"Oct 26": TAB})
    changes = PlanChanges(articles=(ArticleChange("кальян|UZS", 0, 1),))
    outcome = save_plan(sheets, "Oct 26", changes)
    assert outcome.invalid == frozenset({"rows"})
    assert sheets.writes == []


def test_write_cells_sends_one_raw_request() -> None:
    book = SimpleNamespace(values_batch_update=Mock())
    service = GoogleSheetsService.__new__(GoogleSheetsService)
    service.client, service.sheet = None, book
    service._worksheets, service._balance_cells = {}, None
    data = [{"range": "'Oct 26'!D2", "values": [[-1.0]]}]
    service.write_cells(data)
    service.write_cells([])
    book.values_batch_update.assert_called_once_with(
        {"valueInputOption": "RAW", "data": data}
    )
