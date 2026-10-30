"""Советник «Плана»: контекст черновика для модели и проверка её ответа."""

from datetime import date

import pytest
from gspread.exceptions import WorksheetNotFound

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.day_budget import day_budget
from app.services.plan import (
    Article,
    ArticleChange,
    PlanChanges,
    article_history,
    read_plan,
)
from app.services.plan_advice import (
    AdviceContext,
    AdvicePoint,
    Purchases,
    advice_numbers,
    checked_points,
    full_months,
    gather_advice,
    monthly_sums,
    purchases,
    unplanned,
)
from tests.test_analytics_service import serial
from tests.test_day_budget import FACT_HEADER, fact_row
from tests.test_day_budget import RATES as BUDGET_RATES
from tests.test_day_budget import frame as budget_frame
from tests.test_day_budget import tab as budget_tab
from tests.test_month_plan import CATALOG, TAB, PlanSheets, article, month_tab

RATES = Rates({"UZS": 1.0, "USD": 12_000.0, "RUB": 150.0})
COFFEE = Article("кофе|UZS", "spend", "🍔 ЕДА", "кофе", "UZS", 1_500_000, 2)
RENT = Article("квартплата|USD", "spend", "🏚️ ДОМ", "квартплата", "USD", 500, 3)
TODAY = date(2026, 10, 11)


def bought(day: date, sub: str, amount: float, place: str, group: str = "🍔 ЕДА"):
    """Строка fact с магазином в комментарии, как пишет разбор SMS."""
    return [serial(day), group, sub, amount, "", f"AI: {place}", "UZS", "VISA 9090 UZS"]


def history_frame():
    return transactions_frame(
        [
            FACT_HEADER,
            bought(date(2026, 8, 5), "кофе", 50_000, "Starbucks"),
            bought(date(2026, 9, 10), "кофе", 30_000, "Shavi Cafe"),
            bought(date(2026, 9, 12), "кофе", 30_000, "Shavi Cafe"),
            bought(date(2026, 9, 20), "кофе", 30_000, "Shavi Cafe"),
            bought(date(2026, 9, 21), "кофе", -20_000, "Shavi Cafe"),  # возврат
            bought(date(2026, 9, 25), "кино", 300_000, "Cinema", "⛱️ ОТДЫХ"),
            bought(date(2026, 10, 1), "кофе", 999, "Shavi Cafe"),  # текущий месяц
        ],
        RATES,
    )


def test_full_months_are_counted_back_from_today() -> None:
    assert full_months(TODAY, 3) == (
        date(2026, 7, 1),
        date(2026, 8, 1),
        date(2026, 9, 1),
    )


def test_monthly_sums_keep_refunds_and_empty_months() -> None:
    sums = monthly_sums(history_frame(), full_months(TODAY, 3))
    assert sums["кофе|UZS"] == (0, 50_000, 70_000)
    assert sums["кино|UZS"] == (0, 0, 300_000)


def test_purchases_count_the_average_check_and_the_places() -> None:
    found = purchases(history_frame(), full_months(TODAY, 3))
    assert found["кофе|UZS"] == Purchases(
        4, 35_000, (("SHAVI CAFE", 90_000), ("STARBUCKS", 50_000))
    )


def test_spending_outside_the_plan_is_listed_in_sums() -> None:
    found = unplanned(history_frame(), full_months(TODAY, 3), read_plan(TAB))
    assert found == (("⛱️ ОТДЫХ / кино", 300_000),)


def context(**over) -> AdviceContext:
    frame = history_frame()
    months = full_months(TODAY, 3)
    draft = read_plan(TAB)
    values = {
        "draft": draft,
        "month": date(2026, 10, 1),
        "today": TODAY,
        "rates": RATES,
        "history": article_history(frame, TODAY),
        "months": months,
        "by_month": monthly_sums(frame, months),
        "old_plans": {date(2026, 9, 1): {"кофе|UZS": 1_200_000}},
        "purchases": purchases(frame, months),
        "unplanned": unplanned(frame, months, draft),
        "budget": None,
        "profile": "Семья из четырёх, Ташкент.\nНа детях не экономим.",
    }
    return AdviceContext(**{**values, **over})


def test_the_model_gets_the_family_the_month_and_every_article() -> None:
    lines = advice_numbers(context()).splitlines()
    assert lines[:4] == [
        "О семье (написали сами):",
        "Семья из четырёх, Ташкент.",
        "На детях не экономим.",
        "",
    ]
    assert "План на текущий месяц (октябрь 2026): до конца месяца 21 дн." in lines
    assert "Курсы: 1 RUB = 150 UZS; 1 USD = 12000 UZS" in lines
    assert "Свободно после цели: -5500000 UZS" in lines
    assert "15 | +3000000 UZS" in lines and "18 | -500.00 USD" in lines
    assert (
        "кофе|UZS | 🍔 ЕДА / кофе | расход | 1500000 | 40000 | 70000 | 999 | UZS"
        in lines
    )
    assert "кофе|UZS | 0 | 50000 | 70000" in lines  # факт июль–сентябрь
    assert "кофе|UZS | 1200000" in lines  # план сентября из вкладки
    assert "кофе|UZS | 4 | 35000 | SHAVI CAFE 90000; STARBUCKS 50000" in lines
    assert "⛱️ ОТДЫХ / кино | 300000" in lines
    assert not any(line.startswith("Сейчас") for line in lines)  # без «Сводки»


def test_without_notes_the_model_is_told_so() -> None:
    assert advice_numbers(context(profile="")).splitlines()[1] == "— не заполнено"


def test_the_next_month_is_planned_for_all_its_days() -> None:
    text = advice_numbers(context(month=date(2026, 11, 1)))
    assert "План на следующий месяц (ноябрь 2026), 30 дн." in text.splitlines()


def test_the_current_month_pace_and_money_come_from_the_dashboard() -> None:
    spent = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 400_000)
    budget = day_budget(
        budget_frame(spent), budget_tab(), date(2026, 10, 2), BUDGET_RATES, "USD"
    )
    lines = advice_numbers(context(budget=budget, today=date(2026, 10, 2))).splitlines()
    assert "Потрачено за 1 дн.: 400000 UZS при плане 100000 UZS" in lines
    assert "Можно в день до зарплаты 30-го: 227586 UZS" in lines
    assert any(
        line.startswith("Сумы на картах и наличными: 4600000 UZS") for line in lines
    )


def test_apply_stays_only_for_editable_articles_with_a_sane_new_sum() -> None:
    editable = {article.key: article for article in (COFFEE, RENT)}
    points = [
        AdvicePoint("Кофе — как в среднем.", "кофе|UZS", 1_834_567),
        AdvicePoint("Квартплата в долларах.", "квартплата|USD", 512.346),
        AdvicePoint("Кальяна в плане нет.", "кальян|UZS", 100_000),
        AdvicePoint("Отрицательная сумма.", "кофе|UZS", -5),
        AdvicePoint("  "),
        AdvicePoint("Как было.", "кофе|UZS", 1_500_200),
        AdvicePoint("Шестой пункт — лишний."),
    ]
    assert checked_points(points, editable) == [
        AdvicePoint("Кофе — как в среднем.", "кофе|UZS", 1_835_000),
        AdvicePoint("Квартплата в долларах.", "квартплата|USD", 512.35),
        AdvicePoint("Кальяна в плане нет."),
        AdvicePoint("Отрицательная сумма."),
        AdvicePoint("Как было."),  # до тысяч — 1 500 000, то же, что в черновике
    ]


def test_the_context_is_gathered_from_the_tabs_the_fact_and_the_profile() -> None:
    september = month_tab([article("Расходы", "🍔 ЕДА", "кофе", 1_200_000)])
    fact = [FACT_HEADER, fact_row(date(2026, 9, 5), "🍔 ЕДА", "кофе", 900_000)]
    sheets = PlanSheets({"Oct 26": TAB, "Sep 26": september}, fact)
    sheets.profile = "Семья из четырёх"
    changes = PlanChanges(articles=(ArticleChange("кофе|UZS", 1_500_000, 1_700_000),))
    gathered = gather_advice(
        sheets, date(2026, 10, 1), TODAY, lambda plan: changes, CATALOG
    )
    found = gathered.context
    assert gathered.invalid == frozenset() and found is not None
    assert [(a.key, a.plan) for a in found.draft.articles][0] == ("кофе|UZS", 1_700_000)
    assert found.old_plans == {
        date(2026, 9, 1): {"кофе|UZS": 1_200_000}
    }  # июля, августа нет
    assert len(found.months) == 12 and found.by_month["кофе|UZS"][-1] == 900_000
    assert found.budget is not None and found.profile == "Семья из четырёх"


def test_the_next_month_is_advised_with_this_month_pace() -> None:
    sheets = PlanSheets({"Nov 26": TAB, "Oct 26": TAB})
    gathered = gather_advice(
        sheets, date(2026, 11, 1), TODAY, lambda plan: PlanChanges(), CATALOG
    )
    assert gathered.context is not None
    assert gathered.context.month == date(2026, 11, 1)
    assert gathered.context.budget is not None  # темп и деньги — октября


def test_changes_the_tab_does_not_know_stop_the_advisor() -> None:
    sheets = PlanSheets({"Oct 26": TAB})
    changes = PlanChanges(articles=(ArticleChange("кальян|UZS", 0, 1),))
    gathered = gather_advice(
        sheets, date(2026, 10, 1), TODAY, lambda plan: changes, CATALOG
    )
    assert (gathered.invalid, gathered.context) == (frozenset({"rows"}), None)


def test_without_the_planned_tab_there_is_nothing_to_advise() -> None:
    with pytest.raises(WorksheetNotFound):
        gather_advice(
            PlanSheets({}),
            date(2026, 10, 1),
            TODAY,
            lambda plan: PlanChanges(),
            CATALOG,
        )
