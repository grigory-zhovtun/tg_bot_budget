"""AnalyticsService на синтетическом листе fact: валюты, знаки, окно, план-факт."""

from datetime import date
from typing import Any

import pandas as pd
import pytest

from app.domain import Rates
from app.services.analytics_service import (
    CAPTION_DAYS,
    CAPTION_PIE,
    AnalyticsService,
    chart_label,
    expenses,
    income,
    md_escape,
    pie_chart,
    to_day,
    transactions_frame,
)

TODAY = date(2026, 10, 6)
RATES = Rates({"UZS": 1.0, "USD": 12000.0})
HEADER = ["Дата", "Категория", "Название", "Сумма", "Баланс", "Комментарий", "Валюта"]


def serial(day: date) -> int:
    return (day - date(1899, 12, 30)).days


def row(day: date, group: str, sub: str, amount: float, comment: str, cur: str = "UZS"):
    return [serial(day), group, sub, amount, "", comment, cur, "VISA 9120 UZS"]


FACT = [
    HEADER,
    row(date(2026, 10, 4), "🍔 ЕДА", "кофе", 48000, "AI: Shavi_Coffee"),
    row(date(2026, 10, 5), "🌛 ЕЖЕМЕСЯЧНО", "подписка", 10, "AI: Claude", "USD"),
    row(date(2026, 10, 6), "🍔 ЕДА", "кафе", -5000, "AI: возврат"),
    row(date(2026, 10, 6), "💳 СЧЕТА", "переводы", 1000000, "перевод"),
    row(date(2026, 10, 6), "💰 ДОХОДЫ", "зарплата", 3000000, "ЗП"),
    row(date(2026, 10, 6), "💰 ДОХОДЫ", "нач остаток", 777, "выравнивание"),
    row(date(2026, 10, 3), "🍔 ЕДА", "кофе", 1000000, "вне окна"),
    row(date(2026, 9, 15), "🍔 ЕДА", "кафе", 300000, "сентябрь"),
    row(date(2026, 8, 15), "🏚️ ДОМ", "продукты", 600000, "август"),
    row(date(2026, 6, 15), "🏚️ ДОМ", "продукты", 9000000, "старше трёх месяцев"),
    ["", "", "", "", "", "", "", ""],
    row(date(2026, 10, 6), "🍔 ЕДА", "кофе", 3, "в сингапурских долларах", "SGD"),
]
PLAN = [
    [
        "Статья",
        "Категория",
        "Подкатегория",
        "План",
        "Факт",
        "Дельта",
        "Валюта",
        "ЭП",
        "ЭФ",
    ],
    ["Расходы", "🍔 ЕДА", "кофе", -2000000, -1048000, 0, "UZS", -2000000, -1048000],
    ["Расходы", "🌛 ЕЖЕМЕСЯЧНО", "подписка", 0, -10, 0, "USD", 0, -120000],
    ["Расходы", "🏚️ ДОМ", "одежда", 0, 0, 0, "UZS", 0, 0],
    ["", "", "", "", "", "", "", -81000000, -11000000],
]


class FakeSheets:
    def get_values(self, name: str) -> list[list[Any]]:
        if name == "fact":
            return FACT
        if name == "Oct 26":
            return PLAN
        raise KeyError(name)

    def get_rates(self) -> Rates:
        return RATES


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (46301, date(2026, 10, 6)),
        ("06.10.2026", date(2026, 10, 6)),
        ("", None),
        ("вчера", None),
        (True, None),
    ],
)
def test_to_day(value: Any, expected: date | None) -> None:
    assert to_day(value) == expected


def test_frame_converts_currency_and_skips_unknown() -> None:
    frame = transactions_frame(FACT, RATES)
    assert len(frame) == len(FACT) - 3  # шапка, пустая строка, SGD без курса
    claude = frame[frame["comment"] == "Claude"].iloc[0]
    assert claude["amount_uzs"] == 120000.0


def test_expenses_and_income_filters() -> None:
    frame = transactions_frame(FACT, RATES)
    assert "💳 СЧЕТА" not in set(expenses(frame)["group"])
    assert list(income(frame)["subcategory"]) == ["зарплата"]


def test_three_day_report_numbers_and_charts() -> None:
    text, charts = AnalyticsService(FakeSheets()).generate_3day_report(TODAY)
    assert "📅 04.10 – 06.10.2026" in text
    assert "📈 Доходы: 3 000 000" in text
    assert "📉 Расходы: 163 000" in text  # 48 000 + 10 USD − 5 000 возврат
    assert "📝 Операций: 4" in text
    assert "1. 🌛 ЕЖЕМЕСЯЧНО: 120 000 (73.6%)" in text
    assert "2. 🍔 ЕДА: 43 000 (26.4%)" in text
    assert "06.10: −0 / +3 000 000" in text
    assert "Shavi\\_Coffee" in text
    assert [caption for caption, _ in charts] == [CAPTION_PIE, CAPTION_DAYS]
    assert all(buffer.getvalue().startswith(b"\x89PNG") for _, buffer in charts)


def test_empty_window_has_no_charts() -> None:
    text, charts = AnalyticsService(FakeSheets()).generate_3day_report(
        date(2026, 1, 15)
    )
    assert "Операций за эти дни нет." in text
    assert charts == []


def test_pie_ignores_refund_only_categories() -> None:
    chart = pie_chart(pd.Series({"🍔 ЕДА": -5000.0, "🏚️ ДОМ": 1000.0}))
    assert chart.getvalue().startswith(b"\x89PNG")


def test_advice_context_has_pace_averages_and_plan_fact() -> None:
    text = AnalyticsService(FakeSheets()).advice_context(TODAY)
    assert "прошло 6 из 31 дней" in text
    # октябрь: 48 000 + 120 000 − 5 000 + 1 000 000; июнь в среднее не входит
    assert "Траты месяца: 1 163 000 сум" in text
    assert "прогноз на месяц: 6 008 833 сум" in text
    assert "средний месяц за последние 3: 300 000 сум" in text
    assert "🏚️ ДОМ | 0 | 200 000" in text
    assert "Расходы: 🍔 ЕДА / кофе (UZS) | 2 000 000 | 1 048 000 | 52%" in text
    assert "Расходы: 🌛 ЕЖЕМЕСЯЧНО / подписка (USD) | 0 | 120 000 | вне плана" in text
    assert "одежда" not in text


def test_advice_context_without_plan_sheet() -> None:
    text = AnalyticsService(FakeSheets()).advice_context(date(2026, 11, 2))
    assert "Листа с планом «Nov 26» нет." in text


def test_chart_label_drops_emoji() -> None:
    assert chart_label("🍔 ЕДА") == "ЕДА"
    assert (
        chart_label("🌛 ЕЖЕМЕСЯЧНО / подписка (USD)") == "ЕЖЕМЕСЯЧНО / подписка (USD)"
    )


def test_md_escape() -> None:
    assert md_escape("a_b*c`d[e") == "a\\_b\\*c\\`d\\[e"
