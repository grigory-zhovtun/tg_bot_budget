"""Чужие деньги на своей карте: статьи «чужие», остаток по картам, свои деньги."""

import logging
from datetime import date
from typing import Any

import pandas as pd
import pytest
from gspread.exceptions import WorksheetNotFound

from app import config
from app.domain import Rates
from app.services import held
from app.services.analytics_service import transactions_frame
from app.services.held import (
    HELD_TTL,
    HeldArticle,
    HeldOnCard,
    held_articles,
    held_lines,
    held_on_cards,
    held_text,
    own_money,
    read_held,
)

RATES = Rates({"UZS": 1.0, "USD": 12000.0})
HEADER = [
    "Дата",
    "Категория",
    "Название",
    "Сумма",
    "Баланс",
    "Комментарий",
    "Валюта",
    "Карта",
]
TODAY = date(2026, 12, 15)
D1, D2, D3 = date(2026, 12, 10), date(2026, 12, 11), date(2026, 12, 12)
PARTY = HeldArticle("сбор на праздник", "🎉")
HUMO = "HUMO 2222 UZS"
VISA = "VISA 1111 UZS"


def row(
    day: str, group: str, name: str, amount: float, card: str = HUMO, code: str = "UZS"
) -> list[Any]:
    return [day, group, name, amount, "", "", code, card]


def frame_of(*rows: list[Any], keep_unknown: bool = False) -> pd.DataFrame:
    return transactions_frame([HEADER, *rows], RATES, keep_unknown=keep_unknown)


def test_held_articles_marker(caplog: pytest.LogCaptureFixture) -> None:
    rows = [
        ["💳 СЧЕТА", "сбор на праздник", " Чужие ", "🎉"],
        ["💳 СЧЕТА", "переводы", "", "🔄"],
        ["🏚️ ДОМ", "продукты", "чужие", "🛒"],  # не «СЧЕТА» — не сбор
        ["💳 СЧЕТА", "сбор на праздник", "чужие", "🎉"],  # повтор — один раз
        ["💳 СЧЕТА", "сбор на поход", "чужие"],  # без иконки
    ]
    held._warned.clear()
    with caplog.at_level(logging.WARNING):
        assert held_articles(rows) == (PARTY, HeldArticle("сбор на поход", "🤝"))
        assert held_articles(rows) == (PARTY, HeldArticle("сбор на поход", "🤝"))
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1  # метка у чужой группы — один раз за жизнь процесса
    assert "🏚️ ДОМ" in warnings[0] and "продукты" in warnings[0]


class _Broken:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def get_values(self, worksheet_name: str, max_age: float = 0.0) -> list[list[Any]]:
        raise self.error


def test_read_held_failure_is_empty(caplog) -> None:
    for error in (RuntimeError("boom"), WorksheetNotFound("system")):
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            assert read_held(_Broken(error)) == ()
        assert any(r.levelno == logging.WARNING for r in caplog.records)


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float]] = []

    def get_values(self, worksheet_name: str, max_age: float = 0.0) -> list[list[Any]]:
        self.calls.append((worksheet_name, max_age))
        return [["Заголовок"], ["💳 СЧЕТА", "сбор на праздник", "чужие", "🎉"]]


def test_read_held_uses_copy() -> None:
    gs = _Recorder()
    assert read_held(gs, 0) == (PARTY,)
    assert read_held(gs, 900) == (PARTY,)
    assert gs.calls == [
        (config.SYSTEM_SHEET_NAME, HELD_TTL),
        (config.SYSTEM_SHEET_NAME, 900),
    ]


def test_held_on_cards_split() -> None:
    frame = frame_of(
        row("10.12.2026", "💳 СЧЕТА", "сбор на праздник", -400000),
        row("10.12.2026", "💳 СЧЕТА", "сбор на праздник", -300000),
        row("11.12.2026", "💳 СЧЕТА", "сбор на праздник", 50000),
        row("11.12.2026", "💳 СЧЕТА", "сбор на праздник", -100000, VISA),
        row("16.12.2026", "💳 СЧЕТА", "сбор на праздник", -999999),  # завтра
        row("11.12.2026", "💳 СЧЕТА", "переводы", -5000),
    )
    assert held_on_cards(frame, [PARTY], TODAY) == (
        HeldOnCard(PARTY, HUMO, "UZS", 700000.0, 50000.0),
        HeldOnCard(PARTY, VISA, "UZS", 100000.0, 0.0),
    )


def test_held_on_cards_zero_left_hidden() -> None:
    frame = frame_of(
        row("10.12.2026", "💳 СЧЕТА", "сбор на праздник", -300000),
        row("11.12.2026", "💳 СЧЕТА", "сбор на праздник", 300000),
    )
    assert held_on_cards(frame, [PARTY], TODAY) == ()


def test_own_money_drops_held_rows() -> None:
    frame = frame_of(
        row("10.12.2026", "💳 СЧЕТА", "переводы", -5000),
        row("10.12.2026", "💳 СЧЕТА", "сбор на праздник", -400000),
        row("11.12.2026", "🏚️ ДОМ", "продукты", 20000),
        row("12.12.2026", "💰 ДОХОДЫ", "зарплата", 900000),
    )
    own = own_money(frame, [PARTY])
    assert list(own.index) == [0, 2, 3]
    assert PARTY.name not in set(own["subcategory"])


def test_own_money_keeps_overspend() -> None:
    frame = frame_of(
        row("10.12.2026", "💳 СЧЕТА", "сбор на праздник", -1_000_000),
        row("11.12.2026", "💳 СЧЕТА", "сбор на праздник", 1_300_000),
        row("12.12.2026", "💳 СЧЕТА", "сбор на праздник", -300_000),
    )
    own = own_money(frame, [PARTY])
    flow = -own["amount"]
    assert [flow[own["day"] <= d].sum() for d in (D1, D2, D3)] == [0, -300000, 0]
    assert (own["amount_uzs"] == own["amount"]).all()


def test_own_money_unknown_rate() -> None:
    frame = frame_of(
        row("10.12.2026", "💳 СЧЕТА", "сбор на праздник", -100, "XXX 3333 EUR", "EUR"),
        row("11.12.2026", "💳 СЧЕТА", "сбор на праздник", 150, "XXX 3333 EUR", "EUR"),
        keep_unknown=True,
    )
    own = own_money(frame, [PARTY])
    assert len(own) == 1
    assert own["amount"].tolist() == [50.0]
    assert own["amount_uzs"].tolist() == [0.0]


def test_own_money_without_held_is_same() -> None:
    frame = frame_of(row("10.12.2026", "🏚️ ДОМ", "продукты", 20000))
    assert own_money(frame, ()) is frame


def test_held_text() -> None:
    assert (
        held_text(HeldOnCard(PARTY, HUMO, "UZS", 1_250_000, 50_000))
        == "🎉 сбор на праздник 1 200 000 — чужие, не в счёт"
    )
    assert (
        held_text(HeldOnCard(PARTY, HUMO, "UZS", 100_000, 400_000))
        == "🎉 сбор на праздник — доложено из своих 300 000"
    )


def test_held_lines_name_the_card_only_when_the_article_is_on_several() -> None:
    one = HeldOnCard(PARTY, HUMO, "UZS", 700_000, 0)
    other = HeldOnCard(PARTY, VISA, "UZS", 300_000, 0)
    trip = HeldOnCard(HeldArticle("сбор на поход", "🥾"), HUMO, "UZS", 50_000, 0)
    assert held_lines([one]) == [held_text(one)]  # одна карта — текст прежний
    assert held_lines([one, other, trip]) == [
        "🎉 сбор на праздник (HUMO 2222) 700\u00a0000 — чужие, не в счёт",
        "🎉 сбор на праздник (VISA 1111) 300\u00a0000 — чужие, не в счёт",
        held_text(trip),  # другая статья на одной карте — без карты
    ]
    assert held_text(other, with_card=True).startswith(
        "🎉 сбор на праздник (VISA 1111)"
    )


def test_own_money_keeps_the_order_of_other_rows() -> None:
    """Порядок строк важен recurring._clusters: свои строки не переставляются."""
    frame = frame_of(
        row("10.12.2026", "💳 СЧЕТА", "сбор на праздник", -1_000_000),
        row("11.12.2026", "💳 СЧЕТА", "сбор на праздник", 1_300_000),
        row("12.12.2026", "🏚️ ДОМ", "продукты", 20000),
        row("10.12.2026", "🏚️ ДОМ", "продукты", 30000),
    )
    own = own_money(frame, [PARTY])
    assert list(own.index)[:2] == [2, 3]  # как в исходной таблице, не по дням
    assert set(own.index[2:]) == {4}  # поправка — после, со своим днём
    assert own.loc[4, "day"] == D2


def test_own_money_overspend_in_usd_uses_the_group_rate() -> None:
    """Перерасход в валюте: сумма в сумах поправки — по курсу группы."""
    card = "HUMO 3333 USD"
    frame = frame_of(
        row("10.12.2026", "💳 СЧЕТА", "сбор на праздник", -100, card, "USD"),
        row("11.12.2026", "💳 СЧЕТА", "сбор на праздник", 150, card, "USD"),
    )
    own = own_money(frame, [PARTY])
    assert own["amount"].tolist() == [50.0]
    assert own["currency"].tolist() == ["USD"]
    assert own["amount_uzs"].tolist() == [50.0 * 12000.0]
