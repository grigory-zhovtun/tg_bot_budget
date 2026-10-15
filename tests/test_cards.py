"""Остатки карт для «Сводки»: блок остатков листа fact (I:Q), как в таблице."""

from datetime import date, datetime
from typing import Any

import pytest

from app.domain import Rates
from app.services import google_sheets
from app.services.cards import BANK, CHECKED, NAME, TABLE, card_balances
from tests.test_analytics_service import serial

RATES = Rates({"UZS": 1.0, "USD": 12_000.0, "RUB": 150.0})
TODAY = date(2026, 10, 15)
BLOCK_HEADER = ["Счет", "Текущий баланс", "Баланс в UZS", "Баланс в RUB",
                "Баланс в USD", "Проверка", "Расхождения", "", "Сверено"]  # fmt: skip


def card(name: str, balance: Any, bank: Any = "", checked: Any = "") -> list[Any]:
    """Строка блока I:Q: карта, «Текущий баланс», «Проверка» (банк), «Сверено»."""
    return [name, balance, "", "", "", bank, "", "", checked]


def fact(*block: list[Any]) -> list[list[Any]]:
    """Лист fact: строки трат в A:H не нужны — блок остатков справа, со 2-й строки."""
    return [["Дата"] + [""] * 7 + BLOCK_HEADER] + [[""] * 8 + row for row in block]


def stamp(moment: datetime) -> float:
    """«Сверено» без форматирования — серийный номер дня с долей суток."""
    return serial(moment.date()) + (moment.hour * 60 + moment.minute) / 1440


def test_cards_are_read_in_sheet_order_with_the_sum_equivalent() -> None:
    found = card_balances(
        fact(
            card("VISA 1111 UZS", 5_250_000.4),
            card("VISA 2222 USD", 1500.25),
            card("МИР 3333 RUB", 2000.5),
        ),
        RATES,
    )
    assert [(c.name, c.currency, c.balance, c.uzs) for c in found] == [
        ("VISA 1111 UZS", "UZS", 5_250_000.4, 5_250_000.4),
        ("VISA 2222 USD", "USD", 1500.25, pytest.approx(18_003_000)),
        ("МИР 3333 RUB", "RUB", 2000.5, pytest.approx(300_075)),
    ]


def test_bank_check_matches_within_a_cent_of_dollars_or_a_sum() -> None:
    found = card_balances(
        fact(
            card("VISA 2222 USD", 1500.25, 1500.24),
            card("VISA 1111 UZS", 5_250_000.4, 5_250_000.9),
            card("HUMO 4444 UZS", 300_000, 299_995),
            card("UZCARD 5555 UZS", 40_000),
        ),
        RATES,
    )
    assert [c.matches for c in found] == [True, True, False, None]
    assert found[2].bank == 299_995


def test_the_check_time_is_read_from_the_serial_number() -> None:
    evening = datetime(2026, 10, 15, 20, 30)
    found = card_balances(
        fact(
            card("VISA 1111 UZS", 100, 100, stamp(evening)),
            card("VISA 6666 RUB", 100, 100, stamp(datetime(2026, 10, 9, 12, 12))),
            card("HUMO 4444 UZS", 100, 100, stamp(datetime(2026, 10, 7, 9, 0))),
            card("МИР 3333 RUB", 100, 100, "давно"),
        ),
        RATES,
    )
    assert found[0].checked == evening
    assert found[3].checked is None
    # как напоминание в воскресной сводке: сверке больше недели — давно
    assert [c.stale(TODAY) for c in found] == [False, False, True, False]


def test_an_error_in_the_balance_cell_gives_no_number() -> None:
    found = card_balances(fact(card("VISA 7777 UZS", "#N/A", 500)), RATES)
    assert (found[0].balance, found[0].uzs, found[0].matches) == (None, None, None)


def test_a_currency_without_a_rate_has_no_sum_equivalent() -> None:
    found = card_balances(fact(card("REVOLUT 8888 EUR", 40.5)), RATES)
    assert (found[0].currency, found[0].balance, found[0].uzs) == ("EUR", 40.5, None)


def test_the_totals_row_and_rows_below_the_block_are_not_cards() -> None:
    rows = fact(
        card("VISA 1111 UZS", 100),
        ["", "", 23_553_075, 157_020, 1963],  # итоги блока: в I пусто
    )
    rows += [[""] * 17 for _ in range(30)]
    rows[30][NAME] = "Заметка ниже блока"  # строка 31 — уже не блок I2:I30
    assert [c.name for c in card_balances(rows, RATES)] == ["VISA 1111 UZS"]


def test_block_columns_are_the_ones_the_sheet_service_writes() -> None:
    def index(letter: str) -> int:
        return ord(letter) - ord("A")

    assert NAME == index(google_sheets.BALANCE_BLOCK[0])
    assert TABLE == index(google_sheets.TABLE_BALANCES.split(":")[1][0])
    assert BANK == index(google_sheets.BALANCE_COLUMN)
    assert CHECKED == index(google_sheets.CHECKED_COLUMN)
