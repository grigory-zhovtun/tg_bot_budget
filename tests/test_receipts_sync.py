"""Колонка «Пришло к утру»: что бот пишет во вкладку месяца."""

from datetime import date
from typing import Any

import pytest
from gspread.exceptions import WorksheetNotFound

from app.services import receipts_sync
from app.services.receipts_sync import (
    COLUMN_ADVANCE_SUBSCRIPTIONS,
    COLUMN_SUBSCRIPTIONS,
    COLUMN_T,
    LIMIT_LABEL,
    RECEIVED_HEADER,
    sync_receipts,
)
from tests.test_carry_over import HALF_PAID, NOVEMBER_TAB, OCTOBER_TAB, rubles
from tests.test_carry_over import RATES as CARRY_RATES
from tests.test_day_budget import FACT_HEADER, OPENING, RATES, charge, fact_row, tab

ADVANCE_PAID = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000)


def wide(rows: list[list[Any]]) -> list[list[Any]]:
    return [list(row) + [""] * (COLUMN_T + 1 - len(row)) for row in rows]


class Sheets:
    """Вкладка «Oct 26» и fact в памяти; write_cells пишет в колонку T."""

    def __init__(self, rows: list[list[Any]] | None, *fact: list[Any]) -> None:
        self.tabs = {} if rows is None else {"Oct 26": wide(rows)}
        self.fact = [FACT_HEADER, *OPENING, *fact]
        self.writes: list[list[dict[str, Any]]] = []

    def get_values(self, name: str) -> list[list[Any]]:
        if name == "fact":
            return self.fact
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return [list(row) for row in self.tabs[name]]

    def get_rates(self) -> Any:
        return RATES

    def write_cells(self, data: list[dict[str, Any]]) -> None:
        self.writes.append(data)
        for item in data:
            title, cell = item["range"].split("!")
            column = ord(cell[0]) - ord("A")
            row = int(cell[1:])
            self.tabs[title.strip("'")][row - 1][column] = item["values"][0][0]

    def column(self) -> list[Any]:
        """T11:T15 — заголовок, аванс, ЗП, квартплата, пустая строка списка."""
        return [row[COLUMN_T] for row in self.tabs["Oct 26"][10:15]]


def test_what_came_by_this_morning_is_written_under_the_header() -> None:
    sheets = Sheets(tab(), ADVANCE_PAID)
    assert sync_receipts(sheets, date(2026, 10, 16)) == 2
    assert sheets.column() == [RECEIVED_HEADER, 2_000_000, "", "", ""]


def test_todays_income_waits_for_the_night() -> None:
    sheets = Sheets(tab(), ADVANCE_PAID)
    sync_receipts(sheets, date(2026, 10, 15))
    assert sheets.column() == [RECEIVED_HEADER, "", "", "", ""]


def test_nothing_changed_nothing_is_written() -> None:
    sheets = Sheets(tab(), ADVANCE_PAID)
    sync_receipts(sheets, date(2026, 10, 16))
    assert sync_receipts(sheets, date(2026, 10, 16)) == 0
    assert len(sheets.writes) == 1


def test_stale_values_are_cleared() -> None:
    rows = wide(tab())
    rows[10][COLUMN_T] = RECEIVED_HEADER
    rows[12][COLUMN_T] = 5_000  # ЗП не приходила
    rows[14][COLUMN_T] = "x"  # пустая строка списка
    sheets = Sheets(rows)
    assert sync_receipts(sheets, date(2026, 10, 16)) == 2
    assert sheets.column() == [RECEIVED_HEADER, "", "", "", ""]


def test_on_the_first_the_new_month_starts_empty() -> None:
    rows = wide(tab())
    rows[10][COLUMN_T] = RECEIVED_HEADER
    rows[11][COLUMN_T] = 2_000_000  # отметка прошлого месяца в копии вкладки
    sheets = Sheets(rows, ADVANCE_PAID)
    sync_receipts(sheets, date(2026, 10, 1))
    assert sheets.column()[1] == ""


def test_without_the_tab_or_the_list_nothing_is_written() -> None:
    assert sync_receipts(Sheets(None), date(2026, 10, 16)) == 0
    no_list = Sheets([["Статья", "Категория"], ["Расходы", "🍔 ЕДА", "кафе"]])
    assert sync_receipts(no_list, date(2026, 10, 16)) == 0
    assert no_list.writes == []


CLOUD = [
    charge(date(2026, 8, 20), 50_000, "CLOUD STORAGE"),
    charge(date(2026, 9, 20), 50_000, "CLOUD STORAGE"),
]


def with_limit_row() -> list[list[Any]]:
    rows = wide(tab())
    rows[4][14] = f"{LIMIT_LABEL} (с утра; сумы до зарплаты)"
    return rows


def test_the_subscriptions_until_the_salary_go_next_to_the_limit_cell() -> None:
    # формула лимита во вкладке подписок не найдёт — их сумму пишет бот, в Q той строки
    sheets = Sheets(with_limit_row(), ADVANCE_PAID, *CLOUD)
    sync_receipts(sheets, date(2026, 10, 16))
    assert sheets.tabs["Oct 26"][4][COLUMN_SUBSCRIPTIONS] == 50_000
    assert sync_receipts(sheets, date(2026, 10, 16)) == 0  # не поменялось — не пишем


NEWS = [
    charge(date(2026, 8, 5), 30_000, "NEWS DAILY"),
    charge(date(2026, 9, 5), 30_000, "NEWS DAILY"),
]


def test_the_subscriptions_until_the_advance_go_to_r_of_the_limit_row() -> None:
    # 1-го в сроке аванс 15-го: подписки по день ЗП (5-е и 20-е) — в Q, по день аванса
    # (только 5-е) — в R, для страховки формулы
    sheets = Sheets(with_limit_row(), *CLOUD, *NEWS)
    sync_receipts(sheets, date(2026, 10, 1))
    row = sheets.tabs["Oct 26"][4]
    assert (row[COLUMN_SUBSCRIPTIONS], row[COLUMN_ADVANCE_SUBSCRIPTIONS]) == (
        80_000,
        30_000,
    )
    # 16-го аванс пришёл — авансов в сроке нет, R пустая
    after = Sheets(with_limit_row(), ADVANCE_PAID, *CLOUD, *NEWS)
    after.tabs["Oct 26"][4][COLUMN_ADVANCE_SUBSCRIPTIONS] = "← подпись"
    sync_receipts(after, date(2026, 10, 16))
    assert after.tabs["Oct 26"][4][COLUMN_ADVANCE_SUBSCRIPTIONS] == ""


def test_no_subscriptions_write_a_zero_and_no_limit_row_writes_nothing() -> None:
    sheets = Sheets(with_limit_row(), ADVANCE_PAID)
    sync_receipts(sheets, date(2026, 10, 16))
    assert sheets.tabs["Oct 26"][4][COLUMN_SUBSCRIPTIONS] == 0
    plain = Sheets(tab(), ADVANCE_PAID, *CLOUD)
    sync_receipts(plain, date(2026, 10, 16))
    assert all(
        row[COLUMN_SUBSCRIPTIONS] in ("", None) for row in plain.tabs["Oct 26"][:10]
    )


def test_a_broken_limit_does_not_stop_the_received_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(*args: Any, **kwargs: Any) -> None:
        raise ValueError("bad list")

    monkeypatch.setattr(receipts_sync, "day_budget", broken)
    sheets = Sheets(with_limit_row(), ADVANCE_PAID, *CLOUD)
    assert sync_receipts(sheets, date(2026, 10, 16)) == 2  # T11 и T12 — как всегда
    assert sheets.column() == [RECEIVED_HEADER, 2_000_000, "", "", ""]


class RubleSheets(Sheets):
    """Вкладки месяцев в памяти и курсы с рублём."""

    def __init__(self, tabs: dict[str, list[list[Any]]], *fact: list[Any]) -> None:
        super().__init__(None, *fact)
        self.tabs = {name: wide(rows) for name, rows in tabs.items()}

    def get_rates(self) -> Any:
        return CARRY_RATES


def test_this_months_rows_get_what_is_left_after_last_months_rest() -> None:
    came = (HALF_PAID, rubles(date(2026, 11, 3), 70_000))
    sheets = RubleSheets({"Oct 26": OCTOBER_TAB, "Nov 26": NOVEMBER_TAB}, *came)
    sync_receipts(sheets, date(2026, 11, 4))
    # 55 000 закрыли остаток октября: ноябрьской «ЗП Питер» — 15 000; перенос в лист
    # не пишется, вкладка октября не меняется
    november = sheets.tabs["Nov 26"]
    assert [row[COLUMN_T] for row in november[10:14]] == [
        RECEIVED_HEADER,
        "",
        15_000,
        "",
    ]
    assert sheets.tabs["Oct 26"] == wide(OCTOBER_TAB)
    # вкладки октября нет — как раньше: всё пришедшее — ноябрьской строке
    alone = RubleSheets({"Nov 26": NOVEMBER_TAB}, *came)
    sync_receipts(alone, date(2026, 11, 4))
    assert alone.tabs["Nov 26"][12][COLUMN_T] == 70_000
