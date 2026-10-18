"""Фоновая запись копилок: итоги окон, новый срок, колонки бота, формулы."""

import math
import re
from datetime import date
from typing import Any

from gspread.exceptions import WorksheetNotFound

from app import config
from app.services.google_sheets import PIGGY_JOURNAL_HEADER, PIGGY_LIST_HEADER
from app.services.piggy_sync import goal_cells, sync_piggy
from tests.test_day_budget import FACT_HEADER, RATES, fact_row, tab

TODAY = date(2027, 1, 25)  # окно «Нового года» к 20.12 закрылось 19.01
CATALOG = (
    {"UZS", "USD"},
    ["🎉 ПРАЗДНИК", "🍔 ЕДА"],
    {"🎉 ПРАЗДНИК": ["сезонный"], "🍔 ЕДА": ["кафе"]},
)
ROW = ["p3k9za", "Новый год", 12000000, "UZS", "20.12.2026", "каждый год",
       "🎉 ПРАЗДНИК", "сезонный", 12000000, "", 12000000, "", "Тест", ""]  # fmt: skip
SAVED = ["30.10.2026", "p3k9za", "Новый год", 12000000, "UZS", "отложил", "Тест", ""]
GIFTS = fact_row(date(2026, 12, 18), "🎉 ПРАЗДНИК", "сезонный", 2_400_000)


class SyncSheets:
    """Таблица в памяти с методами GoogleSheetsService, которые зовёт фон."""

    def __init__(self, rows: list[list[Any]], moves: list[list[Any]]) -> None:
        self.rows = [list(PIGGY_LIST_HEADER), *(list(row) for row in rows)]
        self.journal = [list(PIGGY_JOURNAL_HEADER), *(list(row) for row in moves)]
        self.fact = [FACT_HEADER, GIFTS]
        self.cells: list[dict[str, Any]] = []
        self.formulas: list[int] = []
        self.updates: list[tuple[str, dict[str, Any]]] = []

    def get_piggy(self, max_age: float = 0.0) -> tuple[list[Any], list[Any]]:
        return [list(r) for r in self.rows], [list(r) for r in self.journal]

    def get_values(self, name: str, max_age: float = 0.0) -> list[list[Any]]:
        if name == config.FACT_SHEET_NAME:
            return self.fact
        if name != "Jan 27":
            raise WorksheetNotFound(name)
        return tab()

    def get_rates(self, max_age: float = 0.0) -> Any:
        return RATES

    def add_piggy_moves(self, rows: list[list[Any]]) -> None:
        self.journal.extend(list(row) for row in rows)

    def update_piggy(self, piggy_id: str, cells: dict[str, Any]) -> bool:
        self.updates.append((piggy_id, dict(cells)))
        for row in self.rows[1:]:
            if row[0] == piggy_id:
                for column, value in cells.items():
                    row[ord(column) - ord("A")] = value
                return True
        return False

    def write_cells(self, data: list[dict[str, Any]]) -> None:
        self.cells.extend(data)
        for item in data:
            column, number = re.fullmatch(r"'.+'!([A-Z])(\d+)", item["range"]).groups()
            self.rows[int(number) - 1][ord(column) - ord("A")] = item["values"][0][0]

    def write_piggy_formulas(self, rows: list[int]) -> None:
        self.formulas.extend(rows)


def test_a_closed_window_is_written_once_then_the_deadline_moves() -> None:
    sheets = SyncSheets([ROW], [SAVED])
    assert sync_piggy(sheets, TODAY, *CATALOG) > 0
    assert sheets.journal[-1] == ["25.01.2027", "p3k9za", "Новый год", -2_400_000,
                                  "UZS", "потрачено в окне", "бот",
                                  "окно:20.12.2026"]  # fmt: skip
    assert sheets.updates == [("p3k9za", {"E": "20.12.2027"})]
    rows = len(sheets.journal)
    sync_piggy(sheets, TODAY, *CATALOG)
    assert len(sheets.journal) == rows  # итог окна — один раз
    assert sheets.updates == [("p3k9za", {"E": "20.12.2027"})]


def test_a_crash_between_the_two_writes_is_finished_next_time() -> None:
    total = ["20.01.2027", "p3k9za", "Новый год", -2_400_000, "UZS",
             "потрачено в окне", "бот", "окно:20.12.2026"]  # fmt: skip
    sheets = SyncSheets([ROW], [SAVED, total])
    sync_piggy(sheets, TODAY, *CATALOG)
    assert sheets.journal[-1] == total  # итог уже был — второй не пишется
    assert sheets.updates == [("p3k9za", {"E": "20.12.2027"})]


def test_a_deadline_moved_ahead_by_hand_is_left_alone() -> None:
    ahead = [*ROW[:4], "20.12.2027", *ROW[5:]]
    sheets = SyncSheets([ahead], [SAVED])
    sync_piggy(sheets, TODAY, *CATALOG)
    assert sheets.updates == []
    assert len(sheets.journal) == 2


def test_writes_only_changed_bot_cells() -> None:
    ahead = [*ROW[:4], "20.12.2027", *ROW[5:]]
    sheets = SyncSheets([ahead], [SAVED])
    sync_piggy(sheets, TODAY, *CATALOG)
    assert sorted(item["range"] for item in sheets.cells) == [
        "'Копилки'!J2",
        "'Копилки'!L2",
    ]
    sheets.cells.clear()
    assert sync_piggy(sheets, TODAY, *CATALOG) == 0
    assert sheets.cells == []


def test_restores_missing_formulas() -> None:
    lost = [*ROW[:4], "20.12.2027", *ROW[5:8], "", "", "", "", "Тест", ""]
    sheets = SyncSheets([ROW, lost], [SAVED])
    sheets.rows[1][4] = "20.12.2027"  # первая — без закрытого окна, формулы на месте
    sync_piggy(sheets, TODAY, *CATALOG)
    assert sheets.formulas == [3]


def test_without_the_sheets_nothing_is_written() -> None:
    sheets = SyncSheets([], [])
    sheets.rows, sheets.journal = [], []
    assert sync_piggy(sheets, TODAY, *CATALOG) == 0


def test_a_yearly_piggy_without_an_article_gets_its_next_deadline() -> None:
    plain = [*ROW[:6], "", "", *ROW[8:]]
    sheets = SyncSheets([plain], [SAVED])
    sync_piggy(sheets, TODAY, *CATALOG)
    assert sheets.updates == [("p3k9za", {"E": "20.12.2027"})]
    assert len(sheets.journal) == 2  # окна без статьи нет — и итога нет


def test_an_empty_window_is_written_as_a_plain_zero() -> None:
    sheets = SyncSheets([ROW], [SAVED])
    sheets.fact = [FACT_HEADER]  # в окне ничего не потратили
    sync_piggy(sheets, TODAY, *CATALOG)
    amount = sheets.journal[-1][3]
    assert amount == 0 and math.copysign(1.0, amount) == 1.0


def test_goal_cells_count_the_month_of_the_new_tab() -> None:
    # 17.10: «Новый год» 12 млн к 20.12, ноябрь — 6 млн взноса + 10 % от 12 млн
    sheets = SyncSheets([ROW], [])
    sheets.fact = [FACT_HEADER]
    cells = goal_cells(sheets, date(2026, 10, 17), *CATALOG)
    assert cells(tab(goal=2_000_000), date(2026, 11, 1)) == (20, 7_200_000)


def test_goal_cells_without_open_piggies_leave_the_cell() -> None:
    sheets = SyncSheets([], [])
    cells = goal_cells(sheets, date(2026, 10, 17), *CATALOG)
    assert cells(tab(goal=2_000_000), date(2026, 11, 1)) is None


def test_goal_cells_need_the_goal_row() -> None:
    cells = goal_cells(SyncSheets([ROW], []), date(2026, 10, 17), *CATALOG)
    assert cells(tab(), date(2026, 11, 1)) is None
