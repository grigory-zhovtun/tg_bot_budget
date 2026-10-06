"""Сервис таблицы с поддельным gspread: какие запросы уходят в Google Sheets."""

from datetime import date
from typing import Any

import pytest

from app.domain import SheetRow
from app.services import google_sheets
from app.services.google_sheets import (
    BALANCE_FORMULA,
    GoogleSheetsService,
    appended_rows,
    row_values,
)


class FakeWorksheet:
    def __init__(self, sheet_id: int = 0, cells: list[list[str]] | None = None) -> None:
        self.id = sheet_id
        self.cells = cells or []
        self.appended: list[dict[str, Any]] = []
        self.fail_next = 0

    def append_rows(self, values: list[list[Any]], **options: Any) -> dict[str, Any]:
        if self.fail_next:
            self.fail_next -= 1
            raise ConnectionError("token expired")
        self.appended.append({"values": values, **options})
        first = 4169
        return {
            "updates": {"updatedRange": f"fact!A{first}:H{first + len(values) - 1}"}
        }

    def get(self, range_name: str, **options: Any) -> list[list[str]]:
        return self.cells


class FakeSpreadsheet:
    def __init__(self, worksheet: FakeWorksheet) -> None:
        self.ws = worksheet
        self.batch: list[dict[str, Any]] = []
        self.values_batch: list[dict[str, Any]] = []

    def worksheet(self, name: str) -> FakeWorksheet:
        return self.ws

    def batch_update(self, body: dict[str, Any]) -> None:
        self.batch.append(body)

    def values_batch_update(self, body: dict[str, Any]) -> None:
        self.values_batch.append(body)


def make_service(ws: FakeWorksheet) -> GoogleSheetsService:
    service = GoogleSheetsService.__new__(GoogleSheetsService)
    service.client = None
    service.sheet = FakeSpreadsheet(ws)
    service._worksheets = {}
    service._balance_cells = None
    service.reconnects = 0

    def reconnect() -> None:
        service.reconnects += 1
        service._worksheets.clear()

    service._authenticate = reconnect
    return service


ROW = SheetRow(
    day=date(2026, 10, 6),
    category="🍔 ЕДА",
    subcategory="кофе",
    amount=48000.0,
    comment="латте",
    currency="UZS",
    source="VISA 9120 UZS",
)


@pytest.mark.parametrize(
    ("updated", "expected"),
    [
        ("fact!A4169:H4170", (4169, 4170)),
        ("'fact'!A5:H5", (5, 5)),
        ("fact!A7", (7, 7)),
    ],
)
def test_appended_rows(updated: str, expected: tuple[int, int]) -> None:
    assert appended_rows({"updates": {"updatedRange": updated}}) == expected


def test_balance_formula_has_no_row_numbers() -> None:
    assert "ROW()" in BALANCE_FORMULA
    assert not any(
        ch.isdigit()
        for ch in BALANCE_FORMULA.replace("$D$2", "")
        .replace("$H$2", "")
        .replace("$G$2", "")
        .replace("$B$2", "")
    )
    assert BALANCE_FORMULA.startswith("=SUMIFS(") and ";" in BALANCE_FORMULA


def test_append_writes_all_rows_once_and_copies_format() -> None:
    ws = FakeWorksheet(sheet_id=7)
    service = make_service(ws)

    assert service.append_transactions([ROW, ROW]) == (4169, 4170)

    [call] = ws.appended
    assert call["values"] == [row_values(ROW), row_values(ROW)]
    assert call["values"][0][:4] == ["06.10.2026", "🍔 ЕДА", "кофе", 48000.0]
    assert call["table_range"] == "A:H"
    [batch] = service.sheet.batch
    copies = [r["copyPaste"] for r in batch["requests"]]
    assert {c["pasteType"] for c in copies} == {"PASTE_FORMAT", "PASTE_DATA_VALIDATION"}
    for copy in copies:
        assert copy["source"] == {
            "sheetId": 7,
            "startRowIndex": 4167,
            "endRowIndex": 4168,
            "startColumnIndex": 0,
            "endColumnIndex": 8,
        }
        assert (
            copy["destination"]["startRowIndex"],
            copy["destination"]["endRowIndex"],
        ) == (4168, 4170)


def test_append_reconnects_once_after_failure() -> None:
    ws = FakeWorksheet()
    ws.fail_next = 1
    service = make_service(ws)
    assert service.append_transactions([ROW]) == (4169, 4169)
    assert service.reconnects == 1


def test_append_raises_when_retry_fails_too() -> None:
    ws = FakeWorksheet()
    ws.fail_next = 2
    service = make_service(ws)
    with pytest.raises(ConnectionError):
        service.append_transactions([ROW])


def test_balance_cells_follow_the_block_on_the_sheet() -> None:
    ws = FakeWorksheet(
        cells=[["VISA 9120 UZS"], ["UZCARD 5837 UZS"], [], ["VISA 7450 RUB"]]
    )
    service = make_service(ws)
    assert service.balance_cells() == {
        "VISA 9120 UZS": "N2",
        "UZCARD 5837 UZS": "N3",
        "VISA 7450 RUB": "N5",
    }


def test_update_balances_writes_known_sources_in_one_call(
    caplog: pytest.LogCaptureFixture,
) -> None:
    ws = FakeWorksheet(cells=[["VISA 9120 UZS"], ["VISA 7450 RUB"]])
    service = make_service(ws)
    updated = service.update_balances({"VISA 7450 RUB": 3595.38, "CASH UZS": 10.0})
    assert updated == ["VISA 7450 RUB"]
    [body] = service.sheet.values_batch
    assert body["data"] == [{"range": "fact!N3", "values": [[3595.38]]}]
    assert "CASH UZS" in caplog.text


def test_reload_forgets_cached_balance_cells() -> None:
    ws = FakeWorksheet(cells=[["VISA 9120 UZS"]])
    service = make_service(ws)
    assert service.balance_cells() == {"VISA 9120 UZS": "N2"}
    ws.cells = [["HUMO 6845 UZS"], ["VISA 9120 UZS"]]
    service.reload()
    assert service.balance_cells() == {"HUMO 6845 UZS": "N2", "VISA 9120 UZS": "N3"}


def test_module_constants() -> None:
    assert google_sheets.BALANCE_BLOCK == "I2:I30"
