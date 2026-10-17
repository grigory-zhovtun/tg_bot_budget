"""Лист «Календарь» в сервисе таблицы: чтение с копией на минуту, запись по ID."""

from collections.abc import Callable
from typing import Any

import pytest
from gspread.exceptions import WorksheetNotFound
from gspread.utils import ValueInputOption

from app.services import google_sheets
from app.services.calendar import HEADER
from app.services.google_sheets import GoogleSheetsService

ROW = ["k7f2qa", "17.10.2026", "нет", "", "👶 ДЕТИ", "развитие", "UZS", 320000,
       "Детский центр", "Тест"]  # fmt: skip


class CalendarWorksheet:
    def __init__(self, rows: list[list[Any]] | None = None) -> None:
        self.id = 7
        self.rows = rows if rows is not None else []
        self.reads = 0
        self.appended: list[tuple[list[list[Any]], dict[str, Any]]] = []
        self.updates: list[dict[str, Any]] = []

    def get_all_values(self, **options: Any) -> list[list[Any]]:
        self.reads += 1
        return [list(row) for row in self.rows]

    def col_values(self, column: int) -> list[Any]:
        return [row[column - 1] if len(row) >= column else "" for row in self.rows]

    def append_rows(self, values: list[list[Any]], **options: Any) -> None:
        self.appended.append((values, options))
        self.rows.extend(list(row) for row in values)

    def update(self, **options: Any) -> None:
        self.updates.append(options)


class SlowReadWorksheet(CalendarWorksheet):
    """Лист прочитан, а ответ ещё в пути: тем временем бот пишет в лист по запросу
    второго пользователя (meanwhile — один раз)."""

    meanwhile: Callable[[], Any] | None = None

    def get_all_values(self, **options: Any) -> list[list[Any]]:
        rows = super().get_all_values(**options)
        if self.meanwhile is not None:
            write, self.meanwhile = self.meanwhile, None
            write()
        return rows


class CalendarSpreadsheet:
    def __init__(self, worksheet: CalendarWorksheet | None) -> None:
        self.ws = worksheet
        self.created: list[tuple[str, int, int]] = []
        self.batch: list[dict[str, Any]] = []

    def worksheet(self, name: str) -> CalendarWorksheet:
        if self.ws is None:
            raise WorksheetNotFound(name)
        return self.ws

    def add_worksheet(self, title: str, rows: int, cols: int) -> CalendarWorksheet:
        self.created.append((title, rows, cols))
        self.ws = CalendarWorksheet()
        return self.ws

    def batch_update(self, body: dict[str, Any]) -> None:
        self.batch.append(body)


def service_with(ws: CalendarWorksheet | None) -> GoogleSheetsService:
    service = GoogleSheetsService.__new__(GoogleSheetsService)
    service.client = None
    service.sheet = CalendarSpreadsheet(ws)
    service._worksheets = {}
    service._balance_cells = None
    service._calendar = None
    service._values = {}
    service._rates = None
    service._generation = 0
    service._authenticate = lambda: None
    return service


def test_the_calendar_is_read_once_a_minute(monkeypatch: pytest.MonkeyPatch) -> None:
    ws = CalendarWorksheet([HEADER, ROW])
    service = service_with(ws)
    clock = iter([100.0, 130.0, 161.0])
    monkeypatch.setattr(google_sheets.time, "monotonic", lambda: next(clock))
    assert service.get_calendar() == [HEADER, ROW]
    assert service.get_calendar() == [HEADER, ROW]  # через 30 секунд — та же копия
    service.get_calendar()  # через 61 секунду — читаем снова
    assert ws.reads == 2


def test_without_the_sheet_there_are_no_records() -> None:
    assert service_with(None).get_calendar() == []


def test_reload_forgets_the_copy_of_the_calendar() -> None:
    ws = CalendarWorksheet([HEADER])
    service = service_with(ws)
    service.get_calendar()
    service.reload()
    service.get_calendar()
    assert ws.reads == 2


def test_a_new_record_creates_the_sheet_with_its_header() -> None:
    service = service_with(None)
    service.add_calendar_row(ROW)
    assert service.sheet.created == [("Календарь", 200, 10)]
    assert service.sheet.ws is not None
    assert service.sheet.ws.appended == [
        ([HEADER], {"value_input_option": ValueInputOption.raw}),
        ([ROW], {"value_input_option": ValueInputOption.raw, "table_range": "A:J"}),
    ]


def test_writes_forget_the_copy_of_the_sheet() -> None:
    ws = CalendarWorksheet([HEADER])
    service = service_with(ws)
    service.get_calendar()
    service.add_calendar_row(ROW)
    assert service.get_calendar() == [HEADER, ROW]
    assert ws.reads == 2


def test_a_record_added_during_a_read_is_in_the_next_read() -> None:
    ws = SlowReadWorksheet([HEADER])
    service = service_with(ws)
    ws.meanwhile = lambda: service.add_calendar_row(ROW)
    assert service.get_calendar() == [HEADER]  # прочитано до записи
    assert service.get_calendar() == [HEADER, ROW]


@pytest.mark.parametrize(
    "write",
    [
        lambda service: service.update_calendar_row("k7f2qa", ROW),
        lambda service: service.delete_calendar_row("k7f2qa"),
    ],
    ids=["update", "delete"],
)
def test_after_a_change_during_a_read_the_sheet_is_read_again(
    write: Callable[[GoogleSheetsService], bool],
) -> None:
    ws = SlowReadWorksheet([HEADER, ROW])
    service = service_with(ws)
    ws.meanwhile = lambda: write(service)
    service.get_calendar()
    service.get_calendar()
    assert ws.reads == 2


def test_a_record_is_rewritten_in_its_row() -> None:
    ws = CalendarWorksheet([HEADER, ["a11111"], ["k7f2qa"], ["z99999"]])
    service = service_with(ws)
    assert service.update_calendar_row("k7f2qa", ROW)
    assert ws.updates == [
        {
            "values": [ROW],
            "range_name": "A3:J3",
            "value_input_option": ValueInputOption.raw,
        }
    ]
    assert not service.update_calendar_row("nnnnnn", ROW)
    assert not service_with(None).update_calendar_row("k7f2qa", ROW)


def test_a_record_is_deleted_by_its_id() -> None:
    ws = CalendarWorksheet([HEADER, ["a11111"], ["k7f2qa"]])
    service = service_with(ws)
    assert service.delete_calendar_row("k7f2qa")
    assert service.sheet.batch == [
        {
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": 7,
                            "dimension": "ROWS",
                            "startIndex": 2,
                            "endIndex": 3,
                        }
                    }
                }
            ]
        }
    ]
    assert not service.delete_calendar_row("nnnnnn")
    assert not service_with(None).delete_calendar_row("k7f2qa")
