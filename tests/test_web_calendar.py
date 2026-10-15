"""Календарь записей: GET /api/forecast с записями и POST/PUT/DELETE /api/calendar."""

import asyncio
import time
from datetime import date, timedelta
from itertools import pairwise
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from app import config
from app.services.calendar import HEADER, read_calendar
from app.services.forecast import month_forecast
from app.web.server import create_app
from tests.test_day_budget import RATES, frame
from tests.test_web_api import OWNER, TODAY, WIFE, auth, client_for, make_application

ROW = ["m3a9zt", "01.10.2026", "каждый день", "", "🍔 ЕДА", "кофе", "UZS", 30000,
       "Кофе", "Иван"]  # fmt: skip
ABOVE = ["a1yoga", *ROW[1:8], "Йога", "Иван"]
BELOW = ["t7taxi", *ROW[1:8], "Такси", "Иван"]


@pytest.fixture(autouse=True)
def allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({OWNER, WIFE}))


class CalendarSheets:
    """Лист «Календарь» в памяти с теми же методами, что у GoogleSheetsService."""

    def __init__(self, rows: list[list[Any]] | None = None) -> None:
        self.rows = [list(HEADER), *(rows or [])]
        self.fail = False

    def get_calendar(self) -> list[list[Any]]:
        return [list(row) for row in self.rows]

    def add_calendar_row(self, row: list[Any]) -> None:
        if self.fail:
            raise ConnectionError("Google is down")
        self.rows.append(list(row))

    def update_calendar_row(self, record_id: str, row: list[Any]) -> bool:
        for index, current in enumerate(self.rows[1:], start=1):
            if current[0] == record_id:
                self.rows[index] = list(row)
                return True
        return False

    def delete_calendar_row(self, record_id: str) -> bool:
        for index, current in enumerate(self.rows[1:], start=1):
            if current[0] == record_id:
                del self.rows[index]
                return True
        return False


class TwoStepSheets(CalendarSheets):
    """Как в Google: правка и удаление — два запроса, найти строку по ID и записать
    в неё или удалить её, а между ними лист может сдвинуться; spans — когда шла
    каждая запись."""

    def __init__(self, rows: list[list[Any]]) -> None:
        super().__init__(rows)
        self.spans: list[tuple[float, float]] = []

    def number(self, record_id: str) -> int | None:
        return next(
            (n for n, row in enumerate(self.rows) if n and row[0] == record_id), None
        )

    def add_calendar_row(self, row: list[Any]) -> None:
        start = time.monotonic()
        time.sleep(0.05)
        super().add_calendar_row(row)
        self.spans.append((start, time.monotonic()))

    def update_calendar_row(self, record_id: str, row: list[Any]) -> bool:
        start, number = time.monotonic(), self.number(record_id)
        time.sleep(0.1)  # строка найдена, запись ещё в пути — удаление выше успеет
        if number is not None:
            self.rows[number] = list(row)
        self.spans.append((start, time.monotonic()))
        return number is not None

    def delete_calendar_row(self, record_id: str) -> bool:
        start, number = time.monotonic(), self.number(record_id)
        time.sleep(0.05)
        if number is not None:
            del self.rows[number]
        self.spans.append((start, time.monotonic()))
        return number is not None


class CalendarAnalytics:
    """Прогноз по листу в памяти: что записали — то и в ответе."""

    def __init__(self, sheets: CalendarSheets) -> None:
        self.sheets = sheets

    def forecast(self, today: date, currencies: Any) -> Any:
        records = read_calendar(self.sheets.get_calendar())
        return month_forecast(frame(), (), (), today, RATES, currencies, records)


def setup(
    rows: list[list[Any]] | None = None, sheets: CalendarSheets | None = None
) -> tuple[Any, CalendarSheets]:
    sheets = CalendarSheets(rows) if sheets is None else sheets
    application = make_application(sheets)
    application.bot_data["analytics_service"] = CalendarAnalytics(sheets)
    application.bot.get_chat = AsyncMock(
        return_value=SimpleNamespace(first_name="Иван")
    )
    return application, sheets


def body(**changes: Any) -> dict[str, Any]:
    value = {
        "entry_id": str(uuid4()),
        "date": TODAY.isoformat(),
        "repeat": "once",
        "weekdays": [],
        "group": "🍔 ЕДА",
        "subcategory": "кафе",
        "currency": "UZS",
        "amount": "320000",
        "name": "Детский центр",
    }
    return {**value, **changes}


def test_a_new_record_is_written_and_the_forecast_comes_back() -> None:
    application, sheets = setup()
    response = client_for(application).post(
        "/api/calendar", json=body(), headers=auth()
    )
    assert response.status_code == 200
    written = sheets.rows[-1]
    assert written[1:] == [f"{TODAY:%d.%m.%Y}", "нет", "", "🍔 ЕДА", "кафе", "UZS",
                           320000.0, "Детский центр", "Иван"]  # fmt: skip
    found = response.json()
    assert [r["name"] for r in found["calendar"]] == ["Детский центр"]
    uzs = found["currencies"][0]
    assert uzs["records"] == [
        {"id": written[0], "day": TODAY.day, "name": "Детский центр", "group": "🍔 ЕДА",
         "subcategory": "кафе", "repeat": "once", "amount": 320000, "open": 320000,
         "done": False, "over": True}
    ]  # fmt: skip
    note = application.bot.send_message.await_args.kwargs
    assert note["chat_id"] == WIFE
    assert note["text"] == (
        f"📅 Иван — новая запись: {TODAY:%d.%m} Детский центр 320 000 UZS (ЕДА / кафе)"
    )


def test_without_a_name_the_note_still_goes() -> None:
    application, sheets = setup()
    application.bot.get_chat = AsyncMock(side_effect=RuntimeError("chat not found"))
    client_for(application).post("/api/calendar", json=body(), headers=auth())
    assert sheets.rows[-1][9] == ""
    assert application.bot.send_message.await_args.kwargs["text"] == (
        f"📅 Новая запись: {TODAY:%d.%m} Детский центр 320 000 UZS (ЕДА / кафе)"
    )


def test_the_same_entry_twice_is_written_once() -> None:
    application, sheets = setup()
    client = client_for(application)
    once = body()
    client.post("/api/calendar", json=once, headers=auth())
    client.post("/api/calendar", json=once, headers=auth())
    assert len(sheets.rows) == 2


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"group": "💰 ДОХОДЫ", "subcategory": "зарплата"}, "group"),
        ({"group": "💳 СЧЕТА", "subcategory": "переводы"}, "group"),
        ({"subcategory": "кальян"}, "subcategory"),
        ({"currency": "EUR"}, "currency"),
        ({"date": (TODAY - timedelta(days=1)).isoformat()}, "date"),
        ({"date": (TODAY + timedelta(days=400)).isoformat()}, "date"),
        ({"repeat": "weekdays"}, "weekdays"),
        ({"repeat": "weekdays", "weekdays": [8]}, "weekdays"),
        ({"repeat": "daily", "weekdays": [1]}, "weekdays"),
        ({"amount": "0"}, "amount"),
        ({"name": "я" * 61}, "name"),
        ({"repeat": "yearly"}, "repeat"),
    ],
)
def test_bad_fields_are_named(changes: dict[str, Any], field: str) -> None:
    application, sheets = setup()
    response = client_for(application).post(
        "/api/calendar", json=body(**changes), headers=auth()
    )
    assert response.status_code == 422
    assert field in response.json()["error"]["fields"]
    assert len(sheets.rows) == 1


def test_a_record_is_rewritten_and_a_past_start_of_a_repeat_stays() -> None:
    application, sheets = setup([ROW])
    changes = {"date": "2026-10-01", "repeat": "daily", "subcategory": "кофе",
               "amount": "35000", "name": "Кофе"}  # fmt: skip
    response = client_for(application).put(
        "/api/calendar/m3a9zt", json=body(entry_id=None, **changes), headers=auth()
    )
    assert response.status_code == 200
    assert sheets.rows[1][:8] == ["m3a9zt", "01.10.2026", "каждый день", "", "🍔 ЕДА",
                                  "кофе", "UZS", 35000.0]  # fmt: skip
    assert application.bot.send_message.await_args.kwargs["text"] == (
        "📅 Иван — запись изменена: каждый день с 01.10 Кофе 35 000 UZS (ЕДА / кофе)"
    )


def test_a_one_off_cannot_move_into_the_past() -> None:
    application, _ = setup([ROW])
    past = body(entry_id=None, date="2026-10-01", subcategory="кофе")
    response = client_for(application).put(
        "/api/calendar/m3a9zt", json=past, headers=auth()
    )
    assert (response.status_code, list(response.json()["error"]["fields"])) == (
        422,
        ["date"],
    )


def test_a_record_is_deleted() -> None:
    application, sheets = setup([ROW])
    response = client_for(application).delete("/api/calendar/m3a9zt", headers=auth())
    assert response.status_code == 200
    assert sheets.rows == [list(HEADER)]
    assert application.bot.send_message.await_args.kwargs["text"] == (
        "📅 Иван — запись удалена: каждый день с 01.10 Кофе 30 000 UZS (ЕДА / кофе)"
    )


def coffee_saved() -> dict[str, Any]:
    """PUT записи ROW: тот же повтор, другое название."""
    return body(entry_id=None, date="2026-10-01", repeat="daily", subcategory="кофе",
                name="Капучино")  # fmt: skip


async def test_a_save_while_a_record_above_is_deleted_loses_no_record() -> None:
    sheets = TwoStepSheets([ABOVE, ROW, BELOW])
    application, _ = setup(sheets=sheets)
    transport = httpx.ASGITransport(app=create_app(application, "s"))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        answers = await asyncio.gather(
            client.put("/api/calendar/m3a9zt", json=coffee_saved(), headers=auth(WIFE)),
            client.delete("/api/calendar/a1yoga", headers=auth()),
        )
    assert [a.status_code for a in answers] == [200, 200]
    assert [row[8] for row in sheets.rows[1:]] == ["Капучино", "Такси"]


async def test_calendar_writes_go_one_at_a_time() -> None:
    sheets = TwoStepSheets([ABOVE, ROW, BELOW])
    application, _ = setup(sheets=sheets)
    transport = httpx.ASGITransport(app=create_app(application, "s"))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await asyncio.gather(
            client.post("/api/calendar", json=body(), headers=auth()),
            client.put("/api/calendar/m3a9zt", json=coffee_saved(), headers=auth(WIFE)),
            client.delete("/api/calendar/a1yoga", headers=auth()),
        )
    spans = sorted(sheets.spans)
    overlaps = [(a, b) for a, b in pairwise(spans) if a[1] > b[0]]
    assert (len(spans), overlaps) == (3, [])


def test_a_record_someone_already_deleted_is_404() -> None:
    application, _ = setup()
    client = client_for(application)
    for response in (
        client.put("/api/calendar/nnnnnn", json=body(entry_id=None), headers=auth()),
        client.delete("/api/calendar/nnnnnn", headers=auth()),
    ):
        assert (response.status_code, response.json()["error"]["code"]) == (
            404,
            "record_not_found",
        )


def test_a_sheet_failure_is_503_and_a_retry_writes() -> None:
    application, sheets = setup()
    sheets.fail = True
    client = client_for(application)
    same = body()
    response = client.post("/api/calendar", json=same, headers=auth())
    assert (response.status_code, response.json()["error"]["code"]) == (
        503,
        "sheets_unavailable",
    )
    sheets.fail = False
    assert client.post("/api/calendar", json=same, headers=auth()).status_code == 200
    assert len(sheets.rows) == 2


def test_calendar_needs_a_signature() -> None:
    application, _ = setup()
    assert client_for(application).post("/api/calendar", json=body()).status_code == 401


def test_the_forecast_has_records_articles_hints_and_rates() -> None:
    application, _ = setup([ROW])
    found = client_for(application).get("/api/forecast", headers=auth()).json()
    assert found["rates"] == {"USD": 12000.0}
    assert [r["id"] for r in found["calendar"]] == ["m3a9zt"]
    assert found["calendar"][0]["weekdays"] == []
    assert found["calendar"][0]["booked"] == found["calendar"][0]["month_total"]
    assert found["articles"] == [
        {"group": "🍔 ЕДА", "subcategory": "кофе", "currency": "UZS", "plan": 0,
         "rest": 0, "booked": found["calendar"][0]["month_total"]}
    ]  # fmt: skip
    assert found["hints"] == {}
