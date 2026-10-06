"""Вкладка план-факта нового месяца: имя, копия прошлой, даты, сообщение владельцу."""

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app import config
from app.domain import month_bounds, month_title, parse_month_title
from app.handlers import month
from app.services.google_sheets import GoogleSheetsService, MonthTab


@pytest.mark.parametrize(
    ("day", "title", "bounds"),
    [
        (date(2026, 10, 6), "Oct 26", (date(2026, 10, 1), date(2026, 10, 31))),
        (date(2026, 12, 31), "Dec 26", (date(2026, 12, 1), date(2026, 12, 31))),
        (date(2028, 2, 10), "Feb 28", (date(2028, 2, 1), date(2028, 2, 29))),
    ],
)
def test_month_names_and_bounds(
    day: date, title: str, bounds: tuple[date, date]
) -> None:
    assert month_title(day) == title
    assert parse_month_title(title) == bounds[0]
    assert month_bounds(day) == bounds


@pytest.mark.parametrize("title", ["fact", "system", "Okt 26", "Oct 2026", "oct 26"])
def test_other_sheets_are_not_months(title: str) -> None:
    assert parse_month_title(title) is None


class Tab:
    def __init__(self, title: str, sheet_id: int, index: int, hidden: bool) -> None:
        self.title, self.id, self.index, self.isSheetHidden = (
            title,
            sheet_id,
            index,
            hidden,
        )
        self.dates: list[list[Any]] = []

    def get(self, range_name: str, **options: Any) -> list[list[Any]]:
        assert range_name == "M1:M2"
        return self.dates


class Book:
    """Таблица: список вкладок, duplicateSheet копирует даты источника."""

    def __init__(self, tabs: list[Tab]) -> None:
        self.tabs = tabs
        self.batches: list[dict[str, Any]] = []
        self.values: list[dict[str, Any]] = []

    def worksheets(self) -> list[Tab]:
        return list(self.tabs)

    def worksheet(self, name: str) -> Tab:
        return next(tab for tab in self.tabs if tab.title == name)

    def batch_update(self, body: dict[str, Any]) -> dict[str, Any]:
        self.batches.append(body)
        request = body["requests"][0]
        if "duplicateSheet" in request:
            spec = request["duplicateSheet"]
            source = next(t for t in self.tabs if t.id == spec["sourceSheetId"])
            copy = Tab(spec["newSheetName"], 999, spec["insertSheetIndex"], False)
            copy.dates = source.dates
            self.tabs.append(copy)
            return {"replies": [{"duplicateSheet": {"properties": {"sheetId": 999}}}]}
        return {}

    def values_batch_update(self, body: dict[str, Any]) -> None:
        self.values.append(body)


def service_for(book: Book) -> GoogleSheetsService:
    service = GoogleSheetsService.__new__(GoogleSheetsService)
    service.client, service.sheet = None, book
    service._worksheets, service._balance_cells = {}, None
    return service


def october_book() -> Book:
    sep = Tab("Sep 26", 30, 30, hidden=True)
    oct_ = Tab("Oct 26", 29, 29, hidden=False)
    oct_.dates = [[46296], [46326]]
    return Book([Tab("fact", 0, 6, False), oct_, sep, Tab("system", 1, 42, False)])


def test_new_month_is_a_copy_of_the_last_one_with_its_own_dates() -> None:
    book = october_book()
    tab = service_for(book).ensure_month_tab(date(2026, 11, 1))

    assert tab == MonthTab("Nov 26", "Oct 26", created=True, hidden=["Oct 26"])
    duplicate = book.batches[0]["requests"][0]["duplicateSheet"]
    assert duplicate == {
        "sourceSheetId": 29,
        "insertSheetIndex": 29,  # перед прошлым месяцем, как остальные вкладки
        "newSheetName": "Nov 26",
    }
    visibility = [
        r["updateSheetProperties"]["properties"] for r in book.batches[1]["requests"]
    ]
    assert visibility == [
        {"sheetId": 999, "hidden": False},
        {"sheetId": 29, "hidden": True},
    ]
    [dates] = book.values
    assert dates["data"] == [{"range": "'Nov 26'!M1:M2", "values": [[46327], [46356]]}]


def test_existing_month_is_left_alone() -> None:
    book = october_book()
    tab = service_for(book).ensure_month_tab(date(2026, 10, 6))
    assert tab == MonthTab("Oct 26")
    assert book.batches == [] and book.values == []


def test_wrong_dates_of_an_existing_month_are_fixed() -> None:
    book = october_book()
    book.worksheet("Oct 26").dates = [[46266], [46295]]  # сентябрь
    tab = service_for(book).ensure_month_tab(date(2026, 10, 6))
    assert tab.dates_fixed and not tab.created
    assert book.values[0]["data"][0]["values"] == [[46296], [46326]]


def test_without_any_month_tab_nothing_is_created() -> None:
    book = Book([Tab("fact", 0, 0, False)])
    with pytest.raises(ValueError, match="нет вкладки прошлого месяца"):
        service_for(book).ensure_month_tab(date(2026, 11, 1))


def job_context(tab: MonthTab | Exception) -> SimpleNamespace:
    def ensure(day: date) -> MonthTab:
        if isinstance(tab, Exception):
            raise tab
        return tab

    return SimpleNamespace(
        bot_data={"gs_service": SimpleNamespace(ensure_month_tab=ensure)},
        bot=SimpleNamespace(send_message=AsyncMock()),
    )


@pytest.fixture
def owner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ANALYTICS_CHAT_ID", None)
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({77, 42}))


@pytest.mark.usefixtures("owner")
async def test_owner_is_told_about_the_new_tab() -> None:
    context = job_context(MonthTab("Nov 26", "Oct 26", created=True, hidden=["Oct 26"]))
    await month.month_tab_job(context)
    chat_id, text = context.bot.send_message.await_args.args
    assert chat_id == 42
    assert text.startswith("📅 Новый месяц: создал вкладку «Nov 26»")
    assert "Скрыл прошлые месяцы: Oct 26." in text


@pytest.mark.usefixtures("owner")
@pytest.mark.parametrize("result", [MonthTab("Oct 26"), ConnectionError("offline")])
async def test_quiet_when_nothing_changed_or_sheets_fail(
    result: MonthTab | Exception,
) -> None:
    context = job_context(result)
    await month.month_tab_job(context)
    context.bot.send_message.assert_not_awaited()


def test_owner_chat_prefers_the_report_chat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ANALYTICS_CHAT_ID", "-100500")
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({42}))
    assert month.owner_chat_id() == -100500
    monkeypatch.setattr(config, "ANALYTICS_CHAT_ID", None)
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset())
    assert month.owner_chat_id() is None
