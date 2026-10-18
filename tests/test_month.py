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
        self.rows: list[list[Any]] = [["Статья"]]  # значения вкладки для цели

    def get(self, range_name: str, **options: Any) -> list[list[Any]]:
        assert range_name == "M1:M2"
        return self.dates

    def get_all_values(self, **options: Any) -> list[list[Any]]:
        return self.rows


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
            copy.dates, copy.rows = source.dates, source.rows
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
    goals: list[Any] = []

    def ensure(day: date, goal: Any = None) -> MonthTab:
        goals.append(goal)
        if isinstance(tab, Exception):
            raise tab
        return tab

    return SimpleNamespace(
        bot_data={"gs_service": SimpleNamespace(ensure_month_tab=ensure)},
        bot=SimpleNamespace(send_message=AsyncMock()),
        goals=goals,
    )


@pytest.fixture
def owner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ANALYTICS_CHAT_ID", None)
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({77, 42}))
    monkeypatch.setattr(config, "OWNER_ID", 42)


@pytest.fixture(autouse=True)
def reconcile(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    sync = AsyncMock(return_value=0)
    monkeypatch.setattr(month, "sync_now", sync)
    return sync


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
    monkeypatch.setattr(config, "OWNER_ID", None)
    assert month.owner_chat_id() is None


def test_owner_is_the_first_listed_id_not_the_smallest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "ANALYTICS_CHAT_ID", None)
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({42, 77}))
    monkeypatch.setattr(config, "OWNER_ID", 77)
    assert month.owner_chat_id() == 77


def test_next_month_is_a_hidden_copy_of_the_current_one() -> None:
    book = october_book()
    tab = service_for(book).ensure_next_month_tab(date(2026, 10, 13))
    assert tab == MonthTab("Nov 26", "Oct 26", created=True)
    duplicate = book.batches[0]["requests"][0]["duplicateSheet"]
    assert duplicate == {
        "sourceSheetId": 29,
        "insertSheetIndex": 29,
        "newSheetName": "Nov 26",
    }
    visibility = [
        r["updateSheetProperties"]["properties"] for r in book.batches[1]["requests"]
    ]
    assert visibility == [{"sheetId": 999, "hidden": True}]  # октябрь остаётся виден
    [dates] = book.values
    assert dates["data"] == [{"range": "'Nov 26'!M1:M2", "values": [[46327], [46356]]}]


def test_next_month_that_exists_is_left_alone() -> None:
    book = october_book()
    november = Tab("Nov 26", 31, 28, hidden=True)
    november.dates = [[46327], [46356]]
    book.tabs.append(november)
    tab = service_for(book).ensure_next_month_tab(date(2026, 10, 13))
    assert tab == MonthTab("Nov 26")
    assert book.batches == [] and book.values == []


def test_next_month_created_meanwhile_is_not_an_error() -> None:
    class Busy(Book):
        def batch_update(self, body: dict[str, Any]) -> dict[str, Any]:
            if "duplicateSheet" in body["requests"][0]:
                # второй пользователь успел первым — имя уже занято
                self.tabs.append(Tab("Nov 26", 31, 28, hidden=True))
                raise RuntimeError('A sheet with the name "Nov 26" already exists')
            return super().batch_update(body)

    book = Busy(october_book().tabs)
    tab = service_for(book).ensure_next_month_tab(date(2026, 10, 13))
    assert tab == MonthTab("Nov 26")


def test_next_month_after_december_is_january() -> None:
    december = Tab("Dec 26", 40, 20, hidden=False)
    december.dates = [[46357], [46387]]
    book = Book([Tab("fact", 0, 6, False), december, Tab("system", 1, 42, False)])
    tab = service_for(book).ensure_next_month_tab(date(2026, 12, 20))
    assert tab.title == "Jan 27"
    assert book.values[0]["data"] == [
        {"range": "'Jan 27'!M1:M2", "values": [[46388], [46418]]}
    ]


def november_book() -> Book:
    """1 ноября: «Nov 26» составлен заранее и спрятан, октябрь ещё виден."""
    book = october_book()
    november = Tab("Nov 26", 31, 28, hidden=True)
    november.dates = [[46327], [46356]]
    book.tabs.append(november)
    return book


def test_planned_month_becomes_visible_and_past_months_hide() -> None:
    book = november_book()
    tab = service_for(book).ensure_month_tab(date(2026, 11, 1))
    assert tab == MonthTab("Nov 26", revealed=True, hidden=["Oct 26"])
    visibility = [
        r["updateSheetProperties"]["properties"] for r in book.batches[0]["requests"]
    ]
    assert visibility == [
        {"sheetId": 31, "hidden": False},
        {"sheetId": 29, "hidden": True},
    ]
    assert book.values == []


def test_a_month_opened_by_hand_is_not_hidden_again() -> None:
    book = november_book()
    book.worksheet("Nov 26").isSheetHidden = False  # уже показан
    tab = service_for(book).ensure_month_tab(date(2026, 11, 2))
    assert tab == MonthTab("Nov 26")
    assert book.batches == []


@pytest.mark.usefixtures("owner")
async def test_owner_is_told_the_planned_month_became_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(month, "local_today", lambda zone: date(2026, 11, 1))
    context = job_context(MonthTab("Nov 26", revealed=True, hidden=["Oct 26"]))
    await month.month_tab_job(context)
    _, text = context.bot.send_message.await_args.args
    assert text == (
        "📅 Новый месяц: вкладка «Nov 26» с планом, составленным заранее, "
        "стала текущей.\nСкрыл прошлые месяцы: Oct 26."
    )


def test_a_new_month_made_meanwhile_by_another_process_is_not_an_error() -> None:
    class Busy(Book):
        def batch_update(self, body: dict[str, Any]) -> dict[str, Any]:
            if "duplicateSheet" in body["requests"][0]:
                # кнопка «Создать план» в ту же секунду сделала вкладку и спрятала октябрь
                november = Tab("Nov 26", 31, 28, hidden=False)
                november.dates = [[46327], [46356]]
                self.tabs.append(november)
                self.worksheet("Oct 26").isSheetHidden = True
                raise RuntimeError('A sheet with the name "Nov 26" already exists')
            return super().batch_update(body)

    book = Busy(october_book().tabs)
    assert service_for(book).ensure_month_tab(date(2026, 11, 1)) == MonthTab("Nov 26")


def test_listing_after_a_reconnect_uses_the_new_connection() -> None:
    class Dead(Book):
        def worksheets(self) -> list[Tab]:
            raise ConnectionError("token expired")

    fresh = october_book()
    service = service_for(Dead(fresh.tabs))
    service._authenticate = lambda: setattr(service, "sheet", fresh)
    assert service.ensure_month_tab(date(2026, 10, 6)) == MonthTab("Oct 26")


def test_on_the_first_past_months_hide_even_if_the_month_was_shown_by_hand() -> None:
    book = november_book()
    book.worksheet("Nov 26").isSheetHidden = False  # показали руками до 1-го
    tab = service_for(book).ensure_month_tab(date(2026, 11, 1))
    assert tab == MonthTab("Nov 26", hidden=["Oct 26"])
    visibility = [
        r["updateSheetProperties"]["properties"] for r in book.batches[0]["requests"]
    ]
    assert visibility == [{"sheetId": 29, "hidden": True}]


def test_a_month_hidden_by_hand_and_shown_again_is_not_called_new() -> None:
    text = month.describe(MonthTab("Oct 26", revealed=True), date(2026, 10, 14))
    assert text == "📅 Вкладка «Oct 26» была скрыта — показал её: это текущий месяц."


def test_past_months_hidden_on_the_first_are_reported() -> None:
    text = month.describe(MonthTab("Nov 26", hidden=["Oct 26"]), date(2026, 11, 1))
    assert text == (
        "📅 Новый месяц: вкладка «Nov 26» уже открыта.\n"
        "Скрыл прошлые месяцы: Oct 26."
    )


def test_a_half_made_next_month_is_finished_on_the_next_try() -> None:
    """Копия есть, но спрятать и поставить даты помешал сбой — доделать."""
    book = october_book()
    half = Tab("Nov 26", 31, 28, hidden=False)
    half.dates = [[46296], [46326]]  # даты октября, как у источника
    book.tabs.append(half)
    tab = service_for(book).ensure_next_month_tab(date(2026, 10, 13))
    assert tab == MonthTab("Nov 26")
    visibility = [
        r["updateSheetProperties"]["properties"] for r in book.batches[0]["requests"]
    ]
    assert visibility == [{"sheetId": 31, "hidden": True}]
    assert book.values[0]["data"] == [
        {"range": "'Nov 26'!M1:M2", "values": [[46327], [46356]]}
    ]


def test_a_copy_made_despite_an_error_is_finished() -> None:
    class Lost(Book):
        def batch_update(self, body: dict[str, Any]) -> dict[str, Any]:
            if "duplicateSheet" in body["requests"][0]:
                copy = Tab("Nov 26", 999, 29, hidden=False)
                copy.dates = [[46296], [46326]]
                self.tabs.append(copy)
                raise RuntimeError("the reply was lost")  # копия сделана, ответа нет
            return super().batch_update(body)

    book = Lost(october_book().tabs)
    tab = service_for(book).ensure_next_month_tab(date(2026, 10, 13))
    assert tab == MonthTab("Nov 26")
    visibility = [
        r["updateSheetProperties"]["properties"] for r in book.batches[-1]["requests"]
    ]
    assert visibility == [{"sheetId": 999, "hidden": True}]
    assert book.values[-1]["data"] == [
        {"range": "'Nov 26'!M1:M2", "values": [[46327], [46356]]}
    ]


def test_hiding_the_copy_survives_one_failure() -> None:
    class Flaky(Book):
        failed = False

        def batch_update(self, body: dict[str, Any]) -> dict[str, Any]:
            if "updateSheetProperties" in body["requests"][0] and not self.failed:
                self.failed = True
                raise ConnectionError("token expired")
            return super().batch_update(body)

    book = Flaky(october_book().tabs)
    service = service_for(book)
    reconnects: list[bool] = []
    service._authenticate = lambda: reconnects.append(True)
    tab = service.ensure_next_month_tab(date(2026, 10, 13))
    assert tab == MonthTab("Nov 26", "Oct 26", created=True)
    assert reconnects == [True]
    visibility = [
        r["updateSheetProperties"]["properties"] for r in book.batches[-1]["requests"]
    ]
    assert visibility == [{"sheetId": 999, "hidden": True}]


@pytest.mark.usefixtures("owner")
async def test_the_list_is_reconciled_every_night(reconcile: AsyncMock) -> None:
    context = job_context(MonthTab("Oct 26"))
    await month.month_tab_job(context)
    reconcile.assert_awaited_once()
    assert reconcile.await_args.args[0] is context.bot_data


@pytest.mark.usefixtures("owner")
async def test_no_reconciliation_when_the_sheet_fails(reconcile: AsyncMock) -> None:
    await month.month_tab_job(job_context(ConnectionError("offline")))
    reconcile.assert_not_awaited()


# --- «Отложить за месяц» новой вкладки (этап 3, спека 4.7) ---


class Goal:
    """Цель новой вкладки: что спросили и что ответить."""

    def __init__(self, answer: tuple[int, float] | Exception | None) -> None:
        self.answer = answer
        self.calls: list[tuple[list[list[Any]], date]] = []

    def __call__(self, rows: list[list[Any]], first: date) -> tuple[int, float] | None:
        self.calls.append((rows, first))
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


def test_a_new_tab_gets_the_goal_with_its_dates() -> None:
    book, goal = october_book(), Goal((20, 7_200_000.0))
    service_for(book).ensure_month_tab(date(2026, 11, 1), goal)
    [write] = book.values
    assert write["data"] == [
        {"range": "'Nov 26'!M1:M2", "values": [[46327], [46356]]},
        {"range": "'Nov 26'!Q20:R20", "values": [[7_200_000.0, "UZS"]]},
    ]
    assert [first for _, first in goal.calls] == [date(2026, 11, 1)]


def test_right_dates_keep_the_owners_goal() -> None:
    book, goal = october_book(), Goal((20, 7_200_000.0))
    service_for(book).ensure_month_tab(date(2026, 10, 6), goal)
    assert book.values == [] and goal.calls == []


def test_a_failed_goal_still_writes_the_dates() -> None:
    book = october_book()
    service_for(book).ensure_month_tab(date(2026, 11, 1), Goal(ConnectionError("x")))
    [write] = book.values
    assert write["data"] == [{"range": "'Nov 26'!M1:M2", "values": [[46327], [46356]]}]


def test_the_next_month_tab_gets_the_goal() -> None:
    book, goal = october_book(), Goal((20, -9_800_000.0))
    service_for(book).ensure_next_month_tab(date(2026, 10, 20), goal)
    assert book.values[-1]["data"][-1] == {
        "range": "'Nov 26'!Q20:R20",
        "values": [[-9_800_000.0, "UZS"]],
    }


@pytest.mark.usefixtures("owner")
async def test_the_job_asks_the_piggies_for_the_goal() -> None:
    context = job_context(MonthTab("Oct 26"))
    await month.month_tab_job(context)
    assert len(context.goals) == 1 and callable(context.goals[0])
