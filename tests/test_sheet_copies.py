"""Копии листов для экранов Mini App: сколько живут и когда сбрасываются."""

import threading
from collections.abc import Callable
from typing import Any

import gspread
import pytest
from gspread.exceptions import WorksheetNotFound

from app.domain import Rates
from app.services import google_sheets
from app.services.google_sheets import (
    GoogleSheetsService,
    WriteAwareHTTPClient,
    read_month_sheets,
)

FACT = [["Дата", "Группа"], [46309, "🍔 ЕДА"]]


class CountingWorksheet:
    """Лист, который считает чтения; meanwhile — что успеть сделать во время чтения."""

    def __init__(self, rows: list[list[Any]]) -> None:
        self.rows = rows
        self.reads = 0
        self.meanwhile: Callable[[], None] | None = None

    def get_all_values(self, **options: Any) -> list[list[Any]]:
        self.reads += 1
        if self.meanwhile is not None:
            action, self.meanwhile = self.meanwhile, None
            action()
        return [list(row) for row in self.rows]

    def get(self, range_name: str, **options: Any) -> list[list[Any]]:
        self.reads += 1
        if self.meanwhile is not None:
            action, self.meanwhile = self.meanwhile, None
            action()
        return [["USD", 12500.0]]


class OneSheet:
    def __init__(self, ws: CountingWorksheet) -> None:
        self.ws = ws

    def worksheet(self, name: str) -> CountingWorksheet:
        return self.ws


def service_with(ws: CountingWorksheet) -> GoogleSheetsService:
    service = GoogleSheetsService.__new__(GoogleSheetsService)
    service.client = None
    service.sheet = OneSheet(ws)
    service._worksheets = {}
    service._balance_cells = None
    service._calendar = None
    service._values = {}
    service._rates = None
    service._generation = 0
    service._authenticate = lambda: None
    return service


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Часы monotonic, которые тест переводит сам: now[0] — текущее время."""
    now = [100.0]
    monkeypatch.setattr(google_sheets.time, "monotonic", lambda: now[0])
    return now


def test_a_screen_takes_a_copy_younger_than_its_max_age(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    first = service.get_values("fact", max_age=120)
    clock[0] += 119
    assert service.get_values("fact", max_age=120) is first
    assert ws.reads == 1


def test_an_older_copy_is_read_again(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    service.get_values("fact", max_age=120)
    clock[0] += 121
    service.get_values("fact", max_age=120)
    assert ws.reads == 2


def test_without_max_age_the_sheet_is_always_read(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    service.get_values("fact", max_age=120)
    assert service.get_values("fact") == FACT
    assert ws.reads == 2


def test_a_fresh_read_renews_the_copy(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    service.get_values("fact", max_age=120)
    clock[0] += 100
    fresh = service.get_values("fact")  # «Обновить»: таблица заново
    clock[0] += 100  # первой копии уже 200 секунд, свежей — 100
    assert service.get_values("fact", max_age=120) is fresh
    assert ws.reads == 2


def test_a_write_forgets_every_copy(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    service.get_values("fact", max_age=120)
    service.get_rates(max_age=120)
    service._written()
    service.get_values("fact", max_age=120)
    service.get_rates(max_age=120)
    assert ws.reads == 4


def test_a_read_during_a_write_is_not_kept(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    ws.meanwhile = service._written  # бот записал, пока лист читался
    service.get_values("fact", max_age=120)
    service.get_values("fact", max_age=120)
    assert ws.reads == 2


def test_rates_keep_a_copy_too(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    assert service.get_rates(max_age=120).to_uzs == {"UZS": 1.0, "USD": 12500.0}
    assert service.get_rates(max_age=120).to_uzs == {"UZS": 1.0, "USD": 12500.0}
    assert ws.reads == 1


def test_rates_read_during_a_write_are_not_kept(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    ws.meanwhile = service._written
    service.get_rates(max_age=120)
    service.get_rates(max_age=120)
    assert ws.reads == 2


def test_failed_rates_are_not_kept(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)

    def broken(range_name: str, **options: Any) -> list[list[Any]]:
        ws.reads += 1
        raise ConnectionError("sheets down")

    ws.get = broken  # type: ignore[method-assign]
    assert service.get_rates(max_age=120).to_uzs == {"UZS": 1.0}
    service.get_rates(max_age=120)
    assert ws.reads == 4  # по попытке с повтором на каждый вызов


def test_reload_forgets_the_copies(clock: list[float]) -> None:
    ws = CountingWorksheet(FACT)
    service = service_with(ws)
    service.get_values("fact", max_age=120)
    service.reload()
    service.get_values("fact", max_age=120)
    assert ws.reads == 2


class FakeResponse:
    ok = True


class FakeSession:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.methods: list[str] = []

    def request(self, method: str, url: str, **options: Any) -> FakeResponse:
        self.methods.append(method)
        if self.error is not None:
            raise self.error
        return FakeResponse()


def client_with(session: FakeSession) -> tuple[WriteAwareHTTPClient, list[str]]:
    client = WriteAwareHTTPClient(auth=None, session=session)  # type: ignore[arg-type]
    writes: list[str] = []
    client.on_write = lambda: writes.append("write")
    return client, writes


@pytest.mark.parametrize("method", ["post", "put", "delete"])
def test_every_change_of_the_table_is_reported(method: str) -> None:
    client, writes = client_with(FakeSession())
    client.request(method, "https://sheets.googleapis.com/v4/spreadsheets/x")
    assert writes == ["write"]


def test_reads_are_not_reported() -> None:
    client, writes = client_with(FakeSession())
    client.request("get", "https://sheets.googleapis.com/v4/spreadsheets/x/values/a")
    assert writes == []


def test_a_failed_write_is_reported_too() -> None:
    # ответ мог потеряться уже после того, как Google записал
    client, writes = client_with(FakeSession(ConnectionError("reset")))
    with pytest.raises(ConnectionError):
        client.request("post", "https://sheets.googleapis.com/v4/spreadsheets/x")
    assert writes == ["write"]


def test_the_connection_reports_writes_to_the_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    made: list[Any] = []

    class FakeClient:
        def __init__(self, http_client: type[WriteAwareHTTPClient]) -> None:
            self.http_client = http_client(auth=None, session=FakeSession())  # type: ignore[arg-type]

        def set_timeout(self, timeout: Any) -> None:
            pass

        def open_by_key(self, key: str) -> Any:
            return OneSheet(CountingWorksheet(FACT))

    def authorize(creds: Any, http_client: type[WriteAwareHTTPClient]) -> FakeClient:
        made.append(FakeClient(http_client))
        return made[-1]

    monkeypatch.setattr(google_sheets.config, "GOOGLE_APPLICATION_CREDENTIALS_PATH", "")
    monkeypatch.setattr(google_sheets.config, "GOOGLE_SERVICE_ACCOUNT_EMAIL", "b@t.iam")
    monkeypatch.setattr(google_sheets.config, "GOOGLE_PRIVATE_KEY", "k")
    monkeypatch.setattr(
        google_sheets.Credentials,
        "from_service_account_info",
        lambda info, scopes: object(),
    )
    monkeypatch.setattr(google_sheets.gspread, "authorize", authorize)
    service = GoogleSheetsService()
    service._values["fact"] = (0.0, FACT)
    made[0].http_client.request("post", "https://sheets.googleapis.com/x")
    assert service._values == {}


class MonthSheetsSource:
    """Таблица для read_month_sheets: что прочитали и с каким max_age."""

    def __init__(self, tabs: dict[str, list[list[Any]]]) -> None:
        self.tabs = tabs
        self.calls: list[tuple[str, float]] = []
        self.lock = threading.Lock()

    def get_values(self, name: str, max_age: float = 0.0) -> list[list[Any]]:
        with self.lock:
            self.calls.append((name, max_age))
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def get_rates(self, max_age: float = 0.0) -> Rates:
        with self.lock:
            self.calls.append(("rates", max_age))
        return Rates({"UZS": 1.0, "USD": 12500.0})

    def get_calendar(self, max_age: float = 0.0) -> list[list[Any]]:
        with self.lock:
            self.calls.append(("calendar", max_age))
        return [["ID"]]


def test_month_sheets_are_read_together_with_the_screen_max_age() -> None:
    source = MonthSheetsSource(
        {"Oct 26": [["План"]], "Sep 26": [["Сентябрь"]], "fact": FACT}
    )
    sheets = read_month_sheets(source, "Oct 26", max_age=120, previous=True)
    assert sheets.month == [["План"]]
    assert sheets.previous == [["Сентябрь"]]  # для переноса недополученного
    assert sheets.fact == FACT
    assert sheets.rates.to_uzs == {"UZS": 1.0, "USD": 12500.0}
    assert sheets.calendar == [["ID"]]
    assert sorted(source.calls) == [
        ("Oct 26", 120),
        ("Sep 26", 120),
        ("calendar", 120),
        ("fact", 120),
        ("rates", 120),
    ]


def test_without_the_month_tab_the_month_is_none() -> None:
    source = MonthSheetsSource({"fact": FACT})
    sheets = read_month_sheets(source, "Nov 26")
    assert (sheets.month, sheets.previous) == (None, None)


def test_january_carries_over_from_last_years_december() -> None:
    source = MonthSheetsSource(
        {"Jan 27": [["План"]], "Dec 26": [["Декабрь"]], "fact": FACT}
    )
    assert read_month_sheets(source, "Jan 27", previous=True).previous == [["Декабрь"]]


def test_screens_without_the_carry_over_do_not_read_last_months_tab() -> None:
    source = MonthSheetsSource(
        {"Oct 26": [["План"]], "Sep 26": [["Сентябрь"]], "fact": FACT}
    )
    sheets = read_month_sheets(source, "Oct 26", calendar=False, previous=False)
    assert sheets.previous is None
    assert ("Sep 26", 0.0) not in source.calls


class DownTab(MonthSheetsSource):
    """Одна вкладка не отвечает: сбой сети, а не «вкладки нет»."""

    def __init__(self, tabs: dict[str, list[list[Any]]], down: str) -> None:
        super().__init__(tabs)
        self.down = down

    def get_values(self, name: str, max_age: float = 0.0) -> list[list[Any]]:
        if name == self.down:
            raise ConnectionError("Google is down")
        return super().get_values(name, max_age)


def test_a_failed_read_of_last_months_tab_leaves_the_screen_without_carry_over(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tabs = {"Oct 26": [["План"]], "Sep 26": [["Сентябрь"]], "fact": FACT}
    sheets = read_month_sheets(DownTab(tabs, "Sep 26"), "Oct 26", previous=True)
    assert (sheets.month, sheets.previous) == ([["План"]], None)
    assert "Sep 26" in caplog.text


def test_a_failed_read_of_the_month_tab_still_fails() -> None:
    tabs = {"Oct 26": [["План"]], "Sep 26": [["Сентябрь"]], "fact": FACT}
    with pytest.raises(ConnectionError):
        read_month_sheets(DownTab(tabs, "Oct 26"), "Oct 26")


def test_the_plan_screen_does_not_read_the_calendar() -> None:
    source = MonthSheetsSource({"Oct 26": [["План"]], "fact": FACT})
    sheets = read_month_sheets(source, "Oct 26", calendar=False)
    assert sheets.calendar == []
    assert ("calendar", 0.0) not in source.calls


def test_sheets_are_read_at_the_same_time() -> None:
    # все пять чтений стоят у барьера одновременно — по очереди тест бы повис
    barrier = threading.Barrier(5, timeout=5)

    class Waiting(MonthSheetsSource):
        def get_values(self, name: str, max_age: float = 0.0) -> list[list[Any]]:
            barrier.wait()
            return super().get_values(name, max_age)

        def get_rates(self, max_age: float = 0.0) -> Rates:
            barrier.wait()
            return super().get_rates(max_age)

        def get_calendar(self, max_age: float = 0.0) -> list[list[Any]]:
            barrier.wait()
            return super().get_calendar(max_age)

    source = Waiting({"Oct 26": [], "fact": FACT})
    sheets = read_month_sheets(source, "Oct 26", previous=True)
    assert sheets.fact == FACT


class JsonResponse:
    ok = True

    def __init__(self, body: dict[str, Any]) -> None:
        self.body = body

    def json(self) -> dict[str, Any]:
        return self.body


class BookSession:
    """Google в памяти: метаданные таблицы с листом fact, на остальное — «{}»."""

    def __init__(self) -> None:
        self.methods: list[str] = []

    def request(self, method: str, url: str, **options: Any) -> JsonResponse:
        self.methods.append(method)
        if method == "get":
            sheet = {"title": "fact", "sheetId": 0, "index": 0}
            sheet["gridProperties"] = {"rowCount": 10, "columnCount": 8}
            return JsonResponse(
                {"properties": {"title": "budget"}, "sheets": [{"properties": sheet}]}
            )
        return JsonResponse({})


def test_every_kind_of_write_through_gspread_is_reported() -> None:
    # настоящий gspread: если его новая версия пойдёт в обход request, тест упадёт
    session = BookSession()
    client = gspread.Client(auth=None, session=session, http_client=WriteAwareHTTPClient)  # type: ignore[arg-type]
    writes: list[str] = []
    assert isinstance(client.http_client, WriteAwareHTTPClient)
    client.http_client.on_write = lambda: writes.append("write")
    book = client.open_by_key("key")
    sheet = book.worksheet("fact")
    sheet.append_rows([["a"]], table_range="A:H")
    sheet.update(values=[["b"]], range_name="A2")
    book.batch_update({"requests": []})
    book.values_batch_update({"valueInputOption": "RAW", "data": []})
    assert len(writes) == 4
    assert len(writes) == sum(method != "get" for method in session.methods)


def connect(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """GoogleSheetsService с поддельным gspread.authorize; made — выданные клиенты."""
    made: list[Any] = []

    class FakeClient:
        def __init__(self, http_client: type[Any]) -> None:
            self.http_client = http_client(auth=None, session=FakeSession())

        def set_timeout(self, timeout: Any) -> None:
            pass

        def open_by_key(self, key: str) -> Any:
            return OneSheet(CountingWorksheet(FACT))

    def authorize(creds: Any, http_client: type[Any]) -> Any:
        made.append(FakeClient(http_client))
        return made[-1]

    monkeypatch.setattr(google_sheets.config, "GOOGLE_APPLICATION_CREDENTIALS_PATH", "")
    monkeypatch.setattr(google_sheets.config, "GOOGLE_SERVICE_ACCOUNT_EMAIL", "b@t.iam")
    monkeypatch.setattr(google_sheets.config, "GOOGLE_PRIVATE_KEY", "k")
    monkeypatch.setattr(
        google_sheets.Credentials,
        "from_service_account_info",
        lambda info, scopes: object(),
    )
    monkeypatch.setattr(google_sheets.gspread, "authorize", authorize)
    return made


def test_a_reconnected_client_reports_writes_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    made = connect(monkeypatch)
    service = GoogleSheetsService()
    service._authenticate()  # протух токен — новый клиент
    service._values["fact"] = (0.0, FACT)
    made[-1].http_client.request("post", "https://sheets.googleapis.com/x")
    assert service._values == {}


def test_a_client_that_cannot_report_writes_is_logged(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    connect(monkeypatch)

    class Plain:
        """Клиент gspread без отметок записей — например, после смены версии."""

        def set_timeout(self, timeout: Any) -> None:
            pass

        def open_by_key(self, key: str) -> Any:
            return OneSheet(CountingWorksheet(FACT))

    monkeypatch.setattr(
        google_sheets.gspread, "authorize", lambda creds, http_client: Plain()
    )
    with caplog.at_level("ERROR"):
        GoogleSheetsService()
    assert "does not report writes" in caplog.text


def test_a_missing_sheet_is_not_a_reason_to_reconnect() -> None:
    class NoSheet:
        def worksheet(self, name: str) -> Any:
            raise WorksheetNotFound(name)

    service = service_with(CountingWorksheet(FACT))
    service.sheet = NoSheet()
    reconnects: list[int] = []
    service._authenticate = lambda: reconnects.append(1)
    with pytest.raises(WorksheetNotFound):
        service.get_values("Nov 26", max_age=120)
    # «Плана на ноябрь ещё нет» — обычное дело, не протухший токен
    assert reconnects == []
