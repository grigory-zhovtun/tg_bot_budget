"""«Накопления»: GET /api/savings и правки копилок POST/PUT /api/piggy…"""

import asyncio
import time
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from app import config
from app.handlers.receipts import DIRTY
from app.services.analytics_service import SavingsTab
from app.services.day_budget import day_budget
from app.services.google_sheets import PIGGY_JOURNAL_HEADER, PIGGY_LIST_HEADER
from app.services.piggy import read_book, savings_screen
from app.services.savings import month_savings
from app.web.server import create_app
from tests.test_day_budget import RATES, fact_row, frame, tab
from tests.test_web_api import OWNER, TODAY, WIFE, auth, client_for, make_application

SOON = TODAY + timedelta(days=30)  # окно трат уже открыто: с 30 дней назад
ROW = ["p3k9za", "Кафе с друзьями", 12000000, "UZS", f"{SOON:%d.%m.%Y}", "каждый год",
       "🍔 ЕДА", "кафе", "", "", "", "", "Иван", ""]  # fmt: skip


def saved(amount: float, piggy_id: str = "p3k9za", currency: str = "UZS") -> list[Any]:
    day = TODAY - timedelta(days=2)
    return [f"{day:%d.%m.%Y}", piggy_id, "Кафе с друзьями", amount, currency,
            "отложил", "Иван", ""]  # fmt: skip


@pytest.fixture(autouse=True)
def allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({OWNER, WIFE}))


class PiggySheets:
    """Листы копилок в памяти с теми же методами, что у GoogleSheetsService."""

    def __init__(
        self,
        rows: list[list[Any]] | None = None,
        moves: list[list[Any]] | None = None,
        exists: bool = True,
    ) -> None:
        # копии строк: правка в одном тесте не должна менять ROW для следующих
        self.list = (
            [list(PIGGY_LIST_HEADER), *(list(row) for row in rows or [])]
            if exists
            else None
        )
        self.journal = (
            [list(PIGGY_JOURNAL_HEADER), *(list(row) for row in moves or [])]
            if exists
            else None
        )
        self.fail = False
        self.created = 0
        self.updates: list[tuple[str, dict[str, Any]]] = []

    def get_piggy(self, max_age: float = 0.0) -> tuple[list[Any], list[Any]]:
        return [list(r) for r in self.list or []], [list(r) for r in self.journal or []]

    def ensure_piggy_sheets(self) -> None:
        if self.list is None:
            time.sleep(0.05)  # создание листа идёт по сети — второй запрос успеет
            self.created += 1
            self.list, self.journal = [list(PIGGY_LIST_HEADER)], [
                list(PIGGY_JOURNAL_HEADER)
            ]

    def add_piggy(self, row: list[Any]) -> int:
        if self.fail:
            raise ConnectionError("Google is down")
        self.ensure_piggy_sheets()
        assert self.list is not None
        self.list.append(list(row))
        return len(self.list)

    def update_piggy(self, piggy_id: str, cells: dict[str, Any]) -> bool:
        assert self.list is not None
        for row in self.list[1:]:
            if row[0] == piggy_id:
                for column, value in cells.items():
                    row[ord(column) - ord("A")] = value
                self.updates.append((piggy_id, dict(cells)))
                return True
        return False

    def add_piggy_moves(self, rows: list[list[Any]]) -> None:
        if self.fail:
            raise ConnectionError("Google is down")
        self.ensure_piggy_sheets()
        assert self.journal is not None
        self.journal.extend(list(row) for row in rows)


class PiggyAnalytics:
    """«Накопления» по листам в памяти: что записали — то и в ответе."""

    def __init__(self, sheets: PiggySheets, *fact: list[Any]) -> None:
        self.sheets = sheets
        self.fact = fact
        self.ages: list[float] = []

    def savings(
        self,
        today: date,
        currencies: Any,
        groups: Any,
        subcategories: Any,
        max_age: float = 0.0,
    ) -> SavingsTab:
        self.ages.append(max_age)
        piggies, journal = self.sheets.get_piggy(max_age)
        book = read_book(piggies, journal, currencies, groups, subcategories)
        found = frame(*self.fact)
        rows = tab()
        budget = day_budget(found, rows, today, RATES, "USD")
        screen = savings_screen(book, found, rows, today, RATES, budget, 0.1, 6)
        return SavingsTab(screen, month_savings(found, today), RATES)


def setup(
    sheets: PiggySheets | None = None, *fact: list[Any]
) -> tuple[Any, PiggySheets]:
    sheets = PiggySheets() if sheets is None else sheets
    application = make_application()
    application.bot_data["gs_service"] = sheets
    application.bot_data["analytics_service"] = PiggyAnalytics(sheets, *fact)
    application.bot.get_chat = AsyncMock(
        return_value=SimpleNamespace(first_name="Иван")
    )
    return application, sheets


def body(**changes: Any) -> dict[str, Any]:
    value = {
        "entry_id": str(uuid4()),
        "name": "Кафе с друзьями",
        "target": "12000000",
        "currency": "UZS",
        "deadline": SOON.isoformat(),
        "repeat": "yearly",
        "group": "🍔 ЕДА",
        "subcategory": "кафе",
    }
    return {**value, **changes}


def told(application: Any) -> list[str]:
    return [
        call.kwargs["text"]
        for call in application.bot.send_message.await_args_list
        if call.kwargs.get("chat_id") == WIFE
    ]


def test_savings_screen_shape() -> None:
    application, _ = setup(PiggySheets([ROW], [saved(6_000_000)]))
    response = client_for(application).get("/api/savings", headers=auth())
    assert response.status_code == 200
    found = response.json()
    assert found["status"] == "ok"
    assert len(found["plan"]) == 12
    assert found["rates"]["USD"] == 12_000
    piggy = found["piggies"][0]
    assert (piggy["id"], piggy["name"], piggy["repeat"]) == (
        "p3k9za",
        "Кафе с друзьями",
        "yearly",
    )
    assert (piggy["saved"], piggy["balance"], piggy["status"]) == (
        6_000_000,
        6_000_000,
        "on_track",
    )
    assert found["cushion"]["piggies"] == 500  # 6 млн по курсу 12 000
    assert len(found["savings"]["months"]) == 6
    assert set(found["month"]) == {
        "month", "piggy", "piggy_left", "share", "share_to", "goals",
    }  # fmt: skip


def test_savings_takes_copies_unless_asked_to_refresh() -> None:
    application, _ = setup()
    client = client_for(application)
    client.get("/api/savings", headers=auth())
    client.get("/api/savings?fresh=1", headers=auth())
    assert application.bot_data["analytics_service"].ages == [120.0, 0.0]


def test_create_writes_the_row_and_tells_the_other() -> None:
    application, sheets = setup()
    response = client_for(application).post("/api/piggy", json=body(), headers=auth())
    assert response.status_code == 200
    assert sheets.list is not None
    row = sheets.list[1]
    assert row[1:8] == ["Кафе с друзьями", 12_000_000, "UZS", f"{SOON:%d.%m.%Y}",
                        "каждый год", "🍔 ЕДА", "кафе"]  # fmt: skip
    assert (row[12], len(row[0])) == ("Иван", 6)
    assert response.json()["piggies"][0]["name"] == "Кафе с друзьями"
    assert told(application) == [
        f"🐷 Иван — новая копилка: Кафе с друзьями 12 000 000 UZS к {SOON:%d.%m}, "
        "каждый год"
    ]
    assert application.bot_data[DIRTY] is True  # «Потрачено» и взнос — на лист фоном


def test_a_long_goal_needs_neither_a_target_nor_a_deadline() -> None:
    application, sheets = setup()
    goal = body(name="Машина", target=None, currency="USD", deadline=None,
                repeat="once", group=None, subcategory=None)  # fmt: skip
    assert (
        client_for(application)
        .post("/api/piggy", json=goal, headers=auth())
        .status_code
        == 200
    )
    assert sheets.list is not None
    assert sheets.list[1][1:8] == ["Машина", "", "USD", "", "", "", ""]
    assert told(application) == ["🐷 Иван — новая копилка: Машина, без срока"]


async def test_two_first_piggies_create_the_sheets_once() -> None:
    application, sheets = setup(PiggySheets(exists=False))
    transport = httpx.ASGITransport(app=create_app(application, "s"))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        answers = await asyncio.gather(
            client.post("/api/piggy", json=body(), headers=auth()),
            client.post("/api/piggy", json=body(name="Отпуск"), headers=auth(WIFE)),
        )
    assert [answer.status_code for answer in answers] == [200, 200]
    assert sheets.created == 1
    assert sheets.list is not None
    assert sorted(row[1] for row in sheets.list[1:]) == ["Кафе с друзьями", "Отпуск"]


def test_a_repeated_entry_writes_once() -> None:
    application, sheets = setup()
    client = client_for(application)
    same = body()
    client.post("/api/piggy", json=same, headers=auth())
    assert client.post("/api/piggy", json=same, headers=auth()).status_code == 200
    assert sheets.list is not None and len(sheets.list) == 2


@pytest.mark.parametrize(
    ("change", "field", "message"),
    [
        ({"name": ""}, "name", "Название — от 1 до 40 символов"),
        ({"name": "Я" * 41}, "name", "Название — от 1 до 40 символов"),
        ({"target": "0"}, "target", "Цель — больше нуля, до двух знаков после запятой"),
        ({"currency": "EUR"}, "currency", "Такой валюты нет у карт"),
        (
            {"deadline": (TODAY - timedelta(days=1)).isoformat()},
            "deadline",
            "Срок — не раньше сегодня и не дальше чем через 5 лет",
        ),
        (
            {"deadline": (TODAY + timedelta(days=6 * 366)).isoformat()},
            "deadline",
            "Срок — не раньше сегодня и не дальше чем через 5 лет",
        ),
        ({"deadline": None}, "repeat", "Каждый год — только со сроком"),
        ({"group": "🚀 КОСМОС"}, "group", "Такой статьи нет — обновите приложение"),
        ({"group": "💰 ДОХОДЫ", "subcategory": "зарплата"}, "group",
         "Такой статьи нет — обновите приложение"),  # fmt: skip
        (
            {"subcategory": "продукты"},
            "subcategory",
            "Такой подкатегории нет — обновите приложение",
        ),
    ],
)
def test_fields_are_checked(change: dict[str, Any], field: str, message: str) -> None:
    application, sheets = setup()
    response = client_for(application).post(
        "/api/piggy", json=body(**change), headers=auth()
    )
    assert response.status_code == 422
    assert response.json()["error"]["fields"][field] == message
    assert sheets.list is not None and len(sheets.list) == 1


def test_an_edit_writes_only_the_owner_columns() -> None:
    application, sheets = setup(PiggySheets([ROW]))
    changed = body(entry_id=None, name="Кафе", target="15000000")
    response = client_for(application).put(
        "/api/piggy/p3k9za", json=changed, headers=auth()
    )
    assert response.status_code == 200
    piggy_id, cells = sheets.updates[0]
    assert piggy_id == "p3k9za"
    assert sorted(cells) == ["B", "C", "D", "E", "F", "G", "H", "M"]
    assert (cells["B"], cells["C"]) == ("Кафе", 15_000_000)
    assert told(application)[0].startswith(
        "🐷 Иван — копилка изменена: Кафе 15 000 000 UZS"
    )


def test_the_currency_of_a_piggy_with_moves_cannot_change() -> None:
    application, sheets = setup(PiggySheets([ROW], [saved(1_000_000)]))
    response = client_for(application).put(
        "/api/piggy/p3k9za", json=body(entry_id=None, currency="USD"), headers=auth()
    )
    assert response.status_code == 422
    assert response.json()["error"]["fields"]["currency"] == (
        "Валюту не поменять: в копилке есть движения. Закройте её и заведите новую"
    )
    assert sheets.updates == []


def test_spending_more_than_the_piggy_holds() -> None:
    application, sheets = setup(PiggySheets([ROW], [saved(1_000_000)]))
    response = client_for(application).post(
        "/api/piggy/p3k9za/moves",
        json={"entry_id": str(uuid4()), "kind": "spent", "amount": "2000000"},
        headers=auth(),
    )
    assert response.status_code == 422
    assert response.json()["error"]["fields"]["amount"] == (
        "В копилке только 1 000 000 UZS"
    )
    assert sheets.journal is not None and len(sheets.journal) == 2


def test_moves_go_to_the_journal_with_their_sign() -> None:
    application, sheets = setup(PiggySheets([ROW], [saved(1_000_000)]))
    client = client_for(application)
    for kind, amount in (
        ("saved", "500000"),
        ("spent", "300000"),
        ("write_off", "200000"),
    ):
        response = client.post(
            "/api/piggy/p3k9za/moves",
            json={"entry_id": str(uuid4()), "kind": kind, "amount": amount},
            headers=auth(),
        )
        assert response.status_code == 200
    assert sheets.journal is not None
    assert [(row[3], row[5]) for row in sheets.journal[2:]] == [
        (500_000, "отложил"),
        (-300_000, "потратил"),
        (-200_000, "списал остаток"),
    ]
    assert told(application)[0] == "🐷 Иван — отложил: Кафе с друзьями 500 000 UZS"


def test_close_returns_the_rest_to_the_cushion() -> None:
    application, sheets = setup(PiggySheets([ROW], [saved(1_000_000)]))
    response = client_for(application).post("/api/piggy/p3k9za/close", headers=auth())
    assert response.status_code == 200
    assert sheets.journal is not None and sheets.list is not None
    assert sheets.journal[-1][3:6] == [-1_000_000, "UZS", "закрыта"]
    assert sheets.list[1][13] == f"{TODAY:%d.%m.%Y}"
    assert response.json()["piggies"] == []  # закрытые не показываются
    assert told(application) == [
        "🐷 Иван — копилка закрыта: Кафе с друзьями (остаток — в подушку)"
    ]


def test_one_sum_is_distributed_by_the_plan() -> None:
    application, sheets = setup(PiggySheets([ROW]))
    client = client_for(application)
    plan = client.get("/api/savings", headers=auth()).json()
    left = plan["piggies"][0]["left_this_month"]
    assert left > 0
    response = client.post(
        "/api/piggy/save",
        json={
            "entry_id": str(uuid4()),
            "amount": str(left + 1_000_000),
            "currency": "UZS",
        },
        headers=auth(),
    )
    assert response.status_code == 200
    assert sheets.journal is not None
    # копилке — её «осталось», остальное — подушке, его не пишем
    assert [(row[1], row[3], row[5]) for row in sheets.journal[1:]] == [
        ("p3k9za", left, "отложил")
    ]
    assert response.json()["piggies"][0]["left_this_month"] == 0


def test_a_spending_can_be_taken_off_the_piggy() -> None:
    cafe = fact_row(TODAY - timedelta(days=1), "🍔 ЕДА", "кафе", 300_000)
    application, sheets = setup(PiggySheets([ROW], [saved(1_000_000)]), cafe)
    client = client_for(application)
    spends = client.get("/api/savings", headers=auth()).json()["piggies"][0]["spends"]
    assert [item["amount"] for item in spends] == [300_000]
    key = spends[0]["key"]
    for _ in range(2):  # второе нажатие строку не повторит
        response = client.post(
            "/api/piggy/p3k9za/exclude", json={"key": key}, headers=auth()
        )
        assert response.status_code == 200
    assert sheets.journal is not None
    marks = [row for row in sheets.journal[1:] if row[5] == "не из копилки"]
    assert [(row[3], row[7]) for row in marks] == [(0, key)]
    assert response.json()["piggies"][0]["spent"] == 0
    day = TODAY - timedelta(days=1)
    assert told(application) == [
        f"🐷 Иван — не из копилки: {day:%d.%m} ЕДА / кафе 300 000"
    ]


def test_an_unknown_piggy_is_404() -> None:
    application, _ = setup(PiggySheets([ROW]))
    client = client_for(application)
    for response in (
        client.put("/api/piggy/zzz999", json=body(entry_id=None), headers=auth()),
        client.post("/api/piggy/zzz999/close", headers=auth()),
        client.post(
            "/api/piggy/zzz999/moves",
            json={"entry_id": str(uuid4()), "kind": "saved", "amount": "1"},
            headers=auth(),
        ),
        client.post("/api/piggy/zzz999/exclude", json={"key": "x"}, headers=auth()),
    ):
        assert (response.status_code, response.json()["error"]["message"]) == (
            404,
            "Копилку уже закрыли — обновите «Накопления»",
        )


def test_a_sheet_failure_is_503_and_a_retry_writes() -> None:
    application, sheets = setup()
    sheets.fail = True
    client = client_for(application)
    same = body()
    response = client.post("/api/piggy", json=same, headers=auth())
    assert (response.status_code, response.json()["error"]["code"]) == (
        503,
        "sheets_unavailable",
    )
    sheets.fail = False
    assert client.post("/api/piggy", json=same, headers=auth()).status_code == 200
    assert sheets.list is not None and len(sheets.list) == 2


def test_piggies_need_a_signature() -> None:
    application, _ = setup()
    client = client_for(application)
    assert client.get("/api/savings").status_code == 401
    assert client.post("/api/piggy", json=body()).status_code == 401


def test_journal_sums_are_rounded_to_the_currency() -> None:
    from app.services.piggy import Piggy
    from app.web.api import move_row

    trip = Piggy("tr0001", 2, "Отпуск", 2500.0, "USD", None, False, None, None,
                 "Тест", None)  # fmt: skip
    assert move_row(TODAY, trip, -(0.1 + 0.2), "закрыта", "Тест")[3] == -0.3
    sums = Piggy("p3k9za", 3, "Новый год", 12e6, "UZS", None, False, None, None,
                 "Тест", None)  # fmt: skip
    zero = move_row(TODAY, sums, -0.0, "закрыта", "Тест")[3]
    assert zero == 0 and str(zero) == "0.0"
