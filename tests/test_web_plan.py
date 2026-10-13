"""API вкладки «План»: план месяца, сохранение, конфликты, уведомления."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from app import config
from app.domain import month_title
from app.services.analytics_service import MONTHS_RU
from app.services.google_sheets import MonthTab
from app.services.plan import shift_month
from app.web.server import create_app
from tests.test_day_budget import FACT_HEADER, fact_row
from tests.test_month_plan import TAB, PlanSheets, article, month_tab
from tests.test_web_api import (
    OWNER,
    TODAY,
    WIFE,
    auth,
    client_for,
    make_application,
)

CURRENT = TODAY.replace(day=1)
NEXT = shift_month(CURRENT, 1)
MONTH = f"{CURRENT:%Y-%m}"
TITLE = month_title(CURRENT)


@pytest.fixture(autouse=True)
def allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({OWNER, WIFE}))


def sheets_with() -> PlanSheets:
    previous = shift_month(CURRENT, -1) + timedelta(days=4)
    fact = [FACT_HEADER, fact_row(previous, "🍔 ЕДА", "кофе", 900_000)]
    return PlanSheets({TITLE: TAB}, fact)


def get_plan(sheets: Any, month: str = MONTH, user: int = OWNER) -> Any:
    application = make_application(sheets)
    return client_for(application).get(f"/api/plan?month={month}", headers=auth(user))


def test_plan_has_rows_with_hints_events_goal_and_rates() -> None:
    body = get_plan(sheets_with()).json()
    assert (body["status"], body["month"], body["title"]) == ("ok", MONTH, TITLE)
    assert body["rows"][0] == {
        "key": "кофе|UZS",
        "kind": "spend",
        "group": "🍔 ЕДА",
        "subcategory": "кофе",
        "icon": "☕",
        "currency": "UZS",
        "plan": 1_500_000,
        "average": 300_000,
        "last": 900_000,
        "current": 0,
        "editable": True,
    }
    assert body["events"][0] == {
        "slot": 0,
        "name": "Аванс",
        "day": 15,
        "amount": 3_000_000,
        "currency": "UZS",
    }
    assert body["events"][2]["name"] is None
    assert body["goal"] == {"amount": 1_000_000, "currency": "UZS"}
    assert body["rates"]["USD"] == 12_000
    assert body["history"]["last"] == f"{shift_month(CURRENT, -1):%Y-%m}"
    assert body["history"]["current_days"] == TODAY.day
    assert body["saved"] is None


def test_the_next_month_without_a_tab_is_not_created() -> None:
    body = get_plan(sheets_with(), f"{NEXT:%Y-%m}").json()
    assert (body["status"], body["rows"], body["events"]) == ("not_created", [], None)


def test_the_current_month_without_a_tab() -> None:
    assert get_plan(PlanSheets({})).json()["status"] == "no_month_tab"


@pytest.mark.parametrize("month", ["", "2026-13", "1999-01", "abc"])
def test_other_months_are_refused(month: str) -> None:
    response = get_plan(sheets_with(), month)
    assert response.status_code == 422
    assert response.json()["error"]["fields"] == {
        "month": "Можно открыть только текущий или следующий месяц"
    }


def test_plan_sheet_failure_is_503() -> None:
    class Broken(PlanSheets):
        def get_values(self, name: str) -> list[list[Any]]:
            raise ConnectionError("Google is down")

    response = get_plan(Broken({}))
    assert (response.status_code, response.json()["error"]["code"]) == (
        503,
        "sheets_unavailable",
    )


def test_plan_needs_a_signature() -> None:
    client = client_for(make_application(sheets_with()))
    assert client.get(f"/api/plan?month={MONTH}").status_code == 401


def test_plan_offers_the_catalog_pairs_missing_in_the_tab() -> None:
    body = get_plan(sheets_with()).json()
    pairs = [(item["subcategory"], item["currency"]) for item in body["addable"]]
    assert pairs[:3] == [("продукты", "UZS"), ("продукты", "RUB"), ("продукты", "USD")]
    assert ("кофе", "UZS") not in pairs  # уже во вкладке
    assert all(item["group"] != "💳 СЧЕТА" for item in body["addable"])
    coffee = body["addable"][pairs.index(("кофе", "USD"))]
    assert coffee == {
        "kind": "spend",
        "group": "🍔 ЕДА",
        "subcategory": "кофе",
        "icon": "☕",
        "currency": "USD",
        "average": 0,
        "last": 0,
        "current": 0,
    }
    assert body["addable"][pairs.index(("зарплата", "USD"))]["kind"] == "income"


EMPTY_CELLS = {"name": None, "day": None, "amount": None, "currency": None}


def change(**over: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "month": MONTH,
        "rows": [{"key": "кофе|UZS", "base": 1_500_000, "plan": "1800000"}],
        "events": [],
        "goal": None,
    }
    return {**body, **over}


def plan_app(sheets: PlanSheets | None = None) -> Any:
    application = make_application(sheets or sheets_with())
    application.bot.get_chat = AsyncMock(
        return_value=SimpleNamespace(first_name="Иван")
    )
    return application


def post_plan(application: Any, body: dict[str, Any], user: int = OWNER) -> Any:
    return client_for(application).post("/api/plan", json=body, headers=auth(user))


def test_save_writes_the_cells_and_answers_with_the_fresh_plan() -> None:
    sheets = sheets_with()
    response = post_plan(plan_app(sheets), change())
    assert response.status_code == 200
    body = response.json()
    assert (body["saved"], body["rows"][0]["plan"]) == (1, 1_800_000)
    assert sheets.writes == [[{"range": f"'{TITLE}'!D2", "values": [[-1_800_000.0]]}]]


CAFE = {"group": "🍔 ЕДА", "subcategory": "кафе", "currency": "UZS", "plan": "700000"}


def test_new_articles_are_inserted_saved_and_told() -> None:
    sheets = sheets_with()
    application = plan_app(sheets)
    response = post_plan(application, change(new_rows=[CAFE]))
    assert response.status_code == 200
    body = response.json()
    assert body["saved"] == 2
    assert [(row["key"], row["plan"]) for row in body["rows"][:2]] == [
        ("кофе|UZS", 1_800_000),
        ("кафе|UZS", 700_000),
    ]
    pairs = [(item["subcategory"], item["currency"]) for item in body["addable"]]
    assert ("кафе", "UZS") not in pairs
    note = application.bot.send_message.await_args.kwargs["text"]
    assert "＋ кафе: 700 000" in note


def test_no_room_for_new_articles_is_said_plainly() -> None:
    rows = [article("Расходы", "🚧 РАЗНОЕ", f"статья {n}", 1) for n in range(59)]
    sheets = PlanSheets({TITLE: month_tab(rows)})
    response = post_plan(plan_app(sheets), change(rows=[], new_rows=[CAFE]))
    assert response.status_code == 422
    message = "Во вкладке нет места для новых статей"
    assert response.json()["error"] == {
        "code": "validation",
        "message": message,
        "fields": {"new_rows": message},
    }
    assert sheets.applied == []


def test_a_new_article_already_in_the_tab_is_a_conflict() -> None:
    sheets = sheets_with()
    coffee = {**CAFE, "subcategory": "кофе"}
    response = post_plan(plan_app(sheets), change(rows=[], new_rows=[coffee]))
    assert response.status_code == 409
    assert response.json()["conflicts"] == ["кофе|UZS"]
    assert sheets.applied == [] and sheets.writes == []


def test_cents_of_a_currency_row_reach_the_sheet() -> None:
    sheets = sheets_with()
    rows = [{"key": "квартплата|USD", "base": 500, "plan": "650.5"}]
    assert post_plan(plan_app(sheets), change(rows=rows)).status_code == 200
    assert sheets.writes[0][0]["values"] == [[-650.5]]


def test_events_and_goal_are_saved() -> None:
    sheets = sheets_with()
    body = change(
        rows=[],
        events=[
            {
                "slot": 2,
                "base": EMPTY_CELLS,
                "value": {
                    "name": "Премия",
                    "day": 25,
                    "amount": "1000000",
                    "currency": "UZS",
                },
            },
            {
                "slot": 1,
                "base": {
                    "name": "Квартплата",
                    "day": 18,
                    "amount": -500,
                    "currency": "USD",
                },
                "value": {
                    "name": "Квартплата",
                    "day": 18,
                    "amount": "-650.5",
                    "currency": "USD",
                },
            },
        ],
        goal={
            "base": {"amount": 1_000_000, "currency": "UZS"},
            "value": {"amount": "2000000", "currency": "UZS"},
        },
    )
    response = post_plan(plan_app(sheets), body)
    assert response.status_code == 200
    fresh = response.json()
    assert fresh["events"][2] == {
        "slot": 2,
        "name": "Премия",
        "day": 25,
        "amount": 1_000_000,
        "currency": "UZS",
    }
    assert fresh["events"][1]["amount"] == -650.5
    assert fresh["goal"] == {"amount": 2_000_000, "currency": "UZS"}
    assert fresh["saved"] == 3


def test_the_other_user_is_told_and_the_author_is_not() -> None:
    application = plan_app()
    post_plan(application, change(), user=WIFE)
    [sent] = application.bot.send_message.await_args_list
    assert sent.kwargs["chat_id"] == OWNER
    lines = sent.kwargs["text"].splitlines()
    assert lines[0] == f"✏️ Иван — правки в плане на {MONTHS_RU[CURRENT.month - 1]}:"
    assert lines[1] == "☕ кофе: 1 500 000 → 1 800 000"


def test_without_a_name_the_note_still_goes() -> None:
    application = plan_app()
    application.bot.get_chat = AsyncMock(side_effect=RuntimeError("chat not found"))
    post_plan(application, change())
    [sent] = application.bot.send_message.await_args_list
    assert sent.kwargs["text"].startswith("✏️ Правки в плане на ")


def test_a_failed_note_does_not_fail_the_save() -> None:
    application = plan_app()
    application.bot.send_message = AsyncMock(side_effect=RuntimeError("blocked"))
    assert post_plan(application, change()).status_code == 200


def test_a_conflict_writes_nothing_and_returns_the_fresh_plan() -> None:
    sheets = sheets_with()
    application = plan_app(sheets)
    rows = [{"key": "кофе|UZS", "base": 1_000_000, "plan": "1800000"}]
    response = post_plan(application, change(rows=rows))
    assert response.status_code == 409
    body = response.json()
    assert (body["error"]["code"], body["conflicts"]) == ("plan_changed", ["кофе|UZS"])
    assert body["plan"]["rows"][0]["plan"] == 1_500_000
    assert sheets.writes == []
    application.bot.send_message.assert_not_awaited()


@pytest.mark.parametrize(
    ("body", "field"),
    [
        (change(rows=[{"key": "кальян|UZS", "base": 0, "plan": "1"}]), "rows"),
        (change(rows=[{"key": "кофе|UZS", "base": 0, "plan": "-1"}]), "rows"),
        (
            change(
                rows=[],
                events=[
                    {
                        "slot": 2,
                        "base": EMPTY_CELLS,
                        "value": {
                            "name": "Премия",
                            "day": 32,
                            "amount": "1",
                            "currency": "UZS",
                        },
                    }
                ],
            ),
            "events",
        ),
        (
            change(
                rows=[],
                events=[
                    {
                        "slot": 2,
                        "base": EMPTY_CELLS,
                        "value": {
                            "name": "Премия",
                            "day": 5,
                            "amount": "0",
                            "currency": "UZS",
                        },
                    }
                ],
            ),
            "events",
        ),
        (
            change(
                rows=[],
                goal={
                    "base": {"amount": 1_000_000, "currency": "UZS"},
                    "value": {"amount": "5", "currency": "EUR"},
                },
            ),
            "goal",
        ),
        (change(rows=[], new_rows=[{**CAFE, "subcategory": "суши"}]), "new_rows"),
        (
            change(
                rows=[],
                new_rows=[{**CAFE, "group": "💳 СЧЕТА", "subcategory": "переводы"}],
            ),
            "new_rows",
        ),
        (change(rows=[], new_rows=[{**CAFE, "currency": "EUR"}]), "new_rows"),
        (change(rows=[], new_rows=[{**CAFE, "plan": "-1"}]), "new_rows"),
        (change(rows=[], new_rows=[CAFE] * 11), "new_rows"),
        (change(rows=[]), "changes"),
        (change(month="1999-01"), "month"),
    ],
)
def test_bad_saves_name_the_field(body: dict[str, Any], field: str) -> None:
    sheets = sheets_with()
    response = post_plan(plan_app(sheets), body)
    assert response.status_code == 422
    assert list(response.json()["error"]["fields"]) == [field]
    assert sheets.writes == []


def test_save_sheet_failure_is_503() -> None:
    class Broken(PlanSheets):
        def write_cells(self, data: list[dict[str, Any]]) -> None:
            raise ConnectionError("Google is down")

    response = post_plan(plan_app(Broken({TITLE: TAB})), change())
    assert (response.status_code, response.json()["error"]["code"]) == (
        503,
        "sheets_unavailable",
    )


async def test_two_saves_at_once_cannot_both_win() -> None:
    sheets = sheets_with()
    sheets.delay = 0.05  # без замка оба прочтут вкладку до первой записи
    transport = httpx.ASGITransport(app=create_app(plan_app(sheets), "s"))
    other = [{"key": "кофе|UZS", "base": 1_500_000, "plan": "1700000"}]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        answers = await asyncio.gather(
            client.post("/api/plan", json=change(), headers=auth()),
            client.post("/api/plan", json=change(rows=other), headers=auth(WIFE)),
        )
    assert sorted(a.status_code for a in answers) == [200, 409]
    assert len(sheets.writes) == 1


class NextSheets(PlanSheets):
    """Таблица, где «Создать план» копирует вкладку текущего месяца."""

    def __init__(self) -> None:
        base = sheets_with()
        super().__init__({TITLE: base.tabs[TITLE]}, base.tabs["fact"])
        self.copies = 0
        self.current = MonthTab(TITLE)  # что сделал ensure_month_tab с текущим месяцем

    def ensure_month_tab(self, today: Any) -> MonthTab:
        return self.current

    def ensure_next_month_tab(self, today: Any) -> MonthTab:
        title = month_title(NEXT)
        if title in self.tabs:
            return MonthTab(title)
        self.tabs[title] = [list(row) for row in self.tabs[TITLE]]
        self.copies += 1
        return MonthTab(title, TITLE, created=True)


def create_next(application: Any, user: int = OWNER) -> Any:
    return client_for(application).post("/api/plan/next", headers=auth(user))


def test_the_next_month_plan_is_a_copy_of_this_one() -> None:
    sheets = NextSheets()
    body = create_next(plan_app(sheets)).json()
    assert (body["status"], body["month"], body["title"]) == (
        "ok",
        f"{NEXT:%Y-%m}",
        month_title(NEXT),
    )
    assert body["rows"][0]["plan"] == 1_500_000
    assert sheets.copies == 1


def test_creating_twice_makes_one_tab() -> None:
    sheets = NextSheets()
    application = plan_app(sheets)
    assert create_next(application).status_code == 200
    assert create_next(application, WIFE).status_code == 200
    assert sheets.copies == 1


def test_the_new_month_can_be_saved() -> None:
    sheets = NextSheets()
    application = plan_app(sheets)
    create_next(application)
    response = post_plan(application, change(month=f"{NEXT:%Y-%m}"))
    assert response.status_code == 200
    assert sheets.writes[0][0]["range"] == f"'{month_title(NEXT)}'!D2"


def test_next_month_failure_is_503() -> None:
    class Broken(NextSheets):
        def ensure_next_month_tab(self, today: Any) -> MonthTab:
            raise ConnectionError("Google is down")

    response = create_next(plan_app(Broken()))
    assert (response.status_code, response.json()["error"]["code"]) == (
        503,
        "sheets_unavailable",
    )


def test_next_month_needs_a_signature() -> None:
    client = client_for(plan_app(NextSheets()))
    assert client.post("/api/plan/next").status_code == 401


def test_the_owner_hears_of_a_month_tab_the_button_made(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "ANALYTICS_CHAT_ID", None)
    monkeypatch.setattr(config, "OWNER_ID", OWNER)
    sheets = NextSheets()
    sheets.current = MonthTab(TITLE, "Sep 26", created=True, hidden=["Sep 26"])
    application = plan_app(sheets)
    assert create_next(application, user=WIFE).status_code == 200
    chat_id, text = application.bot.send_message.await_args.args
    assert chat_id == OWNER
    assert text.startswith(f"📅 Новый месяц: создал вкладку «{TITLE}»")


def test_creating_the_next_month_needs_an_allowed_user() -> None:
    assert create_next(plan_app(NextSheets()), user=777).status_code == 403
