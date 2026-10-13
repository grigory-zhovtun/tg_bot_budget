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
from app.services.plan import shift_month
from app.web.server import create_app
from tests.test_day_budget import FACT_HEADER, fact_row
from tests.test_month_plan import TAB, PlanSheets
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
