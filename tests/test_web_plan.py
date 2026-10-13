"""API вкладки «План»: план месяца, сохранение, конфликты, уведомления."""

from datetime import timedelta
from typing import Any

import pytest

from app import config
from app.domain import month_title
from app.services.plan import shift_month
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
