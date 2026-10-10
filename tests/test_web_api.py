"""API Mini App: справочник, запись траты, повторы, отмена."""

import asyncio
import time
from collections import defaultdict
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from starlette.testclient import TestClient

from app import config
from app.domain import local_today
from app.web.auth import launch_token
from app.web.server import create_app
from tests.test_messages import (
    CATEGORIES,
    SOURCES,
    SUBCATEGORIES,
    FakeSheets,
    sent_message,
)

OWNER, WIFE = 42, 43
TODAY = local_today(config.ANALYTICS_TIMEZONE)


@pytest.fixture(autouse=True)
def allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({OWNER, WIFE}))


def make_application(sheets: Any = None) -> SimpleNamespace:
    """Приложение PTB в памяти: данные бота, user_data, бот без сети."""
    bot = SimpleNamespace(
        username="budget_test_bot",
        send_message=AsyncMock(side_effect=lambda *a, **k: sent_message()),
        delete_message=AsyncMock(),
    )
    return SimpleNamespace(
        bot=bot,
        bot_data={
            "gs_service": FakeSheets() if sheets is None else sheets,
            "categories": CATEGORIES,
            "subcategories": SUBCATEGORIES,
            "sources": SOURCES,
            "icons": {"кофе": "☕"},
            "last_source": "UZCARD 5837 UZS",
        },
        user_data=defaultdict(dict),
        update_queue=asyncio.Queue(),
    )


def client_for(application: SimpleNamespace) -> TestClient:
    return TestClient(create_app(application, "secret"))


def auth(user_id: int = OWNER) -> dict[str, str]:
    return {"Authorization": f"Launch {launch_token(user_id, config.TELEGRAM_TOKEN)}"}


def expense(**changes: Any) -> dict[str, Any]:
    body = {
        "entry_id": str(uuid4()),
        "source": "VISA 9120 UZS",
        "category": "🍔 ЕДА",
        "subcategory": "кофе",
        "amount": "48000",
        "comment": "латте",
        "day": TODAY.isoformat(),
    }
    return {**body, **changes}


def test_requests_without_a_signature_get_401() -> None:
    response = client_for(make_application()).get("/api/bootstrap")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


def test_strangers_get_403() -> None:
    response = client_for(make_application()).get("/api/bootstrap", headers=auth(7))
    assert (response.status_code, response.json()["error"]["code"]) == (
        403,
        "forbidden",
    )


def test_bootstrap_has_the_catalog_and_the_users_card() -> None:
    application = make_application()
    application.user_data[OWNER]["source"] = "VISA 4058 USD"
    body = client_for(application).get("/api/bootstrap", headers=auth()).json()
    assert body["user"]["id"] == OWNER
    assert body["bot_username"] == "budget_test_bot"
    assert body["today"] == TODAY.isoformat()
    assert body["default_source"] == "VISA 4058 USD"
    assert {"name": "VISA 4058 USD", "currency": "USD"} in body["sources"]
    food = next(g for g in body["groups"] if g["name"] == "🍔 ЕДА")
    assert (food["emoji"], food["title"]) == ("🍔", "ЕДА")
    assert food["subcategories"][0] == {"name": "кофе", "icon": "☕"}


def test_bootstrap_falls_back_to_the_card_of_the_last_row() -> None:
    body = client_for(make_application()).get("/api/bootstrap", headers=auth(WIFE))
    assert body.json()["default_source"] == "UZCARD 5837 UZS"


def test_bootstrap_does_not_touch_the_sheet() -> None:
    application = make_application(sheets=object())  # любой вызов таблицы упал бы
    response = client_for(application).get("/api/bootstrap", headers=auth())
    assert response.status_code == 200


def test_expense_is_written_like_a_manual_entry() -> None:
    sheets = FakeSheets()
    application = make_application(sheets)
    response = client_for(application).post(
        "/api/expenses", json=expense(), headers=auth()
    )
    assert response.status_code == 200
    body = response.json()
    assert (body["status"], body["rows"]) == ("written", {"first": 4169, "last": 4169})
    assert body["lines"][0].startswith(
        "✅ 48 000 UZS • 🍔 ЕДА (☕ кофе) • VISA 9120 UZS"
    )
    [row] = sheets.rows
    assert row[1:4] == ["🍔 ЕДА", "кофе", 48000.0]
    assert row[5:] == ["латте", "UZS", "VISA 9120 UZS"]
    sent = application.bot.send_message.await_args_list[0].kwargs
    assert (sent["chat_id"], sent["text"]) == (OWNER, "\n".join(body["lines"]))
    assert application.user_data[OWNER]["last_write"]["first"] == 4169


def test_cents_on_a_dollar_card() -> None:
    sheets = FakeSheets()
    client_for(make_application(sheets)).post(
        "/api/expenses",
        json=expense(source="VISA 4058 USD", amount="12.50"),
        headers=auth(),
    )
    assert (sheets.rows[0][3], sheets.rows[0][6]) == (12.5, "USD")


def test_same_entry_twice_is_written_once() -> None:
    sheets = FakeSheets()
    client = client_for(make_application(sheets))
    body = expense()
    first = client.post("/api/expenses", json=body, headers=auth()).json()
    again = client.post("/api/expenses", json=body, headers=auth()).json()
    assert len(sheets.rows) == 1
    assert (again["status"], again["rows"]) == ("duplicate", first["rows"])


async def test_double_tap_while_the_first_write_runs_writes_once() -> None:
    class SlowSheets(FakeSheets):
        def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
            time.sleep(0.2)  # таблица думает, второй запрос приходит в это время
            return super().append_transactions(rows)

    sheets = SlowSheets()
    app = create_app(make_application(sheets), "secret")
    body = expense()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first, second = await asyncio.gather(
            client.post("/api/expenses", json=body, headers=auth()),
            client.post("/api/expenses", json=body, headers=auth()),
        )
    assert sorted([first.status_code, second.status_code]) == [200, 409]
    assert len(sheets.rows) == 1


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"amount": "0"}, "amount"),
        ({"amount": "-5"}, "amount"),
        ({"amount": "12.345"}, "amount"),
        ({"amount": "1000000000001"}, "amount"),
        ({"amount": "abc"}, "amount"),
        ({"comment": "x" * 201}, "comment"),
        ({"source": "MIR 0000 RUB"}, "source"),
        ({"category": "🚀 КОСМОС"}, "category"),
        ({"subcategory": "кальян"}, "subcategory"),
        ({"day": (TODAY + timedelta(days=1)).isoformat()}, "day"),
        ({"day": (TODAY - timedelta(days=32)).isoformat()}, "day"),
        ({"entry_id": "not-a-uuid"}, "entry_id"),
    ],
)
def test_bad_fields_are_named(changes: dict[str, Any], field: str) -> None:
    sheets = FakeSheets()
    response = client_for(make_application(sheets)).post(
        "/api/expenses", json=expense(**changes), headers=auth()
    )
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation"
    assert field in error["fields"]
    assert sheets.rows == []


@pytest.mark.parametrize("back", [1, 31])
def test_yesterday_and_a_month_back_are_allowed(back: int) -> None:
    sheets = FakeSheets()
    day = TODAY - timedelta(days=back)
    response = client_for(make_application(sheets)).post(
        "/api/expenses", json=expense(day=day.isoformat()), headers=auth()
    )
    assert response.status_code == 200
    assert sheets.rows[0][0] == day.strftime("%d.%m.%Y")


def test_sheet_failure_is_503_and_the_retry_writes() -> None:
    class FlakySheets(FakeSheets):
        failing = True

        def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
            if self.failing:
                self.failing = False
                raise ConnectionError("Google is down")
            return super().append_transactions(rows)

    sheets = FlakySheets()
    client = client_for(make_application(sheets))
    body = expense()
    failed = client.post("/api/expenses", json=body, headers=auth())
    assert (failed.status_code, failed.json()["error"]["code"]) == (
        503,
        "sheets_unavailable",
    )
    retried = client.post("/api/expenses", json=body, headers=auth())
    assert retried.json()["status"] == "written"
    assert len(sheets.rows) == 1


def test_undo_touches_only_the_users_own_write() -> None:
    sheets = FakeSheets()
    client = client_for(make_application(sheets))
    client.post("/api/expenses", json=expense(), headers=auth(OWNER))
    other = client.post("/api/expenses/undo", headers=auth(WIFE)).json()
    assert "Нечего отменять" in other["message"]
    assert not hasattr(sheets, "undone")
    mine = client.post("/api/expenses/undo", headers=auth(OWNER)).json()
    assert mine["message"] == "↩️ Удалил из таблицы: строка 4169."
