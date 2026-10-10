"""Веб-сервис: вебхук Telegram, проверка Render, заголовки безопасности."""

import asyncio
from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient
from telegram import Bot, Update

from app.web.server import SECRET_HEADER, create_app

SECRET = "s" * 64
UPDATE = {
    "update_id": 10,
    "message": {
        "message_id": 1,
        "date": 0,
        "chat": {"id": 42, "type": "private"},
        "text": "48000 латте",
    },
}


@pytest.fixture
def application() -> SimpleNamespace:
    return SimpleNamespace(bot=Bot("123:TEST"), update_queue=asyncio.Queue())


def test_health_answers_without_the_sheet(application: SimpleNamespace) -> None:
    response = TestClient(create_app(application, SECRET)).get("/health")
    assert (response.status_code, response.text) == (200, "ok")
    assert response.headers["Referrer-Policy"] == "no-referrer"
    assert response.headers["X-Content-Type-Options"] == "nosniff"


def test_webhook_puts_the_update_into_the_queue(application: SimpleNamespace) -> None:
    client = TestClient(create_app(application, SECRET))
    response = client.post("/telegram", json=UPDATE, headers={SECRET_HEADER: SECRET})
    assert response.status_code == 200
    update = application.update_queue.get_nowait()
    assert isinstance(update, Update)
    assert (update.update_id, update.message.text) == (10, "48000 латте")


@pytest.mark.parametrize("headers", [{}, {SECRET_HEADER: "wrong"}])
def test_webhook_without_the_secret_is_refused(
    application: SimpleNamespace, headers: dict[str, str]
) -> None:
    client = TestClient(create_app(application, SECRET))
    assert client.post("/telegram", json=UPDATE, headers=headers).status_code == 403
    assert application.update_queue.empty()


@pytest.mark.parametrize("body", ["not json", "[1, 2]"])
def test_webhook_rejects_broken_bodies(application: SimpleNamespace, body: str) -> None:
    client = TestClient(create_app(application, SECRET))
    headers = {SECRET_HEADER: SECRET, "Content-Type": "application/json"}
    assert client.post("/telegram", content=body, headers=headers).status_code == 400
    assert application.update_queue.empty()
