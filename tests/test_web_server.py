"""Веб-сервис: вебхук Telegram, проверка Render, заголовки безопасности."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient
from telegram import Bot, Update

from app.web.server import SECRET_HEADER, create_app, telegram_lifespan

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


def test_lifespan_starts_the_bot_after_the_webhook_and_stops_it() -> None:
    url = "https://budget.onrender.com/telegram"
    calls: list[object] = []

    class FakeBot:
        async def set_webhook(
            self, webhook_url: str, secret_token: str, allowed_updates: list[str]
        ) -> None:
            calls.append(("set_webhook", webhook_url, secret_token, allowed_updates))

    class FakeApplication:
        bot = FakeBot()
        update_queue: asyncio.Queue = asyncio.Queue()

        async def __aenter__(self) -> "FakeApplication":
            calls.append("initialize")
            return self

        async def __aexit__(self, *exc: object) -> None:
            calls.append("shutdown")

        async def start(self) -> None:
            calls.append("start")

        async def stop(self) -> None:
            calls.append("stop")

    async def post_init(_: object) -> None:
        calls.append("post_init")

    application = FakeApplication()
    lifespan = telegram_lifespan(application, url, SECRET, post_init)
    with TestClient(create_app(application, SECRET, lifespan)) as client:
        assert client.get("/health").status_code == 200
        assert calls == [
            "initialize",
            "post_init",  # без run_polling PTB его сам не вызывает
            ("set_webhook", url, SECRET, Update.ALL_TYPES),
            "start",
        ]
    assert calls[-2:] == ["stop", "shutdown"]


def test_mini_app_page_and_assets_have_their_cache_rules(
    application: SimpleNamespace, tmp_path: Path
) -> None:
    (tmp_path / "assets").mkdir()
    (tmp_path / "index.html").write_text('<div id="root"></div>')
    (tmp_path / "assets" / "index-abc123.js").write_text("console.log(1)")
    client = TestClient(create_app(application, SECRET, webapp_dist=tmp_path))
    page = client.get("/app/?launch=42.1.sig")
    assert page.status_code == 200 and 'id="root"' in page.text
    assert page.headers["Cache-Control"] == "no-cache"
    asset = client.get("/app/assets/index-abc123.js")
    assert asset.headers["Cache-Control"] == "public, max-age=31536000, immutable"


def test_no_page_without_a_build(application: SimpleNamespace, tmp_path: Path) -> None:
    app = create_app(application, SECRET, webapp_dist=tmp_path / "missing")
    assert TestClient(app).get("/app/").status_code == 404
