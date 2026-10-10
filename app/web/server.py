"""Веб-сервис бота: вебхук Telegram, проверка Render, API и страница Mini App.

Telegram присылает обновления на POST /telegram с секретом в заголовке; сервер
кладёт их в очередь PTB, дальше работают обычные обработчики бота. Схема — из
примера PTB customwebhookbot (Starlette).
"""

import hmac
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from starlette.applications import Starlette
from starlette.datastructures import MutableHeaders
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from telegram import Update
from telegram.ext import Application

logger = logging.getLogger(__name__)

SECRET_HEADER = "X-Telegram-Bot-Api-Secret-Token"
SECURITY_HEADERS = {
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
}

Lifespan = Callable[[Starlette], AbstractAsyncContextManager[None]]


class SecurityHeaders:
    """Заголовки безопасности на всех ответах, включая статику."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for name, value in SECURITY_HEADERS.items():
                    headers.setdefault(name, value)
            await send(message)

        await self.app(scope, receive, send_with_headers)


def telegram_lifespan(
    application: Application,
    webhook_url: str,
    secret: str,
    post_init: Callable[[Application], Awaitable[None]],
) -> Lifespan:
    """PTB вместе с сервером: инициализация, post_init, вебхук, старт и остановка."""

    @asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        async with application:
            # без run_polling/run_webhook PTB сам post_init не вызывает
            await post_init(application)
            await application.bot.set_webhook(
                webhook_url, secret_token=secret, allowed_updates=Update.ALL_TYPES
            )
            await application.start()
            logger.info("Webhook is set, the bot is running")
            try:
                yield
            finally:
                await application.stop()

    return lifespan


def create_app(
    application: Application, secret: str, lifespan: Lifespan | None = None
) -> Starlette:
    """Маршруты сервиса; lifespan=None — без запуска PTB (для тестов)."""

    async def telegram_webhook(request: Request) -> Response:
        received = request.headers.get(SECRET_HEADER, "")
        if not hmac.compare_digest(received.encode(), secret.encode()):
            return PlainTextResponse("forbidden", status_code=403)
        try:
            data: Any = await request.json()
        except ValueError:
            return PlainTextResponse("bad request", status_code=400)
        if not isinstance(data, dict):
            return PlainTextResponse("bad request", status_code=400)
        await application.update_queue.put(Update.de_json(data, application.bot))
        return Response(status_code=200)

    async def health(_: Request) -> Response:
        return PlainTextResponse("ok")

    routes = [
        Route("/telegram", telegram_webhook, methods=["POST"]),
        Route("/health", health),
    ]
    return Starlette(
        routes=routes, middleware=[Middleware(SecurityHeaders)], lifespan=lifespan
    )
