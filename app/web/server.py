"""Веб-сервис бота: вебхук Telegram, проверка Render, API и страница Mini App.

Telegram присылает обновления на POST /telegram с секретом в заголовке; сервер
кладёт их в очередь PTB, дальше работают обычные обработчики бота. Схема — из
примера PTB customwebhookbot (Starlette).
"""

import hmac
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from typing import Any

from starlette.applications import Starlette
from starlette.datastructures import MutableHeaders
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from telegram import Update
from telegram.ext import Application

from app.web import api
from app.web.auth import AuthError

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


class WebAppFiles(StaticFiles):
    """Страница Mini App: index.html без кэша, файлы сборки с хэшем в имени — на год."""

    def file_response(
        self,
        full_path: str | os.PathLike[str],
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        response = super().file_response(full_path, stat_result, scope, status_code)
        hashed = Path(full_path).parent.name == "assets"
        response.headers["Cache-Control"] = (
            "public, max-age=31536000, immutable" if hashed else "no-cache"
        )
        return response


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
    application: Application,
    secret: str,
    lifespan: Lifespan | None = None,
    webapp_dist: Path | None = None,
) -> Starlette:
    """Маршруты сервиса; lifespan=None — без запуска PTB, webapp_dist — сборка страницы."""

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

    async def auth_failed(_: Request, error: Exception) -> Response:
        if not isinstance(error, AuthError):
            raise error
        return api.error_response(error.status, error.code, error.message)

    async def unexpected(request: Request, error: Exception) -> Response:
        user_id = getattr(request.state, "user_id", None)
        logger.error(
            "Request failed for user_id=%s on %s: %s",
            user_id,
            request.url.path,
            type(error).__name__,
        )
        return api.error_response(
            500, "server_error", "Что-то пошло не так — попробуйте ещё раз"
        )

    routes = [
        Route("/telegram", telegram_webhook, methods=["POST"]),
        Route("/health", health),
        Mount("/api", routes=api.routes(application)),
    ]
    if webapp_dist is not None and webapp_dist.is_dir():
        page = WebAppFiles(directory=webapp_dist, html=True)
        routes.append(Mount("/app", app=page, name="webapp"))
    return Starlette(
        routes=routes,
        middleware=[Middleware(SecurityHeaders)],
        lifespan=lifespan,
        exception_handlers={AuthError: auth_failed, Exception: unexpected},
    )
