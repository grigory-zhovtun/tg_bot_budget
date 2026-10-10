"""Кто открыл Mini App: подпись Telegram (initData) или ссылка запуска из клавиатуры.

initData Telegram передаёт приложению, открытому из профиля бота, по ссылке или с
ярлыка на экране телефона. Приложению из кнопки нижней клавиатуры подписи нет, поэтому
бот кладёт в URL кнопки свой токен: ID пользователя, время выдачи и HMAC-подпись.
Ключи выводятся из токена бота — смена токена отзывает все ссылки.
"""

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl

INIT_DATA_MAX_AGE = 24 * 3600  # initData старше суток не принимаем
LAUNCH_MAX_AGE = 30 * 24 * 3600  # ссылка запуска из клавиатуры живёт 30 дней
UNAUTHORIZED = "Откройте приложение заново из бота"
_LAUNCH_LABEL = b"BudgetMiniAppLaunch"
_WEBHOOK_LABEL = b"BudgetWebhook"


class AuthError(Exception):
    """Запрос без действительной подписи; code уходит в ответ API."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    @property
    def status(self) -> int:
        return 403 if self.code == "forbidden" else 401


@dataclass(frozen=True)
class WebUser:
    id: int
    first_name: str = ""


def _digest(key: bytes, message: bytes) -> bytes:
    return hmac.new(key, message, hashlib.sha256).digest()


def _same(expected: str, received: str) -> bool:
    """Сравнение без утечки по времени; байты — чтобы не-ASCII не ронял compare_digest."""
    return hmac.compare_digest(expected.encode(), received.encode())


def check_init_data(
    init_data: str, bot_token: str, now: float | None = None
) -> WebUser:
    """initData из Telegram.WebApp → пользователь (проверка по документации Mini Apps)."""
    fields = dict(parse_qsl(init_data, keep_blank_values=True))
    received = fields.pop("hash", "")
    check = "\n".join(f"{key}={value}" for key, value in sorted(fields.items()))
    secret = _digest(b"WebAppData", bot_token.encode())
    expected = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    if not received or not _same(expected, received):
        raise AuthError("unauthorized", UNAUTHORIZED)
    try:
        signed_at = int(fields.get("auth_date", ""))
        user = json.loads(fields["user"])
        user_id = int(user["id"])
        first_name = str(user.get("first_name", ""))
    except (KeyError, TypeError, ValueError, AttributeError):
        raise AuthError("unauthorized", UNAUTHORIZED) from None
    current = time.time() if now is None else now
    if current - signed_at > INIT_DATA_MAX_AGE:
        raise AuthError("unauthorized", UNAUTHORIZED)
    return WebUser(user_id, first_name)


def _launch_signature(payload: str, bot_token: str) -> str:
    key = _digest(_LAUNCH_LABEL, bot_token.encode())
    raw = _digest(key, payload.encode())
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def launch_token(user_id: int, bot_token: str, now: float | None = None) -> str:
    """Токен для URL кнопки «📱 Приложение»: «<id>.<время выдачи>.<подпись>»."""
    issued = int(time.time() if now is None else now)
    payload = f"{user_id}.{issued}"
    return f"{payload}.{_launch_signature(payload, bot_token)}"


def check_launch_token(token: str, bot_token: str, now: float | None = None) -> WebUser:
    parts = token.split(".")
    if len(parts) != 3 or not (parts[0].isdigit() and parts[1].isdigit()):
        raise AuthError("unauthorized", UNAUTHORIZED)
    user_id, issued, signature = parts
    if not _same(_launch_signature(f"{user_id}.{issued}", bot_token), signature):
        raise AuthError("unauthorized", UNAUTHORIZED)
    current = time.time() if now is None else now
    if current - int(issued) > LAUNCH_MAX_AGE:
        raise AuthError("launch_expired", "Кнопка устарела — отправьте /start")
    return WebUser(int(user_id))


def authenticate(
    header: str | None,
    bot_token: str,
    allowed: frozenset[int],
    now: float | None = None,
) -> WebUser:
    """Заголовок Authorization («tma <initData>» или «Launch <токен>») → пользователь."""
    scheme, _, value = (header or "").partition(" ")
    if scheme == "tma" and value:
        user = check_init_data(value, bot_token, now)
    elif scheme == "Launch" and value:
        user = check_launch_token(value, bot_token, now)
    else:
        raise AuthError("unauthorized", UNAUTHORIZED)
    if user.id not in allowed:
        raise AuthError("forbidden", "Нет доступа")
    return user


def webhook_secret(bot_token: str) -> str:
    """Секрет вебхука, одинаковый у всех экземпляров сервиса (64 hex-символа)."""
    return hmac.new(_WEBHOOK_LABEL, bot_token.encode(), hashlib.sha256).hexdigest()
