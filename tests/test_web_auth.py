"""Подпись Mini App: initData Telegram и ссылка запуска из клавиатуры."""

import hashlib
import hmac
import json
from urllib.parse import parse_qsl, urlencode

import pytest

from app.web.auth import (
    INIT_DATA_MAX_AGE,
    LAUNCH_MAX_AGE,
    AuthError,
    authenticate,
    check_init_data,
    check_launch_token,
    launch_token,
    webhook_secret,
)

TOKEN = "123456:TEST-TOKEN"
NOW = 1_800_000_000.0


def signed_init_data(fields: dict[str, str], token: str = TOKEN) -> str:
    """initData так, как её подписывает Telegram: hash по всем полям, кроме hash."""
    check = "\n".join(f"{key}={value}" for key, value in sorted(fields.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    digest = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return urlencode({**fields, "hash": digest})


def user_fields(user_id: int = 42, signed_at: float = NOW) -> dict[str, str]:
    user = {"id": user_id, "first_name": "Тест"}
    return {
        "auth_date": str(int(signed_at)),
        "query_id": "AAH",
        "user": json.dumps(user, ensure_ascii=False),
    }


def tampered() -> str:
    """Подпись от пользователя 42, а в поле user подставлен 43."""
    fields = dict(parse_qsl(signed_init_data(user_fields())))
    fields["user"] = json.dumps({"id": 43, "first_name": "Тест"})
    return urlencode(fields)


def test_valid_init_data_gives_the_user() -> None:
    user = check_init_data(signed_init_data(user_fields()), TOKEN, NOW)
    assert (user.id, user.first_name) == (42, "Тест")


def test_signature_field_is_part_of_the_check_string() -> None:
    # Telegram добавил поле signature (Ed25519); при проверке токеном
    # из строки проверки исключается только hash
    fields = {**user_fields(), "signature": "abc-_"}
    assert check_init_data(signed_init_data(fields), TOKEN, NOW).id == 42


@pytest.mark.parametrize(
    "init_data",
    [
        tampered(),
        signed_init_data(user_fields(), token="999:OTHER"),  # чужой бот
        urlencode(user_fields()),  # без hash
        "",
        "hash=ff%C3%A9",  # не-ASCII подпись не роняет сравнение
    ],
)
def test_bad_init_data_is_rejected(init_data: str) -> None:
    with pytest.raises(AuthError) as error:
        check_init_data(init_data, TOKEN, NOW)
    assert (error.value.code, error.value.status) == ("unauthorized", 401)


def test_old_init_data_is_rejected() -> None:
    old = signed_init_data(user_fields(signed_at=NOW - INIT_DATA_MAX_AGE - 1))
    with pytest.raises(AuthError):
        check_init_data(old, TOKEN, NOW)


def test_init_data_without_user_is_rejected() -> None:
    fields = user_fields()
    del fields["user"]
    with pytest.raises(AuthError):
        check_init_data(signed_init_data(fields), TOKEN, NOW)


def test_launch_token_round_trip() -> None:
    token = launch_token(42, TOKEN, NOW)
    assert check_launch_token(token, TOKEN, NOW + 3600).id == 42


@pytest.mark.parametrize(
    "token",
    [
        launch_token(42, TOKEN, NOW).replace("42.", "43.", 1),  # чужой ID
        launch_token(42, "999:OTHER", NOW),  # подписан другим токеном бота
        "42.1800000000",
        "abc.def.ghi",
        "",
    ],
)
def test_bad_launch_token_is_rejected(token: str) -> None:
    with pytest.raises(AuthError) as error:
        check_launch_token(token, TOKEN, NOW)
    assert error.value.code == "unauthorized"


def test_expired_launch_token_asks_for_start() -> None:
    token = launch_token(42, TOKEN, NOW - LAUNCH_MAX_AGE - 1)
    with pytest.raises(AuthError) as error:
        check_launch_token(token, TOKEN, NOW)
    assert error.value.code == "launch_expired"
    assert "/start" in error.value.message


def test_authenticate_accepts_both_schemes_for_allowed_users() -> None:
    allowed = frozenset({42})
    init = authenticate(f"tma {signed_init_data(user_fields())}", TOKEN, allowed, NOW)
    launch = authenticate(f"Launch {launch_token(42, TOKEN, NOW)}", TOKEN, allowed, NOW)
    assert init.id == launch.id == 42


@pytest.mark.parametrize("header", [None, "", "Bearer x", "tma ", "Launch "])
def test_authenticate_without_credentials(header: str | None) -> None:
    with pytest.raises(AuthError) as error:
        authenticate(header, TOKEN, frozenset({42}), NOW)
    assert error.value.status == 401


def test_authenticate_rejects_users_outside_the_list() -> None:
    header = f"Launch {launch_token(7, TOKEN, NOW)}"
    with pytest.raises(AuthError) as error:
        authenticate(header, TOKEN, frozenset({42}), NOW)
    assert (error.value.code, error.value.status) == ("forbidden", 403)


def test_webhook_secret_is_stable_and_allowed_by_telegram() -> None:
    secret = webhook_secret(TOKEN)
    assert secret == webhook_secret(TOKEN) != webhook_secret("999:OTHER")
    # Telegram принимает 1–256 символов A-Z, a-z, 0-9, _ и -
    assert len(secret) == 64 and secret.isalnum()
