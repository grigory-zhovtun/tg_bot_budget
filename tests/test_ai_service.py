"""GeminiService с подставным клиентом: промпт, схема ответа, вложения, подсказки."""

import json
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from google.genai import errors, types

from app.domain import Catalog
from app.services import ai_service
from app.services.ai_service import (
    MERCHANTS_SCHEMA,
    RESPONSE_SCHEMA,
    GeminiService,
    build_parse_prompt,
    merchant_hints,
    merchant_key,
)

HEADER = ["Дата", "Категория", "Название", "Сумма", "Баланс", "Комментарий"]


@pytest.mark.parametrize(
    ("comment", "expected"),
    [
        ("AI: Ip Ooo Anglesey Food", "IP OOO ANGLESEY FOOD"),
        ("AI: Mcdonald's (Сингапур)", "MCDONALD'S"),
        ("AI: Render.com; ≈ 26 USD по курсу таблицы", "RENDER.COM"),
        ("OOO YANDEXGO UB", "OOO YANDEXGO UB"),
        ("AI: без мерчанта (QR/Payme), ≤60 тыс.", None),
        ("AI: выравнивание к остатку", None),
        ("AI: перевод VISA 9120 → HUMO (свой)", None),
        ("123", None),
        ("", None),
    ],
)
def test_merchant_key(comment: str, expected: str | None) -> None:
    assert merchant_key(comment) == expected


def test_merchant_hints_newest_first_one_per_merchant() -> None:
    rows = [
        HEADER,
        ["01.09.2026", "🚧 РАЗНОЕ", "неучтенка", 1, "", "AI: Ooo Beta"],
        ["02.09.2026", "🏚️ ДОМ", "продукты", 1, "", "AI: Ooo Beta"],
        ["03.09.2026", "🍔 ЕДА", "кофе", 1, "", "AI: Ooo Shavi Cafe"],
        ["04.09.2026", "🍔 ЕДА", "кофе", 1, "", "AI: без мерчанта (QR/Payme)"],
        ["05.09.2026", "", "", 1, "", "AI: пустая категория"],
    ]
    assert merchant_hints(rows) == [
        "OOO SHAVI CAFE → 🍔 ЕДА / кофе",
        "OOO BETA → 🏚️ ДОМ / продукты",
    ]
    assert len(merchant_hints(rows, limit=1)) == 1


def test_prompt_contains_catalog_hints_and_rules() -> None:
    prompt = build_parse_prompt(
        user_input="Pokupka 48000 UZS",
        today="06.10.2026",
        categories=["🍔 ЕДА"],
        subcategories={"🍔 ЕДА": ["кофе", "кафе"]},
        sources=["VISA 9120 UZS"],
        hints=["OOO SHAVI CAFE → 🍔 ЕДА / кофе"],
    )
    assert "- 🍔 ЕДА: кофе, кафе" in prompt
    assert "VISA 9120 UZS" in prompt
    assert "OOO SHAVI CAFE → 🍔 ЕДА / кофе" in prompt
    assert "transfer_in" in prompt and "Never convert" in prompt
    assert prompt.endswith("Pokupka 48000 UZS")


def test_response_schema_is_a_list_with_direction_enum() -> None:
    assert RESPONSE_SCHEMA["type"] == "array"
    item = RESPONSE_SCHEMA["$defs"]["AiTransaction"]
    assert item["properties"]["direction"]["enum"] == [
        "expense",
        "income",
        "transfer_out",
        "transfer_in",
        "refund",
    ]
    assert set(item["required"]) == {"amount", "direction"}


class FakeModels:
    def __init__(self, text: str | None) -> None:
        self.text = text
        self.calls: list[dict[str, Any]] = []

    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs)
        return SimpleNamespace(text=self.text, model_version="gemini-test")


class FakeSheets:
    def __init__(self) -> None:
        self.reads = 0

    def get_all_records(self, name: str) -> list[list[Any]]:
        self.reads += 1
        return [HEADER, ["01.09.2026", "🍔 ЕДА", "кофе", 1, "", "AI: Shavi"]]


def make_service(text: str | None) -> tuple[GeminiService, FakeModels, FakeSheets]:
    sheets = FakeSheets()
    service = GeminiService.__new__(GeminiService)
    models = FakeModels(text)
    service.gs_service = sheets
    service.model_name = "gemini-flash-latest"
    service.models = ["gemini-flash-latest", "gemini-flash-lite-latest"]
    service.client = SimpleNamespace(aio=SimpleNamespace(models=models))
    service._hints = None
    return service, models, sheets


async def test_parse_sends_schema_and_attachment_and_returns_list() -> None:
    answer = [{"amount": 48000, "direction": "expense", "comment": "Shavi"}]
    service, models, _ = make_service(json.dumps(answer))

    items = await service.parse_transaction(
        "фото чека", attachment=b"\xff\xd8jpeg", mime_type="image/jpeg"
    )

    assert items == answer
    [call] = models.calls
    assert call["model"] == "gemini-flash-latest"
    config: types.GenerateContentConfig = call["config"]
    assert config.response_mime_type == "application/json"
    assert config.response_json_schema == RESPONSE_SCHEMA
    assert config.temperature == 0
    prompt, part = call["contents"]
    assert "SHAVI → 🍔 ЕДА / кофе" in prompt
    assert part.inline_data.mime_type == "image/jpeg"
    assert part.inline_data.data == b"\xff\xd8jpeg"


async def test_single_object_answer_is_wrapped_in_list() -> None:
    service, _, _ = make_service(json.dumps({"amount": 1, "direction": "income"}))
    assert await service.parse_transaction("ЗП") == [
        {"amount": 1, "direction": "income"}
    ]


async def test_empty_answer_is_an_error() -> None:
    service, _, _ = make_service(None)
    with pytest.raises(ValueError, match="не вернул ответ"):
        await service.parse_transaction("что-то")


async def test_hints_are_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    service, _, sheets = make_service("[]")
    await service.parse_transaction("1")
    await service.parse_transaction("2")
    assert sheets.reads == 1
    monkeypatch.setattr(ai_service, "HINTS_TTL_SECONDS", -1)
    await service.parse_transaction("3")
    assert sheets.reads == 2


async def test_self_check_logs_model(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("INFO")
    service, _, _ = make_service('[{"amount": 1000, "direction": "expense"}]')
    await service.self_check()
    assert "Gemini self-check OK: model gemini-test, 1 item(s)" in caplog.text


async def test_self_check_never_raises(caplog: pytest.LogCaptureFixture) -> None:
    service, _, _ = make_service("not json")
    await service.self_check()
    assert "Gemini self-check failed" in caplog.text


async def test_without_key_parsing_is_refused() -> None:
    service, _, _ = make_service("[]")
    service.client = None
    with pytest.raises(ValueError, match="GEMINI_API_KEY"):
        await service.parse_transaction("1")


def overloaded(code: int = 503) -> errors.APIError:
    body = {"error": {"code": code, "message": "high demand", "status": "UNAVAILABLE"}}
    return (
        errors.ServerError(code, body)
        if code >= 500
        else errors.ClientError(code, body)
    )


class FlakyModels(FakeModels):
    """Первые N вызовов падают с заданной ошибкой, дальше — обычный ответ."""

    def __init__(self, failures: list[errors.APIError]) -> None:
        super().__init__("[]")
        self.failures = failures

    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs)
        if self.failures:
            raise self.failures.pop(0)
        return SimpleNamespace(text=self.text, model_version=kwargs["model"])


def with_models(models: FakeModels) -> GeminiService:
    service, _, _ = make_service("[]")
    service.client = SimpleNamespace(aio=SimpleNamespace(models=models))
    return service


async def test_overloaded_model_falls_back_to_the_next_one() -> None:
    models = FlakyModels([overloaded(503)])
    assert await with_models(models).parse_transaction("1") == []
    assert [call["model"] for call in models.calls] == [
        "gemini-flash-latest",
        "gemini-flash-lite-latest",
    ]


async def test_other_api_errors_are_not_retried_on_another_model() -> None:
    models = FlakyModels([overloaded(400)])
    with pytest.raises(errors.ClientError):
        await with_models(models).parse_transaction("1")
    assert len(models.calls) == 1


async def test_all_models_overloaded_raises_the_last_error() -> None:
    models = FlakyModels([overloaded(503), overloaded(429)])
    with pytest.raises(errors.ClientError) as caught:
        await with_models(models).parse_transaction("1")
    assert caught.value.code == 429


async def test_self_check_overload_is_only_a_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    models = FlakyModels([overloaded(503), overloaded(503)])
    await with_models(models).self_check()
    assert [r.levelname for r in caplog.records].count("ERROR") == 0
    assert "overloaded (503)" in caplog.text


def test_overload_message_for_the_chat() -> None:
    from app.errors import user_message

    assert user_message(overloaded(503)) == (
        "Gemini сейчас перегружен, попробуйте через минуту"
    )


CATALOG = Catalog(
    categories=["🍔 ЕДА", "🏚️ ДОМ", "🚧 РАЗНОЕ"],
    subcategories={
        "🍔 ЕДА": ["кофе", "кафе"],
        "🏚️ ДОМ": ["продукты"],
        "🚧 РАЗНОЕ": ["неучтенка"],
    },
    sources=["VISA 9120 UZS"],
)


async def test_merchants_are_categorized_in_one_call_within_the_catalog() -> None:
    answer = [
        {"name": "MARKTHOF MCHJ", "category": "ДОМ", "subcategory": "продукты"},
        {"name": "PIE POINT", "category": "🍔 ЕДА", "subcategory": "кафе"},
        {"name": "UNKNOWN SHOP", "category": "🚧 РАЗНОЕ", "subcategory": "неучтенка"},
        {"name": "ODD", "category": "🍔 ЕДА", "subcategory": "пицца"},
        {"name": "NOT ASKED", "category": "🍔 ЕДА", "subcategory": "кофе"},
        {"category": "🍔 ЕДА"},
    ]
    service, models, _ = make_service(json.dumps(answer))

    found = await service.categorize_merchants(
        ["MARKTHOF MCHJ", "PIE POINT", "UNKNOWN SHOP", "ODD"], CATALOG
    )

    assert found == {
        "MARKTHOF MCHJ": ("🏚️ ДОМ", "продукты"),
        "PIE POINT": ("🍔 ЕДА", "кафе"),
    }
    [call] = models.calls
    config: types.GenerateContentConfig = call["config"]
    assert config.response_json_schema == MERCHANTS_SCHEMA
    [prompt] = call["contents"]
    assert "1. MARKTHOF MCHJ" in prompt and "4. ODD" in prompt
    assert "- 🍔 ЕДА: кофе, кафе" in prompt
    assert "SHAVI → 🍔 ЕДА / кофе" in prompt  # подсказки из истории


async def test_no_merchants_no_call() -> None:
    service, models, _ = make_service("[]")
    assert await service.categorize_merchants([], CATALOG) == {}
    service.client = None
    assert await service.categorize_merchants(["X"], CATALOG) == {}
    assert models.calls == []


async def test_automatic_function_calling_is_off() -> None:
    service, models, _ = make_service("[]")
    await service.parse_transaction("1")
    config: types.GenerateContentConfig = models.calls[0]["config"]
    assert config.automatic_function_calling.disable is True


async def test_request_timeout_is_short_for_text_and_longer_for_files() -> None:
    service, models, _ = make_service("[]")
    await service.parse_transaction("Pokupka 48000 UZS")
    await service.parse_transaction(
        "фото", attachment=b"\xff\xd8", mime_type="image/jpeg"
    )
    timeouts = [call["config"].http_options.timeout for call in models.calls]
    assert timeouts == [ai_service.TEXT_TIMEOUT_MS, ai_service.FILE_TIMEOUT_MS]
    assert ai_service.TEXT_TIMEOUT_MS <= 30_000


async def test_timeout_falls_back_to_the_next_model() -> None:
    models = FlakyModels([httpx.ReadTimeout("no answer")])
    assert await with_models(models).parse_transaction("1") == []
    assert [call["model"] for call in models.calls] == [
        "gemini-flash-latest",
        "gemini-flash-lite-latest",
    ]


async def test_all_models_silent_gives_a_clear_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from app.errors import user_message

    models = FlakyModels([httpx.ReadTimeout("a"), httpx.ConnectError("b")])
    with pytest.raises(httpx.ConnectError):
        await with_models(models).parse_transaction("1")
    assert user_message(httpx.ReadTimeout("x")) == (
        "Gemini не ответил вовремя, попробуйте ещё раз"
    )

    models = FlakyModels([httpx.ReadTimeout("a"), httpx.ReadTimeout("b")])
    await with_models(models).self_check()
    assert [r.levelname for r in caplog.records].count("ERROR") == 0
    assert "Gemini self-check: no answer (ReadTimeout)" in caplog.text
