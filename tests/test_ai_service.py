"""GeminiService с подставным клиентом: промпт, схема ответа, вложения, подсказки."""

import json
from types import SimpleNamespace
from typing import Any

import pytest
from google.genai import types

from app.services import ai_service
from app.services.ai_service import (
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
