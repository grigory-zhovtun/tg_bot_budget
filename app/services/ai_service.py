"""Gemini: разбор операций из SMS, скриншотов и файлов, финансовый анализ."""

import asyncio
import json
import logging
import re
import time
from typing import Any, Literal

from google import genai
from google.genai import errors, types
from pydantic import BaseModel, TypeAdapter

from app import config
from app.domain import local_today
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

HINTS_TTL_SECONDS = 30 * 60
MAX_HINTS = 300
SELF_CHECK_SMS = "Pokupka: TEST CAFE, 1000.00 UZS, 01.10.2026 12:00, karta *0000"
# Перегрузка и временные сбои Gemini: повтор с паузой, затем запасная модель
OVERLOADED = frozenset({429, 500, 503, 504})
RETRY = types.HttpRetryOptions(
    attempts=4, initial_delay=2.0, max_delay=20.0, http_status_codes=sorted(OVERLOADED)
)
TIMEOUT_MS = 120_000


class AiTransaction(BaseModel):
    """Схема ответа Gemini (structured output). Проверка значений — в app.domain."""

    amount: float
    currency: str | None = None
    date: str | None = None
    category: str | None = None
    subcategory: str | None = None
    comment: str | None = None
    source: str | None = None
    direction: Literal["expense", "income", "transfer_out", "transfer_in", "refund"]
    balance: float | None = None
    card_identifier: str | None = None


RESPONSE_SCHEMA = TypeAdapter(list[AiTransaction]).json_schema()

# Комментарии, по которым не понять магазин: служебные строки бюджета
_GENERIC = re.compile(
    r"^(без мерчанта|humo, тсп|выравнивание|перевод|конвертация|снятие|внесение|"
    r"комиссия|остаток|наличные|←|→|ai$|\?\?)",
    re.IGNORECASE,
)


def merchant_key(comment: str) -> str | None:
    """«AI: Ip Ooo Anglesey Food (Сингапур); ≈ 3 USD» → «IP OOO ANGLESEY FOOD»."""
    text = re.sub(r"^(AI|SMS):\s*", "", (comment or "").strip(), flags=re.IGNORECASE)
    if not text or _GENERIC.match(text):
        return None
    text = re.split(r"[;(,]| ≈ ", text, maxsplit=1)[0]
    text = re.sub(r"\s+", " ", text).strip().upper()[:40]
    return text if len(text) >= 3 and not text.isdigit() else None


def merchant_hints(rows: list[list[Any]], limit: int = MAX_HINTS) -> list[str]:
    """Как владелец раньше разносил магазины: «KEY → категория / подкатегория».

    Берутся последние операции (свежие первыми), по одной строке на магазин.
    Это заменяет 5000 строк истории в промпте: ~300 строк вместо ~100 тыс. токенов.
    """
    hints: dict[str, str] = {}
    for row in reversed(rows[1:]):
        if len(hints) >= limit:
            break
        cells = [str(cell).strip() for cell in row[:6]] + [""] * (6 - len(row[:6]))
        category, subcategory, comment = cells[1], cells[2], cells[5]
        key = merchant_key(comment)
        if key and category and subcategory and key not in hints:
            hints[key] = f"{category} / {subcategory}"
    return [f"{key} → {target}" for key, target in hints.items()]


def build_parse_prompt(
    user_input: str,
    today: str,
    categories: list[str],
    subcategories: dict[str, list[str]],
    sources: list[str],
    hints: list[str],
) -> str:
    tree = "\n".join(
        f"- {cat}: {', '.join(subcategories.get(cat, []))}" for cat in categories
    )
    known = "\n".join(hints) or "(пока нет)"
    return f"""You extract bank transactions for a personal budget in Google Sheets.
Today is {today} (Asia/Tashkent). Return one JSON object per transaction.

Sources — return exactly one of these names, or null if the input names no card:
{", ".join(sources)}

Categories and their subcategories — copy the strings exactly, emoji included:
{tree}

How the owner categorized merchants before (newest first). If the merchant is
listed, use the same category and subcategory:
{known}

Rules:
1. amount: positive number exactly as written; the sign goes to direction.
2. currency: ISO code of that amount as written (UZS, USD, RUB, EUR, SGD...);
   null if not stated. Never convert.
3. direction: expense = purchase/payment/fee (Xarid, Pokupka, Oplata, Spisanie);
   transfer_out = money sent to another card or cash withdrawal;
   transfer_in = money received that is not salary (popolnen, zachislenie,
   perevod na kartu, P2P received); refund = return of a purchase (vozvrat,
   otmena); income = salary, bonus, advance (ZP, zarplata, avans, premiya).
4. date: DD.MM.YYYY of the transaction itself; use {today} only if no date is
   shown.
5. source and card_identifier: the card whose number appears in the input
   (e.g. *9120); card_identifier is its last 4 digits.
6. comment: merchant, sender or a short raw description — never empty.
7. balance: card balance only if the input says so explicitly (Ostatok,
   Остаток, Dostupno, Доступно, Balance, Qoldiq); otherwise null.
8. Screenshots and statements: every transaction row is a separate object.

Input:
{user_input}"""


class GeminiService:
    def __init__(self, gs_service: GoogleSheetsService) -> None:
        self.gs_service = gs_service
        self.model_name = config.GEMINI_MODEL
        self.models = [config.GEMINI_MODEL, *config.GEMINI_FALLBACK_MODELS]
        self.client = (
            genai.Client(
                api_key=config.GEMINI_API_KEY,
                http_options=types.HttpOptions(retry_options=RETRY, timeout=TIMEOUT_MS),
            )
            if config.GEMINI_API_KEY
            else None
        )
        self._hints: tuple[float, list[str]] | None = None
        if self.client is None:
            logger.warning("GEMINI_API_KEY not found. AI features will be disabled.")

    @property
    def enabled(self) -> bool:
        return self.client is not None

    def _load_hints(self) -> list[str]:
        """Подсказки по магазинам из листа fact; кэш на 30 минут."""
        now = time.monotonic()
        if self._hints and now - self._hints[0] < HINTS_TTL_SECONDS:
            return self._hints[1]
        try:
            rows = self.gs_service.get_all_records(config.FACT_SHEET_NAME)
            hints = merchant_hints(rows)
        except Exception:
            logger.exception("Could not build merchant hints, parsing without them")
            hints = []
        self._hints = (now, hints)
        return hints

    async def _generate(
        self, contents: list[Any], schema: dict[str, Any] | None = None
    ) -> types.GenerateContentResponse:
        if self.client is None:
            raise ValueError("AI сервис не настроен (нет GEMINI_API_KEY)")
        config_args: dict[str, Any] = {"temperature": 0}
        if schema is not None:
            config_args |= {
                "response_mime_type": "application/json",
                "response_json_schema": schema,
            }
        last_error: errors.APIError | None = None
        for model in self.models:
            try:
                return await self.client.aio.models.generate_content(
                    model=model,
                    contents=contents,
                    config=types.GenerateContentConfig(**config_args),
                )
            except errors.APIError as error:
                if error.code not in OVERLOADED:
                    raise
                logger.warning("Gemini %s unavailable (%s)", model, error.code)
                last_error = error
        raise last_error

    async def parse_transaction(
        self,
        user_input: str,
        attachment: bytes | None = None,
        mime_type: str | None = None,
        known_categories: list[str] | None = None,
        known_sources: list[str] | None = None,
        known_subcategories: dict[str, list[str]] | None = None,
    ) -> list[dict[str, Any]]:
        """Текст, фото (image/jpeg) или PDF (application/pdf) → список операций."""
        hints = await asyncio.to_thread(self._load_hints)
        prompt = build_parse_prompt(
            user_input=user_input,
            today=local_today(config.ANALYTICS_TIMEZONE).strftime("%d.%m.%Y"),
            categories=known_categories or [],
            subcategories=known_subcategories or {},
            sources=known_sources or [],
            hints=hints,
        )
        contents: list[Any] = [prompt]
        if attachment:
            contents.append(
                types.Part.from_bytes(
                    data=attachment, mime_type=mime_type or "image/jpeg"
                )
            )

        response = await self._generate(contents, RESPONSE_SCHEMA)
        if not response.text:
            raise ValueError("Gemini не вернул ответ (пустой ответ или фильтр)")
        data = json.loads(response.text)
        items = data if isinstance(data, list) else [data]
        logger.info(
            "Gemini %s parsed %d transaction(s)", response.model_version, len(items)
        )
        return items

    async def self_check(self) -> None:
        """Проверка ключа, модели и схемы при старте: результат — в лог."""
        if not self.enabled:
            return
        try:
            prompt = build_parse_prompt(SELF_CHECK_SMS, "01.10.2026", [], {}, [], [])
            response = await self._generate([prompt], RESPONSE_SCHEMA)
            items = json.loads(response.text or "[]")
            logger.info(
                "Gemini self-check OK: model %s, %d item(s)",
                response.model_version,
                len(items),
            )
        except errors.APIError as error:
            if error.code in OVERLOADED:
                logger.warning(
                    "Gemini self-check: overloaded (%s), will retry", error.code
                )
            else:
                logger.exception("Gemini self-check failed")
        except Exception:
            logger.exception("Gemini self-check failed")

    async def analyze_finances(self, numbers: str) -> str:
        """Выводы по готовым цифрам (/advice): считает Python, модель — объясняет."""
        prompt = "\n".join(
            [
                "You are a strict and concise financial analyst for a family budget.",
                "All numbers below are already calculated; do not recalculate them.",
                "",
                numbers,
                "",
                "Write in simple Russian, max 250 words, with sections:",
                "'📊 Анализ' — how this month compares with the 3-month average;",
                "'⚠️ Перерасход' — plan lines where fact exceeds plan or that are"
                " outside the plan, biggest first;",
                "'🔮 Прогноз' — the month forecast vs the average month;",
                "'💡 Совет' — 2-3 concrete steps for the rest of the month.",
                "Use the category names as given. No intro or outro.",
            ]
        )
        response = await self._generate([prompt])
        text = response.text or "Не удалось провести анализ."
        return text if len(text) <= 4000 else text[:3900] + "..."
