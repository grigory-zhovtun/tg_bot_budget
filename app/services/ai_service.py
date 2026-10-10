"""Gemini: разбор операций из SMS, скриншотов и файлов, финансовый анализ."""

import asyncio
import json
import logging
import re
import time
from collections.abc import AsyncIterator
from typing import Any, Literal

import httpx
from google import genai
from google.genai import errors, types
from pydantic import BaseModel, TypeAdapter, ValidationError

from app import config
from app.domain import (
    FALLBACK_CATEGORY,
    FALLBACK_SUBCATEGORY,
    INCOME_GROUP,
    Catalog,
    MerchantBook,
    local_today,
    merchant_key,
    resolve_category,
)
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

HINTS_TTL_SECONDS = 30 * 60
MAX_HINTS = 300
# Перегрузка и временные сбои Gemini: один короткий повтор, затем запасная модель.
# Перегруженная модель держала запрос до таймаута (было 120 с), и пользователь
# ждал ответа на SMS около двух минут.
OVERLOADED = frozenset({429, 500, 503, 504})  # с этими кодами — к запасной модели
TRANSIENT = (httpx.TimeoutException, httpx.ConnectError)  # SDK повторяет и их
# 429 — исчерпана квота (на бесплатном ключе 20 запросов в день на модель):
# повтор через секунду её не вернёт, только потратит время
RETRY = types.HttpRetryOptions(
    attempts=2, initial_delay=1.0, max_delay=4.0, http_status_codes=[500, 503, 504]
)
QUOTA_PAUSE_SECONDS = 15 * 60  # если API не сказал, когда вернётся квота
TEXT_TIMEOUT_MS = 30_000  # SMS и текст: обычный ответ — 2–5 с
FILE_TIMEOUT_MS = 90_000  # фото и PDF модель читает дольше
MAX_MERCHANTS = 60  # магазинов в одном запросе на раскладку
# Нам не нужны вызовы функций: без этого SDK пишет в лог предупреждение об AFC
NO_AFC = types.AutomaticFunctionCallingConfig(disable=True)


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


class CardBalance(BaseModel):
    """Остаток карты на главном экране банковского приложения."""

    card: str | None = None  # последние 4 цифры номера
    balance: float | None = None
    currency: str | None = None
    name: str | None = None


class ScreenParse(BaseModel):
    """Скриншот: остатки по картам или операции (чек, SMS, история)."""

    kind: Literal["balances", "transactions"]
    balances: list[CardBalance] = []
    transactions: list[AiTransaction] = []


SCREEN_SCHEMA = ScreenParse.model_json_schema()
SCREEN_RULES = """Image type — decide first:
- "balances": a bank app main screen or card list whose main content is
  cards with their balances. Put every card with a visible balance into
  "balances": card = last 4 digits of its number, balance = the amount as
  shown, currency = ISO code (₽ → RUB, сум → UZS, $ → USD), name = its label.
  Skip balances hidden with asterisks and products that are not cards
  (digital ruble, deposits, bonuses). Return no transactions for such a
  screen even if a short history list is visible.
- "transactions": receipts, SMS, transaction lists — fill "transactions"
  by the rules above and leave "balances" empty.
"""


class MerchantCategory(BaseModel):
    """Категория магазина из выписки (ответ Gemini)."""

    name: str
    category: str
    subcategory: str


MERCHANTS_SCHEMA = TypeAdapter(list[MerchantCategory]).json_schema()


def merchant_hints(rows: list[list[Any]], limit: int = MAX_HINTS) -> list[str]:
    """Как владелец раньше разносил магазины: «KEY → категория / подкатегория».

    Берутся последние операции (свежие первыми), по одной строке на магазин.
    Это заменяет 5000 строк истории в промпте: ~300 строк вместо ~100 тыс. токенов.
    """
    mixed = MerchantBook.from_sheet(rows).mixed
    hints: dict[str, str] = {}
    for row in reversed(rows[1:]):
        if len(hints) >= limit:
            break
        cells = [str(cell).strip() for cell in row[:6]] + [""] * (6 - len(row[:6]))
        category, subcategory, comment = cells[1], cells[2], cells[5]
        key = merchant_key(comment)
        if key and key not in mixed and category and subcategory and key not in hints:
            hints[key] = f"{category} / {subcategory}"
    return [f"{key} → {target}" for key, target in hints.items()]


def quota_pause(error: errors.APIError) -> float:
    """Через сколько секунд вернётся квота: RetryInfo.retryDelay («27209s»)."""
    body = error.details if isinstance(error.details, dict) else {}
    details = body.get("error", {}).get("details", [])
    for item in details if isinstance(details, list) else []:
        if isinstance(item, dict) and str(item.get("@type", "")).endswith("RetryInfo"):
            match = re.fullmatch(r"(\d+(?:\.\d+)?)s", str(item.get("retryDelay", "")))
            if match:
                return float(match.group(1))
    return QUOTA_PAUSE_SECONDS


def use_history(item: dict[str, Any], book: MerchantBook) -> None:
    """Известный магазин — категория владельца из fact, а не догадка модели.

    «РАЗНОЕ» в истории не считается решением владельца: там модель решает сама.
    Ищем только по имени магазина — без догадок по первому слову комментария.
    """
    known = book.find(str(item.get("comment") or ""), first_word=False)
    if known is None or known[0] == FALLBACK_CATEGORY:
        return
    # Доход и расход история местами не меняет: от категории зависит знак суммы
    if (known[0] == INCOME_GROUP) != (item.get("direction") == "income"):
        return
    if (item.get("category"), item.get("subcategory")) != known:
        logger.info("Category of %s taken from history", merchant_key(item["comment"]))
    item["category"], item["subcategory"] = known


def build_parse_prompt(
    user_input: str,
    today: str,
    categories: list[str],
    subcategories: dict[str, list[str]],
    sources: list[str],
    hints: list[str],
    extra_rules: str = "",
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
6. comment: the merchant or sender name exactly as written in the input
   (e.g. OOO SHAVI CAFE) — the owner's history above is matched by it. If the
   user added their own words, put them after a comma (SHOWPRO, струны).
   Only when the input has no name, a short description. Never empty.
7. balance: card balance only if the input says so explicitly (Ostatok,
   Остаток, Dostupno, Доступно, Balance, Qoldiq); otherwise null.
8. Screenshots and statements: every transaction row is a separate object.
9. Marketplace orders (Uzum Market, Ozon, Wildberries, AliExpress) hold different
   goods: if the input does not say what was bought, use
   "{FALLBACK_CATEGORY} / {FALLBACK_SUBCATEGORY}".
{extra_rules}
Input:
{user_input}"""


def build_merchants_prompt(
    names: list[str],
    categories: list[str],
    subcategories: dict[str, list[str]],
    hints: list[str],
) -> str:
    tree = "\n".join(
        f"- {cat}: {', '.join(subcategories.get(cat, []))}" for cat in categories
    )
    known = "\n".join(hints) or "(пока нет)"
    shops = "\n".join(f"{i}. {name}" for i, name in enumerate(names, start=1))
    return f"""You categorize shops from a bank statement for a family budget
(Tashkent, Uzbekistan; trips abroad happen). MCHJ, OOO, QK, XK, YATT, IP are
legal forms, not part of the shop type.

Categories and their subcategories — copy the strings exactly, emoji included:
{tree}

How the owner categorized shops before (newest first):
{known}

Shops:
{shops}

Return one object per shop: "name" copied exactly from the list, "category"
and "subcategory". If you cannot tell what the shop sells, or it is a
marketplace with all kinds of goods (Uzum Market, Ozon, Wildberries), use
"{FALLBACK_CATEGORY}" with its subcategory."""


def advice_prompt(numbers: str) -> str:
    """Запрос для /advice: цифры уже посчитаны, модель их объясняет."""
    return "\n".join(
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


class GeminiService:
    def __init__(self, gs_service: GoogleSheetsService) -> None:
        self.gs_service = gs_service
        self.model_name = config.GEMINI_MODEL
        self.models = [config.GEMINI_MODEL, *config.GEMINI_FALLBACK_MODELS]
        self.client = (
            genai.Client(
                api_key=config.GEMINI_API_KEY,
                http_options=types.HttpOptions(
                    retry_options=RETRY, timeout=FILE_TIMEOUT_MS
                ),
            )
            if config.GEMINI_API_KEY
            else None
        )
        self._hints: tuple[float, list[str], MerchantBook] | None = None
        # модель → time.monotonic(), до которого она без квоты (ответила 429)
        self._paused: dict[str, float] = {}
        if self.client is None:
            logger.warning("GEMINI_API_KEY not found. AI features will be disabled.")

    @property
    def enabled(self) -> bool:
        return self.client is not None

    def _history(self) -> tuple[list[str], MerchantBook]:
        """Подсказки для промпта и книга магазинов из листа fact; кэш на 30 минут."""
        now = time.monotonic()
        if self._hints and now - self._hints[0] < HINTS_TTL_SECONDS:
            return self._hints[1], self._hints[2]
        try:
            rows = self.gs_service.get_all_records(config.FACT_SHEET_NAME)
            hints, book = merchant_hints(rows), MerchantBook.from_sheet(rows)
        except Exception:
            logger.exception("Could not read merchant history, parsing without it")
            hints, book = [], MerchantBook([])
        self._hints = (now, hints, book)
        return hints, book

    def forget_history(self) -> None:
        """Сбросить кэш подсказок: владелец поправил категорию — учесть сразу."""
        self._hints = None

    def _load_hints(self) -> list[str]:
        return self._history()[0]

    def _available_models(self) -> list[str]:
        """Модели без исчерпанной квоты; если таких нет — та, что вернётся раньше."""
        now = time.monotonic()
        ready = [m for m in self.models if self._paused.get(m, 0.0) <= now]
        return ready or [min(self.models, key=lambda m: self._paused[m])]

    async def _generate(
        self,
        contents: list[Any],
        schema: dict[str, Any] | None = None,
        timeout_ms: int = TEXT_TIMEOUT_MS,
    ) -> types.GenerateContentResponse:
        """Запрос к Gemini; при перегрузке или таймауте — следующая модель списка."""
        if self.client is None:
            raise ValueError("AI сервис не настроен (нет GEMINI_API_KEY)")
        config_args: dict[str, Any] = {
            "temperature": 0,
            "automatic_function_calling": NO_AFC,
            # таймаут уходит и серверу (X-Server-Timeout): он не держит запрос дольше
            "http_options": types.HttpOptions(timeout=timeout_ms),
        }
        if schema is not None:
            config_args |= {
                "response_mime_type": "application/json",
                "response_json_schema": schema,
            }
        last_error: Exception | None = None
        for model in self._available_models():
            try:
                return await self.client.aio.models.generate_content(
                    model=model,
                    contents=contents,
                    config=types.GenerateContentConfig(**config_args),
                )
            except errors.APIError as error:
                if error.code not in OVERLOADED:
                    raise
                if error.code == 429:
                    pause = quota_pause(error)
                    self._paused[model] = time.monotonic() + pause
                    logger.warning(
                        "Gemini %s: quota exhausted, skipped for %.0f min",
                        model,
                        pause / 60,
                    )
                else:
                    logger.warning("Gemini %s unavailable (%s)", model, error.code)
                last_error = error
            except TRANSIENT as error:
                logger.warning(
                    "Gemini %s did not answer: %s", model, type(error).__name__
                )
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
        """Текст, фото (image/jpeg) или PDF (application/pdf) → список операций.

        Магазин, который уже есть в таблице, получает категорию из истории:
        подсказки в промпте модель (особенно запасная) иногда пропускает.
        """
        hints, book = await asyncio.to_thread(self._history)
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

        response = await self._generate(
            contents,
            RESPONSE_SCHEMA,
            FILE_TIMEOUT_MS if attachment else TEXT_TIMEOUT_MS,
        )
        if not response.text:
            raise ValueError("Gemini не вернул ответ (пустой ответ или фильтр)")
        data = json.loads(response.text)
        items = data if isinstance(data, list) else [data]
        logger.info(
            "Gemini %s parsed %d transaction(s)", response.model_version, len(items)
        )
        for item in items:
            if isinstance(item, dict):
                use_history(item, book)
        return items

    async def parse_screenshot(
        self,
        image: bytes,
        caption: str | None = None,
        known_categories: list[str] | None = None,
        known_sources: list[str] | None = None,
        known_subcategories: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """Фото: остатки по картам (главный экран банка) или операции — один запрос.

        Возвращает {"kind": "balances"|"transactions", "balances": [...],
        "transactions": [...]}; у операций категория известного магазина — из истории.
        """
        hints, book = await asyncio.to_thread(self._history)
        prompt = build_parse_prompt(
            user_input=caption or "Скриншот банковского приложения или фото чека",
            today=local_today(config.ANALYTICS_TIMEZONE).strftime("%d.%m.%Y"),
            categories=known_categories or [],
            subcategories=known_subcategories or {},
            sources=known_sources or [],
            hints=hints,
            extra_rules=SCREEN_RULES,
        )
        image_part = types.Part.from_bytes(data=image, mime_type="image/jpeg")
        response = await self._generate(
            [prompt, image_part], SCREEN_SCHEMA, FILE_TIMEOUT_MS
        )
        if not response.text:
            raise ValueError("Gemini не вернул ответ (пустой ответ или фильтр)")
        data = json.loads(response.text)
        screen = {
            "kind": data.get("kind", "transactions"),
            "balances": [b for b in data.get("balances") or [] if isinstance(b, dict)],
            "transactions": [
                t for t in data.get("transactions") or [] if isinstance(t, dict)
            ],
        }
        for item in screen["transactions"]:
            use_history(item, book)
        logger.info(
            "Gemini %s: screenshot with %d balance(s), %d transaction(s)",
            response.model_version,
            len(screen["balances"]),
            len(screen["transactions"]),
        )
        return screen

    async def self_check(self) -> None:
        """Ключ и модели при старте — без генерации, квоту запросов не тратит.

        На бесплатном ключе 20 запросов в день на модель: прежняя проверка
        разбором тестового SMS съедала их за несколько деплоев.
        """
        if not self.enabled:
            return
        found: list[str] = []
        for model in self.models:
            try:
                info = await self.client.aio.models.get(model=model)
                found.append(f"{model} ({info.version or info.name})")
            except errors.APIError as error:
                logger.warning(
                    "Gemini model %s: %s %s", model, error.code, error.status
                )
            except TRANSIENT as error:
                logger.warning(
                    "Gemini model %s: no answer (%s)", model, type(error).__name__
                )
            except Exception:
                logger.exception("Gemini self-check failed for %s", model)
        if found:
            logger.info("Gemini self-check OK: %s", ", ".join(found))

    async def categorize_merchants(
        self, names: list[str], catalog: Catalog
    ) -> dict[str, tuple[str, str]]:
        """Новые магазины из выписки → категория и подкатегория из справочника.

        Неуверенные ответы («РАЗНОЕ» и всё, чего нет в system) не возвращаются:
        такие строки владелец проверит сам.
        """
        names = names[:MAX_MERCHANTS]
        if not names or not self.enabled:
            return {}
        hints = await asyncio.to_thread(self._load_hints)
        prompt = build_merchants_prompt(
            names, catalog.categories, catalog.subcategories, hints[:150]
        )
        response = await self._generate([prompt], MERCHANTS_SCHEMA)
        data = json.loads(response.text or "[]")
        result: dict[str, tuple[str, str]] = {}
        for item in data if isinstance(data, list) else []:
            try:
                answer = MerchantCategory.model_validate(item)
            except ValidationError:
                continue
            if answer.name not in names:
                continue
            category, subcategory, note = resolve_category(
                answer.category, answer.subcategory, catalog
            )
            if note is None and category != FALLBACK_CATEGORY:
                result[answer.name] = (category, subcategory)
        logger.info("Gemini categorized %d of %d merchant(s)", len(result), len(names))
        return result

    async def stream_analysis(self, numbers: str) -> AsyncIterator[str]:
        """То же, что analyze_finances, но текст по мере генерации (для /advice)."""
        if self.client is None:
            raise ValueError("AI сервис не настроен (нет GEMINI_API_KEY)")
        stream = await self.client.aio.models.generate_content_stream(
            model=self._available_models()[0],
            contents=[advice_prompt(numbers)],
            config=types.GenerateContentConfig(
                temperature=0,
                automatic_function_calling=NO_AFC,
                http_options=types.HttpOptions(timeout=TEXT_TIMEOUT_MS),
            ),
        )
        text = ""
        async for chunk in stream:
            text += chunk.text or ""
            yield text

    async def analyze_finances(self, numbers: str) -> str:
        """Выводы по готовым цифрам (/advice): считает Python, модель — объясняет."""
        prompt = advice_prompt(numbers)
        response = await self._generate([prompt])
        text = response.text or "Не удалось провести анализ."
        return text if len(text) <= 4000 else text[:3900] + "..."
