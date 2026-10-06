"""Правила листа fact: знак суммы, валюта, категория, источник, дата.

Колонка D (сумма) хранит знак по правилу таблицы:
- расход и перевод со своей карты — плюс;
- доход (категория «💰 ДОХОДЫ») — плюс;
- приход, который не доход (перевод на карту, возврат), — минус.
Баланс в колонке E = Σ(D доходов) − Σ(D остальных), поэтому знак важен.
"""

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, field_validator

INCOME_GROUP = "💰 ДОХОДЫ"
FALLBACK_CATEGORY = "🚧 РАЗНОЕ"
FALLBACK_SUBCATEGORY = "неучтенка"
MAX_AGE_DAYS = 400
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")  # fmt: skip
NBSP = chr(0xA0)


class Direction(StrEnum):
    """Куда двигаются деньги относительно карты-источника."""

    EXPENSE = "expense"  # покупка, оплата, комиссия
    INCOME = "income"  # зарплата, премия, доход
    TRANSFER_OUT = "transfer_out"  # перевод или снятие с карты
    TRANSFER_IN = "transfer_in"  # зачисление на карту, которое не доход
    REFUND = "refund"  # возврат покупки


INCOMING = frozenset({Direction.INCOME, Direction.TRANSFER_IN, Direction.REFUND})


def parse_number(value: object) -> float | None:
    """«1 234,56», «1,500,000.00», «-1500», «$50», 12.5 → положительное число."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return abs(float(value))
    text = re.sub(r"[^\d,.]", "", str(value))
    if "," in text and "." in text:
        # десятичный разделитель — тот, что стоит последним
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif text.count(",") > 1:
        text = text.replace(",", "")
    elif text.count(".") > 1:
        text = text.replace(".", "")
    elif text.count(",") == 1:
        head, tail = text.split(",")
        text = head + tail if len(tail) == 3 else f"{head}.{tail}"
    try:
        return abs(float(text))
    except ValueError:
        return None


class ParsedTransaction(BaseModel):
    """Операция, как её вернул AI. Поля мягко приводятся к нужным типам."""

    model_config = ConfigDict(extra="ignore")

    amount: float
    currency: str | None = None
    date: str | None = None
    category: str | None = None
    subcategory: str | None = None
    comment: str | None = None
    source: str | None = None
    direction: Direction = Direction.EXPENSE
    balance: float | None = None
    card_identifier: str | None = None

    @field_validator("amount", mode="before")
    @classmethod
    def _amount(cls, value: object) -> float:
        number = parse_number(value)
        if not number:
            raise ValueError(f"no amount in {value!r}")
        return number

    @field_validator("balance", mode="before")
    @classmethod
    def _balance(cls, value: object) -> float | None:
        return parse_number(value)

    @field_validator("currency", mode="before")
    @classmethod
    def _currency(cls, value: object) -> str | None:
        if not value:
            return None
        code = re.sub(r"[^A-Za-z]", "", str(value)).upper()
        aliases = {"SUM": "UZS", "SOM": "UZS", "RUR": "RUB", "UZB": "UZS"}
        code = aliases.get(code, code)
        return code if len(code) == 3 else None

    @field_validator("direction", mode="before")
    @classmethod
    def _direction(cls, value: object) -> Direction:
        try:
            return Direction(str(value).strip().lower())
        except ValueError:
            return Direction.EXPENSE

    @field_validator("card_identifier", mode="before")
    @classmethod
    def _card(cls, value: object) -> str | None:
        digits = re.sub(r"\D", "", str(value or ""))
        return digits[-4:] if len(digits) >= 4 else None


@dataclass(frozen=True)
class Rates:
    """Курсы к суму: {"UZS": 1.0, "USD": 11750.0, "RUB": 141.9}."""

    to_uzs: dict[str, float]

    def convert(self, amount: float, src: str, dst: str) -> float | None:
        if src == dst:
            return amount
        src_rate, dst_rate = self.to_uzs.get(src), self.to_uzs.get(dst)
        if not src_rate or not dst_rate:
            return None
        return amount * src_rate / dst_rate


@dataclass(frozen=True)
class Catalog:
    """Справочники из листа system."""

    categories: list[str]
    subcategories: dict[str, list[str]]
    sources: list[str]


@dataclass(frozen=True)
class SheetRow:
    """Готовая к записи строка fact (без формулы баланса)."""

    day: date
    category: str
    subcategory: str
    amount: float
    comment: str
    currency: str
    source: str
    balance: float | None = None
    card_identifier: str | None = None

    @property
    def date_text(self) -> str:
        return self.day.strftime("%d.%m.%Y")


@dataclass(frozen=True)
class Skipped:
    """Операция, которую нельзя записать без участия человека."""

    reason: str
    comment: str


def local_today(timezone: str) -> date:
    """Сегодняшняя дата в часовом поясе владельца (сервер живёт в UTC)."""
    return datetime.now(ZoneInfo(timezone)).date()


def month_title(day: date) -> str:
    """Вкладка план-факта месяца: «Oct 26» (английские сокращения, как в таблице)."""
    return f"{MONTHS[day.month - 1]} {day:%y}"


def parse_month_title(title: str) -> date | None:
    """«Oct 26» → 01.10.2026; другие названия листов → None."""
    match = re.fullmatch(r"([A-Z][a-z]{2}) (\d\d)", title.strip())
    if not match or match.group(1) not in MONTHS:
        return None
    return date(2000 + int(match.group(2)), MONTHS.index(match.group(1)) + 1, 1)


def month_bounds(day: date) -> tuple[date, date]:
    """Первый и последний день месяца."""
    first = day.replace(day=1)
    following = (first + timedelta(days=32)).replace(day=1)
    return first, following - timedelta(days=1)


def currency_of(source: str) -> str:
    """Валюта источника — последние три буквы названия («VISA 9120 UZS» → UZS)."""
    return source.strip()[-3:].upper()


def _letters(text: str) -> str:
    return re.sub(r"[^a-zа-яё0-9 ]", "", text.lower()).strip()


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


# Организационно-правовые формы и служебные слова в названиях магазинов
LEGAL_WORDS = frozenset(
    "OOO ООО MCHJ MCHDJ QK XK IP ИП ЧП CHP YATT YTT LLC LTD PTE CO INC AJ AO SP THE".split()
)


def merchant_core(name: str) -> str:
    """«Markthof MCHJ QK» → «MARKTHOF», «Transit 3075061…» → «TRANSIT»."""
    words = re.sub(r"[^0-9A-ZА-ЯЁ]+", " ", name.upper()).split()
    return " ".join(w for w in words if w not in LEGAL_WORDS and not w.isdigit())


class MerchantBook:
    """Как владелец раньше разносил магазины в fact (свежие строки важнее).

    entries — (категория, подкатегория, комментарий) от старых строк к новым.
    Магазин ищется по точному имени, по имени без правовой формы и по первому
    слову, если все такие магазины в одной категории.
    """

    def __init__(self, entries: Iterable[tuple[str, str, str]]) -> None:
        newest: dict[str, tuple[str, str]] = {}  # самая свежая строка магазина
        decided: dict[str, tuple[str, str]] = {}  # самая свежая не «РАЗНОЕ»
        choices: dict[str, set[tuple[str, str]]] = defaultdict(set)
        self.first: dict[str, set[tuple[str, str]]] = defaultdict(set)
        for category, subcategory, comment in reversed(list(entries)):
            key = merchant_key(comment)
            if not key or not category or not subcategory:
                continue
            target = (category, subcategory)
            core = merchant_core(key)
            keys = [key, "core:" + core] if core else [key]
            if core:
                self.first[core.split()[0]].add(target)
            for name in keys:
                newest.setdefault(name, target)
                if category != FALLBACK_CATEGORY:  # «РАЗНОЕ» — не решение владельца
                    decided.setdefault(name, target)
                    choices[name].add(target)
        best = {name: decided.get(name, target) for name, target in newest.items()}
        self.exact = {k: v for k, v in best.items() if not k.startswith("core:")}
        self.core = {k[5:]: v for k, v in best.items() if k.startswith("core:")}
        # Магазин, который владелец разносил по разным статьям (маркетплейс:
        # продукты, химия, одежда), история не решает — смотрим, что куплено
        self.mixed = {name for name, targets in choices.items() if len(targets) > 1}

    @classmethod
    def from_sheet(cls, rows: list[list[Any]]) -> "MerchantBook":
        """Лист fact как есть (первая строка — заголовок): B, C и комментарий F."""
        cells = (list(row[:6]) + [""] * (6 - len(row[:6])) for row in rows[1:])
        return cls((str(c[1]).strip(), str(c[2]).strip(), str(c[5])) for c in cells)

    def find(self, name: str, first_word: bool = True) -> tuple[str, str] | None:
        """Категория магазина из истории; first_word=False — только по имени.

        Первое слово годится для названий из выписки («Shavi Coffee» ~ «Shavi
        Cafe»), но не для свободного текста: «Оплата …» в истории — зарплата.
        """
        key = merchant_key(name)
        if not key or key in self.mixed:
            return None
        if key in self.exact:
            return self.exact[key]
        core = merchant_core(key)
        if not core or "core:" + core in self.mixed:
            return None
        if core in self.core:
            return self.core[core]
        if not first_word:
            return None
        word = core.split()[0]
        targets = self.first.get(word, set())
        # по первому слову — только если все такие магазины в одной категории
        return next(iter(targets)) if len(word) >= 4 and len(targets) == 1 else None


def resolve_source(
    raw_source: str | None,
    card_identifier: str | None,
    sources: list[str],
    default: str | None,
) -> str | None:
    """Источник из ответа AI → точное название из system.

    AI пишет «*9120», «VISA 9120» или «HUMO» — ищем по четырём цифрам карты,
    затем по совпадению названия; иначе берём выбранный пользователем источник.
    """
    if raw_source in sources:
        return raw_source
    numbers = [card_identifier] if card_identifier else []
    numbers += re.findall(r"\d{4}", raw_source or "")
    for number in numbers:
        matches = [s for s in sources if number in s]
        if len(matches) == 1:
            return matches[0]
    if raw_source:
        wanted = _letters(raw_source)
        matches = [s for s in sources if wanted and wanted in _letters(s)]
        if len(matches) == 1:
            return matches[0]
    return default


def resolve_category(
    category: str | None, subcategory: str | None, catalog: Catalog
) -> tuple[str, str, str | None]:
    """Категория и подкатегория из справочника либо «РАЗНОЕ / неучтенка».

    Третье значение — пометка для комментария, если ответ AI не подошёл.
    """
    by_name = {_letters(c): c for c in catalog.categories}
    cat = category if category in catalog.categories else None
    if cat is None and category:
        cat = by_name.get(_letters(category))
    wanted = (subcategory or "").strip().lower()

    def find(cat_name: str) -> str | None:
        subs = catalog.subcategories.get(cat_name, [])
        return next((s for s in subs if s.lower() == wanted), None)

    if cat and wanted and (sub := find(cat)):
        return cat, sub, None
    owners = [(c, s) for c in catalog.subcategories if wanted and (s := find(c))]
    if len(owners) == 1:
        return owners[0][0], owners[0][1], None
    guess = " / ".join(x for x in (category, subcategory) if x)
    note = f"AI: {guess}" if guess else None
    return FALLBACK_CATEGORY, FALLBACK_SUBCATEGORY, note


def signed_amount(amount: float, direction: Direction, category: str) -> float:
    """Знак колонки D: доход и расход — плюс, прочий приход — минус."""
    if category == INCOME_GROUP:
        return amount
    return -amount if direction in INCOMING else amount


def parse_day(value: str | None, now: date) -> date:
    """Дата операции из текста; будущее и слишком старое → сегодня."""
    if not value:
        return now
    text = value.strip()
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%y"):
        try:
            day = datetime.strptime(text, fmt).date()
        except ValueError:
            continue
        if now - timedelta(days=MAX_AGE_DAYS) <= day <= now + timedelta(days=1):
            return min(day, now)
        return now
    return now


def build_row(
    parsed: ParsedTransaction,
    catalog: Catalog,
    rates: Rates,
    now: date,
    default_source: str | None,
) -> SheetRow | Skipped:
    """Ответ AI → строка fact по правилам таблицы."""
    comment = (parsed.comment or "").strip() or "AI"
    source = resolve_source(
        parsed.source, parsed.card_identifier, catalog.sources, default_source
    )
    if source is None:
        return Skipped("не понял, с какой карты", comment)

    source_currency = currency_of(source)
    amount = parsed.amount
    notes: list[str] = []
    if parsed.currency and parsed.currency != source_currency:
        converted = rates.convert(amount, parsed.currency, source_currency)
        if converted is None:
            return Skipped(f"нет курса {parsed.currency} → {source_currency}", comment)
        notes.append(f"≈ {amount:g} {parsed.currency} по курсу таблицы")
        amount = round(converted, 2)

    category, subcategory, note = resolve_category(
        parsed.category, parsed.subcategory, catalog
    )
    if note:
        notes.append(note)
    return SheetRow(
        day=parse_day(parsed.date, now),
        category=category,
        subcategory=subcategory,
        amount=signed_amount(amount, parsed.direction, category),
        comment="; ".join([comment, *notes]),
        currency=source_currency,
        source=source,
        balance=parsed.balance,
        card_identifier=parsed.card_identifier,
    )


# Сумма в начале сообщения, за ней пробел или конец текста.
# «12.10.2026 оплата…» не сумма: после «12.10» идёт точка, а не пробел.
_MANUAL = re.compile(
    rf"^\s*([+-]?)\s*(\d{{1,3}}(?:[ {NBSP}]\d{{3}})+|\d+)(?:[.,](\d{{1,2}}))?"
    rf"(?:\s+(.*))?$",
    re.S,
)


def parse_manual_entry(text: str) -> tuple[float, bool, str] | None:
    """«5000 кофе», «5 000,50», «+20000 возврат» → (сумма, приход?, комментарий)."""
    match = _MANUAL.match(text or "")
    if not match:
        return None
    sign, whole, fraction, comment = match.groups()
    number = float(f"{re.sub(r'[^0-9]', '', whole)}.{fraction or 0}")
    if number <= 0:
        return None
    return number, sign == "+", (comment or "").strip()


def manual_row(
    text: str,
    source: str,
    category: str,
    subcategory: str,
    now: date,
) -> SheetRow | None:
    """Ручной ввод после выбора кнопок; None — текст не похож на сумму."""
    parsed = parse_manual_entry(text)
    if parsed is None:
        return None
    amount, incoming, comment = parsed
    direction = Direction.TRANSFER_IN if incoming else Direction.EXPENSE
    return SheetRow(
        day=now,
        category=category,
        subcategory=subcategory,
        amount=signed_amount(amount, direction, category),
        comment=comment,
        currency=currency_of(source),
        source=source,
    )
