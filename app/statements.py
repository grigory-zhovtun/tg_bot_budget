"""Импорт PDF-выписок Капиталбанка («История операций») без AI.

Текст страниц (pypdf, режим layout) разбирается регулярками: у каждой операции
дата проведения, сумма со знаком и детали. Дальше:
1. операции, проведённые не позже отметки «выписка по» (лист system, колонка G
   рядом с картой), загружены раньше и пропускаются;
2. переводы и конвертации между своими картами сводятся в пары;
3. категория — по правилам ниже и по тому, как владелец раньше разносил тот же
   магазин в листе fact (своего справочника магазинов в коде нет);
4. строки, которые уже есть в fact (та же карта и сумма, дата ±3 дня), не
   повторяются; суммы, которые бот когда-то пересчитал по курсу («≈»),
   уточняются по выписке; временные строки выравнивания удаляются.
"""

import io
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from pypdf import PdfReader

from app.domain import (
    FALLBACK_CATEGORY,
    FALLBACK_SUBCATEGORY,
    INCOME_GROUP,
    Catalog,
    SheetRow,
    currency_of,
    merchant_key,
)

logger = logging.getLogger(__name__)

# --- правила владельца бюджета (без имён и номеров счетов) -------------------
ANON_COFFEE_LIMIT = 60_000  # покупка без мерчанта до этой суммы — кофе
SALARY_USD_TERMINALS = ("TM0059",)  # входящие USD через этот терминал — зарплата
POCKET_MONEY_TERMINAL = "TM0053"  # переводы VISA→VISA KB через него — карманные
OWN_FEE_TERMINAL = "TM0105"  # перевод на свою UZCARD с комиссией 0,5 %
OWN_FEE_RATE = 0.005
FX_OUT_TERMINAL, FX_IN_TERMINAL = "TM0092", "TM0093"  # обмен сумов на доллары
SBER_DEVICE = "3664002Y"  # зачисления на HUMO со Сбербанка («Перевод за рубеж»)
RENT_CASH_USD = 650  # наличные USD в кассе банка — квартплата
WINDOW_DAYS = 3  # банк проводит покупку через 1–3 дня
NEAR_SHARE = 0.07  # пересчёт по курсу таблицы расходится с банком на 1–5 %
TEMPORARY_MARK = "ВРЕМЕННАЯ"  # строка выравнивания до следующей выписки

TRANSFERS = ("💳 СЧЕТА", "переводы")
FX = ("💳 СЧЕТА", "обмен валюты")
MISC = (FALLBACK_CATEGORY, FALLBACK_SUBCATEGORY)
SALARY = (INCOME_GROUP, "зарплата")
BONUS = (INCOME_GROUP, "премия")
RENT = ("🏚️ ДОМ", "квартплата")
COFFEE = ("🍔 ЕДА", "кофе")
FOOD = ("🏚️ ДОМ", "продукты")
POCKET = ("👶 ДЕТИ", "карманные")

SERIAL_ZERO = date(1899, 12, 30)  # нулевой день серийных дат Google Sheets

# --- разбор текста ------------------------------------------------------------
ANCHOR = re.compile(
    r"^\s{0,8}(\d\d\.\d\d\.\d{4})\s+(-?\d{1,3}(?: \d{3})*,\d\d)(?:\s{2,}(.*))?$"
)
NOISE = re.compile(
    r"^\s*(Управляющий директор|Онлайн проверка|АКБ «Капиталбанк»|подлинности выписки|"
    r"Мирзажонов|Дата проведения|Сумма\s+Детали операции|операции$|Дата и время генерации|"
    r"\d\d\.\d\d\.\d{4}г\.,|ВАЖНО!|\* В соответствии|могут отсутствовать|могут отражаться)"
)
OP_DATE = (
    re.compile(r"за\s*(\d\d\.\d\d\.\d{4})"),
    re.compile(r"дата (\d{4}-\d\d-\d\d)"),
    re.compile(r"от (\d\d\.\d\d\.\d{4})"),
)
MERCHANT = re.compile(r"покупке\s+(.*?)\s+SLIP No")
CARD_MASK = re.compile(r"\*{4,}(\d{4})")
CURRENCY = re.compile(r"Валюта:\s*([A-Z]{3})")
PERIOD = re.compile(
    r"Выписка за период:\s*с\s*(\d\d\.\d\d\.\d{4}).*?по\s*(\d\d\.\d\d\.\d{4})"
)
GENERATED = re.compile(r"генерации выписки\s+(\d\d\.\d\d\.\d{4})")


def pdf_text(data: bytes) -> str:
    """Текст всех страниц PDF с сохранением колонок; пустая строка, если не вышло."""
    try:
        reader = PdfReader(io.BytesIO(data))
        return "\n".join(
            page.extract_text(extraction_mode="layout") for page in reader.pages
        )
    except Exception:  # зашифрованный, битый или скан без текста
        logger.info("PDF has no extractable text", exc_info=True)
        return ""


def is_kapitalbank_statement(text: str) -> bool:
    return "Капиталбанк" in text and "по истории операций" in text


def parse_date(raw: str) -> date:
    if "-" in raw:
        return date.fromisoformat(raw)
    day, month, year = (int(part) for part in raw.split("."))
    return date(year, month, day)


def operation_date(details: str, posted: date) -> date:
    """Дата покупки из деталей («за 01.07.2026»), иначе дата проведения."""
    for pattern in OP_DATE:
        if match := pattern.search(details):
            return parse_date(match.group(1))
    return posted


@dataclass(frozen=True)
class Txn:
    """Операция из выписки. amount — как в выписке: списание < 0."""

    uid: str
    source: str
    posted: date
    op_date: date
    amount: float
    details: str

    @property
    def kind(self) -> str:
        """visa_uzs / visa_usd / uzcard / humo — тип карты по названию источника."""
        name = self.source.upper()
        if "HUMO" in name:
            return "humo"
        if "UZCARD" in name:
            return "uzcard"
        return "visa_usd" if currency_of(self.source) == "USD" else "visa_uzs"


@dataclass
class Statement:
    source: str
    txns: list[Txn]
    period: tuple[date, date] | None = None
    generated: date | None = None

    @property
    def covered_until(self) -> date | None:
        """Последний день, все операции которого точно попали в выписку.

        В день генерации выписки банк ещё проводит операции, этот день не полный.
        """
        ends = []
        if self.period:
            ends.append(self.period[1])
        if self.generated:
            ends.append(self.generated - timedelta(days=1))
        if not ends and self.txns:
            ends.append(max(t.posted for t in self.txns) - timedelta(days=1))
        return min(ends) if ends else None


def detect_source(text: str, sources: list[str]) -> str:
    """Источник по самым частым четырём цифрам из «427831******4058»."""
    counts: dict[str, int] = defaultdict(int)
    for digits in CARD_MASK.findall(text):
        counts[digits] += 1
    currency = CURRENCY.search(text)
    for digits in sorted(counts, key=counts.__getitem__, reverse=True):
        matches = [s for s in sources if digits in s]
        if currency:
            matches = [s for s in matches if currency_of(s) == currency.group(1)]
        if len(matches) == 1:
            return matches[0]
    raise ValueError("не нашёл карту выписки среди источников (лист system, колонка F)")


def parse_statement(text: str, sources: list[str]) -> Statement:
    """Текст выписки (все страницы) → операции, период и дата генерации."""
    if "Дата проведения" not in text:
        # номер карты есть только в строках операций: без них карту не узнать
        raise ValueError("в выписке нет операций за период")
    source = detect_source(text, sources)
    period = PERIOD.search(text)
    generated = GENERATED.search(text)
    statement = Statement(
        source,
        [],
        (parse_date(period.group(1)), parse_date(period.group(2))) if period else None,
        parse_date(generated.group(1)) if generated else None,
    )
    body = text[text.index("Дата проведения") :].replace("\f", "\n")
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in body.split("\n"):
        if not line.strip():
            if current:
                blocks.append(current)
                current = []
        elif not NOISE.match(line):
            current.append(line)
    if current:
        blocks.append(current)

    for block in blocks:
        anchors = [(i, m) for i, line in enumerate(block) if (m := ANCHOR.match(line))]
        if len(anchors) != 1:
            raise ValueError(f"не разобрал строку выписки: {' / '.join(block)[:120]}")
        index, match = anchors[0]
        parts = [line.strip() for i, line in enumerate(block) if i != index]
        if match.group(3):
            parts.insert(index, match.group(3).strip())
        details = re.sub(r"\s+", " ", " ".join(parts))
        posted = parse_date(match.group(1))
        statement.txns.append(
            Txn(
                uid=f"{source}:{len(statement.txns)}",
                source=source,
                posted=posted,
                op_date=operation_date(details, posted),
                amount=float(match.group(2).replace(" ", "").replace(",", ".")),
                details=details,
            )
        )
    return statement


def merge_statements(statements: list[Statement]) -> list[Statement]:
    """Несколько выписок одной карты → одна; одинаковые операции не задваиваются."""
    by_source: dict[str, list[Statement]] = defaultdict(list)
    for statement in statements:
        by_source[statement.source].append(statement)
    merged: list[Statement] = []
    for source, group in by_source.items():
        if len(group) == 1:
            merged.append(group[0])
            continue
        seen: set[tuple[date, float, str]] = set()
        unique = []
        for txn in sorted((t for s in group for t in s.txns), key=lambda t: t.posted):
            if (key := (txn.posted, txn.amount, txn.details)) not in seen:
                seen.add(key)
                unique.append(txn)
        txns = [
            Txn(f"{source}:{i}", source, t.posted, t.op_date, t.amount, t.details)
            for i, t in enumerate(unique)
        ]
        starts = [s.period[0] for s in group if s.period]
        ends = [end for s in group if (end := s.covered_until)]
        period = (min(starts), max(ends)) if starts and ends else None
        merged.append(Statement(source, txns, period))
    return merged


# --- пары своих переводов --------------------------------------------------------


@dataclass(frozen=True)
class Pair:
    other: Txn
    kind: str  # "own" — перевод между своими картами, "fx" — обмен валюты


def match_pairs(txns: list[Txn]) -> dict[str, Pair]:
    """Сопоставляет списания и зачисления между своими картами."""
    by_kind: dict[str, list[Txn]] = defaultdict(list)
    for txn in txns:
        by_kind[txn.kind].append(txn)
    visa, uzcard = by_kind["visa_uzs"], by_kind["uzcard"]
    humo, usd = by_kind["humo"], by_kind["visa_usd"]
    pairs: dict[str, Pair] = {}

    def free(pool: list[Txn]) -> list[Txn]:
        return [t for t in pool if t.uid not in pairs and t.amount > 0]

    def days(a: date, b: date) -> int:
        return (a - b).days

    def link(out: Txn, cands: list[Txn], kind: str, by_posted: bool = False) -> None:
        if not cands:
            return
        best = min(
            cands,
            key=lambda c: abs(days(c.posted if by_posted else c.op_date, out.op_date)),
        )
        pairs[out.uid] = Pair(best, kind)
        pairs[best.uid] = Pair(out, kind)

    for out in sorted(visa, key=lambda t: t.op_date):
        if out.amount >= 0:
            continue
        need, det = -out.amount, out.details
        if "на HUMO KB" in det:
            cands = [
                t
                for t in free(humo)
                if t.amount == need
                and "на HUMO KB" in t.details
                and abs(days(t.op_date, out.op_date)) <= 2
            ]
            link(out, cands, "own")
        elif "на UZCARD KB" in det and "VNUNK001" in det:
            cands = [
                t
                for t in free(uzcard)
                if t.amount == need
                and "то UZCARD" in t.details
                and 0 <= days(t.posted, out.op_date) <= 3
            ]
            link(out, cands, "own", by_posted=True)
        elif "на UZCARD KB" in det and OWN_FEE_TERMINAL in det:
            cands = [
                t
                for t in free(uzcard)
                if "Эквайер" in t.details
                and abs(need - t.amount * (1 + OWN_FEE_RATE)) < 1
                and 0 <= days(t.posted, out.op_date) <= 3
            ]
            link(out, cands, "own", by_posted=True)
        elif "на VISA UZS KB" in det and FX_OUT_TERMINAL in det:
            cands = [
                t
                for t in free(usd)
                if FX_IN_TERMINAL in t.details
                and abs(days(t.op_date, out.op_date)) <= 1
            ]
            link(out, cands, "fx")

    for out in sorted(humo + uzcard, key=lambda t: t.op_date):
        if out.amount >= 0 or out.uid in pairs:
            continue
        need, det = -out.amount, out.details
        if "на VISA UZS KB" in det or "то VISA UZS KB" in det:
            cands = [
                t
                for t in free(visa)
                if t.amount == need
                and re.search(r"Перевод с (HUMO|UZCARD) KB на VISA UZS", t.details)
                and abs(days(t.op_date, out.op_date)) <= 2
            ]
            link(out, cands, "own")
        elif "HUMO KB TO VISA USD KB" in det:
            cands = [
                t
                for t in free(usd)
                if "HUMO KB TO VISA USD" in t.details
                and abs(days(t.op_date, out.op_date)) <= 1
            ]
            link(out, cands, "fx")

    for out in usd:
        if out.amount < 0 and "VSMCUSD to VSMCUZSKB" in out.details:
            slip = re.search(r"SLIP No (\d+)", out.details)
            cands = [
                t
                for t in free(visa)
                if slip
                and "VSMCUSD to VSMCUZSKB" in t.details
                and f"SLIP No {slip.group(1)}" in t.details
            ]
            link(out, cands[:1], "fx")
    return pairs


# --- лист fact ------------------------------------------------------------------


@dataclass(frozen=True)
class FactRow:
    """Строка листа fact: номер строки, дата, категория, сумма D, комментарий."""

    number: int
    day: date
    category: str
    subcategory: str
    amount: float
    comment: str
    source: str

    @property
    def income(self) -> bool:
        return self.category == INCOME_GROUP

    @property
    def temporary(self) -> bool:
        return TEMPORARY_MARK in self.comment.upper()


def _number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def fact_rows(values: list[list[Any]]) -> list[FactRow]:
    """Строки fact (UNFORMATTED, начиная с первой строки листа) с датой и суммой."""
    rows: list[FactRow] = []
    for number, cells in enumerate(values, start=1):
        cells = list(cells[:8]) + [""] * (8 - len(cells[:8]))
        if not (_number(cells[0]) and _number(cells[3])):
            continue
        rows.append(
            FactRow(
                number=number,
                day=SERIAL_ZERO + timedelta(days=int(cells[0])),
                category=str(cells[1]).strip(),
                subcategory=str(cells[2]).strip(),
                amount=float(cells[3]),
                comment=str(cells[5]).strip(),
                source=str(cells[7]).strip(),
            )
        )
    return rows


# --- категории ------------------------------------------------------------------

# Организационно-правовые формы и служебные слова в названиях магазинов
LEGAL_WORDS = frozenset(
    "OOO ООО MCHJ MCHDJ QK XK IP ИП ЧП CHP YATT YTT LLC LTD PTE CO INC AJ AO SP THE".split()
)


def merchant_core(name: str) -> str:
    """«Markthof MCHJ QK» → «MARKTHOF», «Transit 3075061…» → «TRANSIT»."""
    words = re.sub(r"[^0-9A-ZА-ЯЁ]+", " ", name.upper()).split()
    return " ".join(w for w in words if w not in LEGAL_WORDS and not w.isdigit())


class MerchantBook:
    """Как владелец раньше разносил магазины в fact (свежие строки важнее)."""

    def __init__(self, rows: list[FactRow]) -> None:
        self.exact: dict[str, tuple[str, str]] = {}
        self.core: dict[str, tuple[str, str]] = {}
        self.first: dict[str, set[tuple[str, str]]] = defaultdict(set)
        for row in reversed(rows):
            key = merchant_key(row.comment)
            if not key or not row.category or not row.subcategory:
                continue
            target = (row.category, row.subcategory)
            self.exact.setdefault(key, target)
            if core := merchant_core(key):
                self.core.setdefault(core, target)
                self.first[core.split()[0]].add(target)

    def find(self, name: str) -> tuple[str, str] | None:
        key = merchant_key(name)
        if not key:
            return None
        if key in self.exact:
            return self.exact[key]
        core = merchant_core(key)
        if not core:
            return None
        if core in self.core:
            return self.core[core]
        word = core.split()[0]
        targets = self.first.get(word, set())
        # по первому слову — только если все такие магазины в одной категории
        return next(iter(targets)) if len(word) >= 4 and len(targets) == 1 else None


@dataclass(frozen=True)
class ImportRow:
    row: SheetRow
    review: str = ""  # непустая — показать владельцу перед записью
    merchant: str | None = None  # магазин, которого нет в истории


# Короткие слова, которые в fact пишутся с заглавной буквы, а не капсом
TITLE_CASE_WORDS = frozenset("OOO LLC MCHJ CHP YATT YTT IP XK".split())


def title(name: str) -> str:
    """Как магазины уже записаны в fact: «IP OOO ANGLESEY FOOD» → «Ip Ooo Anglesey Food».

    Короткие слова капсом (THE, LOT, CCK) остаются как есть.
    """
    return " ".join(
        (
            word
            if len(word) <= 3 and word.isupper() and word not in TITLE_CASE_WORDS
            else word.capitalize()
        )
        for word in name.split()
    )


def short(source: str) -> str:
    """«VISA 9120 UZS» → «VISA 9120»; у валютных карт валюта остаётся."""
    return " ".join(part for part in source.split() if part.upper() != "UZS")


def categorize(
    txns: list[Txn],
    pairs: dict[str, Pair],
    book: MerchantBook,
    catalog: Catalog,
    ai_categories: dict[str, tuple[str, str]] | None = None,
) -> list[ImportRow]:
    """Операции → строки fact по правилам таблицы (знак D: расход и доход плюс)."""
    result: list[ImportRow] = []
    ai_categories = ai_categories or {}

    def add(
        txn: Txn,
        target: tuple[str, str],
        comment: str,
        amount: float,
        review: str = "",
        merchant: str | None = None,
    ) -> None:
        group, sub = target
        if sub not in catalog.subcategories.get(group, []):
            review = review or f"нет «{group} / {sub}» в system"
            group, sub = MISC
        row = SheetRow(
            day=txn.op_date,
            category=group,
            subcategory=sub,
            amount=round(amount, 2),
            comment=f"AI: {comment}",
            currency=currency_of(txn.source),
            source=txn.source,
        )
        result.append(ImportRow(row, review, merchant))

    for txn in txns:
        det, value = txn.details, txn.amount
        flow = -value  # знак колонки D для всего, кроме доходов

        if pair := pairs.get(txn.uid):
            arrow = "→" if value < 0 else "←"
            label = "конвертация" if pair.kind == "fx" else "перевод"
            fee = -value - pair.other.amount if pair.kind == "own" and value < 0 else 0
            note = (
                f"{label} {short(txn.source)} {arrow} {short(pair.other.source)} (свой)"
            )
            add(txn, FX if pair.kind == "fx" else TRANSFERS, note, flow - fee)
            if fee > 0.009:
                add(txn, MISC, "комиссия за перевод на свою карту", fee)
            continue
        if "АКБ Капиталбанк" in det or "Оплата аванс" in det:
            found = re.search(r"(ЗП за \w+ \d{4}|аванс за \w+ \d{4}|премия[^,]*)", det)
            what = found.group(1) if found else "выплата"
            add(
                txn,
                BONUS if "премия" in det else SALARY,
                f"Капиталбанк — {what}",
                value,
            )
            continue
        if (
            txn.kind == "visa_usd"
            and value > 0
            and "Перевод с VISA USD KB на VISA USD KB" in det
            and any(term in det for term in SALARY_USD_TERMINALS)
        ):
            add(txn, SALARY, "USD за рубли из зарплаты (обмен)", value)
            continue
        if "выд нал" in det or "выд. нал" in det:
            if txn.kind == "visa_usd":
                note = f"наличные {abs(value):.0f} USD в кассе → квартплата"
                review = "" if abs(value) == RENT_CASH_USD else "квартплата?"
                add(txn, RENT, note, flow, review)
            else:
                add(txn, TRANSFERS, "снятие наличных (банкомат)", flow)
            continue
        if txn.kind == "humo" and det.startswith("Транзакция"):
            add(txn, TRANSFERS, "снятие наличных (банкомат)", flow)
            continue
        if det.startswith("Комиссия"):
            add(txn, MISC, "комиссия банка", flow)
            continue
        if "Пополнение наличными" in det:
            add(txn, TRANSFERS, "внесение наличных (банкомат)", flow)
            continue
        if txn.kind == "humo" and "Зачисление средств" in det:
            if SBER_DEVICE in det:
                add(txn, TRANSFERS, "← перевод со Сбербанка (свой)", flow)
            else:
                add(txn, TRANSFERS, "входящий перевод", flow, "откуда?")
            continue
        if value > 0:
            add(txn, TRANSFERS, "входящий перевод", flow, "откуда?")
            continue
        if "на VISA UZS KB" in det and POCKET_MONEY_TERMINAL in det:
            note = "перевод на карту VISA KB → карманные"
            add(txn, POCKET, note, flow, "карманные?")
            continue
        if "Перевод с" in det or "Списание по переводу" in det:
            dest = re.search(
                r"(?:на|то) (HUMO другого банка|UZCARD другого банка|HUMO KB|UZCARD KB|"
                r"VISA UZS KB)",
                det,
            )
            target = dest.group(1) if dest else "VISA/MC"
            add(txn, MISC, f"перевод на карту {target}", flow, "кому?")
            continue
        anonymous = (
            txn.kind == "uzcard" and ("Эмитент" in det or "QR-Online" in det)
        ) or (txn.kind == "visa_uzs" and "QRonline" in det)
        if anonymous:
            small = -value <= ANON_COFFEE_LIMIT
            note = f"без мерчанта (QR/Payme), {'≤' if small else '>'}60 тыс."
            add(txn, COFFEE if small else MISC, note, flow)
            continue
        if txn.kind == "humo" and ("Расчеты ТСП" in det or "QR-online" in det):
            add(txn, FOOD, "HUMO, ТСП др. банка без мерчанта → продукты", flow)
            continue
        if match := MERCHANT.search(det):
            name = re.sub(r"\s+", " ", match.group(1)).strip()
            if known := book.find(name):
                add(txn, known, title(name), flow)
            elif suggested := ai_categories.get(name):
                add(txn, suggested, title(name), flow, "категория от AI")
            else:
                add(txn, MISC, title(name), flow, "новый магазин", merchant=name)
            continue
        add(txn, MISC, det[:80], flow, "не разобрал")

    # Внутри дня сначала зачисления: баланс не уходит в минус посреди дня
    def is_outflow(item: ImportRow) -> bool:
        return item.row.amount >= 0 and item.row.category != INCOME_GROUP

    result.sort(key=lambda r: (r.row.day, r.row.source, is_outflow(r)))
    return result


# --- сверка с листом fact ----------------------------------------------------------


@dataclass(frozen=True)
class Adjustment:
    """Строка fact, сумму которой бот пересчитывал по курсу: берём сумму банка."""

    fact: FactRow
    amount: float


def _close(a: float, b: float) -> bool:
    return a * b > 0 and abs(a - b) <= NEAR_SHARE * max(abs(a), abs(b))


def reconcile(
    rows: list[ImportRow], facts: list[FactRow]
) -> tuple[list[ImportRow], int, list[Adjustment]]:
    """Новые строки, число уже внесённых и уточнения пересчитанных сумм.

    Уже внесённая — та же карта и сумма, дата ±3 дня. Пересчитанная — строка
    с «≈» в комментарии (бот переводил валюту по курсу таблицы) и суммой в
    пределах 7 %: её сумма заменяется банковской, вторая строка не пишется.
    """
    if not rows:
        return [], 0, []
    start = min(r.row.day for r in rows) - timedelta(days=WINDOW_DAYS)
    pool = [f for f in facts if f.day >= start and not f.temporary]

    def best(item: ImportRow, exact: bool) -> FactRow | None:
        row = item.row
        income = row.category == INCOME_GROUP
        cands = [
            f
            for f in pool
            if f.source == row.source
            and abs((f.day - row.day).days) <= WINDOW_DAYS
            and (
                abs(f.amount - row.amount) < 0.01
                if exact
                else f.income == income
                and "≈" in f.comment
                and _close(f.amount, row.amount)
            )
        ]
        return min(
            cands,
            key=lambda f: (abs(f.amount - row.amount), abs((f.day - row.day).days)),
            default=None,
        )

    unmatched: list[ImportRow] = []
    already = 0
    for item in rows:
        if hit := best(item, exact=True):
            pool.remove(hit)
            already += 1
        else:
            unmatched.append(item)

    fresh: list[ImportRow] = []
    adjustments: list[Adjustment] = []
    for item in unmatched:
        if hit := best(item, exact=False):
            pool.remove(hit)
            adjustments.append(Adjustment(hit, item.row.amount))
        else:
            fresh.append(item)
    return fresh, already, adjustments


# --- план импорта -----------------------------------------------------------------


def _fmt(day: date) -> str:
    return day.strftime("%d.%m.%Y")


@dataclass
class ImportPlan:
    """Что изменится в таблице: показывается владельцу до записи."""

    statements: list[Statement]
    rows: list[ImportRow] = field(default_factory=list)
    already: int = 0
    covered: int = 0  # операции до отметки «выписка по»
    adjustments: list[Adjustment] = field(default_factory=list)
    removals: list[FactRow] = field(default_factory=list)
    marks: dict[str, date] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def sources(self) -> list[str]:
        return sorted({s.source for s in self.statements})

    @property
    def total(self) -> int:
        return sum(len(s.txns) for s in self.statements)

    @property
    def period(self) -> tuple[date, date] | None:
        days = [d for s in self.statements if s.period for d in s.period]
        days += [t.posted for s in self.statements for t in s.txns]
        return (min(days), max(days)) if days else None

    @property
    def has_changes(self) -> bool:
        return bool(self.rows or self.adjustments or self.removals)

    def balance_effect(self) -> dict[str, float]:
        """Насколько изменится остаток каждой карты в таблице после записи."""
        effect: dict[str, float] = defaultdict(float)
        for item in self.rows:
            row = item.row
            sign = 1 if row.category == INCOME_GROUP else -1
            effect[row.source] += sign * row.amount
        for adj in self.adjustments:
            sign = 1 if adj.fact.income else -1
            effect[adj.fact.source] += sign * (adj.amount - adj.fact.amount)
        for fact in self.removals:
            sign = 1 if fact.income else -1
            effect[fact.source] -= sign * fact.amount
        return {k: round(v, 2) for k, v in effect.items() if abs(v) >= 0.005}

    def unknown_merchants(self) -> list[str]:
        return sorted({r.merchant for r in self.rows if r.merchant})

    def review(self) -> list[ImportRow]:
        return [r for r in self.rows if r.review]


def plan_import(
    statements: list[Statement],
    catalog: Catalog,
    fact_values: list[list[Any]],
    marks: dict[str, date],
    ai_categories: dict[str, tuple[str, str]] | None = None,
) -> ImportPlan:
    """Выписки (можно сразу несколько карт) → что записать в fact.

    Пары своих переводов ищутся среди всех операций, в том числе загруженных
    раньше: вторая половина перевода может прийти уже в новой выписке.
    """
    statements = merge_statements(statements)
    plan = ImportPlan(statements)
    facts = fact_rows(fact_values)
    pairs = match_pairs([t for s in statements for t in s.txns])

    fresh: list[Txn] = []
    for statement in statements:
        mark = marks.get(statement.source)
        for txn in statement.txns:
            if mark and txn.posted <= mark:
                plan.covered += 1
            else:
                fresh.append(txn)
        if mark and statement.period and statement.period[0] > mark + timedelta(days=1):
            plan.warnings.append(
                f"{statement.source}: загружено по {_fmt(mark)}, а выписка начинается "
                f"с {_fmt(statement.period[0])} — операции между ними не попадут. "
                f"Пришлите выписку с {_fmt(mark + timedelta(days=1))}"
            )
        until = statement.covered_until
        if until and (mark is None or until > mark):
            plan.marks[statement.source] = until
            plan.removals += [
                f
                for f in facts
                if f.temporary and f.source == statement.source and f.day <= until
            ]

    rows = categorize(fresh, pairs, MerchantBook(facts), catalog, ai_categories)
    plan.rows, plan.already, plan.adjustments = reconcile(rows, facts)
    return plan
