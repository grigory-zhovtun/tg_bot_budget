"""SMS → позиции листа fact: класс операции, категория, пометка проверки, пары.

Чистые функции без сети: историю магазинов, память по сумме и подсказки ИИ
передаёт обработчик чата (app/handlers/sms.py).
"""

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any

from app.domain import (
    ANONYMOUS_PAYMENT,
    FALLBACK_CATEGORY,
    INCOME_GROUP,
    MerchantBook,
    Rates,
    Skipped,
    currency_of,
    merchant_key,
    parse_number,
)
from app.sms import Kind, Sms
from app.statements import (
    ANON_COFFEE_LIMIT,
    COFFEE,
    FX,
    MISC,
    RENT,
    SERIAL_ZERO,
    TRANSFERS,
    short,
)

NEW = "🆕"  # новый магазин: категорию подсказал ИИ
CHECK = "❓"  # операция без магазина: категория временная
MEMORY_DAYS = 60
PAIR_MINUTES = 10
OWN_FEE_SHARE = 0.01  # свой перевод: списание больше зачисления не больше чем на 1 %
FX_SHARE = 0.10  # обмен: суммы по курсу таблицы расходятся не больше чем на 10 %

# «VISA MC UZS 2 UZCARD OB», «WD VISAUZS KB to HUMO» — перевод с карты на карту;
# «VSMCUZSKB to QRonlin» — оплата по QR, её узнаёт ANONYMOUS_PAYMENT
CARD_TO_CARD = re.compile(
    r"(?:WD )?(?:VISA\w*|VSMC\w*|MC|HUMO\w*|UZCARD\w*|ACCOUNT)\b.*? (?:2|to|TO) (?!QR)"
)
VIA_BANK_APP = re.compile(
    r"P2P|NA KARTU|FRB KAPITAL"
)  # переводы и платежи в приложении


class Purpose(StrEnum):
    """Класс операции: от него зависят направление, категория и пометка."""

    MERCHANT = "merchant"  # покупка у магазина
    ANONYMOUS = "anonymous"  # оплата без магазина: Payme, Click, QR
    TRANSFER = "transfer"  # перевод с карты, списание со счёта
    CASH_OUT = "cash_out"
    INCOMING = "incoming"
    CASH_IN = "cash_in"


_BY_KIND = {
    Kind.INCOMING: Purpose.INCOMING,
    Kind.CASH_IN: Purpose.CASH_IN,
    Kind.CASH_OUT: Purpose.CASH_OUT,
    Kind.OUTGOING: Purpose.TRANSFER,
}


def purpose(sms: Sms) -> Purpose:
    """Сначала вид SMS, потом описание. Отмена — класс отменённой операции."""
    if sms.kind in _BY_KIND:
        return _BY_KIND[sms.kind]
    text = sms.description
    if CARD_TO_CARD.match(text) or VIA_BANK_APP.search(text):
        # отменённый перевод с карты — деньги вернулись на карту
        return Purpose.INCOMING if sms.cancelled else Purpose.TRANSFER
    if not text or ANONYMOUS_PAYMENT.fullmatch(text):
        return Purpose.ANONYMOUS
    return Purpose.MERCHANT


def place(description: str) -> str:
    """«VISA MC UZS 2 UZCARD OB>Tashkent UZ» → «VISA MC UZS 2 UZCARD OB»."""
    return description.split(">")[0].strip()


def card_source(card: str, sources: list[str]) -> str | None:
    """Карта из листа system по четырём цифрам; нет такой или их несколько — None."""
    found = [source for source in sources if card in source.split()]
    return found[0] if len(found) == 1 else None


def _decided(target: tuple[str, str] | None) -> tuple[str, str] | None:
    """Решение владельца: не «РАЗНОЕ» и не доход — покупка доходом не бывает."""
    if target is None or target[0] in (FALLBACK_CATEGORY, INCOME_GROUP):
        return None
    return target


def _cell_day(value: Any) -> date | None:
    """Дата из fact: «14.10.2026» или серийный номер Google Sheets."""
    if isinstance(value, int | float) and not isinstance(value, bool):
        return SERIAL_ZERO + timedelta(days=int(value))
    try:
        return datetime.strptime(str(value).strip(), "%d.%m.%Y").date()
    except ValueError:
        return None


def _cell_amount(value: Any) -> float | None:
    """Сумма D со знаком: «65 000,00», «-1 000» или число."""
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    number = parse_number(value)
    if number is None:
        return None
    return -number if str(value).strip().startswith(("-", "−")) else number


def _nameless(comment: str) -> bool:
    """Строка без названия магазина: ввод из Mini App, QR, «без мерчанта»."""
    text = re.sub(r"^(AI|SMS):\s*", "", comment.strip(), flags=re.IGNORECASE)
    text = text.split(";")[0].strip()
    return (
        text.lower() in ("", "ai")
        or "без мерчанта" in text.lower()
        or bool(ANONYMOUS_PAYMENT.fullmatch(text))
    )


@dataclass(frozen=True)
class AmountMemory:
    """Категории расходов fact за 60 дней по (карта, магазин, сумма).

    Магазин "" — строки без названия: так узнаётся кофе по QR за ту же сумму.
    latest — статья самой поздней такой строки: у магазина с названием, если
    статьи расходятся, решает последнее решение владельца (утренний кофе за
    48 000 в кафе, где раньше обедали за 150 000).
    """

    targets: dict[tuple[str, str, float], frozenset[tuple[str, str]]]
    latest: dict[tuple[str, str, float], tuple[str, str]] = field(default_factory=dict)

    @classmethod
    def from_sheet(cls, rows: list[list[Any]], today: date) -> "AmountMemory":
        """Лист fact как есть (первая строка — заголовок)."""
        found: dict[tuple[str, str, float], set[tuple[str, str]]] = defaultdict(set)
        # ключ → (день, номер строки) самой поздней строки и её статья
        latest: dict[
            tuple[str, str, float], tuple[tuple[date, int], tuple[str, str]]
        ] = {}
        start = today - timedelta(days=MEMORY_DAYS)
        for number, row in enumerate(rows[1:]):
            cells = [*row[:8], *[""] * (8 - len(row[:8]))]
            day, amount = _cell_day(cells[0]), _cell_amount(cells[3])
            target = (str(cells[1]).strip(), str(cells[2]).strip())
            if day is None or amount is None or amount <= 0:
                continue
            if not start <= day <= today or not target[1] or _decided(target) is None:
                continue
            comment = str(cells[5])
            name = "" if _nameless(comment) else merchant_key(comment)
            if name is None:
                continue  # перевод, снятие и прочие служебные строки
            key = (str(cells[7]).strip(), name, round(amount, 2))
            found[key].add(target)
            if key not in latest or (day, number) > latest[key][0]:
                latest[key] = ((day, number), target)
        return cls(
            {key: frozenset(value) for key, value in found.items()},
            {key: target for key, (_, target) in latest.items()},
        )

    def find(self, source: str, name: str, amount: float) -> tuple[str, str] | None:
        """Категория: у всех таких строк одна — она; разные — у магазина с
        названием последняя, без названия (QR) — никакая: name "" — без магазина."""
        key = (source, name, round(amount, 2))
        targets = self.targets.get(key, frozenset())
        if len(targets) == 1:
            return next(iter(targets))
        return self.latest.get(key) if name and targets else None


def _merchant_target(
    sms: Sms, source: str, book: MerchantBook, memory: AmountMemory
) -> tuple[str, str] | None:
    """Категория покупки: память по сумме у этого магазина, иначе его история."""
    name = merchant_key(sms.description)
    if name and sms.currency == currency_of(source):
        if remembered := memory.find(source, name, sms.amount):
            return remembered
    return _decided(book.find(sms.description))


def new_merchants(
    operations: list[Sms], sources: list[str], book: MerchantBook, memory: AmountMemory
) -> list[str]:
    """Магазины, которых нет ни в памяти, ни в истории: о них спросим ИИ."""
    names: list[str] = []
    for sms in operations:
        source = card_source(sms.card, sources)
        if purpose(sms) != Purpose.MERCHANT or source is None:
            continue
        if _merchant_target(sms, source, book, memory) is None:
            name = place(sms.description)
            if name and name not in names:
                names.append(name)
    return names


@dataclass(frozen=True)
class SmsRow:
    """Позиция для записи: ответ «как от ИИ» для build_row и пометка проверки."""

    item: dict[str, Any]
    flag: str | None  # NEW, CHECK или None
    sms: Sms | None  # None — строка комиссии своего перевода


@dataclass(frozen=True)
class Pair:
    """Вторая сторона своего перевода или обмена в том же сообщении."""

    other: Sms
    other_source: str
    kind: str  # "own" — перевод между своими картами, "fx" — обмен валюты


def _item(
    sms: Sms,
    source: str,
    target: tuple[str, str],
    comment: str,
    direction: str,
    balance: float | None,
    amount: float | None = None,
) -> dict[str, Any]:
    return {
        "amount": sms.amount if amount is None else amount,
        "currency": sms.currency,
        "date": sms.at.strftime("%d.%m.%Y"),
        "category": target[0],
        "subcategory": target[1],
        "comment": comment,
        "source": source,
        "card_identifier": sms.card,
        "direction": direction,
        "balance": balance,
    }


def _amounts_match(debit: Sms, credit: Sms, rates: Rates) -> bool:
    if debit.currency == credit.currency:
        return credit.amount <= debit.amount <= credit.amount * (1 + OWN_FEE_SHARE)
    converted = rates.convert(debit.amount, debit.currency, credit.currency)
    return (
        converted is not None
        and abs(converted - credit.amount) <= FX_SHARE * credit.amount
    )


def _twins(cancel: Sms, purchase: Sms) -> bool:
    """Отмена и её покупка: та же карта, сумма, валюта и описание."""
    return (
        cancel.cancelled
        and not purchase.cancelled
        and (cancel.card, cancel.amount, cancel.currency, cancel.description)
        == (purchase.card, purchase.amount, purchase.currency, purchase.description)
    )


def find_pairs(
    operations: list[Sms], cards: list[str | None], rates: Rates
) -> dict[int, Pair]:
    """Списание с одной своей карты и зачисление на другую за 10 минут — пара.

    Каждая SMS — не больше чем в одной паре, ближайшее по времени зачисление первым.
    """
    debits = [
        i
        for i, sms in enumerate(operations)
        if cards[i] and purpose(sms) == Purpose.TRANSFER
    ]
    credits = [
        i
        for i, sms in enumerate(operations)
        if cards[i] and purpose(sms) == Purpose.INCOMING
    ]
    pairs: dict[int, Pair] = {}
    for i in sorted(debits, key=lambda index: operations[index].at):
        best: tuple[float, int] | None = None
        for j in credits:
            if j in pairs or cards[j] == cards[i]:
                continue
            gap = abs((operations[j].at - operations[i].at).total_seconds())
            if gap > PAIR_MINUTES * 60:
                continue
            if not _amounts_match(operations[i], operations[j], rates):
                continue
            if best is None or gap < best[0]:
                best = (gap, j)
        if best is None:
            continue
        j = best[1]
        debit_card, credit_card = cards[i] or "", cards[j] or ""
        kind = "fx" if currency_of(debit_card) != currency_of(credit_card) else "own"
        pairs[i] = Pair(operations[j], credit_card, kind)
        pairs[j] = Pair(operations[i], debit_card, kind)
    return pairs


def _pair_rows(
    sms: Sms, source: str, pair: Pair, balance: float | None
) -> list[SmsRow]:
    """Строки своего перевода или обмена — как в импорте выписок."""
    label = "конвертация" if pair.kind == "fx" else "перевод"
    arrow = "←" if sms.incoming else "→"
    comment = f"{label} {short(source)} {arrow} {short(pair.other_source)} (свой)"
    target = FX if pair.kind == "fx" else TRANSFERS
    direction = "transfer_in" if sms.incoming else "transfer_out"
    fee = 0.0
    if pair.kind == "own" and not sms.incoming:
        fee = round(sms.amount - pair.other.amount, 2)
    if fee < 0.01:
        return [
            SmsRow(_item(sms, source, target, comment, direction, balance), None, sms)
        ]
    sent = _item(sms, source, target, comment, direction, balance, pair.other.amount)
    charge = _item(
        sms, source, MISC, "комиссия за перевод на свою карту", "expense", None, fee
    )
    return [SmsRow(sent, None, sms), SmsRow(charge, None, None)]


def _decide(
    sms: Sms,
    source: str,
    book: MerchantBook,
    memory: AmountMemory,
    suggestions: dict[str, tuple[str, str]],
    rates: Rates,
    rent_cash_usd: float | None,
) -> tuple[tuple[str, str], str | None, str, str]:
    """Категория, пометка, комментарий и направление одной SMS."""
    kind = purpose(sms)
    where = place(sms.description)
    if kind == Purpose.INCOMING:
        what = where or "пополнение счёта"
        return TRANSFERS, CHECK, f"входящий перевод: {what}", "transfer_in"
    if kind == Purpose.CASH_IN:
        return TRANSFERS, CHECK, f"внесение наличных: {where}", "transfer_in"
    if kind == Purpose.CASH_OUT:
        comment = f"снятие наличных: {where}"
        if sms.currency == "USD":  # доллары в кассе банка — квартплата
            rent = rent_cash_usd is not None and sms.amount == rent_cash_usd
            flag = None if rent else CHECK
            return RENT, flag, comment, "transfer_out"
        return MISC, CHECK, comment, "transfer_out"
    if kind == Purpose.TRANSFER:
        what = where or "списание со счёта"
        return MISC, CHECK, f"перевод: {what}", "transfer_out"
    direction = "refund" if sms.cancelled else "expense"
    if kind == Purpose.ANONYMOUS:
        comment = sms.description or "без мерчанта"
        if sms.currency == currency_of(source):
            if remembered := memory.find(source, "", sms.amount):
                return remembered, None, comment, direction
        in_sums = rates.convert(sms.amount, sms.currency, "UZS")
        small = in_sums is not None and in_sums <= ANON_COFFEE_LIMIT
        return (COFFEE if small else MISC), CHECK, comment, direction
    if known := _merchant_target(sms, source, book, memory):
        return known, None, sms.description, direction
    suggested = _decided(suggestions.get(where))
    return (suggested or MISC), NEW, sms.description, direction


def sms_rows(
    operations: list[Sms],
    sources: list[str],
    book: MerchantBook,
    memory: AmountMemory,
    suggestions: dict[str, tuple[str, str]],
    rates: Rates,
    *,
    rent_cash_usd: float | None = None,
) -> tuple[list[SmsRow], list[Skipped]]:
    """SMS сообщения → позиции в порядке времени и то, что записать нельзя.

    Доллары наличными в кассе банка — квартплата; ровно rent_cash_usd — без пометки.

    Вставку из приложения «сверху новые» сначала разворачиваем: SMS одной минуты
    остаются в порядке банка. Остаток в «Проверку» — только у самой поздней SMS карты.
    Если все SMS одной минуты, порядок вставки неизвестен: отмена идёт после своей
    покупки, а остаток пишется, только когда последняя SMS карты известна точно.
    """
    ops = list(operations)
    if len(ops) > 1 and ops[0].at > ops[-1].at:
        ops.reverse()
    unordered = len(ops) > 1 and len({sms.at for sms in ops}) == 1
    if unordered:
        ops.sort(key=lambda sms: sms.cancelled)  # отмена — после своей покупки
    order = sorted(range(len(ops)), key=lambda index: ops[index].at)
    cards = [card_source(sms.card, sources) for sms in ops]
    latest = {cards[i]: i for i in order if cards[i]}
    if unordered:
        for card in set(filter(None, cards)):
            mine = [ops[i] for i in order if cards[i] == card]
            if len(mine) > 1 and not (len(mine) == 2 and _twins(mine[1], mine[0])):
                del latest[card]  # кто из них новее — неизвестно, остаток не пишем
    pairs = find_pairs(ops, cards, rates)
    rows: list[SmsRow] = []
    skipped: list[Skipped] = []
    for i in order:
        sms, source = ops[i], cards[i]
        if source is None:
            what = place(sms.description) or (
                "пополнение счёта" if sms.incoming else "списание со счёта"
            )
            skipped.append(
                Skipped(f"карта *{sms.card} не найдена в листе system", what)
            )
            continue
        own = latest.get(source) == i and sms.balance_currency == currency_of(source)
        balance = sms.balance if own else None
        if i in pairs:
            rows += _pair_rows(sms, source, pairs[i], balance)
            continue
        target, flag, comment, direction = _decide(
            sms, source, book, memory, suggestions, rates, rent_cash_usd
        )
        rows.append(
            SmsRow(_item(sms, source, target, comment, direction, balance), flag, sms)
        )
    return rows, skipped
