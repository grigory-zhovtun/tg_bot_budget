"""Банковские SMS Капиталбанка и UZCARD → операции по шаблонам, без ИИ.

Текст разбирается, только если каждая его строка целиком состоит из SMS
известных видов; иначе parse_sms возвращает None и текст разбирает ИИ, как раньше.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from app.domain import MONTHS


class Kind(StrEnum):
    """Что за операция по виду SMS — до разбора описания."""

    PAYMENT = "payment"  # покупка, оплата, operacija
    OUTGOING = "outgoing"  # перевод с карты, списание со счёта
    CASH_OUT = "cash_out"  # снятие наличных
    INCOMING = "incoming"  # зачисление на карту
    CASH_IN = "cash_in"  # внесение наличных


INCOMING_KINDS = frozenset({Kind.INCOMING, Kind.CASH_IN})

# почему SMS не пишется в таблицу
CODE = "код подтверждения"
DECLINED = "отказ банка"
WRONG_PIN = "неверный PIN"


@dataclass(frozen=True)
class Sms:
    """Одна операция из SMS так, как её написал банк."""

    text: str  # SMS целиком — по нему отпечаток повторов
    card: str  # последние 4 цифры карты
    kind: Kind
    description: str  # магазин, место или «откуда 2 куда»; у счёта — пусто
    amount: float  # без знака
    currency: str
    at: datetime  # местное время операции
    balance: float | None = None
    balance_currency: str | None = None
    cancelled: bool = False  # отмена покупки: деньги вернулись

    @property
    def incoming(self) -> bool:
        """Деньги пришли на карту: зачисление или отмена списания."""
        return self.cancelled or self.kind in INCOMING_KINDS


@dataclass(frozen=True)
class SmsBatch:
    """Сообщение из SMS: операции и пропущенные SMS (коды, отказы)."""

    operations: list[Sms] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


_AMOUNT = r"\d+(?:\.\d+)?"
_MONTHS = [name.upper() for name in MONTHS]
_KAPITAL_KINDS = {
    "Xarid/Pokupka": Kind.PAYMENT,
    "operacija": Kind.PAYMENT,
    "vznos": Kind.INCOMING,
    "Naqd yechish/Snyatie nalichnih": Kind.CASH_OUT,
    "Kirim/Vnesenie nalichnih": Kind.CASH_IN,
}
KAPITAL_CARD = re.compile(
    r"(?:(?P<cancel>Avtorizacija otmenena\. Avtorizacija bekor qilindi\.)"
    r"|(?P<declined>Hisobda mablag. yetarli emas\. Nedostatochno sredstv\.))? ?"
    r"Karta \*(?P<card>\d{4})\. "
    rf"(?P<kind>{'|'.join(map(re.escape, _KAPITAL_KINDS))}) "
    rf'"(?P<desc>[^"]*)", (?P<sign>[+-]?)(?P<amount>{_AMOUNT}), (?P<cur>[A-Z]{{3}}), '
    r'"(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4}|\d{2}) (?P<time>\d{2}:\d{2})"\. '
    rf"Dostupno: (?P<balance>-?{_AMOUNT}), (?P<bcur>[A-Z]{{3}})\."
)
KAPITAL_P2P = re.compile(
    r"(?P<kind>Snyatiye|Popolneniye), "
    r"(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{2}) v (?P<time>\d{2}:\d{2})\. "
    r"Karta \(\*(?P<card>\d{4})\)\. "
    rf"Summa: (?P<amount>{_AMOUNT}) (?P<cur>[A-Z]{{3}})\. (?P<desc>.+?)\. "
    rf"Dostupno: (?P<balance>-?{_AMOUNT}) (?P<bcur>[A-Z]{{3}})\.?"
)
KAPITAL_ACCOUNT = re.compile(
    r"Schet po karte \*(?P<card>\d{4}) (?P<kind>spisan|popolnen) na summu "
    rf"-?(?P<amount>{_AMOUNT}) (?P<cur>[A-Z]{{3}})\. "
    r"(?P<day>\d{2})-(?P<month>[A-Z]{3})-(?P<year>\d{4}|\d{2}) (?P<time>\d{2}:\d{2})"
)
_UZCARD_KINDS = {
    "E-Com oplata": Kind.PAYMENT,
    "Online to'lov": Kind.PAYMENT,
    "To'lov": Kind.PAYMENT,
    "Pokupka": Kind.PAYMENT,
    "Xarid": Kind.PAYMENT,
    "Platezh": Kind.PAYMENT,
    "Spisanie c karty": Kind.OUTGOING,
    "Vidacha nalichnykh v bankomate": Kind.CASH_OUT,
    "Perevod na kartu": Kind.INCOMING,
    "Kartaga o'tkazma": Kind.INCOMING,
    "Popolnenie scheta": Kind.INCOMING,
}
_APOSTROPHES = "'‘’`ʻ"
_UZCARD_KIND = "|".join(
    re.escape(kind).replace("'", f"[{_APOSTROPHES}]") for kind in _UZCARD_KINDS
)
UZCARD = re.compile(
    rf"(?P<cancel>BEKOR QILINDI: |OTMENA )?(?P<kind>{_UZCARD_KIND}):? "
    r"(?P<desc>.*?),? ?(?:UZ,)?"
    r"(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{2}) (?P<time>\d{2}:\d{2}),? ?"
    r"karta \*{3}(?P<card>\d{4})\. "
    rf"(?:summa|Miqdor):(?P<amount>{_AMOUNT}) UZS[.,]? ?"
    rf"(?:balans|Qoldiq):(?P<balance>-?{_AMOUNT}) UZS"
)
# Служебная SMS (код, неверный PIN) тянется до конца строки или до следующей SMS
_NEXT_SMS = (
    r"Karta \*\d{4}\.|Avtorizacija otmenena|Hisobda mablag|Snyatiye,|Popolneniye,"
    r"|Schet po karte|BEKOR QILINDI:|OTMENA |<#> |Kapitalbank|KAPITALBANK"
    rf"|INCORRECT PIN|(?:{_UZCARD_KIND}):?"
)
_REST = rf".*?(?= (?:{_NEXT_SMS})|$)"
CODES = re.compile(
    rf"(?:<#> )?KAPITALBANK NIKOMU NE PEREDAVAYTE KOD!{_REST}"
    rf"|(?:<#> )?Kapitalbank kirish tasdiqlash kodi: {_REST}"
    r"|Kapitalbank: data plateja .*?Kod podtverjdeniya(?: oplati)?: \d+"
)
INCORRECT_PIN = re.compile(rf"INCORRECT PIN {_REST}")


def _when(match: re.Match[str], month: int) -> datetime:
    """Время операции: год бывает из двух цифр."""
    year = int(match["year"]) + (2000 if len(match["year"]) == 2 else 0)
    hours, minutes = match["time"].split(":")
    return datetime(year, month, int(match["day"]), int(hours), int(minutes))


def _kapital_card(match: re.Match[str]) -> Sms | str | None:
    if match["declined"]:
        return DECLINED
    kind = _KAPITAL_KINDS[match["kind"]]
    sign = match["sign"]
    if sign and (sign == "+") != (kind in INCOMING_KINDS):
        return None  # знак не совпал с видом — смысл SMS не тот, что мы знаем
    if match["cancel"] and kind != Kind.PAYMENT:
        return None
    return Sms(
        text=match[0],
        card=match["card"],
        kind=kind,
        description=match["desc"].strip(),
        amount=float(match["amount"]),
        currency=match["cur"],
        at=_when(match, int(match["month"])),
        balance=float(match["balance"]),
        balance_currency=match["bcur"],
        cancelled=bool(match["cancel"]),
    )


def _kapital_p2p(match: re.Match[str]) -> Sms:
    return Sms(
        text=match[0],
        card=match["card"],
        kind=Kind.OUTGOING if match["kind"] == "Snyatiye" else Kind.INCOMING,
        description=match["desc"].strip(),
        amount=float(match["amount"]),
        currency=match["cur"],
        at=_when(match, int(match["month"])),
        balance=float(match["balance"]),
        balance_currency=match["bcur"],
    )


def _kapital_account(match: re.Match[str]) -> Sms | None:
    if match["month"] not in _MONTHS:
        return None
    return Sms(
        text=match[0],
        card=match["card"],
        kind=Kind.OUTGOING if match["kind"] == "spisan" else Kind.INCOMING,
        description="",
        amount=float(match["amount"]),
        currency=match["cur"],
        at=_when(match, _MONTHS.index(match["month"]) + 1),
    )


def _uzcard(match: re.Match[str]) -> Sms | None:
    kind = _UZCARD_KINDS[re.sub(f"[{_APOSTROPHES}]", "'", match["kind"])]
    if match["cancel"] and kind != Kind.PAYMENT:
        return None
    return Sms(
        text=match[0],
        card=match["card"],
        kind=kind,
        description=match["desc"].strip(),
        amount=float(match["amount"]),
        currency="UZS",
        at=_when(match, int(match["month"])),
        balance=float(match["balance"]),
        balance_currency="UZS",
        cancelled=bool(match["cancel"]),
    )


_TEMPLATES = (
    (KAPITAL_CARD, _kapital_card),
    (KAPITAL_P2P, _kapital_p2p),
    (KAPITAL_ACCOUNT, _kapital_account),
    (UZCARD, _uzcard),
    (CODES, lambda _: CODE),
    (INCORRECT_PIN, lambda _: WRONG_PIN),
)


def _read(line: str, start: int) -> tuple[Sms | str, int] | None:
    """SMS с позиции start: операция или причина пропуска и где SMS кончилась."""
    for pattern, convert in _TEMPLATES:
        match = pattern.match(line, start)
        if match is None:
            continue
        try:
            result = convert(match)
        except ValueError:  # 31.02 и прочие невозможные даты
            return None
        return None if result is None else (result, match.end())
    return None


def parse_sms(text: str) -> SmsBatch | None:
    """Текст из SMS → операции; None — если хоть что-то в тексте не узнано."""
    batch = SmsBatch()
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        position = 0
        while position < len(line):
            found = _read(line, position)
            if found is None:
                return None
            result, position = found
            if isinstance(result, Sms):
                batch.operations.append(result)
            else:
                batch.skipped.append(result)
            while position < len(line) and line[position] == " ":
                position += 1
    return batch if batch.operations or batch.skipped else None
