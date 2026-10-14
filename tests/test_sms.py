"""Разбор банковских SMS по шаблонам: виды, поля, отказ в пользу ИИ.

Образцы обезличены: магазины, суммы и остатки условные.
"""

from datetime import datetime

import pytest

from app.sms import CODE, DECLINED, WRONG_PIN, Kind, Sms, parse_sms

PURCHASE = (
    'Karta *9120. Xarid/Pokupka "OOO SHAVI CAFE>TASHKENT", -48000.00, UZS, '
    '"14-10-2026 12:33". Dostupno: 5000000.00, UZS.'
)
USD_ON_UZS = (
    'Karta *9120. Xarid/Pokupka "GOOGLE *Google One>8", -19.99, USD, '
    '"13-10-2026 12:44". Dostupno: 4000000.00, UZS.'
)
QR = (
    'Karta *9120. Xarid/Pokupka "VSMCUZSKB to QRonlin", -25000.00, UZS, '
    '"14-10-2026 08:10". Dostupno: 5048000.00, UZS.'
)
CANCELLED = (
    "Avtorizacija otmenena. Avtorizacija bekor qilindi.Karta *9120. "
    'Xarid/Pokupka "MYTAXI>TASHKENT", -1000.00, UZS, "11-10-2026 11:08". '
    "Dostupno: 3001000.00, UZS."
)
DECLINED_SMS = (
    "Hisobda mablag' yetarli emas. Nedostatochno sredstv. Karta *4058. "
    'Xarid/Pokupka "STEAM>BELLEVUE", -4.99, USD, "05-10-2026 21:15". '
    "Dostupno: 1.20, USD."
)
TOP_UP = (
    'Karta *9120. vznos "HUMO KB 2 VISA MC UZ", +500000.00, UZS, '
    '"02-02-26 10:00". Dostupno: 7000000.00, UZS.'
)
OPERATION = (
    'Karta *9120. operacija "VISA MC UZS 2 HUMO K", 300000.00, UZS, '
    '"03-02-2026 09:30". Dostupno: 6700000.00, UZS.'
)
CASH = (
    'Karta *4058. Naqd yechish/Snyatie nalichnih "OPERU KASSA VISA 2>T", -650.00, '
    'USD, "08-10-2026 11:00". Dostupno: 1000.00, USD.'
)
P2P_OUT = (
    "Snyatiye, 12.10.26 v 14:25. Karta (*9120). Summa: 52500.00 UZS. "
    "VISA MC UZS 2 UZCARD OB>Tashkent UZ. Dostupno: 4500000.00 UZS"
)
P2P_IN = (
    "Popolneniye, 12.04.26 v 09:05. Karta (*9120). Summa: 150000.00 UZS. "
    "UZCARDKB 2 VISA MC UZS>Tashkent UZ. Dostupno: 8000000.00 UZS"
)
ACCOUNT = "Schet po karte  *4058 popolnen na summu 100.00 USD. 05-FEB-2026 10:15"
UZCARD_QR = (
    "Online to'lov: PAYME OPLATA, UZ,14.10.26 10:15,karta ***5837. "
    "Miqdor:27000.00 UZS. Qoldiq:400000.00 UZS"
)
UZCARD_IN = (
    "Kartaga o'tkazma: VISA UZS KB TO UZCARD KB, UZ,07.10.26 15:24,karta ***5837. "
    "Miqdor:300000.00 UZS Qoldiq:700000.00 UZS"
)
UZCARD_CANCEL = (
    "BEKOR QILINDI: Xarid: OOO SHAVI CAFE, UZ,06.10.26 13:00,karta ***5837. "
    "Miqdor:48000.00 UZS Qoldiq:652000.00 UZS"
)
UZCARD_OLD = (
    "Pokupka: OOO SHAVI CAFE, TOSHKENT, Chilonzor tumani 16.03.25 12:00 "
    "karta ***5837. summa:48000.00 UZS, balans:1000000.00 UZS"
)
CODE_SMS = (
    "Kapitalbank:  data plateja  11 October 2026 06:07:23  summa  1000.00 UZS  "
    "s karti  9120. Kod podtverjdeniya oplati: 111111"
)
WRONG_PIN_SMS = (
    "INCORRECT PIN 4000***5837 12.10.26 10:00 TOSHKENT POS purchase 0.00 UZS"
)


def only(text: str) -> Sms:
    batch = parse_sms(text)
    assert batch is not None and len(batch.operations) == 1
    return batch.operations[0]


def test_kapital_purchase() -> None:
    sms = only(PURCHASE)
    assert (sms.card, sms.kind, sms.description) == (
        "9120",
        Kind.PAYMENT,
        "OOO SHAVI CAFE>TASHKENT",
    )
    assert (sms.amount, sms.currency) == (48000.0, "UZS")
    assert sms.at == datetime(2026, 10, 14, 12, 33)
    assert (sms.balance, sms.balance_currency) == (5000000.0, "UZS")
    assert not sms.incoming and not sms.cancelled
    assert sms.text == PURCHASE


def test_amount_in_another_currency_keeps_its_currency() -> None:
    sms = only(USD_ON_UZS)
    assert (sms.amount, sms.currency, sms.balance_currency) == (19.99, "USD", "UZS")


def test_cancelled_purchase_is_money_back_even_without_a_space() -> None:
    sms = only(CANCELLED)
    assert sms.cancelled and sms.incoming
    assert (sms.kind, sms.description, sms.amount) == (
        Kind.PAYMENT,
        "MYTAXI>TASHKENT",
        1000.0,
    )


@pytest.mark.parametrize(
    ("text", "kind", "description", "at"),
    [
        (TOP_UP, Kind.INCOMING, "HUMO KB 2 VISA MC UZ", datetime(2026, 2, 2, 10, 0)),
        (OPERATION, Kind.PAYMENT, "VISA MC UZS 2 HUMO K", datetime(2026, 2, 3, 9, 30)),
        (CASH, Kind.CASH_OUT, "OPERU KASSA VISA 2>T", datetime(2026, 10, 8, 11, 0)),
        (
            P2P_OUT,
            Kind.OUTGOING,
            "VISA MC UZS 2 UZCARD OB>Tashkent UZ",
            datetime(2026, 10, 12, 14, 25),
        ),
        (
            P2P_IN,
            Kind.INCOMING,
            "UZCARDKB 2 VISA MC UZS>Tashkent UZ",
            datetime(2026, 4, 12, 9, 5),
        ),
        (ACCOUNT, Kind.INCOMING, "", datetime(2026, 2, 5, 10, 15)),
        (UZCARD_QR, Kind.PAYMENT, "PAYME OPLATA", datetime(2026, 10, 14, 10, 15)),
        (
            UZCARD_IN,
            Kind.INCOMING,
            "VISA UZS KB TO UZCARD KB",
            datetime(2026, 10, 7, 15, 24),
        ),
        (
            UZCARD_OLD,
            Kind.PAYMENT,
            "OOO SHAVI CAFE, TOSHKENT, Chilonzor tumani",
            datetime(2025, 3, 16, 12, 0),
        ),
    ],
)
def test_kinds_descriptions_and_times(
    text: str, kind: Kind, description: str, at: datetime
) -> None:
    sms = only(text)
    assert (sms.kind, sms.description, sms.at) == (kind, description, at)


def test_account_sms_has_no_balance() -> None:
    sms = only(ACCOUNT)
    assert (sms.card, sms.amount, sms.currency, sms.balance) == (
        "4058",
        100.0,
        "USD",
        None,
    )


def test_uzcard_cancellation_and_any_apostrophe() -> None:
    sms = only(UZCARD_CANCEL)
    assert sms.cancelled and sms.description == "OOO SHAVI CAFE"
    assert only(UZCARD_IN.replace("o'tkazma", "o‘tkazma")).kind == Kind.INCOMING


def test_codes_declines_and_wrong_pin_are_skipped() -> None:
    batch = parse_sms("\n".join([CODE_SMS, DECLINED_SMS, WRONG_PIN_SMS, PURCHASE]))
    assert batch is not None
    assert batch.skipped == [CODE, DECLINED, WRONG_PIN]
    assert [sms.description for sms in batch.operations] == ["OOO SHAVI CAFE>TASHKENT"]


def test_several_sms_on_lines_and_on_one_line() -> None:
    batch = parse_sms(f"{PURCHASE}\n\n{QR} {UZCARD_QR}\n")
    assert batch is not None
    assert [sms.card for sms in batch.operations] == ["9120", "9120", "5837"]


def test_unusual_spaces_are_ignored() -> None:
    text = "  " + PURCHASE.replace(" ", "   ", 2) + "  "
    assert only(text).text == PURCHASE


@pytest.mark.parametrize(
    "text",
    [
        f"{PURCHASE} струны",  # приписка владельца
        f"кофе с коллегами\n{PURCHASE}",
        PURCHASE.replace("Xarid/Pokupka", "Qaytarish/Vozvrat"),  # незнакомый вид
        PURCHASE.replace('"14-10-2026', '"31-02-2026'),  # невозможная дата
        PURCHASE.replace("-48000.00", "+48000.00"),  # знак не совпал с видом
        PURCHASE[:60] + "\n" + PURCHASE[60:],  # SMS разорвана переносом
        "привет",
        "",
    ],
)
def test_anything_unknown_goes_to_ai(text: str) -> None:
    assert parse_sms(text) is None


@pytest.mark.parametrize(
    ("service", "operation"),
    [
        (
            "<#> KAPITALBANK NIKOMU NE PEREDAVAYTE KOD! Osteregaytes moshennikov. "
            "Dlya oplati vvedite kod podtverjdeniya: 111111. abc123",
            PURCHASE,
        ),
        (
            "<#> Kapitalbank kirish tasdiqlash kodi: 22222. HECH KIMGA KODNI BERMANG!",
            PURCHASE,
        ),
        (WRONG_PIN_SMS, UZCARD_QR),
    ],
)
def test_service_sms_does_not_swallow_the_next_sms_on_its_line(
    service: str, operation: str
) -> None:
    batch = parse_sms(f"{service} {operation}")
    assert batch is not None
    assert len(batch.skipped) == 1
    assert [sms.text for sms in batch.operations] == [operation]
