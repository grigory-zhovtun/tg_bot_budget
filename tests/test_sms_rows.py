"""SMS → позиции fact: класс, категория, пометки, память по сумме, пары, порядок."""

from datetime import date
from typing import Any

import pytest

from app.domain import MerchantBook, Rates, Skipped
from app.sms import Sms, parse_sms
from app.sms_rows import (
    CHECK,
    NEW,
    AmountMemory,
    Purpose,
    SmsRow,
    card_source,
    new_merchants,
    purpose,
    sms_rows,
)
from tests.test_sms import (
    ACCOUNT,
    CANCELLED,
    CASH,
    OPERATION,
    P2P_IN,
    P2P_OUT,
    PURCHASE,
    QR,
    TOP_UP,
    USD_ON_UZS,
    UZCARD_IN,
    UZCARD_QR,
)

SOURCES = ["VISA 9090 UZS", "UZCARD 5656 UZS", "VISA 3434 USD"]
RATES = Rates({"UZS": 1.0, "USD": 12000.0, "RUB": 150.0})
TODAY = date(2026, 10, 14)
HEADER = ["Дата", "Категория", "Название", "Сумма", "Баланс", "Комментарий"]
EMPTY = AmountMemory({})

OWN_OUT = (
    "Snyatiye, 07.10.26 v 15:23. Karta (*9090). Summa: 300300.00 UZS. "
    "VISA MC UZS 2 UZCARD KB>Tashkent UZ. Dostupno: 9000000.00 UZS"
)
OWN_IN = (
    "Kartaga o'tkazma: VISA UZS KB TO UZCARD KB, UZ,07.10.26 15:24,karta ***5656. "
    "Miqdor:300000.00 UZS Qoldiq:800000.00 UZS"
)
FX_OUT = (
    "Snyatiye, 05.10.26 v 10:00. Karta (*9090). Summa: 1210000.00 UZS. "
    "VISA MC UZS 2 VISA MC USD>Tashkent UZ. Dostupno: 9000000.00 UZS"
)
FX_IN = (
    'Karta *3434. vznos "VISA MC UZS 2 VISA M", +100.00, USD, "05-10-2026 10:01". '
    "Dostupno: 1100.00, USD."
)


def ops(*texts: str) -> list[Sms]:
    batch = parse_sms("\n".join(texts))
    assert batch is not None
    return batch.operations


def rows_of(
    *texts: str,
    book: MerchantBook | None = None,
    memory: AmountMemory = EMPTY,
    suggestions: dict[str, tuple[str, str]] | None = None,
) -> tuple[list[SmsRow], list[Skipped]]:
    return sms_rows(
        ops(*texts), SOURCES, book or MerchantBook([]), memory, suggestions or {}, RATES
    )


def fact_row(
    day: str, category: str, sub: str, amount: Any, comment: str, source: str = ""
) -> list[Any]:
    source = source or "VISA 9090 UZS"
    return [day, category, sub, amount, "", comment, source[-3:], source]


def target(row: SmsRow) -> tuple[str, str]:
    return row.item["category"], row.item["subcategory"]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (PURCHASE, Purpose.MERCHANT),
        (QR, Purpose.ANONYMOUS),
        (UZCARD_QR, Purpose.ANONYMOUS),
        (OPERATION, Purpose.TRANSFER),  # operacija «VISA MC UZS 2 HUMO K»
        (P2P_OUT, Purpose.TRANSFER),
        (P2P_IN, Purpose.INCOMING),
        (TOP_UP, Purpose.INCOMING),
        (UZCARD_IN, Purpose.INCOMING),
        (ACCOUNT, Purpose.INCOMING),
        (CASH, Purpose.CASH_OUT),
        (CANCELLED, Purpose.MERCHANT),
    ],
)
def test_purpose(text: str, expected: Purpose) -> None:
    [sms] = ops(text)
    assert purpose(sms) == expected


def test_aggregator_with_a_name_is_a_merchant() -> None:
    [sms] = ops(UZCARD_QR.replace("PAYME OPLATA", "PAYME OPLATA MUNIS"))
    assert purpose(sms) == Purpose.MERCHANT


def test_card_is_found_by_its_four_digits_only_once() -> None:
    assert card_source("9090", SOURCES) == "VISA 9090 UZS"
    assert card_source("1234", SOURCES) is None
    assert card_source("9090", [*SOURCES, "HUMO 9090 UZS"]) is None


def test_known_merchant_takes_the_history_category() -> None:
    book = MerchantBook([("🍔 ЕДА", "кафе", "OOO SHAVI CAFE>TASHKENT")])
    [row], skipped = rows_of(PURCHASE, book=book)
    assert skipped == []
    assert row.flag is None
    assert row.item == {
        "amount": 48000.0,
        "currency": "UZS",
        "date": "14.10.2026",
        "category": "🍔 ЕДА",
        "subcategory": "кафе",
        "comment": "OOO SHAVI CAFE>TASHKENT",
        "source": "VISA 9090 UZS",
        "card_identifier": "9090",
        "direction": "expense",
        "balance": 5000000.0,
    }


def test_new_merchant_takes_the_ai_suggestion_and_is_flagged() -> None:
    assert new_merchants(ops(PURCHASE), SOURCES, MerchantBook([]), EMPTY) == [
        "OOO SHAVI CAFE"
    ]
    suggestions = {"OOO SHAVI CAFE": ("🍔 ЕДА", "кафе")}
    [row], _ = rows_of(PURCHASE, suggestions=suggestions)
    assert (target(row), row.flag) == (("🍔 ЕДА", "кафе"), NEW)


@pytest.mark.parametrize(
    "suggestion", [None, ("💰 ДОХОДЫ", "зарплата"), ("🚧 РАЗНОЕ", "неучтенка")]
)
def test_new_merchant_without_a_usable_suggestion_goes_to_misc(
    suggestion: tuple[str, str] | None,
) -> None:
    suggestions = {"OOO SHAVI CAFE": suggestion} if suggestion else {}
    [row], _ = rows_of(PURCHASE, suggestions=suggestions)
    assert (target(row), row.flag) == (("🚧 РАЗНОЕ", "неучтенка"), NEW)


def test_income_in_history_is_not_a_purchase_category() -> None:
    book = MerchantBook([("💰 ДОХОДЫ", "зарплата", "OOO SHAVI CAFE")])
    [row], _ = rows_of(PURCHASE, book=book)
    assert row.flag == NEW


def test_cancelled_purchase_is_a_refund_in_the_same_category() -> None:
    book = MerchantBook([("🚌 ТРАНСПОРТ", "такси", "MYTAXI>TASHKENT")])
    [row], _ = rows_of(CANCELLED, book=book)
    assert (row.item["direction"], target(row), row.flag) == (
        "refund",
        ("🚌 ТРАНСПОРТ", "такси"),
        None,
    )


def test_anonymous_payment_up_to_the_limit_is_coffee_to_check() -> None:
    [row], _ = rows_of(QR)  # 25 000 сумов
    assert (target(row), row.flag) == (("🍔 ЕДА", "кофе"), CHECK)
    assert row.item["comment"] == "VSMCUZSKB to QRonlin"


def test_anonymous_payment_above_the_limit_is_misc_to_check() -> None:
    [row], _ = rows_of(QR.replace("-25000.00", "-65000.00"))
    assert (target(row), row.flag) == (("🚧 РАЗНОЕ", "неучтенка"), CHECK)


def test_amount_memory_decides_an_anonymous_payment() -> None:
    fact = [
        HEADER,
        fact_row("12.10.2026", "🍔 ЕДА", "кофе", "65 000,00", ""),  # из Mini App
        fact_row("13.10.2026", "🍔 ЕДА", "кофе", 65000, "VSMCUZSKB to QRonlin"),
        fact_row("13.10.2026", "🚧 РАЗНОЕ", "неучтенка", 65000, "PAYME OPLATA"),
        fact_row("01.10.2026", "🏚️ ДОМ", "продукты", 65000, "OOO SHAVI CAFE"),
    ]
    memory = AmountMemory.from_sheet(fact, TODAY)
    [row], _ = rows_of(QR.replace("-25000.00", "-65000.00"), memory=memory)
    assert (target(row), row.flag) == (("🍔 ЕДА", "кофе"), None)


@pytest.mark.parametrize(
    "rows",
    [
        [
            fact_row("12.10.2026", "🍔 ЕДА", "кофе", 65000, ""),
            fact_row("13.10.2026", "🍔 ЕДА", "кафе", 65000, ""),
        ],
        [fact_row("01.08.2026", "🍔 ЕДА", "кофе", 65000, "")],  # старше 60 дней
        [fact_row("12.10.2026", "🍔 ЕДА", "кофе", 65000, "", "UZCARD 5656 UZS")],
        [fact_row("12.10.2026", "🍔 ЕДА", "кофе", "-65 000", "")],  # приход
        [fact_row("12.10.2026", "💰 ДОХОДЫ", "зарплата", 65000, "")],
    ],
)
def test_amount_memory_needs_one_recent_expense_category_on_the_card(
    rows: list[list[Any]],
) -> None:
    memory = AmountMemory.from_sheet([HEADER, *rows], TODAY)
    assert memory.find("VISA 9090 UZS", "", 65000) is None


def test_amount_memory_tells_coffee_from_lunch_in_one_cafe() -> None:
    lunches = [
        fact_row(f"0{day}.10.2026", "🍔 ЕДА", "кафе", 78000 + day, "OOO SHAVI CAFE")
        for day in range(1, 6)
    ]
    coffee = fact_row("09.10.2026", "🍔 ЕДА", "кофе", 48000, "OOO SHAVI CAFE>TASHKENT")
    fact = [HEADER, *lunches, coffee]
    book, memory = MerchantBook.from_sheet(fact), AmountMemory.from_sheet(fact, TODAY)
    [row], _ = rows_of(PURCHASE, book=book, memory=memory)  # 48 000
    assert (target(row), row.flag) == (("🍔 ЕДА", "кофе"), None)
    [row], _ = rows_of(
        PURCHASE.replace("-48000.00", "-95000.00"), book=book, memory=memory
    )
    assert (target(row), row.flag) == (("🍔 ЕДА", "кафе"), None)  # 5 из 6 — кафе


@pytest.mark.parametrize(
    ("text", "expected", "flag", "comment", "direction"),
    [
        (
            P2P_OUT,
            ("🚧 РАЗНОЕ", "неучтенка"),
            CHECK,
            "перевод: VISA MC UZS 2 UZCARD OB",
            "transfer_out",
        ),
        (
            P2P_IN,
            ("💳 СЧЕТА", "переводы"),
            CHECK,
            "входящий перевод: UZCARDKB 2 VISA MC UZS",
            "transfer_in",
        ),
        (
            UZCARD_IN,
            ("💳 СЧЕТА", "переводы"),
            CHECK,
            "входящий перевод: VISA UZS KB TO UZCARD KB",
            "transfer_in",
        ),
        (
            ACCOUNT,
            ("💳 СЧЕТА", "переводы"),
            CHECK,
            "входящий перевод: пополнение счёта",
            "transfer_in",
        ),
        (
            CASH,
            ("🏚️ ДОМ", "квартплата"),
            None,
            "снятие наличных: OPERU KASSA VISA 2",
            "transfer_out",
        ),
        (
            CASH.replace("-650.00", "-700.00"),
            ("🏚️ ДОМ", "квартплата"),
            CHECK,
            "снятие наличных: OPERU KASSA VISA 2",
            "transfer_out",
        ),
    ],
)
def test_operations_without_a_merchant(
    text: str, expected: tuple[str, str], flag: str | None, comment: str, direction: str
) -> None:
    [row], _ = rows_of(text)
    item = row.item
    assert (target(row), row.flag, item["comment"], item["direction"]) == (
        expected,
        flag,
        comment,
        direction,
    )


def test_cash_in_sums_is_misc_to_check() -> None:
    text = (
        CASH.replace("*3434", "*9090")
        .replace("-650.00, USD", "-500000.00, UZS")
        .replace("1000.00, USD", "1000.00, UZS")
    )
    [row], _ = rows_of(text)
    assert (target(row), row.flag) == (("🚧 РАЗНОЕ", "неучтенка"), CHECK)


def test_unknown_card_is_reported_not_guessed() -> None:
    rows, [skipped] = rows_of(PURCHASE.replace("*9090", "*1234"))
    assert rows == []
    assert skipped == Skipped("карта *1234 не найдена в листе system", "OOO SHAVI CAFE")


def test_newest_first_paste_is_written_in_time_order_with_the_latest_balance() -> None:
    later = PURCHASE.replace("12:33", "18:26").replace("5000000.00", "4990000.00")
    rows, _ = rows_of(later, PURCHASE, QR)  # как в приложении: новые сверху
    assert [row.item["comment"][:4] + row.item["date"] for row in rows] == [
        "VSMC14.10.2026",
        "OOO 14.10.2026",
        "OOO 14.10.2026",
    ]
    assert [row.item["balance"] for row in rows] == [None, None, 4990000.0]


def test_sms_of_one_minute_keep_the_bank_order() -> None:
    hold = CANCELLED.replace("Avtorizacija otmenena. Avtorizacija bekor qilindi.", "")
    hold = hold.replace("3001000.00", "3000000.00")
    later = PURCHASE.replace("14-10-2026 12:33", "11-10-2026 11:30")
    newest_first = rows_of(later, CANCELLED, hold)[0]  # отмена новее холда
    oldest_first = rows_of(hold, CANCELLED, later)[0]
    for rows in (newest_first, oldest_first):
        assert [row.item["direction"] for row in rows] == [
            "expense",
            "refund",
            "expense",
        ]
        assert [row.item["balance"] for row in rows] == [None, None, 5000000.0]


def test_balance_in_another_currency_is_not_the_card_balance() -> None:
    [row], _ = rows_of(PURCHASE.replace("5000000.00, UZS", "500.00, USD"))
    assert row.item["balance"] is None


def test_amount_in_another_currency_is_left_for_conversion() -> None:
    book = MerchantBook([("🌛 ЕЖЕМЕСЯЧНО", "подписка", "GOOGLE *Google One")])
    [row], _ = rows_of(USD_ON_UZS, book=book)
    assert (row.item["amount"], row.item["currency"], row.item["source"]) == (
        12.34,
        "USD",
        "VISA 9090 UZS",
    )
    assert row.flag is None


def test_own_transfer_in_one_message_is_a_pair_with_the_fee_apart() -> None:
    rows, _ = rows_of(OWN_OUT, OWN_IN)
    assert [
        (
            row.item["source"],
            target(row),
            row.item["amount"],
            row.item["comment"],
            row.item["direction"],
            row.flag,
        )
        for row in rows
    ] == [
        (
            "VISA 9090 UZS",
            ("💳 СЧЕТА", "переводы"),
            300000.0,
            "перевод VISA 9090 → UZCARD 5656 (свой)",
            "transfer_out",
            None,
        ),
        (
            "VISA 9090 UZS",
            ("🚧 РАЗНОЕ", "неучтенка"),
            300.0,
            "комиссия за перевод на свою карту",
            "expense",
            None,
        ),
        (
            "UZCARD 5656 UZS",
            ("💳 СЧЕТА", "переводы"),
            300000.0,
            "перевод UZCARD 5656 ← VISA 9090 (свой)",
            "transfer_in",
            None,
        ),
    ]
    assert [row.sms is None for row in rows] == [False, True, False]


def test_exchange_between_own_cards_is_a_pair() -> None:
    rows, _ = rows_of(FX_OUT, FX_IN)
    assert [(target(row), row.item["comment"], row.flag) for row in rows] == [
        (
            ("💳 СЧЕТА", "обмен валюты"),
            "конвертация VISA 9090 → VISA 3434 USD (свой)",
            None,
        ),
        (
            ("💳 СЧЕТА", "обмен валюты"),
            "конвертация VISA 3434 USD ← VISA 9090 (свой)",
            None,
        ),
    ]


@pytest.mark.parametrize(
    "credit",
    [
        OWN_IN.replace("15:24", "15:40"),  # больше 10 минут
        OWN_IN.replace("Miqdor:300000.00", "Miqdor:250000.00"),  # суммы не сходятся
        OWN_IN.replace("***5656", "***9090"),  # та же карта — не пара
    ],
)
def test_far_or_different_operations_are_not_a_pair(credit: str) -> None:
    rows, _ = rows_of(OWN_OUT, credit)
    assert all(row.flag == CHECK for row in rows)


def test_each_sms_joins_one_pair_nearest_first() -> None:
    near = OWN_IN.replace("15:24", "15:23")
    rows, _ = rows_of(OWN_OUT, OWN_IN, near)
    assert [row.flag for row in rows].count(CHECK) == 1


@pytest.mark.parametrize("cancel_first", [True, False])
def test_hold_and_its_cancellation_alone_keep_the_bank_order(
    cancel_first: bool,
) -> None:
    hold = CANCELLED.replace("Avtorizacija otmenena. Avtorizacija bekor qilindi.", "")
    hold = hold.replace("3001000.00", "3000000.00")
    rows, _ = rows_of(*((CANCELLED, hold) if cancel_first else (hold, CANCELLED)))
    assert [row.item["direction"] for row in rows] == ["expense", "refund"]
    assert [row.item["balance"] for row in rows] == [None, 3001000.0]


def test_sms_of_one_minute_in_unknown_order_give_no_balance() -> None:
    other = PURCHASE.replace("SHAVI CAFE", "KOFE UYI").replace(
        "5000000.00", "4952000.00"
    )
    rows, _ = rows_of(other, PURCHASE)  # обе в 12:33: кто из них новее — неизвестно
    assert [row.item["balance"] for row in rows] == [None, None]
