from datetime import date

import pytest
from pydantic import ValidationError

from app.domain import (
    FALLBACK_CATEGORY,
    FALLBACK_SUBCATEGORY,
    Catalog,
    Direction,
    ParsedTransaction,
    Rates,
    SheetRow,
    Skipped,
    build_row,
    entry_row,
    manual_row,
    parse_day,
    parse_manual_entry,
    parse_number,
    resolve_category,
    resolve_source,
    signed_amount,
)

TODAY = date(2026, 10, 6)
CATALOG = Catalog(
    categories=[
        "🏚️ ДОМ",
        "🍔 ЕДА",
        "🌛 ЕЖЕМЕСЯЧНО",
        "🚧 РАЗНОЕ",
        "💳 СЧЕТА",
        "💰 ДОХОДЫ",
    ],
    subcategories={
        "🏚️ ДОМ": ["квартплата", "продукты"],
        "🍔 ЕДА": ["кофе", "кафе"],
        "🌛 ЕЖЕМЕСЯЧНО": ["подписка"],
        "🚧 РАЗНОЕ": ["неучтенка"],
        "💳 СЧЕТА": ["переводы", "обмен валюты"],
        "💰 ДОХОДЫ": ["зарплата", "нач остаток"],
    },
    sources=["VISA 9120 UZS", "UZCARD 5837 UZS", "VISA 4058 USD", "VISA 7450 RUB"],
)
RATES = Rates({"UZS": 1.0, "USD": 12000.0, "RUB": 150.0})


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (12.5, 12.5),
        (-1500, 1500.0),
        ("1 234,56", 1234.56),
        ("1,500,000.00", 1500000.0),
        ("1.500.000,50", 1500000.5),
        ("1,500", 1500.0),
        ("12,5", 12.5),
        ("$50", 50.0),
        ("12.21 USD", 12.21),
        ("abc", None),
        (None, None),
        (True, None),
    ],
)
def test_parse_number(raw: object, expected: float | None) -> None:
    assert parse_number(raw) == expected


def test_parsed_transaction_coerces_fields() -> None:
    parsed = ParsedTransaction.model_validate(
        {
            "amount": "-41 845,10",
            "currency": "sum",
            "direction": "Refund",
            "balance": "1,234,567.89",
            "card_identifier": "*9120",
            "unknown_field": 1,
        }
    )
    assert parsed.amount == 41845.1
    assert parsed.currency == "UZS"
    assert parsed.direction is Direction.REFUND
    assert parsed.balance == 1234567.89
    assert parsed.card_identifier == "9120"


@pytest.mark.parametrize("amount", [0, "0", "", None, "abc"])
def test_parsed_transaction_without_amount_is_invalid(amount: object) -> None:
    with pytest.raises(ValidationError):
        ParsedTransaction.model_validate({"amount": amount})


def test_unknown_direction_means_expense() -> None:
    assert ParsedTransaction(amount=1, direction="purchase").direction is (
        Direction.EXPENSE
    )


@pytest.mark.parametrize(
    ("amount", "src", "dst", "expected"),
    [
        (12.21, "USD", "UZS", 146520.0),
        (150000, "UZS", "USD", 12.5),
        (100, "RUB", "USD", 1.25),
        (5, "UZS", "UZS", 5),
        (5, "SGD", "USD", None),
    ],
)
def test_rates_convert(amount: float, src: str, dst: str, expected: float) -> None:
    result = RATES.convert(amount, src, dst)
    assert result == (pytest.approx(expected) if expected is not None else None)


@pytest.mark.parametrize(
    ("raw", "card", "default", "expected"),
    [
        ("VISA 9120 UZS", None, None, "VISA 9120 UZS"),
        ("*9120", None, None, "VISA 9120 UZS"),
        (None, "4058", None, "VISA 4058 USD"),
        ("Карта VISA 7450", None, None, "VISA 7450 RUB"),
        ("uzcard", None, None, "UZCARD 5837 UZS"),
        ("VISA", None, "UZCARD 5837 UZS", "UZCARD 5837 UZS"),
        ("Сбер", None, None, None),
    ],
)
def test_resolve_source(
    raw: str | None, card: str | None, default: str | None, expected: str | None
) -> None:
    assert resolve_source(raw, card, CATALOG.sources, default) == expected


@pytest.mark.parametrize(
    ("category", "subcategory", "expected"),
    [
        ("🍔 ЕДА", "кофе", ("🍔 ЕДА", "кофе", None)),
        ("ЕДА", "Кафе", ("🍔 ЕДА", "кафе", None)),
        ("🍔 ЕДА", "продукты", ("🏚️ ДОМ", "продукты", None)),
        (None, "подписка", ("🌛 ЕЖЕМЕСЯЧНО", "подписка", None)),
        (
            "Прочее",
            "AI (Не распознано)",
            (
                FALLBACK_CATEGORY,
                FALLBACK_SUBCATEGORY,
                "AI: Прочее / AI (Не распознано)",
            ),
        ),
        ("🍔 ЕДА", None, (FALLBACK_CATEGORY, FALLBACK_SUBCATEGORY, "AI: 🍔 ЕДА")),
        (None, None, (FALLBACK_CATEGORY, FALLBACK_SUBCATEGORY, None)),
    ],
)
def test_resolve_category(
    category: str | None, subcategory: str | None, expected: tuple
) -> None:
    assert resolve_category(category, subcategory, CATALOG) == expected


@pytest.mark.parametrize(
    ("direction", "category", "expected"),
    [
        (Direction.EXPENSE, "🍔 ЕДА", 100.0),
        (Direction.TRANSFER_OUT, "💳 СЧЕТА", 100.0),
        (Direction.TRANSFER_IN, "💳 СЧЕТА", -100.0),
        (Direction.REFUND, "🍔 ЕДА", -100.0),
        (Direction.INCOME, "💰 ДОХОДЫ", 100.0),
        (Direction.TRANSFER_IN, "💰 ДОХОДЫ", 100.0),
        (Direction.INCOME, "🚧 РАЗНОЕ", -100.0),
    ],
)
def test_signed_amount(direction: Direction, category: str, expected: float) -> None:
    assert signed_amount(100.0, direction, category) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("05.10.2026", date(2026, 10, 5)),
        ("05/10/2026", date(2026, 10, 5)),
        ("2026-10-05", date(2026, 10, 5)),
        ("05.10.26", date(2026, 10, 5)),
        ("07.10.2026", TODAY),
        ("01.01.2020", TODAY),
        ("вчера", TODAY),
        (None, TODAY),
    ],
)
def test_parse_day(raw: str | None, expected: date) -> None:
    assert parse_day(raw, TODAY) == expected


def build(**fields: object) -> SheetRow | Skipped:
    return build_row(
        ParsedTransaction.model_validate(fields), CATALOG, RATES, TODAY, None
    )


def test_usd_purchase_on_uzs_card_is_converted() -> None:
    row = build(
        amount=12.21,
        currency="USD",
        source="*9120",
        category="🌛 ЕЖЕМЕСЯЧНО",
        subcategory="подписка",
        comment="JetBrains",
        date="05.01.2026",
    )
    assert isinstance(row, SheetRow)
    assert (row.amount, row.currency, row.source) == (146520.0, "UZS", "VISA 9120 UZS")
    assert row.comment == "JetBrains; ≈ 12.21 USD по курсу таблицы"
    assert row.date_text == "05.01.2026"


def test_unknown_currency_is_skipped_not_guessed() -> None:
    result = build(amount=30, currency="SGD", source="VISA 4058 USD", comment="Airalo")
    assert isinstance(result, Skipped)
    assert "SGD" in result.reason


def test_incoming_transfer_gets_minus() -> None:
    row = build(
        amount=1000000,
        source="HUMO",
        card_identifier="5837",
        category="💳 СЧЕТА",
        subcategory="переводы",
        direction="transfer_in",
    )
    assert isinstance(row, SheetRow)
    assert (row.source, row.amount) == ("UZCARD 5837 UZS", -1000000)


def test_salary_stays_positive() -> None:
    row = build(
        amount=33385000,
        source="VISA 9120 UZS",
        category="ДОХОДЫ",
        subcategory="зарплата",
        direction="income",
    )
    assert isinstance(row, SheetRow)
    assert (row.category, row.amount) == ("💰 ДОХОДЫ", 33385000)


def test_unknown_category_goes_to_misc_with_note() -> None:
    row = build(amount=28000, source="UZCARD 5837 UZS", category="Прочее")
    assert isinstance(row, SheetRow)
    assert (row.category, row.subcategory) == (FALLBACK_CATEGORY, FALLBACK_SUBCATEGORY)
    assert row.comment == "AI; AI: Прочее"


def test_missing_source_is_skipped() -> None:
    assert isinstance(build(amount=100, source="Сбер"), Skipped)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("5000 кофе", (5000.0, False, "кофе")),
        ("5000", (5000.0, False, "")),
        ("5 000,50 обед", (5000.5, False, "обед")),
        ("+20000 возврат", (20000.0, True, "возврат")),
        ("1500.5", (1500.5, False, "")),
        ("12.10.2026 Oplata 50000 UZS", None),
        ("Pokupka 50000 UZS", None),
        ("0 ноль", None),
        ("", None),
    ],
)
def test_parse_manual_entry(text: str, expected: tuple | None) -> None:
    assert parse_manual_entry(text) == expected


@pytest.mark.parametrize(
    ("text", "category", "expected"),
    [
        ("48000 латте", "🍔 ЕДА", 48000.0),
        ("+5000 вернули", "🍔 ЕДА", -5000.0),
        ("+1000000 с HUMO", "💳 СЧЕТА", -1000000.0),
        ("30000000 аванс", "💰 ДОХОДЫ", 30000000.0),
        ("+30000000 аванс", "💰 ДОХОДЫ", 30000000.0),
    ],
)
def test_manual_row_sign(text: str, category: str, expected: float) -> None:
    row = manual_row(text, "VISA 9120 UZS", category, "кофе", TODAY)
    assert row is not None
    assert (row.amount, row.currency, row.day) == (expected, "UZS", TODAY)


@pytest.mark.parametrize(
    ("category", "incoming", "expected"),
    [
        ("🍔 ЕДА", False, 48000.0),  # расход — плюс
        ("💰 ДОХОДЫ", False, 48000.0),  # доход — плюс
        ("💳 СЧЕТА", True, -48000.0),  # приход, который не доход, — минус
    ],
)
def test_entry_row_signs_like_a_manual_entry(
    category: str, incoming: bool, expected: float
) -> None:
    row = entry_row(
        48000.0, incoming, "латте", "VISA 4058 USD", category, "кофе", TODAY
    )
    assert (row.amount, row.currency, row.source) == (expected, "USD", "VISA 4058 USD")
    assert (row.day, row.comment, row.category) == (TODAY, "латте", category)
