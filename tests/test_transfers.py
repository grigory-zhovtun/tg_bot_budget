"""Перевод между своими картами из «Ввода»: две строки fact одной записью."""

from datetime import date

import pytest

from app.transfers import transfer_rows

DAY = date(2026, 10, 5)


@pytest.mark.parametrize(
    ("to_source", "subcategory", "amount", "to_amount", "comment", "expected"),
    [
        (
            "UZCARD 5656 UZS",
            "переводы",
            500_000,
            None,
            "",
            [
                (
                    "VISA 9090 UZS",
                    500_000,
                    "UZS",
                    "перевод VISA 9090 → UZCARD 5656 (свой)",
                ),
                (
                    "UZCARD 5656 UZS",
                    -500_000,
                    "UZS",
                    "перевод UZCARD 5656 ← VISA 9090 (свой)",
                ),
            ],
        ),
        (
            "VISA 3434 USD",
            "обмен валюты",
            1_210_800,
            100,
            "",
            [
                (
                    "VISA 9090 UZS",
                    1_210_800,
                    "UZS",
                    "конвертация VISA 9090 → VISA 3434 USD (свой)",
                ),
                (
                    "VISA 3434 USD",
                    -100,
                    "USD",
                    "конвертация VISA 3434 USD ← VISA 9090 (свой)",
                ),
            ],
        ),
        (
            "UZCARD 5656 UZS",
            "переводы",
            500_000,
            500_000,
            "на отпуск",
            [
                (
                    "VISA 9090 UZS",
                    500_000,
                    "UZS",
                    "перевод VISA 9090 → UZCARD 5656 (свой), на отпуск",
                ),
                (
                    "UZCARD 5656 UZS",
                    -500_000,
                    "UZS",
                    "перевод UZCARD 5656 ← VISA 9090 (свой), на отпуск",
                ),
            ],
        ),
    ],
)
def test_a_transfer_is_a_withdrawal_and_a_top_up(
    to_source: str,
    subcategory: str,
    amount: float,
    to_amount: float | None,
    comment: str,
    expected: list[tuple[str, float, str, str]],
) -> None:
    rows = transfer_rows(
        amount,
        to_amount,
        comment,
        "VISA 9090 UZS",
        to_source,
        "💳 СЧЕТА",
        subcategory,
        DAY,
    )
    assert [(r.source, r.amount, r.currency, r.comment) for r in rows] == expected
    assert {(r.day, r.category, r.subcategory) for r in rows} == {
        (DAY, "💳 СЧЕТА", subcategory)
    }
