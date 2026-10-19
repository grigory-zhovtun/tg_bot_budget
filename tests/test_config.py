"""Настройки из переменных окружения читаются бережно."""

import pytest

from app.config import parse_amount


@pytest.mark.parametrize(
    ("text", "amount"),
    [
        ("380", 380.0),
        ("380.5", 380.5),
        ("380,5", 380.5),
        (" 380 ", 380.0),
        ("", None),
        ("off", None),
        ("abc", None),
        ("0", None),
        ("-5", None),
    ],
)
def test_the_rent_cash_sum_is_read_kindly(text: str, amount: float | None) -> None:
    assert parse_amount(text) == amount
