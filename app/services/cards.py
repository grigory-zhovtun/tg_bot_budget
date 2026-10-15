"""Остатки карт для «Сводки»: блок остатков листа fact — как его видит таблица.

Блок I2:Q30 листа fact: I — карта, J — «Текущий баланс» (формула по строкам
fact, в валюте карты), N — «Проверка» (остаток банка из SMS или скрина),
Q — «Сверено» (когда). «Сводка» читает fact целиком ради трат, поэтому блок
берётся из того же чтения — без лишнего запроса к Google.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from app.domain import Rates, currency_of, same_balance
from app.services.analytics_service import SERIAL_ZERO, STALE_CHECK_DAYS

# колонки блока (с 0) — те же, что пишет и читает GoogleSheetsService
NAME, TABLE, BANK, CHECKED = 8, 9, 13, 16  # I, J, N, Q
FIRST_ROW, LAST_ROW = 2, 30  # BALANCE_BLOCK = I2:I30


@dataclass(frozen=True)
class CardBalance:
    name: str  # как в блоке: «VISA 1111 UZS»
    currency: str
    balance: float | None  # «Текущий баланс»; None — в ячейке ошибка (#N/A)
    uzs: float | None  # по курсу листа system; None — нет курса или остатка
    bank: float | None  # «Проверка»: остаток банка при последней сверке
    checked: datetime | None  # «Сверено», время Ташкента

    @property
    def matches(self) -> bool | None:
        """Сходится ли остаток таблицы с банком; None — сверять не с чем."""
        if self.balance is None or self.bank is None:
            return None
        return same_balance(self.bank - self.balance, self.name)

    def stale(self, today: date) -> bool:
        """Сверке больше недели — как в напоминании воскресной сводки.

        Остаток банка без времени сверки — тоже давно: бот всегда ставит время,
        такой остаток вписан руками раньше. Без остатка банка сверки нет вовсе.
        """
        if self.bank is None:
            return False
        if self.checked is None:
            return True
        return (today - self.checked.date()).days > STALE_CHECK_DAYS


def _number(value: Any) -> float | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return None


def _moment(value: Any) -> datetime | None:
    """Серийный номер таблицы с долей суток → время с точностью до минуты.

    Число вне дат — скажем, остаток, вставленный не в ту колонку, — не время.
    """
    number = _number(value)
    if number is None:
        return None
    start = datetime.combine(SERIAL_ZERO, datetime.min.time())
    try:
        return start + timedelta(minutes=round(number * 24 * 60))
    except (OverflowError, ValueError):  # позже 9999 года или раньше 1-го
        return None


def card_balances(
    rows: Sequence[Sequence[Any]], rates: Rates
) -> tuple[CardBalance, ...]:
    """Карты блока по порядку листа; строка итогов (в I пусто) — не карта."""
    found = []
    width = CHECKED + 1
    for row in rows[FIRST_ROW - 1 : LAST_ROW]:
        cells = list(row[:width]) + [""] * (width - len(row[:width]))
        name = str(cells[NAME]).strip()
        if not name:
            continue
        currency = currency_of(name)
        balance = _number(cells[TABLE])
        rate = rates.to_uzs.get(currency)
        found.append(
            CardBalance(
                name,
                currency,
                balance,
                balance * rate if balance is not None and rate else None,
                _number(cells[BANK]),
                _moment(cells[CHECKED]),
            )
        )
    return tuple(found)
