"""Остатки карт для «Сводки»: блок остатков листа fact — как его видит таблица.

Блок I2:Q30 листа fact: I — карта, J — «Текущий баланс» (формула по строкам
fact, в валюте карты), N — «Проверка» (остаток банка из SMS или скрина),
Q — «Сверено» (когда). «Сводка» читает fact целиком ради трат, поэтому блок
берётся из того же чтения — без лишнего запроса к Google.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any

from app.domain import Rates, currency_of, same_balance
from app.services.analytics_service import SERIAL_ZERO, STALE_CHECK_DAYS
from app.services.held import HeldOnCard

logger = logging.getLogger(__name__)

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
    held: tuple[HeldOnCard, ...] = ()  # чужие деньги на карте (см. attach_held)
    held_uzs: float = (
        0.0  # чужие деньги, что остались, в сумах; перерасход не считается
    )

    @property
    def own(self) -> float | None:
        """Свои деньги на карте в её валюте; None — остатка нет (ошибка в ячейке)."""
        if self.balance is None:
            return None
        return self.balance - sum(max(0.0, item.left) for item in self.held)

    @property
    def own_uzs(self) -> float | None:
        """Свои деньги на карте в сумах; None — нет курса или остатка."""
        if self.uzs is None:
            return None
        return self.uzs - self.held_uzs

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


def attach_held(
    cards: Sequence[CardBalance], held: Sequence[HeldOnCard], rates: Rates
) -> tuple[CardBalance, ...]:
    """Карты с чужими деньгами на них: сбор к карте — по имени из fact!H.

    Сбор на карте, которой нет в блоке остатков, в лог (warning), на экран не идёт.
    """
    known = {card.name for card in cards}
    for item in held:
        if item.card not in known:
            logger.warning(
                "Held money of %s is on a card missing from the balance block: %s",
                item.article.name,
                item.card,
            )
    result = []
    for card in cards:
        mine = tuple(item for item in held if item.card == card.name)
        left = sum(
            rates.convert(item.left, item.currency, "UZS") or 0.0
            for item in mine
            if item.left > 0
        )
        result.append(replace(card, held=mine, held_uzs=left))
    return tuple(result)
