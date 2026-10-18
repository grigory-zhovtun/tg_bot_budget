"""Копилки: листы «Копилки» и «Журнал копилок» → копилки и их движения.

Деньги копилок лежат на долларовой карте заморозки вместе с подушкой: копилка —
пометка «сколько из этих денег на что». Журнал только дополняется: плюс — отложили,
минус — забрали; траты по статье копилки в открытом окне считаются из fact.
Без сети: на входе значения листов.
"""

import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from app.domain import INCOME_GROUP
from app.services.analytics_service import TRANSFERS_GROUP, to_day

logger = logging.getLogger(__name__)

LIST_HEADER = [
    "ID",
    "Копилка",
    "Цель",
    "Валюта",
    "Срок",
    "Повтор",
    "Группа",
    "Подкатегория",
    "Отложено",
    "Потрачено",
    "Осталось",
    "В этом месяце",
    "Кто",
    "Закрыта",
]
JOURNAL_HEADER = ["Дата", "ID", "Копилка", "Сумма", "Валюта", "Что", "Кто", "Ключ"]

ONCE, YEARLY = "один раз", "каждый год"
SAVED = "отложил"
SPENT = "потратил"
WINDOW = "потрачено в окне"
WRITE_OFF = "списал остаток"
CLOSED = "закрыта"
FIX = "исправление"
NOT_FROM = "не из копилки"
CLOSED_GROUPS = frozenset(
    {INCOME_GROUP, TRANSFERS_GROUP}
)  # статья копилки — только траты


@dataclass(frozen=True)
class Piggy:
    """Строка листа «Копилки»; error — что не так (строка в план не идёт)."""

    id: str
    row: int  # номер строки листа
    name: str
    target: float | None  # None — цель не задана
    currency: str
    deadline: date | None  # None — долгая цель без срока
    yearly: bool
    group: str | None  # статья трат копилки; None — без статьи
    subcategory: str | None
    author: str
    closed: date | None
    error: str | None = None


@dataclass(frozen=True)
class Move:
    """Строка журнала: сумма в валюте копилки, плюс — отложили, минус — забрали."""

    day: date
    piggy_id: str
    name: str
    amount: float
    currency: str
    kind: str
    who: str
    key: str


@dataclass(frozen=True)
class Book:
    piggies: tuple[Piggy, ...]
    moves: tuple[Move, ...]  # в порядке журнала, только у копилок из списка

    def moves_of(self, piggy_id: str) -> tuple[Move, ...]:
        return tuple(move for move in self.moves if move.piggy_id == piggy_id)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    text = _text(value).replace(" ", "").replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _cells(row: Sequence[Any], width: int) -> list[Any]:
    cells = list(row[:width])
    return cells + [""] * (width - len(cells))


def _moves(rows: Sequence[Sequence[Any]]) -> list[Move]:
    found: list[Move] = []
    for number, row in enumerate(rows[1:], start=2):
        cells = _cells(row, len(JOURNAL_HEADER))
        day, piggy_id, amount = to_day(cells[0]), _text(cells[1]), _number(cells[3])
        if day is None or not piggy_id or amount is None:
            if any(_text(cell) for cell in cells):
                logger.warning("Piggy journal row %s is skipped: %s", number, cells)
            continue
        found.append(
            Move(
                day,
                piggy_id,
                _text(cells[2]),
                amount,
                _text(cells[4]).upper(),
                _text(cells[5]).lower(),
                _text(cells[6]),
                _text(cells[7]),
            )
        )
    return found


def _problem(
    cells: list[Any],
    moves: Sequence[Move],
    currencies: Collection[str],
    groups: Collection[str],
    subcategories: Mapping[str, Sequence[str]],
) -> str | None:
    """Что не так со строкой списка; None — строка в порядке."""
    target, currency = _text(cells[2]), _text(cells[3]).upper()
    deadline, repeat = _text(cells[4]), _text(cells[5]).lower()
    group, sub = _text(cells[6]), _text(cells[7])
    if not _text(cells[0]):
        return "нет ID"
    if not _text(cells[1]):
        return "нет названия"
    if currency not in currencies:
        return f"валюты «{currency}» нет у карт"
    amount = _number(cells[2]) if target else None
    if target and (amount is None or amount <= 0):
        return "цель — не число больше нуля"
    if deadline and to_day(cells[4]) is None:
        return "срок — не дата"
    if repeat and repeat not in (ONCE, YEARLY):
        return "повтор — не «один раз» и не «каждый год»"
    if repeat == YEARLY and not deadline:
        return "«каждый год» без срока"
    if (group or sub) and (
        group not in groups
        or group in CLOSED_GROUPS
        or sub not in subcategories.get(group, [])
    ):
        return "статьи нет среди трат справочника"
    if any(move.currency and move.currency != currency for move in moves):
        return "валюта не та, что у строк журнала"
    return None


def read_book(
    list_rows: list[list[Any]],
    journal_rows: list[list[Any]],
    currencies: Collection[str],
    groups: Collection[str],
    subcategories: Mapping[str, Sequence[str]],
) -> Book:
    """Значения листов (UNFORMATTED, первая строка — шапка) → копилки и движения.

    Строка списка с ошибкой остаётся копилкой с error: её покажет приложение с номером
    строки, а остаток по журналу всё равно вычтется из подушки. Строка с ID, который
    уже был выше (скопировали руками), пропускается; движения копилок, которых нет в
    списке, — тоже: их деньги возвращаются в подушку.
    """
    moves = _moves(journal_rows)
    by_id: dict[str, list[Move]] = {}
    for move in moves:
        by_id.setdefault(move.piggy_id, []).append(move)
    piggies: list[Piggy] = []
    seen: set[str] = set()
    for number, row in enumerate(list_rows[1:], start=2):
        cells = _cells(row, len(LIST_HEADER))
        if not any(_text(cell) for cell in cells):
            continue
        piggy_id = _text(cells[0])
        if piggy_id in seen:
            logger.warning(
                "Piggy row %s repeats ID %s and is skipped", number, piggy_id
            )
            continue
        if piggy_id:
            seen.add(piggy_id)
        error = _problem(
            cells, by_id.get(piggy_id, []), currencies, groups, subcategories
        )
        if error:
            logger.warning("Piggy row %s: %s", number, error)
        target = _number(cells[2]) if _text(cells[2]) else None
        deadline = to_day(cells[4]) if _text(cells[4]) else None
        piggies.append(
            Piggy(
                piggy_id,
                number,
                _text(cells[1]),
                target,
                _text(cells[3]).upper(),
                deadline,
                _text(cells[5]).lower() == YEARLY,
                _text(cells[6]) or None,
                _text(cells[7]) or None,
                _text(cells[12]),
                to_day(cells[13]) if _text(cells[13]) else None,
                error,
            )
        )
    for unknown in sorted(by_id.keys() - seen):
        logger.warning("Piggy journal moves of unknown ID %s are left out", unknown)
    return Book(tuple(piggies), tuple(move for move in moves if move.piggy_id in seen))
