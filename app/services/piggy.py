"""Копилки: листы «Копилки» и «Журнал копилок» → копилки и их движения.

Деньги копилок лежат на долларовой карте заморозки вместе с подушкой: копилка —
пометка «сколько из этих денег на что». Журнал только дополняется: плюс — отложили,
минус — забрали; траты по статье копилки в открытом окне считаются из fact.
Без сети: на входе значения листов.
"""

import hashlib
import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd

from app.domain import INCOME_GROUP, Rates
from app.services.analytics_service import TRANSFERS_GROUP, expenses, to_day

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
# статья копилки — только траты
CLOSED_GROUPS = frozenset({INCOME_GROUP, TRANSFERS_GROUP})
BEFORE_DAYS = 60  # окно трат «каждый год» открывается за 60 дней до срока
AFTER_DAYS = 30  # и закрывается через 30 дней после


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


# --- траты из копилки и остатки (спека 4.2, 4.6) ---


@dataclass(frozen=True)
class Window:
    """Окно трат копилки: траты её статьи в эти дни идут из копилки."""

    start: date
    end: date
    deadline: date


@dataclass(frozen=True)
class Spend:
    """Трата из копилки: строка fact (ключ) и сколько ушло, в валюте копилки."""

    key: str
    day: date
    group: str
    subcategory: str
    amount: float


@dataclass(frozen=True)
class Balance:
    """Копилка на дату, в её валюте."""

    saved: float  # плюсовые строки журнала
    spent: float  # минусовые строки журнала и траты открытого окна
    left: float  # saved − spent
    collected: float  # к ближайшему сроку: остаток и то, что из него уже потратили
    spends: tuple[Spend, ...]  # траты открытого окна


def window_key(deadline: date) -> str:
    """Ключ строки журнала с итогом окна: «окно:20.12.2026»."""
    return f"окно:{deadline:%d.%m.%Y}"


def open_window(piggy: Piggy, moves: Sequence[Move]) -> Window | None:
    """Окно нынешнего срока; None — нет срока, статьи, ошибка строки или окно уже
    закрыто итогом в журнале (дальше оно из fact не пересчитывается)."""
    if piggy.error or piggy.deadline is None or not piggy.subcategory:
        return None
    if any(move.key == window_key(piggy.deadline) for move in moves):
        return None
    end = piggy.deadline + timedelta(days=AFTER_DAYS)
    if piggy.yearly:
        return Window(piggy.deadline - timedelta(days=BEFORE_DAYS), end, piggy.deadline)
    if not moves:
        return None
    return Window(min(move.day for move in moves), end, piggy.deadline)


def fact_key(
    day: date,
    group: str,
    subcategory: str,
    amount: float,
    currency: str,
    source: str,
    comment: str,
) -> str:
    """Ключ строки fact для отметки «не из копилки»: правка строки его меняет."""
    note = comment.strip().removeprefix("AI:").strip()
    return "|".join(
        (
            f"{day:%Y-%m-%d}",
            group.strip(),
            subcategory.strip(),
            f"{amount:.2f}",
            currency.strip().upper(),
            source.strip(),
            note,
        )
    )


def row_keys(frame: pd.DataFrame) -> list[str]:
    """Ключи строк фрейма по порядку; одинаковые строки — с «#2», «#3»…"""
    seen: dict[str, int] = {}
    keys: list[str] = []
    for row in frame.itertuples(index=False):
        key = fact_key(
            row.day,
            row.group,
            row.subcategory,
            row.amount,
            row.currency,
            row.source,
            row.comment,
        )
        seen[key] = seen.get(key, 0) + 1
        keys.append(key if seen[key] == 1 else f"{key}#{seen[key]}")
    return keys


def fingerprint(key: str) -> str:
    """Короткий отпечаток ключа — для кнопки «Не из копилки» в чате."""
    return hashlib.sha1(key.encode()).hexdigest()[:10]


def _article_rows(
    frame: pd.DataFrame, articles: set[tuple[str, str]], until: date
) -> list[tuple[str, Any]]:
    """Траты статей копилок по until с ключами, по дням."""
    spent = expenses(frame)
    if spent.empty or not articles:
        return []
    pairs = pd.MultiIndex.from_frame(spent[["group", "subcategory"]])
    rows = spent[pairs.isin(list(articles)) & (spent["day"] <= until).to_numpy()]
    keyed = list(zip(row_keys(rows), rows.itertuples(index=False), strict=True))
    return sorted(keyed, key=lambda item: item[1].day)


def balances(
    book: Book, frame: pd.DataFrame, rates: Rates, until: date
) -> dict[str, Balance]:
    """Остатки копилок на дату: журнал по until и траты их статей в открытом окне.

    Трата идёт копилке, в окне которой её день, с ближайшим к дню сроком; кончился
    остаток — остаток траты идёт следующей такой копилке, потом в обычные траты.
    Возврат уменьшает траты копилки; отмеченные «не из копилки» строки не идут никуда.
    """
    moves = [move for move in book.moves if move.day <= until]
    excluded = {move.key for move in moves if move.kind == NOT_FROM and move.key}
    own: dict[str, list[Move]] = {}
    for move in moves:
        own.setdefault(move.piggy_id, []).append(move)
    piggies = [piggy for piggy in book.piggies if piggy.id]
    windows: dict[str, Window] = {}
    for piggy in piggies:
        found = open_window(piggy, book.moves_of(piggy.id))
        if found is not None and found.start <= until:
            windows[piggy.id] = found
    by_id = {piggy.id: piggy for piggy in piggies}
    room = {
        piggy_id: max(sum(move.amount for move in own.get(piggy_id, [])), 0.0)
        for piggy_id in windows
    }
    drawn: dict[str, list[Spend]] = {piggy_id: [] for piggy_id in windows}
    articles = {
        (by_id[piggy_id].group or "", by_id[piggy_id].subcategory or "")
        for piggy_id in windows
    }
    for key, row in _article_rows(frame, articles, until):
        if key in excluded or NOT_FROM in row.comment.lower():
            continue
        candidates = sorted(
            (
                piggy_id
                for piggy_id, window in windows.items()
                if (by_id[piggy_id].group, by_id[piggy_id].subcategory)
                == (row.group, row.subcategory)
                and window.start <= row.day <= window.end
            ),
            key=lambda piggy_id: (
                abs((windows[piggy_id].deadline - row.day).days),
                windows[piggy_id].deadline,
            ),
        )
        if not candidates:
            continue
        if row.amount < 0:  # возврат — той же копилке, что взяла бы трату
            piggy_id = candidates[0]
            back = rates.convert(row.amount, row.currency, by_id[piggy_id].currency)
            if back is not None:
                drawn[piggy_id].append(
                    Spend(key, row.day, row.group, row.subcategory, back)
                )
                room[piggy_id] -= back
            continue
        rest = float(row.amount)
        for piggy_id in candidates:
            currency = by_id[piggy_id].currency
            wanted = rates.convert(rest, row.currency, currency)
            if wanted is None or room[piggy_id] <= 0:
                continue
            take = min(wanted, room[piggy_id])
            room[piggy_id] -= take
            drawn[piggy_id].append(
                Spend(key, row.day, row.group, row.subcategory, take)
            )
            rest -= rates.convert(take, currency, row.currency) or 0.0
            if rest <= 1e-6:
                break
    found: dict[str, Balance] = {}
    for piggy in piggies:
        mine = own.get(piggy.id, [])
        saved = sum(move.amount for move in mine if move.amount > 0)
        taken = -sum(move.amount for move in mine if move.amount < 0)
        spends = tuple(drawn.get(piggy.id, []))
        window_spent = max(sum(item.amount for item in spends), 0.0)
        window = windows.get(piggy.id)
        early = (
            -sum(
                move.amount
                for move in mine
                if move.amount < 0
                and move.kind in (SPENT, WRITE_OFF)
                and window.start <= move.day <= window.end
            )
            if window is not None
            else 0.0
        )
        left = saved - taken - window_spent
        found[piggy.id] = Balance(
            saved, taken + window_spent, left, left + window_spent + early, spends
        )
    return found
