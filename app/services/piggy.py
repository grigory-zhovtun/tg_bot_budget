"""Копилки: листы «Копилки» и «Журнал копилок» → копилки и их движения.

Деньги копилок лежат на долларовой карте заморозки вместе с подушкой: копилка —
пометка «сколько из этих денег на что». Журнал только дополняется: плюс — отложили,
минус — забрали; траты по статье копилки в открытом окне считаются из fact.
Без сети: на входе значения листов.
"""

import calendar
import hashlib
import logging
import math
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from app.domain import INCOME_GROUP, Rates
from app.services.analytics_service import TRANSFERS_GROUP, expenses, income, to_day
from app.services.day_budget import DayBudget, frozen_split, read_forecast
from app.services.google_sheets import (
    PIGGY_JOURNAL_HEADER,
    PIGGY_LIST_HEADER,
    PIGGY_WIDTH,
)
from app.services.plan import read_plan
from app.services.savings import (
    LIFE_GROUPS,
    Cushion,
    life_groups,
    life_month,
    month_savings,
    monthly_spending,
)

logger = logging.getLogger(__name__)

LIST_HEADER, JOURNAL_HEADER = PIGGY_LIST_HEADER, PIGGY_JOURNAL_HEADER

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
    order: int | None = None  # номер важности (O); None — после пронумерованных


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
    percent: float | None = None  # доля с поступлений из Q1; None — пусто или плохо
    percent_bad: bool = False  # в Q1 не число от 0 до 100
    # обязательные группы месяца жизни из S1; None — ячейка пустая (по умолчанию)
    life_groups: tuple[str, ...] | None = None

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


def _order(value: Any) -> int | None:
    """Номер важности: целое от 1; иначе — без номера."""
    number = _number(value) if _text(value) else None
    if number is None or number < 1 or number != int(number):
        return None
    return int(number)


def _percent(head: Sequence[Any]) -> tuple[float | None, bool]:
    """Q1 шапки «Копилок» → доля и «ячейка не читается»; пусто — (None, False)."""
    cell = _cells(head, PIGGY_WIDTH)[16]
    if not _text(cell):
        return None, False
    value = _number(cell)
    if value is None or not 0 <= value <= 100:
        return None, True
    return value / 100, False


def _life(head: Sequence[Any]) -> tuple[str, ...] | None:
    """S1 шапки «Копилок» → обязательные группы месяца жизни по запятым, без пробелов
    по краям и пустых; пустая ячейка — None (группы по умолчанию)."""
    cell = _text(_cells(head, PIGGY_WIDTH)[18])
    if not cell:
        return None
    return tuple(name.strip() for name in cell.split(",") if name.strip())


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
                _order(cells[14]),
            )
        )
    for unknown in sorted(by_id.keys() - seen):
        logger.warning("Piggy journal moves of unknown ID %s are left out", unknown)
    percent, bad = _percent(list_rows[0]) if list_rows else (None, False)
    if bad:
        logger.warning("Piggy percent in Q1 is not a number from 0 to 100")
    return Book(
        tuple(piggies),
        tuple(move for move in moves if move.piggy_id in seen),
        percent,
        bad,
        _life(list_rows[0]) if list_rows else None,
    )


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
    collected: float  # к сроку deadline: остаток и то, что из него уже потратили
    spends: tuple[Spend, ...]  # траты открытого окна
    deadline: date | None = None  # срок окна на дату; None — окно ещё не началось


def window_key(deadline: date) -> str:
    """Ключ строки журнала с итогом окна: «окно:20.12.2026»."""
    return f"окно:{deadline:%d.%m.%Y}"


def _window_for(piggy: Piggy, moves: Sequence[Move], deadline: date) -> Window | None:
    """Окно срока: «каждый год» — [срок − 60; срок + 30], «один раз» — [первая строка
    журнала; срок + 30] (без строк окна ещё нет)."""
    end = deadline + timedelta(days=AFTER_DAYS)
    if piggy.yearly:
        return Window(deadline - timedelta(days=BEFORE_DAYS), end, deadline)
    if not moves:
        return None
    return Window(min(move.day for move in moves), end, deadline)


def _key_deadline(key: str) -> date | None:
    """Срок из ключа итога окна «окно:20.12.2026»; другой ключ — None."""
    prefix, _, value = key.partition(":")
    if prefix != "окно":
        return None
    try:
        return datetime.strptime(value.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None


def open_window(piggy: Piggy, moves: Sequence[Move]) -> Window | None:
    """Окно нынешнего срока; None — нет срока, статьи, ошибка строки или окно уже
    закрыто итогом в журнале (дальше оно из fact не пересчитывается)."""
    if piggy.error or piggy.deadline is None or not piggy.subcategory:
        return None
    if any(move.key == window_key(piggy.deadline) for move in moves):
        return None
    return _window_for(piggy, moves, piggy.deadline)


def _window_at(piggy: Piggy, moves: Sequence[Move], until: date) -> Window | None:
    """Окно, к сроку которого копилка относится на until: нынешнего срока, если оно
    уже началось, иначе прежнего, чей итог фон записал позже until (срок «каждый
    год» уже перенесли, а на until окно ещё шло). Так остаток на прошлую дату не
    меняется оттого, что фон закрыл окно."""
    if piggy.error or piggy.deadline is None:
        return None
    current = _window_for(piggy, moves, piggy.deadline)
    if current is not None and current.start <= until:
        return current
    for move in moves:
        deadline = _key_deadline(move.key) if move.kind == WINDOW else None
        if deadline is None or move.day <= until:
            continue
        earlier = _window_for(piggy, moves, deadline)
        if earlier is not None and earlier.start <= until:
            return earlier
    return None


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
    return f"{row_prefix(day, group, subcategory, amount, currency, source)}|{note}"


def row_prefix(
    day: date, group: str, subcategory: str, amount: float, currency: str, source: str
) -> str:
    """Ключ строки fact без комментария: трата копилки — та, чей ключ с него
    начинается (комментарий Sheets мог поменять при записи)."""
    return "|".join(
        (
            f"{day:%Y-%m-%d}",
            group.strip(),
            subcategory.strip(),
            f"{amount:.2f}",
            currency.strip().upper(),
            source.strip(),
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
    cycles: dict[str, Window] = {}
    windows: dict[str, Window] = {}  # окна, чьи траты считаются из fact
    for piggy in piggies:
        found = _window_at(piggy, book.moves_of(piggy.id), until)
        if found is None:
            continue
        cycles[piggy.id] = found
        frozen = any(
            move.key == window_key(found.deadline) for move in own.get(piggy.id, [])
        )
        if piggy.subcategory and not frozen:
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
        cycle = cycles.get(piggy.id)
        # потрачено к сроку окна руками и итогом окна — «собрано» их помнит
        early = (
            -sum(
                move.amount
                for move in mine
                if move.amount < 0
                and (
                    move.kind in (SPENT, WRITE_OFF)
                    and cycle.start <= move.day <= cycle.end
                    or move.kind == WINDOW
                    and move.key == window_key(cycle.deadline)
                )
            )
            if cycle is not None
            else 0.0
        )
        left = saved - taken - window_spent
        found[piggy.id] = Balance(
            saved,
            taken + window_spent,
            left,
            left + window_spent + early,
            spends,
            cycle.deadline if cycle is not None else None,
        )
    return found


# --- план взносов, 10 %, подушка, «хватает ли» (спека 4.3–4.5, 4.8) ---

PLAN_MONTHS = 12  # на экране — год с текущего месяца
HORIZON_MONTHS = 24  # сроки «каждый год» — на два года вперёд
STARTER_MONTHS = 1.0  # пока подушки меньше чем на месяц трат — весь остаток в неё
STATUSES = ("on_track", "late", "short", "no_target", "done", "leftover", "goal",
            "broken")  # fmt: skip


@dataclass(frozen=True)
class MonthPlan:
    """Месяц плана, в сумах: взносы копилок со сроком, остаток процента подушке и
    долгим целям."""

    month: date  # первое число
    total: float  # сумма взносов копилок со сроком
    items: dict[str, float]  # ID копилки → взнос
    cushion: float
    goals: dict[str, float]  # ID долгой цели → доля
    ready: tuple[str, ...]  # копилки, собранные к сроку в этом месяце


@dataclass(frozen=True)
class PiggyState:
    """Копилка на экране; суммы — в её валюте."""

    piggy: Piggy
    balance: Balance
    status: str  # STATUSES
    this_month: float  # взнос текущего месяца по плану
    left_this_month: float  # сколько из него ещё не отложено
    short: float  # «к сроку не успеть» или процента не хватает: сколько не хватит
    leftover: float  # остаток прошлого окна
    history: tuple[Move, ...]  # последние строки журнала, новые первыми
    order: int | None = None  # номер важности 1…n у копилок порядка
    forecast: float | None = None  # будет к ближайшему сроку по плану
    now: float = 0.0  # «отложить сейчас»: часть процента пришедшего минус отложенное
    saved_month: float = 0.0  # отложено в этом месяце


@dataclass(frozen=True)
class FromPiggy:
    """Траты из копилки в месяце (спека 4.7): amount — в её валюте, uzs — в сумах."""

    piggy_id: str
    name: str
    group: str | None
    subcategory: str | None
    currency: str
    amount: float
    uzs: float


@dataclass(frozen=True)
class MonthGoal:
    """«🎯 Отложить за месяц» (спека 4.7), в сумах: взносы копилок + доля 10 % −
    траты из копилок; меньше нуля — месяц тратит накопленное."""

    month: date  # первое число
    piggies: float
    share: float
    spent: float
    total: float
    from_piggies: tuple[FromPiggy, ...]
    percent: float = 0.0  # процент с поступлений, доля


@dataclass(frozen=True)
class Enough:
    """Хватает ли: средние за полные месяцы, в сумах."""

    income: float
    save: float  # процент с поступлений от дохода
    life: float  # income − save
    ordinary: float  # траты без статей копилок «каждый год»
    gap: float  # life − ordinary: меньше нуля — не хватает


@dataclass(frozen=True)
class Screen:
    today: date
    piggies: tuple[PiggyState, ...]  # по сроку, долгие цели, потом строки с ошибкой
    plan: tuple[MonthPlan, ...]
    cushion: Cushion | None  # «Заморожено» за вычетом копилок
    frozen: float | None  # «Заморожено» на сейчас
    envelopes: float  # остатки открытых копилок на сейчас, в валюте заморозки
    over: float  # на сколько копилки больше «Заморожено»
    share_to: str  # куда идёт доля 10 % в этом месяце: cushion | split | goals
    enough: Enough | None
    goals: tuple[MonthGoal, ...] = ()  # «Отложить за месяц» по месяцам плана
    in_plan: float | None = None  # «Отложить за месяц» вкладки в сумах; нет строки
    percent: float = 0.0  # процент с поступлений, доля
    percent_source: str = "default"  # sheet | default | bad | preview
    percent_needed: float | None = None  # доля целого процента «успеть всё»
    capacity: float = 0.0  # мощность месяца E в сумах
    received: float = 0.0  # пришло с 1-го по сегодня, сумы
    due: float = 0.0  # процент от пришедшего — отложить за месяц, сумы
    cushion_now: float = 0.0  # его часть подушке, сумы
    rates: Rates | None = None  # курсы таблицы — для раскладки сумм


@dataclass(frozen=True)
class _Need:
    piggy_id: str
    deadline: date
    need: float  # в сумах
    first: bool  # ближайший срок копилки


def thousands(value: float) -> float:
    """Сумы до тысячи — так «Отложить за месяц» пишется в таблицу и в «Применить»."""
    return float(round(value / 1000) * 1000)


def goal_of(screen: Screen, month: date) -> MonthGoal | None:
    """«Отложить за месяц» месяца плана; месяца нет в плане — None."""
    first = month.replace(day=1)
    return next((goal for goal in screen.goals if goal.month == first), None)


def goal_in_plan(rows: list[list[Any]] | None, rates: Rates) -> float | None:
    """Строка «🎯 Отложить за месяц» вкладки (Q и R) в сумах; None — строки нет."""
    goal = read_plan(rows).goal if rows else None
    if goal is None:
        return None
    return rates.convert(goal.amount, goal.currency, "UZS")


def money_day(rows: list[list[Any]] | None) -> int | None:
    """День последнего поступления (+) жёлтого списка вкладки; None — их нет."""
    found = read_forecast(rows) if rows else None
    days = [item.day for item in found.items if item.amount > 0] if found else []
    return max(days, default=None)


# справочник бота для проверки строк копилок — как его отдаёт bot_catalog: валюты
# карт, группы и подкатегории
Catalog = tuple[Collection[str], Sequence[str], Mapping[str, Sequence[str]]]


def piggies_now(
    sheets: tuple[list[list[Any]], list[list[Any]]] | None,
    catalog: Catalog,
    frame: pd.DataFrame,
    rates: Rates,
    today: date,
    currency: str | None,
) -> float | None:
    """Копилки на сейчас в currency — для «Заморожено» «Сводки» и утра: листы
    «Копилки» и «Журнал копилок» по справочнику бота, журнал и траты окон по
    сегодня. Листов нет (не прочитались) или валюты заморозки нет — None."""
    if sheets is None or not currency:
        return None
    book = read_book(*sheets, *catalog)
    return piggies_total(book, balances(book, frame, rates, today), rates, currency)


def piggies_total(
    book: Book, found: Mapping[str, Balance], rates: Rates, currency: str
) -> float:
    """Остатки открытых копилок (found — balances на дату) в currency по курсу
    таблицы, до цента: деньги копилок лежат на карте заморозки вместе с подушкой."""
    return round(
        sum(
            rates.convert(found[piggy.id].left, piggy.currency, currency) or 0.0
            for piggy in book.piggies
            if piggy.closed is None and piggy.id in found
        ),
        2,
    )


def _income(rows: list[list[Any]] | None) -> float:
    """Поступления (+) списка вкладки в сумах."""
    found = read_forecast(rows) if rows else None
    return sum(item.uzs for item in found.items if item.amount > 0) if found else 0.0


def _add_months(month: date, count: int) -> date:
    index = month.year * 12 + month.month - 1 + count
    return date(index // 12, index % 12 + 1, 1)


def next_year(day: date) -> date:
    try:
        return day.replace(year=day.year + 1)
    except ValueError:  # 29 февраля
        return day.replace(year=day.year + 1, day=28)


def _money_date(month: date, day: int | None) -> date:
    last = calendar.monthrange(month.year, month.month)[1]
    return month.replace(day=min(day or last, last))


def _up(value: float, currency: str) -> float:
    """Вверх: сумы — до тысячи, остальные валюты — до целых."""
    step = 1000 if currency == "UZS" else 1
    return float(math.ceil(value / step - 1e-6) * step) if value > 0 else 0.0


def _schedule(
    money: Sequence[date], needs: Sequence[_Need]
) -> tuple[list[dict[int, float]], list[list[int]]]:
    """Взносы по месяцам ступеньками: наименьшая ровная сумма, которой хватает на
    все сроки, держится до самого тесного срока, потом — то же для остальных;
    внутри месяца — ближайший срок первым. Возвращает взносы и собранные сроки
    (индексы needs) по месяцам."""
    order = sorted(range(len(needs)), key=lambda k: (needs[k].deadline, k))
    left = {k: needs[k].need for k in order}
    alloc: list[dict[int, float]] = [{} for _ in money]
    ready: list[list[int]] = [[] for _ in money]
    first = done = 0
    while done < len(order) and first < len(money):
        best: tuple[float, int, int] | None = None
        total = 0.0
        for position in range(done, len(order)):
            k = order[position]
            total += left[k]
            count = sum(
                1 for i in range(first, len(money)) if money[i] <= needs[k].deadline
            )
            if count and (best is None or total / count >= best[0] - 1e-9):
                best = (total / count, position, count)
        if best is None:
            break
        rate, last, count = best
        for i in range(first, first + count):
            budget = rate
            for position in range(done, last + 1):
                k = order[position]
                take = min(budget, left[k])
                if take > 1e-9:
                    alloc[i][k] = alloc[i].get(k, 0.0) + take
                    left[k] -= take
                    budget -= take
                    if left[k] <= 1e-6:
                        ready[i].append(k)
                if budget <= 1e-9:
                    break
        first, done = first + count, last + 1
    return alloc, ready


def ordered_piggies(
    piggies: Sequence[Piggy], order: Sequence[str] | None
) -> list[Piggy]:
    """Порядок важности: ID предпросмотра — первыми в их порядке; потом с номером в
    O — по номеру (равные — по строке листа); потом без номера — по сроку."""
    position = {piggy_id: index for index, piggy_id in enumerate(order or ())}

    def key(piggy: Piggy) -> tuple[int, int, date, int]:
        if piggy.id in position:
            return (0, position[piggy.id], date.min, piggy.row)
        if piggy.order is not None:
            return (1, piggy.order, date.min, piggy.row)
        return (2, 0, piggy.deadline or date.max, piggy.row)

    return sorted(piggies, key=key)


def _funded(
    money: Sequence[date], capacity: float, needs: Sequence[_Need]
) -> list[float]:
    """«Успеет к сроку» по очереди: нужде — меньшее из неё и свободного места.
    Место срока d — capacity × месяцы денег до d минус отданное нуждам со сроком не
    позже d; проверяются её срок и все уже разобранные более поздние: так важная
    нужда берёт своё первой, а менее важная не отнимает у неё."""
    taken: list[tuple[date, float]] = []
    funded: list[float] = []
    for need in needs:
        checks = [need.deadline] + [day for day, _ in taken if day > need.deadline]
        free = min(
            capacity * sum(1 for paid in money if paid <= check)
            - sum(amount for day, amount in taken if day <= check)
            for check in checks
        )
        got = max(min(need.need, free), 0.0)
        taken.append((need.deadline, got))
        funded.append(got)
    return funded


def _first_step(money: Sequence[date], needs: Sequence[_Need]) -> float:
    """Первая ступенька плана от полных нужд — наименьшая ровная сумма в месяц,
    которой хватает на все сроки."""
    best, total = 0.0, 0.0
    for need in sorted(needs, key=lambda item: item.deadline):
        total += need.need
        count = sum(1 for paid in money if paid <= need.deadline)
        if count:
            best = max(best, total / count)
    return best


def _average_income(frame: pd.DataFrame, today: date) -> float:
    """Средний доход за полные месяцы с данными — как в «Хватает ли»."""
    months = [
        item for item in month_savings(frame, today).months if item.spent or item.income
    ]
    return sum(item.income for item in months) / len(months) if months else 0.0


def _saved_in(moves: Sequence[Move], month: date) -> float:
    """Отложено за месяц: «отложил» и «исправление» в обе стороны, не меньше нуля."""
    return max(
        sum(
            move.amount
            for move in moves
            if move.kind in (SAVED, FIX)
            and (move.day.year, move.day.month) == (month.year, month.month)
        ),
        0.0,
    )


def _deadline_now(piggy: Piggy, today: date) -> date | None:
    """Срок, к которому копят сейчас: окно «каждый год» прошло, а фон ещё не
    перенёс срок, — следующий год."""
    if piggy.deadline is None:
        return None
    if piggy.yearly and today > piggy.deadline + timedelta(days=AFTER_DAYS):
        return next_year(piggy.deadline)
    return piggy.deadline


def _towards(balance: Balance, deadline: date | None) -> float:
    """Собрано к сроку: с тратами его окна, если остаток посчитан к этому сроку."""
    if deadline is not None and balance.deadline == deadline:
        return balance.collected
    return balance.left


def _leftover(moves: Sequence[Move], left: float) -> float:
    """Остаток после последнего итога окна, пока после него ничего не отложили."""
    last = max(
        (index for index, move in enumerate(moves) if move.kind == WINDOW), default=None
    )
    if last is None or any(move.kind == SAVED for move in moves[last + 1 :]):
        return 0.0
    return max(left, 0.0)


def _enough(
    frame: pd.DataFrame,
    today: date,
    yearly: set[tuple[str, str]],
    share: float,
) -> Enough | None:
    months = [
        item for item in month_savings(frame, today).months if item.spent or item.income
    ]
    if not months:
        return None
    count = len(months)
    income = sum(item.income for item in months) / count
    spent = expenses(frame)
    firsts = spent["day"].map(lambda day: day.replace(day=1))
    pairs = pd.MultiIndex.from_frame(spent[["group", "subcategory"]])
    chosen = {item.month for item in months}
    article = spent[pairs.isin(list(yearly)) & firsts.isin(list(chosen)).to_numpy()]
    ordinary = (
        sum(item.spent for item in months) - float(article["amount_uzs"].sum())
    ) / count
    save = share * income
    life = income - save
    return Enough(income, save, life, ordinary, life - ordinary)


def savings_screen(
    book: Book,
    frame: pd.DataFrame,
    rows: list[list[Any]] | None,
    today: date,
    rates: Rates,
    budget: DayBudget | None,
    share: float,
    cushion_months: int,
    *,
    percent: float | None = None,
    order: Sequence[str] | None = None,
    groups: Collection[str],
) -> Screen:
    """«Накопления» (раздел «Плана»): копилки, план взносов на год, подушка и «хватает ли».

    share — процент с поступлений по умолчанию; Q1 листа его заменяет, а percent и
    order — предпросмотр окон «Процент» и «Порядок» (ничего не пишется). groups —
    справочник групп бота: из него обязательные группы месяца жизни подушки — какие
    названы в S1 листа (пусто — по умолчанию)."""
    month = today.replace(day=1)
    before = month - timedelta(days=1)
    start = balances(book, frame, rates, before)  # план месяца — от остатков на 1-е
    now = balances(book, frame, rates, today)
    open_piggies = [piggy for piggy in book.piggies if piggy.closed is None]
    valid = [piggy for piggy in open_piggies if piggy.id and not piggy.error]
    scheduled = [
        piggy
        for piggy in valid
        if piggy.deadline is not None and piggy.target is not None
    ]
    goals = [piggy for piggy in valid if piggy.deadline is None]
    if percent is not None:
        portion, source = percent, "preview"
    elif book.percent is not None:
        portion, source = book.percent, "sheet"
    else:
        portion, source = share, "bad" if book.percent_bad else "default"
    base = _income(rows) or _average_income(frame, today)
    capacity = portion * base
    # месяц жизни — по обязательным группам из S1 листа (пусто — по умолчанию)
    chosen = LIFE_GROUPS if book.life_groups is None else book.life_groups
    life = life_month(frame, today, life_groups(groups, chosen))

    # сроки и их нужда в сумах — слоями: ближайшие сроки по порядку важности, потом
    # сроки следующего года в том же порядке
    horizon = _add_months(month, HORIZON_MONTHS)
    pay_day = money_day(rows)
    ranked = ordered_piggies(
        [
            piggy
            for piggy in scheduled
            if piggy.yearly
            or piggy.deadline is None
            or today <= piggy.deadline + timedelta(days=AFTER_DAYS)
        ],  # окно «один раз» прошло: копилка ждёт решения об остатке
        order,
    )
    layers: list[list[_Need]] = []
    start_collected: dict[str, float] = {}
    late: dict[str, float] = {}
    for piggy in ranked:
        rate = rates.to_uzs.get(piggy.currency)
        if not rate or piggy.deadline is None or piggy.target is None:
            continue
        deadline = _deadline_now(piggy, today) or piggy.deadline
        collected = _towards(start[piggy.id], deadline)
        start_collected[piggy.id] = collected
        own = [
            _Need(piggy.id, deadline, max(piggy.target - collected, 0.0) * rate, True)
        ]
        if piggy.yearly:
            following = next_year(deadline)
            while following < horizon:
                own.append(_Need(piggy.id, following, piggy.target * rate, False))
                following = next_year(following)
        for index, need in enumerate(own):
            if len(layers) <= index:
                layers.append([])
            layers[index].append(need)
    needs = [need for layer in layers for need in layer]
    last = max((need.deadline for need in needs), default=month)
    span = max(
        PLAN_MONTHS, (last.year - month.year) * 12 + last.month - month.month + 1
    )
    months = [_add_months(month, index) for index in range(span)]
    money = [_money_date(item, pay_day) for item in months]
    for need in needs:
        if (
            need.first
            and need.need > 0
            and not any(day <= need.deadline for day in money)
        ):
            late[need.piggy_id] = need.need
    planned = [
        need
        for need in needs
        if need.need > 1e-6 and not (need.first and need.piggy_id in late)
    ]
    funded = _funded(money, capacity, planned)
    firsts = {
        need.piggy_id: (need.need, got)
        for need, got in zip(planned, funded, strict=True)
        if need.first
    }
    covered = [
        (replace(need, need=got), got >= need.need - 1e-6)
        for need, got in zip(planned, funded, strict=True)
        if got > 1e-6
    ]
    alloc, ready = _schedule(money, [need for need, _ in covered])
    expected: dict[tuple[str, date], float] = {}  # уйдёт к сроку, в валюте копилки
    targets = {piggy.id: piggy for piggy in ranked}
    for need, got in zip(planned, funded, strict=True):
        piggy = targets[need.piggy_id]
        rate = rates.to_uzs.get(piggy.currency, 0.0)
        if not rate or piggy.target is None:
            continue
        if need.first:
            have = start_collected[need.piggy_id] + got / rate
            expected[(need.piggy_id, need.deadline)] = min(have, piggy.target)
        else:
            expected[(need.piggy_id, need.deadline)] = got / rate
    step = _first_step(money, planned)
    percent_needed = (
        math.ceil(step / base * 100 - 1e-9) / 100 if base > 0 and step > 1e-6 else None
    )

    # доля 10 %: по подушке на 1-е число, дальше подушка растёт на свою часть; до
    # этапа 4 — в месяцах всех трат, как раньше: план посреди месяца не двигается
    monthly = monthly_spending(frame, today)
    currency = budget.frozen_currency if budget is not None else None
    frozen_rate = rates.to_uzs.get(currency or "", 0.0)
    cushion_uzs: float | None = None
    if (
        budget is not None
        and budget.frozen is not None
        and budget.frozen_start is not None
        and currency
        and frozen_rate
    ):
        envelopes_start = piggies_total(book, start, rates, currency)
        cushion_uzs = (budget.frozen_start - envelopes_start) * frozen_rate
    collected_goals = {
        piggy.id: start[piggy.id].collected * rates.to_uzs.get(piggy.currency, 0.0)
        for piggy in goals
    }
    plan: list[MonthPlan] = []
    share_to = "cushion"
    for index, first_day in enumerate(months[:PLAN_MONTHS]):
        items: dict[str, float] = {}
        for k, amount in alloc[index].items():
            piggy_id = covered[k][0].piggy_id
            items[piggy_id] = items.get(piggy_id, 0.0) + amount
        rest = max(capacity - sum(items.values()), 0.0)  # остаток процента месяца
        cover = (
            cushion_uzs / monthly if cushion_uzs is not None and monthly > 0 else None
        )
        active = [
            piggy
            for piggy in goals
            if piggy.target is None
            or collected_goals[piggy.id]
            < piggy.target * rates.to_uzs.get(piggy.currency, 0.0)
        ]
        if cover is None or cover < STARTER_MONTHS or not active:
            to_goals = 0.0
        elif cover < cushion_months:
            to_goals = rest / 2
        else:
            to_goals = rest
        if index == 0:
            share_to = (
                "cushion" if to_goals == 0 else "goals" if to_goals >= rest else "split"
            )
        each = to_goals / len(active) if active else 0.0
        parts: dict[str, float] = {}
        for piggy in active:  # цели не дают больше, чем ей не хватает
            rate = rates.to_uzs.get(piggy.currency, 0.0)
            wanted = (
                piggy.target * rate - collected_goals[piggy.id]
                if piggy.target is not None
                else each
            )
            part = min(each, max(wanted, 0.0))
            if part > 0:  # цель без доли в месяце не называют
                parts[piggy.id] = part
                collected_goals[piggy.id] += part
        to_goals = sum(parts.values())
        if cushion_uzs is not None:
            cushion_uzs += rest - to_goals
        plan.append(
            MonthPlan(
                first_day,
                sum(items.values()),
                items,
                rest - to_goals,
                parts,
                tuple(  # собрана — только если «успеет» всю нужду
                    dict.fromkeys(
                        covered[k][0].piggy_id for k in ready[index] if covered[k][1]
                    )
                ),
            )
        )

    # копилки на экране
    states: list[PiggyState] = []
    numbers = {piggy.id: index for index, piggy in enumerate(ranked, start=1)}
    for piggy in open_piggies:
        moves = book.moves_of(piggy.id) if piggy.id else ()
        balance = now.get(piggy.id, Balance(0.0, 0.0, 0.0, 0.0, ()))
        balance = replace(
            balance, collected=_towards(balance, _deadline_now(piggy, today))
        )
        rate = rates.to_uzs.get(piggy.currency, 0.0)
        planned_now = plan[0].items.get(piggy.id, 0.0) + plan[0].goals.get(
            piggy.id, 0.0
        )
        this_month = _up(planned_now / rate, piggy.currency) if rate else 0.0
        saved = _saved_in(moves, month)
        left_month = max(this_month - saved, 0.0)
        status, short, leftover = "on_track", 0.0, _leftover(moves, balance.left)
        if piggy.error or not piggy.id:
            status = "broken"
        elif piggy.deadline is None:
            reached = piggy.target is not None and balance.collected >= piggy.target
            status = "done" if reached else "goal"
        elif piggy.target is None:
            status = "no_target"
        elif not piggy.yearly and today > piggy.deadline + timedelta(days=AFTER_DAYS):
            # окно «один раз» прошло: остаток ждёт решения, иначе копилку закрыть
            if balance.left > 0:
                status, leftover = "leftover", balance.left
            else:
                status = "done"
        elif balance.collected >= piggy.target:
            status = "done"
        elif piggy.id in late:
            status, short = "late", (late[piggy.id] / rate if rate else 0.0)
        elif (
            rate
            and piggy.id in firsts
            and firsts[piggy.id][1] < (firsts[piggy.id][0] - 1e-6)
        ):  # процента на этот срок не хватает
            lacking = firsts[piggy.id][0] - firsts[piggy.id][1]
            status, short = "short", _up(lacking / rate, piggy.currency)
        forecast = None
        if piggy.id in start_collected and rate:
            got = firsts.get(piggy.id, (0.0, 0.0))[1]
            forecast = start_collected[piggy.id] + got / rate
        states.append(
            PiggyState(
                piggy,
                balance,
                status,
                this_month,
                left_month,
                short,
                leftover,
                tuple(reversed(moves[-5:])),
                numbers.get(piggy.id) if status != "broken" else None,
                forecast,
                saved_month=saved,
            )
        )
    states.sort(  # порядок важности, другие со сроком, долгие цели, с ошибкой
        key=lambda item: (
            (
                3
                if item.status == "broken"
                else (
                    0
                    if item.order is not None
                    else 2 if item.piggy.deadline is None else 1
                )
            ),
            item.order or 0,
            item.piggy.deadline or date.max,
            item.piggy.row,
        )
    )

    # подушка — на сейчас, как «Заморожено»: без копилок, в месяцах жизни
    cushion, envelopes, over = None, 0.0, 0.0
    frozen = budget.frozen if budget is not None else None
    if budget is not None and frozen is not None and currency and frozen_rate:
        envelopes = piggies_total(book, now, rates, currency)
        kept, over, lacking = frozen_split(frozen, envelopes, budget.frozen_total)
        cushion = Cushion(
            kept,
            currency,
            kept * frozen_rate,
            frozen_rate,
            life.amount,
            cushion_months,
            frozen=frozen,
            card=budget.frozen_total,
            payments=budget.frozen_payments,
            piggies=envelopes,
            over=over,
            lacking=lacking,
            life=life,
        )
    yearly = {
        (piggy.group or "", piggy.subcategory or "")
        for piggy in scheduled
        if piggy.yearly and piggy.subcategory
    }
    came = income(frame)
    came = came[
        (came["day"] >= month) & (came["day"] <= today) & (came["amount_uzs"] > 0)
    ]
    received = float(came["amount_uzs"].sum())
    due = portion * received
    screen = Screen(
        today,
        tuple(states),
        tuple(plan),
        cushion,
        frozen,
        envelopes,
        over,
        share_to,
        _enough(frame, today, yearly, portion),
        _month_goals(plan, states, book, today, rates, late, expected, portion),
        goal_in_plan(rows, rates),
        portion,
        source,
        percent_needed,
        capacity,
        received,
        due,
        0.0,
        rates,
    )
    parts = parts_of(screen, due, after_saved=False)
    return replace(
        screen,
        piggies=tuple(
            replace(state, now=_now_of(state, parts.get(state.piggy.id, 0.0), rates))
            for state in screen.piggies
        ),
        cushion_now=max(due - sum(parts.values()), 0.0),
    )


def _now_of(state: PiggyState, part: float, rates: Rates) -> float:
    """«Отложить сейчас»: часть пришедшего (сумы) в валюте копилки минус отложенное
    с 1-го, вверх."""
    rate = rates.to_uzs.get(state.piggy.currency)
    if not rate:
        return 0.0
    value = part / rate - state.saved_month
    return _up(value, state.piggy.currency) if value > 1e-9 else 0.0


def _spent_in_month(state: PiggyState, moves: Sequence[Move], month: date) -> float:
    """Ушло из копилки в месяце по сегодня: траты окна из fact и «потратил» /
    «списал остаток» журнала, в валюте копилки."""

    def inside(day: date) -> bool:
        return (day.year, day.month) == (month.year, month.month)

    from_fact = sum(item.amount for item in state.balance.spends if inside(item.day))
    by_hand = -sum(
        move.amount
        for move in moves
        if move.amount < 0 and move.kind in (SPENT, WRITE_OFF) and inside(move.day)
    )
    return from_fact + by_hand


def _month_goals(
    plan: Sequence[MonthPlan],
    states: Sequence[PiggyState],
    book: Book,
    today: date,
    rates: Rates,
    late: Mapping[str, float],
    expected: Mapping[tuple[str, date], float] | None = None,
    percent: float = 0.0,
) -> tuple[MonthGoal, ...]:
    """«Отложить за месяц» для каждого месяца плана (спека 4.7).

    Траты из копилок: в текущем месяце — уже ушедшее в нём; у копилки со сроком в
    месяце — ещё и не потраченная часть «собрано к сроку» (цель, у «к сроку не
    успеть» — собранное). Траты в окне до месяца срока заранее не угадать.
    """
    current = today.replace(day=1)
    last = plan[-1].month if plan else current
    goals: list[MonthGoal] = []
    for month_plan in plan:
        month = month_plan.month
        found: list[FromPiggy] = []
        for state in states:
            piggy = state.piggy
            rate = rates.to_uzs.get(piggy.currency)
            if state.status == "broken" or not piggy.id or not rate:
                continue
            amount = 0.0
            if month == current:
                amount += _spent_in_month(state, book.moves_of(piggy.id), month)
            first = _deadline_now(piggy, today)
            if first is not None and not (
                not piggy.yearly and today > first + timedelta(days=AFTER_DAYS)
            ):
                deadline: date | None = first
                while deadline is not None and deadline.replace(day=1) <= last:
                    if deadline.replace(day=1) == month:
                        now = deadline == first  # траты окна этого срока уже были
                        if now and piggy.id in late:
                            goes = state.balance.collected
                        elif expected and (piggy.id, deadline) in expected:
                            goes = expected[(piggy.id, deadline)]  # «успеет к сроку»
                        else:
                            goes = piggy.target or 0.0
                        spent = (
                            state.balance.collected - state.balance.left if now else 0.0
                        )
                        amount += max(goes - spent, 0.0)
                    deadline = next_year(deadline) if piggy.yearly else None
            if amount > 1e-6:
                found.append(
                    FromPiggy(
                        piggy.id,
                        piggy.name,
                        piggy.group,
                        piggy.subcategory,
                        piggy.currency,
                        amount,
                        amount * rate,
                    )
                )
        share = month_plan.cushion + sum(month_plan.goals.values())
        spent_uzs = sum(item.uzs for item in found)
        goals.append(
            MonthGoal(
                month,
                month_plan.total,
                share,
                spent_uzs,
                month_plan.total + share - spent_uzs,
                tuple(found),
                percent,
            )
        )
    return tuple(goals)


def parts_of(screen: Screen, amount: float, *, after_saved: bool) -> dict[str, float]:
    """Раскладка суммы в сумах: копилкам порядка — до взноса месяца, копилкам
    «short» — до «не хватит», долгим целям — до их доли; остаток — подушке, в
    словарь не входит. after_saved — пределы за вычетом отложенного с 1-го."""
    rates = screen.rates
    if rates is None or amount <= 0:
        return {}
    rest = amount
    parts: dict[str, float] = {}

    def give(state: PiggyState, cap: float) -> None:
        nonlocal rest
        rate = rates.to_uzs.get(state.piggy.currency)
        if not rate or cap <= 1e-9 or rest <= 1e-9:
            return
        take = min(cap * rate, rest)
        parts[state.piggy.id] = parts.get(state.piggy.id, 0.0) + take
        rest -= take

    ordered = [
        state
        for state in screen.piggies
        if state.order is not None and state.status != "broken"
    ]
    for state in ordered:  # взносы месяца по порядку
        give(state, state.left_this_month if after_saved else state.this_month)
    for state in ordered:  # лишнее — тем, кто к сроку не успевает
        if state.status == "short":
            ahead = max(state.saved_month - state.this_month, 0.0)
            give(state, state.short - (ahead if after_saved else 0.0))
    for state in screen.piggies:  # долгим целям — их доля
        if state.piggy.deadline is None and state.status != "broken":
            give(state, state.left_this_month if after_saved else state.this_month)
    return parts


def income_cushion(screen: Screen, amount: float) -> float:
    """Часть подушки одного поступления (сумы); экран — уже с ним: его процент
    минус то, на сколько поступление увеличило части копилок и долгих целей."""
    share = screen.percent * amount
    before = parts_of(screen, max(screen.due - share, 0.0), after_saved=False)
    after = parts_of(screen, screen.due, after_saved=False)
    return max(share - (sum(after.values()) - sum(before.values())), 0.0)


def distribute(
    screen: Screen, amount: float, currency: str, rates: Rates
) -> list[tuple[str, float]]:
    """«Отложил…» одной суммой: раскладка parts_of за вычетом отложенного с 1-го —
    в валютах копилок; остальное — подушке, его не пишут."""
    total = rates.convert(amount, currency, "UZS")
    if total is None or total <= 0:
        return []
    named = {state.piggy.id: state.piggy for state in screen.piggies}
    chosen: list[tuple[str, float]] = []
    for piggy_id, uzs in parts_of(screen, total, after_saved=True).items():
        piggy = named[piggy_id]
        rate = rates.to_uzs.get(piggy.currency)
        if not rate:
            continue
        digits = 0 if piggy.currency == "UZS" else 2
        chosen.append((piggy_id, round(uzs / rate, digits)))
    return chosen
