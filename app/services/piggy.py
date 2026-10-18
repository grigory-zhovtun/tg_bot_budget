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
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd

from app.domain import INCOME_GROUP, Rates
from app.services.analytics_service import TRANSFERS_GROUP, expenses, to_day
from app.services.day_budget import DayBudget, read_forecast
from app.services.google_sheets import PIGGY_JOURNAL_HEADER, PIGGY_LIST_HEADER
from app.services.savings import Cushion, month_savings, monthly_spending

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


# --- план взносов, 10 %, подушка, «хватает ли» (спека 4.3–4.5, 4.8) ---

PLAN_MONTHS = 12  # на экране — год с текущего месяца
HORIZON_MONTHS = 24  # сроки «каждый год» — на два года вперёд
STARTER_MONTHS = 1.0  # пока подушки меньше чем на месяц трат — вся доля 10 % в неё
STATUSES = ("on_track", "late", "no_target", "done", "leftover", "goal", "broken")


@dataclass(frozen=True)
class MonthPlan:
    """Месяц плана, в сумах: взносы копилок со сроком, доля 10 % подушке и целям."""

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
    short: float  # «к сроку не успеть»: сколько не хватает
    leftover: float  # остаток прошлого окна
    history: tuple[Move, ...]  # последние строки журнала, новые первыми


@dataclass(frozen=True)
class Enough:
    """Хватает ли: средние за полные месяцы, в сумах."""

    income: float
    save: float  # взносы копилок месяца и доля 10 % от дохода
    life: float  # income − save
    ordinary: float  # траты без статей копилок «каждый год»
    gap: float  # life − ordinary: меньше нуля — не хватает


@dataclass(frozen=True)
class Screen:
    today: date
    piggies: tuple[PiggyState, ...]  # по сроку, долгие цели, потом строки с ошибкой
    plan: tuple[MonthPlan, ...]
    cushion: Cushion | None  # «Заморожено» за вычетом копилок
    frozen: float | None  # «Заморожено» на вечер вчера
    envelopes: float  # остатки копилок на вечер вчера, в валюте заморозки
    over: float  # на сколько копилки больше «Заморожено»
    share_to: str  # куда идёт доля 10 % в этом месяце: cushion | split | goals
    enough: Enough | None


@dataclass(frozen=True)
class _Need:
    piggy_id: str
    deadline: date
    need: float  # в сумах
    first: bool  # ближайший срок копилки


def money_day(rows: list[list[Any]] | None) -> int | None:
    """День последнего поступления (+) жёлтого списка вкладки; None — их нет."""
    found = read_forecast(rows) if rows else None
    days = [item.day for item in found.items if item.amount > 0] if found else []
    return max(days, default=None)


def _income(rows: list[list[Any]] | None) -> float:
    """Поступления (+) списка вкладки в сумах."""
    found = read_forecast(rows) if rows else None
    return sum(item.uzs for item in found.items if item.amount > 0) if found else 0.0


def _add_months(month: date, count: int) -> date:
    index = month.year * 12 + month.month - 1 + count
    return date(index // 12, index % 12 + 1, 1)


def _next_year(day: date) -> date:
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


def _saved_in(moves: Sequence[Move], month: date) -> float:
    return sum(
        move.amount
        for move in moves
        if move.amount > 0
        and (move.day.year, move.day.month) == (month.year, month.month)
    )


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
    piggies_now: float,
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
    save = piggies_now + share * income
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
) -> Screen:
    """Вкладка «Накопления»: копилки, план взносов на год, подушка и «хватает ли»."""
    month = today.replace(day=1)
    before = month - timedelta(days=1)
    start = balances(book, frame, rates, before)  # план месяца — от остатков на 1-е
    now = balances(book, frame, rates, today)
    night = balances(book, frame, rates, today - timedelta(days=1))
    open_piggies = [piggy for piggy in book.piggies if piggy.closed is None]
    valid = [piggy for piggy in open_piggies if piggy.id and not piggy.error]
    scheduled = [
        piggy
        for piggy in valid
        if piggy.deadline is not None and piggy.target is not None
    ]
    goals = [piggy for piggy in valid if piggy.deadline is None]

    # сроки и их нужда в сумах
    horizon = _add_months(month, HORIZON_MONTHS)
    pay_day = money_day(rows)
    needs: list[_Need] = []
    late: dict[str, float] = {}
    for piggy in scheduled:
        rate = rates.to_uzs.get(piggy.currency)
        if not rate or piggy.deadline is None or piggy.target is None:
            continue
        deadline = piggy.deadline
        passed = today > deadline + timedelta(days=AFTER_DAYS)
        if passed and not piggy.yearly:
            continue  # окно «один раз» прошло: копилка ждёт решения об остатке
        if passed:  # срок ещё не перенесли — считаем со следующего года
            deadline = _next_year(deadline)
        collected = start[piggy.id].left if passed else start[piggy.id].collected
        needs.append(
            _Need(piggy.id, deadline, max(piggy.target - collected, 0.0) * rate, True)
        )
        if piggy.yearly:
            following = _next_year(deadline)
            while following < horizon:
                needs.append(_Need(piggy.id, following, piggy.target * rate, False))
                following = _next_year(following)
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
    alloc, ready = _schedule(money, planned)

    # доля 10 %: по подушке на 1-е число, дальше подушка растёт на свою часть
    monthly = monthly_spending(frame, today)
    currency = budget.frozen_currency if budget is not None else None
    frozen_rate = rates.to_uzs.get(currency or "", 0.0)
    cushion_uzs: float | None = None
    if budget is not None and budget.frozen is not None and currency and frozen_rate:
        envelopes_start = sum(
            rates.convert(start[piggy.id].left, piggy.currency, currency) or 0.0
            for piggy in open_piggies
            if piggy.id in start
        )
        cushion_uzs = (
            budget.frozen - budget.frozen_change - envelopes_start
        ) * frozen_rate
    share_month = share * _income(rows)
    collected_goals = {
        piggy.id: start[piggy.id].collected * rates.to_uzs.get(piggy.currency, 0.0)
        for piggy in goals
    }
    plan: list[MonthPlan] = []
    share_to = "cushion"
    for index, first_day in enumerate(months[:PLAN_MONTHS]):
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
            to_goals = share_month / 2
        else:
            to_goals = share_month
        if index == 0:
            share_to = (
                "cushion"
                if to_goals == 0
                else "goals" if to_goals >= share_month else "split"
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
            parts[piggy.id] = min(each, max(wanted, 0.0))
            collected_goals[piggy.id] += parts[piggy.id]
        to_goals = sum(parts.values())
        if cushion_uzs is not None:
            cushion_uzs += share_month - to_goals
        items: dict[str, float] = {}
        for k, amount in alloc[index].items():
            items[planned[k].piggy_id] = items.get(planned[k].piggy_id, 0.0) + amount
        plan.append(
            MonthPlan(
                first_day,
                sum(items.values()),
                items,
                share_month - to_goals,
                parts,
                tuple(dict.fromkeys(planned[k].piggy_id for k in ready[index])),
            )
        )

    # копилки на экране
    states: list[PiggyState] = []
    for piggy in open_piggies:
        moves = book.moves_of(piggy.id) if piggy.id else ()
        balance = now.get(piggy.id, Balance(0.0, 0.0, 0.0, 0.0, ()))
        rate = rates.to_uzs.get(piggy.currency, 0.0)
        planned_now = plan[0].items.get(piggy.id, 0.0) + plan[0].goals.get(
            piggy.id, 0.0
        )
        this_month = _up(planned_now / rate, piggy.currency) if rate else 0.0
        left_month = max(this_month - _saved_in(moves, month), 0.0)
        status, short, leftover = "on_track", 0.0, _leftover(moves, balance.left)
        if piggy.error or not piggy.id:
            status = "broken"
        elif piggy.deadline is None:
            reached = piggy.target is not None and balance.collected >= piggy.target
            status = "done" if reached else "goal"
        elif piggy.target is None:
            status = "no_target"
        elif (
            not piggy.yearly
            and today > piggy.deadline + timedelta(days=AFTER_DAYS)
            and balance.left > 0
        ):
            status, leftover = "leftover", balance.left
        elif balance.collected >= piggy.target:
            status = "done"
        elif piggy.id in late:
            status, short = "late", (late[piggy.id] / rate if rate else 0.0)
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
            )
        )
    states.sort(
        key=lambda item: (
            2 if item.status == "broken" else 1 if item.piggy.deadline is None else 0,
            item.piggy.deadline or date.max,
            item.piggy.row,
        )
    )

    # подушка — на вечер вчера, как «Заморожено»
    cushion, envelopes, over = None, 0.0, 0.0
    frozen = budget.frozen if budget is not None else None
    if frozen is not None and currency and frozen_rate:
        envelopes = sum(
            rates.convert(night[piggy.id].left, piggy.currency, currency) or 0.0
            for piggy in open_piggies
            if piggy.id in night
        )
        amount = frozen - envelopes
        over = max(-amount, 0.0)
        kept = max(amount, 0.0)
        cushion = Cushion(
            kept, currency, kept * frozen_rate, frozen_rate, monthly, cushion_months
        )
    yearly = {
        (piggy.group or "", piggy.subcategory or "")
        for piggy in scheduled
        if piggy.yearly and piggy.subcategory
    }
    return Screen(
        today,
        tuple(states),
        tuple(plan),
        cushion,
        frozen,
        envelopes,
        over,
        share_to,
        _enough(frame, today, yearly, plan[0].total if plan else 0.0, share),
    )


def distribute(
    screen: Screen, amount: float, currency: str, rates: Rates
) -> list[tuple[str, float]]:
    """«Отложил…» одной суммой: копилкам со сроком по порядку экрана их «осталось
    в этом месяце», потом долгим целям; остальное — подушке, его не пишут."""
    rest = rates.convert(amount, currency, "UZS")
    if rest is None or rest <= 0:
        return []
    chosen: list[tuple[str, float]] = []
    ordered = [item for item in screen.piggies if item.piggy.deadline is not None] + [
        item for item in screen.piggies if item.piggy.deadline is None
    ]
    for item in ordered:
        if item.status == "broken" or item.left_this_month <= 0 or rest <= 0:
            continue
        rate = rates.to_uzs.get(item.piggy.currency)
        if not rate:
            continue
        take = min(item.left_this_month * rate, rest)
        rest -= take
        digits = 0 if item.piggy.currency == "UZS" else 2
        chosen.append((item.piggy.id, round(take / rate, digits)))
    return chosen
