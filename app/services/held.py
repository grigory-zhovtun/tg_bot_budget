"""Чужие деньги на своей карте: статьи «💳 СЧЕТА» с пометкой «чужие».

Пометка стоит в листе system: A — группа, B — статья, C — «чужие», D — значок.
Пришедшее по такой статье (минус в колонке D fact) — чужие деньги, потраченное
по ней (плюс) — их расход. Остаток чужих денег на карте в «свои» не входит:
own_money убирает строки таких статей из таблицы fact, а перерасход статьи
(потрачено больше, чем пришло) оставляет поправкой — это уже свои деньги.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd

from app import config
from app.domain import TRANSFERS_GROUP
from app.rich import money_text

logger = logging.getLogger(__name__)

HELD_MARK = "чужие"
HELD_TTL = 600.0  # с, копия листа system для меток
DEFAULT_ICON = "🤝"
OVERSPEND_COMMENT = "перерасход сбора"
_warned: set[tuple[str, str]] = set()  # метки не у «СЧЕТА» уже в логе


@dataclass(frozen=True)
class HeldArticle:
    name: str  # статья «💳 СЧЕТА», system!B
    icon: str  # system!D; пусто — DEFAULT_ICON


@dataclass(frozen=True)
class HeldOnCard:
    article: HeldArticle
    card: str  # fact!H как есть, без пробелов по краям: «HUMO 2222 UZS»
    currency: str
    came: float  # ≥ 0, в валюте карты
    spent: float  # ≥ 0

    @property
    def left(self) -> float:
        """Сколько чужих денег осталось на карте; минус — доложено из своих."""
        return round(self.came - self.spent, 2)


class ValuesSource(Protocol):
    def get_values(
        self, worksheet_name: str, max_age: float = 0.0
    ) -> list[list[Any]]: ...


def held_articles(rows: Sequence[Sequence[Any]]) -> tuple[HeldArticle, ...]:
    """Статьи «💳 СЧЕТА» с пометкой «чужие» — по строкам system без заголовка."""
    found: dict[str, HeldArticle] = {}
    for row in rows:
        group, name, mark, icon = (
            str(cell).strip() for cell in [*row[:4], *[""] * (4 - len(row[:4]))]
        )
        if not name or mark.lower() != HELD_MARK:
            continue
        if group != TRANSFERS_GROUP:
            if (group, name) not in _warned:
                _warned.add((group, name))
                logger.warning(
                    "Mark «%s» on %s / %s means nothing: only %s articles can be held",
                    HELD_MARK, group, name, TRANSFERS_GROUP,
                )  # fmt: skip
            continue
        found.setdefault(name, HeldArticle(name, icon or DEFAULT_ICON))
    return tuple(found.values())


def read_held(gs: ValuesSource, max_age: float = 0.0) -> tuple[HeldArticle, ...]:
    """Метки «чужие» из копии листа system; сбой чтения — как будто их нет."""
    try:
        rows = gs.get_values(config.SYSTEM_SHEET_NAME, max(max_age, HELD_TTL))
    except Exception:
        logger.warning("Held articles are not read, no held money", exc_info=True)
        return ()
    return held_articles(rows[1:])


def _held_rows(frame: pd.DataFrame, held: Sequence[HeldArticle]) -> pd.Series:
    """Маска строк fact по статьям-сборам."""
    names = [article.name for article in held]
    return (frame["group"] == TRANSFERS_GROUP) & frame["subcategory"].isin(names)


def held_on_cards(
    frame: pd.DataFrame, held: Sequence[HeldArticle], today: date
) -> tuple[HeldOnCard, ...]:
    """Чужие деньги по статьям и картам на сегодня; нулевой остаток не возвращается."""
    rows = frame[_held_rows(frame, held) & (frame["day"] <= today)]
    result: list[HeldOnCard] = []
    for article in held:
        part = rows[rows["subcategory"] == article.name]
        for (card, code), group in part.groupby(["source", "currency"], sort=False):
            amounts = group["amount"]
            item = HeldOnCard(
                article,
                str(card),
                str(code),
                # + 0.0: пустая сумма даёт «минус ноль», в JSON он станет «-0.0»
                round(float(-amounts[amounts < 0].sum()), 2) + 0.0,
                round(float(amounts[amounts > 0].sum()), 2) + 0.0,
            )
            if item.left != 0:
                result.append(item)
    return tuple(result)


def _uzs_rate(group: pd.DataFrame) -> float:
    """Курс группы в сумах по первой ненулевой строке; нет такой — 0."""
    known = group[(group["amount"] != 0) & (group["amount_uzs"] != 0)]
    if known.empty:
        return 0.0
    first = known.iloc[0]
    return float(first["amount_uzs"] / first["amount"])


def own_money(frame: pd.DataFrame, held: Sequence[HeldArticle]) -> pd.DataFrame:
    """Таблица fact без чужих денег: строки сборов убраны, перерасход оставлен.

    Когда по сбору потрачено больше, чем пришло, разница — свои деньги: на
    каждый день, где она меняется, добавляется строка-поправка (тот же день,
    группа и статья). Индексы остальных строк не меняются.
    """
    if not held:
        return frame
    mask = _held_rows(frame, held)
    corrections: list[dict[str, Any]] = []
    for (name, card, code), group in frame[mask].groupby(
        ["subcategory", "source", "currency"]
    ):
        rate = _uzs_rate(group)
        by_day = (-group["amount"]).groupby(group["day"]).sum().sort_index().cumsum()
        deficit_before = 0.0
        for day, left in by_day.items():
            deficit = round(min(0.0, float(left)), 2)
            if deficit != deficit_before:
                amount = -round(deficit - deficit_before, 2)
                corrections.append(
                    {
                        "day": day,
                        "group": TRANSFERS_GROUP,
                        "subcategory": name,
                        "amount": amount,
                        "comment": OVERSPEND_COMMENT,
                        "currency": code,
                        "source": card,
                        "amount_uzs": amount * rate,
                    }
                )
                deficit_before = deficit
    own = frame[~mask]
    if not corrections:
        return own
    start = int(frame.index.max()) + 1 if len(frame) else 0
    extra = pd.DataFrame(
        corrections, columns=frame.columns, index=range(start, start + len(corrections))
    )
    # без пересортировки: порядок строк важен recurring._clusters; поправки идут
    # в конце и держат свой день
    return pd.concat([own, extra])


def _card_label(item: HeldOnCard) -> str:
    """Карта без кода валюты на конце: «HUMO 2222 UZS» → «HUMO 2222»."""
    name, _, tail = item.card.rpartition(" ")
    return name if name and tail == item.currency else item.card


def held_text(item: HeldOnCard, with_card: bool = False) -> str:
    """Строка сводки: чужие деньги не в счёт или сколько доложено из своих.

    with_card — с названием карты: статья лежит на нескольких картах, и строки
    без него не отличить друг от друга."""
    label = f"{item.article.icon} {item.article.name}"
    if with_card:
        label += f" ({_card_label(item)})"
    if item.left > 0:
        return f"{label} {money_text(item.left, item.currency)} — чужие, не в счёт"
    return f"{label} — доложено из своих {money_text(-item.left, item.currency)}"


def held_lines(items: Sequence[HeldOnCard]) -> list[str]:
    """Строки сводки по всем чужим деньгам; карта названа там, где статья на двух
    картах и больше."""
    count: dict[str, int] = {}
    for item in items:
        count[item.article.name] = count.get(item.article.name, 0) + 1
    return [held_text(item, count[item.article.name] > 1) for item in items]
