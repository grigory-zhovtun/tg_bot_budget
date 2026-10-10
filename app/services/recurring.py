"""Регулярные списания: подписки и платежи раз в месяц по истории листа fact.

Серия — траты одного магазина (первые два слова названия без цифр и знаков)
с похожей суммой (±25 %), минимум в двух разных месяцах. Последние три
списания должны приходиться на один день месяца (±4 дня), а в недавние месяцы
их не больше двух — иначе это обычный магазин, а не подписка. Серия без
списаний больше 40 дней — «давно не было», в итог месяца не входит.
"""

import re
import statistics
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from app.services.analytics_service import MONTHS_RU, expenses, money
from app.services.day_budget import MONTHS_PREPOSITIONAL, short

WINDOW_MONTHS = 4  # сколько месяцев истории смотреть
AMOUNT_SPREAD = 0.25  # сумма одной серии отличается не больше чем на 25 %
DAY_SPREAD = 4  # последние списания — в пределах ±4 дней месяца
RECENT = 3  # по скольким последним списаниям судить о дне
FREQUENT = 3  # столько и больше за месяц — обычный магазин
LAPSED_DAYS = 40  # дольше без списаний — «отменили?»
SUBSCRIPTION = "подписка"
# Общие комментарии банка вместо магазина: разные люди и сервисы под одним словом
GENERIC_WORDS = frozenset({"перевод", "без", "зачисление", "пополнение", "снятие"})


@dataclass(frozen=True)
class Charge:
    day: date
    uzs: float
    amount: float
    currency: str
    comment: str
    subcategory: str


@dataclass(frozen=True)
class Series:
    key: str
    day: int  # обычный день списания
    charges: tuple[Charge, ...]  # по дате

    @property
    def last(self) -> Charge:
        return self.charges[-1]

    @property
    def name(self) -> str:
        return clean_name(self.last.comment) or self.key

    def charged_in(self, today: date) -> list[Charge]:
        return [
            c
            for c in self.charges
            if (c.day.year, c.day.month) == (today.year, today.month)
        ]

    def lapsed(self, today: date) -> bool:
        return (today - self.last.day).days > LAPSED_DAYS

    def expected_on(self, today: date) -> date:
        """День списания в месяце today (31-е в коротком месяце — последний день)."""
        next_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
        last_day = (next_month - timedelta(days=1)).day
        return today.replace(day=min(self.day, last_day))


def recurring_key(comment: str) -> str:
    """«GOOGLE *ChatGPT; ≈ 22.39 USD» и «Google - .4 Chatgpt» → «google chatgpt»."""
    text = re.split(r"[;(,]", comment or "")[0].lower()
    words = [w for w in re.findall(r"[a-zа-яё]+", text) if len(w) >= 2]
    return " ".join(list(dict.fromkeys(words))[:2])


def clean_name(comment: str) -> str:
    """Название для списка: без хвоста «; ≈ 22.39 USD», «(сумма)», «, 3.49 USD»."""
    return re.split(r"[;(,]", comment or "")[0].strip()


def _clusters(charges: list[Charge]) -> list[list[Charge]]:
    """Траты одного магазина → серии с похожей суммой (две подписки Apple)."""
    groups: list[list[Charge]] = []
    for charge in sorted(charges, key=lambda c: c.uzs):
        for group in groups:
            middle = statistics.median(c.uzs for c in group)
            if abs(charge.uzs - middle) <= AMOUNT_SPREAD * middle:
                group.append(charge)
                break
        else:
            groups.append([charge])
    return groups


def _month_index(day: date) -> int:
    return day.year * 12 + day.month


def find_series(frame: pd.DataFrame, today: date) -> list[Series]:
    """Серии регулярных трат за последние WINDOW_MONTHS месяцев (frame — весь fact)."""
    spent = expenses(frame)
    start = _month_index(today) - WINDOW_MONTHS
    by_key: dict[str, list[Charge]] = {}
    for record in spent.itertuples(index=False):
        if record.amount_uzs <= 0 or _month_index(record.day) < start:
            continue
        key = recurring_key(record.comment)
        if key and key.split()[0] not in GENERIC_WORDS:
            by_key.setdefault(key, []).append(
                Charge(
                    record.day,
                    float(record.amount_uzs),
                    float(record.amount),
                    record.currency,
                    record.comment,
                    record.subcategory,
                )
            )

    recent_months = range(_month_index(today) - RECENT + 1, _month_index(today) + 1)
    found = []
    for key, charges in by_key.items():
        visits = [_month_index(c.day) for c in charges]
        if any(visits.count(m) >= FREQUENT for m in recent_months):
            continue  # магазин или кафе: покупки по нескольку раз в месяц
        for group in _clusters(charges):
            group.sort(key=lambda c: c.day)
            months = [_month_index(c.day) for c in group]
            if len(set(months)) < 2:
                continue
            days = [c.day.day for c in group[-RECENT:]]
            middle = statistics.median(days)
            if max(abs(d - middle) for d in days) > DAY_SPREAD:
                continue
            found.append(Series(key, int(middle), tuple(group)))
    return sorted(found, key=lambda s: (s.day, s.key))


def _amount(charge: Charge) -> str:
    """«378 560» или «118,04 USD ≈ 1,42 млн»."""
    if charge.currency == "UZS":
        return money(charge.uzs)
    value = (
        money(charge.amount)
        if float(charge.amount).is_integer()
        else f"{charge.amount:,.2f}".replace(",", " ").replace(".", ",")
    )
    return f"{value} {charge.currency} ≈ {short(charge.uzs)}"


def _dates(charges: Iterable[Charge]) -> str:
    return ", ".join(c.day.strftime("%d.%m") for c in charges)


def format_subs(
    series: list[Series], today: date, subscriptions_plan: float | None = None
) -> str:
    """Ответ /subs: по дню месяца, итог, вторые списания и пропавшие подписки."""
    active = [s for s in series if not s.lapsed(today)]
    total = sum(s.last.uzs for s in active)
    subscriptions = sum(
        s.last.uzs for s in active if s.last.subcategory == SUBSCRIPTION
    )
    lines = [f"🔁 Регулярные списания ≈ {short(total)} в месяц"]
    if subscriptions:
        plan = f" при плане {short(subscriptions_plan)}" if subscriptions_plan else ""
        lines.append(f"из них подписки ≈ {short(subscriptions)}{plan}")
    month = MONTHS_PREPOSITIONAL[today.month - 1]
    for s in active:
        line = f"• {s.day:02d} — {s.name}: {_amount(s.last)}"
        this_month = s.charged_in(today)
        if not this_month and s.expected_on(today) >= today:
            line += f" (ждём {s.expected_on(today):%d.%m})"
        elif not this_month:
            line += f" (в {month} не было)"  # отменили или не пришло SMS
        if len(this_month) >= 2:
            line += f" ⚠️ в {month} дважды: {_dates(this_month)}"
        lines.append(line)
    gone = [s for s in series if s.lapsed(today)]
    if gone:
        lines.append(
            "Давно не было (отменили?): "
            + ", ".join(f"{s.name} — последнее {s.last.day:%d.%m}" for s in gone)
        )
    return "\n".join(lines)


def morning_lines(series: list[Series], today: date) -> list[str]:
    """Для утреннего сообщения: что спишется сегодня и завтра, второе списание вчера."""

    def amount(s: Series) -> str:
        if s.last.currency == "UZS":
            return f"≈ {money(s.last.uzs)}"
        return f"≈ {_amount(s.last).split(' ≈ ')[0]} ({short(s.last.uzs)})"

    active = [s for s in series if not s.lapsed(today) and not s.charged_in(today)]
    lines = []
    tomorrow = today + timedelta(days=1)
    for label, day in (("Сегодня", today), ("Завтра", tomorrow)):
        if day.month != today.month:
            continue
        due = [s for s in active if s.expected_on(today) == day]
        if due:
            items = " • ".join(f"{s.name} {amount(s)}" for s in due)
            lines.append(f"🔁 {label} спишется: {items}")
    yesterday = today - timedelta(days=1)
    for s in series:
        this_month = s.charged_in(today)
        if len(this_month) >= 2 and this_month[-1].day == yesterday:
            month = MONTHS_RU[today.month - 1]
            lines.append(
                f"⚠️ {s.name} списан второй раз за {month}: {_dates(this_month)}"
            )
    return lines
