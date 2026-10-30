"""Советник «Плана»: контекст черновика для модели и проверка её ответа.

Модель получает готовые числа — как /advice — и то, что владелец написал о
семье в листе system: итог месяца, статьи черновика, помесячный факт за год,
планы прошлых месяцев, покупки (сколько, средний чек, где больше всего),
траты вне плана, темп текущего месяца, деньги и курсы. Интернета у модели
нет. «Применить» остаётся у пункта, только если статья есть во вкладке, её
можно править, а новая сумма разумна и отличается от черновика.
"""

import calendar
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
from gspread.exceptions import WorksheetNotFound

from app import config
from app.domain import Rates, merchant_key, month_title
from app.services.analytics_service import MONTHS_RU, expenses, transactions_frame
from app.services.calendar import read_calendar
from app.services.day_budget import DayBudget, day_budget, until_text
from app.services.google_sheets import GoogleSheetsService
from app.services.plan import (
    TOLERANCE,
    Article,
    Catalog,
    History,
    MonthPlan,
    PlanChanges,
    applied,
    article_history,
    article_key,
    read_plan,
    shift_month,
    totals,
    unknown,
)

MAX_POINTS = 5
MAX_PLAN = 10**12
HISTORY_MONTHS = 12  # помесячный факт: тренды и тот же месяц год назад
PURCHASE_MONTHS = 3  # покупки и траты вне плана
TOP_PLACES = 3
TOP_UNPLANNED = 5


@dataclass(frozen=True)
class AdvicePoint:
    """Пункт совета; key и plan — «Применить»: новая сумма статьи в её валюте."""

    text: str
    key: str | None = None
    plan: float | None = None


@dataclass(frozen=True)
class Purchases:
    """Покупки статьи: сколько, средний чек и места, где тратится больше всего."""

    count: int
    average: float
    places: tuple[tuple[str, float], ...]


@dataclass(frozen=True)
class AdviceContext:
    """Всё, что советник знает о месяце, — для одного запроса к модели."""

    draft: MonthPlan
    month: date  # первое число планируемого месяца
    today: date
    rates: Rates
    history: History
    months: tuple[date, ...]  # полные месяцы до текущего, от старого к новому
    by_month: Mapping[str, tuple[float, ...]]  # ключ → факт по months
    old_plans: Mapping[date, Mapping[str, float]]  # месяц → ключ → план вкладки
    purchases: Mapping[str, Purchases]
    unplanned: tuple[tuple[str, float], ...]  # «группа / подкатегория», сумы
    budget: DayBudget | None  # текущий месяц, как в «Сводке»; None — нет данных
    profile: str = ""


def full_months(today: date, count: int) -> tuple[date, ...]:
    """count полных месяцев до текущего, от старого к новому."""
    current = today.replace(day=1)
    return tuple(shift_month(current, -back) for back in range(count, 0, -1))


def _within(frame: pd.DataFrame, months: Sequence[date]) -> pd.DataFrame:
    end = shift_month(months[-1], 1)
    return frame[(frame["day"] >= months[0]) & (frame["day"] < end)]


def monthly_sums(
    frame: pd.DataFrame, months: Sequence[date]
) -> dict[str, tuple[float, ...]]:
    """Факт по паре (подкатегория, валюта) за каждый месяц; без записей — 0.

    Знак как во вкладке: трата и доход плюс, возврат минус.
    """
    window = _within(frame, months)
    if window.empty:
        return {}
    month = window["day"].map(lambda value: value.replace(day=1))
    totals_by = (
        window.assign(month=month)
        .groupby(["subcategory", "currency", "month"])["amount"]
        .sum()
    )
    found: dict[str, dict[date, float]] = {}
    for (sub, currency, first), amount in totals_by.items():
        found.setdefault(article_key(sub, currency), {})[first] = float(amount)
    return {
        key: tuple(values.get(first, 0.0) for first in months)
        for key, values in found.items()
    }


def purchases(frame: pd.DataFrame, months: Sequence[date]) -> dict[str, Purchases]:
    """Покупки за months: сколько, средний чек и TOP_PLACES мест по сумме."""
    window = _within(expenses(frame), months)
    window = window[window["amount"] > 0]  # возвраты — не покупки
    found: dict[str, Purchases] = {}
    for (sub, currency), rows in window.groupby(["subcategory", "currency"]):
        places: dict[str, float] = {}
        for comment, amount in zip(rows["comment"], rows["amount"], strict=True):
            if name := merchant_key(comment):
                places[name] = places.get(name, 0.0) + float(amount)
        top = sorted(places.items(), key=lambda item: item[1], reverse=True)
        found[article_key(sub, currency)] = Purchases(
            len(rows), float(rows["amount"].mean()), tuple(top[:TOP_PLACES])
        )
    return found


def unplanned(
    frame: pd.DataFrame, months: Sequence[date], draft: MonthPlan
) -> tuple[tuple[str, float], ...]:
    """Траты за months по парам, которых нет в черновике, — крупнейшие, в сумах."""
    window = _within(expenses(frame), months)
    present = {a.key for a in draft.articles}
    found: dict[str, float] = {}
    sums = window.groupby(["group", "subcategory", "currency"])["amount_uzs"].sum()
    for (group, sub, currency), amount in sums.items():
        if article_key(sub, currency) in present or amount <= 0:
            continue
        label = (
            f"{group} / {sub}" if currency == "UZS" else f"{group} / {sub} {currency}"
        )
        found[label] = float(amount)
    top = sorted(found.items(), key=lambda item: item[1], reverse=True)
    return tuple(top[:TOP_UNPLANNED])


def _plain(value: float, currency: str) -> str:
    """Число без пробелов между разрядами: модели так проще, чем «1 500 000»."""
    return f"{value:.0f}" if currency == "UZS" else f"{value:.2f}"


def _when(month: date, today: date) -> str:
    days = calendar.monthrange(month.year, month.month)[1]
    name = f"{MONTHS_RU[month.month - 1]} {month.year}"
    if month == today.replace(day=1):
        return f"План на текущий месяц ({name}): до конца месяца {days - today.day + 1} дн."
    return f"План на следующий месяц ({name}), {days} дн."


def _now(budget: DayBudget, today: date) -> list[str]:
    """Текущий месяц, как в «Сводке»: темп трат, лимит, сумы, доллары."""
    done = [day for day in budget.history if day.day < today]
    spent = sum(day.spent for day in done)
    plan = budget.plan_per_day * len(done)
    lines = [
        f"Сейчас ({today:%d.%m.%Y}):",
        f"Потрачено за {len(done)} дн.: {_plain(spent, 'UZS')} UZS "
        f"при плане {_plain(plan, 'UZS')} UZS",
        f"Можно в день {until_text(budget)}: {_plain(budget.limit, 'UZS')} UZS",
    ]
    runway = budget.runway
    if runway is not None:
        line = f"Сумы на картах и наличными: {_plain(runway.balance, 'UZS')} UZS; "
        if runway.short <= 0:
            line += "до конца месяца хватит"
        else:
            last = (
                f"хватит до {runway.last_day:%d.%m}"
                if runway.last_day is not None and runway.last_day >= today
                else "уже закончились"
            )
            until = f"до {runway.until:%d.%m}" if runway.until else "до конца месяца"
            need = (
                f"обменять ≈ {_plain(runway.dollars, 'UZS')} USD"
                if runway.dollars is not None
                else f"не хватит {_plain(runway.short, 'UZS')} UZS"
            )
            line += f"{last}; {until} {need}"
        lines.append(line)
    if budget.frozen is not None and budget.frozen_currency:
        code = budget.frozen_currency
        lines.append(
            f"Отложено в {code} (на жизнь не тратится): {_plain(budget.frozen, code)} {code}"
        )
    return lines


def advice_numbers(context: AdviceContext) -> str:
    """Текст для модели: «О нас», месяц, итог, список, деньги, статьи, история."""
    draft, history = context.draft, context.history
    total = totals(draft, context.rates)
    codes = sorted(code for code in context.rates.to_uzs if code != "UZS")
    rates = "; ".join(
        f"1 {code} = {_plain(context.rates.to_uzs[code], 'UZS')} UZS" for code in codes
    )
    lines = [
        "О семье (написали сами):",
        *(context.profile.splitlines() or ["— не заполнено"]),
        "",
        _when(context.month, context.today),
        f"Курсы: {rates}",
        f"Поступления по списку: {_plain(total.incomes, 'UZS')} UZS",
        f"Расходы по плану: {_plain(total.spend, 'UZS')} UZS",
        f"Отложить (цель): {_plain(total.goal, 'UZS')} UZS",
        f"Свободно после цели: {_plain(total.free, 'UZS')} UZS",
    ]
    events = sorted(
        (
            e
            for e in draft.events or ()
            if e.name and e.day and e.amount is not None and e.currency
        ),
        key=lambda e: e.day or 0,
    )
    if events:
        lines += ["", "Поступления (+) и платежи (−) по дням, без названий:"]
        for e in events:
            amount = e.amount or 0.0
            sign = "+" if amount > 0 else ""
            lines.append(
                f"{e.day} | {sign}{_plain(amount, e.currency or 'UZS')} {e.currency}"
            )
    if context.budget is not None:
        lines += ["", *_now(context.budget, context.today)]
    average = ", ".join(MONTHS_RU[m.month - 1] for m in history.average_months)
    last = MONTHS_RU[history.last_month.month - 1]
    current = MONTHS_RU[history.current_month.month - 1]
    lines += [
        "",
        "Статьи черновика: ключ | группа / подкатегория | тип | план | среднее за "
        f"{average} | {last} | {current} за {history.current_days} дн. | валюта",
    ]
    for article in draft.articles:
        sums = (article.plan, *history.of(article.key))
        cells = [
            article.key,
            f"{article.group} / {article.subcategory}",
            "доход" if article.kind == "income" else "расход",
            *(_plain(value, article.currency) for value in sums),
            article.currency,
        ]
        lines.append(" | ".join(cells))
    if context.by_month:
        heads = " | ".join(f"{m:%Y-%m}" for m in context.months)
        lines += ["", f"Факт по месяцам: ключ | {heads}"]
        for article in draft.articles:
            values = context.by_month.get(article.key)
            if values and any(values):
                cells = [_plain(value, article.currency) for value in values]
                lines.append(" | ".join([article.key, *cells]))
    if context.old_plans:
        months = sorted(context.old_plans)
        heads = " | ".join(f"{m:%Y-%m}" for m in months)
        lines += ["", f"План прошлых месяцев (вкладки): ключ | {heads}"]
        for article in draft.articles:
            values = [context.old_plans[m].get(article.key) for m in months]
            if any(values):
                cells = [
                    "—" if value is None else _plain(value, article.currency)
                    for value in values
                ]
                lines.append(" | ".join([article.key, *cells]))
    if context.purchases:
        lines += [
            "",
            "Покупки за 3 мес.: ключ | сколько | средний чек | где больше всего",
        ]
        for article in draft.articles:
            bought = context.purchases.get(article.key)
            if bought is None:
                continue
            places = "; ".join(
                f"{name} {_plain(amount, article.currency)}"
                for name, amount in bought.places
            )
            average_check = _plain(bought.average, article.currency)
            lines.append(
                f"{article.key} | {bought.count} | {average_check} | {places or '—'}"
            )
    if context.unplanned:
        lines += ["", "Траты вне плана за 3 мес., в сумах:"]
        lines += [
            f"{label} | {_plain(amount, 'UZS')}" for label, amount in context.unplanned
        ]
    return "\n".join(lines)


def _rounded(value: float, currency: str) -> float:
    """Сумы — до тысяч, валюта — до копеек: план не нужен точнее."""
    return float(round(value / 1000) * 1000) if currency == "UZS" else round(value, 2)


def checked_points(
    points: list[AdvicePoint], editable: Mapping[str, Article]
) -> list[AdvicePoint]:
    """До MAX_POINTS непустых пунктов; «Применить» — только разумная правка.

    editable — статьи вкладки, которые можно править, с планом из черновика.
    """
    checked: list[AdvicePoint] = []
    for point in points:
        text = point.text.strip()
        if not text:
            continue
        if len(checked) == MAX_POINTS:
            break
        article = editable.get(point.key or "")
        value = point.plan
        if article is None or value is None or not 0 <= value <= MAX_PLAN:
            checked.append(AdvicePoint(text))
            continue
        value = _rounded(value, article.currency)
        if abs(value - article.plan) < TOLERANCE:
            checked.append(AdvicePoint(text))
            continue
        checked.append(AdvicePoint(text, article.key, value))
    return checked


@dataclass(frozen=True)
class Gathered:
    plan: MonthPlan  # вкладка как есть: по ней видно, что можно править
    context: AdviceContext | None  # None — правки не подходят к вкладке
    invalid: frozenset[str] = frozenset()


def _rows(gs: GoogleSheetsService, title: str) -> list[list[Any]] | None:
    try:
        return gs.get_values(title)
    except WorksheetNotFound:
        return None


def gather_advice(
    gs: GoogleSheetsService,
    first: date,
    today: date,
    changes_for: Callable[[MonthPlan], PlanChanges],
    catalog: Catalog,
) -> Gathered:
    """Всё для советника (синхронно): вкладки, факт, курсы, «О нас».

    changes_for строит правки черновика по прочитанной вкладке: база правок —
    то, что во вкладке сейчас. WorksheetNotFound — вкладки планируемого месяца нет.
    """
    rows = gs.get_values(month_title(first))
    plan = read_plan(rows)
    changes = changes_for(plan)
    rates = gs.get_rates()
    bad = unknown(plan, changes, rates.to_uzs.keys(), catalog)
    bad.discard("space")  # советнику место для вставки не важно
    if bad:
        return Gathered(plan, None, frozenset(bad))
    frame = transactions_frame(gs.get_values(config.FACT_SHEET_NAME), rates)
    current = today.replace(day=1)
    current_rows = rows if first == current else _rows(gs, month_title(current))
    months = full_months(today, HISTORY_MONTHS)
    old_tabs = {month: _rows(gs, month_title(month)) for month in months[-3:]}
    old_plans = {
        month: {a.key: a.plan for a in read_plan(old).articles}
        for month, old in old_tabs.items()
        if old is not None
    }
    budget = None
    if current_rows is not None:
        records = read_calendar(gs.get_calendar())
        budget = day_budget(
            frame,
            current_rows,
            today,
            rates,
            config.FROZEN_CURRENCY,
            records,
            previous=old_tabs[months[-1]],  # прошлый месяц: перенос недополученного
        )
    draft = applied(plan, changes)
    recent = months[-PURCHASE_MONTHS:]
    context = AdviceContext(
        draft=draft,
        month=first,
        today=today,
        rates=rates,
        history=article_history(frame, today),
        months=months,
        by_month=monthly_sums(frame, months),
        old_plans=old_plans,
        purchases=purchases(frame, recent),
        unplanned=unplanned(frame, recent, draft),
        budget=budget,
        profile=gs.get_advisor_profile(),
    )
    return Gathered(plan, context)
