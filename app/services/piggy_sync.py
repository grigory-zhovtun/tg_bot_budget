"""Фоновая запись копилок — из той же задачи, что «Пришло к утру».

Закрывшееся окно трат бот подводит строкой журнала «потрачено в окне» (дальше это
окно из fact не пересчитывается) и у «каждый год» переносит срок на год вперёд;
пишет свои колонки «Потрачено» (J) и «В этом месяце» (L) — только изменившиеся
ячейки — и восстанавливает стёртые формулы I и K.
"""

import logging
from collections.abc import Collection, Mapping, Sequence
from datetime import date, timedelta
from typing import Any

from app import config
from app.domain import Rates, currency_of, month_title
from app.services.analytics_service import transactions_frame
from app.services.day_budget import day_budget
from app.services.google_sheets import (
    GoalCells,
    GoogleSheetsService,
    read_month_sheets,
)
from app.services.piggy import (
    AFTER_DAYS,
    WINDOW,
    Book,
    Screen,
    balances,
    goal_of,
    next_year,
    read_book,
    savings_screen,
    thousands,
    window_key,
)
from app.services.plan import read_plan

logger = logging.getLogger(__name__)

SHEET_AGE = 120.0  # fact, курсы и вкладка — из копии экранов; листы копилок — заново
WHOLE = frozenset({"UZS", "RUB"})


def _rounded(value: float, currency: str) -> float:
    return float(round(value)) if currency in WHOLE else round(value, 2)


def _close_windows(
    gs: GoogleSheetsService, book: Book, frame: Any, rates: Rates, today: date
) -> int:
    """Итоги закрывшихся окон и новые сроки «каждый год». Сбой между ними доделает
    следующий запуск: итог окна уже есть — остаётся только перенести срок."""
    moves: list[list[Any]] = []
    rolls: list[tuple[str, date]] = []
    for piggy in book.piggies:
        if piggy.error or not piggy.id or piggy.closed or piggy.deadline is None:
            continue
        end = piggy.deadline + timedelta(days=AFTER_DAYS)
        if today <= end:
            continue
        key = window_key(piggy.deadline)
        # итог — только окну со статьёй: без неё трат из fact у копилки нет
        if piggy.subcategory and not any(
            move.key == key for move in book.moves_of(piggy.id)
        ):
            spent = sum(
                item.amount
                for item in balances(book, frame, rates, end)[piggy.id].spends
            )
            moves.append(
                [
                    f"{today:%d.%m.%Y}",
                    piggy.id,
                    piggy.name,
                    -_rounded(max(spent, 0.0), piggy.currency) or 0.0,  # не «-0»
                    piggy.currency,
                    WINDOW,
                    "бот",
                    key,
                ]
            )
        if piggy.yearly:
            rolls.append((piggy.id, next_year(piggy.deadline)))
    if moves:
        gs.add_piggy_moves(moves)
    for piggy_id, deadline in rolls:
        gs.update_piggy(piggy_id, {"E": f"{deadline:%d.%m.%Y}"})
    return len(moves) + len(rolls)


def _same(cell: Any, value: float) -> bool:
    try:
        return abs(float(str(cell).replace(" ", "").replace(",", ".")) - value) < 0.005
    except ValueError:
        return False


def _bot_cells(
    gs: GoogleSheetsService, rows: Sequence[Sequence[Any]], screen: Screen
) -> int:
    """«Потрачено» (J) и «В этом месяце» (L) — только изменившиеся ячейки строк,
    чей ID совпал в этом же чтении."""
    name = config.PIGGY_SHEET_NAME
    states = {state.piggy.id: state for state in screen.piggies if state.piggy.id}
    data: list[dict[str, Any]] = []
    for number, row in enumerate(rows[1:], start=2):
        piggy_id = str(row[0]).strip() if row else ""
        state = states.get(piggy_id)
        if state is None or state.piggy.row != number:
            continue
        code = state.piggy.currency
        for column, index, value in (
            ("J", 9, _rounded(state.balance.spent, code)),
            ("L", 11, _rounded(state.this_month, code)),
        ):
            if not _same(row[index] if len(row) > index else "", value):
                data.append(
                    {"range": f"'{name}'!{column}{number}", "values": [[value]]}
                )
    if data:
        gs.write_cells(data)
    return len(data)


def _formulas(gs: GoogleSheetsService, rows: Sequence[Sequence[Any]]) -> int:
    """Строки копилок, где формулу I или K стёрли руками."""
    lost = [
        number
        for number, row in enumerate(rows[1:], start=2)
        if row
        and str(row[0]).strip()
        and any(str(row[index]).strip() == "" if len(row) > index else True
                for index in (8, 10))  # fmt: skip
    ]
    if lost:
        gs.write_piggy_formulas(lost)
    return len(lost)


def sync_piggy(
    gs: GoogleSheetsService,
    today: date,
    currencies: Collection[str],
    groups: Collection[str],
    subcategories: Mapping[str, Sequence[str]],
) -> int:
    """Фоновая запись копилок; возвращает число записанных строк и ячеек."""
    rows, journal = gs.get_piggy(0.0)
    if len(rows) < 2:
        return 0
    # копилкам нужна только заморозка — переносу в ней нечего менять
    sheets = read_month_sheets(
        gs, month_title(today), SHEET_AGE, calendar=False, previous=False
    )
    rates = sheets.rates
    frame = transactions_frame(sheets.fact, rates)
    book = read_book(rows, journal, currencies, groups, subcategories)
    written = _close_windows(gs, book, frame, rates, today)
    if written:  # итоги и новые сроки — в расчёт колонок бота
        rows, journal = gs.get_piggy(0.0)
        book = read_book(rows, journal, currencies, groups, subcategories)
    budget = (
        day_budget(frame, sheets.month, today, rates, config.FROZEN_CURRENCY)
        if sheets.month is not None
        else None
    )
    screen = savings_screen(
        book,
        frame,
        sheets.month,
        today,
        rates,
        budget,
        config.SAVE_SHARE,
        config.CUSHION_MONTHS,
    )
    written += _bot_cells(gs, rows, screen)
    written += _formulas(gs, rows)
    if written:
        logger.info("Piggy banks: %d row(s) and cell(s) written", written)
    return written


def bot_catalog(
    bot_data: Mapping[str, Any],
) -> tuple[set[str], list[str], dict[str, list[str]]]:
    """Валюты карт и справочник статей бота — по ним проверяются строки копилок."""
    cards = {currency_of(source) for source in bot_data.get("sources", [])}
    return (
        cards or {"UZS"},
        list(bot_data.get("categories", [])),
        dict(bot_data.get("subcategories", {})),
    )


def goal_cells(
    gs: GoogleSheetsService,
    today: date,
    currencies: Collection[str],
    groups: Collection[str],
    subcategories: Mapping[str, Sequence[str]],
) -> GoalCells:
    """«Отложить за месяц» новой вкладки по копилкам (спека 4.7): строка цели по
    подписи и сумма до тысячи; без открытых копилок ячейку владельца не трогаем."""

    def cells(rows: list[list[Any]], first: date) -> tuple[int, float] | None:
        goal = read_plan(rows).goal
        if goal is None:
            return None
        piggies, journal = gs.get_piggy(0.0)
        book = read_book(
            piggies, journal, set(currencies), list(groups), dict(subcategories)
        )
        if not any(
            piggy.id and not piggy.error and piggy.closed is None
            for piggy in book.piggies
        ):
            return None
        rates = gs.get_rates(SHEET_AGE)
        frame = transactions_frame(
            gs.get_values(config.FACT_SHEET_NAME, SHEET_AGE), rates
        )
        screen = savings_screen(
            book,
            frame,
            rows,
            today,
            rates,
            None,
            config.SAVE_SHARE,
            config.CUSHION_MONTHS,
        )
        found = goal_of(screen, first)
        return None if found is None else (goal.row, thousands(found.total))

    return cells
