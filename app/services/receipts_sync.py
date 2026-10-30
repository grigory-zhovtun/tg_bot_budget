"""Колонка «Пришло к утру» жёлтого списка вкладки текущего месяца и подписки до
зарплаты рядом с лимитом.

Бот сверяет строки списка с fact (services.receipts) по операциям до вчерашнего
дня включительно — как лимит «с утра» — и пишет в колонку T, сколько пришло по
строке, в её валюте и со знаком. Формулы вкладки (P5, Q10) считают по колонке.
Остатки поступлений прошлого месяца (перенос) сверка закрывает первыми, а в T
идут только строки этого месяца — то, что им досталось после остатков: в лист
перенос не пишется, и копии вкладок его дальше не тащат.
Формула лимита (P5) — по сумам до зарплаты, как у бота; подписки она не найдёт —
их сумму по день зарплаты бот пишет в Q той же строки, а по день аванса, для
страховки до аванса, — в R (авансов в сроке нет — R пустая).
"""

import logging
from datetime import date, timedelta
from typing import Any

from gspread.exceptions import WorksheetNotFound

from app import config
from app.domain import month_title
from app.services.analytics_service import transactions_frame
from app.services.day_budget import (
    COLUMN_O,
    GOAL_LABEL,
    LIST_HEADER,
    TABLE_HEADER,
    carried_items,
    day_budget,
    read_forecast,
)
from app.services.google_sheets import GoogleSheetsService
from app.services.receipts import Receipt, match_receipts

logger = logging.getLogger(__name__)

COLUMN_T = COLUMN_O + 5  # справа от «В сумах» (S)
RECEIVED_HEADER = "Пришло к утру"
LIMIT_LABEL = (
    "Лимит на сегодня"  # строка лимита в сводке блока: O — подпись, P — формула
)
COLUMN_SUBSCRIPTIONS = COLUMN_O + 2  # Q той строки: подписки до зарплаты для формулы
COLUMN_ADVANCE_SUBSCRIPTIONS = COLUMN_O + 3  # R: подписки по день аванса — страховка


def _cell(row: list[Any], column: int) -> Any:
    return row[column] if len(row) > column else ""


def _same(have: Any, want: Any) -> bool:
    if want == "":
        return have in ("", None)
    number = isinstance(have, int | float) and not isinstance(have, bool)
    return number and abs(have - want) < 0.005


def received_cells(
    title: str, rows: list[list[Any]], receipts: dict[int, Receipt]
) -> list[dict[str, Any]]:
    """Ячейки колонки T, которые надо переписать: заголовок, суммы, лишние значения."""
    start = next(
        (
            index
            for index, row in enumerate(rows)
            if str(_cell(row, COLUMN_O)).startswith(LIST_HEADER)
        ),
        None,
    )
    if start is None:
        return []
    data: list[dict[str, Any]] = []
    if _cell(rows[start], COLUMN_T) != RECEIVED_HEADER:
        data.append({"range": f"'{title}'!T{start + 1}", "values": [[RECEIVED_HEADER]]})
    for index in range(start + 1, len(rows)):
        name = str(_cell(rows[index], COLUMN_O))
        if name == TABLE_HEADER or GOAL_LABEL in name:
            break
        receipt = receipts.get(index + 1)
        want: Any = round(receipt.received, 2) if receipt and receipt.received else ""
        if not _same(_cell(rows[index], COLUMN_T), want):
            data.append({"range": f"'{title}'!T{index + 1}", "values": [[want]]})
    return data


def subscription_cells(
    title: str,
    rows: list[list[Any]],
    subscriptions: float,
    advance: float | None = None,
) -> list[dict[str, Any]]:
    """Q строки лимита — подписки по день зарплаты, R — по день аванса (None — пусто);
    строки нет или числа те же — ничего."""
    for index, row in enumerate(rows):
        if str(_cell(row, COLUMN_O)).startswith(LIMIT_LABEL):
            wants = {
                "Q": (COLUMN_SUBSCRIPTIONS, round(subscriptions, 2)),
                "R": (
                    COLUMN_ADVANCE_SUBSCRIPTIONS,
                    "" if advance is None else round(advance, 2),
                ),
            }
            return [
                {"range": f"'{title}'!{letter}{index + 1}", "values": [[want]]}
                for letter, (column, want) in wants.items()
                if not _same(_cell(row, column), want)
            ]
    return []


def sync_receipts(gs: GoogleSheetsService, today: date) -> int:
    """Записать «Пришло к утру» во вкладку текущего месяца; сколько ячеек переписано."""
    title = month_title(today)
    try:
        rows = gs.get_values(title)
    except WorksheetNotFound:
        return 0
    forecast = read_forecast(rows)
    if forecast is None:
        return 0
    rates = gs.get_rates()
    frame = transactions_frame(gs.get_values(config.FACT_SHEET_NAME), rates)
    first = today.replace(day=1)
    try:
        previous: list[list[Any]] | None = gs.get_values(
            month_title(first - timedelta(days=1))
        )
    except WorksheetNotFound:  # первый месяц учёта — переносить нечего
        previous = None
    carried = carried_items(previous, frame, first)
    receipts = match_receipts(
        frame, forecast.items, first, today - timedelta(days=1), carried
    )
    data = received_cells(title, rows, receipts)
    try:
        budget = day_budget(frame, rows, today, rates, None, previous=previous)
    except Exception:  # сбой лимита не должен остановить «Пришло к утру»
        logger.exception("Subscriptions for the limit cell failed")
        budget = None
    if budget is not None:
        data += subscription_cells(
            title, rows, budget.salary_subscriptions, budget.advance_subscriptions
        )
    if data:
        gs.write_cells(data)
    return len(data)
