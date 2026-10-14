"""Колонка «Пришло к утру» жёлтого списка вкладки текущего месяца.

Бот сверяет строки списка с fact (services.receipts) по операциям до вчерашнего
дня включительно — как лимит «с утра» — и пишет в колонку T, сколько пришло по
строке, в её валюте и со знаком. Формулы вкладки (P5, Q10) считают по колонке.
"""

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
    read_forecast,
)
from app.services.google_sheets import GoogleSheetsService
from app.services.receipts import Receipt, match_receipts

COLUMN_T = COLUMN_O + 5  # справа от «В сумах» (S)
RECEIVED_HEADER = "Пришло к утру"


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
    frame = transactions_frame(gs.get_values(config.FACT_SHEET_NAME), gs.get_rates())
    first = today.replace(day=1)
    receipts = match_receipts(frame, forecast.items, first, today - timedelta(days=1))
    data = received_cells(title, rows, receipts)
    if data:
        gs.write_cells(data)
    return len(data)
