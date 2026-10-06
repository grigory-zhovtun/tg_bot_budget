"""Работа с Google-таблицей бюджета (синхронно — вызывать через asyncio.to_thread)."""

import logging
import os
import re
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import ValueInputOption, ValueRenderOption

from app import config
from app.domain import Rates, SheetRow

logger = logging.getLogger(__name__)

T = TypeVar("T")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# Баланс карты на текущей строке. Формула не содержит номера строки (INDEX/ROW()),
# поэтому верна, куда бы ни легла строка: после дописывания, сортировки, удаления.
_BALANCE_PART = (
    "SUMIFS($D$2:INDEX($D:$D;ROW()); $H$2:INDEX($H:$H;ROW()); INDEX($H:$H;ROW()); "
    '$G$2:INDEX($G:$G;ROW()); INDEX($G:$G;ROW()); $B$2:INDEX($B:$B;ROW()); "{op}💰 ДОХОДЫ")'
)
BALANCE_FORMULA = (
    "=" + _BALANCE_PART.format(op="") + " - " + _BALANCE_PART.format(op="<>")
)

# Блок остатков карт на листе fact: I — название источника, N — остаток из банка
BALANCE_BLOCK = "I2:I30"
BALANCE_COLUMN = "N"


def row_values(row: SheetRow) -> list[Any]:
    """Строка листа fact A:H для записи с USER_ENTERED."""
    return [
        row.date_text,
        row.category,
        row.subcategory,
        row.amount,
        BALANCE_FORMULA,
        row.comment,
        row.currency,
        row.source,
    ]


def appended_rows(response: dict[str, Any]) -> tuple[int, int]:
    """Номера первой и последней строки из ответа values.append."""
    updated = response["updates"]["updatedRange"]  # "fact!A4169:H4170"
    match = re.search(r"![A-Z]+(\d+)(?::[A-Z]+(\d+))?$", updated)
    if not match:
        raise ValueError(f"unexpected updatedRange: {updated}")
    first = int(match.group(1))
    return first, int(match.group(2) or first)


class GoogleSheetsService:
    def __init__(self) -> None:
        self.client: gspread.Client | None = None
        self.sheet: gspread.Spreadsheet | None = None
        self._worksheets: dict[str, gspread.Worksheet] = {}
        self._balance_cells: dict[str, str] | None = None
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticates with Google Sheets API."""
        try:
            if config.GOOGLE_APPLICATION_CREDENTIALS_PATH and os.path.exists(
                config.GOOGLE_APPLICATION_CREDENTIALS_PATH
            ):
                self.client = gspread.service_account(
                    filename=config.GOOGLE_APPLICATION_CREDENTIALS_PATH
                )
            elif config.GOOGLE_SERVICE_ACCOUNT_EMAIL and config.GOOGLE_PRIVATE_KEY:
                email = config.GOOGLE_SERVICE_ACCOUNT_EMAIL
                creds_info = {
                    "type": "service_account",
                    "private_key": config.GOOGLE_PRIVATE_KEY,
                    "client_email": email,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/"
                    + email.replace("@", "%40"),
                }
                creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
                self.client = gspread.authorize(creds)
            else:
                # Fallback to default auth if available (e.g. cloud environment)
                import google.auth

                creds, _ = google.auth.default(scopes=SCOPES)
                self.client = gspread.authorize(creds)

            self.sheet = self.client.open_by_key(config.SPREADSHEET_ID)
            self._worksheets.clear()
            logger.info("Successfully connected to Google Sheets.")
        except Exception:
            logger.exception("Failed to authenticate with Google Sheets")
            raise

    def _worksheet(self, name: str) -> gspread.Worksheet:
        """Лист по имени. Кэш: sheet.worksheet() каждый раз качает метаданные."""
        if name not in self._worksheets:
            if self.sheet is None:
                self._authenticate()
            self._worksheets[name] = self.sheet.worksheet(name)
        return self._worksheets[name]

    def _with_retry(self, action: str, operation: Callable[[], T]) -> T:
        """Одна повторная попытка после переподключения (протухший токен, сеть)."""
        try:
            return operation()
        except Exception:
            logger.warning("%s failed, reconnecting and retrying once", action)
            self._authenticate()
            return operation()

    def reload(self) -> None:
        """Сбросить кэши (после правок структуры таблицы, команда /reboot)."""
        self._worksheets.clear()
        self._balance_cells = None

    def get_categories_and_sources(
        self,
    ) -> tuple[list[str], dict[str, list[str]], list[str]]:
        """Retrieves categories, subcategories, and sources from the 'system' sheet."""
        data = self._with_retry(
            "Reading system sheet",
            lambda: self._worksheet(config.SYSTEM_SHEET_NAME).get_all_values(),
        )
        categories: list[str] = []
        subcategories: dict[str, list[str]] = {}
        sources: list[str] = []
        for row in data[1:]:  # Skip header
            row = row + [""] * (6 - len(row))
            cat, sub, src = row[0].strip(), row[1].strip(), row[5].strip()
            if cat and cat not in categories:
                categories.append(cat)
            if cat and sub:
                subcategories.setdefault(cat, []).append(sub)
            if src and src not in sources:
                sources.append(src)
        return categories, subcategories, sources

    def append_transactions(self, rows: Sequence[SheetRow]) -> tuple[int, int]:
        """Дописать строки в fact одним запросом; вернуть номера первой и последней.

        Формат и выпадающие списки копируются с предыдущей строки, чтобы новые
        строки выглядели как остальные (дата, «48 000», списки категорий).
        """
        values = [row_values(row) for row in rows]

        def append() -> dict[str, Any]:
            return self._worksheet(config.FACT_SHEET_NAME).append_rows(
                values,
                value_input_option=ValueInputOption.user_entered,
                table_range="A:H",
            )

        first, last = appended_rows(self._with_retry("Appending rows", append))
        try:
            self._copy_format(first, last)
        except Exception:
            logger.exception("Rows %s-%s written, formatting not copied", first, last)
        return first, last

    def _copy_format(self, first: int, last: int) -> None:
        if first <= 2:
            return
        sheet_id = self._worksheet(config.FACT_SHEET_NAME).id

        def grid(start: int, end: int) -> dict[str, int]:
            return {
                "sheetId": sheet_id,
                "startRowIndex": start - 1,
                "endRowIndex": end,
                "startColumnIndex": 0,
                "endColumnIndex": 8,
            }

        requests = [
            {
                "copyPaste": {
                    "source": grid(first - 1, first - 1),
                    "destination": grid(first, last),
                    "pasteType": paste_type,
                }
            }
            for paste_type in ("PASTE_FORMAT", "PASTE_DATA_VALIDATION")
        ]
        self.sheet.batch_update({"requests": requests})

    def balance_cells(self) -> dict[str, str]:
        """Ячейка «Проверка» (остаток из банка) для каждого источника из блока I:N."""
        if self._balance_cells is None:
            names = self._with_retry(
                "Reading balance block",
                lambda: self._worksheet(config.FACT_SHEET_NAME).get(BALANCE_BLOCK),
            )
            first_row = int(re.search(r"\d+", BALANCE_BLOCK).group())
            self._balance_cells = {
                row[0].strip(): f"{BALANCE_COLUMN}{first_row + offset}"
                for offset, row in enumerate(names)
                if row and row[0].strip()
            }
        return self._balance_cells

    def update_balances(self, balances: dict[str, float]) -> list[str]:
        """Записать остатки из банка в колонку «Проверка»; вернуть обновлённые источники."""
        cells = self.balance_cells()
        known = {source: value for source, value in balances.items() if source in cells}
        for source in balances.keys() - known.keys():
            logger.warning("No balance cell for source %r", source)
        if not known:
            return []
        data = [
            {"range": f"{config.FACT_SHEET_NAME}!{cells[source]}", "values": [[value]]}
            for source, value in known.items()
        ]
        self._with_retry(
            "Updating balances",
            lambda: self.sheet.values_batch_update(
                {"valueInputOption": "USER_ENTERED", "data": data}
            ),
        )
        return list(known)

    def get_all_records(self, worksheet_name: str) -> list[list[Any]]:
        """Retrieves all records from a worksheet with retry logic."""
        return self._with_retry(
            f"Reading {worksheet_name}",
            lambda: self._worksheet(worksheet_name).get_all_values(),
        )

    def get_rates(self) -> Rates:
        """Курсы к суму из system!H2:I10: код валюты в H, курс (GOOGLEFINANCE) в I.

        Если лист недоступен, остаётся только UZS: операции в другой валюте
        тогда не записываются, а бот просит внести их вручную.
        """
        rates = {"UZS": 1.0}
        try:
            values = self._with_retry(
                "Reading rates",
                lambda: self._worksheet(config.SYSTEM_SHEET_NAME).get(
                    "H2:I10", value_render_option=ValueRenderOption.unformatted
                ),
            )
            for row in values:
                if len(row) < 2 or not str(row[0]).strip():
                    continue
                code, rate = str(row[0]).strip().upper(), row[1]
                if isinstance(rate, int | float) and rate > 0:
                    rates[code] = float(rate)
        except Exception:
            logger.exception("Could not read currency rates from the system sheet")
        return Rates(rates)
