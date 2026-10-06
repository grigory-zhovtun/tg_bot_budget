"""Работа с Google-таблицей бюджета (синхронно — вызывать через asyncio.to_thread)."""

import logging
import os
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, TypeVar

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import ValueInputOption, ValueRenderOption

from app import config
from app.domain import Rates, SheetRow, month_bounds, month_title, parse_month_title

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

# Блок остатков карт на листе fact: I — название источника, J — остаток по
# таблице, N — остаток из банка
BALANCE_BLOCK = "I2:I30"
BALANCE_COLUMN = "N"
TABLE_BALANCES = "I2:J30"
# Лист system: F — источники, G рядом — по какой день загружены выписки
MARKS_RANGE = "F1:G60"
SOURCES_RANGE = "F1:F60"
MARKS_HEADER = "выписка по"
SERIAL_ZERO = date(1899, 12, 30)


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


@dataclass
class MonthTab:
    """Что сделано со вкладкой месяца: создана, даты поправлены, скрыты прошлые."""

    title: str
    source: str | None = None  # с какой вкладки скопирован план
    created: bool = False
    dates_fixed: bool = False
    hidden: list[str] = field(default_factory=list)


def _serial(day: date) -> int:
    return (day - SERIAL_ZERO).days


def _same_row(cells: list[Any], row: SheetRow) -> bool:
    """Строка листа (UNFORMATTED) та же, что записал бот? Баланс E не сравнивается."""
    cells = list(cells) + [""] * (8 - len(cells))
    serial, amount = cells[0], cells[3]
    return (
        isinstance(serial, int | float)
        and SERIAL_ZERO + timedelta(days=int(serial)) == row.day
        and isinstance(amount, int | float)
        and abs(amount - row.amount) < 0.005
        and [str(c).strip() for c in (cells[1], cells[2], cells[5], cells[6], cells[7])]
        == [
            row.category,
            row.subcategory,
            row.comment.strip(),
            row.currency,
            row.source,
        ]
    )


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

    def get_values(self, worksheet_name: str) -> list[list[Any]]:
        """Значения листа как есть: числа — числами, даты — серийными номерами."""
        return self._with_retry(
            f"Reading {worksheet_name}",
            lambda: self._worksheet(worksheet_name).get_all_values(
                value_render_option=ValueRenderOption.unformatted
            ),
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

    def get_table_balances(self) -> dict[str, float]:
        """Остатки карт по таблице (fact, колонка J блока остатков)."""
        values = self._with_retry(
            "Reading table balances",
            lambda: self._worksheet(config.FACT_SHEET_NAME).get(
                TABLE_BALANCES, value_render_option=ValueRenderOption.unformatted
            ),
        )
        return {
            str(row[0]).strip(): float(row[1])
            for row in values
            if len(row) >= 2 and str(row[0]).strip() and isinstance(row[1], int | float)
        }

    def get_statement_marks(self) -> dict[str, date]:
        """По какой день загружены выписки каждой карты (system, колонка G)."""
        values = self._with_retry(
            "Reading statement marks",
            lambda: self._worksheet(config.SYSTEM_SHEET_NAME).get(
                MARKS_RANGE, value_render_option=ValueRenderOption.unformatted
            ),
        )
        marks: dict[str, date] = {}
        for row in values[1:]:
            if len(row) < 2 or not str(row[0]).strip():
                continue
            source, mark = str(row[0]).strip(), row[1]
            if isinstance(mark, int | float) and not isinstance(mark, bool):
                marks[source] = SERIAL_ZERO + timedelta(days=int(mark))
            elif isinstance(mark, str) and re.fullmatch(r"\d\d\.\d\d\.\d{4}", mark):
                day, month, year = map(int, mark.split("."))
                marks[source] = date(year, month, day)
        return marks

    def set_statement_marks(self, marks: dict[str, date]) -> None:
        """Записать отметки «выписка по» рядом с картами в system!G."""
        sheet = self._worksheet(config.SYSTEM_SHEET_NAME)
        names = self._with_retry("Reading sources", lambda: sheet.get(SOURCES_RANGE))
        rows = {
            str(row[0]).strip(): number
            for number, row in enumerate(names, start=1)
            if row and str(row[0]).strip()
        }
        data = [{"range": f"{config.SYSTEM_SHEET_NAME}!G1", "values": [[MARKS_HEADER]]}]
        for source, day in marks.items():
            if source not in rows:
                logger.warning("No row for source %r in the system sheet", source)
                continue
            data.append(
                {
                    "range": f"{config.SYSTEM_SHEET_NAME}!G{rows[source]}",
                    "values": [[day.strftime("%d.%m.%Y")]],
                }
            )
        self._with_retry(
            "Writing statement marks",
            lambda: self.sheet.values_batch_update(
                {"valueInputOption": "USER_ENTERED", "data": data}
            ),
        )

    def update_fact_amounts(self, changes: Sequence[tuple[int, float, str]]) -> None:
        """Поправить сумму (D) и комментарий (F) в строках fact: (номер, сумма, текст)."""
        data = []
        for number, amount, comment in changes:
            data.append(
                {"range": f"{config.FACT_SHEET_NAME}!D{number}", "values": [[amount]]}
            )
            data.append(
                {"range": f"{config.FACT_SHEET_NAME}!F{number}", "values": [[comment]]}
            )
        if data:
            self._with_retry(
                "Updating fact amounts",
                lambda: self.sheet.values_batch_update(
                    {"valueInputOption": "RAW", "data": data}
                ),
            )

    def delete_fact_rows(self, numbers: Sequence[int], must_contain: str) -> list[int]:
        """Удалить строки fact, в комментарии которых есть must_contain.

        Перед удалением строки перечитываются: если таблицу успели поправить
        и на этом месте уже другая строка, она не тронется.
        """
        if not numbers:
            return []
        worksheet = self._worksheet(config.FACT_SHEET_NAME)
        comments = self._with_retry(
            "Reading rows to delete",
            lambda: worksheet.batch_get([f"F{n}" for n in numbers]),
        )
        confirmed = sorted(
            (
                number
                for number, cell in zip(numbers, comments, strict=True)
                if cell and cell[0] and must_contain in str(cell[0][0]).upper()
            ),
            reverse=True,  # снизу вверх: номера строк выше не сдвигаются
        )
        if confirmed:
            requests = [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": worksheet.id,
                            "dimension": "ROWS",
                            "startIndex": number - 1,
                            "endIndex": number,
                        }
                    }
                }
                for number in confirmed
            ]
            # без повтора: если ответ потерялся, второй запрос снёс бы другие строки
            self.sheet.batch_update({"requests": requests})
        return confirmed

    def delete_rows_if_match(
        self, first: int, last: int, rows: Sequence[SheetRow]
    ) -> bool:
        """Удалить строки first..last, если в таблице всё ещё именно они.

        Если строки успели поправить или сдвинуть, ничего не удаляется.
        """
        worksheet = self._worksheet(config.FACT_SHEET_NAME)
        current = self._with_retry(
            "Reading rows to undo",
            lambda: worksheet.get(
                f"A{first}:H{last}", value_render_option=ValueRenderOption.unformatted
            ),
        )
        if len(current) != len(rows) or not all(
            _same_row(cells, row) for cells, row in zip(current, rows, strict=True)
        ):
            return False
        # без повтора: если ответ потерялся, второй запрос снёс бы другие строки
        self.sheet.batch_update(
            {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": worksheet.id,
                                "dimension": "ROWS",
                                "startIndex": first - 1,
                                "endIndex": last,
                            }
                        }
                    }
                ]
            }
        )
        return True

    def ensure_month_tab(self, day: date) -> MonthTab:
        """Вкладка план-факта месяца day: создать копией прошлой, если её нет.

        Факт во вкладке считается формулами по датам M1 (первый день) и M2
        (последний), поэтому у копии меняются только они; план остаётся
        прошлый. Прошедшие месяцы скрываются: видны fact, текущий месяц, system.
        Повторный вызов ничего не меняет.
        """
        result = MonthTab(month_title(day))
        first, last = month_bounds(day)
        sheets = self._with_retry("Listing sheets", self.sheet.worksheets)
        if not any(ws.title == result.title for ws in sheets):
            months = [
                (start, ws)
                for ws in sheets
                if (start := parse_month_title(ws.title)) and start < first
            ]
            if not months:
                raise ValueError("нет вкладки прошлого месяца, чтобы скопировать план")
            _, source = max(months, key=lambda item: item[0])
            # без повтора: копия с тем же именем второй раз не создастся
            reply = self.sheet.batch_update(
                {
                    "requests": [
                        {
                            "duplicateSheet": {
                                "sourceSheetId": source.id,
                                "insertSheetIndex": source.index,
                                "newSheetName": result.title,
                            }
                        }
                    ]
                }
            )
            new_id = reply["replies"][0]["duplicateSheet"]["properties"]["sheetId"]
            result.created, result.source = True, source.title
            visibility = [(new_id, False)] + [
                (ws.id, True) for _, ws in months if not ws.isSheetHidden
            ]
            result.hidden = [ws.title for _, ws in months if not ws.isSheetHidden]
            self.sheet.batch_update(
                {
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {"sheetId": sheet_id, "hidden": hidden},
                                "fields": "hidden",
                            }
                        }
                        for sheet_id, hidden in visibility
                    ]
                }
            )

        dates = [[_serial(first)], [_serial(last)]]
        current = self._with_retry(
            "Reading month dates",
            lambda: self._worksheet(result.title).get(
                "M1:M2", value_render_option=ValueRenderOption.unformatted
            ),
        )
        if current != dates:
            self._with_retry(
                "Writing month dates",
                lambda: self.sheet.values_batch_update(
                    {
                        "valueInputOption": "RAW",
                        "data": [{"range": f"'{result.title}'!M1:M2", "values": dates}],
                    }
                ),
            )
            result.dates_fixed = not result.created
        return result
