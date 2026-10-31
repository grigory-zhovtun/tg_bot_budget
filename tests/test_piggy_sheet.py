"""Листы «Копилки» и «Журнал копилок» в сервисе таблицы: создание, строки, формулы."""

from typing import Any

from gspread.exceptions import WorksheetNotFound
from gspread.utils import ValueInputOption

from app import config
from app.services.google_sheets import (
    LIFE_LABEL,
    PERCENT_LABEL,
    PIGGY_JOURNAL_HEADER,
    PIGGY_LIST_HEADER,
    GoogleSheetsService,
)

LIST, JOURNAL = config.PIGGY_SHEET_NAME, config.PIGGY_JOURNAL_NAME
ROW = ["p3k9za", "Новый год", 12000000, "UZS", "20.12.2026", "каждый год",
       "🎉 ПРАЗДНИК", "сезонный", "", "", "", "", "Тест", ""]  # fmt: skip
OTHER = ["m4c8x1", "Машина", "", "USD", "", "", "", "", "", "", "", "", "Тест", ""]
MOVE = ["30.10.2026", "p3k9za", "Новый год", 6000000, "UZS", "отложил", "Тест", ""]
SAVED = (
    "=SUMIFS('Журнал копилок'!$D:$D;'Журнал копилок'!$B:$B;INDEX($A:$A;ROW());"
    "'Журнал копилок'!$D:$D;\">0\")"
)
LEFT = "=INDEX($I:$I;ROW())-INDEX($J:$J;ROW())"


class Sheet:
    def __init__(
        self, title: str, rows: list[list[Any]] | None = None, cols: int = 14
    ) -> None:
        self.title = title
        self.rows = rows if rows is not None else []
        self.reads = 0
        self.appended: list[tuple[list[list[Any]], dict[str, Any]]] = []
        self.col_count = cols
        self.added_cols: list[int] = []

    def add_cols(self, cols: int) -> None:
        self.added_cols.append(cols)
        self.col_count += cols

    def get_all_values(self, **options: Any) -> list[list[Any]]:
        self.reads += 1
        return [list(row) for row in self.rows]

    def col_values(self, column: int) -> list[Any]:
        return [row[column - 1] if len(row) >= column else "" for row in self.rows]

    def append_rows(self, values: list[list[Any]], **options: Any) -> dict[str, Any]:
        first = len(self.rows) + 1
        self.appended.append((values, options))
        self.rows.extend(list(row) for row in values)
        last = first + len(values) - 1
        return {"updates": {"updatedRange": f"'{self.title}'!A{first}:N{last}"}}


class Spreadsheet:
    def __init__(self, *sheets: Sheet) -> None:
        self.sheets = {sheet.title: sheet for sheet in sheets}
        self.created: list[tuple[str, int, int]] = []
        self.batches: list[dict[str, Any]] = []

    def worksheet(self, name: str) -> Sheet:
        if name not in self.sheets:
            raise WorksheetNotFound(name)
        return self.sheets[name]

    def add_worksheet(self, title: str, rows: int, cols: int) -> Sheet:
        self.created.append((title, rows, cols))
        self.sheets[title] = Sheet(title, cols=cols)
        return self.sheets[title]

    def values_batch_update(self, body: dict[str, Any]) -> None:
        self.batches.append(body)


def service_with(*sheets: Sheet) -> GoogleSheetsService:
    service = GoogleSheetsService.__new__(GoogleSheetsService)
    service.client = None
    service.sheet = Spreadsheet(*sheets)
    service._worksheets = {}
    service._balance_cells = None
    service._calendar = None
    service._values = {}
    service._rates = None
    service._generation = 0
    service._authenticate = lambda: None
    return service


def test_creates_both_sheets_once_with_headers() -> None:
    service = service_with()
    service.ensure_piggy_sheets()
    service.ensure_piggy_sheets()
    book = service.sheet
    assert book.created == [(LIST, 200, 19), (JOURNAL, 200, 8)]
    assert book.sheets[LIST].rows == [PIGGY_LIST_HEADER]
    assert book.sheets[JOURNAL].rows == [PIGGY_JOURNAL_HEADER]


def test_a_new_piggy_gets_row_independent_formulas() -> None:
    service = service_with(
        Sheet(LIST, [PIGGY_LIST_HEADER, OTHER, OTHER, OTHER]),
        Sheet(JOURNAL, [PIGGY_JOURNAL_HEADER]),
    )
    assert service.add_piggy(ROW) == 5
    values, options = service.sheet.sheets[LIST].appended[0]
    assert values == [ROW]
    assert options == {
        "value_input_option": ValueInputOption.raw,
        "table_range": "A:N",
    }
    assert service.sheet.batches == [
        {
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"'{LIST}'!I5", "values": [[SAVED]]},
                {"range": f"'{LIST}'!K5", "values": [[LEFT]]},
            ],
        }
    ]


def test_the_first_piggy_creates_the_sheets() -> None:
    service = service_with()
    assert service.add_piggy(ROW) == 2
    assert [title for title, _, _ in service.sheet.created] == [LIST, JOURNAL]


def test_update_finds_the_row_by_id_after_a_shift() -> None:
    # строку выше удалили руками — копилка теперь в 3-й строке, а не в 4-й
    service = service_with(Sheet(LIST, [PIGGY_LIST_HEADER, OTHER, ROW]))
    assert service.update_piggy("p3k9za", {"B": "НГ", "M": "Тест"})
    assert service.sheet.batches[-1] == {
        "valueInputOption": "RAW",
        "data": [
            {"range": f"'{LIST}'!B3", "values": [["НГ"]]},
            {"range": f"'{LIST}'!M3", "values": [["Тест"]]},
        ],
    }
    assert not service.update_piggy("zzz999", {"B": "Нет такой"})


def test_moves_are_appended_as_values() -> None:
    service = service_with(
        Sheet(LIST, [PIGGY_LIST_HEADER, ROW]), Sheet(JOURNAL, [PIGGY_JOURNAL_HEADER])
    )
    service.add_piggy_moves([MOVE])
    values, options = service.sheet.sheets[JOURNAL].appended[0]
    assert values == [MOVE]
    assert options == {"value_input_option": ValueInputOption.raw, "table_range": "A:H"}


def test_reads_both_sheets_and_keeps_a_copy_until_a_write() -> None:
    piggies = Sheet(LIST, [PIGGY_LIST_HEADER, ROW])
    journal = Sheet(JOURNAL, [PIGGY_JOURNAL_HEADER, MOVE])
    service = service_with(piggies, journal)
    assert service.get_piggy(max_age=60) == ([PIGGY_LIST_HEADER, ROW],
                                             [PIGGY_JOURNAL_HEADER, MOVE])  # fmt: skip
    service.get_piggy(max_age=60)
    assert (piggies.reads, journal.reads) == (1, 1)
    service.add_piggy_moves([MOVE])
    service.get_piggy(max_age=60)
    assert (piggies.reads, journal.reads) == (2, 2)


def test_without_the_sheets_there_are_no_piggies() -> None:
    assert service_with().get_piggy() == ([], [])


def test_the_percent_widens_an_old_sheet_and_goes_into_p1_q1() -> None:
    service = service_with(
        Sheet(LIST, [PIGGY_LIST_HEADER[:14], ROW]),
        Sheet(JOURNAL, [PIGGY_JOURNAL_HEADER]),
    )
    service.write_piggy_percent(5)
    assert service.sheet.sheets[LIST].added_cols == [5]
    assert service.sheet.batches == [
        {
            "valueInputOption": "RAW",
            "data": [{"range": f"'{LIST}'!P1:Q1", "values": [[PERCENT_LABEL, 5]]}],
        }
    ]


def test_the_mandatory_groups_widen_the_sheet_and_go_into_r1_s1() -> None:
    service = service_with(Sheet(LIST, [PIGGY_LIST_HEADER, ROW], cols=17))
    service.write_life_groups(["🏚️ ДОМ", "🌛 ЕЖЕМЕСЯЧНО"])
    assert service.sheet.sheets[LIST].added_cols == [2]
    assert service.sheet.batches == [
        {
            "valueInputOption": "RAW",
            "data": [
                {
                    "range": f"'{LIST}'!R1:S1",
                    "values": [[LIFE_LABEL, "🏚️ ДОМ, 🌛 ЕЖЕМЕСЯЧНО"]],
                }
            ],
        }
    ]


def test_the_order_goes_into_o_by_id() -> None:
    service = service_with(
        Sheet(LIST, [PIGGY_LIST_HEADER, ROW, OTHER], cols=17),
        Sheet(JOURNAL, [PIGGY_JOURNAL_HEADER]),
    )
    assert service.write_piggy_order({"p3k9za": 2, "m4c8x1": 1})
    assert service.sheet.sheets[LIST].added_cols == [2]
    assert service.sheet.batches == [
        {
            "valueInputOption": "RAW",
            "data": [
                {"range": f"'{LIST}'!O1", "values": [["Порядок"]]},
                {"range": f"'{LIST}'!O2", "values": [[2]]},
                {"range": f"'{LIST}'!O3", "values": [[1]]},
            ],
        }
    ]


def test_an_unknown_id_writes_no_order() -> None:
    service = service_with(
        Sheet(LIST, [PIGGY_LIST_HEADER, ROW], cols=17),
        Sheet(JOURNAL, [PIGGY_JOURNAL_HEADER]),
    )
    assert not service.write_piggy_order({"p3k9za": 1, "zzz999": 2})
    assert service.sheet.batches == []
