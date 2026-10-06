"""Выписки Капиталбанка: разбор текста, правила категорий, сверка с листом fact."""

from datetime import date, timedelta

import pytest

from app import statements as st
from app.domain import Catalog, MerchantBook, merchant_core

SOURCES = ["VISA 9120 UZS", "UZCARD 5837 UZS", "HUMO 6845 UZS", "VISA 4058 USD"]
CATALOG = Catalog(
    categories=["🍔 ЕДА", "🏚️ ДОМ", "👶 ДЕТИ", "🚧 РАЗНОЕ", "💳 СЧЕТА", "💰 ДОХОДЫ"],
    subcategories={
        "🍔 ЕДА": ["кофе", "кафе"],
        "🏚️ ДОМ": ["продукты", "квартплата", "инвентарь"],
        "👶 ДЕТИ": ["карманные"],
        "🚧 РАЗНОЕ": ["неучтенка"],
        "💳 СЧЕТА": ["переводы", "обмен валюты"],
        "💰 ДОХОДЫ": ["зарплата", "премия"],
    },
    sources=SOURCES,
)

# Текст pypdf (layout) в формате «История операций»; номера счёта и карты выдуманы
LAYOUT = """\
Акционерный коммерческий банк «Капиталбанк»
                                                         Выписка
                                                по истории операций
ФИО:    TEST USER
Счет №:   00000 000 000000000 001
Валюта:   USD
Выписка за период:     с 01.07.2026 год по 06.10.2026 год.

ВАЖНО!
* В соответствии с правилами платежных систем, платежи по карте отражаются
могут отсутствовать некоторые операции, совершенные по карте

 Дата проведения
                         Сумма                 Детали операции
 операции

                                               Списание по операциям покупке TEST STORE ONE SLIP No 56167 за 01.07.2026 Номер
 03.07.2026              -32,81
                                               карты: 400000******4058; TID: 57070564 RRN:_1_7605

Управляющий директор
АКБ «Капиталбанк»
\f Дата проведения
                    Сумма     Детали операции
  29.09.2026                1 000,00          Перевод с HUMO KB TO VISA USD KB через МП ТЕR.ID: TM0065; за 28.09.2026; Номер карты:
                              400000******4058; RRN:627114116548 15028_1

Дата и время генерации выписки
06.10.2026г., 15:24
"""


def txn(
    source: str, amount: float, details: str, when: date = date(2026, 8, 1)
) -> st.Txn:
    return st.Txn(
        f"{source}:{details[:12]}:{amount}", source, when, when, amount, details
    )


def fact_values(*rows: list[object]) -> list[list[object]]:
    header = ["Дата", "Категория", "Название", "Сумма", "Баланс", "Комм", "Вал", "Ист"]
    return [header, *rows]


def serial(day: date) -> int:
    return (day - st.SERIAL_ZERO).days


# --- разбор текста ------------------------------------------------------------


def test_parse_statement_reads_operations_period_and_generation_date() -> None:
    statement = st.parse_statement(LAYOUT, SOURCES)
    assert statement.source == "VISA 4058 USD"
    assert [(t.posted, t.op_date, t.amount) for t in statement.txns] == [
        (date(2026, 7, 3), date(2026, 7, 1), -32.81),
        (date(2026, 9, 29), date(2026, 9, 28), 1000.0),
    ]
    assert st.MERCHANT.search(statement.txns[0].details).group(1) == "TEST STORE ONE"
    assert statement.period == (date(2026, 7, 1), date(2026, 10, 6))
    assert statement.generated == date(2026, 10, 6)
    # в день генерации банк ещё проводит операции — он не входит в «загружено по»
    assert statement.covered_until == date(2026, 10, 5)


def test_statement_detection() -> None:
    assert st.is_kapitalbank_statement(LAYOUT)
    assert not st.is_kapitalbank_statement("Чек из магазина, итого 48 000")


def test_unknown_card_is_reported() -> None:
    with pytest.raises(ValueError, match="не нашёл карту"):
        st.parse_statement(LAYOUT, ["VISA 9120 UZS"])


def test_card_is_chosen_by_currency_when_digits_repeat() -> None:
    text = "Валюта:   USD\n400000******1111 400000******1111"
    sources = ["HUMO 1111 UZS", "VISA 1111 USD"]
    assert st.detect_source(text, sources) == "VISA 1111 USD"


def test_statement_without_operations() -> None:
    text = LAYOUT[: LAYOUT.index(" Дата проведения")]
    with pytest.raises(ValueError, match="нет операций"):
        st.parse_statement(text, SOURCES)


def test_broken_block_is_an_error() -> None:
    text = LAYOUT.replace(" 03.07.2026              -32,81", " ??")
    with pytest.raises(ValueError, match="не разобрал"):
        st.parse_statement(text, SOURCES)


@pytest.mark.parametrize(
    ("details", "expected"),
    [
        ("покупке X SLIP No 1 за 01.07.2026 Номер", date(2026, 7, 1)),
        ("Эквайер … RRN 0312 дата 2026-08-30 12.05.42", date(2026, 8, 30)),
        ("ЗП за июль 2026 сог вед № 626 от 31.07.2026 к выдаче", date(2026, 7, 31)),
        ("UzCard: 96300282 Эмитент взаиморасчёты", date(2026, 9, 9)),
    ],
)
def test_operation_date(details: str, expected: date) -> None:
    assert st.operation_date(details, posted=date(2026, 9, 9)) == expected


def test_pdf_text_of_garbage_is_empty() -> None:
    assert st.pdf_text(b"%PDF-1.7 not really") == ""


def test_merge_statements_drops_repeated_operations() -> None:
    first = st.Statement(
        "VISA 9120 UZS",
        [txn("VISA 9120 UZS", -1, "A", date(2026, 10, 1))],
        (date(2026, 9, 1), date(2026, 10, 1)),
        date(2026, 10, 2),
    )
    second = st.Statement(
        "VISA 9120 UZS",
        [
            txn("VISA 9120 UZS", -1, "A", date(2026, 10, 1)),
            txn("VISA 9120 UZS", -2, "B", date(2026, 10, 3)),
        ],
        (date(2026, 10, 1), date(2026, 10, 5)),
        date(2026, 10, 5),
    )
    [merged] = st.merge_statements([first, second])
    assert [t.amount for t in merged.txns] == [-1, -2]
    assert len({t.uid for t in merged.txns}) == 2
    assert merged.period == (date(2026, 9, 1), date(2026, 10, 4))
    assert merged.covered_until == date(2026, 10, 4)


# --- категории ------------------------------------------------------------------

BOOK = MerchantBook.from_sheet(
    fact_values(
        [
            46000,
            "🏚️ ДОМ",
            "продукты",
            1,
            "",
            "AI: Ip Ooo Anglesey Food",
            "UZS",
            "X",
        ],
        [46001, "🍔 ЕДА", "кофе", 1, "", "AI: Ooo Shavi Cafe", "UZS", "X"],
        [46002, "🍔 ЕДА", "кафе", 1, "", "AI: Pie Point Mchdj", "UZS", "X"],
        [46003, "🏚️ ДОМ", "инвентарь", 1, "", "AI: Yandex.go", "UZS", "X"],
        [46004, "🍔 ЕДА", "кафе", 1, "", "AI: Yandex Lavka", "UZS", "X"],
        [46005, "🏚️ ДОМ", "продукты", 1, "", "AI: Havas Food", "UZS", "X"],
        [46006, "🏚️ ДОМ", "продукты", 1, "", "AI: Havas Market", "UZS", "X"],
        [46007, "🍔 ЕДА", "кофе", 1, "", "AI: без мерчанта (QR/Payme)", "UZS", "X"],
    )
)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("IP OOO ANGLESEY FOOD", ("🏚️ ДОМ", "продукты")),  # как в истории
        ("OOO SHAVI CAFE", ("🍔 ЕДА", "кофе")),
        ("PIE POINT MCHJ", ("🍔 ЕДА", "кафе")),  # другая правовая форма
        ("HAVAS FOOD MCHJ QK", ("🏚️ ДОМ", "продукты")),
        ("HAVAS GROCERY", ("🏚️ ДОМ", "продукты")),  # первое слово, категория одна
        ("YANDEX SCOOTERS", None),  # у YANDEX в истории разные категории
        ("SOMETHING NEW LLC", None),
    ],
)
def test_merchant_book(name: str, expected: tuple[str, str] | None) -> None:
    assert BOOK.find(name) == expected


def test_title_matches_existing_rows() -> None:
    assert st.title("IP OOO ANGLESEY FOOD") == "Ip Ooo Anglesey Food"
    assert st.title("VENDEX PTE LTD") == "Vendex PTE LTD"
    assert st.title("MARKTHOF MCHJ QK") == "Markthof Mchj QK"


def test_merchant_core() -> None:
    assert merchant_core("Markthof MCHJ QK") == "MARKTHOF"
    assert merchant_core("Transit 30750615172966") == "TRANSIT"


@pytest.mark.parametrize(
    ("source", "amount", "details", "target", "column_d", "review"),
    [
        (
            "UZCARD 5837 UZS",
            -48000,
            "UzCard: 963 Эмитент взаиморасчёты",
            st.COFFEE,
            48000,
            "",
        ),
        (
            "UZCARD 5837 UZS",
            -180000,
            "UzCard: 963 Эмитент взаиморасчёты",
            st.MISC,
            180000,
            "",
        ),
        (
            "HUMO 6845 UZS",
            -424551.34,
            "Расчеты ТСП др. банка при оплате",
            st.FOOD,
            424551.34,
            "",
        ),
        (
            "VISA 4058 USD",
            750,
            "Перевод с VISA USD KB на VISA USD KB через МП ТЕR.ID: TM0059; за 15.07.2026;",
            st.SALARY,
            750,
            "",
        ),
        ("VISA 4058 USD", -650, "Списание по выд нал в POS KASSA", st.RENT, 650, ""),
        (
            "VISA 4058 USD",
            -450,
            "Списание по выд нал в POS KASSA",
            st.RENT,
            450,
            "квартплата?",
        ),
        ("VISA 9120 UZS", -500000, "Списание по выд нал ATM", st.TRANSFERS, 500000, ""),
        (
            "VISA 9120 UZS",
            2720000,
            "Оплата аванс за август 2026 сотруд",
            st.SALARY,
            2720000,
            "",
        ),
        (
            "VISA 9120 UZS",
            3045000,
            "АКБ Капиталбанк Оплата премия к празднику",
            st.BONUS,
            3045000,
            "",
        ),
        (
            "VISA 9120 UZS",
            -50500,
            "Перевод с VISA UZS KB на UZCARD другого банка через МП",
            st.MISC,
            50500,
            "кому?",
        ),
        (
            "VISA 9120 UZS",
            -300000,
            "Перевод с VISA UZS KB на VISA UZS KB через МП ТЕR.ID:TM0053; за 14.08.2026;",
            st.POCKET,
            300000,
            "карманные?",
        ),
        (
            "HUMO 6845 UZS",
            13180631.36,
            "Зачисление средств на карту 986010******6845 через устройство 3664002Y",
            st.TRANSFERS,
            -13180631.36,
            "",
        ),
        (
            "HUMO 6845 UZS",
            595931.49,
            "Зачисление средств на карту через устройство 36623005",
            st.TRANSFERS,
            -595931.49,
            "откуда?",
        ),
        ("VISA 9120 UZS", -1500, "Комиссия за SMS", st.MISC, 1500, ""),
        (
            "VISA 9120 UZS",
            -98000,
            "покупке NEW PLACE SLIP No 1 за 01.08.2026",
            st.MISC,
            98000,
            "новый магазин",
        ),
        (
            "VISA 9120 UZS",
            -48000,
            "покупке OOO SHAVI CAFE SLIP No 2 за 01.08.2026",
            st.COFFEE,
            48000,
            "",
        ),
        ("VISA 9120 UZS", -7, "что-то странное", st.MISC, 7, "не разобрал"),
    ],
)
def test_categorize_rules(
    source: str,
    amount: float,
    details: str,
    target: tuple[str, str],
    column_d: float,
    review: str,
) -> None:
    [item] = st.categorize([txn(source, amount, details)], {}, BOOK, CATALOG)
    row = item.row
    assert ((row.category, row.subcategory), row.amount, item.review) == (
        target,
        column_d,
        review,
    )
    assert (row.source, row.currency) == (source, source[-3:])
    assert row.comment.startswith("AI: ")


def test_new_merchant_is_remembered_for_ai_and_ai_answer_is_used() -> None:
    operation = txn(
        "VISA 9120 UZS", -98000, "покупке NEW PLACE SLIP No 1 за 01.08.2026"
    )
    [item] = st.categorize([operation], {}, BOOK, CATALOG)
    assert item.merchant == "NEW PLACE"
    [item] = st.categorize(
        [operation], {}, BOOK, CATALOG, {"NEW PLACE": ("🏚️ ДОМ", "инвентарь")}
    )
    assert (item.row.category, item.row.subcategory, item.review) == (
        "🏚️ ДОМ",
        "инвентарь",
        "категория от AI",
    )
    assert item.row.comment == "AI: NEW Place"  # короткие слова капсом, как в fact


def test_category_missing_in_system_goes_to_misc() -> None:
    catalog = Catalog(["🚧 РАЗНОЕ"], {"🚧 РАЗНОЕ": ["неучтенка"]}, SOURCES)
    [item] = st.categorize(
        [txn("UZCARD 5837 UZS", -1000, "UzCard: Эмитент")], {}, BOOK, catalog
    )
    assert (item.row.category, item.review) == (
        "🚧 РАЗНОЕ",
        "нет «🍔 ЕДА / кофе» в system",
    )


def test_own_transfer_pairs_and_fee() -> None:
    txns = [
        txn(
            "VISA 9120 UZS",
            -1_000_000,
            "Перевод с VISA UZS KB на HUMO KB через МП ТЕR.ID:TM0056;",
        ),
        txn(
            "HUMO 6845 UZS",
            1_000_000,
            "Перевод с VISA UZS KB на HUMO KB через МП ТЕR.ID1067001L;",
        ),
        txn(
            "VISA 9120 UZS",
            -3_015_000,
            "Перевод с VISA UZS KB на UZCARD KB через МП ТЕR.ID: TM0105;",
        ),
        txn(
            "UZCARD 5837 UZS",
            3_000_000,
            "UzCard: 91105515 Эквайер взаиморасчёты по безналичным",
        ),
    ]
    rows = st.categorize(txns, st.match_pairs(txns), BOOK, CATALOG)
    assert sorted((r.row.source, r.row.subcategory, r.row.amount) for r in rows) == [
        ("HUMO 6845 UZS", "переводы", -1_000_000),
        ("UZCARD 5837 UZS", "переводы", -3_000_000),
        ("VISA 9120 UZS", "неучтенка", 15_000),
        ("VISA 9120 UZS", "переводы", 1_000_000),
        ("VISA 9120 UZS", "переводы", 3_000_000),
    ]
    notes = {r.row.comment for r in rows}
    assert "AI: перевод VISA 9120 → HUMO 6845 (свой)" in notes
    assert "AI: перевод HUMO 6845 ← VISA 9120 (свой)" in notes


def test_currency_exchange_pairs() -> None:
    txns = [
        txn(
            "VISA 9120 UZS",
            -11_830_000,
            "Перевод с VISA UZS KB на VISA UZS KB ТЕR.ID: TM0092;",
        ),
        txn(
            "VISA 4058 USD",
            1000,
            "Перевод с VISA USD KB на VISA USD KB ТЕR.ID: TM0093;",
        ),
    ]
    rows = st.categorize(txns, st.match_pairs(txns), BOOK, CATALOG)
    assert {(r.row.source, r.row.subcategory, r.row.amount) for r in rows} == {
        ("VISA 9120 UZS", "обмен валюты", 11_830_000),
        ("VISA 4058 USD", "обмен валюты", -1000),
    }


def test_income_comes_first_within_day() -> None:
    txns = [
        txn("VISA 9120 UZS", -7_692_750, "Перевод с VISA UZS KB на чужую через МП"),
        txn(
            "VISA 9120 UZS", 33_385_000, "АКБ Капиталбанк ЗП за сентябрь 2026 сог расч."
        ),
    ]
    rows = st.categorize(txns, {}, BOOK, CATALOG)
    assert [r.row.subcategory for r in rows] == ["зарплата", "неучтенка"]


# --- сверка с fact --------------------------------------------------------------

DAY = date(2026, 9, 29)


def coffee(source: str, amount: float, day: date = DAY) -> st.ImportRow:
    operation = txn(source, -amount, "UzCard: 963 Эмитент", day)
    [item] = st.categorize([operation], {}, BOOK, CATALOG)
    return item


def test_rows_already_in_fact_are_skipped_once() -> None:
    facts = st.fact_rows(
        fact_values(
            [serial(DAY), "🍔 ЕДА", "кофе", 48000, "", "", "UZS", "UZCARD 5837 UZS"]
        )
    )
    rows = [
        coffee("UZCARD 5837 UZS", 48000),
        coffee("UZCARD 5837 UZS", 48000, DAY + timedelta(days=2)),  # вторая такая же
        coffee("HUMO 6845 UZS", 48000),
    ]
    fresh, already, adjustments = st.reconcile(rows, facts)
    assert already == 1 and adjustments == []
    assert [(r.row.source, r.row.day) for r in fresh] == [
        ("UZCARD 5837 UZS", DAY + timedelta(days=2)),
        ("HUMO 6845 UZS", DAY),
    ]


def test_amount_converted_by_the_bot_is_corrected_not_duplicated() -> None:
    facts = st.fact_rows(
        fact_values(
            [
                serial(DAY),
                "🍔 ЕДА",
                "кафе",
                9.43,
                "",
                "AI: Cafe; ≈ 9.43 USD по курсу таблицы",
                "USD",
                "VISA 4058 USD",
            ],
            [
                serial(DAY),
                "🍔 ЕДА",
                "кафе",
                9.40,
                "",
                "AI: Cafe (без пересчёта)",
                "USD",
                "VISA 4058 USD",
            ],
        )
    )
    operation = txn("VISA 4058 USD", -9.51, "покупке OOO SHAVI CAFE SLIP No 3", DAY)
    rows = st.categorize([operation], {}, BOOK, CATALOG)
    fresh, already, [adjustment] = st.reconcile(rows, facts)
    assert (fresh, already) == ([], 0)
    assert (adjustment.fact.number, adjustment.amount) == (2, 9.51)


def test_far_amount_is_not_a_correction() -> None:
    facts = st.fact_rows(
        fact_values(
            [serial(DAY), "🍔 ЕДА", "кафе", 8.0, "", "≈ 8 USD", "USD", "VISA 4058 USD"]
        )
    )
    operation = txn("VISA 4058 USD", -9.51, "покупке X SLIP No 3", DAY)
    fresh, _, adjustments = st.reconcile(
        st.categorize([operation], {}, BOOK, CATALOG), facts
    )
    assert len(fresh) == 1 and adjustments == []


def test_fact_rows_skip_header_and_text_cells() -> None:
    rows = st.fact_rows(
        fact_values(
            ["05.10.2026", "🍔 ЕДА", "кофе", 1, "", "", "UZS", "X"],
            [serial(DAY), "🍔 ЕДА", "кофе", "", "", "", "UZS", "X"],
            [
                serial(DAY),
                "💰 ДОХОДЫ",
                "зарплата",
                5,
                "",
                "ВРЕМЕННАЯ строка",
                "UZS",
                "X",
            ],
        )
    )
    assert [(r.number, r.income, r.temporary) for r in rows] == [(4, True, True)]


# --- план импорта -----------------------------------------------------------------

STATEMENT_DAY = date(2026, 11, 10)


def statement(*txns: st.Txn, start: date = date(2026, 10, 1)) -> st.Statement:
    return st.Statement(
        "VISA 9120 UZS", list(txns), (start, STATEMENT_DAY), STATEMENT_DAY
    )


def test_plan_skips_loaded_operations_and_moves_the_mark() -> None:
    old = txn(
        "VISA 9120 UZS", -1000, "покупке OOO SHAVI CAFE SLIP No 1", date(2026, 10, 5)
    )
    new = txn(
        "VISA 9120 UZS", -2000, "покупке OOO SHAVI CAFE SLIP No 2", date(2026, 10, 7)
    )
    marks = {"VISA 9120 UZS": date(2026, 10, 5)}
    plan = st.plan_import([statement(old, new)], CATALOG, fact_values(), marks)
    assert plan.covered == 1
    assert [r.row.amount for r in plan.rows] == [2000]
    assert plan.marks == {"VISA 9120 UZS": date(2026, 11, 9)}
    assert plan.balance_effect() == {"VISA 9120 UZS": -2000}
    assert plan.warnings == []


def test_plan_warns_about_a_gap_after_the_mark() -> None:
    marks = {"VISA 9120 UZS": date(2026, 9, 20)}
    plan = st.plan_import([statement()], CATALOG, fact_values(), marks)
    [warning] = plan.warnings
    assert "загружено по 20.09.2026" in warning and "с 21.09.2026" in warning


def test_old_statement_changes_nothing() -> None:
    old = txn("VISA 9120 UZS", -1000, "покупке X SLIP No 1", date(2026, 10, 5))
    marks = {"VISA 9120 UZS": date(2026, 12, 1)}
    plan = st.plan_import([statement(old)], CATALOG, fact_values(), marks)
    assert (plan.has_changes, plan.marks, plan.covered) == (False, {}, 1)


def test_temporary_row_is_removed_by_the_next_statement_only() -> None:
    temp = [
        serial(date(2026, 10, 6)),
        "🚧 РАЗНОЕ",
        "неучтенка",
        1243960.8,
        "",
        "AI: выравнивание. ВРЕМЕННАЯ: удалить при следующей заливке",
        "UZS",
        "VISA 9120 UZS",
    ]
    values = fact_values(temp)

    plan = st.plan_import(
        [statement()], CATALOG, values, {"VISA 9120 UZS": date(2026, 10, 5)}
    )
    assert [f.number for f in plan.removals] == [2]
    assert plan.balance_effect() == {"VISA 9120 UZS": 1243960.8}

    # та же старая выписка, что уже загружена, временную строку не трогает
    old = st.Statement(
        "VISA 9120 UZS", [], (date(2026, 7, 1), date(2026, 10, 6)), date(2026, 10, 6)
    )
    plan = st.plan_import([old], CATALOG, values, {"VISA 9120 UZS": date(2026, 10, 5)})
    assert plan.removals == [] and not plan.has_changes


def test_transfer_pairs_with_an_already_loaded_half() -> None:
    out = txn(
        "VISA 9120 UZS",
        -500_000,
        "Перевод с VISA UZS KB на HUMO KB через МП",
        date(2026, 10, 5),
    )
    inc = txn(
        "HUMO 6845 UZS",
        500_000,
        "Перевод с VISA UZS KB на HUMO KB через МП",
        date(2026, 10, 6),
    )
    visa = st.Statement(
        "VISA 9120 UZS", [out], (date(2026, 10, 1), STATEMENT_DAY), STATEMENT_DAY
    )
    humo = st.Statement(
        "HUMO 6845 UZS", [inc], (date(2026, 10, 1), STATEMENT_DAY), STATEMENT_DAY
    )
    marks = {"VISA 9120 UZS": date(2026, 10, 5), "HUMO 6845 UZS": date(2026, 10, 5)}
    plan = st.plan_import([visa, humo], CATALOG, fact_values(), marks)
    [item] = plan.rows
    assert item.row.comment == "AI: перевод HUMO 6845 ← VISA 9120 (свой)"
    assert item.review == ""
