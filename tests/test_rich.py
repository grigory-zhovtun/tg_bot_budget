"""Помощники расширенных сообщений: числа не рвутся, текст из таблицы экранируется."""

from app.rich import (
    Report,
    cents,
    heading,
    in_currency,
    money_text,
    note_report,
    para,
    percent,
    signed,
    table,
    whole,
)

N = " "


def test_numbers_keep_together() -> None:
    assert whole(1_234_567.4) == f"1{N}234{N}567"
    assert whole(-1500) == f"−1{N}500"
    assert cents(1234.5) == f"1{N}234,50"
    assert in_currency(380, "USD") == f"380{N}$"
    assert in_currency(75_000, "RUB") == f"75{N}000{N}₽"
    assert in_currency(50, "EUR") == f"50{N}EUR"
    assert in_currency(1234.5, "USD", with_cents=True) == f"1{N}234,50{N}$"
    assert in_currency(3_150_000, "UZS") == f"3{N}150{N}000"
    assert signed(-420, "USD") == f"−420{N}$"
    assert signed(75_000, "RUB") == f"+75{N}000{N}₽"
    assert percent(0.437) == f"44{N}%"


def test_big_lines_are_headings_with_monospace_digits() -> None:
    # шрифт заголовков Telegram — с засечками; цифры — моноширинные знаки Юникода
    assert heading(f"💸 1{N}186{N}667 сум", 1) == f"<h1>💸 𝟷{N}𝟷𝟾𝟼{N}𝟼𝟼𝟽 сум</h1>"
    # заголовок переносит строки сам — длинная строка целиком
    over = f"⚠️ Сверх плана на 2{N}100{N}000 за 23 дня"
    assert heading(over) == f"<h3>⚠️ Сверх плана на 𝟸{N}𝟷𝟶𝟶{N}𝟶𝟶𝟶 за 𝟸𝟹 дня</h3>"
    assert heading(f"🧊 Заморожено 1{N}500,25 $", 2) == (
        f"<h2>🧊 Заморожено 𝟷{N}𝟻𝟶𝟶,𝟸𝟻 $</h2>"
    )


def test_blocks_escape_sheet_text() -> None:
    assert heading("Аренда & <офис>", 1) == "<h1>Аренда &amp; &lt;офис&gt;</h1>"
    assert para("a < b", "c") == "<p>a &lt; b<br>c</p>"
    assert table([["кафе & бар", f"1{N}000"]], "lr", ["статья", "факт"]) == (
        "<table bordered striped><tr><th>статья</th><th>факт</th></tr>"
        f'<tr><td><b>кафе &amp; бар</b></td><td align="right"><b>1{N}000</b></td>'
        "</tr></table>"
    )


def test_note_report_splits_the_head() -> None:
    text = "📅 Жена — новая запись: 21.10 Кружок 180 000 UZS"
    assert note_report(text) == Report(
        text, heading("📅 Жена — новая запись") + para("21.10 Кружок 180 000 UZS")
    )
    many = note_report("✏️ Правки в плане на октябрь:\nкафе: 1 → 2\nСвободно: 3 → 4")
    assert many.html == heading("✏️ Правки в плане на октябрь") + para(
        "кафе: 1 → 2", "Свободно: 3 → 4"
    )
    assert note_report("Нечего отменять").html == heading("Нечего отменять")


def test_money_text_shows_cents_only_when_there_are_any() -> None:
    assert money_text(48_000, "UZS") == f"48{N}000"
    assert money_text(380, "USD") == f"380{N}$"
    assert money_text(12.5, "USD") == f"12,50{N}$"
