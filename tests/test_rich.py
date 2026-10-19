"""Помощники расширенных сообщений: числа не рвутся, текст из таблицы экранируется."""

from app.rich import cents, heading, in_currency, para, percent, signed, table, whole

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


def test_blocks_escape_sheet_text() -> None:
    assert heading("Аренда & <офис>", 1) == "<h1>Аренда &amp; &lt;офис&gt;</h1>"
    assert heading("Вчера") == "<h3>Вчера</h3>"
    assert para("a < b", "c") == "<p>a &lt; b<br>c</p>"
    assert table([["кафе & бар", f"1{N}000"]], "lr", ["статья", "факт"]) == (
        "<table bordered striped><tr><th>статья</th><th>факт</th></tr>"
        f'<tr><td><b>кафе &amp; бар</b></td><td align="right"><b>1{N}000</b></td>'
        "</tr></table>"
    )
