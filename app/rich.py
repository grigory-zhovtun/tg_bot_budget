"""Расширенные сообщения Telegram (Bot API 10.1+): крупные строки и таблицы.

Сводка собирается блоками rich HTML. Тому, кто их не примет, уходит то же сообщение
обычным текстом (Report.text) — его собирают прежние форматтеры. Крупные строки —
моноширинными блоками: заголовки h1–h6 Telegram рисует шрифтом с засечками, а их
владелец не читает (выбрал блок, 24.10.2026).
"""

import html
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass

NBSP = " "  # внутри числа и перед единицей: число не разрывается переносом
# Блок сам не переносит строки: длиннее — переносим по словам; 28 знаков влезают в
# пузырь телефона и при шрифте Telegram крупнее обычного
PRE_WIDTH = 28
RULE = "<hr/>"
SYMBOLS = {"USD": "$", "RUB": "₽"}


@dataclass(frozen=True)
class Report:
    text: str  # обычный текст: запасной вид
    html: str | None = None  # расширенное сообщение; None — только текст


def _escape(text: str) -> str:
    return html.escape(text, quote=False)


def whole(value: float) -> str:
    """1234567.4 → «1 234 567», −1500 → «−1 500» (пробелы неразрывные)."""
    text = f"{abs(value):,.0f}".replace(",", NBSP)
    return f"−{text}" if round(value) < 0 else text


def cents(value: float) -> str:
    """1234.5 → «1 234,50»."""
    text = f"{abs(value):,.2f}".replace(",", NBSP).replace(".", ",")
    return f"−{text}" if round(value, 2) < 0 else text


def in_currency(amount: float, code: str, with_cents: bool = False) -> str:
    """«380 $», «75 000 ₽», «50 EUR»; сумы — без единицы."""
    number = cents(amount) if with_cents else whole(amount)
    if code == "UZS":
        return number
    return f"{number}{NBSP}{SYMBOLS.get(code, code)}"


def money_text(amount: float, code: str) -> str:
    """Сумма записи или копилки: копейки — только у дробной суммы в валюте."""
    fraction = code != "UZS" and not float(amount).is_integer()
    return in_currency(amount, code, with_cents=fraction)


def signed(amount: float, code: str) -> str:
    """Строка списка со знаком: «+75 000 ₽», «−380 $»."""
    sign = "−" if amount < 0 else "+"
    return f"{sign}{in_currency(abs(amount), code)}"


def percent(share: float) -> str:
    """0.437 → «44 %»."""
    return f"{share * 100:.0f}{NBSP}%"


def _cells(text: str) -> int:
    """Ширина строки в знаках моноширинного шрифта: эмодзи и широкие знаки — по два."""
    width = 0
    for char in text:
        if char == "\ufe0f":  # «⚠️»: знак становится эмодзи — шире
            width += 1
        elif ord(char) > 0xFFFF or unicodedata.east_asian_width(char) in "WF":
            width += 2
        else:
            width += 1
    return width


def _wrap(text: str, width: int = PRE_WIDTH) -> str:
    """Переносы по словам; число не рвётся — в нём неразрывные пробелы."""
    lines: list[str] = []
    line = ""
    for word in text.split(" "):
        joined = f"{line} {word}" if line else word
        if line and _cells(joined) > width:
            lines.append(line)
            line = word
        else:
            line = joined
    lines.append(line)
    return "\n".join(lines)


def heading(text: str, level: int = 3) -> str:
    """Крупная строка — моноширинным блоком (см. начало модуля); level — уровень по
    разметке вызовов, на вид блока не влияет."""
    return f"<pre>{_escape(_wrap(text))}</pre>"


def para(*lines: str) -> str:
    return f"<p>{'<br>'.join(_escape(line) for line in lines)}</p>"


def table(
    rows: Sequence[Sequence[str]], align: str, head: Sequence[str] | None = None
) -> str:
    """Таблица с рамкой и полосами; align по колонкам: «l» — влево, «r» — вправо."""

    def cell(text: str, index: int) -> str:
        side = ' align="right"' if align[index] == "r" else ""
        return f"<td{side}><b>{_escape(text)}</b></td>"

    lines = []
    if head is not None:
        # у заголовка без align: в справке Telegram он есть только у <td>
        lines.append(f"<tr>{''.join(f'<th>{_escape(c)}</th>' for c in head)}</tr>")
    lines += [
        f"<tr>{''.join(cell(c, i) for i, c in enumerate(row))}</tr>" for row in rows
    ]
    return f"<table bordered striped>{''.join(lines)}</table>"


def note_report(text: str) -> Report:
    """Заметка «заголовок: подробности» → крупная строка и абзац под ней.

    Первая строка до первого «: » (или без завершающего «:») — заголовок, её
    остаток и остальные строки — абзацем; текст отчёта — заметка как есть.
    """
    first, *rest = text.splitlines() or [""]
    head, sep, tail = first.partition(": ")
    if not sep:
        head, tail = first.removesuffix(":"), ""
    lines = [line for line in (tail, *rest) if line.strip()]
    return Report(text, heading(head) + (para(*lines) if lines else ""))
