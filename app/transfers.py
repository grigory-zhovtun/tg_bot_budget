"""Перевод между своими картами из «Ввода» Mini App: снятие и пополнение.

Одна запись — две строки fact одной датой: снятие с карты «Откуда» (D плюсом) и
пополнение карты «Куда» (D минусом, как приход вне доходов). Комментарии — как у
SMS и выписок: «перевод VISA 9090 → UZCARD 5656 (свой)» у снятия и «перевод
UZCARD 5656 ← VISA 9090 (свой)» у пополнения, у обмена валют — «конвертация …».
Выписка сверяет карту, сумму D со знаком и дату, поэтому пару второй раз не пишет.
"""

from datetime import date

from app.domain import SheetRow, entry_row
from app.statements import FX, short


def transfer_rows(
    amount: float,
    to_amount: float | None,
    comment: str,
    source: str,
    to_source: str,
    category: str,
    subcategory: str,
    day: date,
) -> list[SheetRow]:
    """Снятие с source и пополнение to_source.

    amount — «Списано» в валюте source; to_amount — «Зачислено» в валюте
    to_source, None — та же сумма (валюта одна). Свой комментарий владельца
    дописывается после пометки через «, ».
    """
    label = "конвертация" if subcategory == FX[1] else "перевод"

    def note(arrow: str, card: str, other: str) -> str:
        text = f"{label} {short(card)} {arrow} {short(other)} (свой)"
        return f"{text}, {comment}" if comment else text

    got = amount if to_amount is None else to_amount
    return [
        entry_row(
            amount,
            False,
            note("→", source, to_source),
            source,
            category,
            subcategory,
            day,
        ),
        entry_row(
            got,
            True,
            note("←", to_source, source),
            to_source,
            category,
            subcategory,
            day,
        ),
    ]
