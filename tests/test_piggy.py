"""Копилки: листы «Копилки» и «Журнал копилок», траты из копилки, план взносов."""

from datetime import date
from typing import Any

import pytest

from app.services.piggy import JOURNAL_HEADER, LIST_HEADER, Book, Piggy, read_book

CURRENCIES = {"UZS", "USD"}
GROUPS = ["🎉 ПРАЗДНИК", "✈️ ПОЕЗДКИ", "🍔 ЕДА", "💰 ДОХОДЫ", "💳 СЧЕТА"]
SUBCATEGORIES = {
    "🎉 ПРАЗДНИК": ["сезонный", "день рождения"],
    "✈️ ПОЕЗДКИ": ["путешествие"],
    "🍔 ЕДА": ["кафе"],
    "💰 ДОХОДЫ": ["зарплата"],
    "💳 СЧЕТА": ["переводы"],
}


def piggy_row(**cells: Any) -> list[Any]:
    """Строка листа «Копилки»; по умолчанию — «Новый год» к 20.12 каждый год."""
    row = {
        "id": "p3k9za",
        "name": "Новый год",
        "target": 12_000_000,
        "currency": "UZS",
        "deadline": "20.12.2026",
        "repeat": "каждый год",
        "group": "🎉 ПРАЗДНИК",
        "sub": "сезонный",
        "author": "Иван",
        "closed": "",
    } | cells
    return [
        row["id"],
        row["name"],
        row["target"],
        row["currency"],
        row["deadline"],
        row["repeat"],
        row["group"],
        row["sub"],
        "",
        "",
        "",
        "",
        row["author"],
        row["closed"],
    ]


def move_row(**cells: Any) -> list[Any]:
    """Строка журнала; по умолчанию — «отложил» 6 млн в «Новый год» 30.10."""
    row = {
        "day": "30.10.2026",
        "id": "p3k9za",
        "name": "Новый год",
        "amount": 6_000_000,
        "currency": "UZS",
        "kind": "отложил",
        "who": "Иван",
        "key": "",
    } | cells
    return [row[name] for name in row]


def book(*piggies: list[Any], moves: tuple[list[Any], ...] = ()) -> Book:
    return read_book(
        [LIST_HEADER, *piggies],
        [JOURNAL_HEADER, *moves],
        CURRENCIES,
        GROUPS,
        SUBCATEGORIES,
    )


def test_reads_a_piggy_and_its_moves() -> None:
    found = book(
        piggy_row(),
        moves=(move_row(), move_row(day=46300, amount=-1_000_000, kind="Потратил")),
    )
    assert found.piggies == (
        Piggy(
            id="p3k9za",
            row=2,
            name="Новый год",
            target=12_000_000.0,
            currency="UZS",
            deadline=date(2026, 12, 20),
            yearly=True,
            group="🎉 ПРАЗДНИК",
            subcategory="сезонный",
            author="Иван",
            closed=None,
        ),
    )
    moves = found.moves_of("p3k9za")
    assert [(move.day, move.amount, move.kind) for move in moves] == [
        (date(2026, 10, 30), 6_000_000.0, "отложил"),
        (date(2026, 10, 5), -1_000_000.0, "потратил"),  # серийная дата таблицы
    ]
    assert moves[0].currency == "UZS"


def test_long_goal_has_no_deadline_and_maybe_no_target() -> None:
    found = book(
        piggy_row(
            id="m4c8x1",
            name="Машина",
            target="",
            currency="USD",
            deadline="",
            repeat="",
            group="",
            sub="",
        )
    )
    goal = found.piggies[0]
    assert (goal.target, goal.deadline, goal.yearly) == (None, None, False)
    assert (goal.group, goal.subcategory, goal.error) == (None, None, None)


@pytest.mark.parametrize(
    "change",
    [
        {"id": ""},
        {"name": ""},
        {"currency": "EUR"},
        {"target": "abc"},
        {"target": 0},
        {"deadline": "завтра"},
        {"repeat": "раз в год"},
        {"deadline": "", "repeat": "каждый год"},
        {"group": "🚀 КОСМОС"},
        {"group": "💰 ДОХОДЫ", "sub": "зарплата"},
        {"sub": "кафе"},
        {"group": "", "sub": "сезонный"},
    ],
)
def test_broken_rows_keep_their_row_number(change: dict[str, Any]) -> None:
    found = book(
        piggy_row(id="ok0001", name="Первая"),
        piggy_row(**({"id": "ok0002", "name": "Вторая"} | change)),
    )
    first, broken = found.piggies
    assert first.error is None
    assert broken.row == 3
    assert broken.error is not None


def test_a_new_currency_with_moves_is_broken() -> None:
    found = book(piggy_row(currency="USD"), moves=(move_row(currency="UZS"),))
    assert found.piggies[0].error is not None


def test_a_copied_id_keeps_the_first_row(caplog: pytest.LogCaptureFixture) -> None:
    found = book(piggy_row(name="Первая"), piggy_row(name="Копия"))
    assert [piggy.name for piggy in found.piggies] == ["Первая"]
    assert "p3k9za" in caplog.text


def test_moves_of_an_unknown_piggy_are_left_out(
    caplog: pytest.LogCaptureFixture,
) -> None:
    found = book(piggy_row(), moves=(move_row(), move_row(id="zzz999")))
    assert found.moves_of("zzz999") == ()
    assert len(found.moves) == 1
    assert "zzz999" in caplog.text


def test_journal_rows_without_a_date_an_id_or_a_sum_are_skipped() -> None:
    found = book(
        piggy_row(),
        moves=(move_row(day=""), move_row(id=""), move_row(amount="много")),
    )
    assert found.moves == ()


def test_a_closed_piggy_is_marked() -> None:
    found = book(piggy_row(closed="25.10.2026"))
    assert found.piggies[0].closed == date(2026, 10, 25)
