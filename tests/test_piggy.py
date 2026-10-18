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


# --- траты из копилки и остатки (спека 4.2, 4.6) ---

from app.services.analytics_service import transactions_frame  # noqa: E402
from app.services.piggy import (  # noqa: E402
    NOT_FROM,
    Window,
    balances,
    fact_key,
    open_window,
    row_keys,
)
from tests.test_day_budget import FACT_HEADER, RATES, fact_row  # noqa: E402


def frame(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, *rows], RATES)


def spend(
    day: date,
    amount: float,
    sub: str = "сезонный",
    group: str = "🎉 ПРАЗДНИК",
    currency: str = "UZS",
    comment: str = "",
) -> list[Any]:
    row = fact_row(day, group, sub, amount, currency)
    row[5] = comment
    return row


def test_open_window_of_a_yearly_and_a_one_time_piggy() -> None:
    yearly = book(piggy_row()).piggies[0]
    assert open_window(yearly, ()) == Window(
        date(2026, 10, 21), date(2027, 1, 19), date(2026, 12, 20)
    )
    once = book(piggy_row(repeat="один раз"), moves=(move_row(day="01.09.2026"),))
    assert open_window(once.piggies[0], once.moves) == Window(
        date(2026, 9, 1), date(2027, 1, 19), date(2026, 12, 20)
    )
    goal = book(piggy_row(deadline="", repeat="")).piggies[0]
    assert open_window(goal, ()) is None


def test_spending_in_the_window_comes_from_the_piggy() -> None:
    found = balances(
        book(piggy_row(), moves=(move_row(amount=12_000_000),)),
        frame(spend(date(2026, 12, 18), 2_400_000)),
        RATES,
        date(2026, 12, 20),
    )["p3k9za"]
    assert (found.saved, found.spent, found.left, found.collected) == (
        12_000_000,
        2_400_000,
        9_600_000,
        12_000_000,
    )
    assert [item.amount for item in found.spends] == [2_400_000]


def test_spending_outside_the_window_is_ordinary() -> None:
    found = balances(
        book(piggy_row(), moves=(move_row(amount=12_000_000),)),
        frame(spend(date(2026, 9, 1), 2_400_000)),
        RATES,
        date(2026, 12, 20),
    )["p3k9za"]
    assert (found.spent, found.left) == (0, 12_000_000)


def test_spending_over_the_balance_is_ordinary() -> None:
    found = balances(
        book(piggy_row(), moves=(move_row(amount=1_000_000),)),
        frame(spend(date(2026, 12, 18), 2_400_000)),
        RATES,
        date(2026, 12, 20),
    )["p3k9za"]
    assert (found.spent, found.left) == (1_000_000, 0)


def test_a_refund_lowers_the_spending() -> None:
    found = balances(
        book(piggy_row(), moves=(move_row(amount=12_000_000),)),
        frame(
            spend(date(2026, 12, 18), 2_400_000),
            spend(date(2026, 12, 19), -400_000),
        ),
        RATES,
        date(2026, 12, 20),
    )["p3k9za"]
    assert found.spent == 2_000_000


def test_one_article_two_piggies_never_twice() -> None:
    rows = (
        piggy_row(id="dr0001", name="ДР 1", deadline="10.03.2027", sub="день рождения"),
        piggy_row(id="dr0002", name="ДР 2", deadline="25.03.2027", sub="день рождения"),
    )
    moves = (
        move_row(id="dr0001", amount=2_000_000),
        move_row(id="dr0002", amount=5_000_000),
    )
    found = balances(
        book(*rows, moves=moves),
        frame(spend(date(2027, 3, 5), 3_000_000, sub="день рождения")),
        RATES,
        date(2027, 3, 31),
    )
    # ближайший срок первым, остаток траты — следующей копилке, дважды — никогда
    assert (found["dr0001"].spent, found["dr0002"].spent) == (2_000_000, 1_000_000)


def test_marked_spending_is_not_from_the_piggy() -> None:
    spending = frame(spend(date(2026, 12, 18), 2_400_000))
    key = row_keys(spending)[0]
    marked = book(
        piggy_row(),
        moves=(
            move_row(amount=12_000_000),
            move_row(day="18.12.2026", amount=0, kind=NOT_FROM, key=key),
        ),
    )
    assert balances(marked, spending, RATES, date(2026, 12, 20))["p3k9za"].spent == 0
    by_hand = frame(
        spend(date(2026, 12, 18), 2_400_000, comment="подарок коллеге, НЕ из копилки")
    )
    plain = book(piggy_row(), moves=(move_row(amount=12_000_000),))
    assert balances(plain, by_hand, RATES, date(2026, 12, 20))["p3k9za"].spent == 0


def test_another_currency_by_the_table_rate() -> None:
    trip = piggy_row(
        id="tr0001",
        name="Отпуск",
        target=2500,
        currency="USD",
        deadline="01.06.2027",
        group="✈️ ПОЕЗДКИ",
        sub="путешествие",
    )
    found = balances(
        book(trip, moves=(move_row(id="tr0001", amount=500, currency="USD"),)),
        frame(spend(date(2027, 5, 20), 1_200_000, "путешествие", "✈️ ПОЕЗДКИ")),
        RATES,
        date(2027, 5, 31),
    )["tr0001"]
    assert found.spent == pytest.approx(100.0)


def test_a_frozen_window_is_not_counted_again() -> None:
    moves = (
        move_row(amount=12_000_000),
        move_row(
            day="20.01.2027",
            amount=-2_400_000,
            kind="потрачено в окне",
            key="окно:20.12.2026",
        ),
    )
    spending = frame(spend(date(2026, 12, 18), 2_400_000))
    # срок ещё не перенесли — окно уже закрыто итогом, трата из fact не идёт второй раз
    found = balances(book(piggy_row(), moves=moves), spending, RATES, date(2027, 1, 25))
    assert (found["p3k9za"].spent, found["p3k9za"].left) == (2_400_000, 9_600_000)
    rolled = book(piggy_row(deadline="20.12.2027"), moves=moves)
    assert balances(rolled, spending, RATES, date(2027, 1, 25))["p3k9za"].spent == (
        2_400_000
    )


def test_a_closed_window_counts_until_it_is_frozen() -> None:
    found = balances(
        book(piggy_row(), moves=(move_row(amount=12_000_000),)),
        frame(spend(date(2026, 12, 18), 2_400_000)),
        RATES,
        date(2027, 1, 25),  # окно кончилось 19.01, итога в журнале ещё нет
    )["p3k9za"]
    assert found.spent == 2_400_000


def test_collected_keeps_what_was_already_spent() -> None:
    trip = piggy_row(
        id="tr0001",
        name="Отпуск",
        target=2500,
        currency="USD",
        deadline="01.06.2027",
        group="✈️ ПОЕЗДКИ",
        sub="путешествие",
    )
    moves = (
        move_row(id="tr0001", day="01.03.2027", amount=2000, currency="USD"),
        move_row(
            id="tr0001", day="15.04.2027", amount=-800, currency="USD", kind="потратил"
        ),
    )
    found = balances(book(trip, moves=moves), frame(), RATES, date(2027, 4, 30))[
        "tr0001"
    ]
    # билеты купили заранее: в копилке меньше, но к сроку собрано столько же
    assert (found.left, found.collected) == (1200, 2000)


def test_fact_keys_number_repeats_and_ignore_ai() -> None:
    twice = frame(spend(date(2026, 12, 18), 50_000), spend(date(2026, 12, 18), 50_000))
    first, second = row_keys(twice)
    assert second == f"{first}#2"
    day = date(2026, 12, 18)
    assert fact_key(day, "🍔 ЕДА", "кафе", 50_000, "UZS", "VISA", "AI: кафе") == (
        fact_key(day, "🍔 ЕДА", "кафе", 50_000, "UZS", "VISA", "кафе")
    )
