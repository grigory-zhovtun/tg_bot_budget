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


# --- план взносов, 10 %, подушка, «хватает ли» (спека 4.3–4.5, 4.8) ---

from types import SimpleNamespace  # noqa: E402

from app.services.piggy import (  # noqa: E402
    Balance,
    Enough,
    PiggyState,
    Screen,
    distribute,
    money_day,
    savings_screen,
)
from tests.test_day_budget import tab  # noqa: E402

TODAY = date(2026, 10, 17)
TRIP = {
    "id": "tr0001",
    "name": "Отпуск",
    "target": 2500,
    "currency": "USD",
    "deadline": "01.06.2027",
    "group": "✈️ ПОЕЗДКИ",
    "sub": "путешествие",
}
GOAL = {
    "id": "car001",
    "name": "Машина",
    "target": "",
    "currency": "USD",
    "deadline": "",
    "repeat": "",
    "group": "",
    "sub": "",
}


def frozen(amount: float, change: float = 0.0) -> Any:
    """Дневной бюджет — только то, что берут копилки: «Заморожено» в долларах."""
    return SimpleNamespace(
        frozen=amount,
        frozen_change=change,
        frozen_currency="USD",
        frozen_uzs=amount * 12_000,
    )


def screen(
    *piggies: list[Any],
    moves: tuple[list[Any], ...] = (),
    fact: tuple[list[Any], ...] = (),
    today: date = TODAY,
    budget: Any = None,
) -> Screen:
    # список вкладки: аванс 15-го и ЗП 30-го — день денег 30-е, поступлений 12 млн
    return savings_screen(
        book(*piggies, moves=moves), frame(*fact), tab(), today, RATES, budget, 0.1, 6
    )


def state(found: Screen, piggy_id: str) -> PiggyState:
    return next(item for item in found.piggies if item.piggy.id == piggy_id)


def spending_months(amount: float = 10_000_000) -> tuple[list[Any], ...]:
    """Траты июля–сентября по 10 млн: средние траты в месяц — 10 млн."""
    return tuple(
        fact_row(date(2026, month, 3), "🍔 ЕДА", "кафе", amount) for month in (7, 8, 9)
    )


def test_money_day_is_the_last_income_of_the_list() -> None:
    assert money_day(tab()) == 30
    assert money_day(None) is None


def test_the_spec_example_steps_down() -> None:
    found = screen(piggy_row(), piggy_row(**TRIP))
    totals = [round(month.total) for month in found.plan[:9]]
    assert totals == [6_000_000] * 2 + [5_000_000] * 6 + [3_500_000]
    assert len(found.plan) == 12
    assert found.plan[0].items == {"p3k9za": 6_000_000}
    assert found.plan[2].items == {"tr0001": 5_000_000}
    assert found.plan[1].ready == ("p3k9za",)
    assert found.plan[7].ready == ("tr0001",)
    assert (state(found, "p3k9za").this_month, state(found, "tr0001").this_month) == (
        6_000_000,
        0,
    )
    assert state(found, "p3k9za").status == "on_track"


def test_a_deadline_before_any_money_day_is_late() -> None:
    found = screen(
        piggy_row(target=1_000_000, deadline="25.10.2026", repeat="один раз")
    )
    late = state(found, "p3k9za")
    assert (late.status, late.short) == ("late", 1_000_000)


def test_a_contribution_after_the_money_day_counts_for_its_month() -> None:
    found = screen(
        piggy_row(),
        moves=(move_row(day="31.10.2026", amount=6_000_000),),
        today=date(2026, 10, 31),
    )
    piggy = state(found, "p3k9za")
    assert (piggy.this_month, piggy.left_this_month) == (6_000_000, 0)


def test_rounds_up_to_thousands_and_whole_dollars() -> None:
    sums = screen(piggy_row(target=11_998_002))
    assert state(sums, "p3k9za").this_month == 6_000_000
    dollars = screen(piggy_row(**(TRIP | {"target": 420.4, "deadline": "20.12.2026"})))
    assert state(dollars, "tr0001").this_month == 211


def test_no_target_asks_for_a_sum() -> None:
    assert state(screen(piggy_row(target="")), "p3k9za").status == "no_target"


def test_done_when_collected_reaches_the_target() -> None:
    found = screen(piggy_row(), moves=(move_row(day="10.10.2026", amount=12_000_000),))
    assert state(found, "p3k9za").status == "done"


def test_the_share_goes_to_the_cushion_under_a_month() -> None:
    found = screen(piggy_row(**GOAL), fact=spending_months(), budget=frozen(500))
    assert found.share_to == "cushion"  # 6 млн при тратах 10 млн — 0,6 месяца
    assert state(found, "car001").this_month == 0


def test_the_share_splits_between_a_month_and_the_target() -> None:
    found = screen(piggy_row(**GOAL), fact=spending_months(), budget=frozen(2000))
    assert found.share_to == "split"  # 24 млн — 2,4 месяца
    # 10 % от поступлений 12 млн — 1,2 млн, половина цели: 600 000 = $50
    assert state(found, "car001").this_month == 50
    assert found.plan[0].goals == {"car001": 600_000}
    assert found.plan[0].cushion == 600_000


def test_the_share_goes_to_goals_from_the_target() -> None:
    found = screen(piggy_row(**GOAL), fact=spending_months(), budget=frozen(6000))
    assert found.share_to == "goals"  # 72 млн — 7,2 месяца из 6
    assert state(found, "car001").this_month == 100


def test_a_goal_at_its_target_passes_its_part() -> None:
    laptop = GOAL | {"id": "lap001", "name": "Ноутбук"}
    car = GOAL | {"target": 1000}
    found = screen(
        piggy_row(**car),
        piggy_row(**laptop),
        moves=(move_row(id="car001", day="01.09.2026", amount=1000, currency="USD"),),
        fact=spending_months(),
        budget=frozen(2000),  # подушка 1000 $ = 12 млн — 1,2 месяца
    )
    assert state(found, "car001").status == "done"
    assert (state(found, "car001").this_month, state(found, "lap001").this_month) == (
        0,
        50,
    )


def test_the_cushion_is_frozen_minus_piggies_as_of_last_night() -> None:
    found = screen(
        piggy_row(**TRIP),
        moves=(
            move_row(id="tr0001", day="16.10.2026", amount=400, currency="USD"),
            move_row(id="tr0001", day="17.10.2026", amount=100, currency="USD"),
        ),
        fact=spending_months(),
        budget=frozen(1500),
    )
    # сегодняшние доллары «Заморожено» увидит только завтра — и копилка тоже
    assert (found.envelopes, found.over) == (400, 0)
    assert found.cushion is not None
    assert found.cushion.amount == 1100


def test_piggies_over_the_frozen_money_warn() -> None:
    found = screen(
        piggy_row(**TRIP),
        moves=(move_row(id="tr0001", day="16.10.2026", amount=400, currency="USD"),),
        fact=spending_months(),
        budget=frozen(300),
    )
    assert found.cushion is not None
    assert (found.cushion.amount, found.over) == (0, 100)


def test_enough_compares_money_for_life_with_ordinary_spending() -> None:
    fact = tuple(
        row
        for month in range(4, 10)
        for row in (
            fact_row(date(2026, month, 2), "💰 ДОХОДЫ", "зарплата", 40_000_000),
            fact_row(date(2026, month, 3), "🍔 ЕДА", "кафе", 32_000_000),
            fact_row(date(2026, month, 4), "🎉 ПРАЗДНИК", "сезонный", 2_000_000),
        )
    )
    found = screen(piggy_row(), fact=fact)
    # отложить: 6 млн в «Новый год» и 10 % от 40 млн; обычные траты — без статьи копилки
    assert found.enough == Enough(
        income=40_000_000,
        save=10_000_000,
        life=30_000_000,
        ordinary=32_000_000,
        gap=-2_000_000,
    )


def _state(piggy_id: str, currency: str, left: float, deadline: date | None) -> Any:
    piggy = Piggy(piggy_id, 2, piggy_id, None, currency, deadline, True, None, None,
                  "", None)  # fmt: skip
    status = "goal" if deadline is None else "on_track"
    return PiggyState(piggy, Balance(0, 0, 0, 0, ()), status, left, left, 0, 0, ())


def test_distribute_by_deadlines_then_goals() -> None:
    found = Screen(
        TODAY,
        (
            _state("ny", "UZS", 6_000_000, date(2026, 12, 20)),
            _state("trip", "USD", 210, date(2027, 6, 1)),
            _state("goal", "UZS", 1_500_000, None),
        ),
        (),
        None,
        None,
        0,
        0,
        "split",
        None,
    )
    assert distribute(found, 12_000_000, "UZS", RATES) == [
        ("ny", 6_000_000),
        ("trip", 210),
        ("goal", 1_500_000),
    ]  # остальные 1 980 000 — подушке, их не пишем
    assert distribute(found, 7_000_000, "UZS", RATES) == [
        ("ny", 6_000_000),
        ("trip", 83.33),
    ]


def test_a_goal_gets_no_more_than_it_lacks() -> None:
    car = GOAL | {"target": 1000}
    found = screen(
        piggy_row(**car),
        moves=(move_row(id="car001", day="01.09.2026", amount=980, currency="USD"),),
        fact=spending_months(),
        budget=frozen(6000),  # подушка 5020 $ ≈ 6 месяцев — вся доля целям
    )
    # не хватает 20 $ = 240 000: больше цель не берёт, остальное — подушке
    assert found.plan[0].goals == {"car001": 240_000}
    assert found.plan[0].cushion == 960_000
    assert state(found, "car001").this_month == 20
