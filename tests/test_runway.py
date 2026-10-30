"""Карточка «💳 Сумы»: сумы по дням срока — до зарплаты, которая ещё не пришла."""

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any

import pytest

from app.domain import Rates
from app.rich import money_text
from app.services.analytics_service import transactions_frame
from app.services.day_budget import day_budget, format_day_budget, runway_line
from app.services.runway import Runway, horizon_runway
from tests.test_day_budget import FACT_HEADER, RATES, fact_row, frame, tab

TODAY = date(2026, 10, 13)
OPENING = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 9_000_000)
LIST = [
    ("Аванс", 15, 1_000_000, "UZS"),
    ("ЗП", 30, 20_000_000, "UZS"),
    ("Квартплата", 18, -500, "USD"),
]


def day(number: int) -> date:
    """День срока по счёту: 1 — сегодня."""
    return TODAY + timedelta(days=number - 1)


def walk(
    now: float,
    pace: float,
    events: Sequence[tuple[date, float]] = (),
    days: int = 10,
    spent: float = 0.0,
    rates: Rates = RATES,
) -> Runway:
    """Срок в days дней с сегодняшним, день срока — последний."""
    return horizon_runway(now, pace, spent, TODAY, day(days), events, rates, day(days))


def test_the_sums_last_until_the_salary() -> None:
    runway = walk(3_000_000, 100_000, spent=40_000)
    # сегодня уже потрачено 40 000 — от темпа дня остаётся 60 000
    assert runway.left == 3_000_000 - 60_000 - 9 * 100_000
    assert (runway.short, runway.last_day, runway.until, runway.dollars) == (
        0,
        None,
        None,
        None,
    )
    assert (runway.balance, runway.pace, runway.payday) == (3_000_000, 100_000, day(10))


def test_the_advance_closes_the_dip() -> None:
    runway = walk(500_000, 200_000, [(day(4), 2_000_000)])
    # 300 000, 100 000, −100 000: минус на 3-й день, аванс 4-го его закрывает
    assert runway.last_day == day(2)
    assert runway.short == 100_000
    assert runway.left == 500_000 + 2_000_000 - 10 * 200_000
    assert runway.until == day(10)  # обменять — до дня срока


def test_payments_come_off_on_their_day() -> None:
    events = [(day(1), -300_000), (day(4), -1_000_000), (day(4), -200_000)]
    runway = walk(2_000_000, 100_000, events, days=6)
    # 1 600 000, 1 500 000, 1 400 000, 100 000, 0, −100 000
    assert runway.last_day == day(5)
    assert (runway.short, runway.left) == (100_000, -100_000)


def test_dollars_round_up_to_ten() -> None:
    runway = walk(0, 550_000)
    assert runway.short == 5_500_000
    assert runway.dollars == 460  # 5,5 млн / 12 000 = 458,3 → вверх до $10
    assert walk(0, 550_000, rates=Rates({"UZS": 1.0})).dollars is None


def test_sums_already_gone() -> None:
    runway = walk(-400_000, 100_000, days=3)
    assert runway.last_day == TODAY - timedelta(days=1)
    assert runway.short == 700_000


def facts(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, *rows], RATES)


def spending(days: int, amount: float, today: date = TODAY) -> list[list[Any]]:
    """Каждый из days дней до today — трата amount с сумовой карты."""
    return [
        fact_row(today - timedelta(days=k), "🍔 ЕДА", "кафе", amount)
        for k in range(1, days + 1)
    ]


def card(
    rows: list[list[Any]],
    items: list[tuple[str, int, float, str]] | None = None,
    today: date = TODAY,
) -> Runway:
    budget = day_budget(facts(*rows), tab(items=items or []), today, RATES, "USD")
    assert budget is not None and budget.runway is not None
    return budget.runway


def test_the_card_runs_until_the_salary_with_the_advance() -> None:
    runway = card([OPENING, *spending(14, 500_000)], LIST)
    assert (runway.balance, runway.pace) == (2_000_000, 500_000)
    # срок — по день ЗП 30-го; аванс 15-го +1 млн; 18-го ровно 0, 19-го уже минус
    assert (runway.payday, runway.last_day) == (date(2026, 10, 30), date(2026, 10, 18))
    # сама зарплата не прибавляется: ниже всего — вечер её дня
    assert (runway.short, runway.left) == (6_000_000, -6_000_000)
    assert (runway.until, runway.dollars) == (date(2026, 10, 30), 500)


def test_without_a_salary_the_card_runs_to_the_month_end() -> None:
    runway = card([OPENING, *spending(14, 500_000)])
    assert (runway.payday, runway.until) == (None, None)
    assert runway.last_day == date(2026, 10, 16)
    assert runway.short == 500_000 * 19 - 2_000_000  # 13–31 октября, 19 дней


def test_a_salary_day_past_the_month_end_counts_on_its_last_day() -> None:
    today = date(2026, 11, 20)
    opening = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_900_000)
    salary = [("ЗП", 31, 5_000_000, "UZS")]  # в ноябре 30 дней
    runway = card([opening, *spending(14, 100_000, today)], salary, today)
    assert (runway.payday, runway.last_day) == (date(2026, 11, 30), date(2026, 11, 24))
    assert runway.short == 600_000


def test_the_pace_is_ordinary_spending_and_the_balance_has_today() -> None:
    rows = [
        OPENING,
        fact_row(TODAY - timedelta(days=1), "🍔 ЕДА", "кафе", 1_400_000),
        fact_row(TODAY - timedelta(days=2), "🍔 ЕДА", "кафе", -280_000),  # возврат
        fact_row(TODAY - timedelta(days=3), "🍔 ЕДА", "кофе", 50, "USD"),
        # обмен: на сумовую карту пришло 1,2 млн (перевод между своими)
        fact_row(TODAY - timedelta(days=4), "💳 СЧЕТА", "переводы", -1_200_000),
        fact_row(TODAY - timedelta(days=5), "💰 ДОХОДЫ", "зарплата", 3_000_000),
        fact_row(TODAY - timedelta(days=20), "🍔 ЕДА", "кафе", 9_999_999),  # до окна
        fact_row(TODAY, "🍔 ЕДА", "кафе", 777_777),  # сегодня: в остатке, не в темпе
    ]
    runway = card(rows)
    assert runway.pace == (1_400_000 - 280_000) / 14
    assert runway.balance == (
        9_000_000 - 1_400_000 + 280_000 + 1_200_000 + 3_000_000 - 9_999_999 - 777_777
    )


def test_a_list_payment_does_not_speed_up_the_pace() -> None:
    camp = [("Лагерь", 10, -3_500_000, "UZS")]
    paid = fact_row(date(2026, 10, 10), "👶 ДЕТИ", "развитие", 3_500_000)
    runway = card([OPENING, *spending(14, 100_000), paid], camp)
    assert runway.pace == pytest.approx(100_000)
    assert runway.balance == 9_000_000 - 1_400_000 - 3_500_000


def test_todays_spending_takes_only_what_is_left_of_the_days_pace() -> None:
    rows = [OPENING, *spending(14, 500_000)]
    coffee = fact_row(TODAY, "🍔 ЕДА", "кофе", 30_000)
    morning = card(rows, LIST)
    evening = card([*rows, coffee], LIST)
    # 30 000 уже ушли — до вечера по темпу уйдёт только 470 000: картина та же
    assert evening.balance == morning.balance - 30_000
    assert (evening.last_day, evening.short, evening.left) == (
        morning.last_day,
        morning.short,
        morning.left,
    )


SALARY_DAY = date(2026, 10, 30)


def sums(
    balance: float,
    last_day: date | None = None,
    short: float = 0,
    dollars: float | None = None,
    left: float = 0,
    payday: date | None = SALARY_DAY,
    name: str = "ЗП",
) -> Runway:
    """Карточка с темпом 500 000; обменять — до дня срока, если не хватает."""
    until = payday if short > 0 else None
    return Runway(balance, 500_000, last_day, short, until, dollars, left, payday, name)


@pytest.mark.parametrize(
    ("runway", "line"),
    [
        (
            sums(7_600_000, left=5_900_000),
            "💳 Сумы 7,60 млн — до зарплаты 30-го хватит, останется ≈ 5,90 млн",
        ),
        (
            sums(7_600_000, left=500_000, payday=None, name=""),
            "💳 Сумы 7,60 млн — до конца месяца хватит, останется ≈ 500 000",
        ),
        (
            sums(17_000_000, left=1_200_000, payday=date(2026, 11, 30)),
            "💳 Сумы 17,00 млн — до зарплаты 30 ноября хватит, останется ≈ 1,20 млн",
        ),
        (
            sums(300_000, left=100_000, payday=TODAY),
            "💳 Сумы 300 000 — до зарплаты сегодня хватит, останется ≈ 100 000",
        ),
        (
            sums(2_000_000, date(2026, 10, 18), 5_500_000, 460),
            "💳 Сумы 2,00 млн — хватит до 18.10 · до зарплаты 30-го обменять ≈ $460",
        ),
        (
            sums(2_000_000, date(2026, 10, 16), 7_500_000, 630, payday=None, name=""),
            "💳 Сумы 2,00 млн — хватит до 16.10 · до конца месяца обменять ≈ $630",
        ),
        (
            sums(2_000_000, date(2026, 10, 18), 5_500_000),
            "💳 Сумы 2,00 млн — хватит до 18.10 · до зарплаты 30-го не хватит 5,50 млн",
        ),
        (
            sums(-400_000, date(2026, 10, 12), 1_900_000, 1_160),
            "💳 Сумы -400 000 — уже закончились · до зарплаты 30-го обменять ≈ $1 160",
        ),
        (
            sums(-400_000, date(2026, 10, 12), 500_000, 50, payday=TODAY),
            "💳 Сумы -400 000 — уже закончились · до зарплаты сегодня обменять ≈ $50",
        ),
        (
            # строк основной зарплаты нет — срок задаёт аванс, как у лимита
            sums(300_000, TODAY, 200_000, 20, payday=date(2026, 10, 15), name="Аванс"),
            "💳 Сумы 300 000 — хватит только на сегодня · до аванса 15-го обменять ≈ $20",
        ),
    ],
)
def test_the_morning_line(runway: Runway, line: str) -> None:
    assert runway_line(runway, TODAY) == line


def test_the_line_follows_the_frozen_money_in_the_morning_message() -> None:
    budget = day_budget(frame(), tab(), date(2026, 10, 2), RATES, "USD")
    assert budget is not None and budget.runway is not None
    lines = format_day_budget(budget).splitlines()
    frozen = next(i for i, text in enumerate(lines) if text.startswith("🧊"))
    # сразу после строк платежей «Заморожено»: подпись срока и квартплата 18-го
    assert lines[frozen + 1 : frozen + 4] == [
        "   на платежи до зарплаты 30-го:",
        f"   Квартплата 18-го — {money_text(400, 'USD')}",
        "💳 Сумы 5,00 млн — до зарплаты 30-го хватит, останется ≈ 7,00 млн",
    ]
