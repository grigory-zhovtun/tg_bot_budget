"""Календарь записей: строки листа, дни записей, закрытие фактом, подсказки."""

from collections.abc import Sequence
from datetime import date
from typing import Any

import pytest

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.calendar import (
    HEADER,
    CalendarRecord,
    CalendarState,
    Hint,
    calendar_state,
    hints,
    new_id,
    occurrences,
    read_calendar,
    record_row,
)
from tests.test_analytics_service import serial
from tests.test_day_budget import FACT_HEADER, fact_row

RATES = Rates({"UZS": 1.0, "USD": 12_000.0})
TODAY = date(2026, 10, 15)  # четверг
END = date(2026, 10, 31)
SINCE = date(2026, 10, 1)


def record(
    repeat: str = "once",
    start: date = date(2026, 10, 17),
    amount: float = 320_000,
    sub: str = "развитие",
    record_id: str = "k7f2qa",
    weekdays: frozenset[int] = frozenset(),
) -> CalendarRecord:
    return CalendarRecord(
        record_id, start, repeat, weekdays, "👶 ДЕТИ", sub, "UZS", amount, "", "Тест"
    )


def facts(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, *rows], RATES)


def state(
    rows: Sequence[list[Any]] = (),
    records: Sequence[CalendarRecord] = (),
    used: frozenset[int] = frozenset(),
) -> CalendarState:
    return calendar_state(facts(*rows), records, TODAY, END, SINCE, used, used)


def test_rows_of_the_sheet_become_records() -> None:
    rows = [
        HEADER,
        ["k7f2qa", serial(date(2026, 10, 17)), "нет", "", "👶 ДЕТИ", "развитие",
         "UZS", 320000, "Детский центр", "Тест"],
        ["m3a9zt", "01.10.2026", "каждый день", "", "🍔 ЕДА", "кофе", "uzs",
         "30 000", "", "Тест"],
        ["p1c4xy", serial(date(2026, 10, 6)), "по дням недели", "пн ср", "👶 ДЕТИ",
         "развитие", "UZS", 80000.0, "Кружок", ""],
        ["q9w8er", serial(date(2026, 10, 31)), "Раз в месяц", "", "🏚️ ДОМ",
         "интернет", "USD", 12.5, "Интернет", "Тест"],
    ]  # fmt: skip
    found = read_calendar(rows)
    assert [(r.id, r.repeat, r.start, r.amount) for r in found] == [
        ("k7f2qa", "once", date(2026, 10, 17), 320_000),
        ("m3a9zt", "daily", date(2026, 10, 1), 30_000),
        ("p1c4xy", "weekdays", date(2026, 10, 6), 80_000),
        ("q9w8er", "monthly", date(2026, 10, 31), 12.5),
    ]
    assert found[0].key == "развитие|UZS" and found[0].title == "Детский центр"
    assert (found[1].currency, found[1].title) == ("UZS", "кофе")  # без названия
    assert found[2].weekdays == frozenset({1, 3})


def test_sums_typed_by_hand_are_read() -> None:
    row = ["a1b2c3", "17.10.2026", "нет", "", "👶 ДЕТИ", "развитие", "UZS",
           "1\u00a0500,5", "", ""]  # fmt: skip
    assert read_calendar([HEADER, row])[0].amount == 1500.5


def test_broken_rows_are_skipped() -> None:
    day = serial(date(2026, 10, 17))
    rows = [
        HEADER,
        ["", day, "нет", "", "👶 ДЕТИ", "развитие", "UZS", 1, "", ""],  # нет ID
        ["a1", "когда-нибудь", "нет", "", "👶 ДЕТИ", "развитие", "UZS", 1, "", ""],
        ["a2", day, "раз в год", "", "👶 ДЕТИ", "развитие", "UZS", 1, "", ""],
        ["a3", day, "по дням недели", "", "👶 ДЕТИ", "развитие", "UZS", 1, "", ""],
        ["a4", day, "по дням недели", "пн пятница", "👶 ДЕТИ", "развитие", "UZS", 1,
         "", ""],
        ["a5", day, "нет", "", "👶 ДЕТИ", "развитие", "UZS", 0, "", ""],
        ["a6", day, "нет", "", "", "развитие", "UZS", 1, "", ""],
        [],
    ]  # fmt: skip
    assert read_calendar(rows) == ()
    assert read_calendar([]) == ()


@pytest.mark.parametrize(
    ("rule", "first", "last", "days"),
    [
        (record(), date(2026, 10, 15), END, [17]),
        (record(start=date(2026, 10, 10)), date(2026, 10, 15), END, []),
        (record("daily", date(2026, 10, 29)), date(2026, 10, 15), END, [29, 30, 31]),
        (record("daily", date(2026, 10, 1)), date(2026, 10, 30), END, [30, 31]),
        (
            record("weekdays", date(2026, 10, 6), weekdays=frozenset({1, 3})),
            date(2026, 10, 15),
            END,
            [19, 21, 26, 28],
        ),
        (record("monthly", date(2026, 9, 5)), date(2026, 10, 1), END, [5]),
        (
            record("monthly", date(2026, 10, 31)),
            date(2026, 11, 1),
            date(2026, 11, 30),
            [30],  # 31-е в ноябре — его последний день
        ),
        (record("monthly", date(2026, 10, 20)), date(2026, 10, 1), END, [20]),
        (record("monthly", date(2026, 11, 3)), date(2026, 10, 1), END, []),
    ],
)
def test_days_of_a_record(
    rule: CalendarRecord, first: date, last: date, days: list[int]
) -> None:
    assert [day.day for day in occurrences(rule, first, last)] == days


def test_a_record_is_written_as_plain_values() -> None:
    rule = CalendarRecord("k7f2qa", date(2026, 10, 6), "weekdays", frozenset({3, 1}),
                          "👶 ДЕТИ", "развитие", "UZS", 80_000, "Кружок", "Тест")  # fmt: skip
    assert record_row(rule) == ["k7f2qa", "06.10.2026", "по дням недели", "пн ср",
                                "👶 ДЕТИ", "развитие", "UZS", 80_000, "Кружок",
                                "Тест"]  # fmt: skip
    assert read_calendar([HEADER, record_row(rule)]) == (rule,)


def test_a_new_id_starts_with_a_letter_and_is_not_taken() -> None:
    taken = {new_id(set()) for _ in range(50)}
    code = new_id(taken)
    assert code not in taken
    assert len(code) == 6 and code[0].isalpha() and code.isalnum() and code.islower()


def test_records_are_open_on_their_days() -> None:
    coffee = record("daily", date(2026, 10, 1), 30_000, "кофе", "m3a9zt")
    found = state(records=[record(), coffee])
    kids = [
        (o.day.day, o.morning, o.now) for o in found.occurrences if o.record == record()
    ]
    assert kids == [(17, 320_000, 320_000)]
    assert len([o for o in found.occurrences if o.record == coffee]) == 17
    assert [o.day.day for o in found.occurrences][:4] == [15, 16, 17, 17]
    assert found.closed_morning == found.closed_now == frozenset()


def test_a_one_off_paid_early_is_closed_by_one_payment() -> None:
    tomorrow = record(start=date(2026, 10, 16))
    paid = fact_row(date(2026, 10, 14), "👶 ДЕТИ", "развитие", 310_000)  # ±10 %
    book = fact_row(date(2026, 10, 14), "👶 ДЕТИ", "развитие", 30_000)
    found = state([paid, book], [tomorrow])
    only = found.occurrences[0]
    assert (only.day.day, only.morning, only.now) == (16, 0, 0)
    assert found.closed_morning == found.closed_now == frozenset({0})


def test_a_payment_too_far_from_the_record_does_not_close_it() -> None:
    tomorrow = record(start=date(2026, 10, 16))
    off = state(
        [fact_row(date(2026, 10, 14), "👶 ДЕТИ", "развитие", 250_000)], [tomorrow]
    )
    early = state(
        [fact_row(date(2026, 10, 12), "👶 ДЕТИ", "развитие", 320_000)], [tomorrow]
    )
    taken = state(
        [fact_row(date(2026, 10, 14), "👶 ДЕТИ", "развитие", 320_000)],
        [tomorrow],
        used=frozenset({0}),  # операцией уже закрыт платёж жёлтого списка
    )
    assert [s.occurrences[0].now for s in (off, early, taken)] == [320_000] * 3


def test_todays_spending_covers_todays_records() -> None:
    coffee = record("daily", date(2026, 10, 1), 30_000, "кофе", "m3a9zt")
    partly = state([fact_row(TODAY, "🍔 ЕДА", "кофе", 25_000)], [coffee])
    today = partly.occurrences[0]
    assert (today.morning, today.covered) == (30_000, 25_000)
    assert today.now == pytest.approx(5_000)
    assert partly.occurrences[1].now == 30_000  # завтра — как было
    done = state([fact_row(TODAY, "🍔 ЕДА", "кофе", 28_000)], [coffee]).occurrences[0]
    assert (done.now, done.covered) == (0, pytest.approx(28_000))
    more = state([fact_row(TODAY, "🍔 ЕДА", "кофе", 45_000)], [coffee]).occurrences[0]
    assert (more.now, more.covered) == (0, 30_000)


def test_a_one_off_paid_today_is_open_in_the_morning_and_closed_now() -> None:
    found = state(
        [fact_row(TODAY, "👶 ДЕТИ", "развитие", 320_000)], [record(start=TODAY)]
    )
    only = found.occurrences[0]
    assert (only.morning, only.now, only.covered) == (320_000, 0, 0)
    assert (found.closed_morning, found.closed_now) == (frozenset(), frozenset({0}))


def test_the_morning_match_stays_when_a_closer_payment_comes_today() -> None:
    rows = [
        fact_row(date(2026, 10, 14), "👶 ДЕТИ", "развитие", 300_000),
        fact_row(TODAY, "👶 ДЕТИ", "развитие", 320_000),
    ]
    found = state(rows, [record(start=date(2026, 10, 16))])
    assert found.closed_morning == found.closed_now == frozenset({0})


def test_one_offs_of_the_last_two_weeks_close_too() -> None:
    last_week = record(start=date(2026, 10, 8))
    found = state(
        [fact_row(date(2026, 10, 8), "👶 ДЕТИ", "развитие", 320_000)], [last_week]
    )
    assert found.occurrences == ()  # прошла — в прогнозе её нет
    assert found.closed_morning == frozenset({0})


def test_hints_from_the_history_of_an_article() -> None:
    rows = [
        fact_row(date(2026, 7, 1), "👶 ДЕТИ", "развитие", 999_000),  # старше 90 дней
        fact_row(date(2026, 8, 20), "👶 ДЕТИ", "развитие", 320_000),
        fact_row(date(2026, 9, 26), "👶 ДЕТИ", "развитие", 340_400),
        fact_row(date(2026, 10, 10), "👶 ДЕТИ", "развитие", 300_000),
        fact_row(date(2026, 10, 12), "👶 ДЕТИ", "развитие", -50_000),  # возврат
        fact_row(TODAY, "👶 ДЕТИ", "развитие", 1_000_000),  # сегодня — не история
        fact_row(date(2026, 10, 1), "🍔 ЕДА", "кофе", 12.6, "USD"),
    ]
    found = hints(facts(*rows), TODAY)
    # 30 дней: 340 400 + 300 000 = 640 400 / 30 ≈ 21 347 → 21 000
    assert found["развитие|UZS"] == Hint(typical=320_000, last=300_000, per_day=21_000)
    assert found["кофе|USD"] == Hint(typical=13, last=13, per_day=0)
    assert "кафе|UZS" not in found
