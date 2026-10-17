"""Прогноз до конца месяца: деньги сейчас, план и темп в день, события, минус."""

from collections.abc import Sequence
from datetime import date
from typing import Any

import pytest

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.calendar import CalendarRecord
from app.services.day_budget import ForecastItem
from app.services.forecast import (
    ArticleState,
    CurrencyForecast,
    Dip,
    Evening,
    Event,
    currency_forecast,
    month_forecast,
)
from app.services.plan import Article
from tests.test_day_budget import FACT_HEADER, fact_row

RATES = Rates({"UZS": 1.0, "USD": 12_000.0, "RUB": 150.0})
TODAY = date(2026, 10, 15)  # четверг; до конца октября 17 дней вместе с сегодня
OPENING = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 10_000_000)


def facts(*rows: list[Any]) -> Any:
    return transactions_frame([FACT_HEADER, OPENING, *rows], RATES)


def article(
    sub: str, plan: float, currency: str = "UZS", kind: str = "spend"
) -> Article:
    return Article(f"{sub}|{currency}", kind, "🍔 ЕДА", sub, currency, plan, 2)


def item(
    name: str, day: int, amount: float, row: int, currency: str = "UZS"
) -> ForecastItem:
    return ForecastItem(
        name, day, amount, currency, amount * RATES.to_uzs[currency], row
    )


def uzs(
    rows: Sequence[list[Any]] = (),
    articles: Sequence[Article] = (),
    items: Sequence[ForecastItem] = (),
    today: date = TODAY,
) -> CurrencyForecast:
    return currency_forecast(facts(*rows), articles, items, today, "UZS")


def test_money_now_is_the_whole_currency_with_todays_rows() -> None:
    rows = [
        fact_row(date(2026, 10, 3), "🍔 ЕДА", "кафе", 400_000),
        fact_row(date(2026, 10, 5), "💰 ДОХОДЫ", "зарплата", 2_000_000),
        fact_row(date(2026, 10, 9), "🍔 ЕДА", "кафе", -50_000),  # возврат
        fact_row(TODAY, "🍔 ЕДА", "кофе", 30_000),  # сегодня — уже в деньгах
        fact_row(date(2026, 10, 16), "🍔 ЕДА", "кафе", 999_999),  # завтра — нет
        fact_row(date(2026, 10, 4), "🍔 ЕДА", "кафе", 70, "USD"),  # другая валюта
    ]
    assert uzs(rows).now == 10_000_000 - 400_000 + 2_000_000 + 50_000 - 30_000


def test_plan_per_day_spreads_the_rest_of_the_articles() -> None:
    rows = [
        fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 1_000_000),
        fact_row(date(2026, 10, 3), "👶 ДЕТИ", "развитие", 200_000),
    ]
    articles = [article("кафе", 2_700_000), article("развитие", 1_200_000)]
    kids = item("Детский центр", 17, -340_000, row=13)
    # (1,7 млн + 1,0 млн − 0,34 млн платежа из списка) / 17 дней
    assert uzs(rows, articles, [kids]).per_day_plan == pytest.approx(2_360_000 / 17)


def test_overspent_articles_give_nothing_and_fact_ignores_the_group() -> None:
    rows = [
        fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 900_000),  # план 500 000
        fact_row(date(2026, 10, 3), "🏚️ ДОМ", "продукты", 300_000),
        fact_row(date(2026, 10, 4), "🍔 ЕДА", "продукты", 200_000),  # та же статья
    ]
    articles = [
        article("кафе", 500_000),
        article("продукты", 2_200_000),
        article("зарплата", 9_000_000, kind="income"),
        article("кофе", 100, currency="USD"),
    ]
    assert uzs(rows, articles).per_day_plan == pytest.approx(1_700_000 / 17)


def test_two_rows_of_one_article_add_up() -> None:
    rows = [fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 600_000)]
    articles = [article("кафе", 500_000), article("кафе", 500_000)]
    assert uzs(rows, articles).per_day_plan == pytest.approx(400_000 / 17)


def test_today_subtracts_only_what_is_left_of_the_day() -> None:
    articles = [article("кафе", 1_700_000)]  # 100 000 в день
    morning = uzs((), articles)
    coffee = fact_row(TODAY, "🍔 ЕДА", "кофе", 30_000)
    evening = uzs([coffee], articles)
    assert evening.per_day_plan == morning.per_day_plan == pytest.approx(100_000)
    assert evening.spent_today == 30_000
    # 30 000 уже ушли из денег сейчас — за день вычитается только 70 000
    assert evening.days[0].plan == pytest.approx(morning.days[0].plan)
    assert evening.days[0].plan == pytest.approx(10_000_000 - 100_000)
    big = fact_row(TODAY, "🍔 ЕДА", "кафе", 250_000)
    assert uzs([big], articles).days[0].plan == pytest.approx(10_000_000 - 250_000)


def test_a_payment_made_today_is_neither_an_event_nor_todays_spending() -> None:
    kids = item("Детский центр", 15, -320_000, row=13)
    paid = fact_row(TODAY, "👶 ДЕТИ", "развитие", 320_000)
    coffee = fact_row(TODAY, "🍔 ЕДА", "кофе", 30_000)
    articles = [article("развитие", 320_000), article("кафе", 1_380_000)]
    line = uzs([paid, coffee], articles, [kids])
    assert line.events == ()
    assert line.spent_today == 30_000
    # к утру платёж ещё не ушёл: он вычтен из плана и в день не попадает
    assert line.per_day_plan == pytest.approx(1_380_000 / 17)
    assert line.days[0].plan == pytest.approx(
        10_000_000 - 350_000 - (1_380_000 / 17 - 30_000)
    )


def test_a_refund_today_does_not_make_the_day_longer() -> None:
    refund = fact_row(TODAY, "🍔 ЕДА", "кафе", -50_000)
    line = uzs([refund], [article("кафе", 1_700_000)])
    assert line.spent_today == 0
    # возврат пришёл на карту, а за день по плану всё равно уйдёт 100 000
    assert line.days[0].plan == pytest.approx(10_000_000 + 50_000 - 100_000)


def test_pace_is_ordinary_spending_of_the_two_weeks_before_today() -> None:
    rent = item("Квартплата", 10, -2_000_000, row=14)
    rows = [
        fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 1_400_000),  # 14 дней назад
        fact_row(date(2026, 9, 30), "🍔 ЕДА", "кафе", 9_000_000),  # 15 дней — нет
        fact_row(date(2026, 10, 14), "🍔 ЕДА", "кафе", -140_000),  # возврат
        fact_row(date(2026, 10, 8), "💳 СЧЕТА", "переводы", 500_000),
        fact_row(date(2026, 10, 9), "💰 ДОХОДЫ", "зарплата", 3_000_000),
        fact_row(date(2026, 10, 10), "🏚️ ДОМ", "квартплата", 2_000_000),  # из списка
        fact_row(TODAY, "🍔 ЕДА", "кафе", 700_000),  # сегодня — не в темпе
    ]
    assert uzs(rows, items=[rent]).per_day_pace == pytest.approx(1_260_000 / 14)


def test_refunds_never_make_the_pace_negative() -> None:
    refund = fact_row(date(2026, 10, 14), "🍔 ЕДА", "кафе", -500_000)
    assert uzs([refund]).per_day_pace == 0


def test_events_are_what_is_still_ahead() -> None:
    items = [
        item("Аванс", 13, 2_000_000, row=12),  # пришло 1,5 млн из 2 — ждём остаток
        item("ЗП", 30, 10_000_000, row=13),
        item("Детский центр", 10, -340_000, row=14),  # оплачен
        item("Кружок", 12, -100_000, row=15),  # опоздал
        item("Квартплата", 18, -500, row=16, currency="USD"),  # другая валюта
    ]
    rows = [
        fact_row(date(2026, 10, 13), "💰 ДОХОДЫ", "зарплата", 1_500_000),
        fact_row(date(2026, 10, 10), "👶 ДЕТИ", "развитие", 340_000),
    ]
    assert uzs(rows, items=items).events == (
        Event(15, "Аванс", 500_000, True),
        Event(15, "Кружок", -100_000, True),
        Event(30, "ЗП", 10_000_000, False),
    )


def test_day_31_of_a_short_month_is_its_last_day() -> None:
    november = date(2026, 11, 20)
    line = uzs(items=[item("ЗП", 31, 5_000_000, row=12)], today=november)
    assert line.events == (Event(30, "ЗП", 5_000_000, False),)
    assert [point.day for point in line.days] == list(range(20, 31))


def test_on_the_first_day_the_plan_is_the_whole_month() -> None:
    first = date(2026, 11, 1)
    # октябрьская трата: в темпе ноября есть, в факте статьи ноября — нет
    rows = [fact_row(date(2026, 10, 25), "🍔 ЕДА", "кафе", 1_400_000)]
    line = uzs(rows, [article("кафе", 3_000_000)], today=first)
    assert line.per_day_plan == pytest.approx(100_000)  # 3 млн / 30 дней
    assert line.per_day_pace == pytest.approx(100_000)  # 18–31 октября


def test_on_the_last_day_the_whole_rest_is_today() -> None:
    last = date(2026, 10, 31)
    line = uzs(articles=[article("кафе", 300_000)], today=last)
    assert line.per_day_plan == pytest.approx(300_000)
    assert line.days == (Evening(31, 9_700_000, 10_000_000),)


def test_the_example_of_the_spec() -> None:
    rows = [
        fact_row(date(2026, 9, 25), "💰 ДОХОДЫ", "зарплата", 8_710_000),
        *(
            fact_row(date(2026, 10, day), "🍔 ЕДА", "кафе", 850_000)
            for day in range(1, 15)
        ),
    ]
    articles = [article("кафе", 19_890_000), article("развитие", 320_000)]
    items = [
        item("Аванс", 15, 2_500_000, row=12),
        item("Детский центр", 17, -320_000, row=13),
        item("ЗП", 30, 30_000_000, row=14),
    ]
    line = uzs(rows, articles, items)
    assert line.now == pytest.approx(6_810_000)
    assert line.per_day_plan == pytest.approx(470_000)
    assert line.per_day_pace == pytest.approx(850_000)
    evenings = {point.day: point for point in line.days}
    assert evenings[15].plan == pytest.approx(8_840_000)
    assert evenings[15].pace == pytest.approx(8_460_000)
    assert evenings[17].plan == pytest.approx(7_580_000)
    assert evenings[17].pace == pytest.approx(6_440_000)
    assert evenings[25].plan == pytest.approx(3_820_000)
    assert evenings[25].pace == pytest.approx(-360_000)
    assert evenings[30].plan == pytest.approx(31_470_000)
    assert evenings[30].pace == pytest.approx(25_390_000)
    assert line.plan_dip is None
    assert line.pace_dip == Dip(25, 30)


def test_a_dip_that_does_not_come_back_has_no_end() -> None:
    spree = fact_row(date(2026, 10, 14), "🍔 ЕДА", "кафе", 7_000_000)  # 500 000 в день
    line = uzs([spree])
    # сейчас 3 млн: 20-го вечером ровно ноль — это ещё не минус, 21-го — минус
    assert line.days[5] == Evening(20, 3_000_000, 0)
    assert line.pace_dip == Dip(21, None)
    assert line.plan_dip is None


def test_currencies_follow_uzs_usd_rub_then_the_alphabet() -> None:
    codes = ["RUB", "EUR", "UZS", "USD", "UZS", ""]
    found = month_forecast(facts(), (), (), TODAY, RATES, codes)
    assert [line.code for line in found.currencies] == ["UZS", "USD", "RUB", "EUR"]
    assert found.counted == frozenset({"UZS", "USD", "RUB"})  # у евро нет курса
    assert found.end == date(2026, 10, 31)


def test_total_is_every_currency_at_todays_rate() -> None:
    rows = [
        fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_000, "USD"),
        fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 20_000, "RUB"),
        fact_row(date(2026, 10, 14), "🍔 ЕДА", "кофе", 14, "USD"),  # $1 в день
    ]
    found = month_forecast(facts(*rows), (), (), TODAY, RATES, ["UZS", "USD", "RUB"])
    first = found.total_uzs[0]
    # 10 млн + ($1000 − $14 − $1 за сегодня) × 12 000 + 20 000 ₽ × 150
    assert first.pace == pytest.approx(10_000_000 + 985 * 12_000 + 3_000_000)
    assert first.plan == pytest.approx(10_000_000 + 986 * 12_000 + 3_000_000)
    assert found.total_usd[0].pace == pytest.approx(first.pace / 12_000)
    assert [point.day for point in found.total_uzs] == list(range(15, 32))


def test_without_a_dollar_rate_there_is_no_total_in_dollars() -> None:
    found = month_forecast(facts(), (), (), TODAY, Rates({"UZS": 1.0}), ["UZS"])
    assert found.total_usd == ()
    assert found.past_usd == ()
    assert found.total_uzs[0].plan == 10_000_000


def test_a_currency_without_a_rate_stays_out_of_the_total() -> None:
    euro = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 300, "EUR")
    frame = transactions_frame([FACT_HEADER, OPENING, euro], RATES, keep_unknown=True)
    found = month_forecast(frame, (), (), TODAY, RATES, ["UZS", "EUR"])
    assert [(line.code, line.now) for line in found.currencies] == [
        ("UZS", 10_000_000),
        ("EUR", 300),
    ]
    assert found.counted == frozenset({"UZS"})
    assert found.total_uzs[0].plan == 10_000_000
    assert found.past_uzs[0].evening == 10_000_000  # евро и в прошлом не во «Всего»


def test_a_list_row_in_a_currency_without_a_card_is_left_out() -> None:
    rubles = item("Подписка", 20, -1_000, row=12, currency="RUB")
    found = month_forecast(facts(), (), [rubles], TODAY, RATES, ["UZS"])
    assert [line.code for line in found.currencies] == ["UZS"]
    assert found.currencies[0].events == ()
    assert found.total_uzs[-1].plan == 10_000_000


def test_the_total_goes_below_zero_only_when_all_money_does() -> None:
    rows = [
        fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 500, "USD"),  # 6 млн
        fact_row(date(2026, 10, 14), "🍔 ЕДА", "кафе", 14_000_000),  # 1 млн в день
    ]
    found = month_forecast(facts(*rows), (), (), TODAY, RATES, ["UZS", "USD"])
    assert found.currencies[0].pace_dip == Dip(15, None)  # сумы: −4 млн уже сейчас
    # всего: 15-го 1 млн, 16-го ноль — ещё не минус, 17-го минус
    assert [point.pace for point in found.total_uzs[:3]] == [
        1_000_000,
        0,
        -1_000_000,
    ]
    assert found.pace_dip == Dip(17, None)
    assert found.plan_dip is None


def test_a_payment_planned_in_another_currency_is_not_counted_twice() -> None:
    # квартплата запланирована в сумах (7,8 млн), а платится из списка долларами
    articles = [article("кафе", 3_400_000), article("квартплата", 7_800_000)]
    rent = item("Квартплата", 18, -650, row=12, currency="USD")
    found = month_forecast(facts(), articles, [rent], TODAY, RATES, ["UZS", "USD"])
    sums, dollars = found.currencies
    assert dollars.events == (Event(18, "Квартплата", -650, False),)
    assert dollars.per_day_plan == 0
    # 650 USD × 12 000 = 7,8 млн уйдут долларами — по дням в сумах только кафе
    assert sums.per_day_plan == pytest.approx(3_400_000 / 17)


def test_a_payment_only_partly_planned_in_its_currency_takes_the_rest_from_sums() -> (
    None
):
    articles = [
        article("кафе", 3_400_000),
        article("квартплата", 300, currency="USD"),
        article("квартплата", 4_200_000),
    ]
    rent = item("Квартплата", 18, -650, row=12, currency="USD")
    found = month_forecast(facts(), articles, [rent], TODAY, RATES, ["UZS", "USD"])
    sums, dollars = found.currencies
    # $300 плана в долларах покрывают часть, $350 × 12 000 = 4,2 млн — из сумов
    assert dollars.per_day_plan == 0
    assert sums.per_day_plan == pytest.approx(3_400_000 / 17)


def booked(
    repeat: str,
    start: date,
    amount: float,
    sub: str = "развитие",
    record_id: str = "k7f2qa",
    weekdays: frozenset[int] = frozenset(),
) -> CalendarRecord:
    return CalendarRecord(
        record_id, start, repeat, weekdays, "👶 ДЕТИ", sub, "UZS", amount, "", "Тест"
    )


def planned(
    rows: Sequence[list[Any]] = (),
    articles: Sequence[Article] = (),
    records: Sequence[CalendarRecord] = (),
) -> CurrencyForecast:
    return currency_forecast(facts(*rows), articles, (), TODAY, "UZS", records=records)


def test_records_are_reserved_in_their_article_and_fall_on_their_days() -> None:
    rows = [fact_row(date(2026, 10, 3), "👶 ДЕТИ", "развитие", 400_000)]
    records = [
        booked("once", date(2026, 10, 17), 320_000),
        booked("weekdays", date(2026, 10, 6), 80_000, record_id="p1c4xy",
               weekdays=frozenset({1, 3})),
    ]  # fmt: skip
    line = planned(rows, [article("развитие", 1_200_000)], records)
    # остаток 800 000: 640 000 в резерве записей, 160 000 — по дням
    per_day = 160_000 / 17
    assert line.per_day_plan == pytest.approx(per_day)
    evenings = {point.day: point for point in line.days}
    assert evenings[17].plan - evenings[16].plan == pytest.approx(-320_000 - per_day)
    assert evenings[19].plan - evenings[18].plan == pytest.approx(-80_000 - per_day)
    # «по темпу»: разовая запись — да, кружок — нет
    pace = line.per_day_pace
    assert evenings[17].pace - evenings[16].pace == pytest.approx(-320_000 - pace)
    assert evenings[19].pace - evenings[18].pace == pytest.approx(-pace)
    assert [(r.day, r.open, r.over) for r in line.records] == [
        (17, 320_000, False),
        (19, 80_000, False),
        (21, 80_000, False),
        (26, 80_000, False),
        (28, 80_000, False),
    ]
    assert line.articles == (
        ArticleState("🍔 ЕДА", "развитие", "UZS", 1_200_000, 800_000, 640_000),
    )


def test_records_beyond_the_rest_count_in_full_and_are_marked() -> None:
    coffee = booked("daily", date(2026, 10, 1), 30_000, "кофе", "m3a9zt")
    line = planned(articles=[article("кофе", 480_000)], records=[coffee])
    assert line.per_day_plan == 0  # весь остаток под записями
    assert line.days[-1].plan == pytest.approx(10_000_000 - 30_000 * 17)
    assert all(day.over for day in line.records)
    assert (line.articles[0].rest, line.articles[0].booked) == (480_000, 510_000)


def test_a_record_of_an_article_without_a_row_is_all_over_plan() -> None:
    theatre = booked("once", date(2026, 10, 20), 100_000, "театр", "t1t2t3")
    line = planned(records=[theatre])
    assert line.articles == (ArticleState("👶 ДЕТИ", "театр", "UZS", 0, 0, 100_000),)
    assert line.records[0].over


def test_todays_spending_on_a_record_is_not_counted_twice() -> None:
    coffee = booked("daily", date(2026, 10, 1), 30_000, "кофе", "m3a9zt")
    articles = [article("кофе", 510_000), article("кафе", 1_700_000)]
    morning = planned(articles=articles, records=[coffee])
    evening = planned([fact_row(TODAY, "🍔 ЕДА", "кофе", 25_000)], articles, [coffee])
    assert evening.per_day_plan == morning.per_day_plan == pytest.approx(100_000)
    assert evening.spent_today == 0  # кофе засчитан в запись
    # 25 000 уже в деньгах, по записи сегодня ещё 5 000, по плану дня — 100 000
    assert evening.days[0].plan == pytest.approx(10_000_000 - 25_000 - 5_000 - 100_000)
    assert evening.days[0].plan == pytest.approx(morning.days[0].plan)


def test_paying_a_one_off_today_does_not_lift_the_plan_per_day() -> None:
    kids = booked("once", TODAY, 320_000)
    articles = [article("развитие", 657_000)]
    morning = planned(articles=articles, records=[kids])
    paid = fact_row(TODAY, "👶 ДЕТИ", "развитие", 320_000)
    evening = planned([paid], articles, [kids])
    assert evening.per_day_plan == morning.per_day_plan == pytest.approx(337_000 / 17)
    assert (evening.records[0].open, evening.spent_today) == (0, 0)
    assert evening.days[0].plan == pytest.approx(morning.days[0].plan)


def test_a_paid_one_off_stays_out_of_the_pace() -> None:
    camp = booked("once", date(2026, 10, 8), 700_000)
    rows = [
        fact_row(date(2026, 10, 8), "👶 ДЕТИ", "развитие", 700_000),
        fact_row(date(2026, 10, 9), "🍔 ЕДА", "кафе", 1_400_000),
    ]
    assert planned(rows, records=[camp]).per_day_pace == pytest.approx(100_000)


def test_the_month_lists_records_with_totals_hints_and_rates() -> None:
    coffee = booked("daily", date(2026, 10, 1), 30_000, "кофе", "m3a9zt")
    kids = booked("once", date(2026, 10, 17), 320_000)
    gone = booked("once", date(2026, 10, 2), 50_000, record_id="o1d000")
    rows = [fact_row(date(2026, 10, 10), "🍔 ЕДА", "кофе", 28_000)]
    found = month_forecast(facts(*rows), [article("кофе", 900_000)], (), TODAY, RATES,
                           ["UZS", "USD"], [kids, coffee, gone])  # fmt: skip
    assert [(m.record.id, m.month_total, m.booked, m.over) for m in found.calendar] == [
        ("m3a9zt", 510_000, 510_000, False),
        ("k7f2qa", 320_000, 320_000, True),  # статьи «развитие» во вкладке нет
    ]
    assert found.hints["кофе|UZS"].last == 28_000
    assert found.rates == {"USD": 12_000.0, "RUB": 150.0}


def test_the_list_keeps_every_repeat_and_one_offs_ahead() -> None:
    internet = booked("monthly", date(2026, 10, 5), 150_000, "интернет", "i5n0et")
    camp = booked("once", date(2026, 11, 3), 700_000, record_id="c4mp00")
    kids = booked("once", TODAY, 320_000)
    euro = CalendarRecord("e1u2r0", date(2026, 9, 10), "monthly", frozenset(),
                          "👶 ДЕТИ", "развитие", "EUR", 50, "", "Тест")  # fmt: skip
    paid = fact_row(TODAY, "👶 ДЕТИ", "развитие", 320_000)
    found = month_forecast(facts(paid), (), (), TODAY, RATES, ["UZS"],
                           [camp, kids, internet, euro])  # fmt: skip
    # 5-е прошло, лагерь — в ноябре, центр оплачен сегодня, евро без карты:
    # всё видно в списке и правится
    assert [(m.record.id, m.month_total, m.booked, m.over) for m in found.calendar] == [
        ("e1u2r0", 0, 0, False),
        ("i5n0et", 0, 0, False),
        ("k7f2qa", 0, 320_000, True),
        ("c4mp00", 0, 0, False),
    ]


def test_past_days_are_the_evenings_by_fact_and_the_change_of_each_day() -> None:
    rows = [
        fact_row(date(2026, 9, 30), "🍔 ЕДА", "кафе", 100_000),  # прошлый месяц
        fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 400_000),
        fact_row(date(2026, 10, 5), "💰 ДОХОДЫ", "зарплата", 2_000_000),
        fact_row(date(2026, 10, 5), "🍔 ЕДА", "кофе", 30_000),
        fact_row(date(2026, 10, 9), "🍔 ЕДА", "кафе", -50_000),  # возврат
        fact_row(TODAY, "🍔 ЕДА", "кофе", 20_000),  # сегодня — то, что уже было
        fact_row(date(2026, 10, 4), "🍔 ЕДА", "кафе", 70, "USD"),  # другая валюта
    ]
    past = uzs(rows).past
    assert [day.day for day in past] == list(range(1, 16))
    evenings = {day.day: (day.evening, day.change) for day in past}
    assert evenings[1] == (9_900_000, 0)
    assert evenings[2] == (9_500_000, -400_000)
    assert evenings[5] == (11_470_000, 1_970_000)  # зарплата минус кофе
    assert evenings[9] == (11_520_000, 50_000)
    assert evenings[14] == (11_520_000, 0)
    assert evenings[15] == (11_500_000, -20_000)  # сегодня: деньги сейчас
    # расход и доход дня — врозь, не одним числом
    moves = {day.day: (day.spent, day.earned) for day in past}
    assert moves[2] == (400_000, 0)
    assert moves[5] == (30_000, 2_000_000)
    assert moves[9] == (-50_000, 0)  # возврат уменьшает траты дня
    assert moves[15] == (20_000, 0)


def test_each_day_has_its_spending_and_income_by_article() -> None:
    rows = [
        fact_row(date(2026, 10, 2), "🍔 ЕДА", "кофе", 30_000),
        fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 400_000),
        fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 100_000),
        fact_row(date(2026, 10, 2), "💰 ДОХОДЫ", "зарплата", 2_000_000),
        fact_row(date(2026, 10, 2), "💳 СЧЕТА", "перевод", 1_000_000),
        fact_row(date(2026, 10, 2), "💰 ДОХОДЫ", "нач остаток", 5_000),
        fact_row(date(2026, 10, 9), "🍔 ЕДА", "кафе", -50_000),  # возврат
        fact_row(TODAY, "🍔 ЕДА", "кофе", 20_000),
    ]
    past = uzs(rows).past
    # траты — суммами по статьям, от больших; доходы — отдельно; переводы
    # между своими картами и «нач остаток» — ни то, ни другое
    assert [(a.group, a.subcategory, a.amount, a.income) for a in past[1].articles] == [
        ("🍔 ЕДА", "кафе", 500_000, False),
        ("🍔 ЕДА", "кофе", 30_000, False),
        ("💰 ДОХОДЫ", "зарплата", 2_000_000, True),
    ]
    assert [(a.subcategory, a.amount) for a in past[8].articles] == [("кафе", -50_000)]
    assert [(a.subcategory, a.amount) for a in past[14].articles] == [("кофе", 20_000)]
    assert past[0].articles == ()


def test_articles_of_a_currency_without_a_rate_have_no_sum_in_sums() -> None:
    rows = [fact_row(date(2026, 10, 4), "🍔 ЕДА", "кафе", 30, "EUR")]
    frame = transactions_frame([FACT_HEADER, OPENING, *rows], RATES, keep_unknown=True)
    [cafe] = currency_forecast(frame, (), (), TODAY, "EUR").past[3].articles
    assert (cafe.amount, cafe.uzs) == (30, 0)


def test_articles_of_a_day_carry_their_sum_in_sums() -> None:
    rows = [fact_row(date(2026, 10, 4), "🍔 ЕДА", "кафе", 70, "USD")]
    dollars = currency_forecast(facts(*rows), (), (), TODAY, "USD")
    [cafe] = dollars.past[3].articles
    assert (cafe.amount, cafe.uzs) == (70, 70 * 12_000)


def test_a_transfer_between_own_cards_is_neither_spending_nor_income() -> None:
    rows = [
        fact_row(date(2026, 10, 3), "💳 СЧЕТА", "перевод", 1_000_000),
        fact_row(date(2026, 10, 3), "💳 СЧЕТА", "перевод", -1_000_000),
        fact_row(date(2026, 10, 3), "💰 ДОХОДЫ", "нач остаток", 500_000),
    ]
    third = uzs(rows).past[2]
    # «нач остаток» — выравнивание, не доход: деньги на вечер растут, дохода нет
    assert (third.change, third.spent, third.earned) == (500_000, 0, 0)


def test_on_the_first_day_the_past_is_only_today() -> None:
    assert [day.day for day in uzs(today=date(2026, 10, 1)).past] == [1]


def test_past_days_in_total_are_every_currency_at_todays_rate() -> None:
    rows = [
        fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_000, "USD"),
        fact_row(date(2026, 10, 3), "🍔 ЕДА", "кафе", 10, "USD"),
        fact_row(date(2026, 10, 3), "🍔 ЕДА", "кафе", 300_000),
    ]
    found = month_forecast(facts(*rows), (), (), TODAY, RATES, ["UZS", "USD"])
    assert [day.day for day in found.past_uzs] == list(range(1, 16))
    third = found.past_uzs[2]
    # статьи — у каждой валюты в её деньгах, у «Всего» их нет
    assert third.articles == ()
    assert third.evening == pytest.approx(9_700_000 + 990 * 12_000)
    assert third.change == pytest.approx(-300_000 - 10 * 12_000)
    assert found.past_usd[2].evening == pytest.approx(third.evening / 12_000)
    assert found.past_usd[2].change == pytest.approx(third.change / 12_000)
    assert third.spent == pytest.approx(300_000 + 10 * 12_000)
    assert found.past_usd[2].spent == pytest.approx(third.spent / 12_000)
    assert third.earned == 0
