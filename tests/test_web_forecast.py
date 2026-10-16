"""Прогноз до конца месяца: AnalyticsService.forecast и GET /api/forecast."""

from datetime import date
from typing import Any

import pytest

from app import config
from app.services.analytics_service import AnalyticsService, transactions_frame
from app.services.day_budget import ForecastItem
from app.services.forecast import month_forecast
from tests.test_day_budget import (
    FACT_HEADER,
    OPENING,
    RATES,
    Sheets,
    fact_row,
    frame,
    tab,
)
from tests.test_web_api import OWNER, auth, client_for, make_application

DAY = date(2026, 10, 15)


@pytest.fixture(autouse=True)
def allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", frozenset({OWNER}))


class FakeAnalytics:
    def __init__(self, result: Any = None, error: Exception | None = None) -> None:
        self.result, self.error = result, error
        self.currencies: list[str] = []

    def forecast(self, today: date, currencies: Any) -> Any:
        self.currencies = list(currencies)
        if self.error is not None:
            raise self.error
        return self.result


def forecast_for(
    found: Any = None,
    error: Exception | None = None,
    sources: list[str] | None = None,
) -> tuple[Any, FakeAnalytics]:
    application = make_application()
    if sources is not None:
        application.bot_data["sources"] = sources
    analytics = FakeAnalytics(found, error)
    application.bot_data["analytics_service"] = analytics
    response = client_for(application).get("/api/forecast", headers=auth())
    return response, analytics


def test_service_reads_the_month_tab_and_the_fact() -> None:
    fact = [FACT_HEADER, *OPENING]
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    found = service.forecast(date(2026, 10, 2), ["USD", "UZS"])
    assert found is not None
    assert [(line.code, line.now) for line in found.currencies] == [
        ("UZS", 5_000_000),
        ("USD", 1000),
    ]
    assert [event.name for event in found.currencies[0].events] == ["Аванс", "ЗП"]
    # квартплата 7,8 млн запланирована в сумах, а платится из списка долларами:
    # по дням в сумах — только кафе 2,6 и инвентарь 0,5 млн, как во вкладке
    assert found.currencies[0].per_day_plan == pytest.approx(3_100_000 / 30)
    missing = AnalyticsService(Sheets({"fact": fact}))
    assert missing.forecast(date(2026, 10, 2), ["UZS"]) is None


def test_service_keeps_a_currency_without_a_rate() -> None:
    euro = fact_row(date(2026, 10, 1), "💰 ДОХОДЫ", "зарплата", 300, "EUR")
    fact = [FACT_HEADER, *OPENING, euro]
    service = AnalyticsService(Sheets({"fact": fact, "Oct 26": tab()}))
    found = service.forecast(date(2026, 10, 2), ["UZS", "EUR"])
    assert found is not None
    assert (found.currencies[1].code, found.currencies[1].now) == ("EUR", 300)
    assert "EUR" not in found.counted


def test_forecast_has_every_currency_of_the_cards_and_the_total() -> None:
    found = month_forecast(frame(), (), (), DAY, RATES, ["UZS", "USD"])
    response, analytics = forecast_for(found)
    body = response.json()
    assert analytics.currencies == ["UZS", "UZS", "USD"]  # карты бота
    assert (body["status"], body["month"]) == ("ok", "Oct 26")
    assert (body["today"], body["end"]) == ("2026-10-15", "2026-10-31")
    uzs, usd = body["currencies"]
    assert (uzs["code"], uzs["now"], uzs["in_total"]) == ("UZS", 5_000_000, True)
    assert usd["now"] == 1000
    assert uzs["days"][0] == {"day": 15, "plan": 5_000_000, "pace": 5_000_000}
    assert len(body["total"]["uzs"]) == 17
    assert body["total"]["usd"][0]["plan"] == 1416.67  # 17 млн / 12 000
    assert body["total"]["below_zero"] == {"plan": None, "pace": None}


def test_sums_and_rubles_are_whole_and_dollars_keep_cents() -> None:
    rows = [
        fact_row(date(2026, 10, 14), "🍔 ЕДА", "кафе", 1_000.4),
        fact_row(date(2026, 10, 14), "🍔 ЕДА", "кофе", 10.5, "USD"),
    ]
    found = month_forecast(frame(*rows), (), (), DAY, RATES, ["UZS", "USD"])
    uzs, usd = forecast_for(found)[0].json()["currencies"]
    assert (uzs["now"], uzs["per_day_pace"]) == (4_999_000, 71)
    assert (usd["now"], usd["per_day_pace"]) == (989.5, 0.75)


def test_events_and_dips_are_in_the_answer() -> None:
    late = ForecastItem("Аванс", 13, 2_000_000, "UZS", 2_000_000, 12)
    spent = fact_row(date(2026, 10, 14), "🍔 ЕДА", "кафе", 14_000_000)  # 1 млн в день
    found = month_forecast(frame(spent), (), [late], DAY, RATES, ["UZS"])
    uzs = forecast_for(found)[0].json()["currencies"][0]
    assert uzs["events"] == [
        {"day": 15, "name": "Аванс", "amount": 2_000_000, "late": True}
    ]
    assert uzs["below_zero"]["pace"] == {"start": 15, "until": None}


def test_past_days_are_in_the_answer() -> None:
    spent = fact_row(date(2026, 10, 3), "🍔 ЕДА", "кафе", 400_000)
    found = month_forecast(frame(spent), (), (), DAY, RATES, ["UZS", "USD"])
    body = forecast_for(found)[0].json()
    uzs = body["currencies"][0]
    assert len(uzs["past"]) == 14  # с 1-го по вчера
    assert uzs["past"][2] == {"day": 3, "evening": 4_600_000, "change": -400_000}
    assert body["total"]["past_uzs"][2]["evening"] == 4_600_000 + 12_000_000
    # 17 млн / 12 000 — доллары до центов
    assert body["total"]["past_usd"][0] == {"day": 1, "evening": 1416.67, "change": 0}


def test_a_currency_without_a_rate_is_outside_the_total() -> None:
    euro = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 300, "EUR")
    data = transactions_frame([FACT_HEADER, *OPENING, euro], RATES, keep_unknown=True)
    found = month_forecast(data, (), (), DAY, RATES, ["UZS", "EUR"])
    body = forecast_for(found)[0].json()
    assert body["currencies"][1] | {"days": [], "events": [], "past": []} == {
        "code": "EUR",
        "now": 300,
        "per_day_plan": 0,
        "per_day_pace": 0,
        "spent_today": 0,
        "in_total": False,
        "events": [],
        "days": [],
        "below_zero": {"plan": None, "pace": None},
        "records": [],
        "past": [],
    }


def test_without_the_cards_list_the_forecast_is_in_sums() -> None:
    found = month_forecast(frame(), (), (), DAY, RATES, ["UZS"])
    _, analytics = forecast_for(found, sources=[])
    assert analytics.currencies == ["UZS"]


def test_forecast_without_the_month_tab() -> None:
    body = forecast_for(None)[0].json()
    assert (body["status"], body["currencies"], body["total"]) == (
        "no_month_tab",
        [],
        None,
    )


def test_forecast_sheet_failure_is_503() -> None:
    response, _ = forecast_for(error=ConnectionError("Google is down"))
    assert (response.status_code, response.json()["error"]["code"]) == (
        503,
        "sheets_unavailable",
    )


def test_forecast_needs_a_signature() -> None:
    response = client_for(make_application()).get("/api/forecast")
    assert response.status_code == 401
