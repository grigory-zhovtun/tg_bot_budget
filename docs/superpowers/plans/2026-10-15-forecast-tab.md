# Вкладка «Прогноз» — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Четвёртая вкладка Mini App «Прогноз» показывает, сколько денег будет на вечер любого дня до конца месяца — по каждой валюте карт и всего (в сумах или долларах), двумя линиями «по плану» и «по темпу»; карточка «💳 Сумы — хватит до…» считается той же моделью.

**Architecture:** Новый чистый модуль `app/services/forecast.py` считает прогноз по листу `fact`, статьям и жёлтому списку вкладки месяца (сверка списка — `match_receipts`). `AnalyticsService.forecast` читает таблицу, `GET /api/forecast` отдаёт JSON. Карточка сумов (`runway.py`) строится из линии «по темпу» модели. В приложении — модель вида `webapp/src/forecast/view.ts`, геометрия графика `chart.ts` и экран из ленты недели, карточки дня, графика и списка валют.

**Tech Stack:** Python 3.12, Starlette, Pydantic 2, gspread, pandas; React 19.3, TypeScript 6.0 (strict, `noUncheckedIndexedAccess`), Vite 8.3, Tailwind 4.3, Vitest 5.0, Testing Library.

**Spec:** `docs/superpowers/specs/2026-10-15-forecast-tab-design.md`

## Global Constraints

- Репозиторий публичный: в коде, тестах, фикстурах и документах — только условные суммы и названия; ID таблицы, реальные суммы, работодатели и имена — никогда.
- Секреты не читаем и не вводим: `.env` не открываем.
- Python: type hints на всех сигнатурах, black + ruff (`E4 E7 E9 F I B UP`, строка 88). Команды Python — из корня репозитория в venv проекта (в этой копии — `../venv_dev/bin/`): перед коммитом `black . && ruff check . && pytest -q`.
- Фронтенд: функциональные компоненты и хуки, TypeScript strict, без `any` и `!`; в `webapp/` перед коммитом `npx prettier --write src && npm run lint && npm test && npm run build`. В этой копии хук `rtk` перехватывает `vitest` и кладёт отчёт в `webapp/.vitest/` — тесты гонять через `rtk proxy npx vitest run …`, иначе `prettier --check` споткнётся об отчёт (папку удалить).
- Деньги сейчас (спека 3.2): по всем строкам `fact` валюты с датой по сегодня включительно; «💰 ДОХОДЫ» плюсом, остальное с обратным знаком колонки D. «Всего» по сегодняшним курсам = P2 вкладки.
- План в день (спека 3.3): статьи с типом «Расходы» валюты, строки с одной подкатегорией и валютой складываются; остаток = план − факт с 1-го по вчера (строки `fact` той же подкатегории и валюты, без учёта группы), перерасход даёт 0; минус неоплаченные к утру платежи списка валюты; `max(…; 0) / left`, `left = end − today + 1`.
- Темп (спека 3.4): `expenses()` валюты за `today − 14 … today − 1` без операций, закрывших платежи списка текущего месяца по сверке к утру; `/ 14`; меньше нуля — 0.
- Потрачено сегодня (спека 3.6): `expenses()` валюты за сегодня без операций, закрывших строки списка по сверке на сейчас; не меньше нуля.
- События (спека 3.5): строки списка по сверке на сейчас, сумма — `remaining()`; день 31 в коротком месяце — последний день; опоздавшая строка — на сегодня с `late`; только валюты карт.
- Остаток на вечер (спека 3.6): `сегодня = сейчас + события(сегодня) − max(в день − потрачено сегодня; 0)`, дальше `вчера + события(d) − в день`.
- Ниже нуля — меньше −0,01 (`CENT`); уход в минус — первый день ниже нуля и первый день после него не ниже (`until` — `None`, если не вернулся).
- Порядок валют: UZS, USD, RUB, остальные по алфавиту; валюта без курса — своя линия, но не во «Всего».
- API (спека 4.2–4.3): `GET /api/forecast`, поля ответа — ровно как в спеке; нет вкладки месяца — 200 со `status: "no_month_tab"`; сбой таблицы — `503 sheets_unavailable` с текстом «Сводки»; сумы и рубли — целые, остальные валюты и «всего в долларах» — до центов.
- Тексты экрана — дословно: вкладка «Прогноз»; «<Месяц>, неделя»; «Под датой — сколько будет вечером, <млн сум | $ | ₽> (по плану)»; «⚠️ По темпу сумы уйдут в минус 25.10, вернутся 30.10»; «Нет вкладки месяца — прогноз появится, когда она будет»; «План — остаток плана статей, разложенный по дням. Темп — средние траты за 14 дней без переводов и платежей из списка.»
- Коммиты — conventional commits, в конце `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; тело PR заканчивается `🤖 Generated with [Claude Code](https://claude.com/claude-code)`; мерж — `gh pr merge --merge --delete-branch`.

## Review Focus

1. **Платёж списка оплачен сегодня** — к утру он ещё в плане и вычтен из него, на сейчас уже закрыт: не событие, не «потрачено сегодня», план в день не меняется (тест в Task 1).
2. **Первое число месяца** — план в день — весь план месяца, факт прошлого месяца в нём не участвует, темп берётся из последних 14 дней прошлого месяца (тест в Task 1).
3. **В телефоне запомнен вид, которого больше нет** — валюта без карты, «$» без курса доллара или испорченная запись: экран открывается на «Всего, сум» (тест в Task 5).
4. **Последний день месяца** — одна плашка, обе стрелки выключены, на графике одна точка посередине (тесты в Task 6 и Task 7).
5. **У бота нет списка карт** (`sources` пуст) — прогноз всё равно отвечает, хотя бы по сумам (тест в Task 4).

---

### Task 1: Прогноз одной валюты

**Files:**
- Create: `app/services/forecast.py`
- Test: `tests/test_forecast.py`

**Interfaces:**
- Consumes: `expenses(frame)` из `app/services/analytics_service.py` (без «💰 ДОХОДЫ» и «💳 СЧЕТА»); `match_receipts(frame, lines, first, through) -> dict[int, Receipt]`, `remaining(line, receipt) -> float`, `NOTHING`, `Receipt` из `app/services/receipts.py`; `INCOME_GROUP` из `app/domain.py`. Фрейм `transactions_frame`: колонки `day` (date), `group`, `subcategory`, `amount` (D со знаком таблицы), `currency`, индекс — номера строк, которые `match_receipts` кладёт в `Receipt.operations`.
- Produces: `PACE_DAYS = 14`, `CENT = 0.01`; протоколы `PlanArticle` (`kind`, `subcategory`, `currency`, `plan`) и `ListItem` (`row`, `name`, `day`, `amount`, `currency`); `Event(day: int, name: str, amount: float, late: bool)`; `Evening(day: int, plan: float, pace: float)`; `Dip(start: int, until: int | None)`; `CurrencyForecast(code, now, per_day_plan, per_day_pace, spent_today, events: tuple[Event, ...], days: tuple[Evening, ...], plan_dip: Dip | None, pace_dip: Dip | None)`; `below_zero(value: float) -> bool`; `month_end(today: date) -> date`; `dip(days: Sequence[int], values: Sequence[float]) -> Dip | None`; `currency_forecast(frame, articles: Sequence[PlanArticle], items: Sequence[ListItem], today: date, code: str) -> CurrencyForecast`.

- [ ] **Step 1: Ветка**

```bash
git checkout main && git pull && git checkout -b feat/forecast-tab
```

- [ ] **Step 2: Падающие тесты**

`tests/test_forecast.py`:

```python
"""Прогноз до конца месяца: деньги сейчас, план и темп в день, события, минус."""

from collections.abc import Sequence
from datetime import date
from typing import Any

import pytest

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.day_budget import ForecastItem
from app.services.forecast import (
    CurrencyForecast,
    Dip,
    Evening,
    Event,
    currency_forecast,
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
    return ForecastItem(name, day, amount, currency, amount * RATES.to_uzs[currency], row)


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
        *(fact_row(date(2026, 10, day), "🍔 ЕДА", "кафе", 850_000) for day in range(1, 15)),
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
```

- [ ] **Step 3: Тесты падают**

Run: `pytest tests/test_forecast.py -q`
Expected: ошибка импорта — `No module named 'app.services.forecast'`.

- [ ] **Step 4: Модуль прогноза**

`app/services/forecast.py`:

```python
"""Прогноз денег до конца месяца — по каждой валюте карт и всего.

Деньги «сейчас» — по листу fact вместе с сегодняшними операциями, как «Сейчас на
картах (факт)» вкладки. Дальше по дням до последнего дня месяца две линии: «по
плану» — остаток плана статей валюты, разложенный по дням, и «по темпу» — средние
траты за PACE_DAYS дней до сегодня. Строки жёлтого списка, которые ещё не пришли и
не ушли, — события в свой день; опоздавшие ждём сегодня.

План и темп в день считаются на утро (сверка списка по вчера) и за день не
меняются. Сегодняшние траты уже в деньгах «сейчас», поэтому за сегодня вычитается
только то, что от дня ещё осталось.
"""

import calendar
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

import pandas as pd

from app.domain import INCOME_GROUP
from app.services.analytics_service import expenses
from app.services.receipts import NOTHING, Receipt, match_receipts, remaining

PACE_DAYS = 14
CENT = 0.01  # ниже нуля — меньше −0,01: ноль из округлений минусом не считается


class PlanArticle(Protocol):
    """Статья вкладки месяца: план положительный, в валюте статьи."""

    kind: str  # spend | income
    subcategory: str
    currency: str
    plan: float


class ListItem(Protocol):
    """Строка жёлтого списка: строка вкладки, название, день, сумма со знаком."""

    row: int
    name: str
    day: int
    amount: float
    currency: str


@dataclass(frozen=True)
class Event:
    """Строка списка, которая ещё впереди: сумма — остаток, в валюте строки."""

    day: int
    name: str
    amount: float
    late: bool  # день строки прошёл, а денег нет — ждём сегодня


@dataclass(frozen=True)
class Evening:
    """Сколько будет вечером дня day: по плану и по темпу."""

    day: int
    plan: float
    pace: float


@dataclass(frozen=True)
class Dip:
    """Уход в минус: первый день ниже нуля и первый день после него снова не ниже."""

    start: int
    until: int | None  # None — до конца месяца не вернётся


@dataclass(frozen=True)
class CurrencyForecast:
    code: str
    now: float  # деньги сейчас, с сегодняшними операциями
    per_day_plan: float
    per_day_pace: float
    spent_today: float  # без платежей списка; не меньше нуля
    events: tuple[Event, ...]
    days: tuple[Evening, ...]  # с сегодня по последний день месяца
    plan_dip: Dip | None
    pace_dip: Dip | None


def below_zero(value: float) -> bool:
    return value < -CENT


def month_end(today: date) -> date:
    return today.replace(day=calendar.monthrange(today.year, today.month)[1])


def dip(days: Sequence[int], values: Sequence[float]) -> Dip | None:
    """Первый день ниже нуля и первый день после него не ниже; None — минуса нет."""
    start = next((i for i, value in enumerate(values) if below_zero(value)), None)
    if start is None:
        return None
    back = next(
        (i for i in range(start + 1, len(values)) if not below_zero(values[i])), None
    )
    return Dip(days[start], None if back is None else days[back])


def _paid(receipts: Mapping[int, Receipt]) -> frozenset[int]:
    """Строки fact, которыми закрыты строки списка."""
    return frozenset(i for receipt in receipts.values() for i in receipt.operations)


def _money_now(frame: pd.DataFrame, code: str, today: date) -> float:
    """Доходы плюсом, остальное с обратным знаком колонки D — по сегодня."""
    own = frame[(frame["currency"] == code) & (frame["day"] <= today)]
    flow = own["amount"].where(own["group"] == INCOME_GROUP, -own["amount"])
    return float(flow.sum())


def _spent(
    frame: pd.DataFrame, code: str, first: date, last: date, paid: frozenset[int]
) -> float:
    """Траты валюты с first по last: без доходов, переводов и платежей списка."""
    spent = expenses(frame)
    mask = (
        (spent["currency"] == code)
        & (spent["day"] >= first)
        & (spent["day"] <= last)
        & ~spent.index.isin(list(paid))
    )
    return float(spent.loc[mask, "amount"].sum())


def _plan_per_day(
    frame: pd.DataFrame,
    articles: Sequence[PlanArticle],
    items: Sequence[ListItem],
    morning: Mapping[int, Receipt],
    code: str,
    today: date,
) -> float:
    """Остаток плана статей без неоплаченных к утру платежей списка — на день."""
    plans: dict[str, float] = {}
    for article in articles:
        if article.kind == "spend" and article.currency == code:
            plans[article.subcategory] = (
                plans.get(article.subcategory, 0.0) + article.plan
            )
    month = frame[
        (frame["currency"] == code)
        & (frame["day"] >= today.replace(day=1))
        & (frame["day"] < today)
    ]
    facts = month.groupby("subcategory")["amount"].sum()
    rest = sum(
        max(plan - float(facts.get(sub, 0.0)), 0.0) for sub, plan in plans.items()
    )
    unpaid = -sum(
        remaining(item, morning.get(item.row, NOTHING))
        for item in items
        if item.currency == code and item.amount < 0
    )
    left = (month_end(today) - today).days + 1
    return max(rest - unpaid, 0.0) / left


def _events(
    items: Sequence[ListItem], now: Mapping[int, Receipt], code: str, today: date
) -> tuple[Event, ...]:
    """Незакрытые строки списка валюты: остаток в их день, опоздавшие — сегодня."""
    last = month_end(today).day
    found: list[Event] = []
    for item in items:
        if item.currency != code:
            continue
        rest = remaining(item, now.get(item.row, NOTHING))
        if not rest:
            continue
        day = min(item.day, last)  # 31-е в коротком месяце — его последний день
        late = day < today.day
        found.append(Event(today.day if late else day, item.name, rest, late))
    return tuple(sorted(found, key=lambda event: event.day))


def _line(
    now: float,
    events: Sequence[Event],
    per_day: float,
    spent_today: float,
    today: date,
) -> list[float]:
    """Остаток на вечер каждого дня: за сегодня — только то, что от дня осталось."""
    moves: dict[int, float] = {}
    for event in events:
        moves[event.day] = moves.get(event.day, 0.0) + event.amount
    left = now + moves.get(today.day, 0.0) - max(per_day - spent_today, 0.0)
    values = [left]
    for number in range(today.day + 1, month_end(today).day + 1):
        left += moves.get(number, 0.0) - per_day
        values.append(left)
    return values


def currency_forecast(
    frame: pd.DataFrame,
    articles: Sequence[PlanArticle],
    items: Sequence[ListItem],
    today: date,
    code: str,
) -> CurrencyForecast:
    """Прогноз одной валюты с сегодня по последний день месяца."""
    first = today.replace(day=1)
    yesterday = today - timedelta(days=1)
    morning = match_receipts(frame, items, first, yesterday)
    now = match_receipts(frame, items, first, today)
    start = today - timedelta(days=PACE_DAYS)
    window = _spent(frame, code, start, yesterday, _paid(morning))
    pace = max(window / PACE_DAYS, 0.0)
    spent = max(_spent(frame, code, today, today, _paid(now)), 0.0)
    per_day = _plan_per_day(frame, articles, items, morning, code, today)
    events = _events(items, now, code, today)
    money = _money_now(frame, code, today)
    plan = _line(money, events, per_day, spent, today)
    paced = _line(money, events, pace, spent, today)
    numbers = list(range(today.day, month_end(today).day + 1))
    return CurrencyForecast(
        code,
        money,
        per_day,
        pace,
        spent,
        events,
        tuple(
            Evening(number, by_plan, by_pace)
            for number, by_plan, by_pace in zip(numbers, plan, paced, strict=True)
        ),
        dip(numbers, plan),
        dip(numbers, paced),
    )
```

- [ ] **Step 5: Тесты проходят**

Run: `pytest tests/test_forecast.py -q`
Expected: PASS (15 тестов).

- [ ] **Step 6: Полная проверка и коммит**

Run: `black . && ruff check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/services/forecast.py tests/test_forecast.py
git commit -m "feat(forecast): money until the month end for one currency

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Все валюты и «Всего»

**Files:**
- Modify: `app/services/forecast.py`
- Modify: `app/services/analytics_service.py` (`transactions_frame`)
- Test: `tests/test_forecast.py`, `tests/test_analytics_service.py`

**Interfaces:**
- Consumes: `currency_forecast`, `month_end`, `dip`, `Evening`, `Dip`, `CurrencyForecast`, `PlanArticle`, `ListItem` из Task 1; `Rates` (`to_uzs: dict[str, float]`) из `app/domain.py`.
- Produces: `ORDER = ("UZS", "USD", "RUB")`; `MonthForecast(today, end, currencies: tuple[CurrencyForecast, ...], counted: frozenset[str], total_uzs: tuple[Evening, ...], total_usd: tuple[Evening, ...], plan_dip: Dip | None, pace_dip: Dip | None)`; `currency_order(codes: Iterable[str]) -> list[str]`; `month_forecast(frame, articles, items, today: date, rates: Rates, currencies: Iterable[str]) -> MonthForecast`; `transactions_frame(rows, rates, keep_unknown: bool = False)` — с `keep_unknown=True` строки валюты без курса остаются с `amount_uzs = 0`.

- [ ] **Step 1: Падающие тесты**

В `tests/test_forecast.py` к импортам из `app.services.forecast` добавить `month_forecast`, затем в конец файла:

```python
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
```

В `tests/test_analytics_service.py` после `test_frame_converts_currency_and_skips_unknown`:

```python
def test_frame_keeps_currencies_without_a_rate_for_the_forecast() -> None:
    frame = transactions_frame(FACT, RATES, keep_unknown=True)
    assert len(frame) == len(FACT) - 2  # шапка и пустая строка
    sgd = frame[frame["currency"] == "SGD"].iloc[0]
    assert (sgd["amount"], sgd["amount_uzs"]) == (3.0, 0.0)
```

- [ ] **Step 2: Тесты падают**

Run: `pytest tests/test_forecast.py tests/test_analytics_service.py -q`
Expected: ошибка импорта `month_forecast`; после неё — `TypeError: transactions_frame() got an unexpected keyword argument 'keep_unknown'`.

- [ ] **Step 3: `keep_unknown` во фрейме**

В `app/services/analytics_service.py` заменить сигнатуру, docstring и конец `transactions_frame`:

```python
def transactions_frame(
    rows: list[list[Any]], rates: Rates, keep_unknown: bool = False
) -> pd.DataFrame:
    """Строки fact (UNFORMATTED) → таблица с суммой в сумах (amount_uzs).

    amount — колонка D со знаком таблицы: расход и доход плюс, прочий приход минус.
    Строки валюты без курса пропускаются; keep_unknown оставляет их с amount_uzs 0 —
    прогноз считает каждую валюту в её собственных деньгах.
    """
```

и вместо последних строк функции (от `unknown = …` до `return frame[rate > 0]`):

```python
    unknown = frame.loc[rate == 0, "currency"].unique()
    if len(unknown) and not keep_unknown:
        logger.warning("No rate for currencies %s, their rows are skipped", unknown)
    frame["amount_uzs"] = frame["amount"] * rate
    return frame if keep_unknown else frame[rate > 0]
```

- [ ] **Step 4: Прогноз месяца**

В `app/services/forecast.py`: импорты — `from collections.abc import Iterable, Mapping, Sequence` и `from app.domain import INCOME_GROUP, Rates`; после `CENT` — константа, после `CurrencyForecast` — класс, в конец файла — две функции:

```python
ORDER = ("UZS", "USD", "RUB")  # дальше — остальные валюты по алфавиту
```

```python
@dataclass(frozen=True)
class MonthForecast:
    today: date
    end: date
    currencies: tuple[CurrencyForecast, ...]  # UZS, USD, RUB, остальные по алфавиту
    counted: frozenset[str]  # валюты с курсом таблицы — они во «Всего»
    total_uzs: tuple[Evening, ...]
    total_usd: tuple[Evening, ...]  # пусто — нет курса доллара
    plan_dip: Dip | None  # у «Всего»
    pace_dip: Dip | None
```

```python
def currency_order(codes: Iterable[str]) -> list[str]:
    """UZS, USD, RUB, остальные по алфавиту; без повторов и пустых."""
    unique = {code for code in codes if code}
    return [code for code in ORDER if code in unique] + sorted(unique - set(ORDER))


def month_forecast(
    frame: pd.DataFrame,
    articles: Sequence[PlanArticle],
    items: Sequence[ListItem],
    today: date,
    rates: Rates,
    currencies: Iterable[str],
) -> MonthForecast:
    """Прогноз по валютам карт и всего — по сегодняшним курсам таблицы."""
    lines = tuple(
        currency_forecast(frame, articles, items, today, code)
        for code in currency_order(currencies)
    )
    rated = [(line, rate) for line in lines if (rate := rates.to_uzs.get(line.code))]
    numbers = list(range(today.day, month_end(today).day + 1))
    plans = [0.0] * len(numbers)
    paces = [0.0] * len(numbers)
    for line, rate in rated:
        for index, point in enumerate(line.days):
            plans[index] += point.plan * rate
            paces[index] += point.pace * rate
    uzs = tuple(
        Evening(number, plan, pace)
        for number, plan, pace in zip(numbers, plans, paces, strict=True)
    )
    dollar = rates.to_uzs.get("USD")
    usd = (
        tuple(Evening(p.day, p.plan / dollar, p.pace / dollar) for p in uzs)
        if dollar
        else ()
    )
    return MonthForecast(
        today,
        month_end(today),
        lines,
        frozenset(line.code for line, _ in rated),
        uzs,
        usd,
        dip(numbers, [point.plan for point in uzs]),
        dip(numbers, [point.pace for point in uzs]),
    )
```

- [ ] **Step 5: Тесты проходят**

Run: `pytest tests/test_forecast.py tests/test_analytics_service.py -q`
Expected: PASS (21 тест в `test_forecast.py`, все в `test_analytics_service.py`).

- [ ] **Step 6: Полная проверка и коммит**

Run: `black . && ruff check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/services/forecast.py app/services/analytics_service.py tests/test_forecast.py tests/test_analytics_service.py
git commit -m "feat(forecast): every currency of the cards and the total

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Карточка «💳 Сумы» на модели прогноза

**Files:**
- Modify: `app/services/runway.py` (целиком)
- Modify: `app/services/day_budget.py` (импорт, docstring `day_budget`, вызов `uzs_runway`)
- Test: `tests/test_runway.py` (целиком)

**Interfaces:**
- Consumes: `currency_forecast(frame, articles, items, today, code)`, `CurrencyForecast`, `below_zero` из Task 1.
- Produces: `uzs_runway(line: CurrencyForecast, today: date, rates: Rates) -> Runway`; `Runway(balance, pace, last_day, short, until, dollars)` и `DOLLAR_STEP = 10` — без изменений. `uzs_balance`, `uzs_pace`, `PACE_DAYS` и протокол `ListItem` из `runway.py` удаляются (других пользователей нет).

- [ ] **Step 1: Тесты под новую карточку**

`tests/test_runway.py` заменить целиком:

```python
"""Карточка «💳 Сумы»: линия «по темпу» прогноза в сумах."""

from datetime import date, timedelta
from typing import Any

import pytest

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.day_budget import (
    ForecastItem,
    day_budget,
    format_day_budget,
    runway_line,
)
from app.services.forecast import currency_forecast
from app.services.runway import Runway, uzs_runway
from tests.test_day_budget import FACT_HEADER, RATES, fact_row, frame, tab

TODAY = date(2026, 10, 13)
OPENING = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 9_000_000)
ADVANCE = ForecastItem("Аванс", 15, 1_000_000, "UZS", 1_000_000, 12)
SALARY = ForecastItem("ЗП", 30, 20_000_000, "UZS", 20_000_000, 13)
RENT = ForecastItem("Квартплата", 18, -500, "USD", -6_000_000, 14)


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
    items: list[ForecastItem] | None = None,
    today: date = TODAY,
    rates: Rates = RATES,
) -> Runway:
    line = currency_forecast(facts(*rows), (), items or [], today, "UZS")
    return uzs_runway(line, today, rates)


def test_sums_run_out_before_the_salary_and_dollars_fill_the_gap() -> None:
    runway = card([OPENING, *spending(14, 500_000)], [ADVANCE, SALARY, RENT])
    assert (runway.balance, runway.pace) == (2_000_000, 500_000)
    # 13-го 1,5 млн, 15-го +аванс, 18-го ровно 0, 19-го уже минус
    assert runway.last_day == date(2026, 10, 18)
    # ниже всего — 29-го: −5,5 млн; 30-го зарплата закрывает минус
    assert (runway.short, runway.until) == (5_500_000, date(2026, 10, 30))
    assert runway.dollars == 460  # 5,5 млн / 12 000 = 458,3 → вверх до $10


def test_enough_sums_until_the_end_of_the_month() -> None:
    runway = card([OPENING, *spending(14, 100_000)])
    assert (runway.last_day, runway.short, runway.until, runway.dollars) == (
        None,
        0,
        None,
        None,
    )


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
    camp = ForecastItem("Лагерь", 10, -3_500_000, "UZS", -3_500_000, 15)
    paid = fact_row(date(2026, 10, 10), "👶 ДЕТИ", "развитие", 3_500_000)
    runway = card([OPENING, *spending(14, 100_000), paid], [camp])
    assert runway.pace == pytest.approx(100_000)
    assert runway.balance == 9_000_000 - 1_400_000 - 3_500_000


def test_todays_spending_takes_only_what_is_left_of_the_days_pace() -> None:
    rows = [OPENING, *spending(14, 500_000)]
    coffee = fact_row(TODAY, "🍔 ЕДА", "кофе", 30_000)
    morning = card(rows, [ADVANCE, SALARY])
    evening = card([*rows, coffee], [ADVANCE, SALARY])
    # 30 000 уже ушли — до вечера по темпу уйдёт только 470 000: картина та же
    assert evening.balance == morning.balance - 30_000
    assert (evening.last_day, evening.short, evening.until) == (
        morning.last_day,
        morning.short,
        morning.until,
    )


def test_the_card_names_the_day_before_the_forecast_goes_below_zero() -> None:
    history = facts(OPENING, *spending(14, 500_000))
    line = currency_forecast(history, (), [ADVANCE, SALARY], TODAY, "UZS")
    runway = uzs_runway(line, TODAY, RATES)
    assert line.pace_dip is not None
    assert runway.last_day == TODAY.replace(day=line.pace_dip.start) - timedelta(
        days=1
    )


def test_without_a_dollar_rate_the_gap_stays_in_sums() -> None:
    runway = card(
        [OPENING, *spending(14, 500_000)], [ADVANCE, SALARY], rates=Rates({"UZS": 1.0})
    )
    assert (runway.short, runway.dollars) == (5_500_000, None)


def test_without_a_salary_the_gap_lasts_to_the_end_of_the_month() -> None:
    runway = card([OPENING, *spending(14, 500_000)])
    assert runway.last_day == date(2026, 10, 16)
    assert runway.until is None
    assert runway.short == 500_000 * 19 - 2_000_000  # 13–31 октября, 19 дней


def test_sums_already_gone() -> None:
    opening = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_000_000)
    runway = card([opening, *spending(14, 100_000)], [SALARY])
    assert runway.balance == -400_000
    assert runway.last_day == TODAY - timedelta(days=1)


def test_a_list_day_past_the_month_end_counts_on_its_last_day() -> None:
    today = date(2026, 11, 20)
    opening = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 1_900_000)
    salary = ForecastItem("ЗП", 31, 5_000_000, "UZS", 5_000_000, 13)  # в ноябре 30
    runway = card([opening, *spending(14, 100_000, today)], [salary], today)
    assert runway.last_day == date(2026, 11, 24)
    assert (runway.short, runway.until) == (500_000, date(2026, 11, 30))


@pytest.mark.parametrize(
    ("runway", "line"),
    [
        (
            Runway(
                2_000_000,
                500_000,
                date(2026, 10, 18),
                5_500_000,
                date(2026, 10, 30),
                460,
            ),
            "💳 Сумы 2,00 млн — хватит до 18.10 · до 30.10 обменять ≈ $460",
        ),
        (
            Runway(7_600_000, 100_000, None, 0, None, None),
            "💳 Сумы 7,60 млн — до конца месяца хватит",
        ),
        (
            Runway(2_000_000, 500_000, date(2026, 10, 16), 7_500_000, None, 630),
            "💳 Сумы 2,00 млн — хватит до 16.10 · до конца месяца обменять ≈ $630",
        ),
        (
            Runway(
                2_000_000,
                500_000,
                date(2026, 10, 18),
                5_500_000,
                date(2026, 10, 30),
                None,
            ),
            "💳 Сумы 2,00 млн — хватит до 18.10 · до 30.10 не хватит 5,50 млн",
        ),
        (
            Runway(
                -400_000,
                100_000,
                date(2026, 10, 12),
                1_900_000,
                date(2026, 10, 30),
                1_160,
            ),
            "💳 Сумы -400 000 — уже закончились · до 30.10 обменять ≈ $1 160",
        ),
        (
            Runway(
                300_000, 500_000, date(2026, 10, 13), 200_000, date(2026, 10, 15), 20
            ),
            "💳 Сумы 300 000 — хватит только на сегодня · до 15.10 обменять ≈ $20",
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
    assert lines[frozen + 1] == "💳 Сумы 5,00 млн — до конца месяца хватит"
```

- [ ] **Step 2: Тесты падают**

Run: `pytest tests/test_runway.py -q`
Expected: FAIL — `TypeError: uzs_runway() missing 1 required positional argument: 'rates'`: старая карточка ждёт фрейм и строки списка, а не линию прогноза.

- [ ] **Step 3: Карточка из линии «по темпу»**

`app/services/runway.py` заменить целиком:

```python
"""Карточка «💳 Сумы»: когда при нынешнем темпе закончатся сумы и сколько долларов
обменять, чтобы дотянуть до поступления, которое закроет минус.

Её считает модель прогноза (services.forecast): линия «по темпу» в сумах — деньги
сейчас вместе с сегодняшними операциями, темп за 14 дней до сегодня без доходов,
переводов и платежей списка, строки списка в их день. Поэтому «Сводка», утреннее
сообщение и вкладка «Прогноз» называют один и тот же день.
"""

import math
from dataclasses import dataclass
from datetime import date, timedelta

from app.domain import Rates
from app.services.forecast import CurrencyForecast, below_zero

DOLLAR_STEP = 10  # обмен округляем вверх до $10


@dataclass(frozen=True)
class Runway:
    balance: float  # сумы сейчас, с сегодняшними операциями
    pace: float  # средние траты из сумов в день
    last_day: date | None  # последний день, когда сумы ещё не в минусе; None — хватит
    short: float  # сколько сумов не хватит в самой низкой точке; 0 — хватает
    until: date | None  # поступление, которое закроет минус; None — до конца месяца
    dollars: float | None  # сколько долларов обменять; None — не нужно или нет курса


def uzs_runway(line: CurrencyForecast, today: date, rates: Rates) -> Runway:
    """Линия «по темпу» прогноза в сумах → карточка «💳 Сумы»."""
    path = [(today.replace(day=point.day), point.pace) for point in line.days]
    gone = next((day for day, value in path if below_zero(value)), None)
    last_day = None if gone is None else gone - timedelta(days=1)
    lowest_day, lowest = min(path, key=lambda point: point[1])
    if not below_zero(lowest):
        return Runway(line.now, line.per_day_pace, last_day, 0.0, None, None)
    short = -lowest
    until = next(
        (day for day, value in path if day > lowest_day and not below_zero(value)),
        None,
    )
    rate = rates.to_uzs.get("USD")
    dollars = None
    if rate:
        dollars = float(math.ceil(short / rate / DOLLAR_STEP) * DOLLAR_STEP)
    return Runway(line.now, line.per_day_pace, last_day, short, until, dollars)
```

- [ ] **Step 4: Лимит на день берёт карточку из прогноза**

В `app/services/day_budget.py` к импортам добавить `from app.services.forecast import currency_forecast`. Docstring `day_budget`:

```python
    """Лимит на сегодня по листу fact и вкладке месяца; None — во вкладке нет списка.

    Строки списка — по сверке с fact (services.receipts): к утру дня — для лимита и
    заморозки; на сейчас — для «Впереди» и трат дня. Прогноз по сумам — линия «по
    темпу» модели прогноза (services.forecast), как во вкладке «Прогноз».
    """
```

Вызов в `return DayBudget(...)` — было:

```python
        runway=uzs_runway(
            frame,
            [replace(item, day=max(item.day, today.day)) for item in morning],
            today,
            rates,
        ),
```

стало:

```python
        runway=uzs_runway(
            currency_forecast(frame, (), items, today, "UZS"), today, rates
        ),
```

- [ ] **Step 5: Тесты проходят**

Run: `pytest tests/test_runway.py tests/test_day_budget.py tests/test_dashboard.py tests/test_plan_advice.py tests/test_web_api.py -q`
Expected: PASS. Старые ожидания карточки (`💳 Сумы 4,60 млн — до конца месяца хватит`, `runway` в `/api/dashboard`, «Сумы на картах и наличными: 4600000 UZS» советника, `short == 0` у поздней зарплаты 31-го) не меняются: в этих данных нет сегодняшних операций и платежей списка в окне.

- [ ] **Step 6: Полная проверка и коммит**

Run: `black . && ruff check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/services/runway.py app/services/day_budget.py tests/test_runway.py
git commit -m "feat(forecast): the sums card follows the forecast's pace line

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `GET /api/forecast`

**Files:**
- Modify: `app/services/analytics_service.py` (метод `AnalyticsService.forecast`, импорты)
- Modify: `app/web/schemas.py` (модели ответа)
- Modify: `app/web/api.py` (`forecast_out`, маршрут)
- Test: `tests/test_web_forecast.py`

**Interfaces:**
- Consumes: `month_forecast`, `MonthForecast`, `Dip`, `Evening` (Task 2, Task 1); `transactions_frame(..., keep_unknown=True)` (Task 2); `read_forecast(rows) -> Forecast | None` (поле `items`) из `app/services/day_budget.py`; `read_plan(rows).articles` из `app/services/plan.py`; `currency_of(source)` из `app/domain.py`; `bot_data["sources"]` — названия карт.
- Produces: `AnalyticsService.forecast(today: date, currencies: Iterable[str]) -> MonthForecast | None` (None — вкладки месяца нет); схемы `ForecastEventOut`, `ForecastDayOut`, `ForecastDipOut`, `ForecastDipsOut`, `ForecastCurrencyOut`, `ForecastTotalOut`, `ForecastOut`; `api.forecast_out(found: MonthForecast | None, today: date) -> ForecastOut`; маршрут `GET /api/forecast`. JSON — как раздел 4.2 спеки: `status`, `month`, `today`, `end`, `currencies[{code, now, per_day_plan, per_day_pace, spent_today, in_total, events[{day, name, amount, late}], days[{day, plan, pace}], below_zero{plan, pace: {start, until} | null}}]`, `total{uzs[], usd[], below_zero}`.

- [ ] **Step 1: Падающие тесты**

`tests/test_web_forecast.py`:

```python
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
    # кафе 2,6 + инвентарь 0,5 + квартплата 7,8 млн в сумах / 30 дней
    assert found.currencies[0].per_day_plan == 10_900_000 / 30
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


def test_a_currency_without_a_rate_is_outside_the_total() -> None:
    euro = fact_row(date(2026, 9, 1), "💰 ДОХОДЫ", "нач остаток", 300, "EUR")
    data = transactions_frame([FACT_HEADER, *OPENING, euro], RATES, keep_unknown=True)
    found = month_forecast(data, (), (), DAY, RATES, ["UZS", "EUR"])
    body = forecast_for(found)[0].json()
    assert body["currencies"][1] | {"days": [], "events": []} == {
        "code": "EUR",
        "now": 300,
        "per_day_plan": 0,
        "per_day_pace": 0,
        "spent_today": 0,
        "in_total": False,
        "events": [],
        "days": [],
        "below_zero": {"plan": None, "pace": None},
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
```

- [ ] **Step 2: Тесты падают**

Run: `pytest tests/test_web_forecast.py -q`
Expected: FAIL — у `AnalyticsService` нет `forecast` (`AttributeError`), `/api/forecast` отвечает 404.

- [ ] **Step 3: Чтение таблицы**

В `app/services/analytics_service.py`: импорт `from collections.abc import Iterable`; в блок `if TYPE_CHECKING:` добавить `from app.services.forecast import MonthForecast`; после метода `dashboard`:

```python
    def forecast(
        self, today: date, currencies: Iterable[str]
    ) -> "MonthForecast | None":
        """Экран «Прогноз» Mini App; None — вкладки месяца нет."""
        from app.services.day_budget import read_forecast
        from app.services.forecast import month_forecast
        from app.services.plan import read_plan

        sheet = month_title(today)
        try:
            rows = self.gs_service.get_values(sheet)
        except WorksheetNotFound:
            logger.info("No plan sheet %s", sheet)
            return None
        rates = self.gs_service.get_rates()
        frame = transactions_frame(
            self.gs_service.get_values(config.FACT_SHEET_NAME), rates, keep_unknown=True
        )
        listed = read_forecast(rows)
        items = listed.items if listed is not None else ()
        return month_forecast(
            frame, read_plan(rows).articles, items, today, rates, currencies
        )
```

- [ ] **Step 4: Модели ответа**

В `app/web/schemas.py` после `DashboardOut`:

```python
class ForecastEventOut(ApiModel):
    day: int
    name: str
    amount: float  # что ещё впереди по строке: в её валюте и со знаком
    late: bool  # день строки прошёл — ждём сегодня


class ForecastDayOut(ApiModel):
    day: int
    plan: float  # остаток на вечер по плану
    pace: float  # остаток на вечер по темпу


class ForecastDipOut(ApiModel):
    start: int  # первый день ниже нуля
    until: int | None  # первый день после него не ниже нуля; None — не вернётся


class ForecastDipsOut(ApiModel):
    plan: ForecastDipOut | None
    pace: ForecastDipOut | None


class ForecastCurrencyOut(ApiModel):
    code: str
    now: float
    per_day_plan: float
    per_day_pace: float
    spent_today: float
    in_total: bool  # False — нет курса, во «Всего» не входит
    events: list[ForecastEventOut]
    days: list[ForecastDayOut]
    below_zero: ForecastDipsOut


class ForecastTotalOut(ApiModel):
    uzs: list[ForecastDayOut]
    usd: list[ForecastDayOut]  # пусто — нет курса доллара
    below_zero: ForecastDipsOut


class ForecastOut(ApiModel):
    """Прогноз до конца месяца по валютам карт и всего; без вкладки — только статус."""

    status: Literal["ok", "no_month_tab"]
    month: str
    today: date
    end: date | None = None
    currencies: list[ForecastCurrencyOut] = []
    total: ForecastTotalOut | None = None
```

- [ ] **Step 5: Ответ и маршрут**

В `app/web/api.py`: импорт `from collections.abc import Mapping, Sequence` (вместо `Mapping`), `from app.services.forecast import Dip, Evening, MonthForecast`, в импорт из `app.web.schemas` — `ForecastCurrencyOut`, `ForecastDayOut`, `ForecastDipOut`, `ForecastDipsOut`, `ForecastEventOut`, `ForecastOut`, `ForecastTotalOut` (по алфавиту). После `dashboard_out`:

```python
WHOLE_CURRENCIES = frozenset({"UZS", "RUB"})  # остальные валюты — до центов


def _rounded(value: float, code: str) -> float:
    return float(round(value)) if code in WHOLE_CURRENCIES else round(value, 2)


def _days_out(days: Sequence[Evening], code: str) -> list[ForecastDayOut]:
    return [
        ForecastDayOut(
            day=point.day,
            plan=_rounded(point.plan, code),
            pace=_rounded(point.pace, code),
        )
        for point in days
    ]


def _dips_out(plan: Dip | None, pace: Dip | None) -> ForecastDipsOut:
    def one(dip: Dip | None) -> ForecastDipOut | None:
        return None if dip is None else ForecastDipOut(start=dip.start, until=dip.until)

    return ForecastDipsOut(plan=one(plan), pace=one(pace))


def forecast_out(found: MonthForecast | None, today: date) -> ForecastOut:
    """Прогноз для экрана; без вкладки месяца — только статус."""
    if found is None:
        return ForecastOut(status="no_month_tab", month=month_title(today), today=today)
    currencies = [
        ForecastCurrencyOut(
            code=line.code,
            now=_rounded(line.now, line.code),
            per_day_plan=_rounded(line.per_day_plan, line.code),
            per_day_pace=_rounded(line.per_day_pace, line.code),
            spent_today=_rounded(line.spent_today, line.code),
            in_total=line.code in found.counted,
            events=[
                ForecastEventOut(
                    day=event.day,
                    name=event.name,
                    amount=_rounded(event.amount, line.code),
                    late=event.late,
                )
                for event in line.events
            ],
            days=_days_out(line.days, line.code),
            below_zero=_dips_out(line.plan_dip, line.pace_dip),
        )
        for line in found.currencies
    ]
    return ForecastOut(
        status="ok",
        month=month_title(found.today),
        today=found.today,
        end=found.end,
        currencies=currencies,
        total=ForecastTotalOut(
            uzs=_days_out(found.total_uzs, "UZS"),
            usd=_days_out(found.total_usd, "USD"),
            below_zero=_dips_out(found.plan_dip, found.pace_dip),
        ),
    )
```

В `routes` после `dashboard`:

```python
    async def forecast(request: Request) -> JSONResponse:
        user = current_user(request)
        today = local_today(config.ANALYTICS_TIMEZONE)
        analytics = application.bot_data["analytics_service"]
        sources: list[str] = application.bot_data.get("sources", [])
        # валюты карт бота; без справочника карт — хотя бы сумы
        codes = [currency_of(source) for source in sources] or ["UZS"]
        try:
            found = await asyncio.to_thread(analytics.forecast, today, codes)
        except Exception:
            logger.exception("Forecast failed for user_id=%s", user.id)
            return error_response(503, "sheets_unavailable", SHEETS_DOWN)
        return JSONResponse(forecast_out(found, today).model_dump(mode="json"))
```

и в список маршрутов после `Route("/dashboard", dashboard),`:

```python
        Route("/forecast", forecast),
```

- [ ] **Step 6: Тесты проходят**

Run: `pytest tests/test_web_forecast.py -q`
Expected: PASS (10 тестов).

- [ ] **Step 7: Полная проверка и коммит**

Run: `black . && ruff check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/services/analytics_service.py app/web/schemas.py app/web/api.py tests/test_web_forecast.py
git commit -m "feat(api): GET /api/forecast — money until the month end

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Приложение — тип ответа, тестовые данные, модель вида

**Files:**
- Modify: `webapp/src/api.ts` (типы, `api.forecast`)
- Modify: `webapp/src/mocks/fixtures.ts` (`forecastFixture`), `webapp/src/mocks/api.ts`, `webapp/src/test/fakeApi.ts`
- Create: `webapp/src/forecast/view.ts`
- Test: `webapp/src/forecast/view.test.ts`

**Interfaces:**
- Consumes: JSON `GET /api/forecast` из Task 4; `money`, `longDay` из `webapp/src/format.ts`.
- Produces: типы `ForecastEvent {day, name, amount, late}`, `ForecastDay {day, plan, pace}`, `ForecastDip {start, until}`, `ForecastDips {plan, pace}`, `ForecastCurrency {code, now, per_day_plan, per_day_pace, spent_today, in_total, events, days, below_zero}`, `Forecast {status, month, today, end, currencies, total}`; `api.forecast(): Promise<Forecast>`; `forecastFixture: Forecast` (15 октября: UZS, USD, RUB). Из `view.ts`: `TOTAL = "total"`, `VIEW_KEY = "forecast-view"`, `View {scope: string; unit: "uzs" | "usd"}`, `DEFAULT_VIEW`, `DayEvent` (событие + `currency`), `CurrencyRow {code, now, end, pace, minus, counted}`; функции `dayOf(iso)`, `isoDay(today, day)`, `shortDate(day, today)`, `weekdayShort(iso)`, `dayTitle(iso)`, `scopes(data)`, `hasDollars(data)`, `viewCode(view)`, `viewDays(data, view)`, `viewDips(data, view)`, `dayEvents(data, view, day)`, `marks(data, view)` → `{day, up}[]`, `amount(value, code)`, `signed(value, code)`, `pillValue(value, code)`, `pillUnit(code)`, `chartNote(view)`, `weekOf(day, today)`, `warnings(data, view)`, `ordinary(data, view, day)`, `perDay(data, view)`, `currencyRows(data)`, `minusRange(dip, today)`, `loadView(data)`, `storeView(view)`.

- [ ] **Step 1: Типы и запрос**

В `webapp/src/api.ts` после `Dashboard`:

```ts
/** Строка списка, которая ещё впереди: сумма — остаток, в валюте строки. */
export interface ForecastEvent {
  day: number;
  name: string;
  amount: number;
  late: boolean; // день строки прошёл — ждём сегодня
}

/** Сколько будет вечером дня: по плану и по темпу. */
export interface ForecastDay {
  day: number;
  plan: number;
  pace: number;
}

/** Уход в минус: первый день ниже нуля и когда остаток вернётся. */
export interface ForecastDip {
  start: number;
  until: number | null; // null — до конца месяца не вернётся
}

export interface ForecastDips {
  plan: ForecastDip | null;
  pace: ForecastDip | null;
}

export interface ForecastCurrency {
  code: string;
  now: number;
  per_day_plan: number;
  per_day_pace: number;
  spent_today: number;
  in_total: boolean; // false — нет курса, во «Всего» не входит
  events: ForecastEvent[];
  days: ForecastDay[]; // с сегодня по последний день месяца
  below_zero: ForecastDips;
}

export interface Forecast {
  status: "ok" | "no_month_tab";
  month: string;
  today: string;
  end: string | null;
  currencies: ForecastCurrency[];
  total: {
    uzs: ForecastDay[];
    usd: ForecastDay[]; // пусто — нет курса доллара
    below_zero: ForecastDips;
  } | null;
}
```

и в объект `api` после `dashboard`:

```ts
  forecast: () => request<Forecast>("/forecast"),
```

- [ ] **Step 2: Тестовые данные**

В `webapp/src/mocks/fixtures.ts`: в импорт типов добавить `Forecast`, `ForecastDay`; в конец файла:

```ts
/** Остаток на вечер с 15-го по 31-е октября: деньги сейчас + события − в день. */
function evenings(
  now: number,
  plan: number,
  pace: number,
  moves: Record<number, number>,
): ForecastDay[] {
  const days: ForecastDay[] = [];
  let byPlan = now;
  let byPace = now;
  for (let day = 15; day <= 31; day += 1) {
    byPlan += (moves[day] ?? 0) - plan;
    byPace += (moves[day] ?? 0) - pace;
    days.push({ day, plan: byPlan, pace: byPace });
  }
  return days;
}

// пример спеки: сумы уходят в минус по темпу 25-го и возвращаются с ЗП 30-го
const uzsDays = evenings(6_810_000, 470_000, 850_000, {
  15: 2_500_000,
  17: -320_000,
  30: 30_000_000,
});
const usdDays = evenings(1_800, 3, 5, { 18: -500 });
const rubDays = evenings(20_000, 300, 0, {});
const totalUzs: ForecastDay[] = uzsDays.map((point, index) => ({
  day: point.day,
  plan:
    point.plan +
    (usdDays[index]?.plan ?? 0) * 12_000 +
    (rubDays[index]?.plan ?? 0) * 150,
  pace:
    point.pace +
    (usdDays[index]?.pace ?? 0) * 12_000 +
    (rubDays[index]?.pace ?? 0) * 150,
}));

export const forecastFixture: Forecast = {
  status: "ok",
  month: "Oct 26",
  today: "2026-10-15",
  end: "2026-10-31",
  currencies: [
    {
      code: "UZS",
      now: 6_810_000,
      per_day_plan: 470_000,
      per_day_pace: 850_000,
      spent_today: 0,
      in_total: true,
      events: [
        { day: 15, name: "Аванс", amount: 2_500_000, late: false },
        { day: 17, name: "Детский центр", amount: -320_000, late: false },
        { day: 30, name: "ЗП", amount: 30_000_000, late: false },
      ],
      days: uzsDays,
      below_zero: { plan: null, pace: { start: 25, until: 30 } },
    },
    {
      code: "USD",
      now: 1_800,
      per_day_plan: 3,
      per_day_pace: 5,
      spent_today: 0,
      in_total: true,
      events: [{ day: 18, name: "Квартплата", amount: -500, late: false }],
      days: usdDays,
      below_zero: { plan: null, pace: null },
    },
    {
      code: "RUB",
      now: 20_000,
      per_day_plan: 300,
      per_day_pace: 0,
      spent_today: 0,
      in_total: true,
      events: [],
      days: rubDays,
      below_zero: { plan: null, pace: null },
    },
  ],
  total: {
    uzs: totalUzs,
    usd: totalUzs.map((point) => ({
      day: point.day,
      plan: point.plan / 12_000,
      pace: point.pace / 12_000,
    })),
    below_zero: { plan: null, pace: null },
  },
};
```

В `webapp/src/mocks/api.ts`: импорт `forecastFixture` и в `mockApi` после `dashboard`:

```ts
  forecast: () => later(forecastFixture),
```

В `webapp/src/test/fakeApi.ts`: импорт `forecastFixture` и после `dashboard`:

```ts
    forecast: vi.fn<Api["forecast"]>().mockResolvedValue(forecastFixture),
```

- [ ] **Step 3: Падающие тесты модели вида**

`webapp/src/forecast/view.test.ts`:

```ts
import { afterEach, describe, expect, it } from "vitest";
import type { Forecast } from "../api";
import { forecastFixture } from "../mocks/fixtures";
import {
  DEFAULT_VIEW,
  VIEW_KEY,
  amount,
  chartNote,
  currencyRows,
  dayEvents,
  dayTitle,
  loadView,
  marks,
  minusRange,
  ordinary,
  perDay,
  pillUnit,
  pillValue,
  storeView,
  viewDays,
  warnings,
  weekOf,
  weekdayShort,
  type View,
} from "./view";

/** Пробелы между разрядами — неразрывные, как в money(). */
const nb = (text: string) => text.replace(/(\d) (?=\d)/g, "$1 ");
const UZS: View = { scope: "UZS", unit: "uzs" };
const DOLLARS: View = { scope: "total", unit: "usd" };

function withUzs(change: Partial<Forecast["currencies"][number]>): Forecast {
  return {
    ...forecastFixture,
    currencies: forecastFixture.currencies.map((line) =>
      line.code === "UZS" ? { ...line, ...change } : line,
    ),
  };
}

describe("forecast view", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("finds the strip week of a day", () => {
    expect(weekOf(15, 15)).toBe(15);
    expect(weekOf(25, 15)).toBe(22);
    expect(weekOf(31, 15)).toBe(29);
  });

  it("names the days", () => {
    expect(weekdayShort("2026-10-15")).toBe("чт");
    expect(dayTitle("2026-10-17")).toBe("Суббота, 17 октября");
  });

  it("writes sums in millions on the pills and other money whole", () => {
    expect(pillValue(8_840_000, "UZS")).toBe("8,8");
    expect(pillValue(-360_000, "UZS")).toBe("−0,4");
    expect(pillValue(-20_000, "UZS")).toBe("0,0");
    expect(pillValue(1_797.4, "USD")).toBe(nb("1 797"));
    expect(pillUnit("UZS")).toBe("млн сум");
    expect(pillUnit("RUB")).toBe("₽");
    expect(amount(-500, "USD")).toBe("−$500");
    expect(amount(20_000, "RUB")).toBe(nb("20 000 ₽"));
    expect(amount(300, "EUR")).toBe("300 EUR");
  });

  it("takes the lines of the chosen view", () => {
    expect(viewDays(forecastFixture, UZS)[2]).toEqual({
      day: 17,
      plan: 7_580_000,
      pace: 6_440_000,
    });
    expect(viewDays(forecastFixture, DEFAULT_VIEW)[0]?.plan).toBe(33_359_000);
    expect(viewDays(forecastFixture, DOLLARS)[0]?.plan).toBeCloseTo(
      33_359_000 / 12_000,
    );
    expect(chartNote(DEFAULT_VIEW)).toBe("в сумах, по курсу таблицы");
    expect(chartNote(UZS)).toBe("в сумах");
  });

  it("collects the events of a day and the marks of the chart", () => {
    expect(dayEvents(forecastFixture, DEFAULT_VIEW, 18)).toEqual([
      { day: 18, name: "Квартплата", amount: -500, late: false, currency: "USD" },
    ]);
    expect(dayEvents(forecastFixture, UZS, 18)).toEqual([]);
    expect(marks(forecastFixture, UZS)).toEqual([
      { day: 15, up: true },
      { day: 17, up: false },
      { day: 30, up: true },
    ]);
  });

  it("warns when a line of the view goes below zero", () => {
    expect(warnings(forecastFixture, DEFAULT_VIEW)).toEqual([]);
    expect(warnings(forecastFixture, UZS)).toEqual([
      "⚠️ По темпу сумы уйдут в минус 25.10, вернутся 30.10",
    ]);
    const gone = withUzs({
      below_zero: { plan: { start: 15, until: null }, pace: null },
    });
    expect(warnings(gone, UZS)).toEqual([
      "⚠️ По плану сумы в минусе уже сегодня",
    ]);
  });

  it("lists the ordinary spending of a day by currencies", () => {
    expect(ordinary(forecastFixture, DEFAULT_VIEW, 17)).toBe(
      nb("≈ 470 000 сум + $3 + 300 ₽"),
    );
    const spent = withUzs({ spent_today: 100_000 });
    expect(ordinary(spent, UZS, 15)).toBe(nb("≈ 370 000 сум"));
    expect(ordinary(spent, UZS, 16)).toBe(nb("≈ 470 000 сум"));
    expect(perDay(forecastFixture, UZS)).toBe(
      nb("В день: по плану 470 000 сум, по темпу 850 000 сум."),
    );
  });

  it("shows every currency now and at the month end", () => {
    expect(currencyRows(forecastFixture)).toEqual([
      {
        code: "UZS",
        now: nb("6 810 000 сум"),
        end: nb("31 000 000 сум"),
        pace: "минус 25–29.10",
        minus: true,
        counted: true,
      },
      {
        code: "USD",
        now: nb("$1 800"),
        end: nb("$1 249"),
        pace: nb("по темпу $1 215"),
        minus: false,
        counted: true,
      },
      {
        code: "RUB",
        now: nb("20 000 ₽"),
        end: nb("14 900 ₽"),
        pace: nb("по темпу 20 000 ₽"),
        minus: false,
        counted: true,
      },
    ]);
    expect(minusRange({ start: 25, until: 26 }, "2026-10-15")).toBe(
      "минус 25.10",
    );
    expect(minusRange({ start: 25, until: null }, "2026-10-15")).toBe(
      "минус с 25.10",
    );
  });

  it("remembers the view and falls back when it no longer fits", () => {
    expect(loadView(forecastFixture)).toEqual(DEFAULT_VIEW);
    storeView(UZS);
    expect(JSON.parse(localStorage.getItem(VIEW_KEY) ?? "null")).toEqual(UZS);
    expect(loadView(forecastFixture)).toEqual(UZS);
    storeView(DOLLARS);
    expect(loadView(forecastFixture)).toEqual(DOLLARS);
    const noDollar: Forecast = {
      ...forecastFixture,
      total: forecastFixture.total && { ...forecastFixture.total, usd: [] },
    };
    expect(loadView(noDollar)).toEqual(DEFAULT_VIEW);
    localStorage.setItem(VIEW_KEY, JSON.stringify({ scope: "EUR", unit: "uzs" }));
    expect(loadView(forecastFixture)).toEqual(DEFAULT_VIEW);
    localStorage.setItem(VIEW_KEY, "{");
    expect(loadView(forecastFixture)).toEqual(DEFAULT_VIEW);
  });
});
```

- [ ] **Step 4: Тесты падают**

Run: `cd webapp && npx vitest run src/forecast/view.test.ts`
Expected: FAIL — `Failed to resolve import "./view"`.

- [ ] **Step 5: Модель вида**

`webapp/src/forecast/view.ts`:

```ts
/** Вид экрана «Прогноз»: что показать, числа и подписи вида, память выбора. */

import type {
  Forecast,
  ForecastCurrency,
  ForecastDay,
  ForecastDip,
  ForecastDips,
  ForecastEvent,
} from "../api";
import { longDay, money } from "../format";

export const TOTAL = "total";
export const VIEW_KEY = "forecast-view";

/** «Всего» в сумах или долларах, или одна валюта (её код). */
export interface View {
  scope: string;
  unit: "uzs" | "usd";
}

export const DEFAULT_VIEW: View = { scope: TOTAL, unit: "uzs" };

const NO_DIPS: ForecastDips = { plan: null, pace: null };
const SIGNS: Record<string, string> = { UZS: "сум", USD: "$", RUB: "₽" };
const SUBJECTS: Record<string, string> = {
  UZS: "сумы",
  USD: "доллары",
  RUB: "рубли",
};
const IN_CURRENCY: Record<string, string> = {
  UZS: "в сумах",
  USD: "в долларах",
  RUB: "в рублях",
};
const WEEKDAYS = ["вс", "пн", "вт", "ср", "чт", "пт", "сб"];
const WEEKDAYS_LONG = [
  "Воскресенье",
  "Понедельник",
  "Вторник",
  "Среда",
  "Четверг",
  "Пятница",
  "Суббота",
];

/** «2026-10-15» → 15. */
export function dayOf(iso: string): number {
  return Number(iso.slice(8, 10));
}

/** День месяца сегодняшней даты: «2026-10-15» и 17 → «2026-10-17». */
export function isoDay(today: string, day: number): string {
  return `${today.slice(0, 7)}-${String(day).padStart(2, "0")}`;
}

/** 25 → «25.10». */
export function shortDate(day: number, today: string): string {
  return `${String(day).padStart(2, "0")}.${today.slice(5, 7)}`;
}

function weekday(iso: string): number {
  return new Date(`${iso}T00:00:00Z`).getUTCDay();
}

/** «сб». */
export function weekdayShort(iso: string): string {
  return WEEKDAYS[weekday(iso)] ?? "";
}

/** «Суббота, 17 октября». */
export function dayTitle(iso: string): string {
  return `${WEEKDAYS_LONG[weekday(iso)] ?? ""}, ${longDay(iso)}`;
}

/** Кнопки переключателя: «Всего» и валюты ответа. */
export function scopes(data: Forecast): string[] {
  return [TOTAL, ...data.currencies.map((line) => line.code)];
}

/** «Всего» в долларах — только если у таблицы есть курс доллара. */
export function hasDollars(data: Forecast): boolean {
  return (data.total?.usd.length ?? 0) > 0;
}

/** Валюта чисел вида: у «Всего» — UZS или USD. */
export function viewCode(view: View): string {
  if (view.scope !== TOTAL) return view.scope;
  return view.unit === "usd" ? "USD" : "UZS";
}

/** Валюты вида: во «Всего» — все, иначе одна. */
function viewCurrencies(data: Forecast, view: View): ForecastCurrency[] {
  return view.scope === TOTAL
    ? data.currencies
    : data.currencies.filter((line) => line.code === view.scope);
}

/** Остаток на вечер по дням в числах вида. */
export function viewDays(data: Forecast, view: View): ForecastDay[] {
  if (view.scope !== TOTAL) return viewCurrencies(data, view)[0]?.days ?? [];
  return (view.unit === "usd" ? data.total?.usd : data.total?.uzs) ?? [];
}

export function viewDips(data: Forecast, view: View): ForecastDips {
  if (view.scope !== TOTAL) {
    return viewCurrencies(data, view)[0]?.below_zero ?? NO_DIPS;
  }
  return data.total?.below_zero ?? NO_DIPS;
}

export interface DayEvent extends ForecastEvent {
  currency: string;
}

/** События дня в виде — каждое со своей валютой. */
export function dayEvents(data: Forecast, view: View, day: number): DayEvent[] {
  return viewCurrencies(data, view).flatMap((line) =>
    line.events
      .filter((event) => event.day === day)
      .map((event) => ({ ...event, currency: line.code })),
  );
}

/** Отметки графика: ▲ в дни поступлений, ▼ в дни платежей. */
export function marks(
  data: Forecast,
  view: View,
): { day: number; up: boolean }[] {
  const found: { day: number; up: boolean }[] = [];
  for (const line of viewCurrencies(data, view)) {
    for (const event of line.events) {
      const up = event.amount > 0;
      if (!found.some((mark) => mark.day === event.day && mark.up === up)) {
        found.push({ day: event.day, up });
      }
    }
  }
  return found.sort((a, b) => a.day - b.day);
}

/** «9 060 000 сум», «−$500», «20 000 ₽», «300 EUR» — целыми. */
export function amount(value: number, code: string): string {
  if (code === "USD") {
    const sign = value < 0 && Math.round(value) !== 0 ? "−" : "";
    return `${sign}$${money(Math.abs(value))}`;
  }
  return `${money(value)} ${SIGNS[code] ?? code}`;
}

/** Со знаком: «+2 500 000 сум», «−$500». */
export function signed(value: number, code: string): string {
  return `${value > 0 ? "+" : ""}${amount(value, code)}`;
}

/** Число на плашке: сумы — миллионы с одним знаком, остальное — целыми. */
export function pillValue(value: number, code: string): string {
  if (code !== "UZS") return money(value);
  const millions = (Math.abs(value) / 1_000_000).toFixed(1).replace(".", ",");
  return `${value < 0 && millions !== "0,0" ? "−" : ""}${millions}`;
}

/** В чём числа плашек: «млн сум», «$», «₽». */
export function pillUnit(code: string): string {
  return code === "UZS" ? "млн сум" : (SIGNS[code] ?? code);
}

/** Подпись графика: «в сумах, по курсу таблицы» у «Всего», «в рублях» у валюты. */
export function chartNote(view: View): string {
  const code = viewCode(view);
  const unit = IN_CURRENCY[code] ?? `в ${code}`;
  return view.scope === TOTAL ? `${unit}, по курсу таблицы` : unit;
}

/** Первый день недели ленты, в которой день day: недели идут с сегодня по семь дней. */
export function weekOf(day: number, today: number): number {
  return today + Math.floor((day - today) / 7) * 7;
}

/** «⚠️ По темпу сумы уйдут в минус 25.10, вернутся 30.10» — по линии в минусе. */
export function warnings(data: Forecast, view: View): string[] {
  const dips = viewDips(data, view);
  const subject =
    view.scope === TOTAL ? "деньги" : (SUBJECTS[view.scope] ?? view.scope);
  const today = dayOf(data.today);
  const lines: [string, ForecastDip | null][] = [
    ["По плану", dips.plan],
    ["По темпу", dips.pace],
  ];
  return lines.flatMap(([line, dip]) => {
    if (!dip) return [];
    const when =
      dip.start === today
        ? "в минусе уже сегодня"
        : `уйдут в минус ${shortDate(dip.start, data.today)}`;
    const back =
      dip.until !== null ? `, вернутся ${shortDate(dip.until, data.today)}` : "";
    return [`⚠️ ${line} ${subject} ${when}${back}`];
  });
}

/** «≈ 470 000 сум + $3 + 300 ₽» — обычные траты дня по плану; сегодня — остаток дня. */
export function ordinary(data: Forecast, view: View, day: number): string | null {
  const today = dayOf(data.today);
  const parts = viewCurrencies(data, view)
    .map((line) => ({
      code: line.code,
      value:
        day === today
          ? Math.max(line.per_day_plan - line.spent_today, 0)
          : line.per_day_plan,
    }))
    .filter((part) => Math.round(part.value) > 0)
    .map((part) => amount(part.value, part.code));
  return parts.length > 0 ? `≈ ${parts.join(" + ")}` : null;
}

/** «В день: по плану 470 000 сум, по темпу 850 000 сум.» */
export function perDay(data: Forecast, view: View): string {
  const shown = viewCurrencies(data, view);
  const list = (pick: (line: ForecastCurrency) => number) =>
    shown.map((line) => amount(pick(line), line.code)).join(" + ");
  return `В день: по плану ${list((line) => line.per_day_plan)}, по темпу ${list((line) => line.per_day_pace)}.`;
}

/** «минус 25–29.10», «минус 25.10» (один день) или «минус с 25.10» (не вернётся). */
export function minusRange(dip: ForecastDip, today: string): string {
  if (dip.until === null) return `минус с ${shortDate(dip.start, today)}`;
  const last = dip.until - 1;
  return last === dip.start
    ? `минус ${shortDate(last, today)}`
    : `минус ${dip.start}–${shortDate(last, today)}`;
}

export interface CurrencyRow {
  code: string;
  now: string;
  end: string; // по плану на последний день
  pace: string; // «по темпу …» или «минус …»
  minus: boolean;
  counted: boolean; // во «Всего»
}

/** «По валютам»: деньги сейчас → по плану на конец месяца, справа — темп или минус. */
export function currencyRows(data: Forecast): CurrencyRow[] {
  return data.currencies.map((line) => {
    const last = line.days[line.days.length - 1];
    const dip = line.below_zero.pace;
    return {
      code: line.code,
      now: amount(line.now, line.code),
      end: last ? amount(last.plan, line.code) : "",
      pace: dip
        ? minusRange(dip, data.today)
        : `по темпу ${last ? amount(last.pace, line.code) : ""}`,
      minus: dip !== null,
      counted: line.in_total,
    };
  });
}

/** Вид из памяти телефона, если он подходит к ответу; иначе «Всего, сум». */
export function loadView(data: Forecast): View {
  try {
    const text = localStorage.getItem(VIEW_KEY);
    if (!text) return DEFAULT_VIEW;
    const value: unknown = JSON.parse(text);
    if (typeof value !== "object" || value === null || !("scope" in value)) {
      return DEFAULT_VIEW;
    }
    const scope: unknown = value.scope;
    const unit: unknown = "unit" in value ? value.unit : "uzs";
    if (typeof scope !== "string" || !scopes(data).includes(scope)) {
      return DEFAULT_VIEW;
    }
    return { scope, unit: unit === "usd" && hasDollars(data) ? "usd" : "uzs" };
  } catch {
    return DEFAULT_VIEW;
  }
}

export function storeView(view: View): void {
  try {
    localStorage.setItem(VIEW_KEY, JSON.stringify(view));
  } catch {
    // память недоступна (приватный режим): вид живёт до закрытия приложения
  }
}
```

- [ ] **Step 6: Тесты проходят**

Run: `cd webapp && npx vitest run src/forecast/view.test.ts`
Expected: PASS (9 тестов).

- [ ] **Step 7: Полная проверка и коммит**

Run: `cd webapp && npx prettier --write src && npm run lint && npm test && npm run build`
Expected: всё зелёное.

```bash
git add webapp/src/api.ts webapp/src/mocks webapp/src/test/fakeApi.ts webapp/src/forecast
git commit -m "feat(webapp): forecast types, test data and the view model

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: График «До конца месяца»

**Files:**
- Create: `webapp/src/forecast/chart.ts`, `webapp/src/forecast/ForecastChart.tsx`
- Test: `webapp/src/forecast/chart.test.ts`, `webapp/src/forecast/ForecastChart.test.tsx`

**Interfaces:**
- Consumes: `ForecastDay` из `webapp/src/api.ts` (Task 5).
- Produces: `Box {width, top, bottom, left, right}`, `Geometry {x(day), y(value), zero, plan, pace, below, slot}`, `geometry(days: ForecastDay[], box: Box): Geometry`, `labelDays(first: number, last: number): number[]`; компонент `ForecastChart` (default) с пропсами `{days: ForecastDay[]; marks: {day: number; up: boolean}[]; selected: number; label: string; onSelect: (day: number) => void}`: `role="img"` с `aria-label={label}`, зона нажатия `rect[data-day]` на каждый день.

- [ ] **Step 1: Падающие тесты**

`webapp/src/forecast/chart.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { geometry, labelDays } from "./chart";

const BOX = { width: 320, top: 10, bottom: 110, left: 8, right: 8 };

describe("forecast chart geometry", () => {
  it("puts today at the left edge and the last day at the right one", () => {
    const days = [
      { day: 15, plan: 100, pace: 100 },
      { day: 31, plan: 50, pace: 20 },
    ];
    const chart = geometry(days, BOX);
    expect([chart.x(15), chart.x(31)]).toEqual([8, 312]);
    expect(chart.slot).toBe(19); // 304 точки на 16 промежутков
  });

  it("puts zero inside the chart when a line goes below it", () => {
    const chart = geometry(
      [
        { day: 15, plan: 100, pace: 100 },
        { day: 16, plan: 50, pace: -100 },
      ],
      BOX,
    );
    expect(chart.below).toBe(true);
    expect([chart.y(100), chart.zero, chart.y(-100)]).toEqual([10, 60, 110]);
    expect(chart.pace).toBe("M8,10 L312,110");
  });

  it("keeps zero at the bottom when nothing is below it", () => {
    const chart = geometry([{ day: 15, plan: 100, pace: 50 }], BOX);
    expect(chart.below).toBe(false);
    expect(chart.zero).toBe(110);
  });

  it("draws one point in the middle on the last day of the month", () => {
    const chart = geometry([{ day: 31, plan: 100, pace: 100 }], BOX);
    expect(chart.x(31)).toBe(160);
    expect(chart.plan).toBe("M160,10");
  });

  it("labels today, every fifth day and the last one", () => {
    expect(labelDays(15, 31)).toEqual([15, 20, 25, 31]);
    expect(labelDays(1, 30)).toEqual([1, 5, 10, 15, 20, 25, 30]);
    expect(labelDays(29, 31)).toEqual([29, 31]);
    expect(labelDays(31, 31)).toEqual([31]);
  });
});
```

`webapp/src/forecast/ForecastChart.test.tsx`:

```tsx
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import ForecastChart from "./ForecastChart";

const DAYS = [
  { day: 15, plan: 100, pace: 50 },
  { day: 16, plan: 60, pace: -20 },
  { day: 17, plan: 30, pace: -60 },
];

describe("ForecastChart", () => {
  it("draws both lines, red below zero and a tap zone for every day", () => {
    const onSelect = vi.fn();
    const { container } = render(
      <ForecastChart
        days={DAYS}
        marks={[{ day: 17, up: false }]}
        selected={15}
        label="До конца месяца"
        onSelect={onSelect}
      />,
    );
    expect(screen.getByRole("img", { name: "До конца месяца" })).toBeTruthy();
    expect(container.querySelectorAll("[data-day]")).toHaveLength(3);
    expect(container.querySelector("g[clip-path]")).not.toBeNull();
    expect(screen.getByText("▼")).toBeTruthy();
    const zone = container.querySelector('[data-day="16"]');
    if (!zone) throw new Error("no tap zone");
    fireEvent.click(zone);
    expect(onSelect).toHaveBeenCalledWith(16);
  });

  it("has no red part when the money stays above zero", () => {
    const { container } = render(
      <ForecastChart
        days={[{ day: 31, plan: 10, pace: 5 }]}
        marks={[]}
        selected={31}
        label="До конца месяца"
        onSelect={() => undefined}
      />,
    );
    expect(container.querySelector("g[clip-path]")).toBeNull();
    expect(container.querySelectorAll("[data-day]")).toHaveLength(1);
  });
});
```

- [ ] **Step 2: Тесты падают**

Run: `cd webapp && npx vitest run src/forecast/chart.test.ts src/forecast/ForecastChart.test.tsx`
Expected: FAIL — `Failed to resolve import "./chart"` и `"./ForecastChart"`.

- [ ] **Step 3: Геометрия**

`webapp/src/forecast/chart.ts`:

```ts
/** Геометрия графика «До конца месяца»: две линии по дням и нулевая линия. */

import type { ForecastDay } from "../api";

export interface Box {
  width: number;
  top: number; // верх шкалы
  bottom: number; // низ шкалы
  left: number;
  right: number;
}

export interface Geometry {
  x: (day: number) => number;
  y: (value: number) => number;
  zero: number; // y нулевой линии
  plan: string; // путь линии «по плану»
  pace: string; // путь линии «по темпу»
  below: boolean; // есть точки ниже нуля
  slot: number; // ширина дня — для зон нажатия
}

const round = (value: number) => Math.round(value * 10) / 10; // SVG хватит десятых

/** Шкала от минимума (не выше нуля) до максимума (не ниже нуля); один день — посередине. */
export function geometry(days: ForecastDay[], box: Box): Geometry {
  const values = days.flatMap((point) => [point.plan, point.pace]);
  const max = Math.max(0, ...values);
  const min = Math.min(0, ...values);
  const span = max - min || 1;
  const first = days[0]?.day ?? 1;
  const last = days[days.length - 1]?.day ?? first;
  const inner = box.width - box.left - box.right;
  const slot = last > first ? inner / (last - first) : inner;
  const x = (day: number) =>
    round(box.left + (last > first ? (day - first) * slot : inner / 2));
  const y = (value: number) =>
    round(box.top + ((max - value) / span) * (box.bottom - box.top));
  const path = (pick: (point: ForecastDay) => number) =>
    days
      .map((point, index) => `${index === 0 ? "M" : "L"}${x(point.day)},${y(pick(point))}`)
      .join(" ");
  return {
    x,
    y,
    zero: y(0),
    plan: path((point) => point.plan),
    pace: path((point) => point.pace),
    below: min < 0,
    slot: round(slot),
  };
}

/** Подписи под графиком: сегодня, каждое пятое число и последний день, не теснее трёх дней. */
export function labelDays(first: number, last: number): number[] {
  const days = [first];
  for (let day = 5; day < last; day += 5) {
    if (day - (days[days.length - 1] ?? first) >= 3) days.push(day);
  }
  const tail = days[days.length - 1] ?? first;
  if (last !== tail) {
    if (last - tail < 3 && days.length > 1) days.pop();
    days.push(last);
  }
  return days;
}
```

- [ ] **Step 4: Компонент**

`webapp/src/forecast/ForecastChart.tsx`:

```tsx
import { useId } from "react";
import type { ForecastDay } from "../api";
import { geometry, labelDays } from "./chart";

const WIDTH = 320;
const HEIGHT = 150;
const BOX = { width: WIDTH, top: 14, bottom: HEIGHT - 18, left: 8, right: 8 };

interface Props {
  days: ForecastDay[];
  marks: { day: number; up: boolean }[]; // ▲ поступление, ▼ платёж
  selected: number;
  label: string;
  onSelect: (day: number) => void;
}

/** Сплошная — по плану, пунктир — по темпу; ниже нуля линии красные, нажатие выбирает день. */
export default function ForecastChart({
  days,
  marks,
  selected,
  label,
  onSelect,
}: Props) {
  const clip = `below-${useId().replace(/[^a-zA-Z0-9_-]/g, "")}`;
  const first = days[0];
  if (!first) return null;
  const last = days[days.length - 1] ?? first;
  const chart = geometry(days, BOX);
  const point = days.find((item) => item.day === selected) ?? first;
  const anchor = (day: number) => {
    if (first.day === last.day) return "middle";
    if (day === first.day) return "start";
    return day === last.day ? "end" : "middle";
  };
  return (
    <svg
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      className="w-full overflow-visible"
      role="img"
      aria-label={label}
    >
      <defs>
        <clipPath id={clip}>
          <rect
            x={0}
            y={chart.zero}
            width={WIDTH}
            height={Math.max(BOX.bottom - chart.zero, 0)}
          />
        </clipPath>
      </defs>
      {chart.below && (
        <rect
          x={0}
          y={chart.zero}
          width={WIDTH}
          height={BOX.bottom - chart.zero}
          fill="var(--color-over)"
          opacity={0.08}
        />
      )}
      <line
        x1={0}
        x2={WIDTH}
        y1={chart.zero}
        y2={chart.zero}
        stroke="var(--color-hint)"
        opacity={0.5}
      />
      <line
        x1={chart.x(point.day)}
        x2={chart.x(point.day)}
        y1={BOX.top}
        y2={BOX.bottom}
        stroke="var(--color-hint)"
        strokeDasharray="2 3"
      />
      <path d={chart.plan} fill="none" stroke="var(--color-link)" strokeWidth={2} />
      <path
        d={chart.pace}
        fill="none"
        stroke="var(--color-warn)"
        strokeWidth={2}
        strokeDasharray="5 4"
      />
      {chart.below && (
        <g clipPath={`url(#${clip})`}>
          <path d={chart.plan} fill="none" stroke="var(--color-over)" strokeWidth={2} />
          <path
            d={chart.pace}
            fill="none"
            stroke="var(--color-over)"
            strokeWidth={2}
            strokeDasharray="5 4"
          />
        </g>
      )}
      {marks.map((mark) => {
        const at = days.find((item) => item.day === mark.day);
        if (!at) return null;
        const y = chart.y(at.plan);
        return (
          <text
            key={`${mark.day}-${mark.up ? "up" : "down"}`}
            x={chart.x(mark.day)}
            y={mark.up ? y - 6 : y + 12}
            fontSize={9}
            textAnchor="middle"
            fill={mark.up ? "var(--color-ok)" : "var(--color-over)"}
          >
            {mark.up ? "▲" : "▼"}
          </text>
        );
      })}
      <circle
        cx={chart.x(first.day)}
        cy={chart.y(first.plan)}
        r={3}
        fill="var(--color-button)"
      />
      <circle
        cx={chart.x(point.day)}
        cy={chart.y(point.plan)}
        r={3.5}
        fill="var(--color-link)"
      />
      <circle
        cx={chart.x(point.day)}
        cy={chart.y(point.pace)}
        r={3.5}
        fill="var(--color-warn)"
      />
      {labelDays(first.day, last.day).map((day) => (
        <text
          key={`label-${day}`}
          x={chart.x(day)}
          y={HEIGHT - 2}
          fontSize={10}
          textAnchor={anchor(day)}
          fill={day === first.day ? "var(--color-text)" : "var(--color-hint)"}
          fontWeight={day === first.day ? 700 : 400}
        >
          {day}
        </text>
      ))}
      {days.map((item) => (
        <rect
          key={`zone-${item.day}`}
          data-day={item.day}
          x={chart.x(item.day) - chart.slot / 2}
          y={0}
          width={chart.slot}
          height={HEIGHT}
          fill="transparent"
          onClick={() => onSelect(item.day)}
        />
      ))}
    </svg>
  );
}
```

- [ ] **Step 5: Тесты проходят**

Run: `cd webapp && npx vitest run src/forecast/chart.test.ts src/forecast/ForecastChart.test.tsx`
Expected: PASS (7 тестов).

- [ ] **Step 6: Полная проверка и коммит**

Run: `cd webapp && npx prettier --write src && npm run lint && npm test && npm run build`
Expected: всё зелёное.

```bash
git add webapp/src/forecast
git commit -m "feat(webapp): the month chart of the forecast

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Экран «Прогноз» и четвёртая вкладка

**Files:**
- Create: `webapp/src/forecast/WeekStrip.tsx`, `webapp/src/forecast/DayCard.tsx`, `webapp/src/forecast/ForecastScreen.tsx`
- Modify: `webapp/src/App.tsx`
- Test: `webapp/src/forecast/ForecastScreen.test.tsx`, `webapp/src/App.test.tsx`

**Interfaces:**
- Consumes: `api.forecast()`, `Forecast`, `ApiError` (Task 5, `api.ts`); всё из `view.ts` (Task 5); `ForecastChart` (Task 6); `capital`, `monthName` из `webapp/src/plan/months.ts`; `longDay` из `webapp/src/format.ts`.
- Produces: `ForecastScreen` (default, пропс `api: Api`); `WeekStrip` (`{data, view, start, selected, onSelect}`); `DayCard` (`{data, view, day}`); вкладка «Прогноз» в `App`.

- [ ] **Step 1: Падающие тесты**

`webapp/src/forecast/ForecastScreen.test.tsx`:

```tsx
import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type Forecast } from "../api";
import { forecastFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import ForecastScreen from "./ForecastScreen";
import { VIEW_KEY } from "./view";

function pill(day: number): HTMLElement {
  return screen.getByRole("button", { name: new RegExp(`^${day} октября`) });
}

describe("ForecastScreen", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("shows the week from today with the evening by plan", async () => {
    render(<ForecastScreen api={fakeApi()} />);
    expect(await screen.findByText("Октябрь, неделя")).toBeTruthy();
    const today = pill(15);
    expect(today.getAttribute("aria-pressed")).toBe("true");
    expect(within(today).getByText("сег")).toBeTruthy();
    expect(within(today).getByText("33,4")).toBeTruthy();
    expect(pill(21)).toBeTruthy();
    expect(screen.queryByRole("button", { name: /^22 октября/ })).toBeNull();
    expect(
      screen.getByText("Под датой — сколько будет вечером, млн сум (по плану)"),
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Предыдущая неделя" }).hasAttribute("disabled"),
    ).toBe(true);
  });

  it("shows the chosen day with its events and ordinary spending", async () => {
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi()} />);
    await screen.findByText("Октябрь, неделя");
    await user.click(pill(17));
    expect(screen.getByText("Суббота, 17 октября")).toBeTruthy();
    expect(screen.getByText("Детский центр")).toBeTruthy();
    expect(screen.getByText("−320 000 сум")).toBeTruthy();
    expect(
      screen.getByText("обычные траты ≈ 470 000 сум + $3 + 300 ₽ (по плану)"),
    ).toBeTruthy();
  });

  it("warns when the pace takes the sums below zero", async () => {
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi()} />);
    await screen.findByText("Октябрь, неделя");
    expect(screen.queryByText(/уйдут в минус/)).toBeNull();
    await user.click(screen.getByRole("radio", { name: "UZS" }));
    expect(
      screen.getByText("⚠️ По темпу сумы уйдут в минус 25.10, вернутся 30.10"),
    ).toBeTruthy();
    expect(within(pill(17)).getByText("7,6")).toBeTruthy();
  });

  it("shows the total in dollars", async () => {
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi()} />);
    await user.click(await screen.findByRole("radio", { name: "$" }));
    expect(
      screen.getByText("Под датой — сколько будет вечером, $ (по плану)"),
    ).toBeTruthy();
    expect(within(pill(15)).getByText("2 780")).toBeTruthy();
  });

  it("pages the weeks inside the month", async () => {
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi()} />);
    await screen.findByText("Октябрь, неделя");
    const next = screen.getByRole("button", { name: "Следующая неделя" });
    await user.click(next);
    expect(pill(22).getAttribute("aria-pressed")).toBe("true");
    await user.click(next);
    expect(pill(31)).toBeTruthy();
    expect(screen.getAllByRole("button", { name: /октября:/ })).toHaveLength(3);
    expect(next.hasAttribute("disabled")).toBe(true);
    expect(screen.getByText("Четверг, 29 октября")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Предыдущая неделя" }));
    expect(screen.getByText("Четверг, 22 октября")).toBeTruthy();
  });

  it("selects the day under a tap on the chart", async () => {
    const { container } = render(<ForecastScreen api={fakeApi()} />);
    await screen.findByText("Октябрь, неделя");
    const zone = container.querySelector('[data-day="25"]');
    if (!zone) throw new Error("no tap zone");
    fireEvent.click(zone);
    expect(screen.getByText("Воскресенье, 25 октября")).toBeTruthy();
    expect(pill(25).getAttribute("aria-pressed")).toBe("true");
  });

  it("remembers the chosen view on the phone", async () => {
    const user = userEvent.setup();
    const { unmount } = render(<ForecastScreen api={fakeApi()} />);
    await user.click(await screen.findByRole("radio", { name: "USD" }));
    expect(JSON.parse(localStorage.getItem(VIEW_KEY) ?? "null")).toEqual({
      scope: "USD",
      unit: "uzs",
    });
    unmount();
    render(<ForecastScreen api={fakeApi()} />);
    const usd = await screen.findByRole("radio", { name: "USD" });
    expect(usd.getAttribute("aria-checked")).toBe("true");
  });

  it("lists every currency now and at the month end", async () => {
    render(<ForecastScreen api={fakeApi()} />);
    expect(await screen.findByText("По валютам: сейчас → 31.10")).toBeTruthy();
    expect(screen.getByText("минус 25–29.10")).toBeTruthy();
    expect(screen.getByText("по темпу $1 215")).toBeTruthy();
  });

  it("shows a single day on the last day of the month", async () => {
    const lastDay: Forecast = {
      ...forecastFixture,
      today: "2026-10-31",
      currencies: forecastFixture.currencies.map((line) => ({
        ...line,
        events: [],
        days: line.days.filter((point) => point.day === 31),
        below_zero: { plan: null, pace: null },
      })),
      total: forecastFixture.total && {
        ...forecastFixture.total,
        uzs: forecastFixture.total.uzs.filter((point) => point.day === 31),
        usd: forecastFixture.total.usd.filter((point) => point.day === 31),
      },
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(lastDay);
    render(<ForecastScreen api={fakeApi({ forecast })} />);
    expect(await screen.findByText("Суббота, 31 октября")).toBeTruthy();
    expect(screen.getAllByRole("button", { name: /октября:/ })).toHaveLength(1);
    for (const name of ["Предыдущая неделя", "Следующая неделя"]) {
      expect(screen.getByRole("button", { name }).hasAttribute("disabled")).toBe(
        true,
      );
    }
  });

  it("marks a row that is late with ⏳", async () => {
    const late: Forecast = {
      ...forecastFixture,
      currencies: forecastFixture.currencies.map((line) =>
        line.code === "UZS"
          ? {
              ...line,
              events: [
                { day: 15, name: "Кружок", amount: -100_000, late: true },
                ...line.events,
              ],
            }
          : line,
      ),
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(late);
    render(<ForecastScreen api={fakeApi({ forecast })} />);
    expect(await screen.findByText("Кружок")).toBeTruthy();
    expect(screen.getByText("⏳")).toBeTruthy();
  });

  it("says when the month tab is missing", async () => {
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue({
      status: "no_month_tab",
      month: "Nov 26",
      today: "2026-11-01",
      end: null,
      currencies: [],
      total: null,
    });
    render(<ForecastScreen api={fakeApi({ forecast })} />);
    expect(
      await screen.findByText(
        "Нет вкладки месяца — прогноз появится, когда она будет",
      ),
    ).toBeTruthy();
  });

  it("offers a retry when the sheet does not answer", async () => {
    const forecast = vi
      .fn<Api["forecast"]>()
      .mockRejectedValueOnce(
        new ApiError(
          503,
          "sheets_unavailable",
          "Таблица не ответила — попробуйте ещё раз",
        ),
      )
      .mockResolvedValueOnce(forecastFixture);
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi({ forecast })} />);
    expect(
      await screen.findByText("Таблица не ответила — попробуйте ещё раз"),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("Октябрь, неделя")).toBeTruthy();
  });
});
```

В `webapp/src/App.test.tsx` после теста `switches to the dashboard`:

```tsx
  it("switches to the forecast", async () => {
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    await user.click(screen.getByRole("tab", { name: "Прогноз" }));
    expect(await screen.findByText("Октябрь, неделя")).toBeTruthy();
  });
```

- [ ] **Step 2: Тесты падают**

Run: `cd webapp && npx vitest run src/forecast/ForecastScreen.test.tsx src/App.test.tsx`
Expected: FAIL — `Failed to resolve import "./ForecastScreen"`; в `App` нет вкладки «Прогноз».

- [ ] **Step 3: Лента недели**

`webapp/src/forecast/WeekStrip.tsx`:

```tsx
import type { Forecast } from "../api";
import { longDay } from "../format";
import { capital, monthName } from "../plan/months";
import {
  dayEvents,
  dayOf,
  isoDay,
  pillUnit,
  pillValue,
  viewCode,
  viewDays,
  weekdayShort,
  type View,
} from "./view";

interface Props {
  data: Forecast;
  view: View;
  start: number; // первый день недели ленты
  selected: number;
  onSelect: (day: number) => void;
}

/** «Октябрь, неделя ‹ ›» и до семи плашек: остаток на вечер по плану и точки событий. */
export default function WeekStrip({
  data,
  view,
  start,
  selected,
  onSelect,
}: Props) {
  const today = dayOf(data.today);
  const end = data.end ? dayOf(data.end) : today;
  const code = viewCode(view);
  const days = viewDays(data, view).filter(
    (point) => point.day >= start && point.day < start + 7,
  );
  return (
    <section className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <h2 className="font-semibold">
          {`${capital(monthName(data.today.slice(0, 7)))}, неделя`}
        </h2>
        <div className="flex gap-1">
          <button
            type="button"
            aria-label="Предыдущая неделя"
            className="px-3 text-xl leading-none text-link disabled:opacity-30"
            disabled={start <= today}
            onClick={() => onSelect(start - 7)}
          >
            ‹
          </button>
          <button
            type="button"
            aria-label="Следующая неделя"
            className="px-3 text-xl leading-none text-link disabled:opacity-30"
            disabled={start + 7 > end}
            onClick={() => onSelect(start + 7)}
          >
            ›
          </button>
        </div>
      </div>
      <div className="grid grid-cols-7 gap-1">
        {days.map((point) => {
          const iso = isoDay(data.today, point.day);
          const on = point.day === selected;
          return (
            <button
              key={point.day}
              type="button"
              aria-pressed={on}
              aria-label={`${longDay(iso)}: вечером ${pillValue(point.plan, code)} ${pillUnit(code)}`}
              className={`flex flex-col items-center rounded-xl px-0.5 py-2 ${
                on ? "bg-button text-button-text" : "bg-section"
              }`}
              onClick={() => onSelect(point.day)}
            >
              <span className={`text-[11px] ${on ? "opacity-80" : "text-hint"}`}>
                {point.day === today ? "сег" : weekdayShort(iso)}
              </span>
              <span className="text-lg leading-tight font-semibold">
                {point.day}
              </span>
              <span
                className={`text-[11px] font-semibold ${
                  point.plan < 0 && !on ? "text-danger" : ""
                }`}
              >
                {pillValue(point.plan, code)}
              </span>
              <span className="mt-0.5 flex h-1.5 gap-0.5">
                {dayEvents(data, view, point.day).map((event, index) => (
                  <i
                    key={`${event.currency}-${event.name}-${index}`}
                    className={`inline-block size-1.5 rounded-full ${
                      event.amount > 0 ? "bg-ok" : "bg-over"
                    }`}
                  />
                ))}
              </span>
            </button>
          );
        })}
      </div>
      <p className="text-xs text-hint">
        {`Под датой — сколько будет вечером, ${pillUnit(code)} (по плану)`}
      </p>
    </section>
  );
}
```

- [ ] **Step 4: Карточка дня**

`webapp/src/forecast/DayCard.tsx`:

```tsx
import type { Forecast } from "../api";
import {
  amount,
  dayEvents,
  dayTitle,
  isoDay,
  ordinary,
  signed,
  viewCode,
  viewDays,
  type View,
} from "./view";

interface Props {
  data: Forecast;
  view: View;
  day: number;
}

/** Выбранный день: остаток на вечер по плану и по темпу, события дня, обычные траты. */
export default function DayCard({ data, view, day }: Props) {
  const point = viewDays(data, view).find((item) => item.day === day);
  if (!point) return null;
  const code = viewCode(view);
  const usual = ordinary(data, view, day);
  return (
    <section className="flex flex-col gap-1 rounded-2xl bg-section p-4">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">{dayTitle(isoDay(data.today, day))}</h2>
        <span className="text-xs text-hint">вечером</span>
      </div>
      <p className="flex justify-between gap-2">
        <span>
          <span className="text-link">━</span> по плану
        </span>
        <span className={`font-semibold ${point.plan < 0 ? "text-danger" : ""}`}>
          {amount(point.plan, code)}
        </span>
      </p>
      <p className="flex justify-between gap-2">
        <span>
          <span className="text-warn">╍</span> по темпу
        </span>
        <span className={`font-semibold ${point.pace < 0 ? "text-danger" : ""}`}>
          {amount(point.pace, code)}
        </span>
      </p>
      <ul className="mt-1 flex flex-col gap-0.5 text-sm">
        {dayEvents(data, view, day).map((event, index) => (
          <li
            key={`${event.currency}-${event.name}-${index}`}
            className="flex justify-between gap-2"
          >
            <span className="flex gap-1">
              {event.late && <span>⏳</span>}
              <span className={event.amount > 0 ? "text-ok" : "text-over"}>
                {event.amount > 0 ? "▲" : "▼"}
              </span>
              <span>{event.name}</span>
            </span>
            <span className={event.amount > 0 ? "text-ok" : ""}>
              {signed(event.amount, event.currency)}
            </span>
          </li>
        ))}
      </ul>
      {usual && (
        <p className="text-xs text-hint">{`обычные траты ${usual} (по плану)`}</p>
      )}
    </section>
  );
}
```

- [ ] **Step 5: Экран**

`webapp/src/forecast/ForecastScreen.tsx`:

```tsx
import { useCallback, useEffect, useState } from "react";
import { ApiError, type Api, type Forecast } from "../api";
import DayCard from "./DayCard";
import ForecastChart from "./ForecastChart";
import {
  DEFAULT_VIEW,
  TOTAL,
  amount,
  chartNote,
  currencyRows,
  dayOf,
  hasDollars,
  loadView,
  marks,
  perDay,
  scopes,
  shortDate,
  storeView,
  viewCode,
  viewDays,
  warnings,
  weekOf,
  type View,
} from "./view";
import WeekStrip from "./WeekStrip";

const EXPLAIN =
  "План — остаток плана статей, разложенный по дням. Темп — средние траты за 14 дней без переводов и платежей из списка.";

function Skeleton() {
  return (
    <div className="flex flex-col gap-3" aria-label="Загрузка">
      {[0, 1, 2].map((key) => (
        <div key={key} className="h-20 animate-pulse rounded-2xl bg-section" />
      ))}
    </div>
  );
}

interface SwitchesProps {
  data: Forecast;
  view: View;
  onView: (view: View) => void;
}

/** «Всего · UZS · USD · RUB» и для «Всего» — «сум | $». */
function Switches({ data, view, onView }: SwitchesProps) {
  const button = (on: boolean) =>
    `rounded-lg px-2 py-1 text-sm font-medium ${on ? "bg-bg shadow-sm" : "text-hint"}`;
  return (
    <div className="flex gap-2">
      <div
        role="radiogroup"
        aria-label="Что показать"
        className="grid flex-1 auto-cols-fr grid-flow-col gap-1 rounded-xl bg-section p-1"
      >
        {scopes(data).map((scope) => (
          <button
            key={scope}
            type="button"
            role="radio"
            aria-checked={view.scope === scope}
            className={button(view.scope === scope)}
            onClick={() => onView({ ...view, scope })}
          >
            {scope === TOTAL ? "Всего" : scope}
          </button>
        ))}
      </div>
      {view.scope === TOTAL && hasDollars(data) && (
        <div
          role="radiogroup"
          aria-label="Единица"
          className="grid grid-cols-2 gap-1 rounded-xl bg-section p-1"
        >
          {(["uzs", "usd"] as const).map((unit) => (
            <button
              key={unit}
              type="button"
              role="radio"
              aria-checked={view.unit === unit}
              className={button(view.unit === unit)}
              onClick={() => onView({ ...view, unit })}
            >
              {unit === "uzs" ? "сум" : "$"}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

interface MonthProps {
  data: Forecast;
  view: View;
  selected: number | null;
  onView: (view: View) => void;
  onSelect: (day: number) => void;
}

function Month({ data, view, selected, onView, onSelect }: MonthProps) {
  const today = dayOf(data.today);
  const end = data.end ? dayOf(data.end) : today;
  const day =
    selected !== null && selected >= today && selected <= end ? selected : today;
  const code = viewCode(view);
  const days = viewDays(data, view);
  const last = days[days.length - 1];
  const ends = last
    ? `по плану ${amount(last.plan, code)}, по темпу ${amount(last.pace, code)}`
    : "";
  return (
    <>
      <Switches data={data} view={view} onView={onView} />
      {warnings(data, view).map((text) => (
        <p key={text} className="text-sm font-medium text-danger">
          {text}
        </p>
      ))}
      <WeekStrip
        data={data}
        view={view}
        start={weekOf(day, today)}
        selected={day}
        onSelect={onSelect}
      />
      <DayCard data={data} view={view} day={day} />
      <section className="flex flex-col gap-2 rounded-2xl bg-section p-4">
        <div className="flex items-baseline justify-between gap-2">
          <h2 className="font-semibold">До конца месяца</h2>
          <span className="text-xs text-hint">{chartNote(view)}</span>
        </div>
        <ForecastChart
          days={days}
          marks={marks(data, view)}
          selected={day}
          label={`До конца месяца: ${ends}`}
          onSelect={onSelect}
        />
        {last && (
          <p className="flex flex-wrap gap-x-3 text-xs text-hint">
            <span>
              <span className="text-link">━</span>
              {` по плану ≈ ${amount(last.plan, code)}`}
            </span>
            <span>
              <span className="text-warn">╍</span>
              {` по темпу ≈ ${amount(last.pace, code)}`}
            </span>
          </p>
        )}
      </section>
      <section className="flex flex-col gap-1">
        <h2 className="font-semibold">
          {`По валютам: сейчас → ${shortDate(end, data.today)}`}
        </h2>
        <ul className="flex flex-col gap-1 text-sm">
          {currencyRows(data).map((row) => (
            <li
              key={row.code}
              className="grid grid-cols-[2.5rem_1fr_auto] items-baseline gap-2"
            >
              <span className="text-hint">{row.code}</span>
              <span>
                {row.now}{" "}
                <span className="whitespace-nowrap">
                  → <b>{row.end}</b>
                </span>
                {!row.counted && (
                  <span className="text-xs text-hint">
                    {" "}
                    · нет курса, не во «Всего»
                  </span>
                )}
              </span>
              <span
                className={`text-xs ${row.minus ? "text-danger" : "text-hint"}`}
              >
                {row.pace}
              </span>
            </li>
          ))}
        </ul>
      </section>
      <p className="text-xs text-hint">{`${EXPLAIN} ${perDay(data, view)}`}</p>
    </>
  );
}

export default function ForecastScreen({ api }: { api: Api }) {
  const [data, setData] = useState<Forecast | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [version, setVersion] = useState(0);
  const [view, setView] = useState<View>(DEFAULT_VIEW);
  const [selected, setSelected] = useState<number | null>(null);

  useEffect(() => {
    let alive = true;
    api.forecast().then(
      (result) => {
        if (!alive) return;
        setData(result);
        setView(loadView(result));
        setError(null);
        setLoading(false);
      },
      (reason: unknown) => {
        if (!alive) return;
        setError(
          reason instanceof ApiError
            ? reason.message
            : "Не получилось загрузить прогноз",
        );
        setLoading(false);
      },
    );
    return () => {
      alive = false;
    };
  }, [api, version]);

  const refresh = useCallback(() => {
    setLoading(true);
    setVersion((current) => current + 1);
  }, []);
  const changeView = useCallback((next: View) => {
    setView(next);
    storeView(next);
  }, []);

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">
          {data ? `Прогноз · ${data.month}` : "Прогноз"}
        </h1>
        <button
          type="button"
          className="text-link disabled:opacity-50"
          disabled={loading}
          onClick={refresh}
        >
          Обновить
        </button>
      </div>
      {loading && !data && <Skeleton />}
      {error && (
        <div className="flex flex-col items-start gap-2" role="alert">
          <p>{error}</p>
          <button type="button" className="text-link" onClick={refresh}>
            Повторить
          </button>
        </div>
      )}
      {data?.status === "no_month_tab" && (
        <p>Нет вкладки месяца — прогноз появится, когда она будет</p>
      )}
      {data?.status === "ok" && (
        <Month
          data={data}
          view={view}
          selected={selected}
          onView={changeView}
          onSelect={setSelected}
        />
      )}
    </div>
  );
}
```

- [ ] **Step 6: Четвёртая вкладка**

В `webapp/src/App.tsx`: импорт `import ForecastScreen from "./forecast/ForecastScreen";`; тип и список вкладок:

```tsx
type Tab = "entry" | "dashboard" | "plan" | "forecast";

const TABS: { name: Tab; title: string }[] = [
  { name: "entry", title: "Ввод" },
  { name: "dashboard", title: "Сводка" },
  { name: "plan", title: "План" },
  { name: "forecast", title: "Прогноз" },
];
```

в `nav` класс `grid grid-cols-3 …` заменить на `grid grid-cols-4 gap-1 rounded-xl bg-section p-1`; после блока вкладки «План»:

```tsx
      {tab === "forecast" && <ForecastScreen api={api} />}
```

- [ ] **Step 7: Тесты проходят**

Run: `cd webapp && npx vitest run src/forecast src/App.test.tsx`
Expected: PASS (все тесты `src/forecast`, 12 — у `ForecastScreen`; 8 — у `App`).

- [ ] **Step 8: Полная проверка, просмотр и коммит**

Run: `cd webapp && npx prettier --write src && npm run lint && npm test && npm run build`
Expected: всё зелёное.

Просмотр в режиме разработки: `cd webapp && npm run dev`, открыть `/app/` в ширине телефона (390 px), вкладка «Прогноз»: лента без горизонтальной прокрутки, плашки читаются, график с красной частью в «UZS», переключатель «сум | $» есть только у «Всего».

```bash
git add webapp/src/forecast webapp/src/App.tsx webapp/src/App.test.tsx
git commit -m "feat(webapp): the «Прогноз» tab — week strip, day card, month chart

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Документация, PR, выкатка и живая проверка

**Files:**
- Modify: `README.md` (пункт Mini App, абзац про «💳 Сумы»)
- Modify: `CLAUDE.md` репозитория (дерево `app/`, потоки данных)

**Interfaces:**
- Consumes: всё из Tasks 1–7.
- Produces: PR в `main`, деплой на Render, сверка прогноза с вкладкой месяца.

- [ ] **Step 1: README**

В конце пункта `*   **Mini App:** …` дописать:

```markdown
«Прогноз» — how much money there will be on every evening until the month ends, for each currency of the cards and in total (in sums or dollars at the `system` rates): a week strip from today with the evening balance by plan, the chosen day's card (by plan and by pace, the yellow-list rows of that day, the ordinary spending), a month chart with two lines — by plan (the rest of the month's articles spread over the days left, list payments on their days) and by pace (the average spending of the 14 days before today without transfers and list payments) — and a warning when a line goes below zero.
```

В абзаце `The message (and «Сводка» in the Mini App) also says how long the sums last: …` предложение `Sums are all UZS sources (cards and cash) at the end of yesterday; the pace is the average spending in sums over the last 14 days (incomes and transfers between own cards excluded, refunds reduce it); day by day to the end of the month the pace goes out and UZS lines of the yellow list (advance, salary) come in.` заменить на:

```markdown
It is the «по темпу» line in sums of the «Прогноз» tab: sums are all UZS sources (cards and cash) now, with today's rows; the pace is the average spending in sums over the 14 days before today (incomes, transfers between own cards and payments of the yellow list excluded, refunds reduce it); today only what is left of the day's pace goes out, then the pace every day, and UZS lines of the yellow list that have not come yet (advance, salary) come in on their day, late ones today.
```

- [ ] **Step 2: CLAUDE.md репозитория**

В дереве: строку `api.py` дополнить `, /api/forecast`; после `dashboard.py` добавить

```
│   ├── forecast.py      # Mini App «Прогноз»: money until the month end per currency and in total, plan and pace lines, list rows as events
│   ├── runway.py        # «💳 Сумы» card: the forecast's pace line in sums → last day, gap, dollars to exchange
```

после абзаца **Mini App dashboard** — новый:

```markdown
**Mini App forecast**: `GET /api/forecast` → `AnalyticsService.forecast(today, card currencies)` → `month_forecast` (`fact` with `keep_unknown`, `read_plan(...).articles`, `read_forecast(...).items`; per currency `currency_forecast`: money now, plan per day by the morning reconciliation, pace of 14 days without list payments, events by the reconciliation now; total at today's rates) → `api.forecast_out`
```

- [ ] **Step 3: Полная проверка, коммит, PR**

Run: `black --check . && ruff check . && pytest -q` и `cd webapp && npm run lint && npm test && npm run build`
Expected: всё зелёное.

```bash
git add README.md CLAUDE.md
git commit -m "docs: the forecast tab and the sums card on the forecast

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/forecast-tab
gh pr create --base main --head feat/forecast-tab --title "feat: the «Прогноз» tab — money until the month end" --body-file - <<'BODY'
Вкладка «Прогноз» в Mini App (спека `docs/superpowers/specs/2026-10-15-forecast-tab-design.md`, план `docs/superpowers/plans/2026-10-15-forecast-tab.md`).

## Что делает
- `app/services/forecast.py`: деньги сейчас по каждой валюте карт, план в день (остаток плана статей без неоплаченных платежей списка), темп за 14 дней без переводов и платежей списка, строки списка как события; остаток на вечер каждого дня до конца месяца и «Всего» в сумах и долларах.
- `GET /api/forecast`.
- Карточка «💳 Сумы — хватит до…» в «Сводке» и утреннем сообщении считается той же линией «по темпу»: даты на двух вкладках совпадают.
- Приложение: четвёртая вкладка — «Всего · UZS · USD · RUB», «сум | $», лента недели, карточка дня, график месяца, валюты «сейчас → конец месяца».

## Проверка
- `black --check . && ruff check . && pytest -q`
- `npm run lint && npm test && npm run build` в `webapp/`
- после выкатки: «сейчас» во «Всего» совпадает с P2 вкладки месяца, «хватит до» карточки — день перед уходом сумов в минус по темпу

🤖 Generated with [Claude Code](https://claude.com/claude-code)
BODY
```

После `gh pr create` — `ccd_pr get_status` / `bind_pr` с адресом PR.

- [ ] **Step 4: CI, merge, деплой**

После зелёного CI: `gh pr merge --merge --delete-branch`. Render деплоит `main` сам: новый бандл на `/app/`, `/health` — 200, в логах после старта нет ошибок, `GET /api/forecast` при открытии вкладки — 200.

- [ ] **Step 5: Сверка с таблицей**

ID таблицы — из заметок владельца (`SHEET_ID`), в репозиторий и в PR не пишется; `TITLE` — вкладка текущего месяца. Выгрузки и скрипт — в папке над репозиторием, не в нём:

```bash
for range in "fact!A1:H" "'$TITLE'!A1:T60" "system!H2:I10"; do
  name=$(echo "$range" | tr -cd 'a-zA-Z0-9')
  gws sheets spreadsheets values get --params '{"spreadsheetId":"'"$SHEET_ID"'","range":"'"$range"'","valueRenderOption":"UNFORMATTED_VALUE"}' > "../live-$name.json"
done
```

Окружение — фиктивное, как в `tests/conftest.py`: `app.config` без него не импортируется, а скрипт в сеть не ходит.

```bash
TELEGRAM_TOKEN=123456:TEST SPREADSHEET_ID=live-check \
GOOGLE_SERVICE_ACCOUNT_EMAIL=live@check.invalid GOOGLE_PRIVATE_KEY=none \
../venv_dev/bin/python - "$TITLE" <<'EOF'
import json
import sys
from pathlib import Path

from app import config
from app.domain import Rates, local_today
from app.services.analytics_service import transactions_frame
from app.services.day_budget import day_budget, read_forecast
from app.services.forecast import month_forecast
from app.services.plan import read_plan

title = sys.argv[1]
def load(range_: str) -> list:
    name = "".join(c for c in range_ if c.isalnum())
    return json.loads(Path(f"../live-{name}.json").read_text())["values"]

fact, tab, pairs = load("fact!A1:H"), load(f"'{title}'!A1:T60"), load("system!H2:I10")
rates = Rates({"UZS": 1.0, **{str(c).upper(): float(r) for c, r in pairs if isinstance(r, int | float)}})
today = local_today(config.ANALYTICS_TIMEZONE)
listed = read_forecast(tab)
found = month_forecast(
    transactions_frame(fact, rates, keep_unknown=True),
    read_plan(tab).articles,
    listed.items if listed else (),
    today,
    rates,
    ["UZS", "USD", "RUB"],
)
now = sum(line.now * rates.to_uzs[line.code] for line in found.currencies if line.code in found.counted)
print(f"сейчас всего {now:,.0f} / P2 {tab[1][15]:,.0f} / разница {now - tab[1][15]:,.2f}")
budget = day_budget(transactions_frame(fact, rates), tab, today, rates, "USD")
dip = found.currencies[0].pace_dip
print("хватит до:", budget.runway.last_day if budget and budget.runway else None, "| минус по темпу с:", dip)
EOF
```

Expected: разница «сейчас всего» и P2 — меньше 1 сума; «хватит до» — день перед `Dip.start` линии сумов (или оба пусты). Числа не публикуются никуда, кроме ответа владельцу.

- [ ] **Step 6: Владельцу**

Попросить открыть «Прогноз» на телефоне: лента, карточка дня, «UZS» с предупреждением (если сумы уходят в минус), «сум | $», и что «хватит до» в «Сводке» совпадает с прогнозом.
