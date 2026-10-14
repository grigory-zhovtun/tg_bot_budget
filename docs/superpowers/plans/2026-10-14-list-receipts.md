# Сверка списка «Поступления и платежи» с фактом — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Строки жёлтого списка вкладки месяца считаются «впереди», пока деньги по ним реально не пришли (или платёж не ушёл), а не по дате: бот сверяет их с листом `fact`, пишет «Пришло к утру» в колонку T, а лимит, «Заморожено», прогноз по сумам и «Впереди» — в боте и в формулах вкладки — считают по этой сверке.

**Architecture:** Новый чистый модуль `app/services/receipts.py` сверяет строки списка с `fact` (`match_receipts`) и отвечает, что по строке ещё впереди (`remaining`) и какой у неё статус. `day_budget.py` считает по нему лимит, траты дня, заморозку, прогноз по сумам и «Впереди». `app/services/receipts_sync.py` пишет колонку «Пришло к утру» во вкладку текущего месяца. `app/handlers/receipts.py` запускает сверку ночью и после изменений. API «Плана» отдаёт статус строки, приложение показывает подпись. Формулы P5 и Q10 вкладки меняются один раз после выкатки.

**Tech Stack:** Python 3.12, python-telegram-bot 22.8, Starlette 1.7, Pydantic 2, gspread, pandas; React 19.3, TypeScript 6.0 (strict), Vite 8.3, Tailwind 4.3, Vitest 5.0, Testing Library.

**Spec:** `docs/superpowers/specs/2026-10-14-list-receipts-design.md`

## Global Constraints

- Репозиторий публичный: в коде, тестах, документах — только условные суммы и названия; ID таблицы, реальные суммы и имена — никогда.
- Секреты не читаем и не вводим: `.env` не открываем.
- Python: type hints на всех сигнатурах, black + ruff; перед коммитом `ruff check . && black --check . && pytest -q` (venv проекта).
- Фронтенд: функциональные компоненты и хуки, TypeScript strict, без `any` и `!`; перед коммитом в `webapp/`: `npm run lint && npm test && npm run build`.
- Правило сверки (спека, раздел 4): окно — с дня строки − 5 дней (не раньше 1-го) до последнего дня месяца; поступление — доходы (`💰 ДОХОДЫ` без «нач остаток») валюты строки, части складываются, доход — строке с ближайшим днём (при равенстве — более ранней); платёж — одна трата (без доходов и `💳 СЧЕТА`) валюты строки на `|сумма| ± 10 %`, ближайшая по сумме, затем по дате, одна трата — одной строке; строка закрыта при `|пришло| ≥ 90 % |сумма|`, иначе впереди `сумма − пришло`.
- «Пришло к утру» (колонка T, заголовок «Пришло к утру» в строке заголовка списка): по операциям с датой по вчерашний день включительно, в валюте строки и с её знаком, `RAW`; пишет только бот, только изменившиеся ячейки текущей вкладки.
- Лимит, заморозка, прогноз по сумам — по сверке к утру; «Впереди», траты дня и статусы на экране «План» — по сверке на сейчас (с сегодняшними операциями).
- Плановые расчёты (P3, P6, P8, план таблицы дней, плановый остаток в боте) — по датам, без изменений.
- Формулы вкладки и код бота меняются вместе: P5 и Q10 — точно как в разделе 6 спеки.
- Коммиты — conventional commits, в конце `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; тело PR заканчивается `🤖 Generated with [Claude Code](https://claude.com/claude-code)`; мерж — `gh pr merge --merge --delete-branch`.

## Review Focus

1. **Деньги пришли сегодня** — лимит «с утра» не меняется до завтра ни в боте, ни во вкладке; «Впереди» и экран «План» уже показывают, что пришло (тесты в Task 2 и Task 3).
2. **Платёж заплачен раньше своего дня или на сумму чуть отличную от плана** — день не краснеет, «Заморожено» не уходит в минус (тесты в Task 2).
3. **Две строки в одной валюте** (аванс и ЗП в сумах): одна пришла раньше, другая опоздала — каждая получает своё (тест в Task 1).
4. **Первое число**: в копии вкладки стоят отметки прошлого месяца — ночная сверка их очищает (тест в Task 3).
5. **Строку подняли после того, как деньги по ней пришли** — строка снова открыта, впереди недостающая часть (тест в Task 2).

---

### Task 1: Сверка строк списка с fact

**Files:**
- Create: `app/services/receipts.py`
- Test: `tests/test_receipts.py`

**Interfaces:**
- Consumes: `expenses`, `income` из `app/services/analytics_service.py` (траты без доходов и `💳 СЧЕТА`; доходы без «нач остаток»); фрейм `transactions_frame` с колонками `day` (date), `group`, `subcategory`, `amount`, `currency`, `amount_uzs`.
- Produces: `EARLY_DAYS = 5`, `DONE_SHARE = 0.9`, `PAYMENT_TOLERANCE = 0.1`; протокол `ListLine` (`row: int`, `day: int`, `amount: float`, `currency: str`); `Line(row, day, amount, currency)`; `Receipt(received: float = 0.0, on: date | None = None, operations: frozenset[int] = frozenset())`; `NOTHING = Receipt()`; `match_receipts(frame, lines: Sequence[ListLine], first: date, through: date) -> dict[int, Receipt]` (ключ — строка вкладки); `closed(line, receipt) -> bool`; `remaining(line, receipt) -> float`; `receipt_status(line, receipt, today) -> str` (`done` | `partial` | `waiting` | `future`).

- [ ] **Step 1: Ветка**

```bash
git checkout main && git pull && git checkout -b feat/list-receipts
```

- [ ] **Step 2: Падающие тесты**

`tests/test_receipts.py`:

```python
"""Сверка жёлтого списка с fact: что пришло и что ушло по строкам."""

from datetime import date
from typing import Any

from app.domain import Rates
from app.services.analytics_service import transactions_frame
from app.services.receipts import (
    NOTHING,
    Line,
    Receipt,
    match_receipts,
    receipt_status,
    remaining,
)
from tests.test_day_budget import FACT_HEADER, fact_row

RATES = Rates({"UZS": 1.0, "USD": 12_000.0, "RUB": 150.0})
OCTOBER = date(2026, 10, 1)
ADVANCE = Line(12, 15, 2_000_000, "UZS")
SALARY = Line(13, 30, 10_000_000, "UZS")
RUB_SALARY = Line(14, 28, 100_000, "RUB")
RENT = Line(15, 18, -650, "USD")
LINES = [ADVANCE, SALARY, RUB_SALARY, RENT]


def income(day: int, amount: float, cur: str = "UZS", sub: str = "зарплата") -> Any:
    return fact_row(date(2026, 10, day), "💰 ДОХОДЫ", sub, amount, cur)


def rent(day: int, amount: float) -> Any:
    return fact_row(date(2026, 10, day), "🏚️ ДОМ", "квартплата", amount, "USD")


def received(*rows: Any, through: int = 31) -> dict[int, Receipt]:
    frame = transactions_frame([FACT_HEADER, *rows], RATES)
    return match_receipts(frame, LINES, OCTOBER, date(2026, 10, through))


def test_a_salary_paid_in_parts_adds_up() -> None:
    found = received(income(28, 60_000, "RUB"), income(29, 40_000, "RUB"))
    assert found[RUB_SALARY.row].received == 100_000
    assert found[RUB_SALARY.row].on == date(2026, 10, 29)
    assert remaining(RUB_SALARY, found[RUB_SALARY.row]) == 0


def test_an_income_goes_to_the_row_with_the_nearest_day() -> None:
    found = received(income(29, 9_900_000), income(17, 2_000_000))
    assert found[SALARY.row].received == 9_900_000  # на день раньше 30-го
    assert found[ADVANCE.row].received == 2_000_000  # аванс опоздал на два дня


def test_the_window_opens_five_days_before_the_row() -> None:
    assert ADVANCE.row not in received(income(9, 2_000_000))
    assert received(income(10, 2_000_000))[ADVANCE.row].received == 2_000_000


def test_operations_after_through_do_not_count_yet() -> None:
    assert ADVANCE.row not in received(income(16, 2_000_000), through=15)
    assert ADVANCE.row in received(income(16, 2_000_000), through=16)


def test_ninety_percent_closes_a_row_and_less_leaves_the_rest_ahead() -> None:
    almost = received(income(15, 1_800_000))[ADVANCE.row]
    assert remaining(ADVANCE, almost) == 0
    part = received(income(15, 1_700_000))[ADVANCE.row]
    assert remaining(ADVANCE, part) == 300_000
    assert receipt_status(ADVANCE, part, date(2026, 10, 20)) == "partial"


def test_a_payment_is_one_spending_close_to_its_sum() -> None:
    paid = received(rent(17, 20), rent(17, 640), rent(18, 500))[RENT.row]
    assert (paid.received, paid.on, len(paid.operations)) == (
        -640,
        date(2026, 10, 17),
        1,
    )
    assert remaining(RENT, paid) == 0


def test_small_purchases_do_not_close_a_payment() -> None:
    assert RENT.row not in received(rent(18, 20), rent(19, 500))


def test_one_spending_closes_one_payment() -> None:
    twice = [RENT, Line(16, 20, -650, "USD")]
    frame = transactions_frame([FACT_HEADER, rent(18, 650)], RATES)
    found = match_receipts(frame, twice, OCTOBER, date(2026, 10, 31))
    assert list(found) == [RENT.row]  # первой по дню


def test_transfers_refunds_and_the_opening_balance_do_not_count() -> None:
    rows = [
        fact_row(date(2026, 10, 18), "💳 СЧЕТА", "переводы", 650, "USD"),
        rent(18, -650),  # возврат
        income(15, 2_000_000, sub="нач остаток"),
    ]
    assert received(*rows) == {}


def test_the_status_follows_the_day_and_what_came() -> None:
    assert receipt_status(ADVANCE, NOTHING, date(2026, 10, 15)) == "future"
    assert receipt_status(ADVANCE, NOTHING, date(2026, 10, 16)) == "waiting"
    paid = received(income(15, 2_000_000))[ADVANCE.row]
    assert receipt_status(ADVANCE, paid, date(2026, 10, 15)) == "done"
```

- [ ] **Step 3: Тесты падают**

Run: `pytest tests/test_receipts.py -q`
Expected: ошибка импорта — `No module named 'app.services.receipts'`.

- [ ] **Step 4: Реализация**

`app/services/receipts.py`:

```python
"""Сверка жёлтого списка вкладки месяца с листом fact: что пришло и что ушло.

Строка списка — поступление (+) или крупный платёж (−): день, сумма со знаком,
валюта. Поступление набирается из доходов той же валюты с дня строки − EARLY_DAYS
до конца месяца: части складываются, доход достаётся строке с ближайшим днём.
Платёж закрывает одна трата той же валюты на сумму плана ± PAYMENT_TOLERANCE.
Строка закрыта, когда пришло DONE_SHARE плана; иначе впереди — недостающая часть.
"""

import calendar
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol

import pandas as pd

from app.services.analytics_service import expenses, income

EARLY_DAYS = 5  # поступление может прийти на несколько дней раньше дня в списке
DONE_SHARE = 0.9  # строка закрыта, когда пришло 90 % плана
PAYMENT_TOLERANCE = 0.1  # платёж — одна трата на сумму плана ± 10 %


class ListLine(Protocol):
    """Строка списка: строка вкладки, день месяца, сумма со знаком, валюта."""

    row: int
    day: int
    amount: float
    currency: str


@dataclass(frozen=True)
class Line:
    row: int
    day: int
    amount: float
    currency: str


@dataclass(frozen=True)
class Receipt:
    """Что пришло по строке — со знаком строки, в её валюте."""

    received: float = 0.0
    on: date | None = None  # дата последней операции
    operations: frozenset[int] = frozenset()  # индексы строк fact во фрейме


NOTHING = Receipt()


def _due(first: date, day: int) -> date:
    """День строки в месяце first; 31-е в коротком месяце — его последний день."""
    last = calendar.monthrange(first.year, first.month)[1]
    return first.replace(day=min(max(day, 1), last))


def _opens(first: date, line: ListLine) -> date:
    return max(_due(first, line.day) - timedelta(days=EARLY_DAYS), first)


def match_receipts(
    frame: pd.DataFrame, lines: Sequence[ListLine], first: date, through: date
) -> dict[int, Receipt]:
    """Что пришло по строкам списка месяца first — по операциям с first по through.

    Ключ — строка вкладки; строк, по которым ничего не пришло, в ответе нет.
    """
    month = frame[(frame["day"] >= first) & (frame["day"] <= through)]
    found: dict[int, Receipt] = {}

    incomes = [line for line in lines if line.amount > 0]
    got = income(month)
    got = got[got["amount"] > 0]
    for index, day, amount, currency in zip(
        got.index, got["day"], got["amount"], got["currency"], strict=True
    ):
        fits = [
            line
            for line in incomes
            if line.currency == currency and _opens(first, line) <= day
        ]
        if not fits:
            continue
        line = min(
            fits,
            key=lambda item: (abs((day - _due(first, item.day)).days), item.day),
        )
        old = found.get(line.row, NOTHING)
        found[line.row] = Receipt(
            old.received + float(amount),
            day if old.on is None else max(old.on, day),
            old.operations | {int(index)},
        )

    spent = expenses(month)
    spent = spent[spent["amount"] > 0]
    used: set[int] = set()
    payments = sorted(
        (line for line in lines if line.amount < 0), key=lambda item: item.day
    )
    for line in payments:
        due = -line.amount
        best: tuple[tuple[float, int], int, date, float] | None = None
        for index, day, amount, currency in zip(
            spent.index, spent["day"], spent["amount"], spent["currency"], strict=True
        ):
            number = int(index)
            if currency != line.currency or day < _opens(first, line):
                continue
            if number in used or abs(float(amount) - due) > PAYMENT_TOLERANCE * due:
                continue
            key = (abs(float(amount) - due), abs((day - _due(first, line.day)).days))
            if best is None or key < best[0]:
                best = (key, number, day, float(amount))
        if best is not None:
            _, number, day, amount = best
            used.add(number)
            found[line.row] = Receipt(-amount, day, frozenset({number}))
    return found


def closed(line: ListLine, receipt: Receipt) -> bool:
    """Пришло DONE_SHARE плана — строки впереди больше нет."""
    return line.amount != 0 and abs(receipt.received) >= DONE_SHARE * abs(line.amount)


def remaining(line: ListLine, receipt: Receipt) -> float:
    """Что ещё впереди по строке, в её валюте; 0 — строка закрыта."""
    if line.amount == 0 or closed(line, receipt):
        return 0.0
    return line.amount - receipt.received


def receipt_status(line: ListLine, receipt: Receipt, today: date) -> str:
    """done — пришло, partial — пришла часть, waiting — день прошёл, future — впереди."""
    if closed(line, receipt):
        return "done"
    if receipt.received:
        return "partial"
    if _due(today.replace(day=1), line.day) < today:
        return "waiting"
    return "future"
```

- [ ] **Step 5: Тесты проходят**

Run: `pytest tests/test_receipts.py -q`
Expected: PASS (10 тестов).

- [ ] **Step 6: Полная проверка и коммит**

Run: `ruff check . && black --check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/services/receipts.py tests/test_receipts.py
git commit -m "feat(plan): reconcile the list of incomes and payments with the fact

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Лимит, траты дня, заморозка, прогноз по сумам и «Впереди» по сверке

**Files:**
- Modify: `app/services/day_budget.py` (`ForecastItem`, `DayBudget`, `read_forecast`, `_spent_on`, `day_budget`, утренний текст), `app/services/dashboard.py` (`build_dashboard`)
- Test: `tests/test_day_budget.py`, `tests/test_dashboard.py`

**Interfaces:**
- Consumes: `NOTHING`, `Receipt`, `match_receipts`, `remaining` (Task 1).
- Produces: `ForecastItem.row: int = 0` (строка вкладки); `DayBudget.ahead: tuple[ForecastItem, ...] = ()` — не закрытые на сейчас строки, сумма — что ещё впереди, по дню; `DayBudget.upcoming` — первые две из них; «Впереди» в утреннем тексте — опоздавшие с «⏳»; `Dashboard.upcoming = budget.ahead`.

- [ ] **Step 1: Падающие тесты**

В `tests/test_day_budget.py` в `test_forecast_list_and_goal_are_read_from_the_block` ожидаемые пункты — со строками вкладки:

```python
    assert forecast.items == (
        ForecastItem("Аванс", 15, 2_000_000, "UZS", 2_000_000, 12),
        ForecastItem("ЗП", 30, 10_000_000, "UZS", 10_000_000, 13),
        ForecastItem("Квартплата", 18, -650, "USD", -7_800_000, 14),
    )
```

В конец `tests/test_day_budget.py`:

```python
ADVANCE_PAID = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000)
RENT_PAID = fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 650, "USD")


def test_an_early_salary_is_not_counted_twice() -> None:
    early = fact_row(date(2026, 10, 29), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    day = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, early)
    # (21,2 млн на картах − 18,1 млн к концу месяца) / 2 дня: зарплата уже на картах
    assert day.limit == pytest.approx(1_550_000)
    assert day.upcoming == ()


def test_a_late_salary_stays_ahead() -> None:
    day = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID)
    # 11,2 млн на картах + 10 млн зарплаты впереди − 18,1 млн к концу месяца
    assert day.limit == pytest.approx(3_100_000)
    assert "📅 Впереди: ⏳ 30.10 ЗП +10,00 млн" in format_day_budget(day)


def test_a_salary_that_came_today_keeps_the_morning_limit() -> None:
    today = fact_row(date(2026, 10, 30), "💰 ДОХОДЫ", "зарплата", 10_000_000)
    day = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, today)
    # лимит с утра: на вечер вчера зарплаты на картах не было — она ещё впереди
    assert day.limit == pytest.approx(1_550_000)
    assert day.upcoming == ()  # а в «Впереди» её уже нет


def test_a_payment_a_bit_off_its_sum_does_not_eat_the_day() -> None:
    rent = fact_row(date(2026, 10, 18), "🏚️ ДОМ", "квартплата", 640, "USD")
    cafe = fact_row(date(2026, 10, 18), "🍔 ЕДА", "кафе", 50_000)
    assert budget(date(2026, 10, 19), rent, cafe).yesterday_spent == pytest.approx(
        50_000
    )


def test_rent_paid_before_its_day_leaves_the_frozen_dollars() -> None:
    early = fact_row(date(2026, 10, 16), "🏚️ ДОМ", "квартплата", 650, "USD")
    assert budget(date(2026, 10, 17), early).frozen == 350


def test_sums_wait_for_a_late_salary_today() -> None:
    cafe = [
        fact_row(date(2026, 10, day), "🍔 ЕДА", "кафе", 1_000_000)
        for day in range(17, 31)
    ]
    late = budget(date(2026, 10, 31), ADVANCE_PAID, *cafe)
    # сумы на вечер 30-го: 5 + 2 − 14 = −7 млн; зарплата 10 млн — сегодня
    assert late.runway is not None and late.runway.short == 0


def test_a_row_raised_after_it_came_opens_again() -> None:
    raised = [("Аванс", 15, 5_000_000, "UZS"), *ITEMS[1:]]
    day = budget(date(2026, 10, 16), ADVANCE_PAID, items=raised)
    assert [(item.name, item.amount) for item in day.ahead][0] == ("Аванс", 3_000_000)
```

В `tests/test_dashboard.py` тест `test_every_upcoming_item_is_listed_by_day` — целиком:

```python
def test_every_upcoming_item_is_listed_by_day() -> None:
    found = build_dashboard(frame(), tab(), date(2026, 10, 16), RATES, "USD")
    # аванс 15-го не пришёл — он впереди, первым
    assert [item.name for item in found.upcoming] == ["Аванс", "Квартплата", "ЗП"]
    paid = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000)
    found = build_dashboard(frame(paid), tab(), date(2026, 10, 16), RATES, "USD")
    assert [item.name for item in found.upcoming] == ["Квартплата", "ЗП"]
```

- [ ] **Step 2: Тесты падают**

Run: `pytest tests/test_day_budget.py tests/test_dashboard.py -q`
Expected: FAIL — у `ForecastItem` нет шестого поля; ранняя зарплата даёт лимит 6,55 млн, поздняя — отрицательный; трата 640 USD попадает в траты дня; заморозка −300; прогноз по сумам — нехватка 8 млн; у `DayBudget` нет `ahead`; в «Сводке» нет опоздавшего аванса.

- [ ] **Step 3: Реализация — модель и разбор**

В `app/services/day_budget.py`:

1. Импорты: добавить `from collections.abc import Mapping`, к `dataclasses` — `replace`, `from app.services.receipts import NOTHING, Receipt, match_receipts, remaining`. Строку `PAYMENT_MATCH = …` удалить.
2. В `ForecastItem` после `uzs: float`:

```python
    row: int = 0  # строка вкладки; 0 — пункт не из вкладки
```

3. В `DayBudget` после `history`:

```python
    ahead: tuple[ForecastItem, ...] = ()  # не пришедшие строки списка: что впереди
```

4. В `read_forecast` цикл по строкам — с номером строки:

```python
    for index in range(start + 1, len(rows)):
        name, day, amount, currency, uzs = _block(rows[index])
        if name == TABLE_HEADER:
            break
        if GOAL_LABEL in str(name):
            goal = _number(uzs) or 0.0
            continue
        day, amount, uzs = _number(day), _number(amount), _number(uzs)
        if (
            isinstance(name, str)
            and name.strip()
            and day is not None
            and 1 <= day <= 31
            and amount is not None
            and uzs is not None
        ):
            code = str(currency).strip().upper() or "UZS"
            items.append(
                ForecastItem(name.strip(), int(day), amount, code, uzs, index + 1)
            )
```

- [ ] **Step 4: Реализация — расчёты**

В `app/services/day_budget.py` `_spent_on` заменить и рядом добавить `_ahead`:

```python
def _spent_on(frame: pd.DataFrame, day: date, paid: frozenset[int]) -> float:
    """Траты дня без платежей из списка: квартплата не съедает лимит своего дня."""
    spent = expenses(frame)
    spent = spent[(spent["day"] == day) & ~spent.index.isin(list(paid))]
    return float(spent["amount_uzs"].sum())


def _ahead(
    items: tuple[ForecastItem, ...], receipts: Mapping[int, Receipt]
) -> tuple[ForecastItem, ...]:
    """Не закрытые строки списка: сумма — что ещё впереди, по дню."""
    left = []
    for item in items:
        rest = remaining(item, receipts.get(item.row, NOTHING))
        if rest:
            left.append(replace(item, amount=rest, uzs=item.uzs * rest / item.amount))
    return tuple(sorted(left, key=lambda item: item.day))
```

В `day_budget` докстринг дополнить и заменить всё после строки `target = max(plan_end, opening + forecast.savings_goal)`:

```python
    def ahead_on(day: date) -> tuple[ForecastItem, ...]:
        """Строки списка, не пришедшие к утру дня day."""
        receipts = match_receipts(frame, items, first, day - timedelta(days=1))
        return _ahead(items, receipts)

    def limit_on(day: date) -> float:
        ahead = sum(item.uzs for item in ahead_on(day))
        left = _balance(frame, day - timedelta(days=1)) + ahead - target
        return left / (days - day.day + 1)

    passed = yesterday.day if yesterday >= first else 0
    planned = (
        opening
        + sum(item.uzs for item in items if item.day <= passed)
        - per_day * passed
    )
    morning = ahead_on(today)
    now = match_receipts(frame, items, first, today)
    paid = frozenset(index for receipt in now.values() for index in receipt.operations)

    frozen, change, frozen_uzs = None, 0.0, 0.0
    rate = rates.to_uzs.get(frozen_currency) if frozen_currency else None
    if rate:
        own = frame[frame["currency"] == frozen_currency]

        def reserved(lines: tuple[ForecastItem, ...]) -> float:
            """Платежи в валюте заморозки, которые ещё впереди: они не отложены."""
            return -sum(
                item.amount
                for item in lines
                if item.currency == frozen_currency and item.amount < 0
            )

        frozen = _balance(own, yesterday, "amount") - reserved(morning)
        start = _balance(own, first - timedelta(days=1), "amount") - reserved(items)
        change, frozen_uzs = frozen - start, frozen * rate

    over = sorted(
        (line for line in spend if line.plan > 0 and line.fact > line.plan),
        key=lambda line: line.fact - line.plan,
        reverse=True,
    )
    left_now = _ahead(items, now)
    return DayBudget(
        today=today,
        limit=limit_on(today),
        plan_per_day=per_day,
        days_left=days - today.day + 1,
        balance=_balance(frame, yesterday),
        planned_balance=planned,
        yesterday_spent=_spent_on(frame, yesterday, paid),
        yesterday_limit=limit_on(yesterday) if passed else None,
        spent_today=_spent_on(frame, today, paid),
        growth=plan_end - opening,
        goal=forecast.savings_goal,
        frozen=frozen,
        frozen_change=change,
        frozen_uzs=frozen_uzs,
        frozen_currency=frozen_currency,
        upcoming=left_now[:2],
        over_plan=tuple(over[:3]),
        runway=uzs_runway(
            frame,
            [replace(item, day=max(item.day, today.day)) for item in morning],
            today,
            rates,
        ),
        history=tuple(
            DaySpend(day, _spent_on(frame, day, paid), limit_on(day))
            for day in (first + timedelta(days=k) for k in range(today.day))
        ),
        ahead=left_now,
    )
```

Докстринг `day_budget`:

```python
    """Лимит на сегодня по листу fact и вкладке месяца; None — во вкладке нет списка.

    Строки списка — по сверке с fact (services.receipts): к утру дня — для лимита,
    заморозки и прогноза по сумам; на сейчас — для «Впереди» и трат дня.
    """
```

- [ ] **Step 5: Реализация — «Впереди» в тексте и в «Сводке»**

В `app/services/day_budget.py` после `_item_amount`:

```python
def _ahead_text(item: ForecastItem, today: date) -> str:
    """«15.10 Аванс +2,00 млн»; опоздавшая строка — «⏳ 30.10 ЗП +10,00 млн»."""
    late = "⏳ " if item.day < today.day else ""
    return f"{late}{item.day:02d}.{today.month:02d} {item.name} {_item_amount(item)}"
```

в `format_day_budget` блок `if b.upcoming:` — целиком:

```python
    if b.upcoming:
        lines.append(
            "📅 Впереди: "
            + " • ".join(_ahead_text(item, b.today) for item in b.upcoming)
        )
```

В `app/services/dashboard.py` в `build_dashboard` вместо вычисления `upcoming` по дням:

```python
    upcoming = budget.ahead
```

- [ ] **Step 6: Тесты проходят**

Run: `pytest tests/test_day_budget.py tests/test_dashboard.py tests/test_runway.py -q`
Expected: PASS. Старые тесты лимита, заморозки и утреннего текста проходят без правок: без доходов в `fact` строки впереди так же, как раньше по датам.

- [ ] **Step 7: Полная проверка и коммит**

Run: `ruff check . && black --check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/services/day_budget.py app/services/dashboard.py tests/test_day_budget.py tests/test_dashboard.py
git commit -m "feat(plan): the day limit counts a list row as ahead until its money comes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: «Пришло к утру» во вкладке

**Files:**
- Create: `app/services/receipts_sync.py`
- Test: `tests/test_receipts_sync.py`

**Interfaces:**
- Consumes: `match_receipts`, `Receipt` (Task 1); `read_forecast`, `COLUMN_O`, `LIST_HEADER`, `TABLE_HEADER`, `GOAL_LABEL` из `day_budget.py`, `ForecastItem.row` (Task 2); `GoogleSheetsService.get_values`, `get_rates`, `write_cells` (`RAW`, без записи при пустом списке).
- Produces: `COLUMN_T = 19`, `RECEIVED_HEADER = "Пришло к утру"`, `received_cells(title, rows, receipts) -> list[dict[str, Any]]`, `sync_receipts(gs, today: date) -> int` — сколько ячеек переписано; 0 — вкладки или списка нет, или всё уже так.

- [ ] **Step 1: Падающие тесты**

`tests/test_receipts_sync.py`:

```python
"""Колонка «Пришло к утру»: что бот пишет во вкладку месяца."""

from datetime import date
from typing import Any

from gspread.exceptions import WorksheetNotFound

from app.services.receipts_sync import COLUMN_T, RECEIVED_HEADER, sync_receipts
from tests.test_day_budget import FACT_HEADER, OPENING, RATES, fact_row, tab

ADVANCE_PAID = fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 2_000_000)


def wide(rows: list[list[Any]]) -> list[list[Any]]:
    return [list(row) + [""] * (COLUMN_T + 1 - len(row)) for row in rows]


class Sheets:
    """Вкладка «Oct 26» и fact в памяти; write_cells пишет в колонку T."""

    def __init__(self, rows: list[list[Any]] | None, *fact: list[Any]) -> None:
        self.tabs = {} if rows is None else {"Oct 26": wide(rows)}
        self.fact = [FACT_HEADER, *OPENING, *fact]
        self.writes: list[list[dict[str, Any]]] = []

    def get_values(self, name: str) -> list[list[Any]]:
        if name == "fact":
            return self.fact
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return [list(row) for row in self.tabs[name]]

    def get_rates(self) -> Any:
        return RATES

    def write_cells(self, data: list[dict[str, Any]]) -> None:
        self.writes.append(data)
        for item in data:
            title, cell = item["range"].split("!")
            row = int(cell.removeprefix("T"))
            self.tabs[title.strip("'")][row - 1][COLUMN_T] = item["values"][0][0]

    def column(self) -> list[Any]:
        """T11:T15 — заголовок, аванс, ЗП, квартплата, пустая строка списка."""
        return [row[COLUMN_T] for row in self.tabs["Oct 26"][10:15]]


def test_what_came_by_this_morning_is_written_under_the_header() -> None:
    sheets = Sheets(tab(), ADVANCE_PAID)
    assert sync_receipts(sheets, date(2026, 10, 16)) == 2
    assert sheets.column() == [RECEIVED_HEADER, 2_000_000, "", "", ""]


def test_todays_income_waits_for_the_night() -> None:
    sheets = Sheets(tab(), ADVANCE_PAID)
    sync_receipts(sheets, date(2026, 10, 15))
    assert sheets.column() == [RECEIVED_HEADER, "", "", "", ""]


def test_nothing_changed_nothing_is_written() -> None:
    sheets = Sheets(tab(), ADVANCE_PAID)
    sync_receipts(sheets, date(2026, 10, 16))
    assert sync_receipts(sheets, date(2026, 10, 16)) == 0
    assert len(sheets.writes) == 1


def test_stale_values_are_cleared() -> None:
    rows = wide(tab())
    rows[10][COLUMN_T] = RECEIVED_HEADER
    rows[12][COLUMN_T] = 5_000  # ЗП не приходила
    rows[14][COLUMN_T] = "x"  # пустая строка списка
    sheets = Sheets(rows)
    assert sync_receipts(sheets, date(2026, 10, 16)) == 2
    assert sheets.column() == [RECEIVED_HEADER, "", "", "", ""]


def test_on_the_first_the_new_month_starts_empty() -> None:
    rows = wide(tab())
    rows[10][COLUMN_T] = RECEIVED_HEADER
    rows[11][COLUMN_T] = 2_000_000  # отметка прошлого месяца в копии вкладки
    sheets = Sheets(rows, ADVANCE_PAID)
    sync_receipts(sheets, date(2026, 10, 1))
    assert sheets.column()[1] == ""


def test_without_the_tab_or_the_list_nothing_is_written() -> None:
    assert sync_receipts(Sheets(None), date(2026, 10, 16)) == 0
    no_list = Sheets([["Статья", "Категория"], ["Расходы", "🍔 ЕДА", "кафе"]])
    assert sync_receipts(no_list, date(2026, 10, 16)) == 0
    assert no_list.writes == []
```

- [ ] **Step 2: Тесты падают**

Run: `pytest tests/test_receipts_sync.py -q`
Expected: ошибка импорта — `No module named 'app.services.receipts_sync'`.

- [ ] **Step 3: Реализация**

`app/services/receipts_sync.py`:

```python
"""Колонка «Пришло к утру» жёлтого списка вкладки текущего месяца.

Бот сверяет строки списка с fact (services.receipts) по операциям до вчерашнего
дня включительно — как лимит «с утра» — и пишет в колонку T, сколько пришло по
строке, в её валюте и со знаком. Формулы вкладки (P5, Q10) считают по колонке.
"""

from datetime import date, timedelta
from typing import Any

from gspread.exceptions import WorksheetNotFound

from app import config
from app.domain import month_title
from app.services.analytics_service import transactions_frame
from app.services.day_budget import (
    COLUMN_O,
    GOAL_LABEL,
    LIST_HEADER,
    TABLE_HEADER,
    read_forecast,
)
from app.services.google_sheets import GoogleSheetsService
from app.services.receipts import Receipt, match_receipts

COLUMN_T = COLUMN_O + 5  # справа от «В сумах» (S)
RECEIVED_HEADER = "Пришло к утру"


def _cell(row: list[Any], column: int) -> Any:
    return row[column] if len(row) > column else ""


def _same(have: Any, want: Any) -> bool:
    if want == "":
        return have in ("", None)
    number = isinstance(have, int | float) and not isinstance(have, bool)
    return number and abs(have - want) < 0.005


def received_cells(
    title: str, rows: list[list[Any]], receipts: dict[int, Receipt]
) -> list[dict[str, Any]]:
    """Ячейки колонки T, которые надо переписать: заголовок, суммы, лишние значения."""
    start = next(
        (
            index
            for index, row in enumerate(rows)
            if str(_cell(row, COLUMN_O)).startswith(LIST_HEADER)
        ),
        None,
    )
    if start is None:
        return []
    data: list[dict[str, Any]] = []
    if _cell(rows[start], COLUMN_T) != RECEIVED_HEADER:
        data.append(
            {"range": f"'{title}'!T{start + 1}", "values": [[RECEIVED_HEADER]]}
        )
    for index in range(start + 1, len(rows)):
        name = str(_cell(rows[index], COLUMN_O))
        if name == TABLE_HEADER or GOAL_LABEL in name:
            break
        receipt = receipts.get(index + 1)
        want: Any = round(receipt.received, 2) if receipt and receipt.received else ""
        if not _same(_cell(rows[index], COLUMN_T), want):
            data.append({"range": f"'{title}'!T{index + 1}", "values": [[want]]})
    return data


def sync_receipts(gs: GoogleSheetsService, today: date) -> int:
    """Записать «Пришло к утру» во вкладку текущего месяца; сколько ячеек переписано."""
    title = month_title(today)
    try:
        rows = gs.get_values(title)
    except WorksheetNotFound:
        return 0
    forecast = read_forecast(rows)
    if forecast is None:
        return 0
    frame = transactions_frame(gs.get_values(config.FACT_SHEET_NAME), gs.get_rates())
    first = today.replace(day=1)
    receipts = match_receipts(frame, forecast.items, first, today - timedelta(days=1))
    data = received_cells(title, rows, receipts)
    if data:
        gs.write_cells(data)
    return len(data)
```

- [ ] **Step 4: Тесты проходят**

Run: `pytest tests/test_receipts_sync.py -q`
Expected: PASS (6 тестов).

- [ ] **Step 5: Полная проверка и коммит**

Run: `ruff check . && black --check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/services/receipts_sync.py tests/test_receipts_sync.py
git commit -m "feat(plan): write what came in by this morning into the month tab

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Сверка после изменений и каждую ночь

**Files:**
- Create: `app/handlers/receipts.py`
- Modify: `app/handlers/messages.py` (`save_rows`), `app/handlers/undo.py` (`undo_last`), `app/handlers/fix.py` (успешное исправление), `app/handlers/month.py` (`month_tab_job`), `app/web/api.py` (`save_month_plan`), `app/main.py` (`_schedule_month_tab`)
- Test: `tests/test_receipts_job.py`, `tests/test_messages.py`, `tests/test_fix.py`, `tests/test_month.py`, `tests/test_web_plan.py`, `tests/test_main.py`

**Interfaces:**
- Consumes: `sync_receipts(gs, today) -> int` (Task 3).
- Produces: `DIRTY = "receipts_dirty"`, `LOCK = "receipts_lock"`, `INTERVAL_SECONDS = 120`; `mark_dirty(bot_data) -> None`; `async sync_now(bot_data, today) -> int` (ошибка — в лог, 0); `async receipts_job(context)` — сверка, только если помечено; задача `receipts` раз в 120 секунд; `month_tab_job` сверяет после проверки вкладки.

- [ ] **Step 1: Падающие тесты**

`tests/test_receipts_job.py`:

```python
"""Сверка «Пришло к утру»: по пометке раз в две минуты и каждую ночь."""

from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from app.handlers import receipts


def stub(monkeypatch: pytest.MonkeyPatch, outcome: int | Exception = 1) -> list[Any]:
    seen: list[Any] = []

    def sync(gs: Any, today: date) -> int:
        seen.append(gs)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(receipts, "sync_receipts", sync)
    return seen


async def test_the_job_reconciles_only_after_a_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = stub(monkeypatch)
    context = SimpleNamespace(bot_data={"gs_service": "sheets"})
    await receipts.receipts_job(context)
    assert seen == []
    receipts.mark_dirty(context.bot_data)
    await receipts.receipts_job(context)
    await receipts.receipts_job(context)
    assert seen == ["sheets"]


async def test_a_failed_reconciliation_stays_in_the_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub(monkeypatch, ConnectionError("Google is down"))
    assert await receipts.sync_now({"gs_service": "sheets"}, date(2026, 10, 16)) == 0
```

В `tests/test_messages.py` в конец:

```python
async def test_a_write_asks_for_a_list_reconciliation() -> None:
    update, context, _ = make_chat("48000 латте", manual_state())
    await messages.text_handler(update, context)
    assert context.bot_data["receipts_dirty"] is True


async def test_a_failed_write_does_not_ask_for_it() -> None:
    class BrokenSheets(FakeSheets):
        def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
            raise ConnectionError("Google is down")

    update, context, _ = make_chat("48000 латте", manual_state())
    context.bot_data["gs_service"] = BrokenSheets()
    await messages.text_handler(update, context)
    assert "receipts_dirty" not in context.bot_data
```

В `test_undo_deletes_the_last_write_and_forgets_the_sms` перед `command, _ = again(update, context, "/undo")`:

```python
    context.bot_data.pop("receipts_dirty")  # запись уже попросила сверку
```

и после проверки текста «↩️ Удалил из таблицы: строка 4169.»:

```python
    assert context.bot_data["receipts_dirty"] is True
```

В `tests/test_fix.py` в `test_single_row_goes_straight_to_groups_and_is_fixed` после `assert markup is None`:

```python
    assert context.bot_data["receipts_dirty"] is True
```

в `test_row_changed_in_the_sheet_is_left_alone` в конец:

```python
    assert "receipts_dirty" not in context.bot_data
```

В `tests/test_month.py` — фикстура после `owner` и тесты в конец:

```python
@pytest.fixture(autouse=True)
def reconcile(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    sync = AsyncMock(return_value=0)
    monkeypatch.setattr(month, "sync_now", sync)
    return sync
```

```python
@pytest.mark.usefixtures("owner")
async def test_the_list_is_reconciled_every_night(reconcile: AsyncMock) -> None:
    context = job_context(MonthTab("Oct 26"))
    await month.month_tab_job(context)
    reconcile.assert_awaited_once()
    assert reconcile.await_args.args[0] is context.bot_data


@pytest.mark.usefixtures("owner")
async def test_no_reconciliation_when_the_sheet_fails(reconcile: AsyncMock) -> None:
    await month.month_tab_job(job_context(ConnectionError("offline")))
    reconcile.assert_not_awaited()
```

В `tests/test_web_plan.py` в конец:

```python
def test_a_saved_plan_asks_for_a_list_reconciliation() -> None:
    application = plan_app()
    assert post_plan(application, change()).status_code == 200
    assert application.bot_data["receipts_dirty"] is True
```

В `tests/test_main.py` после `test_month_tab_is_checked_daily_and_on_start`:

```python
def test_the_list_is_reconciled_every_two_minutes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = build(monkeypatch)
    jobs = {job.name: job for job in app.job_queue.jobs()}
    assert jobs["receipts"].trigger.interval.total_seconds() == 120
```

- [ ] **Step 2: Тесты падают**

Run: `pytest tests/test_receipts_job.py tests/test_messages.py tests/test_fix.py tests/test_month.py tests/test_web_plan.py tests/test_main.py -q`
Expected: ошибка импорта `app.handlers.receipts`; после неё — нет `receipts_dirty`, нет задачи `receipts`, `month.sync_now` не вызывается.

- [ ] **Step 3: Реализация — модуль**

`app/handlers/receipts.py`:

```python
"""Колонка «Пришло к утру»: сверка ночью и после изменений днём.

Сегодняшние операции колонку не меняют (она «к утру»), поэтому днём сверка нужна
только после записей задним числом и правок списка: их помечает mark_dirty, а
задача раз в INTERVAL_SECONDS сверяет. Ночью (month_tab_job) — всегда.
"""

import asyncio
import logging
from datetime import date
from typing import Any

from telegram.ext import ContextTypes

from app import config
from app.domain import local_today
from app.services.receipts_sync import sync_receipts

logger = logging.getLogger(__name__)

DIRTY = "receipts_dirty"  # bot_data: в fact или в списке что-то поменялось
LOCK = "receipts_lock"  # bot_data: одна сверка за раз
INTERVAL_SECONDS = 120


def mark_dirty(bot_data: dict[str, Any]) -> None:
    """Попросить сверку: задача выполнит её не позже чем через INTERVAL_SECONDS."""
    bot_data[DIRTY] = True


async def sync_now(bot_data: dict[str, Any], today: date) -> int:
    """Сверка под замком; ошибка — в лог, колонку поправит следующая сверка."""
    lock: asyncio.Lock = bot_data.setdefault(LOCK, asyncio.Lock())
    async with lock:
        try:
            written = await asyncio.to_thread(
                sync_receipts, bot_data["gs_service"], today
            )
        except Exception:
            logger.exception("Receipts sync failed")
            return 0
    if written:
        logger.info("Receipts: %d cell(s) of the list updated", written)
    return written


async def receipts_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Раз в INTERVAL_SECONDS: сверка, если после прошлой что-то поменяли."""
    if context.bot_data.pop(DIRTY, False):
        await sync_now(context.bot_data, local_today(config.ANALYTICS_TIMEZONE))
```

- [ ] **Step 4: Реализация — пометки и задачи**

1. `app/handlers/messages.py`: импорт `from app.handlers.receipts import mark_dirty`; в `save_rows` в ветке `else:` после `context.user_data[LAST_WRITE] = {…}`:

```python
            mark_dirty(context.bot_data)  # «Пришло к утру» сверит задача receipts
```

2. `app/handlers/undo.py`: импорт `from app.handlers.receipts import mark_dirty`; в `undo_last` после проверки `if not deleted: return …`:

```python
    mark_dirty(context.bot_data)
```

3. `app/handlers/fix.py`: импорт `from app.handlers.receipts import mark_dirty`; перед `logger.info("Row %s recategorized", number)`:

```python
    mark_dirty(context.bot_data)  # доход стал тратой или наоборот — сверка списка
```

4. `app/handlers/month.py`: импорт `from app.handlers.receipts import sync_now`; в `month_tab_job` сразу после блока `try/except` вокруг `ensure_month_tab`:

```python
    await sync_now(context.bot_data, today)  # 1-го числа — уже по новой вкладке
```

5. `app/web/api.py`: импорт `from app.handlers.receipts import mark_dirty`; в `save_month_plan` после `fresh.saved = changes.count()`:

```python
        mark_dirty(application.bot_data)  # строку списка могли поменять
```

6. `app/main.py`: в импорт `from app.handlers import (…)` добавить `receipts`; в `_schedule_month_tab` после `run_once(…, name="month_tab_on_start")`:

```python
    app.job_queue.run_repeating(
        receipts.receipts_job,
        interval=receipts.INTERVAL_SECONDS,
        first=receipts.INTERVAL_SECONDS,
        name="receipts",
    )
```

- [ ] **Step 5: Тесты проходят**

Run: `pytest tests/test_receipts_job.py tests/test_messages.py tests/test_fix.py tests/test_month.py tests/test_web_plan.py tests/test_main.py -q`
Expected: PASS.

- [ ] **Step 6: Полная проверка и коммит**

Run: `ruff check . && black --check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/handlers/receipts.py app/handlers/messages.py app/handlers/undo.py app/handlers/fix.py app/handlers/month.py app/web/api.py app/main.py tests/test_receipts_job.py tests/test_messages.py tests/test_fix.py tests/test_month.py tests/test_web_plan.py tests/test_main.py
git commit -m "feat(plan): reconcile the list every night and after changes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Статусы строк списка в API «Плана»

**Files:**
- Modify: `app/services/plan.py` (`list_lines`, `LoadedPlan`, `load_plan`), `app/web/schemas.py` (`PlanEventOut`), `app/web/api.py` (`plan_out`, `event_out`)
- Test: `tests/test_web_plan.py`

**Interfaces:**
- Consumes: `Line`, `NOTHING`, `Receipt`, `match_receipts`, `receipt_status` (Task 1); `Event`, `MonthPlan` (этапы «Плана»).
- Produces: `list_lines(plan: MonthPlan) -> list[Line]`; `LoadedPlan.receipts: Mapping[int, Receipt]` (сверка на сейчас, только текущий месяц); у строки списка в `GET /api/plan` и в ответе сохранения: `received: float | null` (в валюте строки, со знаком), `received_on: "YYYY-MM-DD" | null`, `status: "done" | "partial" | "waiting" | "future"`; у следующего месяца — всегда `future`.

- [ ] **Step 1: Падающие тесты**

В `tests/test_web_plan.py` импорты: `from datetime import date`, `from unittest.mock import ANY`, `from app.web import api as web_api`. В `test_plan_has_rows_with_hints_events_goal_and_rates` ожидаемая строка списка:

```python
    assert body["events"][0] == {
        "slot": 0,
        "name": "Аванс",
        "day": 15,
        "amount": 3_000_000,
        "currency": "UZS",
        "received": None,
        "received_on": None,
        "status": ANY,  # future или waiting — по сегодняшней дате
    }
```

в тесте с `fresh["events"][2] == {…}` к ожидаемому словарю добавить `"received": None, "received_on": None, "status": ANY`. В конец файла:

```python
OCTOBER_EVENTS = [
    ("Аванс", 15, 3_000_000, "UZS"),
    ("ЗП СПб", 10, 100_000, "RUB"),
    ("Квартплата", 18, -500, "USD"),
    ("Премия", 12, 1_000_000, "UZS"),
]
OCTOBER_FACT = [
    FACT_HEADER,
    fact_row(date(2026, 10, 15), "💰 ДОХОДЫ", "зарплата", 3_000_000),
    fact_row(date(2026, 10, 10), "💰 ДОХОДЫ", "зарплата", 40_000, "RUB"),
]


def october(monkeypatch: pytest.MonkeyPatch, *titles: str) -> PlanSheets:
    monkeypatch.setattr(web_api, "local_today", lambda zone: date(2026, 10, 16))
    tab = month_tab([article("Расходы", "🍔 ЕДА", "кофе", 1_000)], OCTOBER_EVENTS)
    return PlanSheets({title: tab for title in titles}, OCTOBER_FACT)


def test_list_rows_tell_what_came_in_this_month(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = get_plan(october(monkeypatch, "Oct 26"), month="2026-10").json()
    assert [
        (e["name"], e["status"], e["received"], e["received_on"])
        for e in body["events"][:5]
    ] == [
        ("Аванс", "done", 3_000_000, "2026-10-15"),
        ("ЗП СПб", "partial", 40_000, "2026-10-10"),
        ("Квартплата", "future", None, None),
        ("Премия", "waiting", None, None),
        (None, "future", None, None),
    ]


def test_the_next_month_has_nothing_received_yet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sheets = october(monkeypatch, "Oct 26", "Nov 26")
    body = get_plan(sheets, month="2026-11").json()
    assert {e["status"] for e in body["events"]} == {"future"}
    assert {e["received"] for e in body["events"]} == {None}
```

(Аванс 15-го и Премия 12-го — обе в сумах: доход 15-го ближе к авансу, премия ждёт с 12-го.)

- [ ] **Step 2: Тесты падают**

Run: `pytest tests/test_web_plan.py -q`
Expected: FAIL — в ответе нет `received`, `received_on`, `status`.

- [ ] **Step 3: Реализация — сервис**

В `app/services/plan.py`:

1. Импорты: `from dataclasses import dataclass, field, replace`; к `app.domain` — `month_title`; `from app.services.receipts import Line, Receipt, match_receipts`.
2. После `read_plan`:

```python
def list_lines(plan: MonthPlan) -> list[Line]:
    """Заполненные строки списка — для сверки с fact."""
    return [
        Line(e.row, e.day, e.amount, e.currency)
        for e in plan.events or ()
        if e.day is not None and e.amount is not None and e.currency
    ]
```

3. `LoadedPlan` — новое поле:

```python
@dataclass(frozen=True)
class LoadedPlan:
    plan: MonthPlan
    rates: Rates
    history: History
    receipts: Mapping[int, Receipt] = field(default_factory=dict)  # что пришло
```

4. `load_plan` — после чтения fact:

```python
    rates = gs.get_rates()
    frame = transactions_frame(gs.get_values(config.FACT_SHEET_NAME), rates)
    plan = read_plan(rows)
    first = today.replace(day=1)
    receipts = (
        match_receipts(frame, list_lines(plan), first, today)
        if title == month_title(first)
        else {}
    )
    return LoadedPlan(plan, rates, article_history(frame, today), receipts)
```

- [ ] **Step 4: Реализация — API**

В `app/web/schemas.py` `PlanEventOut` целиком:

```python
class PlanEventOut(ApiModel):
    slot: int
    name: str | None
    day: int | None
    amount: float | None  # со знаком: «+» поступление, «−» платёж
    currency: str | None
    received: float | None = None  # пришло по строке: в её валюте и со знаком
    received_on: date | None = None  # дата последней операции
    status: Literal["done", "partial", "waiting", "future"] = "future"
```

В `app/web/api.py`: к импорту из `app.services.plan` — `Event`; `from app.services.receipts import NOTHING, Line, Receipt, receipt_status`; `from collections.abc import Mapping`. Перед `plan_out`:

```python
def event_out(
    event: Event, receipts: Mapping[int, Receipt] | None, today: date
) -> PlanEventOut:
    """Строка списка для экрана; receipts — сверка текущего месяца, None — следующий."""
    status, received, on = "future", None, None
    if receipts is not None and event.day is not None and event.amount is not None:
        line = Line(event.row, event.day, event.amount, event.currency or "UZS")
        receipt = receipts.get(event.row, NOTHING)
        status = receipt_status(line, receipt, today)
        if receipt.received:
            received, on = receipt.received, receipt.on
    return PlanEventOut(
        slot=event.slot,
        name=event.name,
        day=event.day,
        amount=event.amount,
        currency=event.currency,
        received=received,
        received_on=on,
        status=status,
    )
```

в `plan_out` блок `events` — целиком:

```python
    current = first == today.replace(day=1)
    events = None
    if plan.events is not None:
        receipts = loaded.receipts if current else None
        events = [event_out(e, receipts, today) for e in plan.events]
```

- [ ] **Step 5: Тесты проходят**

Run: `pytest tests/test_web_plan.py tests/test_month_plan.py -q`
Expected: PASS.

- [ ] **Step 6: Полная проверка и коммит**

Run: `ruff check . && black --check . && pytest -q`
Expected: всё зелёное.

```bash
git add app/services/plan.py app/web/schemas.py app/web/api.py tests/test_web_plan.py
git commit -m "feat(api): what came in for the rows of the list

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: Подписи в приложении

**Files:**
- Create: `webapp/src/plan/receipt.ts`, `webapp/src/plan/receipt.test.ts`
- Modify: `webapp/src/api.ts` (`PlanEvent`), `webapp/src/plan/PlanScreen.tsx` (строка списка), `webapp/src/dashboard/DashboardScreen.tsx` («Впереди»)
- Test: `webapp/src/plan/PlanScreen.test.tsx`, `webapp/src/dashboard/DashboardScreen.test.tsx`

**Interfaces:**
- Consumes: `received`, `received_on`, `status` строки списка (Task 5); `upcoming` «Сводки» с опоздавшими строками (Task 2); `short`, `withCurrency` из `format.ts`.
- Produces: `receiptHint(event: PlanEvent, month: string): string | null`; под строкой списка на экране «План» — подпись (кроме строк, изменённых в черновике); в «Впереди» «Сводки» — «⏳» у опоздавших.

- [ ] **Step 1: Падающие тесты**

`webapp/src/plan/receipt.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import type { PlanEvent } from "../api";
import { receiptHint } from "./receipt";

function event(over: Partial<PlanEvent>): PlanEvent {
  return {
    slot: 0,
    name: "Аванс",
    day: 15,
    amount: 2_720_000,
    currency: "UZS",
    ...over,
  };
}

describe("receiptHint", () => {
  it("says what came in and when", () => {
    expect(
      receiptHint(
        event({ status: "done", received: 2_720_000, received_on: "2026-10-15" }),
        "2026-10",
      ),
    ).toBe("✓ пришло 2,72 млн · 15.10");
  });

  it("says a payment is paid", () => {
    const rent = event({
      name: "Квартплата",
      amount: -650,
      currency: "USD",
      status: "done",
      received: -650,
      received_on: "2026-10-17",
    });
    expect(receiptHint(rent, "2026-10")).toBe("✓ оплачено 650,00 USD · 17.10");
  });

  it("shows a part that came in", () => {
    const part = event({
      amount: 108_000,
      currency: "RUB",
      status: "partial",
      received: 54_000,
    });
    expect(receiptHint(part, "2026-10")).toBe(
      "пришло 54 000,00 RUB из 108 000,00 RUB",
    );
  });

  it("waits for a late row and says nothing ahead of time", () => {
    expect(receiptHint(event({ day: 30, status: "waiting" }), "2026-10")).toBe(
      "⏳ ждём с 30.10",
    );
    expect(receiptHint(event({ status: "future" }), "2026-10")).toBeNull();
    expect(receiptHint(event({}), "2026-10")).toBeNull();
  });
});
```

В `webapp/src/plan/PlanScreen.test.tsx` в `describe("PlanScreen", …)`:

```tsx
  it("shows what came in for the rows of the list", async () => {
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      events: (planFixture.events ?? []).map((item) => {
        if (item.slot === 0) {
          return {
            ...item,
            status: "done" as const,
            received: 5_000_000,
            received_on: "2026-10-15",
          };
        }
        return item.slot === 2 ? { ...item, status: "waiting" as const } : item;
      }),
    });
    render(<Harness api={fakeApi({ plan })} />);
    expect(await screen.findByText("✓ пришло 5,00 млн · 15.10")).toBeTruthy();
    expect(screen.getByText("⏳ ждём с 18.10")).toBeTruthy();
  });
```

В `webapp/src/dashboard/DashboardScreen.test.tsx` в `describe("DashboardScreen", …)`:

```tsx
  it("marks a row of the list that is late", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      upcoming: [
        { name: "ЗП", day: 5, amount: 10_000_000, currency: "UZS", uzs: 10_000_000 },
      ],
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("⏳ 05 — ЗП")).toBeTruthy();
  });
```

- [ ] **Step 2: Тесты падают**

Run: `cd webapp && npx vitest run src/plan src/dashboard`
Expected: FAIL — нет модуля `./receipt`; нет подписей; нет «⏳».

- [ ] **Step 3: Реализация**

`webapp/src/api.ts` — в `PlanEvent` после `currency`:

```ts
  received?: number | null; // пришло по строке: в её валюте и со знаком
  received_on?: string | null; // «YYYY-MM-DD» последней операции
  status?: "done" | "partial" | "waiting" | "future";
```

`webapp/src/plan/receipt.ts`:

```ts
import type { PlanEvent } from "../api";
import { short, withCurrency } from "../format";

function plain(value: number, currency: string): string {
  return currency === "UZS"
    ? short(Math.abs(value))
    : withCurrency(Math.abs(value), currency);
}

function dayMonth(isoDay: string): string {
  return `${isoDay.slice(8, 10)}.${isoDay.slice(5, 7)}`;
}

/** Подпись под строкой списка: что по ней пришло; null — подписи нет. */
export function receiptHint(event: PlanEvent, month: string): string | null {
  const { amount, currency, day, received, received_on: on, status } = event;
  if (amount === null || currency === null || day === null) return null;
  const verb = amount > 0 ? "пришло" : "оплачено";
  if (status === "done" && typeof received === "number" && on) {
    return `✓ ${verb} ${plain(received, currency)} · ${dayMonth(on)}`;
  }
  if (status === "partial" && typeof received === "number") {
    return `${verb} ${plain(received, currency)} из ${plain(amount, currency)}`;
  }
  if (status === "waiting") {
    return `⏳ ждём с ${String(day).padStart(2, "0")}.${month.slice(5, 7)}`;
  }
  return null;
}
```

`webapp/src/plan/PlanScreen.tsx`: импорт `import { receiptHint } from "./receipt";`; в `filled.map((item) => {…})` после `const amount = item.amount ?? 0;`:

```tsx
            const original = plan.events?.find((e) => e.slot === item.slot);
            const hint =
              !edited && original ? receiptHint(original, plan.month) : null;
```

и название строки:

```tsx
                <span
                  className={`flex flex-1 flex-col ${edited ? "text-link" : ""}`}
                >
                  {item.name}
                  {hint && <span className="text-xs text-hint">{hint}</span>}
                </span>
```

`webapp/src/dashboard/DashboardScreen.tsx`: в блоке «Впереди» текст пункта (опоздавшая строка — день уже прошёл, а деньги не пришли):

```tsx
                <span>
                  {item.day < Number(data.today.slice(8, 10)) ? "⏳ " : ""}
                  {String(item.day).padStart(2, "0")} — {item.name}
                </span>
```

- [ ] **Step 4: Тесты проходят**

Run: `cd webapp && npx vitest run src/plan src/dashboard`
Expected: PASS.

- [ ] **Step 5: Полная проверка и коммит**

Run: `cd webapp && npx prettier --write src && npm run lint && npm test && npm run build`
Expected: всё зелёное (prettier только переносит длинные строки новых тестов).

```bash
git add webapp/src/api.ts webapp/src/plan/receipt.ts webapp/src/plan/receipt.test.ts webapp/src/plan/PlanScreen.tsx webapp/src/plan/PlanScreen.test.tsx webapp/src/dashboard/DashboardScreen.tsx webapp/src/dashboard/DashboardScreen.test.tsx
git commit -m "feat(webapp): what came in under the rows of the list

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: README, PR, выкатка, формулы вкладки и живая проверка

**Files:**
- Modify: `README.md` (пункт `/today`)

**Interfaces:**
- Consumes: всё из Tasks 1–6.
- Produces: PR в `main`, деплой, формулы P5 и Q10 во вкладке текущего месяца, сверка с ботом.

- [ ] **Step 1: README**

В пункт `*   **`/today`** — …` дописать:

```markdown
Rows of the «Поступления и платежи» list count as ahead until their money comes, not by their day: the bot reconciles them with `fact` (incomes of the row's currency from five days before its day, parts added up; a payment is one spending within 10 % of its sum; 90 % closes a row) and writes what came in by this morning into the «Пришло к утру» column of the month tab every night and two minutes after a change. The day limit, the frozen dollars and the sums forecast — in the bot and in the tab's formulas — use it; the «План» screen shows «✓ пришло …», «пришло … из …» or «⏳ ждём с …» under a row.
```

- [ ] **Step 2: Полная проверка, коммит, PR**

Run: `ruff check . && black --check . && pytest -q` и `cd webapp && npm run lint && npm test && npm run build`
Expected: всё зелёное.

```bash
git add README.md
git commit -m "docs: the list of incomes and payments is reconciled with the fact

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/list-receipts
gh pr create --base main --head feat/list-receipts --title "feat: reconcile the list of incomes and payments with the fact" --body-file - <<'BODY'
Сверка строк списка «Поступления и платежи» с `fact` (спека `docs/superpowers/specs/2026-10-14-list-receipts-design.md`, план `docs/superpowers/plans/2026-10-14-list-receipts.md`).

## Что делает
- `app/services/receipts.py`: что пришло по строке — доходы валюты строки с дня − 5 до конца месяца (части складываются, доход — к строке с ближайшим днём), платёж — одна трата ±10 %; строка закрыта при 90 %.
- Лимит, «Заморожено», прогноз по сумам — по сверке к утру; «Впереди» и траты дня — на сейчас. Ранняя зарплата не считается дважды, поздняя не роняет лимит.
- Колонка «Пришло к утру» во вкладке месяца: бот пишет ночью (00:05) и через 2 минуты после изменений; формулы P5 и Q10 вкладки переводятся на неё после выкатки.
- «План»: под строкой списка «✓ пришло …», «пришло … из …», «⏳ ждём с …»; «Сводка»: «⏳» у опоздавших.

## Проверка
- `ruff check . && black --check . && pytest -q`
- `npm run lint && npm test && npm run build` в `webapp/`
- после выкатки: P5 и Q10 вкладки совпадают с расчётом бота по тем же данным

🤖 Generated with [Claude Code](https://claude.com/claude-code)
BODY
```

- [ ] **Step 3: CI, merge, деплой**

После зелёного CI: `gh pr merge --merge --delete-branch`. Render деплоит `main` сам: новый бандл на `/app/`, `/health` — 200. В логах после старта — «Receipts: N cell(s) of the list updated» (сверка при запуске: `month_tab_on_start` через 30 секунд) и ни одной ошибки.

- [ ] **Step 4: Формулы вкладки**

ID таблицы — из заметок владельца (`SHEET_ID`), в репозиторий и в PR не пишется; `TITLE` — вкладка текущего месяца (в октябре — «Oct 26»). Сначала резервная копия блока прогноза с формулами:

```bash
gws sheets spreadsheets values get --params '{"spreadsheetId":"'"$SHEET_ID"'","range":"'"'$TITLE'"'!O1:T20","valueRenderOption":"FORMULA"}' > ../forecast-backup.json
```

Новые P5 и Q10 — заменой старых слагаемых из раздела 6 спеки; каждое должно встретиться ровно один раз, иначе скрипт падает и формулы не трогаются:

```bash
TITLE="$TITLE" ../venv_dev/bin/python - <<'EOF'
import json
import os

OLD_P5 = 'SUMIFS($S$12:$S$19;$P$12:$P$19;">="&DAY($P$1))'
NEW_P5 = (
    "SUMPRODUCT(IFERROR(IF(ABS($T$12:$T$19)>=0.9*ABS($Q$12:$Q$19);0;"
    "$S$12:$S$19*(1-$T$12:$T$19/$Q$12:$Q$19));0))"
)
OLD_Q10 = 'SUMIFS($Q$12:$Q$19;$R$12:$R$19;"USD";$Q$12:$Q$19;"<0";$P$12:$P$19;">="&DAY($P$1))'
NEW_Q10 = (
    'SUMPRODUCT(IFERROR(($R$12:$R$19="USD")*($Q$12:$Q$19<0)*'
    "(ABS($T$12:$T$19)<0.9*ABS($Q$12:$Q$19))*($Q$12:$Q$19-$T$12:$T$19);0))"
)
rows = json.load(open("../forecast-backup.json"))["values"]
p5, q10 = rows[4][1], rows[9][2]  # O1:T20: P5 — строка 5, Q10 — строка 10
assert p5.count(OLD_P5) == 1, p5
assert q10.count(OLD_Q10) == 1, q10
title = os.environ["TITLE"]
body = {
    "valueInputOption": "USER_ENTERED",
    "data": [
        {"range": f"'{title}'!P5", "values": [[p5.replace(OLD_P5, NEW_P5)]]},
        {"range": f"'{title}'!Q10", "values": [[q10.replace(OLD_Q10, NEW_Q10)]]},
    ],
}
json.dump(body, open("../forecast-new.json", "w"), ensure_ascii=False)
print(json.dumps(body, ensure_ascii=False, indent=1))
EOF
gws sheets spreadsheets values batchUpdate --params '{"spreadsheetId":"'"$SHEET_ID"'"}' --json "$(cat ../forecast-new.json)"
```

Expected: `totalUpdatedCells: 2`. Ошибка в формуле видна сразу (`#ERROR!` в P5 или Q10 на шаге 5) — тогда старые формулы возвращаются тем же `batchUpdate` из `../forecast-backup.json`.

- [ ] **Step 5: Живая проверка**

Прочитать `fact`, вкладку текущего месяца, курсы и ячейки P5:Q10 (`UNFORMATTED_VALUE`), посчитать лимит и заморозку кодом бота и сравнить:

```bash
get() {
  gws sheets spreadsheets values get --params '{"spreadsheetId":"'"$SHEET_ID"'","range":"'"$1"'","valueRenderOption":"UNFORMATTED_VALUE"}' > "../live-$2.json"
}
get fact fact && get "'$TITLE'" tab && get "system!I2:I3" rates && get "'$TITLE'!P5:Q10" cells
../venv_dev/bin/python - <<'EOF'
import json

from app.domain import Rates, local_today
from app.services.analytics_service import transactions_frame
from app.services.day_budget import day_budget


def values(name: str) -> list:
    return json.load(open(f"../live-{name}.json"))["values"]


usd, rub = (row[0] for row in values("rates"))
rates = Rates({"UZS": 1.0, "USD": usd, "RUB": rub})
budget = day_budget(
    transactions_frame(values("fact"), rates),
    values("tab"),
    local_today("Asia/Tashkent"),
    rates,
    "USD",
)
cells = values("cells")
print("bot", round(budget.limit), budget.frozen)
print("tab", round(cells[0][0]), cells[5][1])
EOF
```

Expected: лимит бота и P5 совпадают до сума, заморозка бота и Q10 — до цента. Расхождение — найти причину до того, как говорить владельцу, что готово (частая — «Пришло к утру» ещё не записана: запустить сверку записью любой операции или подождать задачу `receipts`).

- [ ] **Step 6: Проверка владельцем**

Попросить владельца в ближайший день поступления или платежа посмотреть «План» (подпись под строкой), «Сводку» и утреннее сообщение. Если доход отнесён не к той строке — разобрать правила раздела 4 спеки.
