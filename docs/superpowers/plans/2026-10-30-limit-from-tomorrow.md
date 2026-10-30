# «С завтра» в день зарплаты и «Сумы» до зарплаты (этап 1б) — план

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** в день прихода основной зарплаты «Можно сегодня», /today и ответ на запись показывают лимит с завтра по деньгам на сейчас; карточка «💳 Сумы сейчас» считает, хватит ли сумов до зарплаты, своим простым расчётом по дням срока.

**Architecture:** в `day_budget.py` деньги срока получают даты: авансы, платежи списка и подписки — списками «(день, сумма)», прежние суммы считаются из них. `allowance_on(day)` умеет день следующего месяца (его строки ещё не пришли), и `day_budget` в день прихода зарплаты кладёт в `DayBudget.tomorrow` лимит `allowance_on(today + 1)`. Новый `horizon_runway` (runway.py) идёт по дням от сегодня до дня ожидаемой зарплаты: сумы сейчас, авансы и платежи в их дни, темп 14 дней. Он заменяет `uzs_runway`. Модель «Календаря» (forecast.py) не меняется: из неё берутся только сумы сейчас, темп и траты сегодня.

**Tech Stack:** Python 3.12 (venv 3.14), pandas, pytest; React + TypeScript + Tailwind, vitest.

**Spec:** `docs/superpowers/specs/2026-10-30-clear-money-design.md` — 1.2, 1.4, 2.1 («Можно сегодня» в день прихода основной зарплаты, «💳 Сумы сейчас»), 2.4 (запись основной зарплаты).

## Global Constraints

- «Зарплата пришла сегодня» — строка из `salaries` (основные; их нет — авансы, правило 1.1) закрыта по сверке на сейчас (`match_receipts(..., through=today)`) и не закрыта по сверке к утру (`through=yesterday`). Закрытой считается строка с ≥ 90 % (`receipts.closed`).
- Лимит с завтра — `allowance_on(today + 1)`: сумы на вечер сегодня (fact по сегодня), срок с завтра по следующую основную зарплату включительно, авансы, платежи и подписки — с завтра.
- День следующего месяца в `allowance_on`: сверка строк пустая (строки следующего месяца ещё не пришли), строки вкладки берутся перенесёнными на тот же день целиком. Иначе 30 ноября «с завтра» (1 декабря) нашло бы закрытую ноябрьскую строку и сдвинуло срок на 30 января.
- Утро (`format_day_budget(now=False)`, `rich_day_budget(now=False)`) не меняется, кроме строки «Сумы».
- Формулы вкладки (P5, Q5, R5) не меняются: вкладка — утренний снимок.
- **Ruling (план):** срок «Сумы» — по день следующей основной зарплаты, **которая ещё не пришла**, как у «Заморожено» (1.5). Сегодняшняя и опоздавшая, пока денег нет, ждутся сегодня: тогда срок кончается сегодня. Сама зарплата в расчёт не прибавляется. Иначе утром в день ЗП, до прихода денег, «Сумы» требовали бы обменять доллары на месяц вперёд, а с приходом денег всё отменялось бы. Цена ошибки: утром в день ЗП «Можно сегодня» говорит «до зарплаты 30 ноября» (лимит её не ждёт, 1.2), а «Сумы» — «до зарплаты сегодня».
- Тексты — дословно из этого плана (они по спеке 2.1 и 2.4). Для плохого зрения: белый жирный текст, без красного текста, числа целиком, перед «сум» обычный пробел.
- Публичный репозиторий: в тестах и документах только выдуманные суммы.
- Python: type hints, black + ruff, строки ≤ 88. TS: строгие типы, prettier, eslint.

## Review Focus

1. ЗП в последний день месяца (30 ноября в 30-дневном месяце, 31 октября): «с завтра» — 1-е число следующего месяца; срок — до ЗП следующего месяца, а не через месяц.
2. Две строки основной зарплаты в месяце (15-го и 30-го): «с завтра» — по той, что закрылась сегодня; срок — до следующей.
3. ЗП пришла частями в один день (60 % + 40 %): закрыта сейчас, не закрыта утром — «с завтра».
4. ЗП записали сегодня задним числом (вчерашней датой): она закрыта уже к утру, «с завтра» не включается. Лимит утра считает её как пришедшую вчера.
5. Траты после зарплаты в тот же день: лимит с завтра считает сумы по сегодня, с этими тратами.

---

### Task 1: Деньги срока по дням

**Files:** Modify `app/services/day_budget.py` (`_advances`, `_list_bills`, `_subscriptions` и новые `_advance_days`, `_bill_days`, `_subscription_days`); Test `tests/test_day_budget.py`.

**Interfaces:** Produces:
- `_advance_days(advances: Sequence[ForecastItem], receipts: Mapping[int, Receipt], day: date, until: date) -> list[tuple[date, float, str]]` — (день, сумма, название) каждого аванса срока, по правилам нынешнего `_advances`: открытые строки этого месяца с днём `day <= due < until` — их непришедшая часть; они же в следующем месяце — целиком.
- `_bill_days(items, receipts, day, until) -> list[tuple[date, float]]` — платежи списка в сумах **положительными** суммами в свой день. Опоздавшие открытые (день раньше `day`) — днём `day`. Строки следующего месяца — целиком.
- `_subscription_days(series, day, until) -> list[tuple[date, float]]` — подписки в сумах в их ожидаемый день.
- `_advances` возвращает `(сумма, первый день, его название)`, `_list_bills` и `_subscriptions` — суммы этих списков. Поведение лимита не меняется.

- [ ] Step 1: тесты (вкладка `tab()` из тестов, выдуманные числа):
  - `test_the_horizons_money_has_its_days`: 16 октября, срок до 31 октября: `_bill_days` даёт квартплату в сумах (добавить в `items` строку `("Свет", 20, -300_000, "UZS")`) днём 20.10; опоздавший платёж («Газ», 10-го, −100 000, не оплачен) — днём 16.10.
  - `test_next_months_rows_come_in_full`: срок 30.10 → 1.12. Аванс 15 ноября 2 000 000 — `(date(2026, 11, 15), 2_000_000, "Аванс")` в `_advance_days`.
  - Все прежние тесты `test_day_budget.py` зелёные: суммы те же.
- [ ] Step 2: `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_day_budget.py` → FAIL (нет `_bill_days`).
- [ ] Step 3: реализовать; `_advances`/`_list_bills`/`_subscriptions` — через новые функции.
- [ ] Step 4: весь набор `pytest` → PASS.
- [ ] Step 5: commit `refactor: the limit's money has its days`.

### Task 2: Лимит с завтра в день зарплаты

**Files:** Modify `app/services/day_budget.py` (`allowance_on`, `day_budget`, `DayBudget`); Test `tests/test_day_budget.py`.

**Interfaces:** Produces поля `DayBudget`:
- `tomorrow: Allowance | None = None` — лимит с завтра, только в день прихода зарплаты;
- `salary_came: float = 0.0` — сколько пришло по закрывшейся сегодня строке (`Receipt.received`);
- `salary_name: str = ""` — её название.

- [ ] Step 1: тесты (выдуманные суммы, вкладка `tab()`: аванс 15-го, ЗП 30-го):
  - `test_on_the_salary_day_the_limit_runs_from_tomorrow`: 30 октября пришла ЗП 10 000 000 (fact 30.10) → `tomorrow` не `None`, `tomorrow.payday == date(2026, 11, 30)`, `tomorrow.days == 31` (с 31 октября по 30 ноября), `tomorrow.sums` — сумы по 30.10 включительно, `salary_came == 10_000_000`, `salary_name == "ЗП"`. `limit` дня — прежний, утренний.
  - `test_a_salary_on_the_last_day_runs_to_next_months_salary`: вкладка со строкой «ЗП» 30-го, сегодня 30 ноября (30-дневный месяц), ЗП пришла → `tomorrow.payday == date(2026, 12, 30)`, `tomorrow.days == 30` и аванс 15 декабря в `tomorrow.advances`.
  - `test_a_salary_from_yesterday_is_the_morning_limit`: ЗП пришла 30-го, сегодня 31-е → `tomorrow is None`.
  - `test_no_tomorrow_until_the_salary_comes`: 30-е, ЗП нет или пришло 50 % → `tomorrow is None`.
  - `test_the_salary_in_two_parts_today_runs_from_tomorrow`: 6 000 000 и 4 000 000 30-го → `tomorrow` есть, `salary_came == 10_000_000`.
  - `test_without_a_main_salary_the_advance_runs_from_tomorrow`: в списке только «Аванс» 15-го, он пришёл 15-го → `tomorrow.payday == date(2026, 11, 15)`.
- [ ] Step 2: FAIL.
- [ ] Step 3: реализовать. `allowance_on(day)`: если `(day.year, day.month) != (first.year, first.month)`, сверка — `{}`; день зарплаты закрылся сегодня — `tomorrow = allowance_on(today + timedelta(days=1))`.
- [ ] Step 4: весь набор → PASS.
- [ ] Step 5: commit `feat: on the salary day the limit runs from tomorrow`.

### Task 3: «Сумы» — до зарплаты по дням

**Files:** Modify `app/services/runway.py` (`Runway`, новый `horizon_runway`, убрать `uzs_runway`), `app/services/day_budget.py` (вызов); Test `tests/test_runway.py` (новый) и правки `tests/test_day_budget.py`.

**Interfaces:**
- `Runway` получает `left: float` (остаток вечером в день срока) и `payday: date | None` (день срока; `None` — конец месяца). `until` — день срока, если не хватает.
- `horizon_runway(now: float, pace: float, spent_today: float, today: date, end: date, events: Sequence[tuple[date, float]], rates: Rates, payday: date | None) -> Runway`:
  - сегодня — `now + события сегодня − max(pace − spent_today, 0)`;
  - каждый следующий день по `end` включительно — `+ события дня − pace`;
  - `short` — самый глубокий минус (0 — не уходит в минус), `last_day` — день перед первым минусом (`None` — минуса нет), `left` — остаток вечером дня `end`, `dollars` — как сейчас (вверх до $10 по курсу USD).
- В `day_budget`: срок — по Ruling выше. Ближайшая открытая по сверке на сейчас строка `salaries`: её день не позже сегодня — `end = today`; иначе — её день; открытых в месяце нет — первая строка `salaries` в следующем месяце; строк нет — конец месяца, `payday=None`. `now`, `pace`, `spent_today` — из `currency_forecast(frame, (), items, today, "UZS", records=records, carried=carried)`. События — с сегодня по `end` по сверке на сейчас: авансы `+`, платежи списка и подписки `−` (функции Task 1).

- [ ] Step 1: тесты `tests/test_runway.py`:
  - `test_the_sums_last_until_the_salary`: сейчас 3 000 000, темп 100 000, сегодня потрачено 40 000, 10 дней → `short == 0`, `left == 3_000_000 − 60_000 − 9 × 100_000`.
  - `test_the_advance_closes_the_dip`: сейчас 500 000, темп 200 000, аванс +2 000 000 на 4-й день → `last_day` — 2-й день, `short` — глубина минуса, `left` после аванса.
  - `test_payments_come_off_on_their_day`.
  - `test_dollars_round_up_to_ten`.
  - В `tests/test_day_budget.py`: `test_sums_wait_for_a_late_salary_today` → по Ruling: опоздавшая ЗП 31-го — срок «Сумы» кончается сегодня, `runway.payday == date(2026, 10, 31)`; утром в день ЗП, до денег, — `runway.payday` сегодня.
- [ ] Step 2: FAIL.
- [ ] Step 3: реализовать, `uzs_runway` удалить; docstring модуля — новый расчёт.
- [ ] Step 4: весь набор → PASS.
- [ ] Step 5: commit `feat: the sums card counts until the salary`.

### Task 4: Тексты бота

**Files:** Modify `app/services/day_budget.py` (`format_day_budget`, `rich_day_budget`, `today_line`, `rich_today_line`, `runway_line`, `_rich_runway`, `_money_line`); Test `tests/test_day_budget.py`, `tests/test_rich_replies.py`.

Тексты (D — «30 ноября» / «30-го», как `until_text`; деньги — как сейчас в каждом месте):
- /today (`now=True`), `tomorrow` есть:
  - текст: `💸 С завтра можно {money(X)} в день до зарплаты D`, ниже `   ✓ пришла ЗП {money(C)}, потрачено сегодня {money(S)}` и `   на сумовых картах сейчас …` — строка денег по `tomorrow`;
  - расширенный: `heading("💸 {whole(X)} сум", 1)`, `para("можно в день с завтра", "до зарплаты D, {days_text(N)}", "✓ пришла ЗП {whole(C)}", "потрачено сегодня {whole(S)}", строка денег)`.
  - Лимита с завтра нет (`X ≤ 0`) — как нынешний «Лимита на сегодня нет», с числами `tomorrow`.
- Ответ на запись (`today_line`, `rich_today_line`), `tomorrow` есть: `💸 С завтра можно {money(X)} в день до зарплаты D` / `heading("💸 С завтра можно {whole(X)}")`, `para("в день до зарплаты D")`. Вместо «На сегодня осталось»: утренний лимит в этот день устарел (1.4).
- «Сумы» (`runway_line`, `_rich_runway`):
  - хватает: `💳 Сумы {short(B)} — до зарплаты D хватит, останется ≈ {short(L)}` / `heading("💳 Сумы сейчас {whole(B)}")`, `para("✓ до зарплаты D хватит", "останется ≈ {whole(L)}")`;
  - не хватает: как сейчас, цель — `до зарплаты D` (`до конца месяца`, если `payday` нет).
  - D сегодня — «до зарплаты сегодня».
- [ ] Step 1: тесты на эти строки (дословно). Step 2: FAIL. Step 3: реализовать. Step 4: весь набор → PASS. Step 5: commit `feat: the bot speaks of tomorrow on the salary day`.

### Task 5: «Сводка → Сегодня»

**Files:** Modify `app/web/schemas.py` (`DashboardOut`, `RunwayOut`, новый `TomorrowOut`), `app/web/api.py` (`dashboard_out`), `webapp/src/api.ts`, `webapp/src/dashboard/DashboardScreen.tsx` (`Today`, `LimitSource`, `Sums`), `webapp/src/mocks/fixtures.ts`; Test `tests/test_web_api.py`, `webapp/src/dashboard/DashboardScreen.test.tsx`; README.md и CLAUDE.md — абзацы про лимит и «Сумы».

**Interfaces:**
- `TomorrowOut`: `limit: float`, `days: int`, `payday: date | None`, `until: str` (`until_text` по `tomorrow`), `sums: float`, `bills: float`, `advances: float`, `advance_on: date | None`.
- `DashboardOut`: `tomorrow: TomorrowOut | None = None`, `salary_came: float | None = None`.
- `RunwayOut`: `left: float | None = None`, `payday: date | None = None` (`None` у старых ответов — приложение показывает как раньше).

Карточки (спека 2.1):
- «Можно сегодня», `tomorrow` есть: `<Good>✓ Пришла ЗП {money(C)}</Good>`, `<Big>{money(X)} сум</Big>`, `<Line>можно в день с завтра</Line>`, `<Line>до зарплаты D, N дней</Line>`, `LimitSource` по `tomorrow` с первой строкой «сумы сейчас», `<Line>потрачено сегодня S</Line>`, строка плана. «Сверх лимита дня» в этот день не показывается.
- «Сумы»: `<Fact>💳 Сумы сейчас B</Fact>`; хватает — `<Good>✓ до зарплаты D хватит</Good>`, `<Line>останется ≈ L</Line>`, `<Line>если тратить ≈ P в день,</Line>`, `<Line>как в среднем за 14 дней</Line>`; не хватает — как сейчас, цель `до зарплаты D`.

- [ ] Step 1: тесты: API отдаёт `tomorrow` и `salary_came` в день ЗП, `null` в другой день. Компонент: тексты и порядок строк обеих карточек; без `tomorrow` — как сейчас.
- [ ] Step 2: FAIL. Step 3: реализовать. Step 4: `pytest`, `npm test`, `npm run lint`, `npm run build` → PASS.
- [ ] Step 5: commit `feat: «Сегодня» shows tomorrow's limit on the salary day`.
