# Лимит до основной зарплаты (этап 1а) — план

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** лимит дня считается до основной зарплаты (ЗП), аванс в сроке прибавляется к деньгам, страховка не даёт потратить деньги аванса раньше, чем он придёт; утро, /today, «Сводка» и ячейка P5 вкладки — по одному правилу.

**Architecture:** правило живёт в `app/services/day_budget.py` (`allowance_on`): новые `_payday` по строкам основной зарплаты, `_advances` и страховка. Поля аванса уходят в `DayBudget`, оттуда — в строку денег сообщений, API «Сводки» и таблицу «откуда» в Mini App. Ячейка P5 вкладки повторяет то же правило формулой; подписки по день аванса бот пишет в R5 рядом с Q5.

**Tech Stack:** Python 3.12, pandas, pytest; React 19 + TypeScript + Tailwind 4, vitest; Google Sheets (gws для разовой правки формулы).

**Spec:** `docs/superpowers/specs/2026-10-30-clear-money-design.md` — разделы 1.1–1.3, 2.1 (таблица «откуда»), 2.4 (строка денег), 2.5 (P5, Q5, R5). Остальное из этапа 1 спеки (1.4 «с завтра», новый расчёт «Сумы», строка в ответе на ЗП) — этап 1б, отдельный план.

## Global Constraints

- Основная зарплата — строка списка с плюсом в UZS и «зарплат» или «ЗП» в названии, без «аванс»; аванс — с «аванс». Нет строк основной зарплаты — срок по строкам аванса, как до правки, авансы не прибавляются.
- Срок — по день основной зарплаты включительно; основная зарплата сегодня или опоздавшая — не ждём (как сейчас).
- Авансы срока — открытые строки аванса этого месяца с днём **от сегодня** (аванс сегодня — в счёт: его день живём на то, что есть, иначе лимит проседает на день; правка по финальному ревью) и раньше конца срока (непришедшая часть) плюс строки аванса, перенесённые в следующий месяц тем же днём (31-е — последний день), с днём раньше конца срока (целиком).
- Лимит = (сумы + авансы − платежи) / дней. Страховка: есть авансы — второй расчёт по день первого аванса включительно без авансов: (сумы − платежи по тот день) / дней; меньше — берём его целиком (срок, платежи, название — аванса).
- Числа целиком, без «млн» в приложении; в бот-сообщениях — как сейчас (`whole`/`money`).
- Публичный репозиторий: в тестах, фикстурах, PR и документах — только выдуманные суммы и карты.
- Вид «Календаря» не меняется.
- Формулу P5 бот не пишет — её ставит разовый скрипт (Task 5); подпись строки должна начинаться с «Лимит на сегодня» (по ней сверка находит строку).

## Review Focus

1. Срок через границу месяца: 31-е после ЗП 30-го — аванс следующего месяца входит целиком, платежи и подписки следующего месяца — тоже; формула P5 даёт то же число.
2. Аванс сегодня, ещё не пришёл — прибавляется (страховка — один сегодняшний день), лимит не проседает; аванс пришёл частично — прибавляется только непришедшая часть.
3. Страховка срабатывает при крупных платежах до аванса: текст «до аванса 15-го», таблица без строки аванса, P5 = тот же MIN.
4. Нет строк основной зарплаты — поведение до правки, без авансов: старые тесты такого списка не меняются.
5. Q5/R5: бот пишет подписки по день основной зарплаты в Q5 и по день аванса в R5 (без аванса — пусто), не затирая другие ячейки; старая подпись в R5 перестаёт мешать формуле (`N()`).

---

### Task 1: Срок до основной зарплаты, авансы и страховка

**Files:**
- Modify: `app/services/day_budget.py` (константы у `SALARY`, `_payday`, новый `_advances`, `Allowance`, `allowance_on`, поля `DayBudget`, docstring модуля)
- Test: `tests/test_day_budget.py`

**Interfaces:**
- Produces: `Allowance(limit, sums, bills, subscriptions, payday, payday_name, days, advances=0.0, advance_on=None, salary_subscriptions=0.0, advance_subscriptions=None)`; `DayBudget` получает `advances: float = 0.0`, `advance_on: date | None = None`, `salary_subscriptions: float = 0.0`, `advance_subscriptions: float | None = None`. `subscriptions`/`bills`/`payday` — по выбранному сроку (основному или страховке). `salary_subscriptions` — подписки по день основной зарплаты всегда; `advance_subscriptions` — по день первого аванса срока, `None` — авансов в сроке нет.

- [ ] **Step 1: тесты (выдуманные суммы, фикстуры файла: 5 000 000 сумов на 1-е, «Аванс» 15-го 2 000 000, «ЗП» 30-го 10 000 000, «Квартплата» 18-го −400 USD)**

```python
def test_first_day_limit_lasts_until_the_salary_with_the_advance() -> None:
    first = budget(date(2026, 10, 1))
    assert (first.payday, first.payday_name, first.days_left) == (date(2026, 10, 30), "ЗП", 30)
    assert (first.advances, first.advance_on) == (2_000_000, date(2026, 10, 15))
    assert first.limit == pytest.approx(7_000_000 / 30)

def test_the_guard_keeps_the_advance_money_until_it_comes() -> None:
    # перед авансом платёж 4 600 000 в сумах: до 15-го остаётся 400 000 на 15 дней
    day = budget(date(2026, 10, 1), items=ITEMS + [("Ремонт", 10, -4_600_000, "UZS")])
    assert (day.payday, day.payday_name, day.days_left) == (date(2026, 10, 15), "Аванс", 15)
    assert (day.advances, day.bills) == (0, 4_600_000)
    assert day.limit == pytest.approx(400_000 / 15)

def test_the_advance_of_next_month_counts_after_the_salary() -> None:
    # 31-е: ЗП 30-го пришла — срок по 30 ноября, аванс 15 ноября целиком
    ...  # payday 2026-11-30, advance_on 2026-11-15, advances 2_000_000, days 31

def test_an_advance_due_today_is_not_counted_until_it_comes() -> None:
    day = budget(date(2026, 10, 15))  # аванс ещё не пришёл
    assert day.advances == 0 and day.payday == date(2026, 10, 30)

def test_without_a_salary_row_the_advance_sets_the_horizon() -> None:
    day = budget(date(2026, 10, 1), items=[("Аванс", 15, 2_000_000, "UZS")])
    assert (day.payday, day.advances) == (date(2026, 10, 15), 0)
```

Ожидания старых тестов, которые считали «до аванса», пересчитать по новому правилу (каждое — по формуле в комментарии теста, как в PR #96).

- [ ] **Step 2: прогнать — новые тесты падают на отсутствии полей `advances`/`advance_on`, старые — на сроке.** Run: `venv/bin/pytest tests/test_day_budget.py -q` Expected: FAIL.
- [ ] **Step 3: реализовать** `MAIN_SALARY`/`ADVANCE` (regex: `зарплат|(?<![а-яё])зп(?![а-яё])` и `аванс`), `_payday` по основным строкам (нет их — по строкам аванса), `_advances(items, receipts, day, until) -> tuple[float, date | None, str]`, страховку в `allowance_on`; поля в `DayBudget`.
- [ ] **Step 4: прогнать файл и весь Python-набор, пересчитать ожидания остальных файлов (`test_rich_morning.py`, `test_plan_feedback.py`, `test_plan_advice.py`, `test_web_api.py`, `test_dashboard.py`) — каждое изменение объяснимо новым сроком.** Run: `venv/bin/pytest -q` Expected: PASS.
- [ ] **Step 5: commit** `feat: the daily limit runs to the salary with the advance added`

### Task 2: Строка денег в сообщениях

**Files:** Modify `app/services/day_budget.py` (`_money_line`, помощник дня `_day_text(day, today)` для `payday_on`); Test: `tests/test_rich_morning.py`, `tests/test_day_budget.py`.

**Interfaces:** Consumes `DayBudget.advances`, `advance_on` (Task 1).

- [ ] **Step 1: тест** — утро с авансом в сроке: строка «на сумовых картах 5 000 000, аванс 15-го +2 000 000» (и «, платежи до 30-го X», если платежи есть); без аванса — как сейчас.
- [ ] **Step 2: прогнать — FAIL** (`venv/bin/pytest tests/test_rich_morning.py -q`).
- [ ] **Step 3: реализовать** в `_money_line`: после сумов — `, аванс {день} +{сумма}` при `advances > 0`; день — «15-го» в этом месяце, «15 ноября» в следующем.
- [ ] **Step 4: прогнать весь набор — PASS.**
- [ ] **Step 5: commit** `feat: the money line names the advance`

### Task 3: R5 — подписки по день аванса

**Files:** Modify `app/services/receipts_sync.py` (запись строки «Лимит на сегодня»: Q — `salary_subscriptions`, R — `advance_subscriptions` или пусто); Test: `tests/test_receipts_sync.py`.

**Interfaces:** Consumes `DayBudget.salary_subscriptions`, `advance_subscriptions` (Task 1).

- [ ] **Step 1: тест** — в одном запросе пишутся Q и R строки «Лимит на сегодня»; без аванса R — пустая строка.
- [ ] **Step 2: прогнать — FAIL.**
- [ ] **Step 3: реализовать** (RAW, тот же batch, что пишет T и Q).
- [ ] **Step 4: прогнать весь набор — PASS.**
- [ ] **Step 5: commit** `feat: the bot writes the subscriptions until the advance`

### Task 4: Таблица «откуда» в «Сводке»

**Files:** Modify `app/web/schemas.py`, `app/web/api.py` (`dashboard_out`: `advances`, `advance_on`, `payday` — ISO), `webapp/src/api.ts`, `webapp/src/dashboard/DashboardScreen.tsx` (`Today`: вместо строки `sumsLine` — `Table`), `webapp/src/mocks/fixtures.ts`; Test: `tests/test_web_api.py`, `webapp/src/dashboard/DashboardScreen.test.tsx`.

**Interfaces:** Consumes Task 1 fields.

- [ ] **Step 1: тесты** — API отдаёт `advances`, `advance_on`, `payday`; карточка «Можно сегодня» показывает таблицу: «сумы утром | X», «аванс ДД.ММ | +Y» (если есть), «платежи до ДД.ММ | −Z» (если больше нуля; без зарплаты — «платежи до конца месяца»), «на N дней | X+Y−Z»; строка «лимит … в день до …» и «потрачено сегодня» остаются; «Лимита нет» — тоже с таблицей.
- [ ] **Step 2: прогнать — FAIL** (`venv/bin/pytest tests/test_web_api.py -q`, `npx vitest run src/dashboard/DashboardScreen.test.tsx`).
- [ ] **Step 3: реализовать.**
- [ ] **Step 4: весь набор Python и webapp, `npm run lint`, `npm run build` — PASS.**
- [ ] **Step 5: commit** `feat: «Сегодня» shows where the limit comes from`

### Task 5: Формула P5 и документы

**Files:** Modify `README.md`, `CLAUDE.md` (правило лимита); разовый скрипт вне репозитория — P5 и O5 во вкладках текущего и следующего месяца (после выкладки).

- [ ] **Step 1: README и CLAUDE.md** — правило лимита: до основной зарплаты, аванс прибавляется, страховка; R5. Commit `docs: the limit runs to the main salary`.
- [ ] **Step 2: после выкладки** — бэкап O5:R5 обеих вкладок, запись новой P5 и подписи O5 (`valueInputOption=USER_ENTERED`, локаль `;`):

```
=IF(DAY($M$2)<DAY($P$1);"—";ARRAYFORMULA(LET(td;$P$1;qs;$Q$12:$Q$19;cur;$R$12:$R$19;ds;$P$12:$P$19;nm;LOWER($O$12:$O$19);got;IF($T$12:$T$19="";0;$T$12:$T$19);isopen;ABS(got)<0,9*ABS(qs);dues;IF(DATE(YEAR(td);MONTH(td);ds)>$M$2;$M$2;DATE(YEAR(td);MONTH(td);ds));plus;(qs>0)*(cur="UZS");isadv;plus*REGEXMATCH(nm;"аванс");ismain;plus*REGEXMATCH(nm;"зарплат|(^|[^а-яё])зп([^а-яё]|$)")*(1-REGEXMATCH(nm;"аванс"));hasmain;SUM(ismain)>0;salary;IF(hasmain;ismain;isadv)>0;here;IFERROR(MIN(FILTER(ds;salary;isopen;dues>td));0);firstday;IFERROR(MIN(FILTER(ds;salary));0);payday;IF(here>0;MIN(DATE(YEAR(td);MONTH(td);here);$M$2);IF(firstday>0;MIN(DATE(YEAR($M$2);MONTH($M$2)+1;firstday);EOMONTH($M$2;1));$M$2));until;payday+1;sums;SUMIFS(fact!$D:$D;fact!$G:$G;"UZS";fact!$A:$A;"<"&td;fact!$B:$B;"💰 ДОХОДЫ")-SUMIFS(fact!$D:$D;fact!$G:$G;"UZS";fact!$A:$A;"<"&td;fact!$B:$B;"<>💰 ДОХОДЫ");nexts;IF(DATE(YEAR($M$2);MONTH($M$2)+1;ds)>EOMONTH($M$2;1);EOMONTH($M$2;1);DATE(YEAR($M$2);MONTH($M$2)+1;ds));bills;SUMPRODUCT((qs<0)*(cur="UZS")*isopen*(dues<until)*(got-qs));later;IF(until>$M$2+1;SUMPRODUCT((qs<0)*(cur="UZS")*(nexts<until)*(-qs));0);adv;IF(hasmain;SUMPRODUCT(isadv*isopen*(dues>=td)*(dues<until)*(qs-got))+IF(until>$M$2+1;SUMPRODUCT(isadv*(nexts<until)*qs);0);0);advon;MIN(IFERROR(FILTER(dues;isadv*isopen*(dues>=td)*(dues<until)>0);DATE(9999;1;1));IF(until>$M$2+1;IFERROR(FILTER(nexts;isadv*(nexts<until)>0);DATE(9999;1;1));DATE(9999;1;1)));main;(sums+adv-bills-later-N($Q$5))/(until-td);guntil;advon+1;gbills;SUMPRODUCT((qs<0)*(cur="UZS")*isopen*(dues<guntil)*(got-qs))+IF(guntil>$M$2+1;SUMPRODUCT((qs<0)*(cur="UZS")*(nexts<guntil)*(-qs));0);guard;(sums-gbills-N($R$5))/(guntil-td);IF(adv>0;MIN(main;guard);main))))
```

  Подпись O5: «Лимит на сегодня (с утра; сумы плюс аванс минус платежи по день зарплаты, на дни по неё включительно; Q — подписки по день зарплаты, R — по день аванса, пишет бот)».
- [ ] **Step 3: сверка** — после записи Q5/R5 ботом P5 обеих вкладок = утренний лимит бота на живых данных (скрипт вне репозитория), до сума.
