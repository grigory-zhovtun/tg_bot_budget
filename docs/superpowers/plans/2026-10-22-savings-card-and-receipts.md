# Короткая карточка «Накоплений», поступления по плану месяца, «Календарь» без лишнего — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** поступления списка набираются из всех доходов месяца по порядку строк; первая карточка «Накоплений» — коротко с полосой отложенного; «Календарь» без строки «обычные траты» и блока «Записи»; «Траты» без подписи.

**Architecture:** сверка — `app/services/receipts.py` (`match_receipts`, часть поступлений); поле `saved` — `app/web/api.py` `savings_screen_out` + `SavingsMonthOut`; экран — `webapp/src/dashboard/SavingsTab.tsx` (`SetAside`), `webapp/src/forecast/` (`DayCard`, `view.ts`, `ForecastScreen`, удалить `RecordsList.tsx`), `dashboard/CategoryChart.tsx`.

**Tech Stack:** Python 3.12+ (pandas), pytest; React 19 + TS + Tailwind 4, vitest.

**Spec:** `docs/superpowers/specs/2026-10-22-savings-card-and-receipts-design.md`

## Global Constraints

- Платежи списка — без изменений (одна трата ± 10 %, окно 5 дней).
- Закрыта строка — при 90 % плана (`DONE_SHARE`), как сейчас.
- Текст карточки «Накоплений» — как в спеке §2, числа через `money`; окно процента не меняется.
- Только выдуманные суммы в тестах.

## Review Focus

1. Две строки одной валюты (аванс 15-го, зарплата 30-го): доход 17-го после пришедшего аванса — в зарплату, не в аванс — тест в задаче 1.
2. Доход больше всех строк валюты — не теряется, ложится в последнюю — тест в задаче 1.
3. Дневной лимит и «Впереди» не считают пришедшее дважды: ранний приход уменьшает «впереди» — тест в задаче 1 (`day_budget`).
4. Копилка в долларах: `saved` в сумах по курсу таблицы — тест в задаче 2.
5. Нет поступлений / отложено больше нужного / есть доля подушки — три состояния карточки — тесты в задаче 2.

---

### Task 1: Поступления по плану месяца

**Files:** Modify `app/services/receipts.py`; Test `tests/test_receipts.py` (+ правки ожиданий в `tests/test_day_budget.py`, `tests/test_receipts_sync.py`, если они полагались на окно).

- [ ] **Step 1: failing tests** — `test_an_early_income_counts_toward_the_months_plan` (UZS 2 000 000 9-го → аванс пришёл); `test_half_of_a_salary_a_week_early_counts` (RUB 47 000 21-го → строка 28-го: пришло 47 000, `remaining` 53 000 при плане 100 000); `test_an_income_over_a_line_flows_to_the_next` (UZS 2 500 000 15-го → аванс 2 000 000, зарплата 500 000); `test_after_the_advance_an_income_goes_to_the_salary` (аванс 15-го 2 000 000, доход 17-го 3 000 000 → зарплата 3 000 000); `test_more_than_the_month_lands_on_the_last_line` (RUB 150 000 → строка 28-го 150 000); старый `test_the_window_opens_five_days_before_the_row` — удалить (окна для поступлений больше нет).
- [ ] **Step 2–4:** FAIL → implement (поступления: по валюте, строки по дню и номеру, доходы по дню; заполнение по порядку, перелив, последняя берёт остаток; `EARLY_DAYS` — только платежи) → `pytest -q` PASS.
- [ ] **Step 5: commit** — `fix: list incomes fill the month's lines in order, without a window`.

### Task 2: Короткая карточка «Накоплений»

**Files:** Modify `app/web/schemas.py` (`SavingsMonthOut.saved`), `app/web/api.py` (`savings_screen_out`), `webapp/src/api.ts` (`SavingsMonth.saved`), `webapp/src/mocks/fixtures.ts`, `webapp/src/dashboard/SavingsTab.tsx`; Test `tests/test_web_piggy.py`, `webapp/src/dashboard/SavingsTab.test.tsx`.

- [ ] **Step 1: failing tests** — сервер: `month` содержит `saved`; копилка в сумах отложила 400 000 → `saved` 400 000; копилка в долларах 100 $ при курсе 12 000 → 1 200 000. Экран: «Откладываем 25 %» и «Изменить»; «за октябрь отложить 4 000 000»; полоса (`role="progressbar"`, `aria-valuenow` — доля); `left > 0` — «отложено 1 000 000, ещё 3 000 000» и «Отложил…»; `left = 0` — «✓ Отложено 5 000 000 из 4 000 000», кнопки нет; доля подушки — «подушке — 300 000»; предупреждения одной строкой: «⚠️ К срокам нужно 30 %», «⚠️ На жизнь не хватит 2 200 000», «⚠️ В плане «Отложить» 2 000 000, нужно 7 200 000»; нет строк «чтобы успеть все копилки», «останется …», «поправьте …», «✓ Успеваем …», «✓ На жизнь хватает».
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` и `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: a short savings card with a progress bar`.

### Task 3: «Календарь» и «Траты» без лишнего

**Files:** Modify `webapp/src/forecast/DayCard.tsx`, `webapp/src/forecast/view.ts`, `webapp/src/forecast/ForecastScreen.tsx`, `webapp/src/dashboard/CategoryChart.tsx`; Delete `webapp/src/forecast/RecordsList.tsx`; Test `forecast/ForecastScreen.test.tsx`, `forecast/view.test.ts`, `dashboard/CategoryChart.test.tsx`.

- [ ] **Step 1: failing tests** — карточка дня (сегодня и будущий день) без текста «обычные траты»; в календаре нет заголовка «Записи» и кнопки «＋ Новая»; запись правится из карточки дня (нажатие на запись дня открывает окно записи); «Траты» без «полоса — потрачено, черта — план».
- [ ] **Step 2–4:** FAIL → implement (убрать `ordinary` и его тесты, `RecordsList`) → `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: the calendar without records list and the ordinary line`.

### Task 4: Документация и проверка

- [ ] README и CLAUDE.md: сверка поступлений по плану месяца, карточка «Накоплений», «Календарь» без «Записей».
- [ ] `npm run lint`, `npm run build`, `npx vitest run`, `pytest -q`, black, ruff — чисто; commit `docs: savings card, receipts by the month's plan, calendar`.
