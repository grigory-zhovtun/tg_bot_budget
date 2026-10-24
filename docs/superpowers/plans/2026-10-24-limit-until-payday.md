# Лимит дня до зарплаты — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** лимит дня считается по сумам до ближайшей зарплаты в сумах (минус платежи до неё), план — одной строкой «Сверх плана на X за N дней»; так — утро, /today, строка после записи, «Сводка → Сегодня», график «Траты по дням», советник.

**Architecture:** расчёт — `app/services/day_budget.py` (`day_budget`: новый `limit_on`, поля `sums`, `bills`, `payday`, `payday_name`, `plan_gap`; без `planned_balance`); подписки — `find_series` + `outside_list` из `app/services/recurring.py` (импорт внутри функции: recurring сам импортирует day_budget); тексты — `rich_day_budget`, `format_day_budget`, `rich_today_line`, `today_line` там же, `app/services/plan_advice.py` `_now`; API — `app/web/api.py` `dashboard_out`, `app/web/schemas.py`; Mini App — `webapp/src/dashboard/DashboardScreen.tsx` (`Today`, без карточки «На картах»), `webapp/src/api.ts`, `webapp/src/mocks/fixtures.ts`.

**Tech Stack:** Python 3.12+ (pandas), pytest; React 19 + TS + Tailwind 4, vitest.

**Spec:** `docs/superpowers/specs/2026-10-24-limit-until-payday-design.md`

## Global Constraints

- Деньги — сумы (строки fact в UZS) на вечер вчера; зарплата — открытая к утру строка списка с плюсом в UZS с днём позже сегодня, иначе первая такая строка в следующем месяце, иначе — конец месяца; дней — до дня перед зарплатой (до конца месяца — с сегодняшним).
- Платежи — открытые строки списка с минусом в UZS до дня перед зарплатой (и опоздавшие) + подписки в UZS вне списка, не отменённые, не списанные в месяце до сегодня, с днём в сроке.
- Тексты — как в спеке §3 (числа через `whole`/`money`); «до аванса» — если в названии строки «аванс».
- Только выдуманные суммы в тестах.

## Review Focus

1. Зарплата сегодня, ещё не пришла — срок до следующей; пришла вчера — со следующего утра считает её сумму — тест в задаче 1.
2. После последней зарплаты месяца — до аванса следующего месяца, подписки следующего месяца в сроке вычитаются — тест в задаче 1.
3. Подписка, уже списанная в этом месяце, и подписка в долларах — не вычитаются — тест в задаче 1.
4. «Вчера … из …» и черты графика — лимит своего дня по тому же правилу — тест в задаче 1.
5. Лимита нет (сумы меньше платежей) — тексты утра, /today, после записи и карточки «Можно сегодня» — тесты в задачах 2 и 3.

---

### Task 1: Расчёт лимита до зарплаты

**Files:** Modify `app/services/day_budget.py`; Test `tests/test_day_budget.py` (новые ожидания лимита, срок, платежи, `plan_gap`).

- [ ] **Step 1: failing tests** — 1-го: сумы 5 млн, аванс 15-го → `payday` 15.10, `days_left` 14, лимит 5 млн / 14; перерасход 1-го 400 000 → 2-го 4,6 млн / 13, `yesterday_limit` 5 млн / 14; история лимитов по дням; в день аванса до прихода — до зарплаты 30-го (15 дней); аванс пришёл 15-го → 16-го до 30-го; обе пришли → 31-го до 15.11 (15 дней); без строк с плюсом в UZS — до конца месяца с сегодняшним; строка списка с минусом в UZS до зарплаты вычитается, после — нет; подписка в UZS (две прошлые оплаты того же магазина) в сроке вычитается, списанная в этом месяце и в долларах — нет; подписка следующего месяца в сроке после последней зарплаты — вычитается; сумы меньше платежей — лимит меньше нуля; `plan_gap` = траты дня с 1-го по вчера − план в день × дней, 1-го — `None`; `bills`, `sums`, `payday_name`.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS (тексты — в задаче 2: временно правим их ожидания там же, где падают).
- [ ] **Step 5: commit** — `feat: the daily limit counts the sums until the next salary`.

### Task 2: Тексты бота и советника

**Files:** Modify `app/services/day_budget.py` (`rich_day_budget`, `format_day_budget`, `rich_today_line`, `today_line`), `app/services/plan_advice.py`; Test `tests/test_day_budget.py`, `tests/test_rich_morning.py`, `tests/test_rich_replies.py`, `tests/test_plan_feedback.py`, `tests/test_plan_advice.py`.

- [ ] **Step 1: failing tests** — утро: «💸 X сум», «можно тратить в день до аванса 15-го», «на сумовых картах X, платежи до 15-го Y» (без платежей — только сумы), строка плана («⚠️ Сверх плана на X за 1 день» / «✓ Запас …» / нет 1-го), «⚠️ Вчера A из B» + «больше на C» / «✅ … уложились, запас …»; нет «Отстаём», «по плану месяца»; лимита нет — «💸 Лимита на сегодня нет» + «до аванса 15-го не хватает X»; зарплата в следующем месяце — «до аванса 15 ноября»; без строк зарплаты — «до конца месяца»; /today — «осталось на сегодня»; после записи — «💸 На сегодня осталось A из B» и без лимита «до аванса 15-го не хватает X»; цель — без «лимит ниже»; советник — «Можно в день до аванса 15-го: X UZS».
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: the morning brief says the limit until the salary and the plan in one line`.

### Task 3: Mini App «Сегодня»

**Files:** Modify `app/web/schemas.py`, `app/web/api.py`, `webapp/src/api.ts`, `webapp/src/mocks/fixtures.ts`, `webapp/src/dashboard/DashboardScreen.tsx`; Test `tests/test_web_api.py` (поля), `webapp/src/dashboard/DashboardScreen.test.tsx`.

- [ ] **Step 1: failing tests** — API: `sums`, `bills`, `payday`, `payday_name`, `days_left`; `planned_balance` — null (старое приложение прячет карточку). Экран «Сегодня»: крупно — осталось на сегодня, «лимит X в день до зарплаты 30-го», «потрачено сегодня Y», «на сумовых картах X, платежи до 30-го Y», строка плана как в «Тратах»; нет карточки «На картах»; лимита нет — «⚠️ Лимита нет», «до зарплаты 30-го не хватает X».
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run`, `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: «Сегодня» shows the limit until the salary`.

### Task 4: Документация и проверка

- [ ] README и CLAUDE.md — лимит до зарплаты; живая проверка на выгрузке (вне репозитория) — число как в макете 3; снимки «Сегодня» и «Траты» (`--mode mock`, 360/320, ×1,25).
- [ ] `npm run lint`, `npm run build`, `npx vitest run`, `pytest -q`, black, ruff — чисто; commit `docs: the daily limit until the salary`.
