# «Ввод» и окна копилок крупно — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** «Ввод» и окна копилок — в крупном виде из спеки; «Прогноз» не меняется; старые документы — без данных из жизни владельца.

**Architecture:** разметка экранов на кирпичах `blocks.tsx` и форматтерах `format.ts`; клавиатура получает параметр `big` (передаёт только «Ввод»); окна копилок меняют свои подписи своими классами, общие `fields.tsx`, `plan/Sheet.tsx`, `plan/AmountPad.tsx` — без изменений.

**Tech Stack:** React 19, TypeScript, Tailwind CSS 4, vitest + Testing Library.

**Spec:** `docs/superpowers/specs/2026-10-19-readable-entry-design.md`

## Global Constraints

- Всё из Global Constraints планов «Сводки» и «Плана».
- Ни одного изменения в `webapp/src/forecast/`, `webapp/src/fields.tsx`, `webapp/src/plan/Sheet.tsx`, `webapp/src/plan/AmountPad.tsx`; `Keypad` без `big` выглядит как сейчас.
- `aria-label` плиток, карт, клавиш и окон — прежние.

## Review Focus

1. Длинные названия групп и подкатегорий и суммы в 8 цифр на плитке 2×2 — без вылезания (живая сверка).
2. Копилка в долларах: «400 $», «2 000,50 $» в окнах.
3. Окно «Отложил» без копилок (всё в подушку) и с несколькими копилками.
4. «Прогноз» не изменился: тесты `forecast/` проходят без правок, файлов `forecast/` нет в диффе.
5. Ответ после записи с ошибкой и «Отменить» — поведение прежнее.

---

### Task 1: Старые документы

- [ ] Скриптом вне репозитория (по приватному списку живых чисел и названий) заменить в `docs/` тестовые суммы и названия выдуманными: суммы — круглыми выдуманными, название подписки — «Longname Trading Company»; описание правила программы про наличные и банк — не трогать. Поиск по `docs/` после замены — пусто, кроме этого правила.
- [ ] commit — `docs: invented amounts in older plans`.

### Task 2: «Ввод»

**Files:** Modify `webapp/src/entry/plan.ts`, `Tiles.tsx`, `SourceChips.tsx`, `Keypad.tsx`, `EntryScreen.tsx`; Test `plan.test.ts`, `Tiles.test.tsx`, `EntryScreen.test.tsx`.

**Interfaces — Produces:** `planLine(value)` → `{ head: string; sum: string; status; filled } | null` («осталось» / «⚠️ сверх на» / «вне плана» и сумма `money`); `Keypad({ decimals, onKey, big? })`.

- [ ] **Step 1: failing tests** — `planLine` на трёх случаях; плитка: «осталось» и «1 500 000» отдельными строками; карты — класс `text-line`; клавиша «5» во «Вводе» — `text-3xl`, у `Keypad` без `big` — прежние классы; «Комментарий» — без `text-hint`; кнопка «Записать 48 000 UZS» — `text-lg`.
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: the entry screen in big text`.

### Task 3: «Отложил», процент и порядок

**Files:** Modify `webapp/src/dashboard/piggyText.ts`, `SaveSheet.tsx`, `PercentSheet.tsx`, `OrderSheet.tsx`; Test `SaveSheet.test.tsx`, `PercentSheet.test.tsx`, `OrderSheet.test.tsx`.

- [ ] **Step 1: failing tests** — «Отложил»: строки таблицы `["Новый год", "4 000 000"]`, подушка — строка `["подушка", …]` и «подушка — в журнал не пишется»; процент: «в месяц ≈ 10 000 000», пункт «Новый год, к 20.12» со строками «будет 12 000 000» и «из 12 000 000 ✓», «⚠️ К срокам нужно 30 %», «⚠️ На жизнь не хватит» и «разница 2 200 000»; порядок: «к 20.12 будет 12 000 000», «из 12 000 000 ✓», название — `text-fact`.
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: piggy windows in big text`.

### Task 4: Окно копилки

**Files:** Modify `webapp/src/dashboard/PiggySheet.tsx`; Test `PiggySheet.test.tsx`.

- [ ] **Step 1: failing tests** — подписи «Название», «Цель», «Срок», «Статья трат» — `text-line` без `text-hint`; «История» — `text-fact`; строка истории с суммой целиком.
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: the piggy window in big text`.

### Task 5: Документация, живая сверка, «Прогноз» не тронут

- [ ] README и CLAUDE.md — строка о крупном «Вводе» и окнах копилок, «Прогноз» — по-прежнему.
- [ ] `git diff --stat main -- webapp/src/forecast webapp/src/fields.tsx webapp/src/plan/Sheet.tsx webapp/src/plan/AmountPad.tsx` — пусто.
- [ ] Живая сверка «Ввода» и окон копилок (360 px тёмная/светлая, 320 px, 125 %).
- [ ] `npm run lint`, `npm run build`, `npx vitest run`, `pytest -q` — чисто; commit `docs: the big-text entry and piggy windows`.
