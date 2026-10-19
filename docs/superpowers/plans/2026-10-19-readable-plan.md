# «План» Mini App крупно — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** вкладка «План» Mini App (месяцы, итог, поступления, совет, статьи, доходы, цель, подсказки, сообщения, окна) — в крупном виде из спеки, на кирпичах и форматтерах «Сводки».

**Architecture:** экран и окна перестраивают разметку на `webapp/src/dashboard/blocks.tsx` (`Card`, `Big`, `Fact`, `Line`, `Warn`, `Good`, `Table`) и `webapp/src/format.ts` (`money`, `moneyText`, `signedMoney`); строки списка поступлений — свой компонент в `webapp/src/plan/` с той же разметкой, что `Items`, но кнопками; черновик, сохранение и сервер не меняются.

**Tech Stack:** React 19, TypeScript, Tailwind CSS 4, vitest + Testing Library; живая сверка — Python (функции сервера) + Playwright, вне репозитория.

**Spec:** `docs/superpowers/specs/2026-10-19-readable-plan-design.md` (продолжает `2026-10-19-readable-summary-design.md`)

## Global Constraints

- Всё из Global Constraints плана «Сводки» (`docs/superpowers/plans/2026-10-19-readable-summary.md`): размеры 40/21/17 px, «⚠️»/«✓», числа только через `format.ts`, без «млн» на «Плане», без цветного текста на полосах, только выдуманные данные в тестах, проверки `npm test`, `npm run lint`, `npx tsc --noEmit`.
- `aria-label` кнопок статей, строк списка и регионов («Итог месяца», «Поступления и платежи», «Доходы по статьям», «Совет по плану», «По копилкам», «Копилки в плане», «Следующий месяц», группы) — прежние: на них держатся тесты поведения.
- Клавиатура (`entry/Keypad.tsx`) и системная кнопка Telegram — без изменений.
- Тексты советника и сообщения-предложения — абзацем (переносы разрешены), остальные строки — без переносов на 360 px.

## Review Focus

1. Длинное название статьи и план в 8 цифр: план уходит строкой ниже и остаётся справа; ничего не шире экрана — живая сверка, задача 5.
2. Статьи в валюте: «500 $» и «500,50 $», исправленный план и подсказка «ср. 500 $, сен 500 $» — тест в задаче 3.
3. Отрицательные: «не хватает» в итоге, цель меньше нуля («💰 из накоплений»), подсказка по копилкам с минусом — тесты в задачах 2–3.
4. Исправленная, новая, конфликтная и «встречается дважды» статья сохраняют поведение (кнопка, `disabled`, `data-conflict`) — тесты в задаче 3.
5. Вне Telegram «Сохранить N изменений» — крупная кнопка; в Telegram — системная, как сейчас — тест в задаче 3.

---

### Task 1: Подписи прихода, сообщения и месяцы

**Files:** Modify `webapp/src/plan/receipt.ts`, `webapp/src/plan/PlanTab.tsx`; Create `webapp/src/plan/notice.ts`; Test `receipt.test.ts`, `PlanTab.test.tsx`, Create `notice.test.ts`.

**Interfaces — Produces:**
- `receiptHint(event, month)` — «✓ пришло 3 150 000, 15.10», «✓ оплачено 380 $, 17.10», «пришло 47 500 ₽ из 95 000 ₽», «⏳ ждём с 30.10» (числа — `moneyText`, без «млн»).
- `noticeParts(text: string): { head: string | null; rest: string }` — по первому «: »: «✅ План сохранён: 2 изменения» → `{ head: "✅ План сохранён", rest: "2 изменения" }`; без «: » — `{ head: null, rest: text }`.

- [ ] **Step 1: failing tests** — подписи прихода в новом виде (выдуманные суммы из `receipt.test.ts`); `noticeParts` на обоих случаях; кнопки месяцев — класс `text-line` и `font-extrabold`.
- [ ] **Step 2: run, expect FAIL.**
- [ ] **Step 3: implement.**
- [ ] **Step 4: run** `npx vitest run` → PASS.
- [ ] **Step 5: commit** — `feat: plan receipts, notices and month buttons in big text`.

### Task 2: Итог, сообщения, поступления и совет

**Files:** Modify `webapp/src/plan/PlanScreen.tsx`, `webapp/src/plan/AdvicePanel.tsx`; Create `webapp/src/plan/PlanEvents.tsx`; Test `PlanScreen.test.tsx`.

**Interfaces — Consumes:** задача 1. **Produces:** `PlanEvents({ items, onPick })` — строки списка кнопками в две строки (+ подпись прихода), `data-conflict` и цвет исправленной — как сейчас.

- [ ] **Step 1: failing tests** — регион «Итог месяца»: «9 500 000 сум» (класс `text-big`), «свободно в октябре», клетки таблицы `["поступления", "22 000 000"]`, `["расходы", "−10 500 000"]`, `["🎯 отложить", "−2 000 000"]`; не хватает — «⚠️ не хватает в октябре»; цель меньше нуля — `["💰 из накоплений", "+…"]`; строки списка — `["Аванс", "15-го", "+5 000 000", "✓ пришло 5 000 000, 15.10"]`; сообщение после сохранения — «✅ План сохранён» (`text-fact`) и «1 изменение»; несохранённые правки — «Несохранённые правки», «остались с прошлого раза»; панель совета — пункт абзацем `text-line`, «Применить: кафе → 2 800 000».
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run` PASS (якоря старых тестов «Свободно» — на регион «Итог месяца»).
- [ ] **Step 5: commit** — `feat: plan totals, list and advice in big text`.

### Task 3: Статьи, доходы, цель, подсказки и пустые случаи

**Files:** Modify `webapp/src/plan/PlanScreen.tsx`, `webapp/src/plan/PiggyHints.tsx`; Test `PlanScreen.test.tsx`.

- [ ] **Step 1: failing tests** — заголовок группы и сумма `text-fact` («🍔 ЕДА», «4 500 000»); статья: «1 500 000» (`text-fact`), «ср. 1 800 000, сен 2 000 000»; валютная — «500 $», «ср. 500 $, сен 500 $»; исправленная — новое цветом ссылки и прежнее зачёркнутым; дважды — «⚠️ статья встречается дважды», «поправьте вкладку», кнопка `disabled`; новая — «новая статья»; доходы не сходятся — «⚠️ Доходы не сходятся», «по статьям 20 000 000», «в поступлениях 22 000 000»; цель — «🎯 Отложить за месяц» и «2 000 000»; по копилкам — «По копилкам −9 800 000», «откладываем 25 % с поступлений», клетки таблицы и кнопка «Применить −9 800 000»; подсказка копилки — «🐷 Из копилки 12 000 000», ««Новый год» в октябре», «заложите в статью», ««ПРАЗДНИК / сезонный»»; «Плана на ноябрь ещё нет» (`text-fact`) и строки; вкладки нет — «⚠️ Вкладки месяца ещё нет»; списка нет — две строки; «Сохранить 1 изменение» вне Telegram — класс `text-lg`.
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: plan articles, goal and hints in big text`.

### Task 4: Окна «Плана»

**Files:** Modify `webapp/src/plan/Sheet.tsx`, `AmountPad.tsx`, `ArticleSheet.tsx`, `AddArticleSheet.tsx`, `EventSheet.tsx`, `GoalSheet.tsx`; Test `sheets.test.tsx`.

- [ ] **Step 1: failing tests** — заголовок окна `text-fact`; валюта у суммы — без `text-hint`; окно статьи: клетки `["было в плане", "1 500 000"]`, `["в среднем", "1 800 000"]`, `["в сентябре", "2 000 000"]`, `["в октябре пока", "900 000"]`, строки «в среднем — за июль–сентябрь», «в октябре — за 19 дней», кнопки «= в среднем 1 800 000» (ставит сумму в тысячах, как сейчас), «= как в сентябре 2 000 000», «↺ как было 1 500 000»; новая статья — то же без «как было»; поступление — подписи полей `text-line` без `text-hint`; цель — «Валюта» `text-line`.
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: plan windows in big text`.

### Task 5: Документация и живая сверка

- [ ] README и CLAUDE.md — строка о крупном «Плане».
- [ ] Живая сверка вне репозитория: `scratchpad/stage2-live/build.py` уже собирает `/api/plan`; снимки вкладки «План» и окна статьи на 360 px тёмная/светлая, 320 px, 125 % (скрипт по образцу `shots/stage2-live.mjs`); переносы и вылезание — правка с `Ruling:`.
- [ ] `npm run lint`, `npm run build`, `npx vitest run`, `pytest -q` — чисто; commit `docs: the big-text plan tab`.
