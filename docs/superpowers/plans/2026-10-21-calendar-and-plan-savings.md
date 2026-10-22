# «Календарь» и «Накопления» во «Плане» — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** вкладка «Прогноз» называется «Календарь», «Накопления» переезжают из «Сводки» вторым разделом во «План», «Сводка» — три раздела в ряд.

**Architecture:** только Mini App (`webapp/src`). Переключатель разделов «Сводки» становится общим (`SectionSwitch` с разделами в параметрах), память выбора — общей (`choice.ts`); «План» получает свой раздел «Накопления» с тем же `SavingsTab`, который сам рисует строку «Накопления · октябрь» с «Обновить». Сервер, бот и вид экранов внутри не меняются.

**Tech Stack:** React 19, TypeScript, Tailwind CSS 4, vitest + Testing Library; снимки — сборка `--mode mock` + Playwright, вне репозитория.

**Spec:** `docs/superpowers/specs/2026-10-21-calendar-and-plan-savings-design.md`

## Global Constraints

- Подписи вкладок: «Ввод», «Сводка», «План», «Календарь»; ширина вкладки — по подписи, ряд — одна строка.
- Заголовок календаря — «Календарь · октябрь», без данных — «Календарь». Остальное в календаре — без изменений.
- «План»: переключатель «План месяца» / «Накопления» (radiogroup «Раздел плана»), по умолчанию «План месяца», выбор помнит телефон; «Октябрь / Ноябрь» — только в «Плане месяца».
- «Накопления» — прежний `SavingsTab` без изменений внутри, над ним «Накопления · октябрь» и «Обновить» (таблица заново).
- «Сводка»: «Сегодня», «Траты», «Впереди» в одну строку; запомненные «Накопления» → «Сегодня»; «Обновить» — только «Сводка».
- `?open=save` → вкладка «План», раздел «Накопления» (и он запоминается), окно «Отложил» — один раз.
- Внутренние имена (`forecast`, `savings`, `dashboard-section`, ключи запросов) не меняются.
- Только выдуманные данные в тестах; проверки `npm test`, `npm run lint`, `npx tsc --noEmit`.

## Review Focus

1. Бот открыл «Отложил», окно закрыли, переключились на «План месяца» или на другую вкладку и вернулись — окно не открывается снова — тесты в задаче 3.
2. Первый запуск по кнопке бота: справочника и черновиков ещё нет — «Накопления» и окно «Отложил» всё равно открываются — тест в задаче 3.
3. Несохранённые правки плана: ушли в «Накопления» и вернулись — черновик и точка на вкладке на месте — тест в задаче 3.
4. Телефон помнит «Накопления» в «Сводке» с прошлой версии — «Сводка» на «Сегодня» — тест в задаче 2.
5. Ряд вкладок с «Календарём» и переключатели при шрифте 125 % и на 320 px не выходят за экран — снимки в задаче 4.

---

### Task 1: «Календарь»

**Files:** Modify `webapp/src/App.tsx`, `webapp/src/forecast/ForecastScreen.tsx`; Test `App.test.tsx`, `forecast/ForecastScreen.test.tsx`.

- [ ] **Step 1: failing tests** — App: вкладки по имени `["Ввод", "Сводка", "План", "Календарь"]` (роль `tab`), вкладки «Прогноз» нет; у `tablist` класс `flex`, у вкладки — `flex-auto` (ширина по подписи); прежние переходы на «Прогноз» в `App.test.tsx` — на «Календарь». ForecastScreen: заголовок уровня 1 — «Календарь · октябрь» (фикстура — октябрь).
- [ ] **Step 2–4:** FAIL → implement (`TABS`: `forecast` → «Календарь»; `nav` — `flex gap-1`, вкладки `flex-auto px-1.5`; заголовок — `monthName(data.today.slice(0, 7))` из `plan/months.ts`, как в «Сводке») → `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: «Календарь» instead of «Прогноз»`.

### Task 2: Общий переключатель и три раздела «Сводки»

**Files:** Create `webapp/src/choice.ts`; Modify `dashboard/SectionSwitch.tsx`, `dashboard/section.ts`, `dashboard/DashboardScreen.tsx`, `App.tsx`; Test `choice.test.ts` (new), `dashboard/SectionSwitch.test.tsx`, `dashboard/section.test.ts`, `dashboard/DashboardScreen.test.tsx`.

**Interfaces — Produces:**
- `readChoice<T extends string>(key: string, allowed: readonly T[], fallback: T): T` — значение из `localStorage`; нет, чужое или память закрыта — `fallback`; `saveChoice(key: string, value: string): void` — память закрыта — молча.
- `SectionSwitch<T extends string>({ label, sections, section, onSection }: { label: string; sections: readonly { name: T; title: string }[]; section: T; onSection: (next: T) => void })` — radiogroup с подписью `label`, все разделы в одну строку (колонок столько, сколько разделов).
- `Section = "today" | "spending" | "ahead"`; `DashboardScreen({ api })` — без `boot` и `openSave`.

- [ ] **Step 1: failing tests** — `readChoice`: нет значения, чужое, исключение доступа — `fallback`; своё — оно. SectionSwitch: radiogroup «Раздел сводки» с радио `["Сегодня", "Траты", "Впереди"]`, класс `grid-cols-3`; с двумя разделами — `grid-cols-2`. section: запомненное `"savings"` → `"today"`. DashboardScreen: заголовки разделов без «Накоплений»; радио «Накопления» нет; `dashboard-section = "savings"` → открыта «Сегодня»; «Обновить» вызывает только `api.dashboard(true)`, не `api.savings`.
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run` PASS (проверки «Накоплений» из `DashboardScreen.test.tsx` переезжают в задачу 3).
- [ ] **Step 5: commit** — `feat: the summary in three sections`.

### Task 3: «План»: «План месяца» и «Накопления»

**Files:** Create `webapp/src/plan/section.ts`; Modify `plan/PlanTab.tsx`, `dashboard/SavingsTab.tsx`, `App.tsx`; Test `plan/PlanTab.test.tsx`, `dashboard/SavingsTab.test.tsx`, `App.test.tsx`.

**Interfaces — Consumes:** `readChoice`, `saveChoice`, `SectionSwitch` (задача 2). **Produces:**
- `PlanSection = "month" | "savings"`; `PLAN_SECTIONS` — «План месяца», «Накопления»; `PLAN_SECTION_KEY = "plan-section"`; `storedPlanSection()`, `storePlanSection(section)`.
- `SavingsTab({ api, boot, openSave? })` — без `refresh`; рисует `h1` «Накопления · октябрь» (без данных — «Накопления») и кнопку «Обновить» / «Обновляю…» (`reload(true)`).
- `PlanTab({ api, boot, months, drafts: Record<string, Draft> | null, restored, onDraft, onRestoredSeen, openSave, onSaveSeen })` — раздел из памяти или «Накопления» при `openSave`; «План месяца» — только при `drafts !== null` и непустых `months`; `onSaveSeen()` — при смене раздела.
- App: `openSave` — состояние, гасится при смене вкладки и через `onSaveSeen`; при `openSave` начальная вкладка — «План»; `PlanTab` рисуется без ожидания справочника и черновиков.

- [ ] **Step 1: failing tests** — PlanTab: radiogroup «Раздел плана», отмечен «План месяца», есть radiogroup «Месяц»; «Накопления» — «Откладываем 25 %», заголовки уровня 2 `["Откладываем 25 %", "🛟 Подушка", "🐷 Копилки", "План на год", "Норма сбережений"]` (из `DashboardScreen.test.tsx`), radiogroup «Месяц» нет, `localStorage["plan-section"] = "savings"`, после перемонтирования отмечены «Накопления»; правка статьи в «Плане месяца» → «Накопления» → назад — «Сохранить 1 изменение» на месте; `drafts={null}` + «Накопления» — карточки есть; `openSave` — окно «Отложил», после закрытия и перехода «План месяца» → «Накопления» окна нет. SavingsTab: «Накопления · октябрь»; «Обновить» → `api.savings(true)`. App: `?open=save` — выбрана вкладка «План», отмечены «Накопления», окно «Отложил»; закрыли окно, ушли на «Сводку» и вернулись — окна нет.
- [ ] **Step 2–4:** FAIL → implement → `npx vitest run` PASS.
- [ ] **Step 5: commit** — `feat: «Накопления» as the second section of «План»`.

### Task 4: Тексты, документация и снимки

- [ ] Комментарии и docstrings, где «Прогноз» — название вкладки или экрана (`dashboard/controls.tsx`, `dashboard/section.ts`, `forecast/view.ts`, `app/services/runway.py`, `app/services/day_budget.py`, `app/services/analytics_service.py`) — «Календарь»; README и CLAUDE.md — вкладки «Ввод / Сводка / План / Календарь», «Сводка» в три раздела, «Накопления» во «Плане», кнопка бота открывает «План → Накопления».
- [ ] Снимки сборки `--mode mock` (скрипт вне репозитория): ряд вкладок, «Сводка», «План» в обоих разделах и «Календарь» на 360 px в тёмной и светлой теме, 320 px, шрифт 125 % — ничего не шире экрана, на 360 px при обычном шрифте подписи вкладок и переключателей не переносятся.
- [ ] `npm run lint`, `npm run build`, `npx vitest run`, `pytest -q` — чисто; commit `docs: «Календарь» and «Накопления» in «План»`.
