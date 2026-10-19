# «Сводка» Mini App крупно — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** экран «Сводка» (шапка, разделы «Сегодня», «Траты», «Накопления», «Впереди») и вкладки приложения — в виде из спеки: главная цифра, крупные строки с пояснениями, плашки статусов, таблицы, числа целиком, без переносов; тестовые данные приложения — выдуманные.

**Architecture:** общие кирпичи — `webapp/src/dashboard/blocks.tsx` (карточка, главная цифра, крупная строка, строка, предупреждение, хорошая новость, плашка, таблица) и форматтеры в `webapp/src/format.ts` по образцу `app/rich.py`; размеры текста и цвета плашек — токены в `webapp/src/index.css`. Компоненты разделов переписывают разметку на кирпичах; данные и ответы сервера не меняются.

**Tech Stack:** React 19, TypeScript, Tailwind CSS 4, vitest + Testing Library; живая сверка — Python (функции сервера) + Playwright (вне репозитория).

**Spec:** `docs/superpowers/specs/2026-10-19-readable-summary-design.md`

## Global Constraints

- Размеры: главная цифра 40 px (`text-big`), крупная строка 21 px (`text-fact`), строки и клетки 17 px (`text-line`), подписи графиков 15 px; плашки — фон `#b42318` / `#8a4f00` / `#146c2e`, белый текст, контраст ≥ 6:1.
- Предупреждение — `⚠️ ` + жирный `text-danger`; хорошая новость — `✓ ` + жирный `text-ok`; пояснения — цветом текста, не `text-hint`.
- Числа только через `format.ts`: `money` (целые), `inCurrency`, `moneyText`, `signedMoney`; неразрывные пробелы, «−»; `short` («млн») в `webapp/src/dashboard` остаётся только в окнах копилок (`OrderSheet`, `PercentSheet`, `PiggySheet`, `SaveSheet`, `piggyText.forecastText/enoughText` для них).
- Окна копилок, экраны «Ввод», «План», «Прогноз» и сервер — не трогать (кроме выдуманных данных в тестах, задача 1).
- `aria-label` регионов («Остатки на картах», «План-факт по категориям», «Подушка безопасности», «Копилки», «План на год», «Норма сбережений», «Откладываем с поступлений», «Сумы») — прежние.
- В тестах, фикстурах, спеке, плане и PR — только выдуманные суммы и карты; перед push — поиск по диффу живых чисел.
- Каждый коммит: `npm test`, `npm run lint`, `npx tsc --noEmit` в `webapp/` — чисто.

## Review Focus

1. Длинные названия карт, копилок, платежей и подписок и суммы от 10 цифр на 360 px и при шрифте 125 % — без переносов и вылезания (живая сверка, задача 7).
2. Валютные суммы: «1 000 $» без копеек и «1 500,25 $» с ними; «−0» не появляется (`moneyText(-0.001, "USD")`) — тест в задаче 2.
3. Светлая тема: плашки, таблицы, предупреждения читаются — тест контраста в задаче 2 и снимки в задаче 7.
4. Группа и статья с планом 0 и тратой 0 — без плашки, «0 вне плана» не пишется (как сейчас: «… вне плана» только при трате) — тест в задаче 4.
5. Пустые случаи: нет карт, групп, трат по дням, списка и подписок, копилок; «Накопления» без вкладки месяца — тексты прежние, раскладка не ломается — тесты в задачах 3–6.

---

### Task 1: Выдуманные данные в тестах и фикстурах приложения

**Files:** Modify `webapp/src/mocks/fixtures.ts`, `webapp/src/App.test.tsx`, `webapp/src/entry/EntryScreen.test.tsx`, `webapp/src/plan/receipt.test.ts`, `webapp/src/plan/draft.test.ts`, `webapp/src/format.test.ts`, `webapp/src/format.ts` (комментарии), `webapp/src/api.ts` (комментарий), `webapp/src/dashboard/DashboardScreen.test.tsx`.

- [ ] **Step 1:** карты источников — «VISA 9090 UZS» и «VISA 3434 USD» (как в тестах бота); платёж списка в фикстуре — «Аренда» −380 USD (−4 560 000 сум); прочие суммы в перечисленных тестах и комментариях — выдуманные (1312,45; 3 150 000; 380; 380,5; 95 000), ожидаемые строки — вслед за ними. Прежние значения в текст коммита и PR не переносить.
- [ ] **Step 2: run** `npm test` → PASS; поиск по `webapp/src` по списку живых чисел и названий (файл вне репозитория, `scratchpad/stage2-live/live-numbers.txt`) → пусто.
- [ ] **Step 3: commit** — `test: invented cards and amounts in the mini app fixtures`.

### Task 2: Форматтеры, размеры, плашки и кирпичи

**Files:** Modify `webapp/src/format.ts`, `webapp/src/format.test.ts`, `webapp/src/index.css`, `webapp/src/theme.test.ts`; Create `webapp/src/dashboard/blocks.tsx`, `webapp/src/dashboard/blocks.test.tsx`.

**Interfaces — Produces:**
- `inCurrency(amount: number, currency: string, cents = false): string` — «380 $», «75 000 ₽», «50 EUR»; UZS — только число.
- `moneyText(amount: number, currency: string): string` — копейки только у дробной суммы не в сумах.
- `signedMoney(amount: number, currency: string): string` — «+2 000 000», «−380 $», «−380,50 $».
- CSS: `text-big` (2.5rem/1.05), `text-fact` (1.3125rem/1.25), `text-line` (1.0625rem/1.35); цвета `chip-bad` `#b42318`, `chip-mid` `#8a4f00`, `chip-good` `#146c2e`.
- `blocks.tsx`: `Card({ label?, children })` — `section`; `Big`, `Fact`, `Line` — `p`; `Warn`, `Good` — `p` с «⚠️ »/«✓ » в тексте; `Chip({ tone: "good" | "mid" | "bad", children })` — `span`; `Table({ rows: ReactNode[][], align: string, head?: ReactNode[], label?: string })` — `table` с `thead` только при `head`, клетки с `align[i] === "r"` — вправо и `whitespace-nowrap`; `Items({ items: { key: string; title: ReactNode; left: ReactNode; right: ReactNode }[], label: string })` — список пунктов в две строки (название жирно во всю ширину, под ним `left` слева и `right` справа, `right` — `whitespace-nowrap`), с рамкой и полосами.

- [ ] **Step 1: failing tests** — `inCurrency(380, "USD") === "380 $"`, `inCurrency(75_000, "RUB") === "75 000 ₽"`, `inCurrency(50, "EUR") === "50 EUR"`, `inCurrency(48_000, "UZS") === "48 000"`, `inCurrency(1312.45, "USD", true) === "1 312,45 $"`; `moneyText(1500.25, "USD") === "1 500,25 $"`, `moneyText(1000, "USD") === "1 000 $"`, `moneyText(5_250_000.4, "UZS") === "5 250 000"`, `moneyText(-0.001, "USD") === "0 $"`; `signedMoney(2_000_000, "UZS") === "+2 000 000"`, `signedMoney(-380, "USD") === "−380 $"`, `signedMoney(-380.5, "USD") === "−380,50 $"`; контраст белого на трёх цветах плашек ≥ 6; `Warn` → текст «⚠️ Нет блока» и класс `text-danger`; `Good` → «✓ …» и `text-ok`; `Chip tone="bad"` → класс `bg-chip-bad` и `text-white`; `Table` без `head` — без `thead`, клетка с `align="r"` — класс `text-right`; `Items` — `list` с подписью `label`, у пункта текст названия, `left` и `right`.
- [ ] **Step 2: run, expect FAIL** (нет экспортов).
- [ ] **Step 3: implement.**
- [ ] **Step 4: run** `npm test` → PASS.
- [ ] **Step 5: commit** — `feat: big-text blocks and number helpers for the summary`.

### Task 3: «Сегодня», шапка и переключатели

**Files:** Modify `webapp/src/dashboard/DashboardScreen.tsx`, `CardBalances.tsx`, `SectionSwitch.tsx`, `Ahead.tsx` (`NoForecastNote`), `webapp/src/App.tsx`; Test `DashboardScreen.test.tsx`, `CardBalances.test.tsx`, `SectionSwitch.test.tsx`.

**Interfaces — Consumes:** задача 2. **Produces:** `NoForecastNote` — «⚠️ Нет блока прогноза» + «лимит на день не посчитать».

- [ ] **Step 1: failing tests** (фикстура): шапка «Сводка · октябрь»; «380 000 сум», «можно потратить сегодня», «лимит 500 000, потрачено 120 000»; сверх лимита (`left_today: -20_000`, `limit: 500_000`) — «−20 000 сум» и «⚠️ сверх лимита дня»; нет лимита (`limit: -50_000`, `days_left: 22`) — «⚠️ Лимита нет», «не хватает 1 100 000», «до плана на конец месяца», «потрачено сегодня 120 000»; «На картах 39 000 000», «утром, по плану 41 500 000», «⚠️ меньше плана на 2 500 000»; больше плана — «✓ больше плана на …»; «🧊 Заморожено 1 000 $», «с 1-го числа −50 $», ноль — «с 1-го числа без изменений»; сумы — «💳 Сумы 2 000 000», «⚠️ хватит до 18 октября», «до 30 октября обменять ≈ 460 $», «не хватит 5 500 000», «в день ≈ 500 000», «в среднем за 14 дней»; хватает — «✓ до конца месяца хватит»; карты — строки таблицы `["VISA 1111", "5 250 000", "✓"]`, `["VISA 2222", "1 500,25 $", "✓ 6 дн."]`, `["VISA 6666", "2 400,50 ₽", "⚠️"]`, под таблицей «⚠️ VISA 6666 в банке 2 450 ₽» и «сверено 01.10», «Всего ≈ 23 613 075 сум»; карта без курса — «без RUB — нет курса»; без прогноза — «⚠️ Нет блока прогноза»; переключатель — сетка `grid-cols-2`; вкладки приложения — `text-base`; заголовки 2-го уровня «Сегодня» — `["Остатки на картах"]`.
- [ ] **Step 2: run, expect FAIL.**
- [ ] **Step 3: implement** (заморожено — своя карточка во всю ширину).
- [ ] **Step 4: run** `npm test` → PASS.
- [ ] **Step 5: commit** — `feat: «Сегодня» in big text with a card table`.

### Task 4: «Траты»

**Files:** Modify `webapp/src/dashboard/CategoryChart.tsx`, `SpendingChart.tsx`; Test `CategoryChart.test.tsx`, Create `SpendingChart.test.tsx`; поправить `DashboardScreen.test.tsx`.

- [ ] **Step 1: failing tests:** карточка «Потрачено 3 842 000», «за 1–10 октября», «план на месяц 5 000 000»; кнопка группы `/🍔 ЕДА.*⚠️ 111 %/`, строка «3 342 000 из 3 000 000»; «🏚️ ДОМ» — плашка «✓ 25 %» (класс `bg-chip-good`); 80–100 % (`plan 1_000_000`, `fact 860_000`) — «86 %» с `bg-chip-mid`; план 0 и трата 600 000 — без плашки, «600 000 вне плана»; раскрытая «🍽️ кафе» — «3 092 000 из 2 700 000» и «⚠️ 115 %»; «полоса — потрачено, черта — план»; не учтено — «⚠️ Нет в плане месяца», строка таблицы `["кафе (USD)", "42 000"]`, «добавьте во вкладке «План»». «Траты по дням»: «✓ Запас 1 100 000» (21 px, `text-ok`), «потрачено 2 950 000 за 9 дней», «по плану 4 050 000»; перерасход — «⚠️ Сверх на 800 000» (`text-danger`); ровно — «✓ Ровно по плану»; подписи графика `font-size` 15; «можно 500 000 в день»; легенда «план 450 000 в день» с классом `text-line`; подпись «можно …» не выходит за `WIDTH` (x + ширина ≤ 320 при `today` на 18-м дне).
- [ ] **Step 2: run, expect FAIL.**
- [ ] **Step 3: implement.**
- [ ] **Step 4: run** `npm test` → PASS.
- [ ] **Step 5: commit** — `feat: «Траты» with plan badges and whole numbers`.

### Task 5: «Накопления»

**Files:** Modify `webapp/src/dashboard/SavingsTab.tsx`, `CushionCard.tsx`, `PiggyCard.tsx`, `YearPlan.tsx`, `SavingsCard.tsx`, `piggyText.ts` (`whenText` — «к 01.06, каждый год»); Test `SavingsTab.test.tsx`, `CushionCard.test.tsx`, `PiggyCard.test.tsx`, `SavingsCard.test.tsx`, Create `YearPlan.test.tsx`; поправить `DashboardScreen.test.tsx` (заголовки).

- [ ] **Step 1: failing tests:** «Откладываем 25 %», «с каждого поступления», кнопка «Изменить процент», «Отложить 4 000 000», «в октябре пришло 16 000 000», таблица `["Новый год", "4 000 000"]` (+ `["подушка", …]` при `cushion > 0`), «⚠️ К срокам нужно 30 %», «чтобы успеть все копилки»; хватает — «✓ Успеваем всё к срокам»; «⚠️ На жизнь не хватит», «останется …», «обычные траты …», «разница …» / «✓ На жизнь хватает» … «запас …»; «⚠️ План месяца не сходится», «в плане «Отложить» 2 000 000», «по копилкам нужно 7 200 000», «поправьте во вкладке «План»»; «⚠️ Процент не читается», «считаю по 25 %». Подушка: заголовок «🛟 Подушка», плашка «⚠️ 0,3 мес.» (`bg-chip-bad`), от 3 мес. — `bg-chip-mid`, цель — «✓ 6,2 мес.» (`bg-chip-good`) и «✓ Цель 6 мес. достигнута»; «1 100 $ ≈ 13 200 000 сум», «траты в месяц 40 000 000», «заморожено 1 500 $», «минус копилки 400 $», «на вечер вчера», «Не хватает ≈ 226 800 000», «до цели 6 месяцев, это 18 900 $»; «⚠️ Копилки больше карты», «разница 200 $», «отложите разницу», «или поправьте копилки». Копилка: «накоплено 400 $ из 2 500 $» (400 $ — `strong`), «к 01.06, каждый год», «⚠️ Не хватит 500 $», «к 01.06 будет 2 000 $ из 2 500 $»; «✓ успеваем» плашкой; «в этом месяце 6 000 000»; взнос сделан — «✓ отложено»; «⚠️ Не указана сумма»; «⚠️ Проверьте строку 4», «на листе «Копилки»»; «⚠️ Срок прошёл», «осталось …»; «⚠️ К сроку не успеть», «не хватает …». План на год: «Октябрь — 10 000 000», «Новый год 6 000 000», «подушка 4 000 000», «✓ Новый год — собрано». Норма: плашка «⚠️ −10 %» / «✓ 25 %» / «5 %», «за июль–сентябрь», «пришло …», «потрачено …»; подписи столбиков — класс `text-[15px]`. Заголовки раздела — `["Откладываем 25 %", "🛟 Подушка", "🐷 Копилки", "План на год", "Норма сбережений"]`.
- [ ] **Step 2: run, expect FAIL.**
- [ ] **Step 3: implement** (суммы копилок — `moneyText`, не `piggyMoney`; окна копилок не трогать).
- [ ] **Step 4: run** `npm test` → PASS.
- [ ] **Step 5: commit** — `feat: «Накопления» in big text with badges`.

### Task 6: «Впереди»

**Files:** Modify `webapp/src/dashboard/Ahead.tsx`; Create `webapp/src/dashboard/Ahead.test.tsx`; поправить `DashboardScreen.test.tsx`.

- [ ] **Step 1: failing tests:** список «Впереди» (`Items`, подпись «Впереди») — пункты `["Аванс", "15-го", "+2 000 000"]`, `["Аренда", "18-го", "−380 $"]`; день прошёл — название «⏳ ЗП» и строка «⏳ — день прошёл, записи ещё нет»; подписки (`Items`, подпись «Подписки») — `["✓ Render.com", "01-го", "312 400"]`, `["⏳ Google One", "14-го", "198 700"]`, валюта — «15,50 $»; «⚠️ » — дважды, «❓ » — не было; расшифровка только встреченных значков отдельными подписями «✓ списано», «⏳ ждём»; название в 25 знаков (выдуманное «Longname Trading Company») остаётся одной строкой-заголовком пункта; пусто — «До конца месяца платежей и подписок нет»; без прогноза — `NoForecastNote`.
- [ ] **Step 2: run, expect FAIL.**
- [ ] **Step 3: implement.**
- [ ] **Step 4: run** `npm test` → PASS.
- [ ] **Step 5: commit** — `feat: «Впереди» as tables`.

### Task 7: Документация и живая сверка

- [ ] README и CLAUDE.md — строка о новом виде «Сводки».
- [ ] Живая сверка вне репозитория (`scratchpad/stage2-live/`): выгрузка листов (`fact`, вкладка месяца, `system`, «Календарь», «Копилки», «Журнал копилок») через `gws`; Python-скрипт собирает ответы `/api/bootstrap`, `/api/dashboard`, `/api/savings` функциями сервера (`build_dashboard` → `dashboard_out`, `AnalyticsService.savings` с подменой листов → `savings_screen_out`); `npm run build`, раздача `dist`, Playwright подменяет `/api/*`; снимки 360 px тёмная/светлая, 320 px, шрифт 125 % — по четырём разделам; проверка: крупные строки, строки пояснений и клетки таблиц не переносятся, ничего не шире экрана. Найденное — правка с `Ruling:` в журнале.
- [ ] `npm run lint`, `npm run build`, `npm test`, `pytest -q` — чисто; commit `docs: the big-text summary screen`.
