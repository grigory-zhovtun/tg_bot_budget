# Копилки: процент с поступлений и порядок важности — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Одна настройка «Откладываем N % с поступлений» и порядок важности копилок вместо фиксированного плана взносов и «10 % в подушку». Экран — по варианту 1; после поступления бот шлёт одно «🐷».

**Architecture:**
- **Модель** — `app/services/piggy.py`:
  - процент (Q1) и порядок (колонка O) читаются из листа «Копилки»;
  - план — сначала «успеет к сроку» по порядку важности от мощности E = P × поступления плана, потом прежние ступеньки от этих сумм;
  - «отложить сейчас» — раскладка P × пришедшего за месяц.
- **API** отдаёт то же через `GET /api/savings`, плюс предпросмотр `?percent=&order=` и две правки.
- **Бот** шлёт одно «🐷» после поступления.
- **Приложение:** карточка месяца, окна «Процент» и «Порядок», номера на копилках.

**Tech Stack:** Python 3.12, Starlette, python-telegram-bot, gspread, pandas, pydantic; React 19, TypeScript strict, Tailwind 4, Vitest.

**Spec:** `docs/superpowers/specs/2026-10-18-piggy-percent-design.md` (дополняет `2026-10-17-piggy-banks-design.md`).

## Global Constraints

- **Публичный репозиторий.** В коде, тестах, фикстурах и документах — только условные суммы и имена (Иван, Мария); нет банков, окончаний карт, реальных сумм. Перед push — grep диффа.
- **Python.** Type hints; `../venv_dev/bin/black . && ../venv_dev/bin/ruff check . && ../venv_dev/bin/pytest -q` перед коммитом.
- **Webapp.** В `webapp/`: `npx prettier --write src && npm run lint && npm test && npm run build`. Через `rtk proxy …`, смотреть код выхода. `webapp/.vitest/` удалить, если появился.
- **Процент P:**
  - в коде — доля (`0.05`);
  - в листе Q1 и в API — проценты (`5`, `2.5`), от 0 до 100, не больше одного знака после запятой;
  - пусто — `config.SAVE_SHARE`; не число или вне 0–100 — тоже `SAVE_SHARE`, и `percent_source = "bad"`.
- **Лист «Копилки»:**
  - O1 «Порядок»; P1 «Откладываем с поступлений, %»; Q1 — число;
  - новые листы — 17 колонок; лист уже 17 — `add_cols` перед записью O или P1:Q1;
  - значения — RAW.
- **Порядок:**
  - в нём участвуют открытые копилки с целью, сроком и без ошибки;
  - с номером в O (целое ≥ 1) — по номеру, равные — по строке листа;
  - без номера — потом, по ближайшему сроку;
  - нужды разбираются слоями: ближайшие сроки всех копилок по порядку, потом сроки следующего года в том же порядке.
- **Мощность** E = P × поступления (+) жёлтого списка текущей вкладки по курсу таблицы. Поступлений в списке нет — средний доход за 6 полных месяцев (`month_savings`). P\* — первая ступенька плана от полных нужд / база, вверх до целого процента.
- **Раскладка суммы** (A месяца и «Отложил…»):
  1. копилки со сроком по порядку — до взноса месяца;
  2. копилки `short` по порядку — до их «не хватит»;
  3. долгие цели — до доли месяца;
  4. остаток — подушке, не пишется.

  Для «Отложил…» пределы 1 и 2 уменьшаются на отложенное с 1-го. Часть подушки у одного поступления = его P % − прирост частей копилок и долгих целей от него, не меньше нуля.
- **Тексты — дословно (приложение):**
  - «Откладываем 5 % с поступлений», «Изменить»;
  - «В октябре пришло 2,72 млн — отложить 82 тыс.»;
  - «Чтобы успеть всё к срокам, нужно 17 %», «Успеваем всё к срокам ✓»;
  - «Процент в таблице не читается — считаю по 10 %»;
  - статус `short` — «⚠ к 20.12 будет 3,0 из 15 млн — не хватит 12,0»;
  - «Порядок», «Порядок важности», «Если на все сроки не хватает, первой собирается копилка сверху.»;
  - «Откладываем с поступлений», «Сохранить 5 %», «Сохранить порядок».
- **Тексты — дословно (бот и заметки):**
  - «🐷 С поступления 8 000 000 UZS — 10 %: 800 000 UZS. За октябрь отложить: «Новый год» 1 200 000 UZS»;
  - добавка «· в подушку с этого поступления 2 000 000 UZS»;
  - копилкам ничего не нужно — «🐷 С поступления 12 000 000 UZS — 10 %: 1 200 000 UZS — всё в подушку» без кнопок;
  - заметки второму — «🐷 Иван — процент: 5 % с поступлений» и «🐷 Иван — порядок копилок: 1. Отпуск · 2. Новый год».
- **Порог.** Доля поступления меньше 10 000 сумов — ни строки «🛟», ни «🐷».
- **Коммиты и PR.** Conventional commits, в конце `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Тело PR заканчивается `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Мерж — `gh pr merge --merge --delete-branch`.

**Решения против спеки** (уже приняты, ledger их повторяет):
- 7.1: если копилкам ничего не нужно, бот шлёт сообщение «… — всё в подушку», а не строку «🛟» в сводке. Сводка строится до расчёта копилок, и считать их до сводки значит замедлить каждую запись. Строка «🛟» остаётся только без листа «Копилки».
- Заметки идут в прежнем формате `piggy_note`.

## Review Focus

1. **Q1 пуст, текст, −5, 150 или «2,5»; O пуст или с повторами.** Ничего не падает, процент — `SAVE_SHARE` или число, экран говорит, что ячейка не читается (тест в Task 1).
2. **Лист «Копилки» в 14 колонок (заведён до этой правки).** Первая запись O или Q1 расширяет лист, без «exceeds grid limits» (тест в Task 1).
3. **Поступление позже плана, в рублях или долларах.** «Отложить сейчас» — в сумах по курсу; лишнее докладывает копилкам `short` по порядку, подушке — остаток (тест в Task 3).
4. **Двое правят процент и порядок одновременно; порядок по устаревшему экрану.** Замок, 409 `piggies_changed` (тест в Task 4).
5. **Расчёт копилок упал после записи поступления.** Сводка та же, «🐷» нет, строка «🛟» не появляется дважды (тест в Task 5).

---

### Task 1: Процент и порядок в листе «Копилки»

**Files:**
- Modify: `app/services/google_sheets.py` (`PIGGY_LIST_HEADER`, `ensure_piggy_sheets`, новые записи), `app/services/piggy.py` (`Piggy`, `Book`, `read_book`)
- Test: `tests/test_piggy.py`, `tests/test_piggy_sheet.py`

**Interfaces:**
- Produces (`google_sheets.py`):
  - `PIGGY_LIST_HEADER` — 15 имён, последнее «Порядок»;
  - `PIGGY_WIDTH = 17`;
  - `PERCENT_LABEL = "Откладываем с поступлений, %"`;
  - `GoogleSheetsService.write_piggy_percent(percent: float) -> None` — P1:Q1 = `[[PERCENT_LABEL, percent]]` (проценты), листы и ширина — заранее;
  - `GoogleSheetsService.write_piggy_order(numbers: Mapping[str, int]) -> bool` — O строк по ID (строки ищутся заново) и O1 «Порядок» одним RAW-запросом; ID нет в листе — `False`, ничего не пишется.
- Produces (`piggy.py`):
  - `Piggy.order: int | None = None`;
  - `Book.percent: float | None = None` (доля);
  - `Book.percent_bad: bool = False`.

- [ ] **Step 1: Write the failing tests**
  - `test_reads_the_percent_and_the_order` — шапка с P1 = `PERCENT_LABEL`, Q1 = `"2,5"` → `book.percent == 0.025`, `percent_bad is False`; O копилки `"2"` → `order == 2`; пусто → `None`.
  - `test_a_bad_percent_falls_back` (parametrize Q1: `""`, `"abc"`, `-5`, `150`) → `percent is None`; `percent_bad` — `False` только для `""`.
  - `test_piggy_sheet.py`:
    - лист в 14 колонок — `write_piggy_percent(5)` зовёт `add_cols(3)`, потом один RAW `'Копилки'!P1:Q1` = `[["Откладываем с поступлений, %", 5]]`;
    - `write_piggy_order({"a1": 2, "b2": 1})` — O2 = 2, O3 = 1 и O1 «Порядок» одним запросом;
    - неизвестный ID → `False`, запросов нет;
    - новый лист создаётся на 17 колонок.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy.py tests/test_piggy_sheet.py -q` — FAIL.
- [ ] **Step 3:** реализовать. Q1 и O читать через `_number`; номер — целое ≥ 1, иначе `None`.
- [ ] **Step 4:** те же — PASS; `black . && ruff check . && pytest -q`.
- [ ] **Step 5:** commit `feat(piggy): the percent and the importance order live in the piggy sheet`.

### Task 2: План взносов от процента и порядка

**Files:**
- Modify: `app/services/piggy.py` (`savings_screen`, `_funded`, `ordered_piggies`, `Screen`, `PiggyState`, `STATUSES`)
- Test: `tests/test_piggy.py` (новые тесты и правка помощника `screen`)

**Interfaces:**
- Consumes: Task 1 — `Book.percent`, `Book.percent_bad`, `Piggy.order`.
- Produces:
  - `savings_screen(book, frame, rows, today, rates, budget, share, cushion_months, *, percent: float | None = None, order: Sequence[str] | None = None) -> Screen`:
    - `share` — P по умолчанию;
    - действующий P — `percent` (предпросмотр), иначе `book.percent`, иначе `share`;
    - `order` (предпросмотр) — эти ID первыми в этом порядке, остальные — по правилу Global Constraints;
  - `ordered_piggies(piggies: Sequence[Piggy], order: Sequence[str] | None) -> list[Piggy]`;
  - `_funded(money: Sequence[date], capacity: float, needs: Sequence[_Need]) -> list[float]` — «успеет к сроку» по очереди;
  - `Screen` — новые поля с умолчаниями в конце: `percent: float`, `percent_source: str` (`"sheet" | "default" | "bad" | "preview"`), `percent_needed: float | None` (доля целого процента; `None` — нужд нет), `capacity: float`;
  - `PiggyState` — `order: int | None` (1…n у копилок порядка) и `forecast: float | None` (будет к ближайшему сроку, в валюте копилки);
  - статус `"short"` — `short` = не хватит, вверх;
  - `Screen.piggies`: копилки порядка по порядку, потом долгие цели, потом с ошибкой.

- [ ] **Step 1: Write the failing tests** (вкладка `tab()` — поступления 12 млн; `piggy_row()` — «Новый год» 12 млн к 20.12; `TRIP` — 2 500 $ к 01.06.2027):
  - `test_the_percent_caps_the_month` (P = 10 %, E = 1 200 000):
    - `plan[0].total == 1_200_000`;
    - «Новый год» — `short`, `forecast == 2_400_000`, `short == 9_600_000`;
    - «Отпуск» — `forecast == 600`, `short == 1_900`;
    - `percent_needed == 0.5`, `capacity == 1_200_000`.
  - `test_the_order_puts_the_trip_first` — `order=("tr0001", "p3k9za")`: «Отпуск» `forecast == 800`, «Новый год» `forecast == 0`, `short == 12_000_000`; номера 1 и 2 у «Отпуска» и «Нового года»; первым в `piggies` идёт «Отпуск».
  - `test_enough_percent_keeps_the_old_plan` — P = 50 %: суммы месяцев `[6_000_000] * 2 + [5_000_000] * 6 + [3_500_000]`, статусов `short` нет.
  - `test_monthly_sums_never_exceed_the_capacity` — три копилки, P из `(0.03, 0.1, 0.3)`: каждый `plan[i].total <= capacity + 1_000 * число копилок`.
  - `test_the_sheet_percent_wins_and_the_preview_wins_over_it` — Q1 = 5 → `percent == 0.05`, `percent_source == "sheet"`; `percent=0.2` → `0.2`, `"preview"`; плохой Q1 → `share`, `"bad"`.
  - `test_a_piggy_without_a_number_goes_after_the_numbered` — O: «Отпуск» 1, «Новый год» пусто → порядок «Отпуск», «Новый год».
  - `test_next_year_deadlines_come_after_all_nearest` — «Новый год» первым, «Отпуск» вторым: «Новый год 2027» не уменьшает «успеет» июньского «Отпуска».
  - `test_no_income_in_the_list_uses_the_average_income`.
  - Помощник `screen(...)` получает `share: float = 1.0`: прежние тесты считают план без ограничения. Тесты доли 10 % (`test_the_share_*`, `test_a_goal_*`) передают `share=0.1`.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy.py -q` — FAIL.
- [ ] **Step 3:** реализовать по спеке 4.2–4.3; остаток E − взносы → подушке и долгим целям прежним правилом. Остальные модули с `config.SAVE_SHARE`, которые проверяют план без ограничения (`test_web_piggy`, `test_piggy_bot`, `test_piggy_sync`, `test_month`, `test_web_plan`), получают в фикстуре Q1 = 100.
- [ ] **Step 4:** `pytest -q` целиком — PASS; `black . && ruff check .`.
- [ ] **Step 5:** commit `feat(piggy): the plan follows the percent and the importance order`.

### Task 3: Сколько отложить сейчас, «Отложил…», Q20 и «Хватает ли»

**Files:**
- Modify: `app/services/piggy.py` (`parts_of`, `income_cushion`, `distribute`, `_month_goals`, `_enough`, поля экрана)
- Test: `tests/test_piggy.py`

**Interfaces:**
- Consumes: Task 2 — план, `forecast`, `short`, порядок `Screen.piggies`.
- Produces:
  - `parts_of(screen: Screen, amount: float, *, after_saved: bool) -> dict[str, float]` — раскладка суммы в сумах по ID копилок и долгих целей (Global Constraints), остаток — подушке и в словарь не входит;
  - `Screen.received: float` (пришло с 1-го по сегодня, сумы), `Screen.due: float` (A), `Screen.cushion_now: float` (A − части);
  - `PiggyState.now: float` — «отложить сейчас», в валюте копилки, вверх;
  - `income_cushion(screen: Screen, amount: float) -> float` — часть подушки одного поступления в сумах (экран — уже с ним);
  - `distribute(screen, amount, currency, rates)` — прежняя сигнатура, по `parts_of(after_saved=True)`;
  - `MonthGoal.total = E − траты из копилок`, `MonthGoal.piggies` — взносы копилок со сроком, `MonthGoal.share = E − piggies`;
  - в месяц срока из копилки уходит её `forecast` (собранное + «успеет»), а не цель;
  - `Enough.save = P × доход`.

- [ ] **Step 1: Write the failing tests** (P = 10 %, взнос «Нового года» в октябре — 1 200 000):
  - `test_now_follows_the_income_of_the_month` — в fact поступление 4 млн 15.10 → `received == 4_000_000`, `due == 400_000`, «Новый год» `now == 400_000`; плюс 8 млн 17.10 → `now == 1_200_000`.
  - `test_extra_money_tops_up_short_piggies_first` — пришло 20 млн → «Новый год» `now == 2_000_000`, `cushion_now == 0`.
  - `test_an_income_in_rubles_counts_by_the_table_rate` — 60 000 RUB (по курсу 150 — 9 млн) → `received == 9_000_000`, `due == 900_000`.
  - `test_the_cushion_part_of_an_income_does_not_repeat` — «Новый год» собран; 4 млн, потом 8 млн → `income_cushion(screen, 8_000_000) == 800_000`.
  - `test_distribute_follows_the_order_and_tops_up_short_piggies` — «Отпуск» первым, сумма больше взносов месяца → сначала взносы по порядку, потом «не хватит» `short`, потом подушка.
  - `test_the_month_goal_is_the_capacity_minus_spending` — октябрь `total == 1_200_000`; декабрь `1_200_000 − 2_400_000 == −1_200_000` (из «Нового года» уходит `forecast`).
  - `test_enough_takes_the_percent_of_income` — `save == 0.1 * income`, `life == income − save`.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy.py -q` — FAIL.
- [ ] **Step 3:** реализовать.
- [ ] **Step 4:** `pytest -q` — PASS; `black . && ruff check .`.
- [ ] **Step 5:** commit `feat(piggy): set aside a percent of what came in, extra money first to piggies that fall short`.

### Task 4: API — экран, предпросмотр, процент и порядок

**Files:**
- Modify: `app/web/schemas.py`, `app/web/api.py` (`savings`, `savings_screen_out`, `piggy_out`, новые маршруты, `plan_out` подсказка), `app/services/analytics_service.py` (`savings`)
- Test: `tests/test_web_piggy.py`, `tests/test_web_plan.py`

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces:
  - `AnalyticsService.savings(today, currencies, groups, subcategories, max_age=0.0, *, percent: float | None = None, order: Sequence[str] | None = None)`;
  - `SavingsMonthOut {month, percent, percent_source, percent_needed, capacity, received, due, left, cushion}`:
    - `percent` — проценты (`5.0`), `percent_needed` — целые проценты или `null`;
    - `left` — Σ `now` в сумах, `cushion` — `cushion_now`;
    - поля `piggy`, `piggy_left`, `share`, `share_to`, `goals` уходят;
  - `PiggyOut` + `order: int | None`, `forecast: float | None`, `now: float`; статус `"short"`;
  - `GET /api/savings?percent=7&order=id1,id2` — предпросмотр без записи; плохой `percent` → 422 `percent`;
  - `PUT /api/piggy/percent {percent}`:
    - 0–100, не больше одного знака после запятой, иначе 422 `percent`;
    - пишет Q1 под `PIGGY_LOCK`, ответ — свежий экран;
    - заметка второму;
  - `PUT /api/piggy/order {ids}`:
    - дубли или чужие ID → 422 `ids`;
    - набор не равен копилкам порядка → 409 `piggies_changed`;
    - пишет O, ответ — экран, заметка;
  - `PlanGoalHintOut` + `percent: float` (проценты).

- [ ] **Step 1: Write the failing tests**
  - `test_savings_answer_has_the_percent_and_the_month` — Q1 = 10: `month.percent == 10`, `percent_source == "sheet"`, `capacity`, `received`, `due`, `left`; у «Нового года» `order == 1`, `status == "short"`, `forecast`, `now`.
  - `test_preview_writes_nothing` — `?percent=50` → план без ограничения, `percent_source == "preview"`; `?order=tr0001,p3k9za` → «Отпуск» первым; записей в листы нет.
  - `test_put_percent_writes_q1_and_notes_the_other` — `{"percent": 5}` → P1:Q1 записаны, второму «🐷 Иван — процент: 5 % с поступлений».
  - `test_bad_percents_are_rejected` (parametrize `-1`, `101`, `2.55`, `"abc"`) → 422 `percent`, записей нет.
  - `test_put_order_writes_o_and_notes_the_other` — второму «🐷 Иван — порядок копилок: 1. Отпуск · 2. Новый год».
  - `test_a_stale_or_bad_order_is_rejected` — лишний или пропущенный ID → 409 `piggies_changed`; дубль → 422 `ids`.
  - `test_web_plan.py` — подсказка «Плана» несёт `percent`.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_web_piggy.py tests/test_web_plan.py -q` — FAIL.
- [ ] **Step 3:** реализовать. Маршруты — рядом с прежними `/api/piggy/*`, после записи — `written(...)`, как у правок копилок.
- [ ] **Step 4:** `pytest -q` — PASS; `black . && ruff check .`.
- [ ] **Step 5:** commit `feat(api): the percent, the importance order and their preview`.

### Task 5: Бот — одно «🐷» после поступления

**Files:**
- Modify: `app/handlers/piggy.py` (`income_prompt`, `_left`, `_sums`, `_set_all_aside`), `app/services/savings.py` (`save_hint`)
- Test: `tests/test_piggy_bot.py`, `tests/test_messages.py`, `tests/test_fix.py`, `tests/test_statement_import.py`

**Interfaces:**
- Consumes: Task 3 — `PiggyState.now`, `income_cushion`, `Screen.percent`.
- Produces:
  - `income_prompt(context, chat_id, user_id, rows)` — прежняя сигнатура, тексты из Global Constraints; поступление — сумма записанных доходов этих строк в сумах;
  - `save_hint(gs, rows)` — `[]`, если лист «Копилки» есть (`gs.get_piggy(RATES_AGE)` даёт строки; у сервиса нет `get_piggy` — листа нет); иначе прежняя строка;
  - «Отложил всё» пишет `now`.

- [ ] **Step 1: Write the failing tests** (Q1 = 10, план поступлений 12 млн, 4 млн пришли 15.10):
  - `test_one_piggy_message_after_an_income` — запись 8 млн → «🐷 С поступления 8 000 000 UZS — 10 %: 800 000 UZS. За октябрь отложить: «Новый год» 1 200 000 UZS», кнопки «Отложил всё» и «Отложил часть…».
  - `test_the_cushion_part_names_only_this_income` — Q1 = 50: запись 12 млн → в тексте «· в подушку с этого поступления 2 000 000 UZS».
  - `test_nothing_for_the_piggies_says_all_to_the_cushion` — копилки собраны → «… — всё в подушку», без кнопок.
  - `test_a_tiny_income_sends_nothing` — 50 000 при 10 %.
  - `test_set_all_aside_writes_now` — журнал получает `now`; второе нажатие — «Уже записано».
  - `test_messages.py` — с листом «Копилки» строки «🛟» в сводке нет; без листа — прежняя.
  - Расчёт упал → сводка та же, «🐷» нет.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy_bot.py tests/test_messages.py tests/test_fix.py tests/test_statement_import.py -q` — FAIL.
- [ ] **Step 3:** реализовать.
- [ ] **Step 4:** `pytest -q` — PASS; `black . && ruff check .`.
- [ ] **Step 5:** commit `feat(bot): one «🐷» per income with the percent`.

### Task 6: Приложение — вариант 1 и документы

**Files:**
- Create: `webapp/src/dashboard/PercentSheet.tsx`, `webapp/src/dashboard/OrderSheet.tsx`
- Modify:
  - `webapp/src/api.ts` (типы, `savings(read, preview)`, `setPercent`, `setOrder`);
  - `webapp/src/dashboard/SavingsTab.tsx`, `PiggyCard.tsx`, `SaveSheet.tsx`, `piggyText.ts`;
  - `webapp/src/plan/PlanScreen.tsx` (подсказка «Откладываем 5 % — …»);
  - `webapp/src/mocks/fixtures.ts`, `webapp/src/mocks/api.ts`;
  - `README.md`, `CLAUDE.md`.
- Test: `SavingsTab.test.tsx`, `PiggyCard.test.tsx`, `PercentSheet.test.tsx`, `OrderSheet.test.tsx`, `SaveSheet.test.tsx`, `plan/PlanScreen.test.tsx`

**Interfaces:**
- Consumes: Task 4 — ответ API.
- Produces:
  - `api.savings(read?: boolean, preview?: { percent?: number; order?: string[] })`;
  - `api.setPercent(percent: number)`, `api.setOrder(ids: string[])` — оба `Promise<SavingsScreen>`;
  - `PercentSheet` и `OrderSheet` с пропсами `{ api, data: SavingsScreen, onSaved, onClose }`.

- [ ] **Step 1: Write the failing tests**
  - Карточка месяца:
    - «Откладываем 3 % с поступлений» и «Изменить» → окно «Откладываем с поступлений»;
    - «В октябре пришло 2,72 млн — отложить 82 тыс.»;
    - «Чтобы успеть всё к срокам, нужно 17 %»;
    - `percent_source: "bad"` → «Процент в таблице не читается — считаю по 10 %»;
    - «Отложил…» — пока `left > 0`.
  - `PercentSheet`:
    - «−» и «+» по 1 %, быстрые 3 · 5 · 10 · 15 · 20 %;
    - предпросмотр зовёт `savings(true, { percent: 5 })` и показывает по копилкам «5,1 из 15 млн»;
    - «Сохранить 5 %» → `setPercent(5)` → `onSaved`.
  - `OrderSheet`:
    - «↑» у первой и «↓» у последней выключены;
    - перестановка зовёт предпросмотр с `order`;
    - «Сохранить порядок» → `setOrder([...])`.
  - `PiggyCard` — номер «1» в круге; `short` — «⚠ к 20.12 будет 3,0 из 15 млн — не хватит 12,0».
  - `SaveSheet` — раскладка по порядку `piggies` и докладка `short`, как сервер.
  - `PlanScreen` — «Откладываем 5 % — …».
- [ ] **Step 2:** `cd webapp && rtk proxy npx vitest run src/dashboard src/plan` — FAIL.
- [ ] **Step 3:** реализовать по макету варианта 1. Окна — общий `Sheet`. Тексты белые или жирные яркие, кнопки ↑ ↓ ≥ 44 px. Предпросмотр — после паузы 300 мс, старый ответ не перетирает новый.
- [ ] **Step 4:** полные проверки webapp; превью 360 px (тёмная и светлая): ничего не вылезает вбок, кнопки окон ≥ 40 px.
- [ ] **Step 5:** абзацы README и CLAUDE.md: процент, порядок, ячейки листа. Commit `feat(webapp): the percent and the importance order on «Накопления»`.
