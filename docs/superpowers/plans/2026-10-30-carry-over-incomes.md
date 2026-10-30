# Перенос неполученных поступлений (этап 2) — план

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** поступление прошлого месяца, по которому пришло меньше 90 %, ждёт свой остаток в этом месяце: сверка этого месяца сначала закрывает такие остатки, «Впереди» показывает их строкой «… за <месяц>, остаток», а строки этого месяца остаются целыми.

**Architecture:** `match_receipts` получает необязательные «перенесённые» строки и ставит их в очередь валюты первыми. `carried_items` (day_budget.py) строит их по вкладке прошлого месяца и fact. Вкладку прошлого месяца читают те же места, что вкладку текущего (`read_month_sheets` — параллельно; утро и ответ после записи — своим чтением), и передают её в `day_budget(..., previous=...)`, сверку «Пришло к утру», «План месяца» и «Календарь».

**Tech Stack:** Python 3.12, pandas, pytest.

**Spec:** `docs/superpowers/specs/2026-10-30-clear-money-design.md`, раздел 1.8 (перенос — без записи в лист).

## Global Constraints

- Переносятся только строки с плюсом (поступления) прошлого месяца, по которым пришло меньше 90 % (`receipts.closed` ложно), по операциям fact с датами прошлого месяца — сверка прошлого месяца пересчитывается каждый раз, без переноса из позапрошлого (только на один месяц назад).
- Перенесённая строка: название `f"{item.name} за {MONTHS_RU[месяц-1]}, остаток"` (как «за октябрь»), день 1, сумма — непришедшая часть (`receipts.remaining`), та же валюта, `uzs` — пропорционально, `row` — отрицательный (−1, −2, … по порядку дней прошлого месяца), чтобы не совпасть со строкой вкладки.
- В лист ничего не пишется: колонка T («Пришло к утру») этого месяца — по его строкам после заполнения остатков.
- Перенесённые строки не входят в `items` вкладки: не задают срок лимита и не прибавляются к деньгам (прочие поступления), не входят в «мощность» и итоги «Плана».
- Вид «Календаря» не меняется (перенос только в сверке строк, новых событий нет).
- Публичный репозиторий: в тестах и документах — только выдуманные суммы.
- Ошибка чтения вкладки прошлого месяца (её нет) — перенос пустой, всё работает как раньше.

## Review Focus

1. Октябрьский доход, записанный позже 1 ноября с октябрьской датой, уменьшает остаток (сверка прошлого месяца — по fact на сейчас).
2. Строка этого месяца в той же валюте остаётся целой, пока остаток не закрыт; деньги сверх остатка идут в строки этого месяца по их дням, как сейчас.
3. Перенесённая строка в сумах с «ЗП» в названии не становится зарплатой лимита.
4. «Впереди» (утро, /today, «Сводка»): перенесённые — первыми, со 2-го числа с «⏳».
5. Вкладки прошлого месяца нет (1-й месяц учёта) — без переноса и без ошибок.

---

### Task 1: Очередь сверки с перенесёнными строками

**Files:** Modify `app/services/receipts.py` (`match_receipts`); Test `tests/test_receipts.py`.

**Interfaces:** Produces `match_receipts(frame, lines, first, through, carried: Sequence[ListLine] = ()) -> dict[int, Receipt]` — `carried` идут в очереди своей валюты перед `lines`, их получения — под их (отрицательными) `row`.

- [ ] Step 1: тесты — (а) перенос 55 000 RUB и строка месяца 100 000 RUB; 70 000 RUB 3-го: перенос закрыт (55 000), строке месяца — 15 000; (б) без `carried` — как раньше (старые тесты зелёные); (в) ключи перенесённых — отрицательные.
- [ ] Step 2: прогнать — FAIL (`venv_dev/bin/python -m pytest tests/test_receipts.py -q`).
- [ ] Step 3: реализовать: валюты — по `lines` и `carried`; очередь — `carried` (по дню, ряду) + `lines` (по дню, ряду); «что сверх последней — последней» — как сейчас.
- [ ] Step 4: весь набор — PASS.
- [ ] Step 5: commit `feat: receipts fill last month's remainders first`.

### Task 2: Остатки прошлого месяца

**Files:** Modify `app/services/day_budget.py` (новая `carried_items`); Test `tests/test_day_budget.py` (или новый `tests/test_carry_over.py`).

**Interfaces:** Produces `carried_items(previous: list[list[Any]] | None, frame: pd.DataFrame, first: date) -> tuple[ForecastItem, ...]`.

- [ ] Step 1: тесты (выдуманные суммы): прошлый месяц — «ЗП Питер» 28-го 100 000 RUB, пришло 45 000 → перенос «ЗП Питер за октябрь, остаток», день 1, 55 000 RUB, row −1; пришло 95 000 (≥ 90 %) → переноса нет; платёж с минусом — не переносится; доход с датой прошлого месяца, записанный позже, уменьшает остаток; `previous=None` или без списка → `()`.
- [ ] Step 2: FAIL. Step 3: реализовать (сверка прошлого месяца — `match_receipts(frame, items, его 1-е, его последний день)` без переноса). Step 4: PASS, весь набор. Step 5: commit `feat: last month's unpaid incomes carry over`.

### Task 3: Лимит, «Впереди» и прогноз сумов с переносом

**Files:** Modify `app/services/day_budget.py` (`day_budget(..., previous=None)`: `carried = carried_items(previous, frame, first)`; во все `match_receipts` внутри — `carried`; «Впереди» — `_ahead((*carried, *items), receipts)`; прогноз сумов — `currency_forecast(..., carried=carried)`), `app/services/forecast.py` (`currency_forecast` и `month_forecast` — параметр `carried`/`previous`, только в сверку строк); Test `tests/test_day_budget.py`.

**Interfaces:** Produces `day_budget(frame, rows, today, rates, frozen_currency, records=(), previous=None)`; `month_forecast(..., previous=None)`.

- [ ] Step 1: тесты: 3 ноября (выдуманные числа, вкладка «Nov 26» и «Oct 26»): в «Впереди» первым — «ЗП Питер за октябрь, остаток» 55 000 RUB с «⏳»; после 55 000 RUB 4-го — его нет, а ноябрьская «ЗП Питер» ждёт 100 000 целиком; перенос UZS «ЗП … за октябрь, остаток» не меняет `payday` лимита.
- [ ] Step 2: FAIL. Step 3: реализовать. Step 4: PASS, весь набор. Step 5: commit `feat: «Впереди» waits for last month's remainders`.

### Task 4: Вкладка прошлого месяца во всех чтениях

**Files:** Modify `app/services/google_sheets.py` (`MonthSheets.previous`, `read_month_sheets` — вкладка `month_title(первое − 1 день)` параллельно, нет её — `None`), `app/services/analytics_service.py` (`morning_brief`, `write_feedback`, `dashboard`, `savings`, `forecast` — передать `previous`), `app/services/dashboard.py` (`build_dashboard(..., previous=None)`), `app/services/receipts_sync.py` (читает вкладку прошлого месяца; T — по сверке с переносом), `app/services/plan.py` (сверка строк «Плана» с переносом), `app/services/piggy_sync.py`, `app/services/plan_advice.py` (передать `previous`, если читают вкладку); Test: `tests/test_receipts_sync.py`, `tests/test_day_budget.py` (сервис), `tests/test_web_api.py`/`tests/test_dashboard.py` по месту.

- [ ] Step 1: тесты: сверка T: в ноябре 70 000 RUB 3-го при остатке октября 55 000 — в T ноябрьской строки 15 000; утреннее сообщение из листов (`Sheets` с «Oct 26» и «Nov 26») показывает перенос во «Впереди»; без вкладки прошлого месяца — как раньше.
- [ ] Step 2: FAIL. Step 3: реализовать. Step 4: PASS, весь набор, `ruff`, `black --check`. Step 5: commit `feat: every screen reads last month's tab for the carry-over`.

### Task 5: Документы

- [ ] README («Впереди»: остатки прошлого месяца первыми), CLAUDE.md (receipts: перенос на месяц назад). Commit `docs: last month's remainders wait in «Впереди»`.
