# Чужие деньги на своей карте — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** статья «💳 СЧЕТА» с меткой «чужие» в system!C копит чужие деньги; бот не считает их своими (лимит, «Сумы», «Календарь», «Заморожено») и показывает отдельно у карты и утром.

**Architecture:** новый модуль `app/services/held.py` — чистые функции над таблицей fact (`transactions_frame`): какие статьи чужие, остаток сбора по картам, таблица «своих денег» без строк сборов. Расчёты денег получают таблицу через `own_money(...)`; блок остатков листа fact, сверка с банком и выписки не меняются. «Сводка» получает сборы у карт через `/api/dashboard`, «🔎 Проверить» — кнопку «В сбор».

**Tech Stack:** Python 3.12, pandas, python-telegram-bot 22.8, Starlette + pydantic (`app/web`), React + TS + Tailwind + vitest (`webapp`).

**Spec:** `docs/superpowers/specs/2026-11-17-held-money-design.md`

## Global Constraints

- Метка — текст «чужие» в колонке C листа system у строки группы «💳 СЧЕТА»; регистр и пробелы по краям не важны; у другой группы метка ничего не значит (warning в лог).
- Осталось = − сумма D строк статьи на карте по сегодня; пришло — сумма минусов, потрачено — сумма плюсов, в валюте карты.
- Свои деньги на карте = остаток карты − max(0, осталось).
- Не меняются: формула блока остатков (fact!I2:Q30), «Проверка», «Выровнять», импорт и сверка выписок.
- Сбор с остатком 0 нигде не показывается.
- Без серого и красного мелкого текста: палитра `webapp/src/index.css` (предупреждение — `text-warn`, оранжевый).
- Репозиторий публичный: в коде, тестах, коммитах и PR — только выдуманные карты, суммы и названия статей («сбор на праздник», «HUMO 2222 UZS»). Перед push — `git diff origin/main | grep` по картам и суммам владельца.
- Проверки: `ruff check . && black --check . && pytest -q`; `cd webapp && npm run lint && npm test && npm run build`.
- Коммиты — conventional, на английском, как в истории репозитория, с `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. Сбор ушёл в минус (за праздник заплатили до того, как все сдали) — лимит должен упасть на разницу, а не делать вид, что её не тратили. Тест — Task 2, `test_day_budget_overspent_held_lowers_sums`.
2. Лист system не прочитался — бот работает как раньше (сбор в своих деньгах), в логе warning, «Сводка» не падает. Тест — Task 1, `test_read_held_failure_is_empty`.
3. Метка стоит, а строк сбора ещё нет — ни строки у карты, ни строки утром, ни ошибки. Тест — Task 4, `test_dashboard_card_without_held_rows`.
4. Валюта сбора без курса (таблица `keep_unknown=True` в «Календаре») — `own_money` не делит на ноль, `amount_uzs` строк-поправок 0. Тест — Task 1, `test_own_money_unknown_rate`.
5. Кнопку «В сбор» нажали после того, как метку стёрли и сделали /reboot — «Кнопки устарели», строка не меняется. Тест — Task 5, `test_held_button_stale_after_reboot`.

---

### Task 1: модуль `held.py`

**Files:**
- Create: `app/services/held.py`
- Modify: `app/domain.py` (константа `TRANSFERS_GROUP = "💳 СЧЕТА"` рядом с `INCOME_GROUP`), `app/services/analytics_service.py:53` (вместо своей константы — `from app.domain import TRANSFERS_GROUP`; имя по-прежнему импортируется из analytics_service — piggy.py, plan.py, api.py не трогаем)
- Modify: `CLAUDE.md` — строка про `held.py` в дереве `app/services`
- Test: `tests/test_held.py`

Константа переезжает, чтобы не было цикла импортов: `google_sheets` (Task 2) импортирует `held`, а `analytics_service` импортирует `google_sheets`. `held.py` импортирует только `app.config`, `app.domain`, `app.rich`.

**Interfaces:**
- Produces:
  ```python
  HELD_MARK = "чужие"
  HELD_TTL = 600.0  # с, копия листа system для меток
  DEFAULT_ICON = "🤝"

  @dataclass(frozen=True)
  class HeldArticle:
      name: str  # статья «💳 СЧЕТА», system!B
      icon: str  # system!D; пусто — DEFAULT_ICON

  @dataclass(frozen=True)
  class HeldOnCard:
      article: HeldArticle
      card: str  # fact!H как есть, без пробелов по краям: «HUMO 2222 UZS»
      currency: str
      came: float  # ≥ 0, в валюте карты
      spent: float  # ≥ 0
      @property
      def left(self) -> float: ...  # round(came - spent, 2)

  class ValuesSource(Protocol):
      def get_values(self, worksheet_name: str, max_age: float = 0.0) -> list[list[Any]]: ...

  def held_articles(rows: Sequence[Sequence[Any]]) -> tuple[HeldArticle, ...]
  def read_held(gs: ValuesSource, max_age: float = 0.0) -> tuple[HeldArticle, ...]
  def own_money(frame: pd.DataFrame, held: Sequence[HeldArticle]) -> pd.DataFrame
  def held_on_cards(frame: pd.DataFrame, held: Sequence[HeldArticle], today: date) -> tuple[HeldOnCard, ...]
  def held_text(item: HeldOnCard) -> str
  ```

- [ ] **Step 1: тесты**

  Таблицу fact в тестах строить через `transactions_frame(rows, RATES)` из строк вида `["10.12.2026", "💳 СЧЕТА", "сбор на праздник", -400000, "", "", "UZS", "HUMO 2222 UZS"]` (минус — пришло).

  ```python
  PARTY = HeldArticle("сбор на праздник", "🎉")

  def test_held_articles_marker():
      rows = [["💳 СЧЕТА", "сбор на праздник", " Чужие ", "🎉"],
              ["💳 СЧЕТА", "переводы", "", "🔄"],
              ["🏚️ ДОМ", "продукты", "чужие", "🛒"],   # не «СЧЕТА» — не сбор
              ["💳 СЧЕТА", "сбор на праздник", "чужие", "🎉"],  # повтор — один раз
              ["💳 СЧЕТА", "сбор на поход", "чужие"]]  # без иконки
      assert held_articles(rows) == (PARTY, HeldArticle("сбор на поход", "🤝"))

  def test_read_held_failure_is_empty(caplog):
      # get_values бросает Exception и WorksheetNotFound → (), в логе warning
      ...
      assert read_held(Broken()) == ()

  def test_read_held_uses_copy():
      # read_held(gs, 0) читает system с max_age == HELD_TTL; max_age 900 → 900
      ...

  def test_held_on_cards_split():
      # HUMO: +400 000, +300 000, −50 000 (D: −400000, −300000, 50000); VISA 1111: +100 000;
      # строка завтрашнего дня не считается; «переводы» не считаются
      assert held_on_cards(frame, [PARTY], TODAY) == (
          HeldOnCard(PARTY, "HUMO 2222 UZS", "UZS", 700000.0, 50000.0),
          HeldOnCard(PARTY, "VISA 1111 UZS", "UZS", 100000.0, 0.0),
      )

  def test_held_on_cards_zero_left_hidden():
      # пришло 300 000, потрачено 300 000 → ()

  def test_own_money_drops_held_rows():
      # переводы и траты остаются; строк статьи PARTY нет; индексы остальных строк прежние
      own = own_money(frame, [PARTY])
      assert list(own.index) == [0, 2, 3]

  def test_own_money_keeps_overspend():
      # d1 пришло 1 000 000, d2 потрачено 1 300 000, d3 пришло 300 000:
      # вклад в остаток (−amount) по дням: d1 0, d2 −300 000, d3 0 (накопленно)
      own = own_money(frame, [PARTY])
      flow = -own["amount"]
      assert [flow[own["day"] <= d].sum() for d in (D1, D2, D3)] == [0, -300000, 0]

  def test_own_money_unknown_rate():
      # валюта без курса (keep_unknown=True, amount_uzs 0): перерасход — поправка с amount_uzs 0

  def test_own_money_without_held_is_same():
      assert own_money(frame, ()).equals(frame)

  def test_held_text():
      assert held_text(HeldOnCard(PARTY, "HUMO 2222 UZS", "UZS", 1_250_000, 50_000)) == \
          "🎉 сбор на праздник 1 200 000 — чужие, не в счёт"
      assert held_text(HeldOnCard(PARTY, "HUMO 2222 UZS", "UZS", 100_000, 400_000)) == \
          "🎉 сбор на праздник — доложено из своих 300 000"
  ```

- [ ] **Step 2:** `pytest tests/test_held.py -q` — FAIL (нет модуля).
- [ ] **Step 3: реализация.**
  - `held_articles`: колонки A, B, C, D строки (короче — дополнить пустыми); порядок листа.
  - `read_held`: `held_articles(gs.get_values(config.SYSTEM_SHEET_NAME, max(max_age, HELD_TTL)))`; любое исключение — `logger.warning(..., exc_info=True)` и `()`. Лист читается без заголовка: `rows[1:]`.
  - `held_on_cards`: строки `group == TRANSFERS_GROUP`, `subcategory` из имён `held`, `day <= today`; группы по (статья, `source`, `currency`), порядок — статьи как в `held`, карты — по первому появлению; `round(left, 2) == 0` — не возвращать.
  - `own_money`: без `held` — тот же объект. Иначе строки сборов убрать (булева маска — индексы остальных не меняются) и на каждый день, когда меняется `min(0, накопленное осталось)` группы (статья, карта, валюта), добавить строку-поправку: тот же день, группа и статья, `amount = −(m(d) − m(пред.))`, `amount_uzs` — по курсу строк группы (`amount_uzs / amount` первой ненулевой строки; нет такой — 0), `comment = "перерасход сбора"`, индексы — после последнего индекса таблицы. Затем `sort_values("day", kind="stable")`.
  - `held_text`: `money_text` из `app.rich`; `left > 0` — «{icon} {name} {сумма} — чужие, не в счёт», `left < 0` — «{icon} {name} — доложено из своих {|left|}».
- [ ] **Step 4:** `pytest tests/test_held.py -q && ruff check . && black --check . && pytest -q` — PASS (перенос константы ничего не сломал).
- [ ] **Step 5:** commit `feat: held money — articles marked «чужие», their balance by card, own money without them`.

### Task 2: расчёты денег без сборов

**Files:**
- Modify: `app/services/google_sheets.py` — `MonthSheets.held: tuple[HeldArticle, ...] = ()`; `read_month_sheets` читает `read_held(gs, max_age)` в том же пуле
- Modify: `app/services/analytics_service.py` — `_brief`, `write_feedback`, `dashboard`, `savings`, `forecast` (текущий месяц, строка 924; прошлый месяц без остатков — не трогать)
- Modify: `app/services/piggy_sync.py` — `sync_piggy`, `cells`
- Modify: `app/services/plan_advice.py` — `gather_advice`
- Modify: фейки листов в `tests/` — где тесты сравнивают `ages`/число чтений, добавить чтение system (`grep -n "ages ==" tests`)
- Test: `tests/test_day_budget.py`, `tests/test_dashboard.py`, `tests/test_web_forecast.py`

**Interfaces:**
- Consumes: `read_held`, `own_money`, `held_on_cards` (Task 1).
- Produces: `MonthSheets.held`; во всех местах выше таблица для `day_budget`, `month_forecast`/`currency_forecast`, `_piggies_now` и «Заморожено» — `own_money(transactions_frame(...), held)`. Где нет `MonthSheets` — `held = read_held(self.gs_service)`.

- [ ] **Step 1: тесты.**
  - `test_day_budget_ignores_held` — fact со сбором на HUMO (+700 000) и тратами; `day_budget(own_money(frame, [PARTY]), ...)`: `budget.balance` и `budget.runway.balance` те же, что у fact без строк сбора.
  - `test_day_budget_overspent_held_lowers_sums` — пришло 200 000, потрачено 500 000 вчера: `budget.balance` на 300 000 меньше, чем у fact без строк сбора.
  - `test_dashboard_limit_without_held` (test_dashboard.py, фейк `Sheets` с листом `system` с меткой): `AnalyticsService(...).dashboard(TODAY).budget.balance` без сбора; без листа `system` в фейке — как раньше.
  - `test_forecast_money_without_held` (test_web_forecast.py): деньги «сейчас» в сумах — без сбора.
- [ ] **Step 2:** `pytest tests/test_day_budget.py tests/test_dashboard.py tests/test_web_forecast.py -q` — новые FAIL.
- [ ] **Step 3:** `read_month_sheets`: `held = pool.submit(read_held, gs, max_age)` (`MONTH_SHEETS` + 1, чтобы чтение не ждало в очереди), в `MonthSheets(..., held=held.result())`. Каждое из мест выше: таблица — `own_money(transactions_frame(...), held)`. В `dashboard` и `_brief` физическую таблицу оставить в переменной `physical` — она нужна Task 3 и Task 4.
- [ ] **Step 4:** полный `ruff check . && black --check . && pytest -q` — PASS.
- [ ] **Step 5:** commit `feat: the limit, «Календарь» and frozen money leave held money out`.

### Task 3: строка сбора утром и в /today

**Files:**
- Modify: `app/services/day_budget.py` — `DayBudget.held: tuple[HeldOnCard, ...] = ()`; `day_budget(..., held: Sequence[HeldOnCard] = ())` кладёт в поле; текст — после `runway_line(...)` (строка 1065), rich — после `_rich_runway(...)` (строка 1182): по строке `held_text(item)` на каждый сбор (rich — через `para`)
- Modify: `app/services/analytics_service.py` — `_brief` передаёт `held=held_on_cards(physical, held, today)`
- Test: `tests/test_rich_morning.py`, `tests/test_day_budget.py`

- [ ] **Step 1:** тесты `test_morning_held_line` (текст и rich: строка «🎉 сбор на праздник 1 200 000 — чужие, не в счёт» сразу под «💳 Сумы»), `test_morning_no_held_line` (`held=()` — текст как раньше, байт в байт).
- [ ] **Step 2:** FAIL. **Step 3:** реализация. **Step 4:** полный pytest — PASS.
- [ ] **Step 5:** commit `feat: the morning message names held money under «Сумы»`.

### Task 4: сборы у карт в `/api/dashboard`

**Files:**
- Modify: `app/services/cards.py` — у `CardBalance` поля `held: tuple[HeldOnCard, ...] = ()`, `held_uzs: float = 0.0`, свойства `own`, `own_uzs`; функция `attach_held`
- Modify: `app/services/analytics_service.py` — `dashboard`: `cards = attach_held(card_balances(fact, rates), held_on_cards(physical, sheets.held, today), rates)`
- Modify: `app/web/schemas.py` — `HeldOut`, поля `CardOut`; `app/web/api.py:351` — `card_out`
- Test: `tests/test_cards.py`, `tests/test_web_api.py`

**Interfaces:**
- Produces:
  ```python
  def attach_held(cards: Sequence[CardBalance], held: Sequence[HeldOnCard], rates: Rates) -> tuple[CardBalance, ...]
  # CardBalance.own -> float | None: balance − Σ max(0, item.left); None, если balance None
  # CardBalance.own_uzs -> float | None: uzs − held_uzs; None, если uzs None

  class HeldOut(ApiModel):
      name: str
      icon: str
      came: float
      spent: float
      left: float  # < 0 — доложено из своих

  # CardOut: + own: float | None, own_uzs: float | None, held: list[HeldOut] = []
  ```
  Сбор к карте — по `item.card == card.name`; сбор на карте, которой нет в блоке, — в лог warning, не показывается.

- [ ] **Step 1:** тесты: `test_attach_held_own` (HUMO 3 600 000, сбор осталось 1 200 000 → `own` 2 400 000, `own_uzs` 2 400 000; `balance` и `matches` прежние), `test_attach_held_overspent` (осталось −300 000 → `own == balance`, сбор в `held` есть), `test_dashboard_card_without_held_rows` (метка есть, строк нет → `held == []`, `own == balance`), `test_api_dashboard_held` (ответ `/api/dashboard`: у HUMO `own`, `own_uzs`, `held[0] == {"name": "сбор на праздник", "icon": "🎉", "came": 1250000, "spent": 50000, "left": 1200000}`).
- [ ] **Step 2:** FAIL. **Step 3:** реализация. **Step 4:** полный pytest — PASS.
- [ ] **Step 5:** commit `feat: /api/dashboard — own money and held money of each card`.

### Task 5: «В сбор» в «🔎 Проверить»

**Files:**
- Modify: `app/main.py:395` и `app/handlers/admin.py` (`reboot`) — `bot_data["held"] = read_held(gs_service)` рядом с `categories`
- Modify: `app/handlers/fix.py` — `check_keyboard(categories, custom, mode, held: Sequence[HeldArticle] = ())`; `inline_check`, `_queue_prompt` передают сборы для строки; в `fix_button` шаг `h`
- Modify: `README.md` — раздел про «💳 СЧЕТА»: метка «чужие», кнопка, /reboot после метки
- Test: `tests/test_sms_check.py`, `tests/test_fix.py`

**Interfaces:**
- Consumes: `HeldArticle`, `read_held` (Task 1).
- Produces: callback `fix:h:<i>` — `i` — номер в `bot_data["held"]`. Кнопка «{icon} В {name}» — первой строкой, над «✓ Верно» (спека: «первой кнопкой»), по строке на сбор, — только у строки «💳 СЧЕТА» с `amount < 0`, статья которой не сбор. Нажатие = выбор группы «💳 СЧЕТА» и статьи сбора: тот же путь, что `fix:s` (вынести его в `_recategorize(query, context, state, last, row, category, subcategory)`).

- [ ] **Step 1:** тесты: `test_check_keyboard_held_button` (входящий перевод → первая строка кнопок `[("🎉 В сбор на праздник", "fix:h:0")]`, вторая — `[("✓ Верно", "fix:ok")]`), `test_check_keyboard_no_held_for_spending` (трата ❓ → кнопки как раньше), `test_check_keyboard_no_held_for_held_row` (строка уже в сборе → без кнопки), `test_held_button_recategorizes` (`fix:h:0` → `recategorize_row(..., "💳 СЧЕТА", "сбор на праздник", -400000)`, ответ «✅ Исправил…», очередь идёт дальше), `test_held_button_stale_after_reboot` (`bot_data["held"] = ()` до нажатия → «Кнопки устарели…», `recategorize_row` не вызван).
- [ ] **Step 2:** FAIL. **Step 3:** реализация. **Step 4:** полный pytest — PASS.
- [ ] **Step 5:** commit `feat: «🔎 Проверить» puts an incoming transfer into a held-money article in one tap`.

### Task 6: макеты строки карты — выбор владельца

Не код. На Design-холсте (как макеты 10.11) — 5 вариантов строки карты со сбором в тёмной и светлой теме Telegram, на 360 px, палитрой `index.css`: что крупно (свои / всего на карте), где строка сбора, как показан перерасход («доложено из своих»), итог «Всего» (свои). Контраст каждого текста — расчётом WCAG ≥ 7:1. Данные — выдуманные. **Стоп до выбора владельца;** выбор записать в спеку одной строкой.

### Task 7: «Сводка» — строка карты со сбором

**Files:**
- Modify: `webapp/src/api.ts` — `HeldMoney { name; icon; came; spent; left }`, у `CardBalance` поля `own: number | null; own_uzs: number | null; held: HeldMoney[]`
- Modify: `webapp/src/dashboard/CardBalances.tsx` — разметка по выбранному макету (по умолчанию — набросок спеки); итог «Всего» — сумма `own_uzs`
- Modify: `webapp/src/mocks/api.ts` — у одной карты `devDashboard` сбор
- Test: `webapp/src/dashboard/CardBalances.test.tsx`

- [ ] **Step 1:** тесты: `shows own money first and the held line` (главная цифра «2 400 000 сум», текст «сбор на праздник», «пришло 1 250 000 · потрачено 50 000», «на карте всего 3 600 000»), `total sums own money` («Всего ≈ …» без сбора), `overspent held says it was added from own` («доложено из своих 300 000», класс `text-warn`), `card without held looks as before` (снимок разметки прежний).
- [ ] **Step 2:** `npm test -- CardBalances` — FAIL. **Step 3:** реализация. **Step 4:** `npm run lint && npm test && npm run build` — PASS; мок-режим `npm run dev`: снимки 360 тёмная/светлая и 320 × 1,25 без вылезаний.
- [ ] **Step 5:** commit `feat: «Сводка» — a card shows own money first and its held money under it`.

## Финал

- Ветка `feat/held-money` → PR (спека, план, код) → CI 3/3 → слить **по команде владельца** → деплой Render → `/health`, логи старта.
- С «да» владельца — «чужие» в system!C строки его сбора, затем `/reboot` в боте.
- Живая сверка: лимит и «Сумы сейчас» до и после метки отличаются ровно на остаток сбора; у HUMO в «Сводке» — свои, сбор и «на карте всего» = «Проверка» банка.
