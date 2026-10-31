# «Заморожено» и подушка (этап 3) — план

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** «🧊 Заморожено» — доллары на сейчас без долларовых платежей до зарплаты, которая ещё не пришла; «🛟 Подушка» — заморожено без копилок на сейчас, в месяцах жизни (обязательные группы) со ступенями 1 / 3 / цель. Одно правило в утре и /today, в «Сводке → Сегодня», в «Плане → Накопления» и в ячейке Q10 вкладки.

**Architecture:** `day_budget` считает «Заморожено» по сверке на сейчас и сроку «Сумы» этапа 1б (`_sums_payday`). Платежи срока даёт `_payment_days` — это `_bill_days` 1б, обобщённый на валюту, с названиями и днями. Копилки на сейчас (`piggies_total`) вычитает тот, кто читает их листы:
- «Накопления» — `savings_screen`; там же месяц жизни и ступени (savings.py);
- «Сводка» и утро — `AnalyticsService` с каталогом бота; «Сводка» читает листы копилок параллельно с остальными (`read_month_sheets(piggy=True)`).

Q10 повторяет правило формулой на утро; её ставит разовый скрипт.

**Tech Stack:** Python 3.12 (venv 3.14), pandas, pytest; React 19 + TypeScript + Tailwind 4, vitest; gws (разовая правка формулы).

**Spec:** `docs/superpowers/specs/2026-10-30-clear-money-design.md`:
- 1.5 и 1.6 (группы месяца жизни — по умолчанию);
- 2.1 «🧊 Заморожено»;
- 2.2 «🛟 Подушка» — без «Изменить» и окна «Месяц жизни»;
- 2.4 — утро и /today;
- 2.5 — O10 и Q10;
- раздел 3 — сверка после выкладки.

1.7, остальное из 2.2 и 2.3 — этап 4.

**Опирается на:**
- этап 1б (`docs/superpowers/plans/2026-10-30-limit-from-tomorrow.md`): `_sums_payday`, `_bill_days`, `DayBudget.tomorrow`, `SALARY_TODAY` в `tests/test_day_budget.py`;
- перенос с пометками месяца (a97fec0, 2905493):
  - `match_receipts(frame, lines, first, through)` — доходы по пометке «за <месяц>» или дате, платежи по датам;
  - `carried_items(previous, frame, first, through)`;
  - вкладку прошлого месяца читают только «Сводка» (`previous=True`), утро и /today.

## Global Constraints

### Правила

- **Заморожено (1.5)** = доллары минус платежи срока в долларах.
  - Доллары — `FROZEN_CURRENCY` по fact по сегодня: `_balance(own, today, "amount")`, все строки этой валюты, как сейчас.
  - Платежи — строки списка с минусом в этой валюте, открытые по сверке на сейчас: `now = match_receipts(frame, items, first, today)`, платежи сверяются по датам.
  - Строки этого месяца — с днём по конец срока, опоздавшие тоже.
  - Строки следующего месяца — тем же днём (31-е — последний день), целиком.
  - Записи «Календаря» (и долларовые) не вычитаются: в 1.5 — только строки списка. В «Сумы» их берёт 1б через `calendar_state`, здесь — нет.
- **Срок «Заморожено»** — срок «Сумы» этапа 1б: `_sums_payday(salaries, now, today)`.
  - Сегодняшняя и опоздавшая ЗП, пока денег нет, — срок до сегодня.
  - В месяце открытых строк нет — первая строка ЗП в следующем месяце.
  - Строк ЗП и аванса нет — конец месяца.
  - Перенесённые строки (1.8) срок не задают.
- **Суммы — до цента.** «Всего на карте» и платежи округляются `round(…, 2)`, «Заморожено» — их разность. Подушку, «копилки больше карты» и «долларов не хватает» даёт `frozen_split` из этих чисел. Так строки сходятся до цента на всех экранах.
- **Подушка (1.6)** = max(заморожено − копилки, 0). Копилки — остатки открытых копилок на сейчас: `balances(book, frame, rates, today)`, журнал и траты окон по сегодня. Считаются в валюте заморозки по курсу таблицы.
- **Месяц жизни** — средние траты групп `LIFE_GROUPS` без доходов и переводов.
  - Группы: 🏚️ ДОМ, 🌛 ЕЖЕМЕСЯЧНО, 🚑 ЗДОРОВЬЕ, 👶 ДЕТИ, 🎓 ОБРАЗОВАНИЕ, 🚌 ТРАНСПОРТ — те, что есть в справочнике бота.
  - Месяцы — те же полные, что у `monthly_spending`.
  - Ячейки в листе «Копилки» и окна «Месяц жизни» нет — этап 4.
- **Ступени** — 1, 3 и `CUSHION_MONTHS`. Плашка месяцев:
  - меньше 1 — `bad`, «⚠️ 0,8 мес.»;
  - от 1 до 3 — `mid`, «1,4 мес.»;
  - от 3 до цели — `good`, «3,5 мес.»;
  - от цели — `good`, «✓ 6,0 мес.».

### Что не меняется

- План взносов, «Откладываем», «Отложил…».
- Доля подушки в плане (`share_to`, `cover`): по `monthly_spending` всех трат и «Заморожено» на 1-е по прежнему правилу (`frozen_start`).
- Карточки копилок, «Норма сбережений», «Календарь», лимит, «Сумы», P5.
- «с 1-го числа ±X» убирается из утра, /today и «Сводки». `FrozenOut.change` всегда 0 — для приложений, открытых до обновления.

### Вкладка

- Q10 — утренний снимок: fact по вчера, строки по T, срок по T. P10 (= Q10 × курс) не трогаем.
- Бот формулу не пишет.
- Подпись O10 — дословно «🧊 Заморожено: доллары без платежей в USD до зарплаты (подушка и копилки)».

### Тексты и вид

- Тексты — дословно из задач (по спеке 2.1, 2.2, 2.4).
- Для плохого зрения:
  - белый жирный текст;
  - без красного текста: предупреждения — `Warn`, оранжевым;
  - плашки с белым текстом (`Chip`);
  - числа целиком, без «млн»;
  - перед «сум» — обычный пробел.
- Доллары — с центами только у дробной суммы: `money_text` / `moneyText`, как «Остатки на картах».

### Репозиторий и код

- Репозиторий публичный: в тестах, фикстурах, PR и документах — только выдуманные суммы. ID таблицы — из переменной `SHEET_ID`.
- Python: type hints, black + ruff, строки ≤ 88.
- TS: строгие типы, prettier, eslint.
- В worktree — своя `npm ci` в `webapp/`, без симлинка `node_modules`.

### Решения, где спека молчит

1. **Ruling:** опоздавшая ЗП держит срок «Заморожено» сегодняшним, как сегодняшняя (`_sums_payday`).
   - Почему: она тоже «ещё не пришла»; иначе опоздание затягивало бы в срок платежи следующего месяца.
   - Цена: пока ЗП опаздывает, платежи позже сегодняшнего дня не вычитаются.
2. **Ruling:** перенесённая сумовая ЗП (1.8) срок «Заморожено» не задаёт — как срок лимита (1.1).
   - Почему: вкладку прошлого месяца не читают «Накопления» и Q10 (спека 5).
   - Цена: если ЗП последнего дня придёт уже 1-го, утром 1-го «Заморожено» и подушка уменьшатся на долларовые платежи нового месяца ещё до денег.
3. **Ruling:** строки следующего месяца берутся из списка текущей вкладки; вкладку следующего месяца не читаем.
   - Почему: одно правило с лимитом и Q10, одна вкладка в чтении.
   - Цена: если в плане следующего месяца долларовый платёж другой, 1-го «Заморожено» сдвинется на разницу.
4. **Ruling:** месяцы подушки — один знак, округление вниз (0,96 → «0,9»).
   - Почему: плашка, полоса и «До 1 месяца — ещё 5 $» не спорят.
   - Цена: число месяцев до 0,1 меньше.
5. **Ruling:** «…» в остальных ступенях — «ещё X $» (вверх до доллара).
   - Следующая ступень: «До 1 месяца», «До 3 месяцев», «До цели 6 месяцев».
   - Цена: только текст.
6. **Ruling:** обязательные группы сравниваются по названию без эмодзи (🏚 = 🏚️), в порядке справочника.
   - Ни одной — «обязательных групп нет в справочнике», без месяцев и ступеней.
   - Цена: группа с тем же словом и другим эмодзи стала бы обязательной.
7. **Ruling:** «Копилки больше карты» показываем и в «Заморожено» «Сводки», и в утре — одной строкой.
   - Почему: иначе «подушка 0 + копилки Y» не сходится с «Заморожено X».
   - Цена: лишняя строка в редкий день.
8. **Ruling:** «Заморожено» меньше нуля без копилок — «⚠️ Долларов не хватает на платежи» и «купите X $ до ДД.ММ».
   - Дата — день платежа, на котором долларов перестаёт хватать.
   - Почему: спека велит «Копилки больше карты», а копилок нет.
   - Цена: только текст.
9. **Ruling:** копилки не прочитались — «Заморожено» без «не трогаем», экран и утро не падают; листа нет — копилки 0.
   - Почему: как с вкладкой прошлого месяца (спека 5).
   - Цена: в такой день разбивки не видно.
10. **Решение владельца-координатора (31.10):** платежи — под подписью «на платежи {срок}:», затем строки «{название} {день} — {сумма}», название как в списке, без склонения.
    - {срок} — тот же текст, что у цели «Сумы» этапа 1б: «до зарплаты 30-го», «до аванса 15-го», «до конца месяца».
    - Почему: свободные названия не склоняются, а «на» перед названием давало неграмматичную строку.
11. **Ruling:** до этапа 4 порог плана «подушка меньше месяца» и подушка на 1-е — прежние: `monthly_spending`, `frozen_start`.
    - Почему: 1.7 — этап 4, план не двигается посреди месяца.
    - Цена: «мес.» карточки и порог плана меряются разными месяцами.
12. **Ruling:** таблица «подушка | копилки» — две строки «название | сумма», как «Откуда лимит».
    - Цена: только вид.

## Review Focus

1. **Утро дня ЗП, денег ещё нет (и опоздавшая ЗП).** «Заморожено» и подушка — как вечером накануне. С приходом ЗП они уменьшаются на долларовые платежи до следующей ЗП: движение денег есть. Тесты — Task 1: `test_the_salary_day_morning_keeps_the_frozen_dollars`, `test_a_late_salary_keeps_next_months_payments_out`.
2. **1-е число.** Доллары «Заморожено» и подушки 1 ноября утром = 31 октября вечером. Строки ноября «тем же днём» из октябрьского списка = строки ноябрьской вкладки. Месяцы подушки сдвигает только окно месяца жизни. Тесты — Task 1 `test_the_first_of_the_month_keeps_the_frozen_dollars`, Task 2 `test_the_cushion_does_not_move_on_the_first`.
3. **Сегодняшние движения.** Купили доллары и отложили их в копилку сегодня — видно сразу. Подушка не меняется ни сейчас, ни завтра утром. Тесты — Task 1 (обмен сегодня), Task 2 `test_a_saving_today_keeps_the_cushion`.
4. **Не сходится.** Копилки больше карты или «Заморожено» меньше нуля: подушка 0, предупреждение говорит, что делать. Утро, «Сводка» и «Накопления» показывают одно и то же. Тесты:
   - Task 1 — `frozen_split`, `short_on`;
   - Task 3 — строки утра;
   - Task 5 и Task 6 — карточки.
5. **Листы копилок не прочитались или их нет.** «Сводка» и утро открываются, «Заморожено» без «не трогаем»; листа нет — копилки 0. Тесты — Task 4.

Q10 = бот проверяет живая сверка в Task 7, юнит-тестом это не поймать.

---

### Task 1: «Заморожено» на сейчас — до зарплаты, которая ещё не пришла

**Files:**
- Modify `app/services/day_budget.py`:
  - новые `Payment`, `_payment_days`, `frozen_split`, `short_on`;
  - `_bill_days` — через `_payment_days`;
  - поля `DayBudget`, блок заморозки в `day_budget`;
  - в `format_day_budget` и `rich_day_budget` — без «с 1-го числа»;
  - абзац «Заморожено» в docstring модуля.
- Modify `app/services/piggy.py` — `savings_screen`: подушка на 1-е = `budget.frozen_start − envelopes_start`.
- Modify `app/web/api.py` — `FrozenOut(..., change=0.0)`.
- Test:
  - `tests/test_day_budget.py` — `test_frozen_is_the_dollar_card_without_dollar_payments_ahead` заменить тестами ниже;
  - `tests/test_rich_morning.py` — `test_runway_and_frozen_variants` без `frozen_change`;
  - `tests/test_piggy.py` — помощник `frozen()`: `frozen_start=amount - change` вместо `frozen_change`.

**Interfaces:**
- Consumes (1б):
  - `_sums_payday(salaries, receipts, today) -> tuple[date | None, str]`;
  - `_bill_days(items, receipts, day, until) -> list[tuple[date, float]]`;
  - `DayBudget.tomorrow`;
  - в `day_budget` — `now`, `sums_payday`, `reach = (sums_payday or month_end) + 1 день`.
- Produces:
  - `Payment(day: date, name: str, amount: float)` — платёж списка в своей валюте: день (в следующем месяце — его день там), название строки, сумма плюсом.
  - `_payment_days(items, receipts, day: date, until: date, currency: str) -> list[Payment]`:
    - правила `_bill_days` 1б, но опоздавшие остаются в свой прошедший день;
    - порядок — по дню, затем по строке списка;
    - `_bill_days` = `sorted((max(p.day, day), p.amount) for p in _payment_days(..., "UZS"))`, его выход не меняется.
  - `frozen_split(frozen: float, piggies: float) -> tuple[float, float, float]` — (подушка, копилки больше карты, долларов не хватает), до цента:
    - подушка = `max(frozen − piggies, 0)`;
    - перебор = `max(piggies − frozen, 0)`, если копилки больше 0, иначе 0;
    - нехватка = `max(−frozen, 0)`, если копилок нет, иначе 0.
  - `short_on(total: float, payments: Sequence[Payment]) -> date | None` — день платежа, на котором накопленные платежи срока превышают `total`; хватает — `None`.
  - Поля `DayBudget`:
    - `frozen` — на сейчас по 1.5; `frozen_uzs = frozen × курс`; `frozen_change` — убрать;
    - `frozen_total: float = 0.0` — всего на картах в валюте заморозки на сейчас;
    - `frozen_payments: tuple[Payment, ...] = ()`;
    - `frozen_start: float | None = None` — на 1-е по прежнему правилу: вечер последнего дня прошлого месяца минус все платежи списка в этой валюте; только для доли подушки в плане;
    - `piggies: float | None = None` — копилки на сейчас в валюте заморозки; `None` — их не читали. `day_budget` его не заполняет.

- [ ] **Step 1: тесты** (`tests/test_day_budget.py`, фикстуры файла: 1000 $ и квартплата 18-го −400 $)

```python
from app.services.day_budget import Payment, frozen_split, short_on

EXCHANGE_TODAY = [
    fact_row(date(2026, 10, 10), "💳 СЧЕТА", "обмен валюты", 100, "USD"),
    fact_row(date(2026, 10, 10), "💳 СЧЕТА", "обмен валюты", -1_200_000),
]


def test_frozen_is_the_dollars_now_without_the_payments_until_the_salary() -> None:
    calm = budget(date(2026, 10, 10))
    # квартплата 18-го — в сроке до ЗП 30-го
    assert (calm.frozen, calm.frozen_total, calm.frozen_uzs) == (600, 1000, 7_200_000)
    assert calm.frozen_payments == (Payment(date(2026, 10, 18), "Квартплата", 400),)
    assert calm.frozen_start == 600  # на 1-е по прежнему правилу — плану копилок
    took = budget(date(2026, 10, 10), *EXCHANGE_TODAY)  # обмен сегодня виден сразу
    assert (took.frozen, took.frozen_total, took.frozen_start) == (500, 900, 600)
    assert budget(date(2026, 10, 19), RENT_PAID).frozen_payments == ()
    assert day_budget(frame(), tab(), date(2026, 10, 10), RATES, None).frozen is None


def test_the_salary_day_morning_keeps_the_frozen_dollars() -> None:
    eve = budget(date(2026, 10, 29), ADVANCE_PAID, RENT_PAID)
    morning = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID)
    # денег ещё нет — срок кончается сегодня: ноябрьская квартплата не вычитается
    assert eve.frozen == morning.frozen == 600
    assert morning.frozen_payments == ()
    came = budget(date(2026, 10, 30), ADVANCE_PAID, RENT_PAID, SALARY_TODAY)
    assert came.tomorrow is not None  # этап 1б: ЗП пришла сегодня
    assert came.frozen == 200  # срок — до ЗП 30 ноября, квартплата 18 ноября целиком
    assert came.frozen_payments == (Payment(date(2026, 11, 18), "Квартплата", 400),)


def test_a_late_salary_keeps_next_months_payments_out() -> None:
    late = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID)  # ЗП 30-го не пришла
    assert (late.frozen, late.frozen_payments) == (600, ())


def test_the_first_of_the_month_keeps_the_frozen_dollars() -> None:
    paid = (ADVANCE_PAID, RENT_PAID, SALARY_TODAY)
    last, first = budget(date(2026, 10, 31), *paid), budget(date(2026, 11, 1), *paid)
    # 1 ноября вкладка ноября с тем же списком: квартплата 18-го — уже строка месяца
    assert last.frozen == first.frozen == 200
    assert last.frozen_payments == first.frozen_payments


def test_an_unpaid_payment_waits_with_its_next_month() -> None:
    day = budget(date(2026, 10, 31), ADVANCE_PAID, SALARY_TODAY)  # октябрь не оплачен
    assert day.frozen == 200  # 1000 − 400 за октябрь − 400 за ноябрь
    assert [p.day for p in day.frozen_payments] == [
        date(2026, 10, 18),
        date(2026, 11, 18),
    ]


def test_the_frozen_money_splits_into_the_cushion_and_the_piggies() -> None:
    assert frozen_split(2_300, 1_000) == (1_300, 0, 0)
    assert frozen_split(1_200, 1_500) == (0, 300, 0)  # копилки больше карты
    assert frozen_split(-400, 0) == (0, 0, 400)  # долларов не хватает на платежи
    assert frozen_split(-400, 500) == (0, 900, 0)  # одно предупреждение — про копилки
    rent = Payment(date(2026, 10, 18), "Квартплата", 400)
    course = Payment(date(2026, 10, 25), "Курсы", 1_000)
    assert short_on(1_000, (rent, course)) == date(2026, 10, 25)
    assert short_on(1_000, (rent,)) is None
```

- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_day_budget.py`. Ожидается FAIL: нет `Payment`.
- [ ] **Step 3: реализовать.** В `day_budget` после `_sums_payday`:
  - `payments = tuple(_payment_days(items, now, today, reach, frozen_currency))` — `reach` тот же, что у «Сумы»;
  - `frozen_total = round(_balance(own, today, "amount"), 2)`, суммы платежей — `round(…, 2)`;
  - `frozen = round(frozen_total − Σ, 2)`;
  - `frozen_start` — нынешнее выражение `start`.

  Из строки утра убрать «(с 1-го числа …)», из rich-блока — строку «с 1-го числа».
- [ ] **Step 4:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider && ../venv_dev/bin/ruff check . && ../venv_dev/bin/black --check .`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: the frozen dollars are counted now, until the salary that has not come`

### Task 2: Подушка — копилки на сейчас, месяц жизни, ступени

**Files:**
- Modify `app/services/savings.py`:
  - добавить `LIFE_GROUPS`, `life_groups`, `average_months`, `LifeMonth`, `life_month`, `Step`;
  - `monthly_spending(..., groups=None)`;
  - `Cushion` — поля и свойства;
  - убрать `cushion_of` (из `app/` его никто не зовёт);
  - docstring модуля.
- Modify `app/services/piggy.py`:
  - `piggies_total`;
  - `savings_screen(..., groups=)` — подушка на сейчас; балансы `night` больше не нужны.
- Modify `app/services/analytics_service.py` — `savings`: `groups=list(groups)`.
- Modify `app/services/piggy_sync.py` — `sync_piggy`, `goal_cells`: `groups=groups`.
- Modify `app/config.py` — комментарий у `CUSHION_MONTHS`: «месяцев жизни».
- Test:
  - `tests/test_savings.py` — тесты `cushion_of` удалить; то, что они проверяли у `Cushion` (без трат `months is None`, без курса `short == 0`), — в новых тестах;
  - `tests/test_piggy.py`:
    - помощник `screen(..., groups=(*GROUPS, "🏚️ ДОМ"))`;
    - помощник `frozen()` получает `frozen_total=amount`, `frozen_payments=()`;
    - `test_the_cushion_is_frozen_minus_piggies_as_of_last_night` заменить первым тестом ниже;
  - `tests/test_web_piggy.py` — `PiggyAnalytics.savings`: `groups=groups`.

**Interfaces:**
- Consumes: Task 1 — `DayBudget.frozen`, `frozen_total`, `frozen_payments`, `frozen_start`, `frozen_split`, `Payment`.
- Produces:
  - `LIFE_GROUPS = ("🏚️ ДОМ", "🌛 ЕЖЕМЕСЯЧНО", "🚑 ЗДОРОВЬЕ", "👶 ДЕТИ", "🎓 ОБРАЗОВАНИЕ", "🚌 ТРАНСПОРТ")`.
  - `life_groups(catalog: Iterable[str]) -> tuple[str, ...]` — группы справочника, чьё название без эмодзи (`leading_emoji`, без учёта регистра) есть в `LIFE_GROUPS`; порядок — справочника.
  - `average_months(frame, today, count=AVERAGE_MONTHS) -> list[date]` — месяцы, которые сейчас берёт `monthly_spending`.
  - `monthly_spending(frame, today, count=AVERAGE_MONTHS, groups: Collection[str] | None = None) -> float` — `groups` оставляет только эти группы (`()` даёт 0); месяцы — `average_months`.
  - `LifeMonth(amount: float, months: tuple[date, ...], groups: tuple[str, ...])`; `life_month(frame, today, groups: Sequence[str]) -> LifeMonth`.
  - `Step(months: int, short: float, short_uzs: float)` — `short` в валюте подушки вверх до целых, `short_uzs` — сумы целыми.
  - `Cushion(amount, currency, uzs, rate, monthly, target_months, frozen=0.0, card=0.0, payments=(), piggies=0.0, over=0.0, lacking=0.0, life: LifeMonth | None = None)`:
    - `monthly` — месяц жизни;
    - `months`, `short_uzs`, `short` — как были;
    - `shown_months -> float | None` — `floor(months × 10 + 1e-9) / 10`;
    - `steps -> tuple[Step, ...]` — ступени 1, 3, `target_months` (не больше цели), которые больше `months`; месяцев нет — `()`.
  - `piggies_total(book: Book, found: Mapping[str, Balance], rates: Rates, currency: str) -> float` — остатки открытых копилок (`closed is None`, ID есть в `found`) в `currency`, до цента. Им же считаются копилки на 1-е в плане.
  - `savings_screen(book, frame, rows, today, rates, budget, share, cushion_months, *, percent=None, order=None, groups: Collection[str]) -> Screen`. `Screen.frozen`, `envelopes`, `over` — те же числа на сейчас.

- [ ] **Step 1: тесты.** `tests/test_savings.py`:

```python
def test_the_month_of_life_counts_only_the_mandatory_groups() -> None:
    rows = [
        *(fact_row(date(2026, m, 5), "🏚️ ДОМ", "квартплата", 6_000_000) for m in (7, 8, 9)),
        *(fact_row(date(2026, m, 6), "🍔 ЕДА", "кафе", 10_000_000) for m in (7, 8, 9)),
        fact_row(date(2026, 8, 7), "👶 ДЕТИ", "садик", 3_000_000),
        fact_row(date(2026, 9, 8), "💳 СЧЕТА", "перевод", 7_000_000),  # не трата
    ]
    life = life_month(frame(*rows), TODAY, ("🏚️ ДОМ", "👶 ДЕТИ"))
    july, august, september = (date(2026, m, 1) for m in (7, 8, 9))
    # (6 + 6 + 6 + 3) / 3; еда и перевод не в счёт
    assert life == LifeMonth(7_000_000, (july, august, september), ("🏚️ ДОМ", "👶 ДЕТИ"))
    assert life_month(frame(*rows), TODAY, ()).amount == 0  # групп нет


def test_the_mandatory_groups_are_the_defaults_in_the_catalog() -> None:
    catalog = ["🍔 ЕДА", "🚌 ТРАНСПОРТ", "🏚 ДОМ", "🌛 ЕЖЕМЕСЯЧНО", "💳 СЧЕТА"]
    # нет 🚑 ЗДОРОВЬЕ, 👶 ДЕТИ, 🎓 ОБРАЗОВАНИЕ — пропущены; «🏚 ДОМ» — та же группа
    assert life_groups(catalog) == ("🚌 ТРАНСПОРТ", "🏚 ДОМ", "🌛 ЕЖЕМЕСЯЧНО")
    assert life_groups(["🍔 ЕДА"]) == ()


@pytest.mark.parametrize(
    ("dollars", "shown", "steps"),
    [
        (800, 0.8, [(1, 200, 2_400_000), (3, 2_200, 26_400_000), (6, 5_200, 62_400_000)]),
        (1_400, 1.4, [(3, 1_600, 19_200_000), (6, 4_600, 55_200_000)]),
        (3_500, 3.5, [(6, 2_500, 30_000_000)]),
        (6_200, 6.2, []),
        (995, 0.9, [(1, 5, 60_000), (3, 2_005, 24_060_000), (6, 5_005, 60_060_000)]),
    ],
)
def test_the_cushion_steps(
    dollars: float, shown: float, steps: list[tuple[int, float, float]]
) -> None:
    # месяц жизни 12 млн — 1000 $ по курсу 12 000, цель 6 месяцев
    found = Cushion(dollars, "USD", dollars * 12_000, 12_000, 12_000_000, 6)
    assert found.shown_months == shown
    assert [(step.months, step.short, step.short_uzs) for step in found.steps] == steps
    assert Cushion(0, "USD", 0, 12_000, 0, 6).shown_months is None  # месяца жизни нет
```

  `tests/test_piggy.py`. Импорты: `budget as budget_on`, `ADVANCE_PAID`, `RENT_PAID`, `SALARY_TODAY` из `tests.test_day_budget`; `Payment`.

```python
def test_the_cushion_is_the_frozen_money_without_the_piggies_now() -> None:
    moves = (
        move_row(id="tr0001", day="16.10.2026", amount=400, currency="USD"),
        move_row(id="tr0001", day="17.10.2026", amount=100, currency="USD"),  # сегодня
    )
    found = screen(piggy_row(**TRIP), moves=moves, budget=budget_on(TODAY))
    cushion = found.cushion
    assert cushion is not None
    # 1000 $ на карте − квартплата 18.10 − копилка с сегодняшним взносом
    assert (cushion.card, cushion.frozen, cushion.piggies, cushion.amount) == (
        1_000, 600, 500, 100,
    )
    assert cushion.payments == (Payment(date(2026, 10, 18), "Квартплата", 400),)
    assert (found.frozen, found.envelopes, found.over) == (600, 500, 0)


def test_a_saving_today_keeps_the_cushion() -> None:
    # сегодня купили 100 $ и отложили их: «Заморожено» и копилки растут вместе
    bought = fact_row(TODAY, "💳 СЧЕТА", "обмен валюты", -100, "USD")
    early = (move_row(id="tr0001", day="16.10.2026", amount=400, currency="USD"),)
    saved = (*early, move_row(id="tr0001", day="17.10.2026", amount=100, currency="USD"))
    before = screen(piggy_row(**TRIP), moves=early, budget=budget_on(TODAY))
    after = screen(piggy_row(**TRIP), moves=saved, budget=budget_on(TODAY, bought))
    assert before.cushion is not None and after.cushion is not None
    assert before.cushion.amount == after.cushion.amount == 200


def test_the_cushion_does_not_move_on_the_first() -> None:
    moves = (move_row(id="tr0001", day="20.10.2026", amount=100, currency="USD"),)
    paid = (ADVANCE_PAID, RENT_PAID, SALARY_TODAY)
    days = (date(2026, 10, 31), date(2026, 11, 1))
    last, first = (
        screen(piggy_row(**TRIP), moves=moves, today=day, budget=budget_on(day, *paid))
        for day in days
    )
    assert last.cushion is not None and first.cushion is not None
    assert last.cushion.amount == first.cushion.amount == 100


def test_the_cushion_counts_months_of_life() -> None:
    home = tuple(
        fact_row(date(2026, m, 5), "🏚️ ДОМ", "квартплата", 12_000_000) for m in (7, 8, 9)
    )
    found = screen(fact=(*home, *spending_months()), budget=frozen(1_300))
    cushion = found.cushion
    assert cushion is not None and cushion.life is not None
    # «🍔 ЕДА» по 10 млн — не обязательные: месяц жизни — только дом
    assert (cushion.monthly, cushion.life.groups) == (12_000_000, ("🏚️ ДОМ",))
    assert cushion.shown_months == 1.3
    assert [step.months for step in cushion.steps] == [3, 6]
```

  Прежние тесты доли подушки (`test_the_share_*`, `test_a_goal_*`) — зелёные без правки: план меряет всеми тратами.

- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_savings.py tests/test_piggy.py`. Ожидается FAIL.
- [ ] **Step 3: реализовать.** В `savings_screen`:
  - `life = life_month(frame, today, life_groups(groups))`;
  - копилки — `piggies_total(book, now, rates, currency)`;
  - `kept, over, lacking = frozen_split(budget.frozen, envelopes)`;
  - `Cushion(..., frozen=budget.frozen, card=budget.frozen_total, payments=budget.frozen_payments, piggies=envelopes, over=over, lacking=lacking, life=life)`;
  - доля подушки в плане — прежняя (`monthly_spending(frame, today)`, `budget.frozen_start`).
- [ ] **Step 4:** весь набор, ruff, black. Ожидается PASS.
- [ ] **Step 5: commit** `feat: the cushion is the frozen money without the piggies, in months of life`

### Task 3: Утро и /today — «🧊 Заморожено … — не трогаем …»

**Files:**
- Modify `app/services/day_budget.py` — блоки заморозки в `format_day_budget` и `rich_day_budget`; общий помощник строк.
- Test:
  - `tests/test_rich_morning.py`;
  - `tests/test_day_budget.py` — `test_morning_message`;
  - `tests/test_runway.py` — `test_the_line_follows_the_frozen_money_in_the_morning_message`: «💳 Сумы …» идёт сразу после строк платежей «Заморожено».

**Interfaces:** Consumes Task 1 — `frozen_split`, `short_on`, `Payment`, `DayBudget.piggies`, `frozen_payments`, `frozen_total`.

Тексты. F, C, P, O, L, W — `money_text(…, код)`; {день} — `_day_text` («18-го», «18 ноября»). Предупреждения — из `frozen_split(frozen, piggies or 0.0)`.

- **Обычный текст:**
  - первая строка — `🧊 Заморожено {F}`; копилки известны — плюс ` — не трогаем: подушка {C}, копилки {P}`;
  - ниже, с отступом «   », по порядку:
    - перебор — `⚠️ копилки больше карты на {O} — отложите разницу или поправьте копилки`;
    - нехватка — `⚠️ долларов не хватает на платежи — купите {L} до {день short_on}` (дня нет — без « до …»);
    - платежи срока есть — строка `на платежи {срок}:`, затем по каждому — `{название} {день} — {W}`.
- **Расширенное сообщение:**
  - `heading("🧊 Заморожено {F}")`;
  - `para("не трогаем: подушка {C}, копилки {P}", <предупреждение>, "на платежи {срок}:", <платежи>)` — только если в нём есть строки.
- Строк «≈ … сум» и «с 1-го числа» нет. Порядок блоков сообщения прежний.

- [ ] **Step 1: тесты** (`tests/test_rich_morning.py`; `usd(x) = money_text(x, "USD")`; `ITEMS`, `ADVANCE_PAID`, `RENT_PAID`, `SALARY_TODAY` — из `tests.test_day_budget`)

```python
def test_the_morning_says_what_the_frozen_money_keeps() -> None:
    b = replace(budget(date(2026, 10, 10)), piggies=500)
    keeps = f"подушка {usd(100)}, копилки {usd(500)}"
    lines = format_day_budget(b).splitlines()
    at = lines.index(f"🧊 Заморожено {usd(600)} — не трогаем: {keeps}")
    assert lines[at + 1 : at + 3] == [
        "   на платежи до зарплаты 30-го:",
        f"   Квартплата 18-го — {usd(400)}",
    ]
    blocks = rich_day_budget(b)
    at = blocks.index(heading(f"🧊 Заморожено {usd(600)}"))
    assert blocks[at + 1] == para(
        f"не трогаем: {keeps}",
        "на платежи до зарплаты 30-го:",
        f"Квартплата 18-го — {usd(400)}",
    )
    assert not any("с 1-го числа" in block for block in blocks)


def test_a_payment_of_next_month_names_its_month() -> None:
    b = budget(date(2026, 10, 31), ADVANCE_PAID, RENT_PAID, SALARY_TODAY)
    assert f"   Квартплата 18 ноября — {usd(400)}" in format_day_budget(b).splitlines()


def test_the_frozen_line_says_what_to_do_when_it_does_not_add_up() -> None:
    over = replace(budget(date(2026, 10, 10)), piggies=700)
    assert (
        f"   ⚠️ копилки больше карты на {usd(100)} — отложите разницу или "
        "поправьте копилки"
    ) in format_day_budget(over).splitlines()
    short = budget(date(2026, 10, 10), items=[*ITEMS, ("Курсы", 25, -1_000, "USD")])
    lines = format_day_budget(short).splitlines()
    at = lines.index(f"🧊 Заморожено {usd(-400)}")  # копилок не читали: без разбивки
    assert lines[at + 1 : at + 5] == [
        f"   ⚠️ долларов не хватает на платежи — купите {usd(400)} до 25-го",
        "   на платежи до зарплаты 30-го:",
        f"   Квартплата 18-го — {usd(400)}",
        f"   Курсы 25-го — {usd(1_000)}",
    ]
```

  Остальные правки тестов:
  - `test_morning_message`: вместо «🧊 Заморожено: 600,00 USD ≈ 7,20 млн» — `f"🧊 Заморожено {usd(600)}"`, `"   на платежи до зарплаты 30-го:"` и `f"   Квартплата 18-го — {usd(400)}"`;
  - `test_morning_blocks_follow_the_mockup`: `heading(f"🧊 Заморожено {usd(b.frozen)}")`, за ним `para("на платежи до зарплаты 30-го:", f"Аренда & Ко 18-го — {usd(420)}")` (срок — как у «Сумы» этого теста).

- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_rich_morning.py tests/test_day_budget.py tests/test_runway.py`. Ожидается FAIL.
- [ ] **Step 3: реализовать** — одна функция строк заморозки для обоих форматтеров. `_cents` больше никто не зовёт — убрать.
- [ ] **Step 4:** весь набор, ruff, black. Ожидается PASS.
- [ ] **Step 5: commit** `feat: the morning says what the frozen money keeps`

### Task 4: Копилки для «Сводки» и утра

**Files:**
- Modify `app/services/google_sheets.py` — `MONTH_SHEETS = 7`, `MonthSheets.piggy`, `read_month_sheets(..., piggy=False)`.
- Modify `app/services/piggy.py` — `Catalog`, `piggies_now`.
- Modify `app/services/dashboard.py` — `build_dashboard(..., piggies=None)`.
- Modify `app/services/analytics_service.py` — `AnalyticsService.__init__(..., catalog=None)`, `dashboard`, `_brief`.
- Modify `app/main.py` — `AnalyticsService(gs_service, catalog=lambda: bot_catalog(app.bot_data))`.
- Test:
  - `tests/test_sheet_copies.py`;
  - `tests/test_dashboard.py`;
  - `tests/test_day_budget.py` — у подделки `Sheets` метод `get_piggy` по вкладкам «Копилки» и «Журнал копилок» (нет вкладки — `[]`);
  - `tests/test_main.py`.

**Interfaces:**
- Consumes: Task 2 — `piggies_total`, `balances`, `read_book`; Task 3 — строки утра.
- Produces:
  - `MonthSheets.piggy: tuple[list[list[Any]], list[list[Any]]] | None = None` — листы «Копилки» и «Журнал копилок»:
    - `None` — не просили или не прочитались (warning в лог);
    - листа нет — `[]`.
  - `read_month_sheets(gs, title, max_age=0.0, calendar=True, previous=False, piggy=False) -> MonthSheets` — `piggy=True`: два листа копилок параллельно с остальными, с тем же `max_age`.
  - `Catalog = tuple[Collection[str], Sequence[str], Mapping[str, Sequence[str]]]` (piggy.py) — то, что отдаёт `bot_catalog`.
  - `piggies_now(sheets, catalog: Catalog, frame, rates, today, currency: str | None) -> float | None`:
    - вызывает `read_book(*sheets, *catalog)`, затем `piggies_total(book, balances(book, frame, rates, today), rates, currency)`;
    - листов нет (`sheets is None`) или нет валюты — `None`.
  - `build_dashboard(..., previous=None, piggies: float | None = None)` — `budget.piggies = piggies`.
  - `AnalyticsService(gs_service, catalog: Callable[[], Catalog] | None = None)`:
    - без `catalog` копилки не читаются нигде;
    - `dashboard()`: `read_month_sheets(..., previous=True, piggy=True)`;
    - `_brief()` (утро и /today): `self.gs_service.get_piggy()` заново; любой сбой — warning в лог и `piggies=None`.

- [ ] **Step 1: тесты.**
  - `tests/test_sheet_copies.py`:
    - `test_piggy_sheets_come_with_the_month_when_asked` — с `piggy=True, max_age=120` `sheets.piggy == (список, журнал)`, в `calls` есть `("Копилки", 120)` и `("Журнал копилок", 120)`; без `piggy` — `None` и чтений копилок нет;
    - `test_missing_piggy_sheets_are_empty` — `([], [])`;
    - `test_a_failed_piggy_read_leaves_the_screen_without_piggies` — `DownTab(..., "Журнал копилок")`: `sheets.piggy is None`, `sheets.month` прочитан.
  - `tests/test_dashboard.py`, `test_the_dashboard_reads_the_piggies_with_the_catalog`:
    - вкладки: «Копилки» — копилка в USD без срока и статьи (так её строка проходит любой справочник с USD), «Журнал копилок» — «отложил» 500 USD;
    - `AnalyticsService(sheets, catalog=lambda: CATALOG).dashboard(TODAY, max_age=120).budget.piggies == 500`;
    - `sheets.ages == [120] * 7`;
    - прежний `test_the_dashboard_may_take_copies_of_the_sheets` — 5 чтений без каталога.
  - `tests/test_day_budget.py`:
    - `test_the_morning_reads_the_piggies_with_the_catalog` — 10.10: в тексте строка `🧊 Заморожено 600 $ — не трогаем: подушка 100 $, копилки 500 $`;
    - `test_without_the_catalog_the_morning_does_not_read_the_piggies` — `get_piggy` не вызван;
    - `test_a_failed_piggy_read_keeps_the_morning` — `get_piggy` бросает `ConnectionError`: в тексте `🧊 Заморожено 600 $` без «не трогаем».
  - `tests/test_main.py`, `test_the_analytics_service_reads_the_bot_catalog` — `app.bot_data["analytics_service"].catalog() == bot_catalog(app.bot_data)`.
- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_sheet_copies.py tests/test_dashboard.py tests/test_day_budget.py tests/test_main.py`. Ожидается FAIL.
- [ ] **Step 3: реализовать.**
- [ ] **Step 4:** весь набор, ruff, black. Ожидается PASS.
- [ ] **Step 5: commit** `feat: «Сводка» and the morning read the piggy banks`

### Task 5: «🧊 Заморожено» в «Сводке → Сегодня»

**Files:**
- Modify `app/web/schemas.py` — `FrozenPaymentOut`, `FrozenOut`.
- Modify `app/web/api.py` — `frozen_out(budget)` для `dashboard_out`.
- Modify `webapp/src/api.ts` — `FrozenPayment`, `Frozen`, `Dashboard.frozen: Frozen | null`.
- Create `webapp/src/dashboard/FrozenCard.tsx` — `FrozenCard`, экспорт `FrozenWarnings`.
- Modify `webapp/src/dashboard/DashboardScreen.tsx` — вместо `Frozen` — `FrozenCard`.
- Modify `webapp/src/mocks/fixtures.ts` — `dashboardFixture.frozen`.
- Test:
  - `tests/test_web_api.py`;
  - `webapp/src/dashboard/FrozenCard.test.tsx` (новый);
  - `webapp/src/dashboard/DashboardScreen.test.tsx` — два теста «с 1-го числа» заменить тестом порядка карточек.

**Interfaces:**
- Consumes: Task 1 — `frozen_split`, `short_on`, `Payment`; Task 4 — `DayBudget.piggies` из `build_dashboard`.
- Produces:
  - `FrozenPaymentOut(day: date, name: str, amount: float)`.
  - `FrozenOut`:
    - `amount`, `currency`, `uzs`;
    - `change: float = 0.0` — для приложений, открытых до обновления;
    - `total: float = 0.0`, `payments: list[FrozenPaymentOut] = []`;
    - `piggies: float | None = None`, `cushion: float | None = None` (`None` — копилки не читали);
    - `over: float = 0.0`, `lacking: float = 0.0`, `lacking_on: date | None = None`;
    - `target: str = ""` — срок платежей текстом, как цель «Сумы» 1б («до зарплаты 30 ноября»).
  - `frozen_out(budget)`:
    - `cushion, over, lacking = frozen_split(frozen, piggies or 0.0)`; `cushion` в ответе — `None`, если `piggies is None`;
    - `lacking_on = short_on(frozen_total, frozen_payments)`, если `lacking > 0`, иначе `None`.
  - TS `Frozen` — те же поля (`day`, `lacking_on` — ISO-строки).
  - `FrozenWarnings({ currency, over, lacking, lackingOn })` — для Task 6.

Карточка `<Card label="Заморожено">` (спека 2.1), по порядку:
1. `<Fact>🧊 Заморожено {moneyText(amount)}</Fact>`.
2. Копилки известны: `<Line>не трогаем:</Line>` и `<Table label="Не трогаем" align="lr">` со строками «подушка | {cushion}», «копилки | {piggies}».
3. `FrozenWarnings`:
   - перебор: `<Warn>Копилки больше карты</Warn>`, `<Line>разница {over}</Line>`, `<Line>отложите разницу</Line>`, `<Line>или поправьте копилки</Line>`;
   - нехватка: `<Warn>Долларов не хватает на платежи</Warn>`, `<Line>купите {lacking} до {ДД.ММ}</Line>` (без `lacking_on` — без « до …»).
4. Платежи есть — `<Line>на платежи {target}:</Line>`, затем по каждому — `<Line>{name} {dayMonth(day)} — {amount}</Line>`.
5. `<Line>всего на карте {total}</Line>`.

Суммы — `moneyText(…, currency)`.

- [ ] **Step 1: тесты.** `tests/test_web_api.py`:

```python
def test_the_frozen_card_adds_up() -> None:
    found = build_dashboard(frame(), tab(), date(2026, 10, 2), RATES, "USD", piggies=500)
    assert dashboard_for(found).json()["frozen"] == {
        "amount": 600, "currency": "USD", "uzs": 7_200_000, "change": 0,
        "total": 1_000,
        "payments": [{"day": "2026-10-18", "name": "Квартплата", "amount": 400}],
        "piggies": 500, "cushion": 100, "over": 0, "lacking": 0, "lacking_on": None,
    }
    plain = build_dashboard(frame(), tab(), date(2026, 10, 2), RATES, "USD")
    body = dashboard_for(plain).json()["frozen"]
    assert (body["piggies"], body["cushion"]) == (None, None)
```

  `FrozenCard.test.tsx`. Фикстура: `amount 2300`, `total 3000`, платёж `{day: "2026-11-18", name: "Квартплата", amount: 700}`, `piggies 1000`, `cushion 1300`.
  - `shows what the frozen dollars keep and where the rest goes` — фикстура с `target: "до зарплаты 30 ноября"`; `textContent` детей карточки по порядку (пробелы сведены): `"🧊 Заморожено 2 300 $"`, `"не трогаем:"`, `"подушка1 300 $копилки1 000 $"`, `"на платежи до зарплаты 30 ноября:"`, `"Квартплата 18.11 — 700 $"`, `"всего на карте 3 000 $"`.
  - `shows no breakdown without the piggies` — `piggies: null, cushion: null`: нет «не трогаем:» и таблицы.
  - `warns when the piggies are over the card` — `amount 1200, piggies 1500, cushion 0, over 300`: «⚠️ Копилки больше карты», «разница 300 $», «отложите разницу», «или поправьте копилки».
  - `says what to buy when the dollars are short` — `amount -400, total 300, piggies 0, cushion 0, lacking 400, lacking_on "2026-11-18"`: «🧊 Заморожено −400 $», «⚠️ Долларов не хватает на платежи», «купите 400 $ до 18.11».

  `DashboardScreen.test.tsx`, `puts the frozen card between the limit and the sums` — в «Сегодня» регион «Заморожено» стоит после «Можно сегодня» и перед «Сумы».
- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_web_api.py` и `cd webapp && npx vitest run src/dashboard/FrozenCard.test.tsx src/dashboard/DashboardScreen.test.tsx`. Ожидается FAIL.
- [ ] **Step 3: реализовать.**
- [ ] **Step 4:** `pytest`, ruff, black, `cd webapp && npm test && npm run lint && npm run build`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: «Сегодня» shows what the frozen dollars keep`

### Task 6: «🛟 Подушка» в «Плане → Накопления»

**Files:**
- Modify `app/web/schemas.py` — `CushionStepOut`, `CushionOut`.
- Modify `app/web/api.py` — `cushion_out(cushion)` со всеми числами; в `savings_screen_out` больше нет `model_copy`.
- Modify `webapp/src/api.ts` — `CushionStep`, `Cushion`.
- Modify `webapp/src/dashboard/CushionCard.tsx` — переписать.
- Modify `webapp/src/mocks/fixtures.ts` — `savingsFixture.cushion`.
- Test:
  - `tests/test_web_piggy.py` — `test_savings_screen_shape`;
  - `webapp/src/dashboard/CushionCard.test.tsx` — переписать;
  - `webapp/src/dashboard/SavingsTab.test.tsx` — тест «минус копилки … на вечер вчера» заменить проверкой таблицы «Расчёт подушки»; тест «Копилки больше карты» — те же строки.

**Interfaces:**
- Consumes: Task 2 — `Cushion`, `Step`, `LifeMonth`; Task 5 — `FrozenPaymentOut`, `FrozenWarnings`; Task 1 — `short_on`.
- Produces:
  - `CushionStepOut(months: int, short: float, short_uzs: float)`.
  - `CushionOut`:
    - прежние поля; `months` — `Cushion.shown_months`, `frozen`, `piggies`, `over` — на сейчас;
    - `card: float = 0.0`, `payments: list[FrozenPaymentOut] = []`;
    - `lacking: float = 0.0`, `lacking_on: date | None = None` — `short_on(card, payments)`, если `lacking > 0`;
    - `life_months: list[str] = []` («2026-07»), `life_groups: list[str] = []`;
    - `steps: list[CushionStepOut] = []`.
  - TS `Cushion` — те же поля, новые обязательные.

Карточка `<Card label="Подушка безопасности">` (спека 2.2), по порядку:
1. **Шапка.** `<h2>🛟 Подушка</h2>` и `Chip` по `months`:
   - меньше 1 — `bad`, «⚠️ 0,8 мес.»;
   - меньше 3 — `mid`, «1,4 мес.»;
   - меньше цели — `good`, «3,5 мес.»;
   - иначе — `good`, «✓ 6,0 мес.».
   Шапка — `flex-wrap`, как сейчас; `months === null` — без плашки и полосы.
2. **Полоса до цели.** `role="progressbar"`, `aria-label="Подушка: 1,3 из 6 месяцев"`, `aria-valuenow = min(months, target)`. Отметки на 1 и 3 (меньше цели) — черты 2 px цвета текста под полосой. Подписи под ними — «1», «3», справа «6 мес.».
3. **Главная цифра.** `<Big>{moneyText(amount)}</Big>`, `<Line>в подушке сейчас</Line>`, `<Line>≈ {money(uzs)} сум</Line>`.
4. **Расчёт.** `<Table label="Расчёт подушки" align="lr">`:
   - «доллары на карте | {card}»;
   - по платежу — «{name} {ДД.ММ} | −{amount}»;
   - «копилки | −{piggies}» (ноль — без минуса);
   - «подушка | {amount}».
5. **Месяц жизни:**
   - групп нет — `<Line>обязательных групп нет в справочнике</Line>`;
   - месяцев нет — `<Line>трат за 3 полных месяца ещё нет</Line>`;
   - иначе — `<Fact>Месяц жизни {money(monthly)}</Fact>`, `<Line>в среднем за {месяцы}</Line>`, `<Line>обязательные: {группы через «, »}</Line>`. Месяцы — `monthName` у одного, иначе `monthRange` («июль–сентябрь»).
6. **Ступени** (только при `months !== null`):
   - `steps` пусто — `<Good>Цель {target} мес. достигнута</Good>`;
   - первая — `<Fact>До 1 месяца — ещё {short}</Fact>` («До 3 месяцев», «До цели 6 месяцев»), под ней `<Line>≈ {money(short_uzs)} сум</Line>`;
   - остальные — `<Line>3 месяца — ещё {short}</Line>`, `<Line>цель 6 месяцев — ещё {short}</Line>`.
   - Слово «месяц» — `plural`, после «До» — родительный: «месяца» у 1, 21…, иначе «месяцев».
7. **Предупреждения** — `FrozenWarnings`.

Суммы в долларах — `moneyText(…, currency)`.

- [ ] **Step 1: тесты.**
  - `tests/test_web_piggy.py`, `test_savings_screen_shape`:
    - `cushion["piggies"] == 500` — как было;
    - `cushion["card"] == 1000`;
    - `cushion["life_groups"] == ["🏚️ ДОМ", "🌛 ЕЖЕМЕСЯЧНО"]` — справочник тестового приложения;
    - в ответе есть ключи `payments`, `lacking`, `lacking_on`, `life_months`, `steps`.
  - `CushionCard.test.tsx`. Фикстура:
    - `card 3000`, платёж `{day: "2026-10-18", name: "Квартплата", amount: 700}`, `piggies 1000`, `frozen 2300`;
    - `amount 1300`, `uzs 15_600_000`, `monthly 12_000_000`, `months 1.3`, `target_months 6`;
    - `life_months ["2026-07", "2026-08", "2026-09"]`, `life_groups ["🏚️ ДОМ", "🌛 ЕЖЕМЕСЯЧНО"]`;
    - `steps [{months: 3, short: 1700, short_uzs: 20_400_000}, {months: 6, short: 4700, short_uzs: 56_400_000}]`.
    
    Тесты:
    - `shows the cushion now and how it is counted`:
      - плашка «1,3 мес.» — `bg-chip-mid`;
      - «1 300 $», «в подушке сейчас», «≈ 15 600 000 сум»;
      - строки таблицы: `["доллары на карте3 000 $", "Квартплата 18.10−700 $", "копилки−1 000 $", "подушка1 300 $"]`;
      - «Месяц жизни 12 000 000», «в среднем за июль–сентябрь», «обязательные: 🏚️ ДОМ, 🌛 ЕЖЕМЕСЯЧНО»;
      - «До 3 месяцев — ещё 1 700 $», «≈ 20 400 000 сум», «цель 6 месяцев — ещё 4 700 $».
    - `colours the months by the steps`:
      - 0.8 — «⚠️ 0,8 мес.», `bg-chip-bad`, «До 1 месяца — ещё …»;
      - 3.5 — «3,5 мес.», `bg-chip-good`, «До цели 6 месяцев — ещё …»;
      - 6.0 и `steps: []` — «✓ 6,0 мес.» и «✓ Цель 6 мес. достигнута».
    - `marks one and three months on the bar` — `aria-valuenow 1.3`, подписи «1», «3», «6 мес.».
    - `says when there are no mandatory groups` — `life_groups: [], months: null`: «обязательных групп нет в справочнике», нет плашки, полосы и ступеней.
- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_web_piggy.py` и `cd webapp && npx vitest run src/dashboard/CushionCard.test.tsx src/dashboard/SavingsTab.test.tsx`. Ожидается FAIL.
- [ ] **Step 3: реализовать.**
- [ ] **Step 4:** весь набор Python и webapp, `npm run lint`, `npm run build`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: «Подушка» shows its calculation, the month of life and the steps`

### Task 7: Q10 и O10, документы, сверка

**Files:**
- Modify `README.md`:
  - абзац «Frozen money…» — правило 1.5, подушка 1.6, ступени;
  - `CUSHION_MONTHS` в примере env — «months of mandatory spending».
- Modify `CLAUDE.md`:
  - строки `savings.py`, `day_budget.py`;
  - `google_sheets.py` — `piggy=True`;
  - «Daily limit» — Q10 «🧊 Заморожено» по тому же правилу, менять вместе с ботом;
  - `CUSHION_MONTHS`.
- Вне репозитория, рядом с worktree — `set_q10.py` и `live_q10_check.py`.

**Q10 — было** (так стоит в «Oct 26» на 30.10):

```
=SUMIFS(fact!$D:$D;fact!$G:$G;"USD";fact!$A:$A;"<"&$P$1;fact!$B:$B;"💰 ДОХОДЫ")-SUMIFS(fact!$D:$D;fact!$G:$G;"USD";fact!$A:$A;"<"&$P$1;fact!$B:$B;"<>💰 ДОХОДЫ")+SUMPRODUCT(IFERROR(($R$12:$R$19="USD")*($Q$12:$Q$19<0)*(ABS($T$12:$T$19)<0,9*ABS($Q$12:$Q$19))*($Q$12:$Q$19-$T$12:$T$19);0))
```

**Q10 — стало.** Локаль `ru_RU`: аргументы через `;`, дробь через запятую. Обозначения:
- `upto` — срок 1.5 по T: самая ранняя открытая строка ЗП (основных нет — аванса), не раньше сегодня; опоздавшая строка, по которой пришла часть денег, пропускается — ждём следующую, как `_sums_payday` (правка исполнителя 31.10); иначе первая строка ЗП в следующем месяце; строк нет — `$M$2`;
- `bills` — открытые по T долларовые платежи этого месяца по `upto`;
- `later` — строки следующего месяца целиком.

```
=ARRAYFORMULA(LET(td;$P$1;qs;$Q$12:$Q$19;cur;$R$12:$R$19;ds;$P$12:$P$19;nm;LOWER($O$12:$O$19);got;IF($T$12:$T$19="";0;$T$12:$T$19);isopen;ABS(got)<0,9*ABS(qs);dues;IF(DATE(YEAR(td);MONTH(td);ds)>$M$2;$M$2;DATE(YEAR(td);MONTH(td);ds));plus;(qs>0)*(cur="UZS");isadv;plus*REGEXMATCH(nm;"аванс");ismain;plus*REGEXMATCH(nm;"зарплат|(^|[^а-яё])зп([^а-яё]|$)")*(1-REGEXMATCH(nm;"аванс"));salary;IF(SUM(ismain)>0;ismain;isadv)>0;here;IFERROR(MIN(FILTER(dues;salary;isopen;((dues>=td)+(got=0))>0));0);firstday;IFERROR(MIN(FILTER(ds;salary));0);upto;IF(here>0;MAX(here;td);IF(firstday>0;MIN(DATE(YEAR($M$2);MONTH($M$2)+1;firstday);EOMONTH($M$2;1));$M$2));nexts;IF(DATE(YEAR($M$2);MONTH($M$2)+1;ds)>EOMONTH($M$2;1);EOMONTH($M$2;1);DATE(YEAR($M$2);MONTH($M$2)+1;ds));usd;SUMIFS(fact!$D:$D;fact!$G:$G;"USD";fact!$A:$A;"<"&td;fact!$B:$B;"💰 ДОХОДЫ")-SUMIFS(fact!$D:$D;fact!$G:$G;"USD";fact!$A:$A;"<"&td;fact!$B:$B;"<>💰 ДОХОДЫ");bills;SUMPRODUCT((qs<0)*(cur="USD")*isopen*(dues<=upto)*(got-qs));later;IF(upto>$M$2;SUMPRODUCT((qs<0)*(cur="USD")*(nexts<=upto)*(-qs));0);usd-bills-later))
```

**O10:** «🧊 Заморожено: доллары без платежей в USD до зарплаты (подушка и копилки)».

**P10** (`=Q10*system!$I$2`) не трогаем.

**`set_q10.py`** (stdlib, `python3`):

```python
"""Q10 «🧊 Заморожено» и подпись O10 вкладок месяца (этап 3).

python3 set_q10.py <план> "Nov 26" ["Dec 26"] [--write | --restore]
Формула — из блока плана, который начинается с «=ARRAYFORMULA(LET(td;». Без --write
только показывает. --write: бэкап O10:Q10 всех вкладок в q10_backup.json (он уже есть —
стоп), затем O10 и Q10 каждой вкладки (values update, USER_ENTERED), затем Q10
читается обратно — должно быть число. --restore — вернуть бэкап. ID — из SHEET_ID.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

LABEL = "🧊 Заморожено: доллары без платежей в USD до зарплаты (подушка и копилки)"
BACKUP = Path("q10_backup.json")


def gws(*args: str) -> dict:
    done = subprocess.run(["gws", *args], capture_output=True, text=True, check=True)
    answer = json.loads(done.stdout or "{}")
    if "error" in answer:
        raise SystemExit(f"gws: {answer['error']}")
    return answer


def params(sheet: str, cells: str, **more: str) -> str:
    found = {"spreadsheetId": os.environ["SHEET_ID"], "range": f"'{sheet}'!{cells}"}
    return json.dumps(found | more, ensure_ascii=False)


def get(sheet: str, cells: str, render: str = "FORMULA") -> list:
    answer = gws("sheets", "spreadsheets", "values", "get",
                 "--params", params(sheet, cells, valueRenderOption=render))
    row = (answer.get("values") or [[]])[0]
    return row + [""] * (3 - len(row))


def put(sheet: str, cells: str, row: list) -> None:
    gws("sheets", "spreadsheets", "values", "update",
        "--params", params(sheet, cells, valueInputOption="USER_ENTERED"),
        "--json", json.dumps({"values": [row]}, ensure_ascii=False))


def formula(plan: Path) -> str:
    text = plan.read_text(encoding="utf-8")
    found = re.findall(r"```\n(=ARRAYFORMULA\(LET\(td;.*?)\n```", text, flags=re.S)
    if len(found) != 1:
        raise SystemExit(f"{len(found)} Q10 formulas in the plan")
    return found[0].strip()


def main() -> None:
    if "--restore" in sys.argv:
        for item in json.loads(BACKUP.read_text(encoding="utf-8")):
            put(item["sheet"], "O10:Q10", item["row"])
            print(item["sheet"], "restored")
        return
    plan, *sheets = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    new = formula(Path(plan))
    saved = []
    for sheet in sheets:
        row = get(sheet, "O10:Q10")
        if not str(row[0]).startswith("🧊 Заморожено"):
            raise SystemExit(f"{sheet}: O10 is not the frozen row: {row[0]!r}")
        saved.append({"sheet": sheet, "row": row[:3]})
        print(sheet, "old O10:", row[0], "old Q10:", row[2], sep="\n")
    print("new Q10:", new, sep="\n")
    if "--write" not in sys.argv:
        return
    if BACKUP.exists():
        raise SystemExit(f"{BACKUP} exists: the first backup stays, move it away")
    BACKUP.write_text(json.dumps(saved, ensure_ascii=False, indent=1), "utf-8")
    for sheet in sheets:
        put(sheet, "O10", [LABEL])
        put(sheet, "Q10", [new])
        value = get(sheet, "Q10", "UNFORMATTED_VALUE")[0]
        if not isinstance(value, int | float):
            raise SystemExit(f"{sheet}: Q10 = {value!r}; roll back with --restore")
        print(sheet, "Q10 written")


if __name__ == "__main__":
    main()
```

**`live_q10_check.py`** (`../venv_dev/bin/python live_q10_check.py <worktree> <ГГГГ-ММ-ДД>`). Делает так же, как сверка P5 этапа 1а:
- читает через gws (только чтение) `system!H2:I10`, `fact` и вкладку месяца — `UNFORMATTED_VALUE`, даты `SERIAL_NUMBER`;
- курсы — `Rates`;
- `morning = day_budget(frame[frame["day"] < today], tab, today, rates, "USD")` — без сегодняшних строк бот считает утренний снимок, как вкладка;
- печатает `morning.frozen`, платежи срока и Q10 вкладки.

Числа — только в терминал, не в PR.

- [ ] **Step 1: документы.** README и CLAUDE.md по списку выше. Commit `docs: the frozen money and the cushion by the clear-money rules`.
- [ ] **Step 2: после мержа и выкладки на Render** (`/health` — 200). Скопировать план в `docs/superpowers/plans/` и запустить `SHEET_ID=… python3 set_q10.py <план> "<текущая>" "<следующая, если создана>"` — без `--write`. Expected: O10 обеих вкладок начинается с «🧊 Заморожено», печатается новая формула.
- [ ] **Step 3:** то же с `--write`. Expected: `q10_backup.json` с прежними O10:Q10, «Q10 written» по каждой вкладке. Не число — `--restore` и разбор.
- [ ] **Step 4: сверка** — `live_q10_check.py` на сегодня. Expected: Q10 = «Заморожено» бота на утро до цента; P5 — сверкой этапа 1а, до сума. Расхождение — сначала проверить, что «Пришло к утру» (T) свежая: сверка пишет её ночью и через 2 минуты после записи.
- [ ] **Step 5: снимки** на моках (`cd webapp && npm run dev`), как в прошлых этапах: «Сводка → Сегодня» (карточка «Заморожено») и «План → Накопления» («Подушка»). Размеры — 360 px в тёмной теме и 320 px с крупным шрифтом (textZoom ×1,25). Expected:
  - числа целиком;
  - плашка, подписи полосы и таблицы не уходят за край;
  - без горизонтальной прокрутки.

---

## Изменения после финального ревью (31.10)

- `frozen_split(frozen, piggies, total)`: «копилки больше карты» — только когда копилок больше, чем всего на карте; иначе нехватка — копилки + платежи срока − всего на карте. День нехватки — `short_on(total − piggies, payments)`; он прошёл — «купите X сегодня» (бот и приложение).
- «Сводка» после записи в копилки перечитывается (`store.expire(["dashboard"])`).
- Предупреждения о несчитанных копилках и непрочитанной вкладке — с причиной (`exc_info`).
- Месяц жизни сравнивает группы fact по названию без эмодзи.
- Спека §1.5–1.6 и §5: правило предупреждений, исключение «ЗП не пришла до 1-го / пришла частью», пример квартплаты — выдуманная сумма.

