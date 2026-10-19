# Сводки бота крупно и таблицами — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** утренняя сводка, /today, недельный отчёт и /plan уходят расширенными сообщениями Telegram (крупные заголовки, таблицы, числа целиком), а нынешний обычный текст остаётся запасным.

**Architecture:** чистые HTML-помощники — в новом `app/rich.py`. Блоки сообщений собираются рядом с нынешними текстовыми форматтерами (`day_budget`, `recurring`, `analytics_service`). Методы сервиса возвращают `Report(text, html)`, обработчики шлют его через `send_report` с цепочкой откатов.

**Tech Stack:** Python 3.12, python-telegram-bot 22.8 (`Bot.do_api_request`), pytest.

**Spec:** `docs/superpowers/specs/2026-10-19-readable-messages-design.md`

## Global Constraints

- PTB остаётся 22.8; `sendRichMessage` — только `bot.do_api_request("sendRichMessage", api_kwargs=…)`.
- `rich_message` = `{"html": <html>, "skip_entity_detection": True}`; `message_effect_id` — отдельным ключом, только когда эффект есть.
- Откаты: расширенное с эффектом → расширенное без эффекта → текст с эффектом → текст без эффекта; следующая попытка — только после `BadRequest` или `EndPointNotFound`, остальные ошибки летят дальше.
- `RICH_MESSAGES=off` (любой регистр, пробелы) — всем обычным текстом; иначе новый вид всем.
- Обычный текст — нынешние форматтеры без изменений (`format_day_budget`, `morning_lines`, `format_plan_report`, строки недели, `balance_reminder`); старые проверки их текста остаются.
- Числа: суммы целиком, разряды через неразрывный пробел (`NBSP`); «$» и «₽» после числа, другие валюты — кодом; «сум» — только в заголовке 1-го уровня; проценты «66 %» с `NBSP`; минус — «−» (U+2212).
- Таблица — `<table bordered striped>`, числовые ячейки `align="right"`, текст ячеек жирный.
- Всё из таблицы (названия статей, строк списка, подписок, карт) экранируется.
- Публичный репозиторий: в новых тестах только выдуманные данные; не брать в проверки квартплату из старой фикстуры `tab()` (`ITEMS`) — передавать `items=` с выдуманными строками.
- black, ruff — чисто; pytest — зелёный; conventional commits с трейлером.

## Review Focus

1. Название из таблицы с `&`, `<` или `>` (строка списка, статья, подписка, карта) показывается как есть, Telegram не отклоняет сообщение — тесты в задачах 1 и 3.
2. Telegram отказал в расширенном сообщении — та же сводка приходит обычным текстом, 🎉 по возможности сохраняется — тест в задаче 2.
3. Сеть или таймаут при отправке расширенного — без дубля обычным текстом, ошибка уходит дальше — тест в задаче 2.
4. Обычный текст недели длиннее 4096 символов — несколько сообщений, эффект только у первого — тест в задаче 2.
5. Число не разрывается переносом: `NBSP` в `whole`, `cents`, `in_currency`, `percent` — тест в задаче 1.

---

### Task 1: HTML-помощники — `app/rich.py`

**Files:**
- Create: `app/rich.py`
- Test: `tests/test_rich.py`

**Interfaces:**
- Produces:
  - `NBSP = " "`, `RULE = "<hr/>"`
  - `@dataclass(frozen=True) class Report: text: str; html: str | None = None`
  - `whole(value: float) -> str`; `cents(value: float) -> str`
  - `in_currency(amount: float, code: str, with_cents: bool = False) -> str` — UZS без единицы
  - `signed(amount: float, code: str) -> str`; `percent(share: float) -> str`
  - `heading(text: str, level: int = 3) -> str`; `para(*lines: str) -> str`
  - `table(rows: Sequence[Sequence[str]], align: str, head: Sequence[str] | None = None) -> str` — `align` по колонке: `"l"`/`"r"`

- [ ] **Step 1: Write the failing tests**

```python
from app.rich import cents, heading, in_currency, para, percent, signed, table, whole

N = " "


def test_numbers_keep_together() -> None:
    assert whole(1_234_567.4) == f"1{N}234{N}567"
    assert whole(-1500) == f"−1{N}500"
    assert cents(1234.5) == f"1{N}234,50"
    assert in_currency(380, "USD") == f"380{N}$"
    assert in_currency(75_000, "RUB") == f"75{N}000{N}₽"
    assert in_currency(50, "EUR") == f"50{N}EUR"
    assert in_currency(1234.5, "USD", with_cents=True) == f"1{N}234,50{N}$"
    assert in_currency(3_150_000, "UZS") == f"3{N}150{N}000"
    assert signed(-420, "USD") == f"−420{N}$"
    assert signed(75_000, "RUB") == f"+75{N}000{N}₽"
    assert percent(0.437) == f"44{N}%"


def test_blocks_escape_sheet_text() -> None:
    assert heading("Аренда & <офис>", 1) == "<h1>Аренда &amp; &lt;офис&gt;</h1>"
    assert heading("Вчера") == "<h3>Вчера</h3>"
    assert para("a < b", "c") == "<p>a &lt; b<br>c</p>"
    assert table([["кафе & бар", f"1{N}000"]], "lr", ["статья", "факт"]) == (
        '<table bordered striped><tr><th>статья</th><th>факт</th></tr>'
        f'<tr><td><b>кафе &amp; бар</b></td><td align="right"><b>1{N}000</b></td>'
        "</tr></table>"
    )
```

- [ ] **Step 2: Run, expect FAIL** — `pytest tests/test_rich.py -q` → `ModuleNotFoundError: app.rich`.
- [ ] **Step 3: Implement `app/rich.py`** — `html.escape(text, quote=False)` для всего текста; `whole` — `f"{abs(value):,.0f}"` с `NBSP` вместо запятых и «−» для отрицательных; `cents` — две цифры после запятой, разделитель дробной части «,».
- [ ] **Step 4: Run, expect PASS** — `pytest tests/test_rich.py -q`.
- [ ] **Step 5: Commit** — `feat: HTML blocks for rich bot messages`.

### Task 2: Отправка с откатами — `send_report` и `RICH_MESSAGES`

**Files:**
- Modify: `app/config.py` (рядом с `CELEBRATE_EFFECT_ID`), `app/handlers/analytics.py`
- Test: `tests/test_send_report.py`

**Interfaces:**
- Consumes: `Report` (задача 1), `split_text` (уже в `app/handlers/analytics.py`).
- Produces:
  - `config.RICH_MESSAGES: bool` — `os.getenv("RICH_MESSAGES", "on").strip().lower() != "off"`
  - `async def send_report(bot: Bot, chat_id: int, report: Report, effect: str | None = None) -> str` — `"rich"` или `"text"`: каким видом ушло.

- [ ] **Step 1: Write the failing tests** (бот — `SimpleNamespace(do_api_request=AsyncMock(...), send_message=AsyncMock())`, `REPORT = Report("x", "<p>x</p>")`, `RICH = {"html": "<p>x</p>", "skip_entity_detection": True}`)

```python
async def test_rich_goes_first_with_the_effect() -> None:
    bot = fake_bot()
    assert await send_report(bot, 7, REPORT, "42") == "rich"
    bot.do_api_request.assert_awaited_once_with(
        "sendRichMessage",
        api_kwargs={"chat_id": 7, "rich_message": RICH, "message_effect_id": "42"},
    )
    bot.send_message.assert_not_awaited()


async def test_a_refused_effect_is_dropped_before_the_text() -> None:
    bot = fake_bot(rich=[BadRequest("EFFECT_ID_INVALID"), None])
    assert await send_report(bot, 7, REPORT, "42") == "rich"
    assert bot.do_api_request.await_args.kwargs["api_kwargs"] == {
        "chat_id": 7, "rich_message": RICH,
    }


async def test_refused_rich_falls_back_to_the_text_with_the_effect() -> None:
    bot = fake_bot(rich=[BadRequest("bad"), BadRequest("bad")])
    assert await send_report(bot, 7, REPORT, "42") == "text"
    bot.send_message.assert_awaited_once_with(7, "x", message_effect_id="42")


async def test_no_method_without_effect_sends_the_text_once() -> None:
    bot = fake_bot(rich=[EndPointNotFound("Not Found")])
    assert await send_report(bot, 7, REPORT) == "text"
    assert bot.do_api_request.await_count == 1
    bot.send_message.assert_awaited_once_with(7, "x")


async def test_text_only_reports_and_the_switch(monkeypatch) -> None:
    bot = fake_bot()
    assert await send_report(bot, 7, Report("x")) == "text"
    monkeypatch.setattr(config, "RICH_MESSAGES", False)
    assert await send_report(bot, 7, REPORT) == "text"
    bot.do_api_request.assert_not_awaited()
    assert bot.send_message.await_count == 2


async def test_a_timeout_is_not_sent_twice() -> None:
    bot = fake_bot(rich=[TimedOut()])
    with pytest.raises(TimedOut):
        await send_report(bot, 7, REPORT, "42")
    bot.send_message.assert_not_awaited()


async def test_long_text_is_split_and_only_the_first_part_has_the_effect() -> None:
    bot = fake_bot(rich=[BadRequest("bad"), BadRequest("bad")])
    long = Report("а" * 3000 + "\n" + "б" * 3000, "<p>x</p>")
    await send_report(bot, 7, long, "42")
    first, second = bot.send_message.await_args_list
    assert first.kwargs == {"message_effect_id": "42"} and second.kwargs == {}


async def test_the_last_refusal_is_raised() -> None:
    bot = fake_bot(rich=[BadRequest("bad")], text=[BadRequest("bad")])
    with pytest.raises(BadRequest):
        await send_report(bot, 7, REPORT)
```

- [ ] **Step 2: Run, expect FAIL** — `pytest tests/test_send_report.py -q` → `ImportError: send_report`.
- [ ] **Step 3: Implement** `send_report` в `app/handlers/analytics.py`: попытки по Global Constraints (без вариантов с эффектом, когда его нет; без расширенного, когда `report.html is None` или `not config.RICH_MESSAGES`); откат — `logger.warning` с причиной; обычный текст — `split_text(report.text) or [report.text]`, эффект только у первого куска, повтор с другим эффектом — только при отказе на первом куске.
- [ ] **Step 4: Run, expect PASS** — `pytest tests/test_send_report.py -q`.
- [ ] **Step 5: Commit** — `feat: send reports as rich messages with plain fallbacks`.

### Task 3: Утро и /today

**Files:**
- Modify: `app/services/day_budget.py` (новая `rich_day_budget`), `app/services/recurring.py` (новая `rich_morning_lines`), `app/services/analytics_service.py` (`_brief`, `morning_brief`, `morning_message`), `app/handlers/analytics.py` (`send_morning_brief`, `today_command`)
- Test: `tests/test_rich_morning.py`; поправить под `Report`: `tests/test_day_budget.py`, `tests/test_live_status.py`, `tests/test_recurring.py`

**Interfaces:**
- Consumes: задачи 1–2.
- Produces:
  - `rich_day_budget(budget: DayBudget, now: bool = False) -> list[str]` — блоки 6.1/6.2 спеки в порядке макета: дата, главная цифра, вчера, план, сумы, заморожено, цель, сверх плана, впереди.
  - `rich_morning_lines(series: list[Series], today: date) -> list[str]` — «🔁 Сегодня/Завтра спишется» таблицей «название | ≈ сумма» и абзац о втором списании.
  - `AnalyticsService.morning_brief(today=None, now=False) -> Report`; `morning_message(today=None) -> tuple[Report, bool]`. `text` — нынешний текст; `html` — `"".join(rich_day_budget(...) + rich_morning_lines(...))`; без вкладки или без списка — `html=None`.
  - `send_morning_brief` логирует, каким видом ушло (`"Morning brief sent as %s…"`); `today_command` шлёт `send_report(context.bot, update.effective_chat.id, report)`.

Тексты блоков — по спеке 6.1–6.2, числа через `whole`/`in_currency`/`signed`. «≈» перед суммой — с `NBSP`. Сумма подписки не в сумах: `f"≈{NBSP}{in_currency(amount, code, with_cents=…)} ({whole(uzs)})"`.

- [ ] **Step 1: Write the failing tests** (`LIST = [("Аванс", 15, 2_000_000, "UZS"), ("ЗП", 30, 10_000_000, "UZS"), ("Аренда & Ко", 18, -420, "USD")]` — как старая `ITEMS`, но с выдуманной арендой; бюджет — `budget(day, *rows, items=LIST, …)` из `tests/test_day_budget.py`; ожидаемые числа берутся из полей бюджета `b`)

```python
def test_morning_blocks_follow_the_mockup() -> None:
    b = budget(date(2026, 10, 2), OVERSPENT, inventory_fact=640_000, items=LIST)
    blocks = rich_day_budget(b)
    assert blocks[:8] == [
        para("☀️ Пятница, 2 октября"),
        heading(f"💸 {whole(b.limit)} сум", 1),
        para("можно потратить сегодня", f"по плану месяца — {whole(b.plan_per_day)} в день"),
        heading(f"⚠️ Вчера {whole(b.yesterday_spent)} из {whole(b.yesterday_limit)}"),
        para(f"больше лимита на {whole(b.yesterday_spent - b.yesterday_limit)}"),
        heading(f"⚠️ Отстаём от плана на {whole(b.planned_balance - b.balance)}"),
        para(f"на картах {whole(b.balance)}", f"по плану {whole(b.planned_balance)}"),
        heading(f"💳 Сумы {whole(b.runway.balance)}"),
    ]
    assert para("до конца месяца хватит") in blocks
    assert heading(f"🧊 Заморожено {in_currency(b.frozen, 'USD', with_cents=True)}") in blocks
    assert table([["инвентарь", whole(640_000), whole(500_000)]], "lrr",
                 ["статья", "факт", "план"]) in blocks
    assert blocks[-1] == table(
        [["15.10", "Аванс", signed(2_000_000, "UZS")],
         ["18.10", "Аренда & Ко", signed(-420, "USD")]], "llr")
    assert "Аренда &amp; Ко" in "".join(blocks)


def test_today_shows_what_is_left_or_the_overspend() -> None:
    coffee = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 30_000)
    b = budget(date(2026, 10, 2), OVERSPENT, coffee, items=LIST)
    assert rich_day_budget(b, now=True)[1:3] == [
        heading(f"💸 {whole(b.limit - 30_000)} сум", 1),
        para("осталось на сегодня", f"лимит {whole(b.limit)}, потрачено {whole(30_000)}",
             f"по плану месяца — {whole(b.plan_per_day)} в день"),
    ]
    spree = fact_row(date(2026, 10, 2), "🍔 ЕДА", "кафе", 10 * b.limit)
    over = budget(date(2026, 10, 2), OVERSPENT, spree, items=LIST)
    assert rich_day_budget(over, now=True)[1] == heading(
        f"⚠️ Сверх лимита на {whole(over.spent_today - over.limit)}", 2)


def test_no_limit_goal_and_the_first_day() -> None:
    spree = fact_row(date(2026, 10, 1), "🍔 ЕДА", "кафе", 8_000_000)
    b = budget(date(2026, 10, 2), spree, items=LIST)
    assert rich_day_budget(b)[1:3] == [
        heading("💸 Лимита на сегодня нет", 2),
        para(f"до плана на конец месяца не хватает {whole(-b.limit * b.days_left)}",
             f"по плану месяца — {whole(b.plan_per_day)} в день"),
    ]
    first = budget(date(2026, 10, 1), goal=500_000, items=LIST)
    blocks = rich_day_budget(first)
    assert heading(f"Вчера {whole(0)}") in blocks
    assert not any("Отстаём" in block for block in blocks)
    assert heading(f"🎯 Цель отложить {whole(500_000)}") in blocks
    assert para("план её покрывает") in blocks


def test_runway_and_frozen_variants() -> None:
    b = budget(date(2026, 10, 2), items=LIST)
    short = Runway(5_000_000, 0, date(2026, 10, 23), 1_000_000, date(2026, 10, 30), 370)
    blocks = rich_day_budget(replace(b, runway=short, frozen_change=-117.45))
    assert para("хватит до 23.10", f"до 30.10 обменять ≈{NBSP}{in_currency(370, 'USD')}") in blocks
    assert para(f"≈{NBSP}{whole(b.frozen_uzs)} сум", f"с 1-го числа −{in_currency(117.45, 'USD', with_cents=True)}") in blocks


def test_due_subscriptions_are_a_table() -> None:
    found = find_series(history(*NOTES), date(2026, 10, 13))
    assert rich_morning_lines(found, date(2026, 10, 13)) == [
        heading("🔁 Завтра спишется"),
        para(f"Cloud Notes: ≈{NBSP}{whole(52_000)}"),
    ]
    twice = find_series(history(*MUSIC), date(2026, 10, 5))
    assert rich_morning_lines(twice, date(2026, 10, 5)) == [
        para("⚠️ Music Box списан второй раз за октябрь: 03.10, 04.10")
    ]


def test_service_report_carries_both_views() -> None:
    fact = [FACT_HEADER, *OPENING, OVERSPENT]
    rows = tab(inventory_fact=640_000, items=LIST)
    report = AnalyticsService(Sheets({"fact": fact, "Oct 26": rows})).morning_brief(date(2026, 10, 2))
    b = budget(date(2026, 10, 2), OVERSPENT, inventory_fact=640_000, items=LIST)
    assert report.text == format_day_budget(b)
    assert report.html == "".join(rich_day_budget(b))
    no_tab = AnalyticsService(Sheets({})).morning_brief(date(2026, 11, 1))
    assert no_tab.html is None
```

Обработчики: `send_morning_brief` с сервисом `morning_message=lambda: (Report("☀️", "<p>☀️</p>"), True)` и `CELEBRATE_EFFECT_ID="42"` → `do_api_request` с `message_effect_id="42"`; `today_command` → `send_report` в `update.effective_chat.id` через `context.bot` (`morning_brief(now=True)`). Старые проверки текста — через `.text`.

- [ ] **Step 2: Run, expect FAIL** — `pytest tests/test_rich_morning.py -q` → `ImportError: rich_day_budget`.
- [ ] **Step 3: Implement** `rich_day_budget`, `rich_morning_lines`, `Report` в `_brief`/`morning_brief`/`morning_message`, обработчики; поправить старые тесты под `Report`.
- [ ] **Step 4: Run, expect PASS** — `pytest -q` (весь набор).
- [ ] **Step 5: Commit** — `feat: morning summary and /today as rich messages`.

### Task 4: Неделя и /plan

**Files:**
- Modify: `app/services/analytics_service.py` (новые `rich_plan_report`, `week_title`; `plan_report`, `weekly_digest` → `Report`), `app/handlers/analytics.py` (`plan_command`, `send_weekly_digest`)
- Test: `tests/test_rich_plan.py`; поправить под `Report`: `tests/test_plan.py`

**Interfaces:**
- Consumes: задачи 1–2.
- Produces:
  - `rich_plan_report(rows: list[list[Any]], today: date) -> list[str]` — спека 7.2.
  - `week_title(start: date, end: date) -> str` — «12–18 октября» / «28 сентября – 4 октября».
  - `AnalyticsService.plan_report(today=None) -> Report`; `weekly_digest(today=None) -> Report` (без вкладки — `html=None` у /plan; у недели вместо план-факта абзац с тем же текстом, что сейчас).
  - Слово для дней: «дня», если число кончается на 1 (кроме 11), иначе «дней».
  - Нет плана расходов — «плана на месяц нет» и без строки «Осталось/План превышен»; нет плана доходов — «плана на месяц нет».

- [ ] **Step 1: Write the failing tests** (выдуманная вкладка `FALL` в самом тесте: продукты 10 000 000/2 500 000, инвентарь 400 000/410 000, подписка 1 000 000/2 000 000, кафе 1 000 000/900 000, сезонный 0/250 000, зарплата 30 000 000/0)

```python
def test_plan_blocks_follow_the_mockup() -> None:
    assert rich_plan_report(FALL, date(2026, 10, 6)) == [
        heading("📋 Октябрь: 6 из 31 дня", 2),
        heading(f"⚠️ Расходы {whole(6_060_000)}"),
        para(f"из {whole(12_400_000)} — {percent(0.49)}", f"быстрее графика: прошло {percent(0.19)} месяца"),
        heading(f"Осталось {whole(6_340_000)}"),
        para(f"≈{NBSP}{whole(243_846)} в день"),
        heading("🔴 Сверх плана"),
        table([["подписка", whole(2_000_000), whole(1_000_000)],
               ["инвентарь", whole(410_000), whole(400_000)]], "lrr", ["статья", "факт", "план"]),
        heading(f"🟡 Почти всё (от {percent(0.8)})"),
        table([["кафе", whole(900_000), whole(1_000_000)]], "lrr", ["статья", "факт", "план"]),
        heading("⚪ Вне плана"),
        table([["🎉 сезонный", whole(250_000)]], "lr"),
        heading(f"💰 Доходы {whole(0)}"),
        para(f"из {whole(30_000_000)} — {percent(0)} плана"),
    ]


def test_days_word_and_a_month_without_a_plan() -> None:
    assert rich_plan_report(FALL, date(2026, 11, 3))[0] == heading("📋 Ноябрь: 3 из 30 дней", 2)
    empty = [HEADER, tab_row("Расходы", "🍔 ЕДА", "кафе", "UZS", 0, -50_000)]
    blocks = rich_plan_report(empty, date(2026, 10, 6))
    assert blocks[1:3] == [heading(f"Расходы {whole(50_000)}"), para("плана на месяц нет")]
    assert not any("Осталось" in b or "превышен" in b for b in blocks)


def test_week_titles() -> None:
    assert week_title(date(2026, 10, 12), date(2026, 10, 18)) == "12–18 октября"
    assert week_title(date(2026, 9, 28), date(2026, 10, 4)) == "28 сентября – 4 октября"


def test_weekly_report_keeps_the_text_and_adds_the_blocks() -> None:
    service = AnalyticsService(Sheets({"fact": FACT, "Oct 26": FALL}))
    report = service.weekly_digest(date(2026, 10, 6))
    assert report.text.splitlines()[0] == "🗓 Неделя 30.09–06.10"
    assert report.html.startswith(para("🗓 Неделя 30 сентября – 6 октября") + "<h1>")
    assert report.html.count(RULE) == 2
    assert "".join(rich_plan_report(FALL, date(2026, 10, 6))) in report.html
    assert report.html.endswith(para("📸 Пришлите скрины главных экранов банков — сверю остатки по картам."))
```

Обработчики: `plan_command` и `send_weekly_digest` шлют `send_report`; ошибки сборки — как сейчас. Старые проверки текста — через `.text`.

- [ ] **Step 2: Run, expect FAIL** — `pytest tests/test_rich_plan.py -q` → `ImportError: rich_plan_report`.
- [ ] **Step 3: Implement** по Interfaces; неделя читает вкладку месяца один раз для обоих видов.
- [ ] **Step 4: Run, expect PASS** — `pytest -q`.
- [ ] **Step 5: Commit** — `feat: weekly report and /plan as rich messages`.

### Task 5: Документация и живая сверка

**Files:**
- Modify: `README.md` (раздел сообщений бота: новый вид, `RICH_MESSAGES`), `CLAUDE.md` (как шлются сводки: `Report`, `send_report`, откаты, `do_api_request`)

- [ ] **Step 1:** README и CLAUDE.md — по строке-две, без реальных данных.
- [ ] **Step 2:** живая сверка вне репозитория: HTML утра и недели из выгрузки таблицы новым кодом → страница с CSS макета → скриншот 390 px; ничего не переносится и совпадает с макетом 8.
- [ ] **Step 3:** `black --check app tests`, `ruff check app tests`, `pytest -q` — чисто.
- [ ] **Step 4: Commit** — `docs: rich bot messages and the RICH_MESSAGES switch`.
