# Ответы бота после записи крупно — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ответ после записи, «🐷», заметки второму, /subs, «уже записано» и /undo уходят расширенными сообщениями Telegram в виде из спеки; нынешний текст — запасной.

**Architecture:** отправка и правка — `send_report`/`edit_report` в `app/handlers/analytics.py` (клавиатура, список отправленных сообщений); блоки — рядом с нынешними строками (`day_budget`, `recurring`, `messages`, `piggy`); общий разбор заметки «заголовок: подробности» — `app/rich.py:note_report`.

**Tech Stack:** Python 3.12, python-telegram-bot 22.8 (`Bot.do_api_request`), pytest.

**Spec:** `docs/superpowers/specs/2026-10-19-readable-replies-design.md` (продолжает `2026-10-19-readable-messages-design.md`)

## Global Constraints

- Всё из Global Constraints плана сводок `docs/superpowers/plans/2026-10-19-readable-messages.md` (откаты, `RICH_MESSAGES`, числа через `app/rich.py`, экранирование, только выдуманные данные в тестах).
- Обычный текст сообщений и строки ответа Mini App (`SaveResult.lines`, `undo_last`) — без изменений; старые проверки их текста остаются.
- `reply_markup` — PTB-объект в `api_kwargs` (PTB сам переводит в JSON); у разбитого на куски текста клавиатура — у последнего куска, эффект — у первого.
- Правка (`editMessageText`) идёт той же цепочкой: `rich_message` → при отказе `text`; без клавиатуры — ключ `reply_markup` не передаётся (Telegram снимает кнопки).

## Review Focus

1. Ответ после записи без новых строк (только «⚠️ Не записал») или с ошибкой записи — сообщение всё равно уходит, клавиатура карт на месте — тест в задаче 3.
2. Название магазина или копилки с `&`/`<` — показывается как есть — тесты в задачах 3 и 4.
3. Нажатие «Отложил всё» на сообщении, ушедшем текстом (откат), — правка текстом проходит — тест в задаче 1.
4. Пачка строк в разных валютах или с поступлением — заголовок «✅ Записал N строк» без суммы — тест в задаче 3.
5. Отправленные сообщения ответа запоминаются для удаления при следующей записи (все куски) — тест в задаче 3.

---

### Task 1: Отправка с клавиатурой, правка и разбор заметки

**Files:** Modify `app/handlers/analytics.py`, `app/rich.py`; Test `tests/test_send_report.py`, `tests/test_rich.py`

**Interfaces — Produces:**
- `class Sent(NamedTuple): kind: str; messages: tuple[Message, ...]` — `kind` «rich»/«text», `messages` — все отправленные сообщения.
- `send_report(bot, chat_id, report, effect=None, reply_markup=None) -> Sent` (было `-> str`; вызовы утра и недели логируют `sent.kind`).
- `edit_report(bot, chat_id, message_id, report, reply_markup=None) -> str` — «rich»/«text».
- `note_report(text: str) -> Report` в `app/rich.py`: первая строка до первого «: » (или вся первая строка без завершающего «:») — заголовок 3-го уровня; остаток первой строки и остальные строки — абзац; `text` отчёта — исходный текст.

- [ ] **Step 1: failing tests** — `test_rich_carries_the_keyboard_and_returns_the_message` (`api_kwargs["reply_markup"] is markup`, `do_api_request` вызван с `return_type=Message`, `sent == Sent("rich", (message,))`); `test_text_keeps_the_keyboard_on_the_last_part` (длинный текст: клавиатура только у второго куска, эффект только у первого, `sent.messages` — оба); `test_edit_tries_rich_then_text` (`editMessageText` с `rich_message` → `BadRequest` → `bot.edit_message_text(chat_id=…, message_id=…, text=…)`, без `reply_markup`, если его не дали); `test_note_report_splits_the_head` (`note_report("📅 Жена — новая запись: 21.10 Кружок 180 000 UZS")` → `heading("📅 Жена — новая запись") + para("21.10 Кружок 180 000 UZS")`; многострочная с «:» в конце первой строки; без «:» — вся строка заголовком).
- [ ] **Step 2: run, expect FAIL** (`ImportError: Sent` / `note_report`).
- [ ] **Step 3: implement**; поправить тесты сводок под `Sent`.
- [ ] **Step 4: run, expect PASS** — `pytest -q`.
- [ ] **Step 5: commit** — `feat: rich reports with keyboards, edits and notes`.

### Task 2: Статьи месяца и «на сегодня» блоками

**Files:** Modify `app/services/day_budget.py`, `app/services/analytics_service.py` (`write_feedback`), `app/handlers/messages.py` (`_plan_feedback`); Test `tests/test_rich_replies.py`

**Interfaces — Produces:**
- `rich_plan_status(line: PlanLine | None, subcategory: str, today: date) -> list[str]` — 2 блока по спеке 2.1 (имя — подкатегория с заглавной: «Кофе»).
- `rich_today_line(budget: DayBudget) -> list[str]` — 2 блока по спеке 2.1.
- `class Feedback(NamedTuple): lines: list[str]; blocks: list[str]`; `write_feedback(...) -> Feedback`; `_plan_feedback(...) -> Feedback` (пустой — `Feedback([], [])`).

- [ ] **Step 1: failing tests** (выдуманные статьи и суммы): 🟢 `[heading("🟢 Такси 365 000"), para("из 900 000 — 41 %")]`; 🟡 `… para("из 1 500 000 — 86 %, осталось 213 000")`; 🔴 `… para("из 9 000 000 — сверх на 380 000")`; ⚪ план 0 `[heading("⚪ Сезонный 2 650 000"), para("вне плана — всё за октябрь")]`; ⚪ нет строки `[heading("⚪ Сезонный"), para("нет в плане месяца")]`; на сегодня — три варианта; `write_feedback` отдаёт те же `lines`, что раньше, и `blocks`.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: plan status and today blocks for replies`.

### Task 3: Ответ после записи

**Files:** Modify `app/handlers/messages.py` (`save_rows`, `_balance_check` → плюс блоки сверки, `_report_duplicate`), `app/handlers/common.py` (`show_main_menu`); Test `tests/test_rich_replies.py`

**Interfaces — Produces:**
- `record_blocks(rows, flags, balances, checks, icons) -> list[str]` (в `messages.py`): одна строка — спека 2.1 «одна строка»; несколько — заголовок «✅ N траты — сумма» только если все — траты в одной валюте, иначе «✅ Записал N строк(и)»; таблица «сумма | статья».
- `show_main_menu(chat, context, success: Report | str | None = None, …)` — сводку шлёт `send_report(context.bot, chat.id, report, reply_markup=sources_keyboard(…))`, запоминает все `sent.messages`.
- `save_rows` собирает `Report(text, html)`: `text` — нынешний; `html` — блоки записи, сверки, статей, «на сегодня», остальные строки абзацем.

- [ ] **Step 1: failing tests** — одна трата (заголовок «✅ 48 000», абзац «☕ кофе · 🍔 ЕДА», «VISA 1111 UZS: 4 205 330», «✓ сходится с банком»); поступление «✅ +31 000 000»; валюта «✅ 380 $»; пачка трат «✅ 3 траты — 136 400» с таблицей; пачка с поступлением «✅ Записал 2 строки»; пометка «🆕 A & B» экранируется; расхождение «⚠️ в банке …, в таблице …» и «разница …»; только «⚠️ Не записал» — абзац; `show_main_menu` шлёт `sendRichMessage` с клавиатурой карт и запоминает сообщения; «⚠️ Это уже записано» — заголовок и абзац.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS (старые проверки `chat.send_message` в тестах ответа — под новую отправку).
- [ ] **Step 5: commit** — `feat: the reply after a record as a rich message`.

### Task 4: «🐷»

**Files:** Modify `app/handlers/piggy.py` (`income_prompt`, `_spend_lines`, `spend_note`, обработчики кнопок); Test `tests/test_rich_piggy.py`

**Interfaces — Produces:** `income_prompt` и `spend_note` шлют `send_report(..., reply_markup=…)`; `_spend_lines(...) -> tuple[list[str], list[str], InlineKeyboardButton | None]` (строки, блоки, кнопка); правки по кнопкам — `edit_report`.

- [ ] **Step 1: failing tests** — поступление в сумах: `heading("🐷 Отложить 930 000", 2)`, `para("3 % с поступления 31 000 000")`, таблица «копилка | сумма» (выдуманная копилка «Отпуск & море»); поступление в долларах: «🐷 Отложить 30 $», абзац «3 % с поступления 1 000 $» и «≈ 360 000 сум»; «всё в подушку» — без кнопок; трата: `heading("🐷 Из копилки −2 400 000")`, `para("«Отпуск & море» — осталось 3 600 000")`; «Отложил всё» и «Не из копилки» правят через `edit_report`.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: piggy bank messages as rich messages`.

### Task 5: Заметки, /subs, /undo

**Files:** Modify `app/handlers/common.py` (`notify_others`), `app/services/recurring.py` (`rich_subs`), `app/services/analytics_service.py` (`subs_report -> Report`), `app/handlers/analytics.py` (`subs_command`), `app/handlers/undo.py`; Test `tests/test_rich_replies.py`

**Interfaces — Produces:** `notify_others` шлёт `note_report(text)` через `send_report`; `rich_subs(series, today, plan=None) -> list[str]` — спека 2.4 (группы «⏳ Ждём в <месяце>», «✅ Списано», «⚠️ Дважды за <месяц>», «❓ Не было в <месяце>», «Давно не было»); `subs_report(today=None) -> Report`; /undo — `send_report(note_report(text))`.

- [ ] **Step 1: failing tests** — заметка второму уходит `sendRichMessage` с заголовком и абзацем; /subs группами на выдуманных подписках (в т. ч. «❓ Не было в октябре»); /undo — «↩️ Удалил из таблицы» заголовком.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: notes, /subs and /undo as rich messages`.

### Task 6: Документация и живая сверка

- [ ] README/CLAUDE.md — строка о новых ответах.
- [ ] Живая сверка вне репозитория: ответ на запись из выгрузки таблицы (кофе, пачка из трёх, поступление) → макет 390 px, переносов нет.
- [ ] `black`, `ruff`, `pytest -q` — чисто; commit `docs: rich replies after a record`.
