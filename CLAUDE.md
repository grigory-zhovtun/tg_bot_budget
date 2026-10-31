# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Telegram Finance Bot for personal finance tracking. Records income/expenses to Google Sheets with AI-powered transaction parsing via Google Gemini. Supports manual entry, SMS parsing, image/receipt recognition, and scheduled analytics. Deployed on Render as a web service (Telegram webhook, later the Mini App API and page; uvicorn + Starlette), Python 3.12.

## Commands

```bash
# Setup
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
pip install -r requirements-dev.txt

# Checks (the same run in CI)
ruff check . && black --check . && pytest -q

# Run (web service when WEBHOOK_URL/RENDER_EXTERNAL_URL is set; otherwise polls — never while a webhook is set, unless FORCE_POLLING=True)
python -m app.main

# Mini App
cd webapp && npm ci && npm run lint && npm test && npm run build
```

## Architecture

```
app/
├── main.py              # build_application(): handlers, access gate, error handler, daily job; main() runs it
├── config.py            # Environment variables and constants
├── auth.py              # Allowlist gate (TypeHandler in group -1)
├── domain.py            # Sheet rules: sign of the amount, currency conversion, category/source/date validation
├── errors.py            # Safe one-line error texts for the chat, application error handler
├── custom_icons.py      # Custom emoji pictures for buttons: pack_map/match by the plain emoji (system!D, group names)
├── chat_state.py        # ChatState per chat (keyboards on/off, «уборка с №», «не удалять №») in system!Q:U; update_state/keep_messages under one lock
├── message_log.py       # MessageLog: last message number of each local day per chat; LoggingRequest logs every sent message from Telegram's answers
├── statements.py        # Kapitalbank PDF statements: parsing, own-transfer pairs, categories, reconcile with fact
├── web/
│   ├── server.py        # Starlette: POST /telegram (webhook → PTB update_queue), GET /health, security headers, PTB lifespan
│   ├── auth.py          # Mini App auth: initData (HMAC with the bot token), signed launch links for the keyboard button, webhook secret
│   ├── api.py           # /api/bootstrap, /api/expenses (entry_row → save_rows, idempotent by entry_id), /api/expenses/undo, /api/dashboard, /api/forecast, /api/calendar (POST, PUT/DELETE by ID), /api/savings, /api/piggy (POST; PUT by ID; /save, /{id}/close|moves|exclude; PUT /percent, /order, /life — the mandatory groups of the month of life into S1, spending groups of the catalog in its order)
│   └── schemas.py       # Pydantic models of the API
├── handlers/
│   ├── common.py        # /start, show_main_menu (summary and buttons by the chat's keyboards: card:… on the summary when both are off), keyboard helpers, message tracking, ANCHOR — the message holding the cards keyboard
│   ├── settings.py      # /keyboards (kb:groups|cards:on|off → system!Q:U, shown at once; «⌨️ Карты скрыты» kept) and /app (web_app button)
│   ├── cleanup.py       # observe (incoming messages → MessageLog), sweep at the morning job (yesterday since «уборка с №», batches of 100, keeps digests/removals/ANCHOR), first_run («⌨️ Клавиатуры скрыты» once)
│   ├── admin.py         # /reboot (reload categories from sheets)
│   ├── messages.py      # Text/photo/document handling, manual entry, AI parsing flow
│   ├── statement_import.py # Statement preview (JobQueue debounce), confirm buttons, apply_plan
│   ├── transactions.py  # Callback query handler for inline buttons
│   ├── undo.py          # /undo: delete the last write after checking the rows are unchanged
│   ├── fix.py           # /fix: new group/subgroup for a row of the last write (B:D, checked like /undo)
│   ├── icons.py         # /icons: custom emoji message → whole pack (get_custom_emoji_stickers/get_sticker_set) → system!J; load_custom_icons on start
│   ├── live.py          # Reactions on the user's SMS (👀 while reading; 👍 written in part, 🤔 nothing — a fully written one is deleted) and «Думаю…» via send_message_draft (fallback 🔍)
│   ├── last_write.py    # Write buttons: last:… in the menu under the summary (text edits), card:… on the summary itself (markup edits; «Да» → undo_rows, the summary deleted)
│   ├── month.py         # Daily job: month plan tab exists (copy of the last one), owner notified
│   ├── income_month.py  # «📅 Эти … — за октябрь?» in the background (`ask_month`, from save_rows and /fix into incomes) after an income without a mark while last month waits in that currency (in sums — a salary/advance while last month waits for its salary/advance); the button (`mon:<fact row>:<YYYY-MM>`) writes «за <месяц>» into the comment (`comment_row`, only an unchanged row; refusals re-read the row with `fact_row_cells`) and the last write
│   ├── piggy.py         # «🐷» in the chat: income_prompt (the percent of the income, «Отложил всё» → «set aside now», «Отложил часть…» → app `?open=save`), spend_note («🐷 Из копилки…», «Не из копилки»); journal row and note helpers shared with api.py
│   ├── receipts.py      # Job every few minutes after writes: «Пришло к утру» (this month's incomes by date or «за <месяц>» mark) and the subscriptions until the salary and until the advance next to the tab's limit formula (sync_receipts → Q and R of the «Лимит на сегодня» row), then the piggy banks (sync_piggy)
│   ├── balances.py      # Bank main-screen screenshots: «Проверка» + «Сверено», «Выровнять» buttons
│   └── analytics.py     # /advice, /analytics, daily report
├── services/
│   ├── google_sheets.py # GoogleSheetsService — synchronous gspread wrapper; copies of sheets for Mini App screens (`get_values`/`get_rates(max_age)`), dropped by any write of the bot (`WriteAwareHTTPClient` reports every POST/PUT/DELETE); `read_month_sheets` reads the month tab, last month's tab only with `previous=True` («Сводка»: its remainders in «Впереди»; a failed read of it — no carry-over), the «Копилки» and «Журнал копилок» sheets only with `piggy=True` («Сводка»: the piggies inside «Заморожено»; a missing sheet — empty, a failed read — `MonthSheets.piggy` None and no breakdown), rates, fact and «Календарь» in parallel (`MONTH_SHEETS` = 7 threads)
│   ├── ai_service.py    # GeminiService (google-genai, JSON schema output, merchant hints from fact)
│   ├── analytics_service.py # AnalyticsService - reports with matplotlib charts
│   ├── recurring.py     # Monthly payments from fact: recurring_key, amount clusters, day ±4, /subs text and morning lines
│   ├── dashboard.py     # Mini App «Сводка»: build_dashboard — DayBudget, plan_lines by group, read_daily (O:R under «Дата»), subscription states, spending by group from fact rows (spent_groups), plan vs fact by category (categories: the tab's plan + spent_groups) and articles without a month-tab row (untracked)
│   ├── savings.py       # «Накопления»: the emergency fund (`Cushion` — «Заморожено» now without the piggies now, in months of life: `life_month` — the average spending of the mandatory groups found in the catalog by `life_groups(catalog, chosen)` (chosen — S1 of «Копилки», empty — `LIFE_GROUPS`), over the full months of `average_months`; `shown_months` one decimal down, `steps` 1 / 3 / CUSHION_MONTHS) and the savings rate (income − spending) / income of 6 full months (`Savings.rate` — 3 months, `half_rate` — 6, `worst` — the month of the lowest rate of the 3 when they are in minus, `current` — this month from the 1st to today); save_lines — «🛟 Отложите N %» in the write summary of an income
│   ├── cards.py         # «Сводка»: card balances from the fact balance block (I name, J «Текущий баланс», N «Проверка», Q «Сверено»), the bank check
│   ├── forecast.py      # Mini App «Календарь» (was «Прогноз»): money until the month end per currency and in total, plan and pace lines, list rows and calendar records as events; the days by fact up to today — evening, change, spending and income, totals and by article (`PastDay.articles`); today is what already happened
│   ├── piggy.py         # Piggy banks: read_book («Копилки» + «Журнал копилок», Q1 percent, O order), windows of spending, balances, the plan by the percent and the importance order (_funded, then the leveled steps under the caps of `_reserves`, `_level`), the rest to the cushion and goals; spec 1.7 from `HALF_FROM` (November 2026): while the cushion on the 1st (`Screen.cushion_start` — `frozen_start` minus the piggies of the previous evening) is under a month of life, half of the share goes to it first — in the plan (`cushion_need`, `_first_step` with the half) and in what to set aside now; `cushion_saved` — its real growth since the 1st, `cushion_room` — what it still lacks of its part; savings_screen («Накопления», set aside now), parts_of / distribute («Отложил…»), income_cushion
│   ├── piggy_sync.py    # Background piggy writes: a closed window → a journal row «потрачено в окне» and the next year's deadline, bot columns J and L, formulas I and K
│   ├── calendar.py      # «Календарь» sheet: records (once, every day, days of the week, once a month), their days, closing by fact, sum hints
│   ├── receipts.py      # The yellow list against fact: incomes by the month's plan, without a window (a currency's incomes from the 1st fill its rows in day order, the overflow to the next, the rest to the last), payments — one spending ±10 % from 5 days before; 90 % closes a row; an income's month is `income_month` — the «за <месяц>» mark in its comment (`MONTH_MARK`), otherwise its date; last month's remainders (`day_budget.carried_items`: its plus rows under 90 % with the marked late parts, one month back, virtual rows −1, −2, … on day 1) are lines of «Впереди» only, never in the queue or the sheet
│   ├── runway.py        # «💳 Сумы» card: its own count by days until the main salary that has not come yet (one due today or late — today; the salary itself not added): sums now − today's rest of the pace − pace a day, advances in, list payments and subscriptions out on their day (`horizon_runway`) → last day, gap, what is left, dollars to exchange; sums now, pace and today's spending come from `currency_forecast`
│   └── day_budget.py    # Daily limit until the next main salary in sums ((sums + advances before it − payments up to its day) / days through that day, a guard to the advance when the money before it is short; subscriptions via recurring; the money of the horizon by days — `_advance_days`, `_bill_days`, `_subscription_days`), on the salary day `DayBudget.tomorrow` — the limit from tomorrow by the sums now (`allowance_on(today + 1)`; a day of the next month — empty reconciliation, the tab's rows on the same days in full), the plan line, frozen money (spec 1.5: the dollars now − the open dollar payments of the list until the salary that has not come — `_sums_payday`, the horizon of «Сумы»; `_payment_days` → `DayBudget.frozen_payments`, to the cent; `frozen_split` → cushion / piggies over the card / dollars short, `short_on` — the day they run out; `frozen_start` — the frozen money on the morning of the 1st by the same rule (the dollars of the last evening of the previous month without the payments due until the month's first salary, empty reconciliation) — the cushion's level on the 1st; `piggies` — filled by the reader of the piggy sheets), morning text
└── utils/
    └── keyboards.py     # Keyboards: icons from system!D (`with_icon`), prompts naming card/choice (inline buttons are as wide as the message), `action(..., style)` coloured buttons
webapp/                  # Mini App: React + TS + Tailwind (Vite), served at /app/; src/entry — entry screen (tiles: plan.ts planLine → head and whole sum on two lines; Keypad big — only here, the windows keep the shared small one), src/plan — «План» (PlanEvents — the list as two-line buttons without stripes, History.tsx — the article window's history table and quick buttons, notice.ts — «head: details» notices; Sheet and AmountPad are shared by every window), src/dashboard — «Сводка» in three sections «Сегодня / Траты / Впереди» (section.ts keeps the chosen one in localStorage `dashboard-section` through src/choice.ts readChoice/saveChoice, a remembered «savings» opens «Сегодня»; SectionSwitch takes its sections as props; SavingsTab is «Накопления», the second section of «План» — plan/section.ts, `plan-section`, its own «Накопления · октябрь» line with «Обновить», `?open=save` opens it with «Отложил» once; its first card «Откладываем N %» with «Изменить» counts X = `month.due` («за октябрь отложить X»), Y = `month.saved` (the piggies since the 1st) + `month.cushion_saved` («Отложено подушке» — the cushion's real growth) and Z = max(X − Y, 0): a bar Y / X with «отложено Y, ещё Z» (with the piggies written, `month.left <= 0`, one more line says the rest is the cushion's part to buy in dollars), or «✓ Октябрь: отложено Y» once Z = 0; then «N % от пришедших …» and the table «куда | отложено» (piggies with `saved_month` in their currency, «подушка» in sums always); one warning per problem with a «что делать» line after it; «Отложил…» (SaveSheet) while `month.left > 0`, by default `left + cushion_room`, laid out like the server's `parts_of(after_saved=True)`; the details in PercentSheet; blocks.tsx — the big-text bricks: Card, Big 40 px, Fact 21 px, Line 17 px, Warn ⚠️ orange, Good ✓ green, Chip badge white on #b42318/#8a4f00/#146c2e, Table, Items two-line list; numbers via format.ts money/inCurrency/moneyText/signedMoney like app/rich.py, no «млн» on the summary and in its windows; controls.tsx — big field, label, chip, segment and field-error of the piggy windows, since fields.tsx stays as «Календарь»'s record window has it and «Календарь» keeps its look), src/api.ts — API client; src/store.ts — answers of the launch and today's copy of them in the phone (`app-cache`, per build and Telegram user): a screen shows them at once and asks the server in the background, the first request of a launch and «Обновить» with `?fresh=1`, App loads «Сводка», then «Календарь» and the month's plan in the background, writes expire what they change; src/index.css — background, text and buttons from the Telegram theme, all other text colors our own high-contrast palette (≥7:1 on light and dark themes, dark values under data-scheme="dark" set by telegram.followScheme; no red text — warnings orange, below zero a red cell/chip with white numbers; table stripes are their own --color-stripe, never bg-text/N — without color-mix old WebViews paint the row in the text colour), checked by src/theme.test.ts; text sizes text-big (min(2.5rem, 11vw)), text-fact, text-line, text-tab
tests/                   # pytest, fakes for Sheets/Gemini/Telegram; no network
```

## Key Data Flows

**Manual Entry** (groups on in /keyboards): /start → source → category → subcategory → "amount comment" (`+` prefix = incoming money) → `domain.manual_row` → `fact`

**Clean chat**: settings per chat in `system!Q:U` (`ChatState`, read on start and by /reboot) → `show_main_menu` builds the summary by them; a fully written input is deleted (`messages._save_sms`, the Gemini path); the morning job → brief (`cleanup.brief_markup`: cards keyboard or app button) → `cleanup.sweep` → per chat `MessageLog.last_before(today)` → delete `[start, last]` minus keep/ANCHOR → `start = last + 1`

**Mini App entry**: «📱 Приложение» (cards keyboard, URL with a signed launch token; with the cards off — the button under the morning message and /app, initData) or the home-screen icon (initData) → `GET /api/bootstrap` → tiles and keypad → `POST /api/expenses` → `domain.entry_row` → `messages.save_rows` (the same summary and buttons as a chat entry; `app:<entry_id>` in `seen_inputs`) Home-screen shortcuts always go through the Main Mini App (`t.me/<bot>?startapp=home` → `offerHomeScreen`), never through the 30-day launch link.

**Mini App dashboard**: `GET /api/dashboard` → `AnalyticsService.dashboard()` → `build_dashboard` (the same `day_budget` as `/today`, `plan_lines` by group, `read_daily` — the month tab's daily table, `find_series` with this month's state, `spent_groups` and `untracked` — this month's fact rows, `categories` — them joined with the tab's plan by group and subcategory, `card_balances` — the fact balance block from the same read of `fact`, no extra request) → `api.dashboard_out`

**Mini App forecast**: `GET /api/forecast` → `AnalyticsService.forecast(today, card currencies)` → `month_forecast` (`fact` with `keep_unknown`, `read_plan(...).articles`, `read_forecast(...).items`, last month's remainders `carried_items(...)` — for matching only, no events; per currency `currency_forecast`: money now, plan per day by the morning reconciliation, pace of 14 days without list payments, events by the reconciliation now, the days by fact up to today — evening, change, spending and income of the day, totals and by article; total at today's rates) → `api.forecast_out` (icons of the articles from `bot_data["icons"]`)

**Mini App speed**: `GET /api/dashboard|forecast|plan` take sheet copies up to `SCREEN_COPY_AGE` (120 s) old — `read_month_sheets` reads the sheets in parallel threads; `?fresh=1` reads the table anew; answers right after a write are read anew too. Any write through gspread drops every copy (`GoogleSheetsService._written`), a read that overlapped a write is not kept

**Calendar records**: the record window → `POST /api/calendar` (idempotent by `entry_id`), `PUT|DELETE /api/calendar/{id}` → `check_record` → `GoogleSheetsService.add_calendar_row|update_calendar_row|delete_calendar_row` (one write at a time — `CALENDAR_LOCK` in `bot_data`; row A:J RAW, found by ID in column A) → a fresh `forecast_out` and a note to the other user. Reading: `get_calendar()` (a copy for 60 s, reset by the bot's writes; a read during which the bot wrote is not kept) → `read_calendar` → `AnalyticsService._records()` → `month_forecast(..., records)` and `day_budget(..., records)`: `calendar_state` closes records by fact in the morning and now, `_book` reserves them in their articles, the open part goes out on its day (one-offs in both lines, repeats by plan only); `day_budget` uses them for «Сумы»: the pace (payments that closed one-offs are not ordinary spending) and the unpaid one-offs in sums as outflows on their day (`calendar_state` with the same 14-day closing window as «Календарь»); today's spending covered by repeats stays in the day's spending

**Piggy banks**: `GET /api/savings` → `AnalyticsService.savings(today, …)` → `read_book` + fact + the month tab → `savings_screen` (the money day is the last income day of the tab's list; the first plan month is the current one) → `api.savings_screen_out`. Writes (`/api/piggy…`, idempotent by `entry_id`, one at a time — `PIGGY_LOCK`) go through `GoogleSheetsService.add_piggy|update_piggy|add_piggy_moves` (RAW; formulas I and K are written USER_ENTERED), `mark_dirty` and a note to the other user; the answer is a fresh `savings_screen_out`. `sync_piggy` (the receipts job) closes windows and keeps J/L up to date. «Отложить за месяц» (`Screen.goals`, `goal_of`) feeds `/api/savings` `goal`, `PlanOut.goal_hint`/`piggy_hints` and Q:R of the goal row of a tab whose dates the bot sets (`_ensure_dates(..., goal_cells(...))` — one request with M1:M2; `goal_cells` counts the cushion's half by the day budget of the current tab — the new tab when it is this month's, otherwise the tab of today; no tab — no cushion). Bot buttons `piggy:all:<ГГГГ-ММ>` and `piggy:no:<ID>:<fingerprint>` recount from the table under `PIGGY_LOCK`; the articles of open piggies live in `bot_data["piggy_articles"]` for 10 minutes so an ordinary spending reads nothing more

**AI Parsing**: text/photo/document → `GeminiService.parse_transaction()` → each item validated by `domain.ParsedTransaction` → `domain.build_row` (source by card digits, currency conversion with rates from `system!H2:I10`, category/subcategory must exist in `system`, otherwise "🚧 РАЗНОЕ / неучтенка") → one `append_transactions` call → one summary message

**Statement import**: PDF → `statements.pdf_text` → `is_kapitalbank_statement` → `parse_statement` (card by `******NNNN`, period, generation date) → basket in `user_data`, preview job after 4 s → `plan_import` (skip operations booked up to `system!G`, pair own transfers, categories from `MerchantBook` over fact, new shops → `GeminiService.categorize_merchants`, `reconcile`: exact duplicates skipped, `≈` rows corrected, `ВРЕМЕННАЯ` rows removed) → buttons `import:ok|no:<version>` → `apply_plan` (amount fixes → row deletions → append → marks)

**Plan**: `/plan` → `AnalyticsService.plan_report()` → `plan_lines` (month tab columns H/I in UZS, sign flipped, currencies joined) → `format_plan_report`; the Sunday job prepends the week's totals (`weekly_digest`)

**Rich reports**: `morning_brief`/`morning_message`, `plan_report` and `weekly_digest` return `app.rich.Report(text, html)` — `text` from the plain formatters (`format_day_budget`, `morning_lines`, `format_plan_report`), `html` from the block builders next to them (`rich_day_budget`, `rich_morning_lines`, `rich_plan_report`, helpers in `app/rich.py`). `handlers.analytics.send_report` sends `sendRichMessage` through `bot.do_api_request` (PTB 22.8 has no method for it) with `skip_entity_detection`; on `BadRequest`/`EndPointNotFound` it falls back rich+effect → rich → text+effect → text, network errors propagate (no duplicates). `RICH_MESSAGES=off` — plain text only. `send_report(..., reply_markup=, send=)` returns `Sent(kind, messages)` (track every message), `send=` is how the plain text goes (chat.send_message, reply_text, keyword bot.send_message) so old tests keep reading it; `edit_report(..., edit=)` edits the same way (editMessageText with rich_message → text); `app.rich.note_report` turns «head: details» notes into a heading and a paragraph. Replies after a record: `messages.record_blocks` + `write_feedback().blocks`; tests run with plain text by default (`tests/conftest.py`), rich tests use the `rich` fixture

**Daily limit**: morning job / `/today` → `AnalyticsService.morning_brief()` → `day_budget` (fact frame + month tab: `read_forecast` reads the yellow list `O:S` after «Поступления…» up to «Дата», goal row «Отложить за месяц») → `format_day_budget`. The tab cell P5 «Лимит на сегодня» uses the same formula — change both together. Q10 «🧊 Заморожено» (O10 its label, P10 = Q10 × rate) is the frozen money by the same rule as the morning snapshot (fact through yesterday, rows by «Пришло к утру», the horizon by them) — change it together with `day_budget`; the bot does not write it, a one-off script does (the formula's text is kept in the stage plan `docs/superpowers/plans/2026-10-31-frozen-and-cushion.md`). The morning, `/today` and «Сводка» read the piggy sheets (`AnalyticsService(catalog=lambda: bot_catalog(bot_data))`; without a catalog — never) for «не трогаем: подушка …, копилки …»; a failure — a warning in the log and the frozen money without the breakdown. On the salary day (`DayBudget.tomorrow`) `/today`, the reply after a record (`today_line`) and «Сводка» (`DashboardOut.tomorrow`, `salary_came`) show the limit from tomorrow; the morning message and P5 stay the morning snapshot

**Analytics**: `/analytics` → `AnalyticsService.generate_3day_report()` → text summary + pie/bar charts as PNG

## Google Sheets Structure

- **`fact` sheet**: Date (DD.MM.YYYY), Category, Subcategory, Amount, Balance (formula), Comment, Currency, Source.
  - Amount sign: expenses and income (`💰 ДОХОДЫ`) positive; incoming money that is not income (transfer to the card, refund) negative.
  - Balance formula: `google_sheets.BALANCE_FORMULA` uses `INDEX(...;ROW())`, no row numbers.
  - Balance block `I2:Q…`: I = source name, J = balance by the sheet, N = bank balance («Проверка», from SMS or a screenshot), O = N − J, P = −O, Q = «Сверено» (when N was written). The bot finds the row by source name.
- **`system` sheet**: Column A: Categories, Column B: Subcategories, Column F: Sources (last 3 chars = currency code), G: statements loaded up to this day (written by the bot), H:I currency rates to UZS, J: emoji packs (`/icons`), Q:U: one row per chat — ID, groups «да»/«нет», cards «да»/«нет», «уборка с №», «не удалять №» (written by the bot as text).
- **`Календарь` sheet** (created by the bot with the first record): A ID (6 chars `a–z0–9`, a letter first), B date (`ДД.ММ.ГГГГ` text — the day of a one-off or the first day of a repeat), C repeat (`нет`, `каждый день`, `по дням недели`, `раз в месяц`), D days (`пн ср`), E group, F subcategory, G currency, H sum (> 0), I name, J who. Written RAW; can be edited by hand — the bot re-reads it at least once a minute (Mini App screens — within two minutes, at once on «Обновить» or a new launch), broken rows (and a row repeating an ID above it, e.g. a copied row) are skipped with a warning in the log.
- **`Копилки` sheet** (created with the first piggy, 19 columns; an older 14- or 17-column sheet is widened before the first write of O, Q1 or S1): A ID, B name, C target (empty — no target), D currency, E deadline (`ДД.ММ.ГГГГ`, empty — none), F repeat (`один раз`, `каждый год`), G group, H subcategory, I «Отложено» (formula: the journal's plus rows of the ID), J «Потрачено» (bot), K «Осталось» (formula I − J), L «В этом месяце» (bot), M who, N `закрыта`, O «Порядок» (importance number from 1; empty — after the numbered, by deadline). P1 «Откладываем с поступлений, %» and Q1 — the percent of every income (0–100; empty — `SAVE_SHARE`, not a number — `SAVE_SHARE` and «не читается»). R1 «Месяц жизни: обязательные группы» and S1 — the mandatory groups of the month of life separated by «, » (written by the «Месяц жизни» window; empty — `LIFE_GROUPS`, a group missing from the catalog is skipped). Rows are found by ID; broken rows are shown as «Проверьте строку N»
- **`Журнал копилок` sheet**: A date, B ID, C name, D sum (plus — saved, minus — taken), E currency, F what (`отложил`, `потратил`, `потрачено в окне`, `списал остаток`, `закрыта`, `исправление`, `не из копилки`), G who, H key (a fact row for «не из копилки», `окно:ДД.ММ.ГГГГ` for a closed window)
- **Monthly sheets** ("Oct 26", `domain.month_title`): plan vs fact by Subcategory + Currency (`SUMIFS` on `fact` between the dates in `M1:M2`), used by `/advice`. Created by `GoogleSheetsService.ensure_month_tab` (duplicate of the latest month, new `M1:M2`, past months hidden).

## Environment Variables

Required:
- `TELEGRAM_TOKEN`, `SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_EMAIL` + `GOOGLE_PRIVATE_KEY` (or `GOOGLE_APPLICATION_CREDENTIALS_PATH`)
- `ALLOWED_USER_IDS` — Telegram user ids allowed to use the bot (falls back to `ANALYTICS_CHAT_ID`; empty = nobody); the first id is the owner (`config.OWNER_ID`): morning message, Sunday digest, month tab, Premium for custom icons

Optional:
- `GEMINI_API_KEY` - Enables AI features; `GEMINI_MODEL` (default `gemini-flash-latest`), `GEMINI_FALLBACK_MODELS` (default `gemini-flash-lite-latest`, tried on 503/429)
- `ANALYTICS_CHAT_ID`, `ANALYTICS_TIME`, `ANALYTICS_TIMEZONE` - daily report (JobQueue); the time zone also defines "today"
- `IGNORED_CARDS` - last 4 digits of cards the screenshot check skips (e.g. a child's card)
- `RENT_CASH_USD` - the rent paid in USD cash: a withdrawal of exactly this sum at the bank (statement or SMS) goes to «квартплата» with no check mark; empty — every USD cash withdrawal is marked. The owner's sum lives only in Render, never in code, tests or docs (public repo); the start log says only whether it is set
- `MORNING_TIME` - morning message with the daily limit to the owner, default `08:00`, `off` disables it; `FROZEN_CURRENCY` (default `USD`, `off` — none) - cards in this currency are savings; `CUSHION_MONTHS` (default `6`) - emergency fund target in months of life (the average spending of the mandatory groups); steps 1 and 3 months come before it; `SAVE_SHARE` (default `0.10`, `0` — off) - share of an income the write summary suggests setting aside
- `WEEKLY_DIGEST_TIME` - Sunday digest to the owner (`ANALYTICS_CHAT_ID` or the first allowed id), default `20:00`, `off` disables it
- `RICH_MESSAGES` - `off` sends the morning message, `/today`, the weekly digest and `/plan` as plain text instead of Telegram rich messages; default `on`
- `WEBHOOK_URL` (or Render's `RENDER_EXTERNAL_URL`) — web service mode; `WEBHOOK_SECRET` (default: derived from the bot token), `PORT`, `LOCAL_RUN=True` (poll locally), `FORCE_POLLING=True` (poll even while a webhook is set — takes the bot over from the web service)

## Code Patterns

- Dependencies injected via `context.bot_data` (gs_service, ai_service, analytics_service, categories, subcategories, sources)
- User state stored in `context.user_data` (source, category, subcategory; `last_write` for /undo and /fix, `fix_state` — the /fix dialog, `seen_inputs` — fingerprints of SMS/files already written)
- The Google Sheets client is synchronous: call it with `asyncio.to_thread` from handlers; Gemini uses the async client (`client.aio`)
- Writes that are not idempotent (row deletion) are not retried: a lost response would make the retry delete other rows
- Gemini quota: a free key gives ~20 generate requests a day per model. Never generate on start-up (self-check uses `models.get`); 429 pauses the model for RetryInfo.retryDelay (`GeminiService._paused`) instead of retrying
- Business rules live in `app/domain.py` as pure functions — test them table-driven
- PTB 20+ `run_daily(days=...)`: 0 = Sunday … 6 = Saturday
- Never show raw exceptions in the chat: use `errors.user_message(e)`; log with `logger.exception`
- httpx logger stays at WARNING: at INFO it logs Telegram URLs with the bot token
