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
├── statements.py        # Kapitalbank PDF statements: parsing, own-transfer pairs, categories, reconcile with fact
├── web/
│   ├── server.py        # Starlette: POST /telegram (webhook → PTB update_queue), GET /health, security headers, PTB lifespan
│   ├── auth.py          # Mini App auth: initData (HMAC with the bot token), signed launch links for the keyboard button, webhook secret
│   ├── api.py           # /api/bootstrap, /api/expenses (entry_row → save_rows, idempotent by entry_id), /api/expenses/undo, /api/dashboard
│   └── schemas.py       # Pydantic models of the API
├── handlers/
│   ├── common.py        # /start, keyboard helpers, message tracking
│   ├── admin.py         # /reboot (reload categories from sheets)
│   ├── messages.py      # Text/photo/document handling, manual entry, AI parsing flow
│   ├── statement_import.py # Statement preview (JobQueue debounce), confirm buttons, apply_plan
│   ├── transactions.py  # Callback query handler for inline buttons
│   ├── undo.py          # /undo: delete the last write after checking the rows are unchanged
│   ├── fix.py           # /fix: new group/subgroup for a row of the last write (B:D, checked like /undo)
│   ├── icons.py         # /icons: custom emoji message → whole pack (get_custom_emoji_stickers/get_sticker_set) → system!J; load_custom_icons on start
│   ├── live.py          # Reactions on the user's SMS (👀/👍/🤔) and «Думаю…» via send_message_draft (fallback 🔍)
│   ├── last_write.py    # Buttons under a write summary: last:fix (start_fix), last:undo → confirm → undo_last
│   ├── month.py         # Daily job: month plan tab exists (copy of the last one), owner notified
│   ├── balances.py      # Bank main-screen screenshots: «Проверка» + «Сверено», «Выровнять» buttons
│   └── analytics.py     # /advice, /analytics, daily report
├── services/
│   ├── google_sheets.py # GoogleSheetsService — synchronous gspread wrapper
│   ├── ai_service.py    # GeminiService (google-genai, JSON schema output, merchant hints from fact)
│   ├── analytics_service.py # AnalyticsService - reports with matplotlib charts
│   ├── recurring.py     # Monthly payments from fact: recurring_key, amount clusters, day ±4, /subs text and morning lines
│   ├── dashboard.py     # Mini App «Сводка»: build_dashboard — DayBudget, plan_lines by group, read_daily (O:R under «Дата»), subscription states
│   └── day_budget.py    # Daily limit from the month tab forecast block, frozen money, morning text
└── utils/
    └── keyboards.py     # Keyboards: icons from system!D (`with_icon`), prompts naming card/choice (inline buttons are as wide as the message), `action(..., style)` coloured buttons
webapp/                  # Mini App: React + TS + Tailwind (Vite), served at /app/; src/entry — entry screen, src/api.ts — API client
tests/                   # pytest, fakes for Sheets/Gemini/Telegram; no network
```

## Key Data Flows

**Manual Entry**: /start → source → category → subcategory → "amount comment" (`+` prefix = incoming money) → `domain.manual_row` → `fact`

**Mini App entry**: «📱 Приложение» (cards keyboard, URL with a signed launch token) or the home-screen icon (initData) → `GET /api/bootstrap` → tiles and keypad → `POST /api/expenses` → `domain.entry_row` → `messages.save_rows` (the same summary and buttons as a chat entry; `app:<entry_id>` in `seen_inputs`)

**Mini App dashboard**: `GET /api/dashboard` → `AnalyticsService.dashboard()` → `build_dashboard` (the same `day_budget` as `/today`, `plan_lines` by group, `read_daily` — the month tab's daily table, `find_series` with this month's state) → `api.dashboard_out`

**AI Parsing**: text/photo/document → `GeminiService.parse_transaction()` → each item validated by `domain.ParsedTransaction` → `domain.build_row` (source by card digits, currency conversion with rates from `system!H2:I10`, category/subcategory must exist in `system`, otherwise "🚧 РАЗНОЕ / неучтенка") → one `append_transactions` call → one summary message

**Statement import**: PDF → `statements.pdf_text` → `is_kapitalbank_statement` → `parse_statement` (card by `******NNNN`, period, generation date) → basket in `user_data`, preview job after 4 s → `plan_import` (skip operations booked up to `system!G`, pair own transfers, categories from `MerchantBook` over fact, new shops → `GeminiService.categorize_merchants`, `reconcile`: exact duplicates skipped, `≈` rows corrected, `ВРЕМЕННАЯ` rows removed) → buttons `import:ok|no:<version>` → `apply_plan` (amount fixes → row deletions → append → marks)

**Plan**: `/plan` → `AnalyticsService.plan_report()` → `plan_lines` (month tab columns H/I in UZS, sign flipped, currencies joined) → `format_plan_report`; the Sunday job prepends the week's totals (`weekly_digest`)

**Daily limit**: morning job / `/today` → `AnalyticsService.morning_brief()` → `day_budget` (fact frame + month tab: `read_forecast` reads the yellow list `O:S` after «Поступления…» up to «Дата», goal row «Отложить за месяц») → `format_day_budget`. The tab cell P5 «Лимит на сегодня» uses the same formula — change both together

**Analytics**: `/analytics` → `AnalyticsService.generate_3day_report()` → text summary + pie/bar charts as PNG

## Google Sheets Structure

- **`fact` sheet**: Date (DD.MM.YYYY), Category, Subcategory, Amount, Balance (formula), Comment, Currency, Source.
  - Amount sign: expenses and income (`💰 ДОХОДЫ`) positive; incoming money that is not income (transfer to the card, refund) negative.
  - Balance formula: `google_sheets.BALANCE_FORMULA` uses `INDEX(...;ROW())`, no row numbers.
  - Balance block `I2:Q…`: I = source name, J = balance by the sheet, N = bank balance («Проверка», from SMS or a screenshot), O = N − J, P = −O, Q = «Сверено» (when N was written). The bot finds the row by source name.
- **`system` sheet**: Column A: Categories, Column B: Subcategories, Column F: Sources (last 3 chars = currency code), G: statements loaded up to this day (written by the bot), H:I currency rates to UZS.
- **Monthly sheets** ("Oct 26", `domain.month_title`): plan vs fact by Subcategory + Currency (`SUMIFS` on `fact` between the dates in `M1:M2`), used by `/advice`. Created by `GoogleSheetsService.ensure_month_tab` (duplicate of the latest month, new `M1:M2`, past months hidden).

## Environment Variables

Required:
- `TELEGRAM_TOKEN`, `SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_EMAIL` + `GOOGLE_PRIVATE_KEY` (or `GOOGLE_APPLICATION_CREDENTIALS_PATH`)
- `ALLOWED_USER_IDS` — Telegram user ids allowed to use the bot (falls back to `ANALYTICS_CHAT_ID`; empty = nobody)

Optional:
- `GEMINI_API_KEY` - Enables AI features; `GEMINI_MODEL` (default `gemini-flash-latest`), `GEMINI_FALLBACK_MODELS` (default `gemini-flash-lite-latest`, tried on 503/429)
- `ANALYTICS_CHAT_ID`, `ANALYTICS_TIME`, `ANALYTICS_TIMEZONE` - daily report (JobQueue); the time zone also defines "today"
- `IGNORED_CARDS` - last 4 digits of cards the screenshot check skips (e.g. a child's card)
- `MORNING_TIME` - morning message with the daily limit to the owner, default `08:00`, `off` disables it; `FROZEN_CURRENCY` (default `USD`, `off` — none) - cards in this currency are savings
- `WEEKLY_DIGEST_TIME` - Sunday digest to the owner (`ANALYTICS_CHAT_ID` or the first allowed id), default `20:00`, `off` disables it
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
