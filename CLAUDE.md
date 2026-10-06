# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Telegram Finance Bot for personal finance tracking. Records income/expenses to Google Sheets with AI-powered transaction parsing via Google Gemini. Supports manual entry, SMS parsing, image/receipt recognition, and scheduled analytics. Deployed on Render as a background worker (polling), Python 3.12.

## Commands

```bash
# Setup
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
pip install -r requirements-dev.txt

# Checks (the same run in CI)
ruff check . && black --check . && pytest -q

# Run (polls Telegram unless WEBHOOK_URL is set)
python -m app.main
```

## Architecture

```
app/
├── main.py              # build_application(): handlers, access gate, error handler, daily job; main() runs it
├── config.py            # Environment variables and constants
├── auth.py              # Allowlist gate (TypeHandler in group -1)
├── domain.py            # Sheet rules: sign of the amount, currency conversion, category/source/date validation
├── errors.py            # Safe one-line error texts for the chat, application error handler
├── handlers/
│   ├── common.py        # /start, keyboard helpers, message tracking
│   ├── admin.py         # /reboot (reload categories from sheets)
│   ├── messages.py      # Text/photo/document handling, manual entry, AI parsing flow
│   ├── transactions.py  # Callback query handler for inline buttons
│   └── analytics.py     # /advice, /analytics, daily report
├── services/
│   ├── google_sheets.py # GoogleSheetsService — synchronous gspread wrapper
│   ├── ai_service.py    # GeminiService (google-genai, JSON schema output, merchant hints from fact)
│   └── analytics_service.py # AnalyticsService - reports with matplotlib charts
└── utils/
    └── keyboards.py     # Telegram keyboard generators
tests/                   # pytest, fakes for Sheets/Gemini/Telegram; no network
```

## Key Data Flows

**Manual Entry**: /start → source → category → subcategory → "amount comment" (`+` prefix = incoming money) → `domain.manual_row` → `fact`

**AI Parsing**: text/photo/document → `GeminiService.parse_transaction()` → each item validated by `domain.ParsedTransaction` → `domain.build_row` (source by card digits, currency conversion with rates from `system!H2:I10`, category/subcategory must exist in `system`, otherwise "🚧 РАЗНОЕ / неучтенка") → one `append_transactions` call → one summary message

**Analytics**: `/analytics` → `AnalyticsService.generate_3day_report()` → text summary + pie/bar charts as PNG

## Google Sheets Structure

- **`fact` sheet**: Date (DD.MM.YYYY), Category, Subcategory, Amount, Balance (formula), Comment, Currency, Source.
  - Amount sign: expenses and income (`💰 ДОХОДЫ`) positive; incoming money that is not income (transfer to the card, refund) negative.
  - Balance formula: `google_sheets.BALANCE_FORMULA` uses `INDEX(...;ROW())`, no row numbers.
  - Balance block `I2:N…`: column I = source name, column N = bank balance written from SMS ("Остаток", "Dostupno"). The bot finds the cell by source name.
- **`system` sheet**: Column A: Categories, Column B: Subcategories, Column F: Sources (last 3 chars = currency code), H:I currency rates to UZS.
- **Monthly sheets** ("Oct 26"): plan vs fact by Subcategory + Currency (`SUMIFS` on `fact`), used by `/advice`.

## Environment Variables

Required:
- `TELEGRAM_TOKEN`, `SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_EMAIL` + `GOOGLE_PRIVATE_KEY` (or `GOOGLE_APPLICATION_CREDENTIALS_PATH`)
- `ALLOWED_USER_IDS` — Telegram user ids allowed to use the bot (falls back to `ANALYTICS_CHAT_ID`; empty = nobody)

Optional:
- `GEMINI_API_KEY` - Enables AI features; `GEMINI_MODEL` (default `gemini-flash-latest`)
- `ANALYTICS_CHAT_ID`, `ANALYTICS_TIME`, `ANALYTICS_TIMEZONE` - daily report (JobQueue); the time zone also defines "today"
- `WEBHOOK_URL` (or Render's `RENDER_EXTERNAL_URL`), `WEBHOOK_SECRET`, `PORT`, `LOCAL_RUN=True`

## Code Patterns

- Dependencies injected via `context.bot_data` (gs_service, ai_service, analytics_service, categories, subcategories, sources)
- User state stored in `context.user_data` (source, category, subcategory)
- Google Sheets and Gemini clients are synchronous: call them with `asyncio.to_thread` from handlers
- Business rules live in `app/domain.py` as pure functions — test them table-driven
- Never show raw exceptions in the chat: use `errors.user_message(e)`; log with `logger.exception`
- httpx logger stays at WARNING: at INFO it logs Telegram URLs with the bot token
