# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Telegram Finance Bot for personal finance tracking. Records income/expenses to Google Sheets with AI-powered transaction parsing via Google Gemini. Supports manual entry, SMS parsing, image/receipt recognition, and scheduled analytics.

## Commands

```bash
# Setup
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt

# Run locally (polling mode)
LOCAL_RUN=True python -m app.main

# Run production (webhook mode)
python -m app.main
```

## Architecture

```
app/
├── main.py              # Entry point, bot setup, handler registration, scheduler
├── config.py            # Environment variables and constants
├── handlers/
│   ├── common.py        # /start, keyboard helpers, message tracking
│   ├── admin.py         # /reboot (reload categories from sheets)
│   ├── messages.py      # Text/photo/document handling, AI parsing flow
│   ├── transactions.py  # Callback query handler for inline buttons
│   └── analytics.py     # /advice, /analytics commands
├── services/
│   ├── google_sheets.py # GoogleSheetsService - CRUD for transactions
│   ├── ai_service.py    # GeminiService - transaction parsing, financial analysis
│   └── analytics_service.py # AnalyticsService - reports with matplotlib charts
└── utils/
    └── keyboards.py     # Telegram keyboard generators
```

## Key Data Flows

**Manual Entry**: /start → select source → select category → select subcategory → enter "amount comment" → saved to `fact` sheet

**AI Parsing**: User sends text/photo/document → `text_handler`/`document_handler` → `GeminiService.parse_transaction()` → extracts amount, category, date from content → saved to `fact` sheet

**Analytics**: `/analytics` → `AnalyticsService.generate_3day_report()` → text summary + pie/bar charts as PNG

## Google Sheets Structure

- **`fact` sheet**: Transaction log. Columns: Date (DD.MM.YYYY), Category, Subcategory, Amount, Balance (formula), Comment, Currency, Source. Column N contains actual card balances for reconciliation (N2-N6 mapped to specific cards)
- **`system` sheet**: Configuration. Column A: Categories, Column B: Subcategories, Column F: Sources (last 3 chars = currency code like UZS, USD)
- **Monthly budget sheets** (e.g., "Jan 25"): Used by `/advice` for plan vs. fact comparison

## Card Balance Tracking

When AI parses a transaction containing card balance (e.g., "Остаток: 1,234,567"), it updates the corresponding cell in column N. Mapping defined in `config.py`:
- N2: VISA *9120
- N3: UZCARD *5837
- N4: МИР *9959
- N5: VISA *4058
- N6: HUMO *6845

## Environment Variables

Required:
- `TELEGRAM_TOKEN` - Bot token from @BotFather
- `SPREADSHEET_ID` - Google Sheet ID
- `GOOGLE_SERVICE_ACCOUNT_EMAIL` + `GOOGLE_PRIVATE_KEY` (or `GOOGLE_APPLICATION_CREDENTIALS_PATH`)

Optional:
- `GEMINI_API_KEY` - Enables AI features
- `WEBHOOK_URL`, `PORT` - For production webhook mode
- `LOCAL_RUN=True` - Forces polling mode
- `ANALYTICS_CHAT_ID`, `ANALYTICS_TIME`, `ANALYTICS_TIMEZONE` - Scheduled daily reports

## Code Patterns

- Dependencies injected via `context.bot_data` (gs_service, ai_service, analytics_service, categories, sources)
- User state stored in `context.user_data` (source, category, subcategory)
- Message tracking for cleanup: `track_message()`, `clear_tracked_messages()`
- All handlers use async/await with python-telegram-bot v20+
- AI service loads last 5000 transactions as context for category prediction
- Amounts always stored as positive floats; income vs expense determined by category
