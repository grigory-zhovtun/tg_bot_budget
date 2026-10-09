# Telegram Finance Bot

## Description

This Telegram bot is designed for convenient personal finance tracking. It allows users to record their income and expenses, categorize them, and analyze data using Google Sheets. The bot supports manual data entry as well as parsing SMS messages from banks.

## Technologies

*   Python 3
*   [python-telegram-bot](https://python-telegram-bot.org/) - for interacting with the Telegram API.
*   [gspread](https://gspread.readthedocs.io/) - for working with the Google Sheets API.
*   [google-auth](https://google-auth.readthedocs.io/) - for authenticating with Google APIs.
*   [python-dotenv](https://github.com/theskumar/python-dotenv) - for managing environment variables.

## Features

*   **Transaction Logging:** Add income and expense records.
*   **Categorization:** Assign categories and subcategories to each transaction.
*   **Source Management:** Select the source of funds (e.g., card, cash) with automatic currency detection.
*   **SMS Parsing:** Automatically recognize and add transactions from bank SMS messages.
*   **Bank Statements:** Kapitalbank PDF statements («История операций») are parsed without AI: transfers between own cards are paired, categories come from how the same shop was categorized before (Gemini suggests one for new shops), rows already in the sheet are skipped, and nothing is written before you confirm the preview.
*   **Google Sheets Integration:** All data is saved and updated in real-time in the specified Google Sheet.
*   **Dynamic Keyboards:** User-friendly interface with buttons for selecting categories, sources, and other actions.
*   **On-the-fly Data Updates:** The `/reboot` command reloads categories, subcategories, and sources from the Google Sheet without restarting the bot.

## Installation and Setup

### 1. Clone the repository:

```bash
git clone <repository_URL>
cd <repository_folder_name>
```

### 2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # for Linux/macOS
# or
venv\Scripts\activate  # for Windows
```

### 3. Install dependencies:

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables:

Create a `.env` file in the root directory of the project and add the following variables:

```env
TELEGRAM_TOKEN="YOUR_TELEGRAM_TOKEN"
SPREADSHEET_ID="YOUR_GOOGLE_SHEET_ID"
GOOGLE_SERVICE_ACCOUNT_EMAIL="YOUR_GOOGLE_SERVICE_ACCOUNT_EMAIL"
GOOGLE_PRIVATE_KEY="YOUR_GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY" # use \n for line breaks

# Who may use the bot: comma-separated Telegram user ids.
# Falls back to ANALYTICS_CHAT_ID; if both are empty the bot answers nobody.
ALLOWED_USER_IDS="123456789"

# AI parsing of SMS, screenshots and documents (Google Gemini)
GEMINI_API_KEY="YOUR_GEMINI_API_KEY"
# GEMINI_MODEL="gemini-flash-latest"   # any Gemini model id; the resolved version is logged
# GEMINI_FALLBACK_MODELS="gemini-flash-lite-latest"  # used when the main model is overloaded (503/429)
# A free Gemini key allows ~20 requests a day per model: after a 429 the bot skips that
# model until its quota returns (RetryInfo) and uses the next one; the start-up check reads
# model metadata only and spends no quota.

# Optional daily report
# ANALYTICS_CHAT_ID="123456789"
# ANALYTICS_TIME="07:00"
# ANALYTICS_TIMEZONE="Asia/Tashkent"   # also used for "today" in new rows
# WEEKLY_DIGEST_TIME="20:00"           # Sunday digest to the owner; "off" disables it
# IGNORED_CARDS="1234"                 # cards (last 4 digits) never matched from screenshots

# Optional webhook mode (otherwise the bot polls Telegram)
# WEBHOOK_URL="https://your-domain.com"   # RENDER_EXTERNAL_URL is used if empty
# WEBHOOK_SECRET="random-string"           # Telegram signs webhook requests with it
# PORT="8443"
# LOCAL_RUN="True"                         # force polling
```

**Important note on `GOOGLE_PRIVATE_KEY`:**
*   The private key from the Google service account JSON file must be pasted as a string.
*   If you copy it directly, it will contain newline characters (`\n`). In the `.env` file, these characters should either be escaped (`\\n`) or the entire string should be enclosed in double quotes if your system supports it. `app/config.py` converts `\n` back to line breaks.

### 5. Set up Google Sheets API:

1.  **Create a project in Google Cloud Console** if you don't have one already.
2.  **Enable the Google Drive API and Google Sheets API** for your project.
3.  **Create a service account:**
    *   Navigate to "IAM & Admin" -> "Service Accounts".
    *   Click "Create Service Account".
    *   Give it a name, ID, and description.
    *   Grant it the "Editor" role or more granular permissions if necessary for security.
    *   Click "Done".
    *   After creating the service account, find it in the list, click the three dots (actions), and select "Manage keys".
    *   Click "Add Key" -> "Create new key".
    *   Choose "JSON" as the key type and click "Create". A JSON file with credentials will be downloaded to your computer.
4.  **Copy the values for `.env`:**
    *   `GOOGLE_SERVICE_ACCOUNT_EMAIL`: this is the `client_email` field from the downloaded JSON file.
    *   `GOOGLE_PRIVATE_KEY`: this is the `private_key` field from the JSON file.
5.  **Share your Google Sheet with the service account:**
    *   Open your Google Sheet.
    *   Click "Share".
    *   In the "Add people and groups" field, paste the `GOOGLE_SERVICE_ACCOUNT_EMAIL` (your service account's email).
    *   Ensure it has "Editor" permissions.
    *   Click "Send" (or "Done").

### 6. Google Sheet Structure:

Ensure your Google Sheet contains two sheets:

*   **`fact`**: All transactions will be recorded here.
    *   **Columns:** `Date`, `Category`, `Subcategory`, `Amount`, `Balance` (formula), `Comment`, `Currency`, `Source`.
    *   **Sign of `Amount`:** expenses and income (category `💰 ДОХОДЫ`) are positive; money that comes in but is not income (transfer to the card, refund) is negative.
    *   **Balance formula** written by the bot. It has no row numbers, so it stays correct after sorting or deleting rows:
        ```excel
        =SUMIFS($D$2:INDEX($D:$D;ROW()); $H$2:INDEX($H:$H;ROW()); INDEX($H:$H;ROW()); $G$2:INDEX($G:$G;ROW()); INDEX($G:$G;ROW()); $B$2:INDEX($B:$B;ROW()); "💰 ДОХОДЫ") - SUMIFS(...; "<>💰 ДОХОДЫ")
        ```
    *   **Balance block `I:N`** (rows 2+): column I lists the sources, column N ("check") receives the bank balance from SMS ("Остаток", "Dostupno").
*   **`system`**: This sheet is used for bot configuration (categories, subcategories, sources, currency rates).
    *   **Column A:** Categories (e.g., "Groceries", "Transport").
    *   **Column B:** Subcategories (e.g., for "Groceries": "Supermarket", "Market"). The corresponding category from Column A must be specified.
    *   **Column F:** Sources (e.g., "Card UZS", "Cash USD"). The last 3 characters of the source name are used to determine the currency (e.g., "UZS", "USD").
    *   **Columns H:I (rows 2–10):** currency code and its rate to UZS (`GOOGLEFINANCE`). Used to convert an SMS amount in another currency into the card currency.
    *   **Column G** (next to the sources): the day up to which bank statements are loaded. The bot fills it after each import.

### 7. Running the bot:

```bash
python -m app.main
```

Without `WEBHOOK_URL` (or with `LOCAL_RUN=True`) the bot polls Telegram — this is how it runs as a Render background worker. With `WEBHOOK_URL` it listens on `PORT` at `/telegram`.

## Usage

1.  **Send `/start`** and pick a source (card) on the keyboard.
2.  **Pick a category and a subcategory** on the inline keyboards.
3.  **Type the amount and an optional comment**: `48000 latte`, `5 000,50 lunch`. A leading `+` records incoming money: `+20000 refund`.
4.  **Or just send an SMS, a screenshot, a PDF/Excel/CSV file** — Gemini extracts the transactions; the bot converts currencies, picks the card by its number, checks categories against `system` and writes all rows in one request. Rows it cannot write are listed in the reply.
    *   The same SMS, screenshot or file sent twice is written once: the bot remembers what it wrote during the last week and answers which rows already hold it.
    *   When an SMS shows the card balance, the reply compares it with the balance in the sheet: «🟰 сходится» or the difference.
6.  **Send screenshots of the banks' main screens** (the card list with balances). The bot reads every card's balance (Gemini decides whether a photo shows balances or transactions — one request), writes it to «Проверка» with the time in «Сверено» (`fact!Q`) and answers card by card: «✅ сходится» or «⚠️ в таблице больше/меньше на …». Transactions visible on such a screen are ignored (they come from SMS and statements). For each mismatch there is a **Выровнять** button: it recomputes the difference at the moment of the tap and adds one row to «🚧 РАЗНОЕ / неучтенка» (undo with `/undo`). Hidden balances, non-card products and cards from `IGNORED_CARDS` are skipped; a card that is not in `system` is reported.
5.  **Send Kapitalbank statements** (PDF «История операций», one or several cards at once). The bot waits a few seconds for all files, then shows one preview: new rows, rows already in the sheet, how each card balance changes and rows worth checking. **✅ Записать** writes everything in one go:
    *   operations booked up to the date in `system!G` («выписка по», next to the card) were loaded before and are skipped; after the import the date moves to the last full day of the statement;
    *   a row the bot once converted at the sheet rate (comment with `≈`) gets the bank amount instead of a second row;
    *   a temporary balancing row (comment contains `ВРЕМЕННАЯ`) is deleted once a statement covers its date;
    *   sending the same statements again changes nothing.

### Weekly digest

On Sundays at `WEEKLY_DIGEST_TIME` (default `20:00`, `off` disables it) the owner gets the week's spending against the previous week, the three biggest subcategories, the `/plan` report and a reminder to send bank screenshots (cards not checked for more than a week are named).

### Monthly plan tabs

Every day at 00:05 (`ANALYTICS_TIMEZONE`) and right after start the bot checks the tab of the current month («Nov 26»). If it is missing, the bot copies the latest month tab (the plan stays the same), sets the month dates in `M1:M2`, hides past months and tells the owner (`ANALYTICS_CHAT_ID`, otherwise the first id from `ALLOWED_USER_IDS`). Fact values in the tab are formulas over `fact` and the dates in `M1:M2`, so nothing else changes.

### Commands

*   **`/analytics`** — report for the last 3 days with charts.
*   **`/plan`** — month plan vs fact without AI: spending pace against the calendar, what is left per day, lines over plan, close to the plan (80%+) and outside the plan. Lines of one subcategory in different currencies are added up in UZS.
*   **`/advice`** — AI analysis of spending vs. the current month plan.
*   **`/undo`** — delete the rows of the last write, if nobody changed them in the sheet since.
*   **`/fix`** — change the group and subgroup of the last write when the AI picked the wrong one (SMS, receipt or screenshot): the bot asks which operation (if there were several), then the group and the subgroup from `system`. The row is changed only if nobody edited it in the sheet; the money keeps its direction (an incoming row moved to or from «💰 ДОХОДЫ» flips the sign in `D`), and the merchant goes to the new category from the next message on.
*   **`/reboot`** — reload categories, subcategories and sources from the `system` sheet.

## Development

```bash
pip install -r requirements-dev.txt
ruff check . && black --check . && pytest -q
```

CI runs the same checks on every push and pull request.

## Contributing

If you'd like to contribute, please fork the repository, make your changes, and submit a Pull Request. We welcome any improvements!

## License

This project is licensed under the MIT License. See the `LICENSE` file for details (you'll need to create this file if you plan to include one).
