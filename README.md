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
*   **SMS Parsing:** Kapitalbank and UZCARD SMS are read by rules, without AI: the card by its digits, the date, the amount and the balance come from the text; the category comes from how the same shop was categorized before, or — for QR payments without a shop name — from the same amount on the same card; the same shop at the same amount on the same card follows the latest choice when its rows disagree (the morning coffee at a café where lunches were «кафе»). Gemini only suggests a category for new shops; any other text goes to Gemini as before.
*   **Bank Statements:** Kapitalbank PDF statements («История операций») are parsed without AI: transfers between own cards are paired, categories come from how the same shop was categorized before (Gemini suggests one for new shops), rows already in the sheet are skipped, and nothing is written before you confirm the preview.
*   **Google Sheets Integration:** All data is saved and updated in real-time in the specified Google Sheet.
*   **Clean Chat:** the buttons of the groups (manual entry) and the keyboard of the cards are off by default and switched on per person in `/keyboards`; the setting lives in the sheet, so clearing the chat history or restarting the bot keeps it. A fully written SMS or screenshot leaves the chat, and every morning the bot deletes yesterday's messages (see «Clean chat» below).
*   **Mini App:** the «📱 Приложение» button opens a Telegram Mini App — in the cards keyboard when it is on, otherwise under the morning message and by `/app`. Text in the app is high-contrast on both Telegram themes (7:1 and more): secondary text is not pale, warnings are orange rather than red, and the money colors are the same everywhere — spending yellow, income mint, planned records cyan, list payments orange, below zero a red cell or chip with white numbers. «Ввод» — big tiles for groups and subcategories (under the name, on two lines, what is left of the plan — «осталось», «⚠️ сверх на» or «вне плана» — and the sum whole) and a keypad with big bold digits (the windows keep the smaller keypad: it is shared with «Календарь»'s record window, and «Календарь» keeps its look); the expense is written exactly like a manual entry (the summary with «Исправить»/«Отменить» comes to the chat, `/undo` and `/fix` work with it). A lost connection does not lose an expense: it is kept on the phone (per person) before the first attempt, the app sends it again with the same id (the server answers a repeat with «уже записано», even when the catalog changed since), and if the connection is still gone it stays under «📤 Не отправлено: N» with the bot's date at the tap and is sent by itself when the screen opens, the phone is back online or the app comes back; a server refusal shows the field's reason with a two-tap «Удалить», an expired login keeps it waiting for the app to be opened again, and «Отменить» under the last expense is dropped once the queue writes (the server undoes the last write). A failed sheet write from the app sends nothing to the chat. «Сводка» has three sections — «Сегодня», «Траты», «Впереди» — and the chosen one is kept on the phone. It is laid out for low vision, like the bot's messages: the main figure is 40 px, every fact is a big line with plain explanation lines under it, statuses are white-on-colour badges with a sign (⚠️ / ✓), lists are bordered tables or two-line items (the name over the day and the sum), numbers are whole («39 000 000», «1 500,25 $», the currency sign after the number, no «млн»), and at 360 px with the normal font size no line wraps. «Сегодня» — what is left for today with the limit a day until the salary, a table of where it comes from (the sums this morning, the advance before the salary, the payments until it and the money for its days) and the plan in one line (as under the spending chart); on the day the main salary comes — «✓ Пришла ЗП …», the limit a day from tomorrow «до зарплаты 30 ноября, N дней» and the same table from the sums now, without «сверх лимита дня»; frozen money, «💳 Сумы сейчас» — whether the sums now last until the salary («✓ до зарплаты 30-го хватит», «останется ≈ …» at the pace of the last 14 days, or when they run out and how many dollars to exchange) — and the balance of every card now (the fact balance block as a table «карта | остаток | сверка», in the card's currency, with the total in sums; ✓ — the table matches the bank's balance from the last SMS or screenshot, with how many days ago it was checked or «давно» when the check has no time — in the text colour, since coloured text on table stripes falls below 7:1; ⚠️ and «… в банке …» under the table — they differ; a currency without a rate is named next to the total). «Траты» — plan vs fact by category (one chart: the month tab's plan and this month's spending from the fact rows from the 1st to today in sums, the biggest spending first, under one total «Потрачено X из Y» — the tab's only «потрачено»; the bar is the spending on one scale for all groups, coloured like the plan status — green, yellow from 80 %, red over it, grey without a plan — the line is the plan, a badge next to the group gives the share of the plan used — green ✓ below 80 %, yellow up to 100 %, red ⚠️ over it — and the line under the bar «spent из plan»; a tap opens the subcategories the same way; articles whose spending the month tab does not count — no row for the subcategory in that currency — are listed in a table «Нет в плане месяца» under the chart), right after it the daily spending bars against each day's limit (red — over it, green — within it, dashed — what can be spent per day until the salary, its sum in the legend under the chart next to the plan, not over the bars; with one line above — «⚠️ Сверх плана на … за N дней» or «✓ Запас … за N дней»: the everyday spending of the finished days, without list payments and today, against the plan for them). «Впереди» — last month's incomes that came short first («ЗП … за октябрь, остаток» on the 1st with the part still missing, see `/today`), then upcoming items of the yellow list and subscriptions as two-line items (a ⏳ before the name — the day passed without a record; a subscription's mark — ✓ charged, ⏳ expected, ⚠️ twice, ❓ missed), or «До конца месяца платежей и подписок нет». Without the forecast block in the month tab «Сегодня» and «Впереди» say so instead of the limit and the list. «План» has two sections, kept on the phone like those of «Сводка»: «План месяца» and «Накопления». «План месяца» — the month's plan from the phone: article sums with hints (the average of the last three full months, the last month and this month so far), the incomes and payments list and the savings goal. It uses the same big-text layout as «Сводка»: «Свободно» as the main figure with the totals in a table, the list as two-line items with what has come in under each, every article's plan in big digits with «ср. …, сен …» under it (an edited plan in the link colour, the old one struck through), the article window's history as a table and quick buttons with their sums («= в среднем 1 800 000»). «Сохранить» writes only the changed cells of the month tab in one request after checking that nobody changed them meanwhile (otherwise the app shows the fresh numbers with the user's edits on top); the other user gets a note in the chat. Unsaved edits stay in the phone until they are saved. The next month can be planned in advance: «Создать план на <месяц>» copies the current month's tab with the next month's dates and keeps it hidden until the 1st; on the 1st the bot shows it and hides the past months. «＋ статья» adds an article the month's tab does not have yet — a subcategory of the catalog or the same subcategory in another currency; on «Сохранить» the row is inserted under its group in columns A–L only, with the formulas of the row above, and the totals row is extended to it. «🧠 Совет по плану» sends the draft with the family's own history to the same Gemini model as /advice (no internet search): the month's totals, every article with its plan, a year of monthly fact, past plans, purchases (how many, the average check, the top places), spending outside the plan, this month's pace and money, rates, and the notes the owner keeps under «🧠 О нас для советника» in the system sheet — no names from the list and no cards. It shows up to five points; a point about an article has «Применить», which only changes the draft. «Накопления» — with its own «Накопления · октябрь» line and «Обновить» — what to set aside this month, the emergency fund, the piggy banks with their plan for a year and the savings rate (see below); its windows use the same big text: «Отложил» shows where the sum goes as a table «копилка | сумма» with a «подушка» row, the percent and order windows give each piggy's forecast on its own lines («будет 12 000 000» / «из 12 000 000 ✓», «⚠️» when it will not make it), field labels are 17 px in the text colour and sums are whole. «Календарь» (it was «Прогноз»: its past days are the fact, the rest the forecast) — how much money there will be on every evening until the month ends, for each currency of the cards and in total (in sums or dollars at the `system` rates): a month grid from the 1st — the days before today by fact (the evening balance, that day's spending and income apart, transfers between own cards being neither), today the evening balance by plan with what was already spent and earned today, after it the evening balance by plan and the planned spending of each day; the numbers are bold and high-contrast (spending, income and records in bright yellow, mint and cyan on the dark theme, dark tones on the light one, no red text — an evening below zero is a red cell with white numbers), the chosen day's card (by plan and by pace, the yellow-list rows and the records of that day; for today and the days before — the spending and income of the day by article, each in its currency), a month chart with two lines — by plan (the rest of the month's articles spread over the days left, list payments and records on their days) and by pace (the average spending of the 14 days before today without transfers, list payments and paid one-off records; one-off records on their days) — and a warning when a line goes below zero. Spending can be planned on days inside an article of the plan: once («the kids' centre on Saturday, 320 000, ДЕТИ / развитие»), every day, on days of the week or once a month. Records live in the «Календарь» sheet (ID, date, repeat, days, group, subcategory, currency, sum, name, who) and can be edited there by hand (a copied row needs a new ID: 6 characters, a letter first). A record is reserved in its article: the plan per day spreads only what is left of the article after its records, and records above the article's rest count in full with the «сверх плана» mark. A spending of the article closes a record — a one-off by one payment within 10 % from three days before its date, today's records by today's spending of the article; a closed record shows what was really spent (that payment, or its share of the article's spending today) with its plan next to it when they differ. The chosen day's card lists them under «Запланировано на день»: a record opens from there, and a new one starts from «＋ Запланировать на …» on today or a later day. Records no day card shows — one-offs after the month, repeats with no days left in it, a currency without a card — are listed under the day card as «Запланировано дальше» (only when there are such) and open from there. The record window suggests a sum from the article's history (the median and the last payment of 90 days, the average per day of 30 days); the other user gets a note in the chat.
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
# The first id is the owner: the morning message, the Sunday digest and month-tab notices go to them.
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
# MORNING_TIME="08:00"                 # morning message with the daily limit; "off" disables it
# FROZEN_CURRENCY="USD"                # cards in this currency are savings ("off" — none)
# CUSHION_MONTHS="6"                    # emergency fund target, months of average spending
# SAVE_SHARE="0.10"                     # share of an income suggested to set aside: 0.1, 10%, off
# CELEBRATE_EFFECT_ID="5046509860389126442"  # 🎉 on the morning message after a day within the limit ("off" — none)
# RICH_MESSAGES="on"                    # morning, weekly, /today and /plan as Telegram rich messages; "off" — plain text for everyone
# IGNORED_CARDS="1234"                 # cards (last 4 digits) never matched from screenshots
# RENT_CASH_USD="380"                  # USD cash withdrawn at the bank of exactly this sum is rent with no ❓ (statements, SMS); empty — every such withdrawal is marked

# Web service mode (Render sets RENDER_EXTERNAL_URL); otherwise the bot polls Telegram
# WEBHOOK_URL="https://your-domain.com"   # RENDER_EXTERNAL_URL is used if empty
# WEBHOOK_SECRET="random-string"           # default: derived from TELEGRAM_TOKEN
# PORT="8443"
# LOCAL_RUN="True"                         # force polling
# FORCE_POLLING="True"                     # poll even while a webhook is set (takes the bot over)
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
    *   **Column D:** an icon (emoji) for the subcategory in the same row — shown on the buttons and in the summaries («☕ кофе»). Subcategory names in the sheets stay as they are; `/reboot` reloads the icons.
    *   **Column F:** Sources (e.g., "Card UZS", "Cash USD"). The last 3 characters of the source name are used to determine the currency (e.g., "UZS", "USD").
    *   **Columns H:I (rows 2–10):** currency code and its rate to UZS (`GOOGLEFINANCE`). Used to convert an SMS amount in another currency into the card currency.
    *   **Column J:** emoji packs for pictures on the buttons («наборы эмодзи», filled by `/icons`).
    *   **Column G** (next to the sources): the day up to which bank statements are loaded. The bot fills it after each import.

### 7. Running the bot:

```bash
python -m app.main
```

On Render the bot runs as a **web service**: Render sets `RENDER_EXTERNAL_URL`, and the bot serves `POST /telegram` (the Telegram webhook, checked with the secret header) and `GET /health` on `PORT`. The Build Command is `pip install -r requirements.txt && bash scripts/build_webapp.sh`, the Start Command `python -m app.main`. Without `WEBHOOK_URL`/`RENDER_EXTERNAL_URL` (or with `LOCAL_RUN=True`) the bot polls Telegram — but only while no webhook is set: polling would delete the production webhook, so a local run with the production token waits until `FORCE_POLLING=True`.

### 8. Mini App (`webapp/`)

React + TypeScript + Tailwind, built by Vite into `webapp/dist` and served by the bot at `/app/`. The page calls `/api/*` with Telegram's `initData` or — when it is opened from the keyboard button, where Telegram passes no `initData` — with the signed launch link the bot puts into the button (30 days, renewed with every cards keyboard).

Screens open at once: the app keeps the answers it got and today's copy of them in the phone, shows them immediately and asks the server in the background («Обновляю…» on the button meanwhile); after «Ввод» it loads «Сводка», then «Календарь» and the month's plan, so switching tabs does not wait. The server reads the month tab, rates, `fact` and «Календарь» at the same time and keeps a copy for two minutes; any write of the bot drops it, and a new launch and «Обновить» always read the table anew — edits made by hand in the table show up there.

Home screen: set the bot's Main Mini App in @BotFather (Bot Settings → Configure Mini App → the service URL with `/app/`); the app opened from the bot profile or by `t.me/<bot>?startapp=home` gets Telegram's `initData`, and «📌 На экран телефона» on «Сводка» adds the icon (Bot API 8.0+).

```bash
cd webapp
nvm use            # Node from .nvmrc
npm ci
npm run dev        # the page with test data in a browser, no Telegram and no bot
npm run lint && npm test && npm run build
```

## Usage

1.  **Send `/start`.** With the keyboards off (the default) it only says how to switch them on; switch on the groups and the cards in `/keyboards` to enter by hand and pick a source (card) on the keyboard. After a restart the bot takes the card of the last record, so it does not ask again.
2.  **Pick a category and a subcategory** (groups on) on the buttons under the message (two per row, subcategories with their icons; the line above names the card and the choice so far). Action buttons are coloured: «✅ Записать» green, «Выровнять» blue.
3.  **Type the amount and an optional comment**: `48000 latte`, `5 000,50 lunch`. A leading `+` records incoming money: `+20000 refund`.
4.  **Or just send an SMS, a screenshot, a PDF/Excel/CSV file** — Gemini extracts the transactions; the bot converts currencies, picks the card by its number, checks categories against `system` and writes all rows in one request. Rows it cannot write are listed in the reply. While Gemini reads it the message gets 👀 and Telegram shows «Думаю…» (message draft, Bot API 9.3); once everything in it is written, the SMS or screenshot leaves the chat — the card shows what was written. One written only in part keeps 👍, one with nothing written keeps 🤔 until the morning sweep.
    *   The same SMS, screenshot or file sent twice is written once: the bot remembers what it wrote during the last week and answers which rows already hold it.
    *   Kapitalbank and UZCARD SMS (one or many pasted at once) are parsed by rules. Rows the bot is not sure about are marked: 🆕 — a new shop (Gemini's suggestion), ❓ — no shop (a transfer, cash, a top-up). Dollars withdrawn in cash at the bank go to «квартплата»; only the sum in `RENT_CASH_USD` comes without ❓ (the sum is a setting, not code: the repository is public). With the groups on (`/keyboards`) a single such SMS gets «✓ Верно» and the groups right under the summary; otherwise, and for several, the summary gets «🔎 Проверить (N)», which goes through them one by one. Nothing pressed — the category stays. A choice is remembered: a new shop goes to the chosen category next time; for a shop with an established category a one-off correction is remembered for that amount only. Confirmation codes and declined payments are skipped, SMS already written are skipped one by one.
    *   When an SMS shows the card balance, the reply compares it with the balance in the sheet: «🟰 сходится» or the difference.
    *   Under the written rows the summary shows how each subcategory of the month stands — «🟢 ☕ кофе: 1,07 из 2,00 млн (53%)», «🟡 … — осталось …» from 80 %, «🔴 … — сверх плана на …», «⚪ … вне плана, в октябре уже …» — and what is left for today: «💸 На сегодня осталось 227 993 из 533 541». Incomes and transfers between own cards get no lines; if the sheet is slow, the record is still written and the lines are skipped.
    *   Each write has **«✏️ Исправить запись»** (the `/fix` dialog) and **«↩️ Отменить запись»** (asks first, then deletes the rows like `/undo`). With both keyboards off they sit on the summary itself: «Отменить» swaps them for «🗑 Да, удалить запись» / «Нет», and «Да» deletes the rows and the summary and says «↩️ Удалил из таблицы: строка …». With a keyboard on they are in the message under the summary («Удалить строку …?»), above the groups when those are on. The buttons disappear with the summary when the next record comes; `/fix` and `/undo` keep working.
6.  **Send screenshots of the banks' main screens** (the card list with balances). The bot reads every card's balance (Gemini decides whether a photo shows balances or transactions — one request), writes it to «Проверка» with the time in «Сверено» (`fact!Q`) and answers card by card: «✅ сходится» or «⚠️ в таблице больше/меньше на …». Transactions visible on such a screen are ignored (they come from SMS and statements). For each mismatch there is a **Выровнять** button: it recomputes the difference at the moment of the tap and adds one row to «🚧 РАЗНОЕ / неучтенка» (undo with `/undo`). Hidden balances, non-card products and cards from `IGNORED_CARDS` are skipped; a card that is not in `system` is reported.
5.  **Send Kapitalbank statements** (PDF «История операций», one or several cards at once). The bot waits a few seconds for all files, then shows one preview: new rows, rows already in the sheet, how each card balance changes and rows worth checking. **✅ Записать** writes everything in one go:
    *   operations booked up to the date in `system!G` («выписка по», next to the card) were loaded before and are skipped; after the import the date moves to the last full day of the statement;
    *   a row the bot once converted at the sheet rate (comment with `≈`) gets the bank amount instead of a second row;
    *   a temporary balancing row (comment contains `ВРЕМЕННАЯ`) is deleted once a statement covers its date;
    *   sending the same statements again changes nothing.

### Weekly digest

On Sundays at `WEEKLY_DIGEST_TIME` (default `20:00`, `off` disables it) the owner gets the week's spending against the previous week, the three biggest subcategories, the `/plan` report and a reminder to send bank screenshots (cards not checked for more than a week are named).

### Morning message and the daily limit

Every morning at `MORNING_TIME` (default `08:00`, `off` disables it) the owner gets how much can be spent today, and `/today` shows the same at any moment with what is spent and left today. The limit is money until the salary, not what is left of the plan: the sums on the evening before (all UZS rows of fact — cards and cash) plus the advances before the salary minus the payments in sums up to and including the day of the next main salary in sums — the open minus rows of the yellow list (columns `O:S` of the month tab) in UZS, late ones too, and the subscriptions in UZS outside the list that are not cancelled and not yet charged in their month — spread over the days up to and including that salary's day: the salary may come in the evening, so its day lives on the money there is. The main salary is the nearest open plus row of the list in UZS named «ЗП» or «зарплата» (not «аванс») later than today — other incomes of the list (a refund, a debt paid back) set no horizon and join the sums when they come; after the month's last one — the first such row of the list in the next month, the same day; without such rows the advance rows set the horizon as before, without them — the month end, today included. Advances («аванс» rows in UZS) due from today and before the salary are added to the money (an advance due today counts — its day lives on the money there is, so the limit does not dip for a day) — the open part of this month's rows and the next month's rows in full: a small mid-month advance does not split the month in two. A guard keeps the advance's money until it comes: when the money before the advance (sums minus the payments up to and including its day) spread over the days up to it gives less than the even limit, the limit counts to the advance without it. A salary due today or late is not waited for until its money is on the cards, nor is a late advance. Rubles and dollars are not this salary: the ruble pay comes when it is collected, and its sums count once transferred. The message names it — «💸 X сум», «можно тратить в день до зарплаты 30-го», «на сумовых картах …, аванс 15-го +…, платежи до 30-го …» — and says the plan in one line, the same as under the spending chart: «⚠️ Сверх плана на … за N дней» or «✓ Запас …» (everyday spending from the 1st to yesterday, without list payments, against the plan per day × the days). The savings goal of the month (yellow row «🎯 Отложить за месяц») is shown but does not change the limit; yesterday is shown against its own limit by the same rule.

On the day the main salary comes (its row is closed by fact now but was not this morning — a salary written today with yesterday's date already counts this morning) the morning limit is out of date: `/today`, «Сводка» (and its daily chart) and the reply to every record that day — the salary itself too, and a transfer moved into incomes by /fix — show the limit from tomorrow — «💸 С завтра можно X в день до зарплаты 30 ноября», «✓ пришла ЗП …» («✓ пришёл аванс …» when the advance stands in for the salary), «потрачено сегодня …», «на сумовых картах сейчас …» — counted by the same rule from the sums now (fact through today, with what was spent after the salary) over the days from tomorrow up to and including the next main salary; on the month's last day tomorrow is the 1st, and the tab's rows count again on the same days of the next month, in full. «Сверх лимита дня» is not shown that day. Without money for it even from tomorrow — «💸 Лимита на сегодня нет: до … не хватает …» with tomorrow's numbers. The next morning counts by the usual rule and gives the same number if nothing more was spent. The morning message itself does not change.

The message (and «Сводка» in the Mini App) also says whether the sums last until the salary: «💳 Сумы 7,83 млн — до зарплаты 30-го хватит, останется ≈ 1,20 млн» or «💳 Сумы 2,00 млн — хватит до 19.10 · до зарплаты 30-го обменять ≈ $1 150». It is its own simple count by days, not the «Календарь» model: the horizon is the day of the next main salary that has not come yet (by fact now; one due today or late, while its money is not on the cards, is waited for today, so the horizon ends today — «до зарплаты сегодня»; the salary itself is not added), after the month's last one — the first salary row in the next month, without salary rows — the month end. It starts from the sums now (all UZS sources — cards and cash — with today's rows); today only what is left of the day's pace goes out, then the pace every day; the advances of the horizon come in on their day, the payments of the list in sums (late ones today), the subscriptions in sums and the unpaid one-off records of «Календарь» in sums go out on theirs — as in the calendar's pace line, so the card and «Календарь» agree; spending that a repeating record covers today stays ordinary spending of the day. The pace is the average spending in sums over the 14 days before today (incomes, transfers between own cards, payments of the yellow list and payments that closed one-off records of the «Календарь» sheet excluded, refunds reduce it). «Хватит» — the sums never go below zero, «останется» — their evening on the salary day; otherwise the date is the last day the sums stay above zero and the dollars cover the deepest gap, at the `system` USD rate, rounded up to $10.

When yesterday stayed within its limit, the morning message comes with the 🎉 effect (`CELEBRATE_EFFECT_ID`; if Telegram rejects the effect, the message is sent without it).

The morning message, `/today`, the Sunday digest and `/plan` go as Telegram rich messages (Bot API 10.1+): the main number and separate facts as headings whose digits are Unicode monospace digits (𝟶–𝟿: Telegram draws rich headings in a serif font, which the owner cannot read in numbers, and the phone draws these digits in a font without serifs; a number never splits on a wrap), lists as bordered tables, whole numbers. If Telegram refuses one, the same report is sent as plain text; `RICH_MESSAGES=off` sends plain text to everyone (an old Telegram app shows a rich message as unsupported instead of refusing it). The reply after a record (the amount as a heading, the card with its balance check, each article of the month and what is left for today), the «🐷» messages, notes to the other user, `/subs` and `/undo` use the same view; the menu of categories and the button dialogs stay plain text.

Frozen money is the balance of the cards in `FROZEN_CURRENCY` (the dollar card) minus payments in that currency from the list that are still ahead (rent until it is paid). The limit never spends it: the limit counts only sums, and dollars join it once exchanged into sums. The message shows how much is frozen and how it changed since the 1st. «Сводка» treats it, minus the balances of the piggy banks as of yesterday evening, as the emergency fund (the «Накопления» section of «План»; a warning shows when the piggy banks hold more than is frozen): «🛟 Подушка безопасности» shows it in months of the average spending of the last three full months (transfers between own cards are not spending), with a bar to the `CUSHION_MONTHS` target (6) and what is short of it — orange under three months, yellow under the target, mint at it. «Норма сбережений» next to it is the share of income left over the last three full months, with a bar for each of the last six (up — saved, down — the reserve was eaten; «нач остаток» is not income); the month in progress is not counted, since before the salary its share would be a huge minus. Without the piggy sheet, the write summary of an income adds «🛟 Отложите 10 % в подушку: 3 000 000 UZS (≈ $250)» — pay yourself first; the equivalent in the cushion currency says how much to exchange. The same line comes when a row is moved into income with /fix or «🔎 Проверить» (a salary by SMS arrives as a transfer) and after a statement import for this month's incomes (`SAVE_SHARE` takes «0.1», «10 %» or «off»; a share under 10 000 sums is not mentioned). With the piggy sheet the «🐷» message below says it instead. Payments from the list are not counted as spending of their day. The tab's limit cell (P5, «Лимит на сегодня») counts the same money until the salary by formula; the subscriptions up to the salary's day, which a formula cannot find, are written by the bot into Q of that row (with «Пришло к утру», every night and after a change).

Piggy banks («🐷 Копилки» in «Накопления») are envelopes inside the frozen money for big irregular spending and goals: a name, a target (or «Без цели» for a goal without a deadline), a currency, a deadline (or «Без срока»), once or every year, and the article whose spending comes out of it. Money is set aside as a percent of every income — income comes in parts and on dates nobody knows in advance, so a fixed monthly sum would hang as a debt: «Откладываем N % с поступлений» (the «Изменить» window: − / + and 3 · 5 · 10 · 15 · 20 %, with a preview of what each piggy will have by its deadline) replaces both the 10 % share and a fixed piggy plan; until it is set the percent is `SAVE_SHARE`. The month's capacity is that percent of the month tab's planned incomes. The piggies with a target and a deadline go in an importance order («Порядок» — ↑ ↓ with the same preview; by deadline until it is saved): each one's nearest deadline, in that order, then next years' deadlines, gets what fits without taking from a more important one, and the plan spreads those amounts over the months at the smallest steady rate, the earliest deadline first. A piggy that will not make it says «⚠ к 20.12 будет X из Y — не хватит Z», and the card names the percent that would make every deadline. What is left of the capacity goes to the cushion and the goals without a deadline (all to the cushion while it is under a month of spending, half to the goals while it is under `CUSHION_MONTHS`, then all to them). The card is short: «Откладываем N %» with «Изменить», the month's sum to set aside («за октябрь отложить …» — the percent of what came in by today; money that has not come yet counts once it is written), a bar of what the piggies' part already got and one line under it — «отложено X, ещё Y» with «Отложил…», or «✓ Отложено X из Y» once nothing is left — and «подушке — …» for the cushion's part. Warnings take one line each (the percent cannot be read, the deadlines need N %, the month leaves too little for life, the plan's «Отложить» differs); the good news and the details stay in the percent window. «Отложил…» takes one sum and lays it out: this month's parts by importance, then the piggies that fall short up to their shortfall, then the goals; what is left over stays in the cushion and is not written. The piggy's window edits it, records «Отложил…» and «Потратил…», writes off what is left after the deadline, closes it (the balance returns to the cushion) and shows its history. Spending of the piggy's article from 60 days before the deadline to 30 days after it (a one-off piggy — from its first saving) is taken from the piggy, the nearest deadline first and never twice; «Не из копилки» returns a payment. When the window closes, the bot writes what was spent in it as one journal row and moves an every-year deadline a year ahead. The piggies live in the «Копилки» sheet and their money moves in «Журнал копилок» (both created with the first piggy, editable by hand); the other user gets a note in the chat.

The bot speaks about the piggies in the chat too. After an income (written by hand, in the app, moved into income with /fix or this month's from a statement) a separate message comes under the menu: «🐷 С поступления 8 000 000 UZS — 10 %: 800 000 UZS. За октябрь отложить: «Новый год» 1 200 000 UZS» (plus «· в подушку с этого поступления …» for this income's cushion part), with «Отложил всё» — it writes what to set aside now to the journal, a second press finds nothing left, a button from a past month says the hint is out of date — and «Отложил часть…», which opens the app on «План → Накопления» with the «Отложил» window (once: switching a section or a tab clears it). When the piggies need nothing it says «… — всё в подушку» without buttons. A spending of a piggy's article inside its window brings «🐷 Из копилки «Новый год»: −2 400 000 UZS, осталось 3 600 000» with «Не из копилки» (several rows at once — a line per piggy without the button; the app's answer gets the same lines). «🎯 Отложить за месяц» follows the percent: the month's capacity minus what the piggies spend that month (in a deadline month a piggy spends what it will have collected) — below zero in a deadline month, shown in «План» as «💰 Из накоплений». A month tab the bot copies gets it with its dates; «План» suggests it with «Применить» and asks to put a piggy's spending into its article; «Накопления» say when the tab's goal differs.

### Monthly plan tabs

Every day at 00:05 (`ANALYTICS_TIMEZONE`) and right after start the bot checks the tab of the current month («Nov 26»). If it is missing, the bot copies the latest month tab (the plan stays the same), sets the month dates in `M1:M2`, hides past months and tells the owner (`ANALYTICS_CHAT_ID`, otherwise the first id from `ALLOWED_USER_IDS`). Fact values in the tab are formulas over `fact` and the dates in `M1:M2`, so nothing else changes.

### Clean chat

Every morning at `MORNING_TIME` (with `off` — at 08:00 anyway), right after the owner's morning message, the bot deletes in each chat of `ALLOWED_USER_IDS` what came before midnight since the last sweep: the previous morning message, summaries, answers, commands, notes about the other person's records, «🐷», the SMS that stayed. What came after midnight stays until the next morning (a night «📅 Новый месяц», a 7:40 card). The Sunday digest stays, so does «⌨️ Карты скрыты» / «⌨️ Клавиатуры скрыты» (deleting a message that removed the cards keyboard could bring it back in some Telegram apps) and the message holding the cards keyboard when it is on. Telegram has no way to read a chat's history, so the bot logs the number and the local day of every message it receives and sends (`app/message_log.py`; message numbers grow with time and are shared by all the bot's private chats, so a sweep skips what any chat keeps) and keeps «уборка с №» — the first message not swept yet — in `system!Q:U` with the keyboards settings. A bot can delete a message only within 48 hours: if the bot was down for more than a day, some messages stay, and the history before this feature is the user's to clear (⋮ → «Очистить историю»). After the update each person gets «⌨️ Клавиатуры скрыты» once; it removes the old cards keyboard and stays.

### Commands

*   **`/analytics`** — report for the last 3 days with charts.
*   **`/today`** — the daily limit: how much can be spent today, spent so far and left, yesterday against its limit, frozen money, lines over plan and the next incomes/payments. Rows of the «Поступления и платежи» list count as ahead until their money comes, not by their day: the bot reconciles them with `fact` (by the month's plan, without a window: the incomes of a currency from the 1st fill that currency's rows in order of their days — the first up to its sum, the rest to the next, what is over the last row to the last — so a salary paid in parts or early counts toward its row (any income of the currency counts, so a gift or cashback written as income is part of the month's plan and raises the limit only once the incomes pass the list); a payment is one spending within 10 % of its sum from five days before its day; 90 % closes a row) and writes what came in by this morning into the «Пришло к утру» column of the month tab every night and two minutes after a change. An income belongs to the month named by «за <месяц>» in its comment («за октябрь», «Зарплата за Сентябрь 2026» — the nearest month of that name, or the year in the mark when it is within a year of the date; the bank writes it only in statements, not in SMS), otherwise to the month of its date, and a month's rows are reconciled only with that month's incomes — so a part of October's salary that comes on 3 November marked «за октябрь» counts for October. A plus row of last month that got less than 90 % (by fact as of now, with its marked late parts) waits for its remainder this month: the morning message, `/today` and «Сводка» show it first in «Впереди» as «<name> за <месяц>, остаток» with the missing part, with ⏳ from the 2nd (the morning shows it on top of the month's two nearest rows). It is only a line of «Впереди»: it takes no money of this month, so the limit, «Пришло к утру» and P5 are as without it, and rows in sums carry over too. After an income without a mark, dated this month, while last month still waits for money in that currency (in sums — only a salary or advance while last month waits for its salary or advance), and when /fix moves a row into incomes, the bot asks in the background «📅 Эти 55 000 ₽ — за октябрь?» with «За октябрь» / «За ноябрь»; the button writes the mark into the row's comment (only if the row was not edited by hand; a second tap says «✓ Уже отмечено»). A statement import does not ask: the bank writes the mark there. Only one month back; payments do not carry over; «Свободно» and the totals of «План» count the tab's rows as before. Nothing else is written to the sheet. The day limit, the frozen dollars and the sums forecast — in the bot and in the tab's formulas — use it; the «План» screen shows «✓ пришло …», «пришло … из …» or «⏳ ждём с …» under a row.
*   **`/subs`** — subscriptions and other monthly payments found in `fact` (last 4 months): the same merchant (first two words of the name without digits and signs) with a similar amount (±25 %) in at least two months, the last three on the same day of the month (±4 days), and not a shop visited 3+ times a month. The list goes by day with the monthly total, subscriptions against their plan line, «⚠️ в октябре дважды», «(ждём 14.10)» / «(в октябре не было)» and payments not seen for 40+ days. The morning message adds «🔁 Сегодня/Завтра спишется: …» and warns the day after a second charge.
*   **`/plan`** — month plan vs fact without AI: spending pace against the calendar, what is left per day, lines over plan, close to the plan (80%+) and outside the plan. Lines of one subcategory in different currencies are added up in UZS.
*   **`/advice`** — AI analysis of spending vs. the current month plan; the answer appears as it is generated (streamed draft), with a plain request as a fallback.
*   **`/undo`** — delete the rows of the last write, if nobody changed them in the sheet since.
*   **`/fix`** — change the group and subgroup of the last write when the AI picked the wrong one (SMS, receipt or screenshot): the bot asks which operation (if there were several), then the group and the subgroup from `system`. The row is changed only if nobody edited it in the sheet; the money keeps its direction (an incoming row moved to or from «💰 ДОХОДЫ» flips the sign in `D`), and the merchant goes to the new category from the next message on.
*   **`/icons`** — pictures on the buttons (custom emoji, Bot API 9.4): after `/icons` send any custom emoji of a pack you like (several packs in one message are fine). The bot connects the whole pack and matches pictures by the plain emoji of `system!D` and of the group names, then reports how many subcategories and groups got a picture. Packs are kept in `system!J`; `/icons off` brings plain emoji back. Telegram shows such buttons only while the bot owner has Telegram Premium — the bot sees it in the owner's updates and falls back to plain emoji otherwise.
*   **`/keyboards`** — switch the buttons of the groups under a write and the cards keyboard on or off (each person for their own chat; off by default). The state is shown with a sign and a word, the buttons say what a tap does; switching on shows the keyboard at once, switching the cards off removes them from the screen.
*   **`/app`** — a button that opens the Mini App (Telegram's sign-in), for when the cards keyboard is off.
*   **`/reboot`** — reload categories, subcategories, sources and the keyboards settings from the `system` sheet.

## Development

```bash
pip install -r requirements-dev.txt
ruff check . && black --check . && pytest -q
cd webapp && npm ci && npm run lint && npm test && npm run build
```

CI runs the same checks — the bot and the Mini App, as two jobs — on every push and pull request.

## Contributing

If you'd like to contribute, please fork the repository, make your changes, and submit a Pull Request. We welcome any improvements!

## License

This project is licensed under the MIT License. See the `LICENSE` file for details (you'll need to create this file if you plan to include one).
