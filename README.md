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
GOOGLE_PRIVATE_KEY="YOUR_GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY" # Can be multi-line, ensure quotes handle it correctly or use \n format

# Optional for running in webhook mode (for production)
# WEBHOOK_URL="YOUR_WEBHOOK_URL" # e.g., https://your-domain.com
# PORT="8443" # Port for the webhook

# Optional for local run, if different from config.py default
# LOCAL_RUN="True"
```

**Important note on `GOOGLE_PRIVATE_KEY`:**
*   The private key from the Google service account JSON file must be pasted as a string.
*   If you copy it directly, it will contain newline characters (`\n`). In the `.env` file, these characters should either be escaped (`\\n`) or the entire string should be enclosed in double quotes if your system supports it. The `bot.py` file attempts to handle `\n` when loading from `.env`.

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
    *   **Columns (minimum):** `Date`, `Category`, `Subcategory`, `Amount`, `Balance` (formula), `Comment`, `Currency`, `Source`.
    *   Formula for the `Balance` column (example for row 2, adapt to your needs and Google Sheets formula language):
        ```excel
        =SUMIFS($D$2:D2, $H$2:H2, $H2, $G$2:G2, $G2, $B$2:B2, "💰 INCOME") - SUMIFS($D$2:D2, $H$2:H2, $H2, $G$2:G2, $G2, $B$2:B2, "<>💰 INCOME")
        ```
        This formula calculates the current balance for a specific source and currency by summing all incomes and subtracting all expenses. Note: "💰 INCOME" should be exactly how your income categories are named or identified. If you use a different term or logic for income vs. expense in your 'Category' column (B), adjust the formula accordingly.
*   **`system`**: This sheet is used for bot configuration (categories, subcategories, sources).
    *   **Column A:** Categories (e.g., "Groceries", "Transport").
    *   **Column B:** Subcategories (e.g., for "Groceries": "Supermarket", "Market"). The corresponding category from Column A must be specified.
    *   **Column F:** Sources (e.g., "Card UZS", "Cash USD"). The last 3 characters of the source name are used to determine the currency (e.g., "UZS", "USD").

### 7. Running the bot:

*   **Local run (polling):**
    Set `LOCAL_RUN="True"` in `.env` (or don't set it if this is the default in `config.py` when `WEBHOOK_URL` is absent).
    ```bash
    python bot.py
    ```
*   **Webhook mode (for production):**
    Ensure `LOCAL_RUN` is not set or is `False`.
    Set `WEBHOOK_URL` and, if necessary, `PORT` in your `.env` file.
    ```bash
    python bot.py
    ```
    The bot will listen for incoming requests from Telegram at the specified `WEBHOOK_URL` and port. You might need to set up a reverse proxy (e.g., Nginx) to handle HTTPS and forward traffic to the bot's port.

## Usage

1.  **Start the bot** in Telegram (find it by the name you gave it when creating the token).
2.  **Send the `/start` command.**
3.  **Select Source:** The bot will prompt you to select a source of funds using a keyboard. The transaction currency will be determined by the last three characters of the source name. If no source is selected, many operations will be unavailable.
4.  **Select Category:** After selecting a source, an inline keyboard with categories will appear.
5.  **Select Subcategory:** After selecting a category, an inline keyboard with subcategories will appear.
6.  **Enter amount and comment:** After selecting a subcategory, the bot will ask you to enter the transaction amount and, optionally, a comment separated by a space (e.g., `150.50 Lunch at cafe`).
7.  **Data logging:** The data will be recorded in the Google Sheet on the `fact` sheet.

### Other commands and features:

*   **`/reboot`**: Updates the lists of categories, subcategories, and sources from the Google Sheet (`system` sheet) without restarting the bot. Useful if you've made changes to the sheet.
*   **"SMS" Button**:
    *   After selecting a source, press the "SMS" button on the inline category keyboard.
    *   The bot will switch to SMS waiting mode.
    *   Paste the text of one or more SMS messages from your bank.
    *   The bot will attempt to recognize the date, amount, currency, and transaction type (income/expense) from each SMS.
    *   Recognized transactions will be added to the `fact` sheet. The comment will be part of the SMS text, and the category will be the transaction type.
*   **"Back" Navigation**: Use the "⬅️ Back" buttons to return to previous selection steps.
*   **Changing Source**: Simply press the button with the desired source on the main keyboard (which appears after `/start` or when changing sources). Category and subcategory selections will be reset, and you'll need to choose them again for the new source.

## Contributing

If you'd like to contribute, please fork the repository, make your changes, and submit a Pull Request. We welcome any improvements!

## License

This project is licensed under the MIT License. See the `LICENSE` file for details (you'll need to create this file if you plan to include one).
