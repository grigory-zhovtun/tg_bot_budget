import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from google.oauth2.service_account import Credentials
import gspread
import re
from datetime import datetime
from requests.exceptions import ConnectionError
# Импортируем переменные из config.py
# Обратите внимание, что если переменные не установлены в окружении и LOCAL_RUN=False,
# они могут быть None после этого импорта, если в config.py нет значений по умолчанию.
from config import TELEGRAM_TOKEN as CONFIG_TELEGRAM_TOKEN, \
                     GOOGLE_PRIVATE_KEY as CONFIG_GOOGLE_PRIVATE_KEY, \
                     GOOGLE_SERVICE_ACCOUNT_EMAIL as CONFIG_GOOGLE_SERVICE_ACCOUNT_EMAIL, \
                     SPREADSHEET_ID as CONFIG_SPREADSHEET_ID

LOCAL_RUN = True # Установите False для продакшена, где переменные окружения настроены на сервере

# Переопределяем переменные из .env, если LOCAL_RUN True, или используем из config (которые берутся из os.environ)
TELEGRAM_TOKEN = CONFIG_TELEGRAM_TOKEN
GOOGLE_PRIVATE_KEY = CONFIG_GOOGLE_PRIVATE_KEY
GOOGLE_SERVICE_ACCOUNT_EMAIL = CONFIG_GOOGLE_SERVICE_ACCOUNT_EMAIL
SPREADSHEET_ID = CONFIG_SPREADSHEET_ID
# GOOGLE_APPLICATION_CREDENTIALS_PATH - если используется, также должен быть загружен из .env/os.environ
GOOGLE_APPLICATION_CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_PATH')


if LOCAL_RUN:
    from dotenv import load_dotenv
    load_dotenv()
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', CONFIG_TELEGRAM_TOKEN)
    GOOGLE_PRIVATE_KEY_ENV = os.getenv('GOOGLE_PRIVATE_KEY')
    GOOGLE_PRIVATE_KEY = GOOGLE_PRIVATE_KEY_ENV.replace('\\n', '\n') if GOOGLE_PRIVATE_KEY_ENV else CONFIG_GOOGLE_PRIVATE_KEY
    GOOGLE_SERVICE_ACCOUNT_EMAIL = os.getenv('GOOGLE_SERVICE_ACCOUNT_EMAIL', CONFIG_GOOGLE_SERVICE_ACCOUNT_EMAIL)
    SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', CONFIG_SPREADSHEET_ID)
    # WEBHOOK_URL и другие переменные, специфичные для окружения, также могут быть загружены здесь
    # WEBHOOK_URL = os.getenv('WEBHOOK_URL')


TOKEN = TELEGRAM_TOKEN # TOKEN используется для ApplicationBuilder

FALLBACK_CURRENCY = 'XXX'

CATEGORIES = []
SUBCATEGORIES = {}
SOURCES = []

# Аутентификация в Google Sheets
if not GOOGLE_PRIVATE_KEY or not GOOGLE_SERVICE_ACCOUNT_EMAIL:
    print("Ошибка: Не все учетные данные Google Cloud установлены (GOOGLE_PRIVATE_KEY или GOOGLE_SERVICE_ACCOUNT_EMAIL). Проверьте переменные окружения или файл .env.")
    creds = None
    client = None
    sheet = None
else:
    creds_info = {
        "type": "service_account",
        "private_key": GOOGLE_PRIVATE_KEY,
        "client_email": GOOGLE_SERVICE_ACCOUNT_EMAIL,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        # URL формируется динамически, убедитесь, что GOOGLE_SERVICE_ACCOUNT_EMAIL корректен
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{GOOGLE_SERVICE_ACCOUNT_EMAIL.replace('@', '%40')}" if GOOGLE_SERVICE_ACCOUNT_EMAIL else ""
    }
    try:
        creds = Credentials.from_service_account_info(creds_info,
                                                      scopes=["https://www.googleapis.com/auth/spreadsheets"])
        client = gspread.authorize(creds)
        if SPREADSHEET_ID:
            sheet = client.open_by_key(SPREADSHEET_ID)
        else:
            print("Ошибка: SPREADSHEET_ID не установлен. Невозможно открыть таблицу.")
            sheet = None
    except Exception as e:
        print(f"Ошибка при аутентификации или открытии таблицы Google: {e}")
        creds = None
        client = None
        sheet = None

MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}


def get_currency_from_source(source_name: str) -> str:
    if source_name and len(source_name) >= 3:
        return source_name[-3:].upper()
    return FALLBACK_CURRENCY


def _parse_one_sms(sms_text: str, current_year: int) -> dict:
    res = {'дата': None, 'сумма': None, 'валюта_из_смс': None, 'операция': 'неизвестно'}
    text = sms_text.strip()
    low = text.lower()

    m_sum_specific = re.search(
        r'summa:?\s*(-?\d+(?:[.,]\d+)?)\s*,?\s*(UZS|RUB|USD|EUR)\b',
        text,
        flags=re.IGNORECASE
    )
    m_sum_to_use = None
    if m_sum_specific:
        m_sum_to_use = m_sum_specific
    else:
        m_sum_general = re.search(
            r'(-?\d+(?:[.,]\d+)?)\s*,?\s*(UZS|RUB|USD|EUR)\b',
            text,
            flags=re.IGNORECASE
        )
        m_sum_to_use = m_sum_general

    if m_sum_to_use:
        amt_str = m_sum_to_use.group(1).replace(',', '.')
        try:
            res['сумма'] = float(amt_str)
            res['валюта_из_смс'] = m_sum_to_use.group(2).upper()
        except ValueError:
            pass

    dt = None
    date_parse_patterns = [
        (r'(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})', 'custom_day_mon_year_time'),
        (r'(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})', '%d.%m.%Y %H:%M'),
        (r'(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2})', '%d-%m-%Y %H:%M'),
        (r'(\d{2}\.\d{2}\.\d{2}\s+\d{2}:\d{2})', '%d.%m.%y %H:%M'),
        (r'(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2})', '%d/%m/%y %H:%M'),
        (r'(\d{2}\.\d{2}\s+\d{2}:\d{2})', '%d.%m %H:%M'),
    ]

    for pat_regex, fmt_or_handler in date_parse_patterns:
        if dt: break
        m_date = re.search(pat_regex, text)
        if m_date:
            try:
                parsed_datetime_obj = None
                date_str_matched = m_date.group(1)
                if fmt_or_handler == 'custom_day_mon_year_time':
                    sub_match = re.match(r'(\d{2})-([A-Za-z]{3})-(\d{4})\s+(\d{2}:\d{2})', date_str_matched)
                    if sub_match:
                        day_s, mon_s, year_s, time_s = sub_match.groups()
                        month_num = MONTH_MAP.get(mon_s.upper())
                        if month_num:
                            std_date_str = f"{day_s}-{month_num:02d}-{year_s} {time_s}"
                            parsed_datetime_obj = datetime.strptime(std_date_str, '%d-%m-%Y %H:%M')
                else:
                    parsed_datetime_obj = datetime.strptime(date_str_matched, fmt_or_handler)

                if fmt_or_handler == '%d.%m %H:%M' and parsed_datetime_obj:
                    dt = parsed_datetime_obj.replace(year=current_year)
                else:
                    dt = parsed_datetime_obj
            except ValueError:
                pass
    res['дата'] = dt

    if res['сумма'] is not None:
        op_type = 'неизвестно'
        income_keywords = ['поступлен', 'zachisl', 'zachislenie', 'popolnen']
        expense_keywords_for_positive_sum = ['xarid', 'pokupka', 'списан', 'spisan', 'oplata', 'platezh']

        if 'otmena' in low:
            op_type = 'доход' if res['сумма'] > 0 else ('расход' if res['сумма'] < 0 else 'неизвестно')
        elif res['сумма'] < 0:
            op_type = 'расход'
        else:
            is_explicit_income = any(keyword in low for keyword in income_keywords)
            is_explicit_expense = any(keyword in low for keyword in expense_keywords_for_positive_sum)

            if is_explicit_income:
                op_type = 'доход'
            elif is_explicit_expense:
                op_type = 'расход'
            elif 'perevod na kartu' in low:
                op_type = 'расход' if not is_explicit_income and res['сумма'] >= 0 else 'доход'
            elif res['сумма'] > 0:
                op_type = 'доход'
        res['операция'] = op_type
    return res


def parse_sms_by_date(input_text: str) -> list[dict]:
    current_year = datetime.now().year
    final_sms_start_patterns = [
        r'Karta\s+\*\d{4}', r'Schet\s+po\s+karte\s+\*\d{4}',
        r'OTMENA\s+E-Com\s+oplata:', r'Pokupka:', r'E-Com\s+oplata:',
        r'Platezh:', r'Perevod na kartu:',
    ]
    combined_start_pattern = r'(?=(' + '|'.join(final_sms_start_patterns) + r'))'
    raw_sms_list = re.split(combined_start_pattern, input_text)
    actual_sms_messages = [sms.strip() for sms in raw_sms_list if sms and sms.strip()]
    parsed_records = [_parse_one_sms(m, current_year) for m in actual_sms_messages]
    unique_records = []
    seen_keys = set()
    for record in parsed_records:
        if record['дата'] and record['сумма'] is not None:
            record_key = (record['дата'], record['сумма'], record['операция'])
            if record_key not in seen_keys:
                seen_keys.add(record_key)
                unique_records.append(record)
    return unique_records


def load_keyboard_data():
    global CATEGORIES, SUBCATEGORIES, SOURCES, sheet, client

    if not sheet:
        print("Sheet не инициализирован в load_keyboard_data. Данные не могут быть загружены.")
        if client and SPREADSHEET_ID:
            try:
                # print("Попытка повторно открыть таблицу в load_keyboard_data...") # Пример удаленного комментария
                sheet = client.open_by_key(SPREADSHEET_ID)
            except Exception as e_reopen:
                print(f"Не удалось повторно открыть таблицу: {e_reopen}")
                return CATEGORIES, SUBCATEGORIES, SOURCES
        else:
            return CATEGORIES, SUBCATEGORIES, SOURCES

    try:
        system_sheet = sheet.worksheet("system")
        data = system_sheet.get_all_values()
    except (gspread.exceptions.APIError, ConnectionError) as e:
        print(f"Ошибка API/Соединения Google Sheets при загрузке данных клавиатуры: {e}. Попытка переподключения...")
        try:
            if GOOGLE_APPLICATION_CREDENTIALS_PATH and os.path.exists(GOOGLE_APPLICATION_CREDENTIALS_PATH):
                client = gspread.service_account(filename=GOOGLE_APPLICATION_CREDENTIALS_PATH)
            elif GOOGLE_PRIVATE_KEY and GOOGLE_SERVICE_ACCOUNT_EMAIL : # Попытка пересоздать creds из основных переменных
                print("Попытка пересоздать аутентификацию gspread...")
                creds_info_retry = {
                    "type": "service_account",
                    "private_key": GOOGLE_PRIVATE_KEY,
                    "client_email": GOOGLE_SERVICE_ACCOUNT_EMAIL,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{GOOGLE_SERVICE_ACCOUNT_EMAIL.replace('@', '%40')}"
                }
                creds_retry = Credentials.from_service_account_info(creds_info_retry, scopes=["https://www.googleapis.com/auth/spreadsheets"])
                client = gspread.authorize(creds_retry)
            else: # крайний случай, если нет ни пути к файлу, ни ключей для ручного создания
                import google.auth
                scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
                creds_retry, _ = google.auth.default(scopes=scopes) # может не сработать без GOOGLE_APPLICATION_CREDENTIALS
                client = gspread.authorize(creds_retry)


            if client and SPREADSHEET_ID:
                sheet = client.open_by_key(SPREADSHEET_ID)
                system_sheet = sheet.worksheet("system")
                data = system_sheet.get_all_values()
            else:
                raise Exception("Не удалось переинициализировать client или sheet.")
        except Exception as ex:
            print(f"Не удалось переподключиться и загрузить данные: {ex}")
            return CATEGORIES, SUBCATEGORIES, SOURCES

    temp_categories = []
    temp_subcategories = {}
    temp_sources = []

    if len(data) > 1:
        for row_idx, row in enumerate(data):
            if row_idx == 0:
                continue

            category = row[0].strip() if len(row) > 0 and row[0] else ""
            subcategory = row[1].strip() if len(row) > 1 and row[1] else ""
            source_from_sheet = row[5].strip() if len(row) > 5 and row[5] else ""

            if category and category not in temp_categories:
                temp_categories.append(category)

            if category and subcategory:
                temp_subcategories.setdefault(category, []).append(subcategory)

            if source_from_sheet and source_from_sheet not in temp_sources:
                temp_sources.append(source_from_sheet)

    CATEGORIES = temp_categories
    SUBCATEGORIES = temp_subcategories
    SOURCES = temp_sources

    return CATEGORIES, SUBCATEGORIES, SOURCES


if sheet:
    load_keyboard_data()
else:
    print("Sheet не был инициализирован при запуске. Данные клавиатуры не загружены.")


def generate_categories_keyboard(context: ContextTypes.DEFAULT_TYPE = None):
    keyboard = []
    row_buttons = []
    for idx, category in enumerate(CATEGORIES, 1):
        row_buttons.append(InlineKeyboardButton(text=category, callback_data=f"cat_{category}"))
        if idx % 3 == 0 or idx == len(CATEGORIES):
            keyboard.append(row_buttons)
            row_buttons = []
    if row_buttons:
        keyboard.append(row_buttons)

    action_buttons_row = [
        InlineKeyboardButton(text="СМС", callback_data="sms")
    ]

    source_button_text = "Источник (не выбран)"
    current_source = None
    if context and context.user_data.get('source'):
        current_source = context.user_data.get('source')
        source_button_text = f"Источник: {current_source}"

    action_buttons_row.append(InlineKeyboardButton(text=source_button_text, callback_data="change_source"))
    keyboard.append(action_buttons_row)

    return InlineKeyboardMarkup(keyboard)


def generate_sources_keyboard():
    keyboard = []
    row_buttons = []
    for idx, src_name in enumerate(SOURCES, 1):
        row_buttons.append(InlineKeyboardButton(text=src_name, callback_data=f"set_source_{src_name}"))
        if idx % 2 == 0 or idx == len(SOURCES):
            keyboard.append(row_buttons)
            row_buttons = []
    if row_buttons:
        keyboard.append(row_buttons)
    keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_categories_from_source")
    ])
    return InlineKeyboardMarkup(keyboard)


def generate_subcategories_keyboard(selected_category):
    keyboard = []
    row_buttons = []
    subcategories_list = SUBCATEGORIES.get(selected_category, [])
    for idx, subcategory_name in enumerate(subcategories_list, 1):
        row_buttons.append(InlineKeyboardButton(text=subcategory_name, callback_data=f"sub_{subcategory_name}"))
        if idx % 2 == 0 or idx == len(subcategories_list):
            keyboard.append(row_buttons)
            row_buttons = []
    if row_buttons:
        keyboard.append(row_buttons)
    keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_categories")
    ])
    return InlineKeyboardMarkup(keyboard)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not sheet:
        await update.message.reply_text(
            'Ошибка: Не удалось подключиться к Google Sheets. Пожалуйста, проверьте конфигурацию (переменные окружения / .env) и перезапустите бота.')
        return

    current_source = context.user_data.get('source')
    if not current_source and SOURCES:
        context.user_data['source'] = SOURCES[0]
        current_source = SOURCES[0]
    elif not SOURCES:
        await update.message.reply_text(
            "Список источников пуст. Пожалуйста, заполните источники в Google Таблице (лист 'system', колонка F) и выполните /reboot.",
            reply_markup=None)
        return

    derived_currency = FALLBACK_CURRENCY
    if current_source:
        derived_currency = get_currency_from_source(current_source)

    welcome_message = f'Привет, {update.effective_user.first_name}!\nВыбери действие:'
    if current_source:
        welcome_message += f'\nТекущий источник: {current_source} (Валюта: {derived_currency})'
    else:
        welcome_message += f'\nИсточник не выбран. Валюта не определена.'

    await update.message.reply_text(
        welcome_message,
        reply_markup=generate_categories_keyboard(context)
    )


async def reboot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not client:
        await update.message.reply_text(
            'Ошибка: Не удалось подключиться к Google Sheets (client не инициализирован). Команда reboot не может обновить данные.')
        return

    global CATEGORIES, SUBCATEGORIES, SOURCES, sheet # Объявляем sheet как global для возможного переназначения
    if not sheet and client and SPREADSHEET_ID:
        try:
            print("Попытка открыть таблицу в reboot...")
            sheet = client.open_by_key(SPREADSHEET_ID)
        except Exception as e_reopen_reboot:
            await update.message.reply_text(f'Ошибка при попытке открыть таблицу в reboot: {e_reopen_reboot}')
            return

    if not sheet: # Повторная проверка после попытки открытия
        await update.message.reply_text(
            'Ошибка: Не удалось подключиться к Google Sheets (sheet не инициализирован). Команда reboot не может обновить данные.')
        return

    CATEGORIES, SUBCATEGORIES, SOURCES = load_keyboard_data() # load_keyboard_data сама обрабатывает ошибки с sheet

    current_source = context.user_data.get('source')
    source_updated = False
    if not current_source and SOURCES:
        context.user_data['source'] = SOURCES[0]
        source_updated = True
    elif current_source and current_source not in SOURCES:
        context.user_data['source'] = SOURCES[0] if SOURCES else None
        source_updated = True

    if SOURCES or source_updated : # Условие изменено чтобы сообщение об обновлении появлялось даже если источники были пустыми и остались пустыми
        await update.message.reply_text('Данные клавиатуры (категории, подкатегории, источники) успешно обновлены.')
        await start(update, context) # Передаем update и context в start
    elif not SOURCES: # Это условие теперь избыточно, т.к. предыдущее его покрывает
        await update.message.reply_text(
             "Данные клавиатуры обновлены, но список источников пуст. Пожалуйста, заполните их в Google Таблице.")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    selected_source = context.user_data.get('source')
    derived_currency = FALLBACK_CURRENCY
    if selected_source:
        derived_currency = get_currency_from_source(selected_source)

    if data.startswith("cat_"):
        selected_category = data[4:]
        context.user_data['category'] = selected_category
        if not selected_source:
            await query.edit_message_text(
                text=f"Сначала выберите ИСТОЧНИК.\nЗатем выберите категорию.",
                reply_markup=generate_categories_keyboard(context)
            )
            return
        await query.edit_message_text(
            text=f"Источник: {selected_source} (Валюта: {derived_currency})\nКатегория: {selected_category}\n\nВыберите подкатегорию:",
            reply_markup=generate_subcategories_keyboard(selected_category)
        )

    elif data in ["back_to_categories", "back_to_categories_from_source"]:
        context.user_data.pop('category', None)
        context.user_data.pop('subcategory', None)

        message_text = "Выбери категорию:"
        if selected_source:
            message_text = f"Источник: {selected_source} (Валюта: {derived_currency})\n{message_text}"
        else:
            message_text = f"Источник не выбран. Валюта не определена.\n{message_text}"

        await query.edit_message_text(
            text=message_text,
            reply_markup=generate_categories_keyboard(context)
        )

    elif data == "sms_back":
        context.user_data.pop('sms_mode', None)
        message_text = "Выбери категорию:"
        if selected_source:
            message_text = f"Источник: {selected_source} (Валюта: {derived_currency})\n{message_text}"
        else:
            message_text = f"Источник не выбран. Валюта не определена.\n{message_text}"
        await query.edit_message_text(
            text=message_text,
            reply_markup=generate_categories_keyboard(context)
        )

    elif data.startswith("sub_"):
        subcategory_name = data[4:]
        context.user_data['subcategory'] = subcategory_name
        category = context.user_data.get('category', 'Не выбрана')
        if not selected_source:
            await query.edit_message_text(
                text=f"Ошибка: Источник не выбран. Пожалуйста, вернитесь и выберите источник.",
                reply_markup=generate_categories_keyboard(context)
            )
            return
        prompt_text = (f"Источник: {selected_source}\n"
                       f"Категория: {category}\n"
                       f"Подкатегория: {subcategory_name}\n"
                       f"Валюта: {derived_currency}\n\n"
                       f"ВНЕСИТЕ СУММУ И КОММЕНТАРИЙ (ЧЕРЕЗ ПРОБЕЛ):")
        await query.edit_message_text(
            text=prompt_text,
            reply_markup=generate_subcategories_keyboard(category) # Здесь остаётся клавиатура подкатегорий для навигации "Назад"
        )

    elif data == "change_source":
        if not SOURCES:
            await query.edit_message_text(
                text="Список источников пуст. Невозможно выбрать источник. Заполните Google Таблицу.",
                reply_markup=generate_categories_keyboard(context)
            )
            return
        await query.edit_message_text(
            text="Выберите источник:",
            reply_markup=generate_sources_keyboard()
        )

    elif data.startswith("set_source_"):
        source_name = data[len("set_source_"):]
        context.user_data['source'] = source_name
        new_derived_currency = get_currency_from_source(source_name)
        context.user_data.pop('category', None)
        context.user_data.pop('subcategory', None)
        await query.edit_message_text(
            text=f"Источник '{source_name}' выбран (Валюта: {new_derived_currency}).\nВыбери категорию:",
            reply_markup=generate_categories_keyboard(context)
        )

    elif data == "sms":
        if not selected_source:
            await query.edit_message_text(
                text=f"Пожалуйста, сначала выберите ИСТОЧНИК.\nЗатем нажмите кнопку 'СМС' снова.",
                reply_markup=generate_categories_keyboard(context)
            )
            return
        context.user_data['sms_mode'] = True
        await query.edit_message_text(
            text=f"Источник: {selected_source} (Валюта: {derived_currency})\nВставьте скопированные СМС:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="sms_back")]
            ])
        )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global client, sheet # Объявляем client и sheet глобальными для возможного переподключения

    if not sheet: # Проверяем sheet в начале
        await update.message.reply_text(
            'Ошибка: Не удалось подключиться к Google Sheets. Данные не могут быть обработаны.')
        return

    user_selected_source = context.user_data.get('source')
    transaction_currency = FALLBACK_CURRENCY
    if user_selected_source:
        transaction_currency = get_currency_from_source(user_selected_source)
    else:
        await update.message.reply_text(
            "Ошибка: Источник не выбран. Пожалуйста, выберите источник через меню.",
            reply_markup=generate_categories_keyboard(context)
        )
        return

    if context.user_data.get('sms_mode'):
        original_message_id = update.message.message_id
        chat_id = update.message.chat_id

        text = update.message.text
        try:
            records = parse_sms_by_date(text)
            if not records:
                await update.message.reply_text("Не удалось распознать транзакции в СМС. Пожалуйста, проверьте формат.")
                context.user_data.pop('sms_mode', None) # Выход из режима СМС
                message_text_sms_fail = "Выбери категорию:"
                if user_selected_source: # Используем уже полученный user_selected_source
                    message_text_sms_fail = f"Источник: {user_selected_source} (Валюта: {transaction_currency})\n{message_text_sms_fail}"
                await update.message.reply_text(
                    message_text_sms_fail,
                    reply_markup=generate_categories_keyboard(context)
                )
                return
        except Exception as e:
            await update.message.reply_text(f"Ошибка при разборе СМС: {e}")
            return

        fact_sheet = None
        num_existing_rows = 0
        try:
            fact_sheet = sheet.worksheet("fact")
            all_values_before_sms = fact_sheet.get_all_values() # Получаем все значения для определения последней строки
            num_existing_rows = len(all_values_before_sms)
        except (ConnectionError, gspread.exceptions.APIError) as e:
            print(f"Ошибка соединения/API при доступе к листу 'fact' (SMS): {e}. Попытка переподключения...")
            try:
                if GOOGLE_APPLICATION_CREDENTIALS_PATH and os.path.exists(GOOGLE_APPLICATION_CREDENTIALS_PATH):
                    client = gspread.service_account(filename=GOOGLE_APPLICATION_CREDENTIALS_PATH)
                elif GOOGLE_PRIVATE_KEY and GOOGLE_SERVICE_ACCOUNT_EMAIL:
                    print("Попытка пересоздать аутентификацию gspread для SMS...")
                    creds_info_retry_sms = {
                        "type": "service_account",
                        "private_key": GOOGLE_PRIVATE_KEY,
                        "client_email": GOOGLE_SERVICE_ACCOUNT_EMAIL,
                        # ... остальные поля creds_info
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{GOOGLE_SERVICE_ACCOUNT_EMAIL.replace('@', '%40')}"

                    }
                    creds_retry_sms = Credentials.from_service_account_info(creds_info_retry_sms, scopes=["https://www.googleapis.com/auth/spreadsheets"])
                    client = gspread.authorize(creds_retry_sms)
                else:
                    # Если нет учетных данных, используем google.auth.default()
                    import google.auth
                    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
                    creds_retry_sms, _ = google.auth.default(scopes=scopes)
                    client = gspread.authorize(creds_retry_sms)


                if client and SPREADSHEET_ID:
                    sheet = client.open_by_key(SPREADSHEET_ID) # Переоткрываем всю таблицу
                    fact_sheet = sheet.worksheet("fact") # Получаем лист заново
                    all_values_before_sms = fact_sheet.get_all_values()
                    num_existing_rows = len(all_values_before_sms)

                else:
                    raise Exception("Не удалось переинициализировать client или sheet для SMS.")
            except Exception as ex_retry:
                print(f"Не удалось переподключиться к Google Sheets (SMS): {ex_retry}")
                await update.message.reply_text('Ошибка при записи данных из СМС. Не удалось подключиться к таблице.')
                return
        if not fact_sheet: # Если fact_sheet все еще None после попыток
             await update.message.reply_text('Критическая ошибка: лист "fact" недоступен для записи СМС.')
             return


        rows_to_append_sms = []
        for i, rec in enumerate(records):
            if not rec['дата']:
                print(f"Пропущена запись из СМС из-за отсутствия даты: {rec}")
                continue

            next_row_num_for_formula_sms = num_existing_rows + 1 + i
            balance_formula_sms = (
                f'=СУММЕСЛИМН($D$2:D{next_row_num_for_formula_sms};'
                f' $H$2:H{next_row_num_for_formula_sms}; $H{next_row_num_for_formula_sms};'
                f' $G$2:G{next_row_num_for_formula_sms}; $G{next_row_num_for_formula_sms};'
                f' $B$2:B{next_row_num_for_formula_sms}; "💰 ДОХОДЫ")'
                f' - '
                f'СУММЕСЛИМН($D$2:D{next_row_num_for_formula_sms};'
                f' $H$2:H{next_row_num_for_formula_sms}; $H{next_row_num_for_formula_sms};'
                f' $G$2:G{next_row_num_for_formula_sms}; $G{next_row_num_for_formula_sms};'
                f' $B$2:B{next_row_num_for_formula_sms}; "<>💰 ДОХОДЫ")'
            )

            date_str = rec['дата'].strftime('%d.%m.%Y')
            amount = abs(rec['сумма']) if rec['сумма'] is not None else 0.0
            op = rec['операция'].upper() if rec['операция'] else 'НЕИЗВЕСТНО'
            category_sms = op if op != 'НЕИЗВЕСТНО' else '' # Для СМС категория = тип операции

            row_sms = [
                date_str,
                category_sms,
                "", # Подкатегория для СМС не указывается
                amount,
                balance_formula_sms,
                f"SMS: {rec.get('валюта_из_смс', '')} {text[:30]}...", # Комментарий - начало текста СМС
                transaction_currency, # Валюта из источника
                user_selected_source
            ]
            rows_to_append_sms.append(row_sms)

        if rows_to_append_sms:
            try:
                fact_sheet.append_rows(rows_to_append_sms, value_input_option='USER_ENTERED')
                response_message_text = f"Записаны {len(rows_to_append_sms)} транзакций из СМС (Источник: {user_selected_source}, Валюта: {transaction_currency})."

                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=original_message_id)
                    print(f"Сообщение {original_message_id} с СМС удалено.")
                except Exception as e_delete:
                    print(f"Не удалось удалить сообщение с СМС {original_message_id}: {e_delete}")

                await update.message.reply_text(
                    response_message_text,
                    reply_markup=generate_categories_keyboard(context) # Возврат к главному меню
                )
            except Exception as e_append:
                print(f"Ошибка при пакетной записи СМС в Google Sheets: {e_append}")
                await update.message.reply_text('Произошла ошибка при записи данных из СМС в таблицу.')
        else:
            await update.message.reply_text(
                "Не найдено корректных транзакций для записи из СМС.",
                reply_markup=generate_categories_keyboard(context) # Возврат к главному меню
            )
        context.user_data.pop('sms_mode', None) # Выход из режима СМС в любом случае
        return

    # Обработка обычного текстового ввода (не СМС)
    category = context.user_data.get('category')
    subcategory = context.user_data.get('subcategory')

    error_parts = []
    if not user_selected_source: # Эта проверка уже была выше, но дублирование не повредит
        error_parts.append("- Источник")
    if not category:
        error_parts.append("- Категорию")
    # Для ручного ввода подкатегория обязательна, если есть категория
    if category and not subcategory:
        error_parts.append("- Подкатегорию")


    if error_parts:
        error_message = "Пожалуйста, сначала выберите:\n" + "\n".join(error_parts)
        if user_selected_source: # Добавляем информацию о текущем источнике, если он есть
            error_message += f'\nТекущий источник: {user_selected_source} (Валюта: {transaction_currency})'
        else:
            error_message += f'\nИсточник не выбран. Валюта не определена.'
        await update.message.reply_text(
            error_message,
            reply_markup=generate_categories_keyboard(context) # Возвращаем к выбору категорий
        )
        return

    message_text = update.message.text
    try:
        amount_str, *comment_parts = message_text.split(' ', 1)
        amount = float(amount_str.replace(',', '.'))
        comment = comment_parts[0] if comment_parts else ""
    except ValueError:
        await update.message.reply_text(
            f"Неверный формат суммы. Пожалуйста, введите сумму (число) и комментарий через пробел.\n"
            f"Источник: {user_selected_source} (Валюта: {transaction_currency})\nКатегория: {category}\nПодкатегория: {subcategory}",
            reply_markup=generate_subcategories_keyboard(category) # Возвращаем к той же подкатегории
        )
        return

    fact_sheet_manual = None
    last_row_idx = 0
    try:
        fact_sheet_manual = sheet.worksheet("fact")
        all_values = fact_sheet_manual.get_all_values()
        last_row_idx = len(all_values) # Определяем последнюю строку для формулы
    except (ConnectionError, gspread.exceptions.APIError) as e:
        print(f"Ошибка соединения/API при доступе к листу 'fact' (ручной ввод): {e}. Попытка переподключения...")
        try:
            # Логика переподключения аналогична той, что в блоке SMS
            if GOOGLE_APPLICATION_CREDENTIALS_PATH and os.path.exists(GOOGLE_APPLICATION_CREDENTIALS_PATH):
                client = gspread.service_account(filename=GOOGLE_APPLICATION_CREDENTIALS_PATH)
            elif GOOGLE_PRIVATE_KEY and GOOGLE_SERVICE_ACCOUNT_EMAIL:
                print("Попытка пересоздать аутентификацию gspread для ручного ввода...")
                creds_info_retry_manual = {
                     "type": "service_account",
                    "private_key": GOOGLE_PRIVATE_KEY,
                    "client_email": GOOGLE_SERVICE_ACCOUNT_EMAIL,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{GOOGLE_SERVICE_ACCOUNT_EMAIL.replace('@', '%40')}"
                }
                creds_retry_manual = Credentials.from_service_account_info(creds_info_retry_manual, scopes=["https://www.googleapis.com/auth/spreadsheets"])

                client = gspread.authorize(creds_retry_manual)
            else:
                import google.auth
                scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
                creds_retry_manual, _ = google.auth.default(scopes=scopes)
                client = gspread.authorize(creds_retry_manual)


            if client and SPREADSHEET_ID:
                sheet = client.open_by_key(SPREADSHEET_ID)
                fact_sheet_manual = sheet.worksheet("fact")
                all_values = fact_sheet_manual.get_all_values() # Пересчитываем last_row_idx
                last_row_idx = len(all_values)
            else:
                raise Exception("Не удалось переинициализировать client или sheet для ручного ввода.")

        except Exception as ex_retry:
            print(f"Не удалось переподключиться к Google Sheets (ручной ввод): {ex_retry}")
            await update.message.reply_text('Ошибка при записи данных. Не удалось подключиться к таблице.')
            return
    except Exception as e_other: # Ловим другие возможные ошибки при работе с fact_sheet
        print(f"Другая ошибка при работе с fact_sheet (ручной ввод): {e_other}")
        await update.message.reply_text('Произошла ошибка при подготовке данных для записи.')
        return

    if not fact_sheet_manual: # Если fact_sheet_manual все еще None
        await update.message.reply_text('Критическая ошибка: лист "fact" недоступен для ручного ввода.')
        return

    next_row_num_for_formula = last_row_idx + 1 # Строка для формулы - следующая за последней существующей
    balance_formula = (
        f'=СУММЕСЛИМН($D$2:D{next_row_num_for_formula};'
        f' $H$2:H{next_row_num_for_formula}; $H{next_row_num_for_formula};'
        f' $G$2:G{next_row_num_for_formula}; $G{next_row_num_for_formula};'
        f' $B$2:B{next_row_num_for_formula}; "💰 ДОХОДЫ")'
        f' - '
        f'СУММЕСЛИМН($D$2:D{next_row_num_for_formula};'
        f' $H$2:H{next_row_num_for_formula}; $H{next_row_num_for_formula};'
        f' $G$2:G{next_row_num_for_formula}; $G{next_row_num_for_formula};'
        f' $B$2:B{next_row_num_for_formula}; "<>💰 ДОХОДЫ")'
    )


    row_to_append = [
        update.message.date.strftime('%d.%m.%Y'),
        category.upper(), # Категория из user_data
        subcategory,      # Подкатегория из user_data
        amount,
        balance_formula,  # Формула баланса
        comment,
        transaction_currency, # Валюта из источника
        user_selected_source  # Источник из user_data
    ]

    try:
        fact_sheet_manual.append_row(row_to_append, value_input_option='USER_ENTERED')
        success_message_text = (f'Данные успешно записаны.\n'
                                f'Источник: {user_selected_source}\n'
                                f'Категория: {category}\n'
                                f'Подкатегория: {subcategory}\n'
                                f'Сумма: {amount} {transaction_currency}\n'
                                f'Комментарий: {comment}')
        await update.message.reply_text(
            success_message_text,
            reply_markup=generate_categories_keyboard(context) # Возврат к главному меню
        )
    except Exception as e_append:
        print(f"Ошибка при добавлении строки в Google Sheets (ручной ввод): {e_append}")
        await update.message.reply_text('Произошла ошибка при записи данных в таблицу.')


def main():
    if not TOKEN: # Проверяем TOKEN (который теперь TELEGRAM_TOKEN из .env или окружения)
        print("Ошибка: TELEGRAM_TOKEN не установлен. Проверьте .env или переменные окружения. Завершение работы.")
        return
    if not sheet: # Проверка sheet остается важной
        print(
            "Критическая ошибка: Не удалось инициализировать подключение к Google Sheets (таблица не открыта) при запуске. Бот не может функционировать. Проверьте SPREADSHEET_ID и учетные данные Google.")
        # Можно добавить return здесь, если без sheet бот не должен даже пытаться запуститься
        # return

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reboot", reboot))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    if LOCAL_RUN:
        print("Запуск бота в режиме polling...")
        app.run_polling()
    else: # Production mode
        port = int(os.environ.get("PORT", "8443")) # PORT также должен быть в переменных окружения для продакшена
        webhook_url = os.getenv('WEBHOOK_URL') # WEBHOOK_URL из переменных окружения
        if webhook_url and TOKEN : # Убедимся что и TOKEN есть для url_path
            print(f"Запуск бота в режиме webhook на URL: {webhook_url}, порт: {port}")
            app.run_webhook(
                listen="0.0.0.0",
                port=port,
                url_path=TOKEN, # Используем TOKEN как часть пути вебхука
                webhook_url=f"{webhook_url}/{TOKEN}"
            )
            print(f"Webhook запущен. URL для Telegram: {webhook_url}/{TOKEN}")
        else:
            print("WEBHOOK_URL не установлен или TOKEN отсутствует. Запуск в режиме polling как fallback (если это прод, то это ошибка конфигурации).")
            app.run_polling()


if __name__ == '__main__':
    main()