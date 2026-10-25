# Чистый чат — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** клавиатуры групп и карт по умолчанию скрыты и включаются в /keyboards (настройка в таблице), кнопки записи — на карточке, вошедшее SMS удаляется после записи, утром чат убирается.

**Architecture:** настройки и состояние уборки — `app/chat_state.py` (`ChatState`, разбор строк блока system!Q:U, `state_of`/`store` над `bot_data`), чтение/запись блока — `GoogleSheetsService.get_chat_states`/`set_chat_state`; меню — `app/utils/keyboards.py` (`record_actions`, `categories_menu(groups=…)`), `app/handlers/common.py` (`show_main_menu`, `start`, `menu_prompt`), `last_write.py` (кнопки `card:` на карточке), `fix.py`, `transactions.py`, `messages.py`; команды — `app/handlers/settings.py` (/keyboards, /app); уборка — `app/handlers/cleanup.py` (`observe`, `morning`, `first_run`, `keep_digest`); сводки — `analytics.send_morning_brief`/`send_weekly_digest` возвращают `Sent`.

**Tech Stack:** Python 3.12+, python-telegram-bot 22.8 (Bot API: `deleteMessages`, `ReplyKeyboardRemove`, `web_app` у inline-кнопки), gspread; pytest.

**Spec:** `docs/superpowers/specs/2026-10-25-clean-chat-design.md`

## Global Constraints

- Блок system!Q:U: заголовки «чат бота: ID», «группы», «карты», «уборка с №», «не удалять №»; значения пишутся RAW текстом: ID, «да»/«нет», номер, номера через пробел. Нет строки — обе «нет».
- Ключ настроек — ID чата (личный чат = ID пользователя).
- Тексты: «⌨️ Клавиатуры», «✅ Группы под записью: включены» / «⬜ Группы под записью: выключены», «✅ Карты внизу экрана: включены» / «⬜ Карты внизу экрана: выключены», кнопки «Включить группы» / «Выключить группы», «Включить карты» / «Выключить карты», «⚠️ Не сохранил, попробуйте ещё раз», «⌨️ Карты скрыты», «📱 Открыть приложение», «⌨️ Клавиатуры скрыты» + «Чтобы чат был чище, кнопки групп и карт больше не показываются.» + «Включить — в меню: /keyboards. Приложение — /app.», /start без клавиатур: «👋 Имя» + «Пришлите SMS банка, скриншот или выписку — запишу.» + «Клавиатуры — /keyboards, приложение — /app.», на карточке после «Отменить»: «🗑 Да, удалить запись» / «Нет», всплывающее «Последней записи уже нет».
- Команды меню: («app», «Приложение 📱»), («keyboards», «Клавиатуры: группы и карты ⌨️»).
- Уборка: пачки по 100 (`delete_messages`), запас 200 номеров, не трогать «не удалять №», сообщение с клавиатурой карт при включённых картах; в 8:00 и при MORNING_TIME=off.
- Только выдуманные ID, номера и суммы в тестах и описании PR.

## Review Focus

1. Перезапуск бота между переключением и записью: настройки берутся из блока таблицы — тест в задаче 1 (`build_application` с прочитанными состояниями) и задаче 2 (меню по ним).
2. Кнопки на карточке, отправленной до перезапуска (`card:undo` без последней записи): всплывающее сообщение и снятые кнопки, текст карточки цел — тест в задаче 2.
3. Два пользователя: настройки жены не меняют чат владельца — тест в задаче 2.
4. Telegram отклонил пачку удаления (старое сообщение): по одному, уборка идёт дальше, «уборка с №» двигается — тест в задаче 5.
5. Таблица не записала настройку: она не меняется, в сообщении «⚠️ Не сохранил…» — тест в задаче 3.

---

### Task 1: Настройки чата в таблице

**Files:** Create `app/chat_state.py`; Modify `app/services/google_sheets.py`, `app/main.py` (`main`, `build_application(chat_states=…)`), `app/handlers/admin.py` (`reboot` перечитывает блок); Test `tests/test_chat_state.py`, `tests/test_main.py`.

**Interfaces — Produces:**
- `ChatState(groups: bool = False, cards: bool = False, start: int | None = None, keep: tuple[int, ...] = ())` (frozen dataclass)
- `CHAT_HEADER: list[str]`, `parse_chat_rows(rows: list[list[Any]]) -> dict[int, ChatState]`, `chat_row(chat_id: int, state: ChatState) -> list[str]`
- `state_of(bot_data: dict, chat_id: int) -> ChatState`; `async store(bot_data: dict, chat_id: int, state: ChatState) -> bool` (запись через `gs_service.set_chat_state` в потоке; успех — `bot_data["chats"][chat_id] = state`; ошибка — лог, False)
- `GoogleSheetsService.get_chat_states() -> dict[int, ChatState]`, `set_chat_state(chat_id: int, state: ChatState) -> None` (строка с этим ID или первая пустая ниже заголовка; заголовок и строка одним `values_batch_update` RAW)
- `bot_data["chats"]: dict[int, ChatState]`, `bot_data["chats_loaded"]: bool`

- [ ] **Step 1: failing tests** — `test_rows_are_read_by_id` (ID текстом и числом, «да»/«Да»/«нет»/пусто, номера через пробел и запятую, мусорный ID пропущен); `test_row_is_written_as_text` (`chat_row(7, ChatState(True, False, 120, (130, 131)))` == `["7", "да", "нет", "120", "130 131"]`); `test_block_lives_in_q_to_u_of_system` (фейковый gspread: чтение, запись в существующую строку и в первую пустую, заголовок); `test_state_of_defaults_to_off`; `test_store_keeps_the_old_state_when_the_sheet_fails`; `test_chat_states_reach_the_handlers` (`build_application(..., chat_states={42: ChatState(groups=True)})` → `bot_data["chats"]`, `chats_loaded`), без них — `{}` и `chats_loaded` False.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: keyboard settings and the cleanup start live in the system tab`.

### Task 2: Меню по настройкам

**Files:** Modify `app/utils/keyboards.py`, `app/handlers/common.py`, `app/handlers/messages.py` (`save_rows`, выбор карты текстом, «⬅️ Назад»), `app/handlers/last_write.py`, `app/handlers/fix.py`, `app/handlers/transactions.py`, `app/main.py` (шаблон `^(last|card):`); Test `tests/test_clean_chat.py`, `tests/test_messages.py` (`make_chat` — обе включены: старые сценарии), `tests/test_keyboards.py`.

**Interfaces — Consumes:** `state_of` (задача 1). **Produces:**
- `CARD_FIX, CARD_UNDO, CARD_CHECK, CARD_UNDO_YES, CARD_UNDO_NO = "card:fix", "card:undo", "card:check", "card:undo:yes", "card:undo:no"`
- `record_actions(check: int = 0, on_card: bool = False) -> list[list[InlineKeyboardButton]]`; `categories_menu(categories, with_actions=False, custom=None, check=0, groups=True)`
- `menu_prompt(context, chat_id: int) -> str` — «💳 карта — выберите категорию» при группах, иначе «💳 карта»
- `ANCHOR = "keyboard_anchor"` (user_data: номер сообщения с клавиатурой карт)
- `LAST_WRITE["on_card"]: bool`

- [ ] **Step 1: failing tests** (обе выключены, если не сказано иное): после записи одна карточка с `record_actions()` на `card:`-кнопках, без клавиатуры карт и без второго сообщения; ≥2 строк с пометкой — «🔎 Проверить (N)» на карточке; одна строка с пометкой — вопрос отдельным сообщением, на карточке действия без 🔎, после «✓ Верно» в вопросе — итог без кнопок; только карты — карточка с клавиатурой карт, второе «💳 VISA 9090 UZS» с действиями `last:`, `ANCHOR` — номер карточки; только группы — карточка без клавиатуры, второе «💳 … — выберите категорию» с действиями и группами; `card:undo` → кнопки «🗑 Да, удалить запись»/«Нет» (только разметка), `card:undo:no` → действия, `card:undo:yes` → строки удалены, карточка удалена, «↩️ Удалил из таблицы: строка 4169», неудача — действия и причина; `card:undo` без последней записи → `answer("Последней записи уже нет", show_alert=True)`, разметка снята, текст не тронут; `card:fix`/`card:check` — как `last:`; /start без клавиатур — «👋 …» с подсказкой, без клавиатуры и групп, с картами — клавиатура (`ANCHOR`), с группами — меню групп; выбор карты текстом при выключенных группах — без меню групп; настройки жены (ID 2) не меняют чат 1; «Нет» после `last:undo` при выключенных группах — «💳 VISA 9090 UZS» и только действия.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS (старые сценарии — с включёнными в `make_chat`).
- [ ] **Step 5: commit** — `feat: the record buttons sit on the card, groups and cards only when switched on`.

### Task 3: /keyboards и /app

**Files:** Create `app/handlers/settings.py`; Modify `app/utils/keyboards.py` (`keyboards_markup`, `app_markup`), `app/main.py` (COMMANDS, обработчики `keyboards`, `app`, `^kb:`); Test `tests/test_settings.py`, `tests/test_main.py`.

**Interfaces — Consumes:** `state_of`, `store` (задача 1), `ANCHOR`, `menu_prompt`, `sources_keyboard` (задача 2). **Produces:** `keyboards_command`, `keyboards_button` (`kb:groups:on|off`, `kb:cards:on|off`), `app_command`; `keyboards_report(state: ChatState) -> Report`; `app_markup(url: str) -> InlineKeyboardMarkup`.

- [ ] **Step 1: failing tests** — /keyboards: текст и кнопки по состоянию (обе выкл — «⬜ … выключены», «Включить группы», «Включить карты»); «Включить группы» → `set_chat_state(1, ChatState(groups=True))`, сообщение обновлено, пришло меню групп; «Включить карты» → клавиатура карт «💳 VISA 9090 UZS», `ANCHOR`; «Выключить карты» → «⌨️ Карты скрыты» с `ReplyKeyboardRemove`, номер — в `keep` (записан); «Выключить группы» — новых сообщений нет; таблица упала → состояние прежнее, в сообщении «⚠️ Не сохранил, попробуйте ещё раз»; /app — «📱 Приложение» с inline-кнопкой `web_app` на `WEBAPP_URL`; без `WEBAPP_URL` — «Приложение не настроено»; команды меню содержат `app` и `keyboards`.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: /keyboards switches the groups and the cards, /app opens the app`.

### Task 4: Вошедшее сообщение удаляется после записи

**Files:** Modify `app/handlers/messages.py` (`_save_sms`, разбор Gemini, экран остатков), `app/handlers/live.py` (описание модуля); Test `tests/test_live_status.py`, `tests/test_clean_chat.py`.

- [ ] **Step 1: failing tests** — SMS по правилам записано целиком → `message.delete` вызван, реакции 👍 нет; только коды/повторы → удалено; часть строк не записана (`skipped`) → не удалено, 👍; ничего → 🤔, не удалено; SMS через Gemini целиком → 👀 и удалено; скрин операций целиком → удалён; экран остатков → удалён; таблица не записала → 🤔, не удалено.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: a fully written SMS or screenshot leaves the chat`.

### Task 5: Утренняя уборка и первый запуск

**Files:** Create `app/handlers/cleanup.py`; Modify `app/handlers/analytics.py` (`send_morning_brief(..., reply_markup=None) -> Sent | None`, `send_weekly_digest(...) -> Sent | None`), `app/main.py` (`observe` в группе −3, `_morning_brief` → сводка и уборка, при MORNING_TIME=off — задача `chat_cleanup` в 08:00, `_weekly_digest` → `keep_digest`, `first_run` в `_post_init`); Test `tests/test_cleanup.py`, `tests/test_main.py`.

**Interfaces — Consumes:** `ChatState`, `state_of`, `store` (1), `ANCHOR`, `sources_keyboard` (2), `app_markup` (3). **Produces:** `TOP = "chat_top"` (bot_data: chat_id → самый большой известный номер), `observe(update, context)`, `async clean(application, chat_id, upper: int | None) -> None`, `async morning(context)`, `async first_run(context)`, `async keep_digest(application, chat_id, ids)`, `OVERREACH = 200`, `CHUNK = 100`.

- [ ] **Step 1: failing tests** — владелец: сводка с кнопкой «📱 Открыть приложение» (карты выкл) / с клавиатурой карт (вкл), затем `delete_messages` по номерам `[start, сводка)` без «не удалять», пачками по 100, `start` = номер сводки, `keep` без номеров меньше него — записано в таблицу; жена: `[start, top + 200]`, `start` = top + 1; номер неизвестен → чат не трогаем; карты вкл и `ANCHOR` известен → его номер не удаляется; карты вкл, `ANCHOR` неизвестен → после уборки без звука «💳 …» с клавиатурой; пачку отклонили → по одному, ошибки в лог, следующий чат убирается; нет `start` → только запоминаем `top + 1`; сводка недели → её номер в `keep` (записан); `observe` запоминает номер сообщения и нажатия (только разрешённых); первый запуск: блок прочитан, строки нет → «⌨️ Клавиатуры скрыты» с `ReplyKeyboardRemove`, строка `ChatState(start=номер + 1)`; строка есть — ничего; блок не прочитан — ничего; задачи: `morning_brief` и при MORNING_TIME=off — `chat_cleanup` в 8:00.
- [ ] **Step 2–4:** FAIL → implement → `pytest -q` PASS.
- [ ] **Step 5: commit** — `feat: the morning brief tidies the chats, a one-time note hides the old keyboard`.

### Task 6: Документация и проверка

- [ ] README и CLAUDE.md — клавиатуры по выбору, /keyboards, /app, уборка; `black`, `ruff`, `pytest -q` — чисто; commit `docs: the clean chat`.
- [ ] После деплоя: уведомление пришло обоим, клавиатура карт исчезла, строка в system!Q:U; /keyboards включает и выключает; утренняя уборка — по логам.
