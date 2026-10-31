# «Зачислить», «Списать» и перевод между своими картами — план

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** записать пополнение (минус в D) из приложения и из чата, а перевод или обмен между своими картами — одной записью «Ввода», которая пишет в fact две строки.

**Architecture:**
- **Часть 1.** Знак едет в `POST /api/expenses` отдельным флагом `incoming`, сумма по-прежнему больше нуля. Сервер передаёт флаг в `entry_row`.
  - На экране суммы — «Зачислить» и «Списать». В Telegram 7.10+ это `SecondaryButton` и `MainButton`, иначе — кнопки в экране.
  - Чат: «-» и «−» перед суммой работают как «+».
- **Часть 2.** Тот же `POST /api/expenses` с `to_source` и `to_amount`.
  - Чистая `transfer_rows` (новый `app/transfers.py`) строит снятие и пополнение.
  - Один `save_rows` пишет обе строки: для отмены, повтора и `/fix` это одна запись.
  - В приложении после «💳 СЧЕТА» → «переводы» / «обмен валюты» открывается `TransferScreen`:
    - окно карт — `CardSheet`;
    - курс и строки «в таблицу» — `entry/transfer.ts`.
  - Отправка, очередь и панель результата — общие с экраном суммы (`EntryScreen.send`).

**Tech Stack:** Python 3.12 (venv 3.14), Starlette, Pydantic 2, pytest; React 19 + TypeScript + Tailwind 4, vitest + Testing Library; Telegram Mini Apps (Bot API 7.10 — `SecondaryButton`).

**Spec:** `docs/superpowers/specs/2026-10-31-transfers-design.md` — владелец одобрил 31.10.
- Раздел 2 — часть 1, разделы 3–5 — часть 2.
- Раздел 6 — что не меняется, 7 — проверки, 8 — этапы.

**Ветки, PR, база:**
- **Часть 1** — ветка `feat/transfers` (main 673436d + коммит спеки 49dc464). Перед Task 1:
  - скопировать этот план в `docs/superpowers/plans/2026-10-31-transfers.md`;
  - commit `docs: the plan — «Зачислить» and a transfer in one entry`.
  - Затем PR, ревью, мерж и выкладка на Render — всё до части 2.
- **Часть 2** — ветка `feat/transfers-pair` от свежего main после мержа части 1. В `webapp/` — своя `npm ci`.
- **База на 49dc464:** pytest — 1511 passed, vitest — 504 passed.

**Карта файлов:**

| Файл | Ответственность | Задачи |
|---|---|---|
| `app/domain.py` | `_MANUAL`: знаки «+», «-», «−» | 1 |
| `app/web/schemas.py`, `app/web/api.py` | `ExpenseIn.incoming`, затем `to_source`/`to_amount`; `check_entry`; `add_expense` | 2, 7 |
| `app/transfers.py` (новый) | `transfer_rows` — две строки пары | 6 |
| `webapp/src/entry/outgoing.ts` (новый) | знак записи, `Outgoing`, заголовки панели | 3, 10 |
| `webapp/src/telegram.ts` | `SecondaryButton`, `setParams`, `secondaryButton()` | 4 |
| `webapp/src/test/telegram.ts` (новый) | заглушка Telegram для тестов | 4 |
| `webapp/src/entry/transfer.ts` (новый) | курс, строки «в таблицу», остаток карты | 8 |
| `webapp/src/entry/CardSheet.tsx` (новый) | окно карт | 9 |
| `webapp/src/entry/EntryFields.tsx` (новый) | сумма крупно, «Комментарий», «Дата» — общие с экраном суммы | 10 |
| `webapp/src/entry/TransferScreen.tsx` (новый) | экран «Откуда → Куда» | 10 |
| `webapp/src/entry/EntryScreen.tsx` | кнопки знака, конвейер `send`, шаг `transfer` | 3, 4, 10, 11 |

## Global Constraints

### Правила (спека, дословно)

**Знак (раздел 2):**
- «Экран суммы «Ввода», любая группа, кроме «💰 ДОХОДЫ»: вместо «Записать …» внизу две кнопки в одном ряду, поровну по ширине: слева «Зачислить» — зелёная (`chip-good`), белый сверхжирный текст; справа «Списать» — цветом кнопки Telegram, белый текст.»
- «Списать» пишет D = +сумма — как сегодня «Записать». «Зачислить» пишет D = −сумма: пополнение, возврат, выравнивание.
- «💰 ДОХОДЫ» — одна кнопка «Записать», D с плюсом, как сейчас.
- «Кнопки недоступны, пока сумма 0, и обе блокируются на время отправки.»
- «В Telegram — родные кнопки панели: «Списать» — `MainButton`, «Зачислить» — `SecondaryButton` с `position: "left"` и своим цветом (`setParams`), если `isVersionAtLeast("7.10")`. Иначе (старый Telegram, браузер, тесты) — те же две кнопки в экране, в том же порядке и цветах.»
- «Панель результата: «✓ Списано 48 000 UZS» или «✓ Зачислено 48 000 UZS — в таблицу −48 000». «Отменить» — как сейчас.»
- «Очередь «Не отправлено» подписывает такую запись «зачисление». Запись в очереди без признака (сохранённая до обновления) — «списание», как и было.»
- Чат: «-48000 …» и «−48000 …» записываются как «+48000 …»: приход, D −48 000 для не-доходов (у дохода плюс).

**Экран перевода (раздел 3).** Открывается вместо экрана суммы после «💳 СЧЕТА» → «переводы» или «обмен валюты». Сверху вниз:
1. заголовок «💳 СЧЕТА · 🔄 переводы»;
2. «Откуда»: подпись, карта крупно, «на карте X» (не загружен — строки нет), справа «›»;
3. «Куда» — так же;
4. суммы;
5. строка курса — только при разных валютах;
6. «Комментарий» и «Дата»;
7. `Keypad big`;
8. таблица «карта | в таблицу» — числа со знаком D и целиком;
9. кнопка.

Правила экрана:
- «Откуда» по умолчанию — текущая карта «Ввода», «Куда» не выбрана.
- В «Откуда» последняя строка — «не моя карта». В «Куда» нет карты из «Откуда», и наоборот; «не моя карта» в «Куда» нет.
- Суммы:
  - валюта карт одна — одно поле «Сумма»;
  - валюты разные — «Списано» в валюте «Откуда» и «Зачислено» в валюте «Куда»;
  - «не моя карта» — одно поле «Сумма» в валюте «Куда».
- Активное поле — толстой рамкой цвета кнопки и словом «— вводите». Тап по полю делает его активным. Смена карты стирает сумму своего поля.
- Курс считается из двух сумм: «курс: 1 $ = 12 108 сум». Единица — та валюта, что дороже. Число целиком, меньше 100 — с двумя знаками («1 $ = 87,50 ₽»).
- Кнопка:
  - «Записать перевод» / «Записать обмен» — по подкатегории;
  - «не моя карта» — «Зачислить»;
  - недоступна, пока не выбраны обе стороны или сумма равна 0.
- «Назад» ведёт к подкатегориям.

**Запись перевода (раздел 4).**
- Один запрос, одна запись `save_rows`, одна дата: снятие (D = +«Списано»), потом пополнение (D = −«Зачислено»).
- Группа «💳 СЧЕТА», подкатегория — выбранная плитка.
- Комментарии: «перевод VISA 1111 → HUMO 2222 (свой)» и «перевод HUMO 2222 ← VISA 1111 (свой)».
  - У «обмен валюты» — «конвертация …».
  - Без «UZS».
  - Свой комментарий — через «, ».
- «Отменить» и `/undo` снимают обе строки. Повтор `entry_id` — «Уже записано: строки N–M».
- Текущей картой становится «Куда».

**Проверки сервера (раздел 5).**
- `to_source` — только у «💳 СЧЕТА», карты из справочника и разные.
- Суммы больше нуля. В одной валюте вторая сумма не нужна, а если пришла — должна совпадать. В разных валютах обязательна.
- Отказ — 422 с полем.

**Анти-цели (раздел 1):**
- полупара;
- дубли при выписке;
- знак, который не видно глазами;
- кнопка, которую легко нажать вместо соседней.

### Что не меняется

- Ответы бота в чате и `format_amount`. «+» в ответе значит «деньги пришли»: после «Зачислить» чат пишет «✅ +48 000 UZS». Знак таблицы виден только в приложении.
- SMS-правила, импорт выписок, аналитика, «Сводка», «План», «Календарь», формулы вкладок.
- `ExpenseIn.amount` по-прежнему больше нуля: `"-5"` остаётся 422 `amount`.
- `bootstrapFixture` общий для девяти тестовых файлов, его не трогаем: новым тестам — свой справочник.
- Новых методов `Api` нет.

### Тексты и вид — для плохого зрения

- Тексты — дословно из задач.
- Текст белый или яркий и жирный.
- Без красного текста: предупреждения — `text-danger` (оранжевый).
- Плашки и кнопки — с белым текстом.
- Числа целиком, без «млн»: `money`, `moneyText`, `sheetAmount`.
- 320 px при textZoom ×1,25 и 360 px в тёмной теме — без горизонтальной прокрутки.

### Репозиторий и код

- Репозиторий публичный: в плане, тестах, фикстурах и PR — только выдуманные карты и суммы.
  - Карты сервера — VISA 9090 UZS, UZCARD 5656 UZS, VISA 3434 USD.
  - Карты приложения — из `bootstrapFixture` (VISA 9090 UZS, VISA 3434 USD) и HUMO 2222 UZS; в тестах экрана перевода и окна карт — VISA 1111 UZS, HUMO 2222 UZS, VISA 3333 USD.
  - Карта выписок — HUMO 7007 UZS.
  - Моки — VISA 1111, VISA 2222, VISA 6666, HUMO 4444.
  - Номера и остатки из макетов спеки в код не переносить.
  - До push: `git diff main... | grep -nE '[0-9]{4}'` — других четырёхзначных номеров карт нет.
- Python: type hints, black и ruff, строки ≤ 88. TS: строгие типы, prettier, eslint (react-hooks).
- В worktree — своя `npm ci` в `webapp/`, без симлинка `node_modules`.
- Команды из корня worktree:
  - `../venv_dev/bin/python -m pytest -q -p no:cacheprovider`;
  - `../venv_dev/bin/ruff check .`;
  - `../venv_dev/bin/black --check .`;
  - `cd webapp && npm test && npm run lint && npm run build`.

## Решения

### Решения оркестратора

1. **Знак на проводе — отдельный флаг `incoming: bool = False`** в `ExpenseIn` и `ExpenseInput`, а не минус в `amount`.
   - `amount` остаётся больше нуля.
   - Старые записи очереди без поля — «списание».
2. **Пара — расширение `POST /api/expenses`.**
   - `to_source: str | None` и `to_amount: Decimal | None` идут с тем же `entry_id` и через ту же очередь.
   - Один вызов `save_rows([снятие, пополнение])`.
   - «Откуда» = «не моя карта» — обычная запись `incoming=True` на карту «Куда», без `to_source`.
3. **Порядок строк — снятие, потом пополнение.** Текущей картой становится «Куда».
4. **Подкатегория — выбранная плитка.**
   - Комментарий — «перевод …» для «переводы», «конвертация …» для «обмен валюты».
   - Свой комментарий владельца — через «, ».
5. **Ответы бота в чате и `format_amount` не меняются.**

### Решения, где спека молчит

1. **Ruling:** «Зачислить» и «Списать» — одним словом, без суммы (макет «Знак · А»). «Записать» у доходов остаётся с суммой, как сейчас.
   - Почему: в половину ряда на 320 px при ×1,25 входит только слово. Сумма крупно стоит над клавиатурой.
   - Цена: на кнопке пары сумма не повторяется.
2. **Ruling:** в старом Telegram (< 7.10) пара кнопок — в экране, а `MainButton` на шаге суммы не показывается. У доходов `MainButton` работает на любой версии, как сейчас.
   - Почему: спека говорит «иначе — те же две кнопки в экране». Одна кнопка в панели и другая в экране — два места для одного выбора.
3. **Ruling:** кнопки панели вызываются в порядке «сначала прогресс, потом активность»:
   - кнопка ждёт ответа — `showProgress(false)`;
   - иначе — `hideProgress()`, затем `enable()` или `disable()`.

   Почему: в telegram-web-app.js `hideProgress()` снова включает кнопку. Нынешняя «Записать …» в Telegram из-за этого активна при сумме 0 — заодно чиним.
4. **Ruling:** прогресс крутится только на нажатой кнопке, соседняя просто недоступна. Состояние — `sending: "spend" | "credit" | null`.
   - Почему: прогресс на «Списать» после нажатия «Зачислить» читается как «не тот знак» — это анти-цель спеки.
5. **Ruling:** панель результата.
   - Первая строка — «✓ Списано …» или «✓ Зачислено … — в таблицу −…», 21 px, сверхжирно.
   - Ниже — строки сервера, как сейчас.
   - У доходов первой строки нет.
   - Повтор — первая строка и «✅ Уже записано: …».
   - Почему: строки сервера называют группу, карту и остаток дня, а спека меняет только первую строку.
   - Цена: сумма видна дважды.
6. **Ruling:** числа в первой строке панели и в «в таблицу» выводит `sheetAmount`.
   - Сумы — целиком. Другие валюты — с копейками только у дробной суммы: «✓ Зачислено 100 USD — в таблицу −100», в макетах — «−500».
   - Цена: строка очереди остаётся «кофе 100,00 USD» (`withCurrency`).
7. **Ruling:** очередь подписывает запись словом «зачисление: » по `entry.incoming` в момент показа. Трату не подписывает.
   - Почему: подпись не разойдётся с тем, что уйдёт на сервер, а старые записи выглядят как были.
8. **Ruling:** приложение шлёт `incoming: true` только у «Зачислить». У «Списать» и «Записать» поля нет.
   - Почему: запрос траты остаётся байт в байт прежним. Старые записи очереди и новые траты — одного вида.
9. **Ruling:** `incoming` — это `StrictBool`: `"yes"` и `1` дают 422.
   - Почему: знак нельзя брать из строки, прочитанной «примерно».
10. **Ruling:** противоречивые поля пары — 422, а не молчаливое исправление:
    - `to_amount` без `to_source` — поле `to_amount`;
    - `incoming` вместе с `to_source` — поле `incoming`.

    Почему: спека требует «если пришла — должна совпадать». Иначе клиент с ошибкой записал бы не то, что показал.

    Цена: две строки проверки.
11. **Ruling:** строки пары строит чистая `transfer_rows` в новом `app/transfers.py`. `short` и `FX` она берёт из `app/statements.py`.
    - Почему: правило должно жить в чистой функции с табличным тестом. `domain.py` импортировать `statements.py` не может — получится цикл.
    - Цена: ещё один модуль.
12. **Ruling:** у каждого поля суммы есть «своя» карта.
    - «Списано» — карта «Откуда», «Зачислено» — «Куда».
    - «Сумма» — карта «Откуда», а при «не моя карта» — «Куда».
    - Замена уже выбранной карты стирает её поле. Первый выбор «Куда» ничего не стирает.
    - Выбранная карта делает своё поле активным.
    - Почему: так буквально выполняется «смена карты стирает сумму своего поля». Сумма, набранная до выбора «Куда», не пропадает. Сумма, набранная для одной валюты, не уходит в другую.
    - Цена: при смене режима поле может спрятаться вместе со значением. Спрятанное значение не отправляется.
13. **Ruling:** рамку и «— вводите» получает и единственное поле (в макете «Перевод · 1» — «Сумма — вводите»).
14. **Ruling:** что видно до выбора.
    - Пока «Куда» не выбрана, вместо карты — «выберите карту» цветом ссылки.
    - Строка курса — когда обе суммы больше нуля.
    - Таблица «в таблицу» — когда кнопка готова.
    - Почему: раньше строк fact ещё нет.
    - Цена: таблица появляется последней.
15. **Ruling:** на экране перевода одна кнопка цвета кнопки Telegram — и для «Зачислить» («не моя карта»).
    - Почему: соседней кнопки нет, путать не с чем. `MainButton` остаётся цвета темы.
    - Цена: «Зачислить» здесь не зелёная.
16. **Ruling:** на шаге перевода ряд карт «Ввода» (`SourceChips`) скрыт: карта здесь — это «Откуда».
    - Почему: два места выбора карты разошлись бы.
17. **Ruling:** пока открыто окно карт, `MainButton` перевода недоступна.
    - Почему: кнопка панели видна поверх окна, и нажатие записало бы прежнюю «Куда».
18. **Ruling:** первая строка панели для пары — «✓ Записан перевод» или «✓ Записан обмен». Для «не моя карта» — как у «Зачислить».
    - Строка очереди для пары: «переводы 500 000 UZS → HUMO 2222 UZS».
19. **Ruling:** окно карт.
    - Выбранная карта — с «✓ » и цветом кнопки.
    - Под «не моя карта» — «деньги от других людей».
    - Пустой список — «других карт нет».
20. **Ruling:** справочник со «💳 СЧЕТА» и «💰 ДОХОДЫ» для `npm run dev` кладём в `mocks/api.ts`, а `bootstrapFixture` не трогаем.
21. **Ruling:** на проде проверяем только чтением — новые подписи есть в бандле `/app/`. Настоящие «Зачислить», перевод и «Отменить» делает владелец: исполнитель в живую таблицу не пишет.

## Review Focus

1. **Кнопки панели Telegram при нуле и после ухода с экрана.**
   - При сумме 0 обе кнопки недоступны (Ruling 3).
   - Зелёная «Зачислить» не остаётся висеть после записи или на другой вкладке.
   - Тесты — Task 4:
     - `keeps Telegram's buttons off at zero and while one is sending`;
     - `hides «Зачислить» when the sum step closes`.
2. **Очередь в день обновления.**
   - Записи, сохранённые старым приложением, уходят тратой и без подписи.
   - Зачисление из очереди уходит со знаком.
   - Тест — Task 3 `names a credit «зачисление» in the queue and keeps old entries spends`.
3. **Смена карты после ввода суммы.** Сумма, набранная для одной карты (валюты), не уходит на другую.
   - Тест — Task 10 `clears the sum of the card that changed`.
4. **Окно карт поверх готовой кнопки в Telegram.** Пока окно открыто, «Записать перевод» недоступна.
   - Тест — Task 10 `uses Telegram's main button and holds it while the card window is open`.
5. **Выписка после перевода из приложения.** Суммы как в банке — выписка не задваивает пару.
   - Тест — Task 6 `test_a_transfer_from_the_app_is_not_written_again`.

Полупару (анти-цель №1) закрывает один `append_rows`. Тест — Task 7 `test_a_failed_transfer_leaves_no_half_and_the_retry_writes_both`.

---

# Часть 1 — первый PR: «Зачислить» и «Списать», чат «-» и «−»

### Task 1: «-» и «−» в чате — как «+»

**Files:**
- Modify `app/domain.py:463-481`:
  - `_MANUAL` — знак `[+\-−]`;
  - `parse_manual_entry` — приход, если знак есть;
  - docstring: «…, «+20000 возврат», «-20000», «−20000» → …».
- Test:
  - `tests/test_domain.py:256-295` — новые строки в двух параметризациях;
  - `tests/test_messages.py` — тест рядом с `test_manual_incoming_with_plus_is_negative`.

**Interfaces:**
- Produces: `parse_manual_entry(text) -> tuple[float, bool, str] | None` — второе значение `True` при «+», «-» и «−» (U+2212). Пробел между знаком и числом допустим, как сейчас.

- [ ] **Step 1: тесты.** В `tests/test_domain.py`:
  - в `test_parse_manual_entry`:
    - `("-20000 возврат", (20000.0, True, "возврат"))`;
    - `("−20 000 возврат", (20000.0, True, "возврат"))`;
    - `("− 5000", (5000.0, True, ""))`;
  - в `test_manual_row_sign`:
    - `("-5000 вернули", "🍔 ЕДА", -5000.0)`;
    - `("−1000000 с HUMO", "💳 СЧЕТА", -1000000.0)`;
    - `("-30000000 аванс", "💰 ДОХОДЫ", 30000000.0)`.

  `tests/test_messages.py`:

```python
@pytest.mark.parametrize("text", ["-1 000 000 с HUMO", "−1 000 000 с HUMO"])
async def test_manual_incoming_with_minus_is_negative(text: str) -> None:
    state = manual_state("💳 СЧЕТА", "переводы")
    update, context, sheets = make_chat(text, state)
    await messages.text_handler(update, context)
    assert sheets.rows[0][3] == -1000000.0
    assert summary(update).startswith("✅ +1 000 000 UZS")  # ответ бота — как у «+»
```

- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_domain.py tests/test_messages.py`. Ожидается FAIL: «-» даёт трату `(…, False, …)`, а «−» не разбирается.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** весь набор, ruff, black. Ожидается PASS.
- [ ] **Step 5: commit** `fix: «-» and «−» before the sum record incoming money like «+»`

### Task 2: сервер — флаг `incoming`

**Files:**
- Modify `app/web/schemas.py:57-68` — `ExpenseIn.incoming`; docstring: «Запись «Ввода»: трата или зачисление; сумма строкой …».
- Modify `app/web/api.py`:
  - `FIELD_MESSAGES["incoming"]`;
  - в `add_expense` (около :1346) — `entry_row(float(entry.amount), entry.incoming, …)` вместо зашитого `False`.
- Test: `tests/test_web_api.py`.

**Interfaces:**
- Produces:
  - `ExpenseIn.incoming: StrictBool = False` — «Зачислить»: D −сумма вне «💰 ДОХОДЫ», у доходов плюс (`signed_amount`).
  - `FIELD_MESSAGES["incoming"] = "Знак записи не читается — обновите приложение"`.

- [ ] **Step 1: тесты.** `tests/test_web_api.py`, после `test_cents_on_a_dollar_card`:

```python
@pytest.mark.parametrize(
    ("changes", "expected", "line"),
    [
        ({}, 48000.0, "✅ 48 000 UZS • 🍔 ЕДА (☕ кофе)"),
        ({"incoming": False}, 48000.0, "✅ 48 000 UZS • 🍔 ЕДА (☕ кофе)"),
        ({"incoming": True}, -48000.0, "✅ +48 000 UZS • 🍔 ЕДА (☕ кофе)"),
        # «не моя карта» второй части — это она: зачисление на карту в «💳 СЧЕТА»
        (
            {"incoming": True, "category": "💳 СЧЕТА", "subcategory": "переводы"},
            -48000.0,
            "✅ +48 000 UZS • 💳 СЧЕТА (переводы)",
        ),
        # доход — всегда плюсом
        (
            {"incoming": True, "category": "💰 ДОХОДЫ", "subcategory": "зарплата"},
            48000.0,
            "✅ +48 000 UZS • 💰 ДОХОДЫ (зарплата)",
        ),
    ],
)
def test_incoming_is_minus_in_the_sheet_except_for_incomes(
    changes: dict[str, Any], expected: float, line: str
) -> None:
    sheets = FakeSheets()
    response = client_for(make_application(sheets)).post(
        "/api/expenses", json=expense(**changes), headers=auth()
    )
    assert response.status_code == 200
    assert sheets.rows[0][3] == expected
    # ответ бота не меняется: «+» — деньги пришли
    assert response.json()["lines"][0].startswith(line)
```

  В `test_bad_fields_are_named` добавить строки `({"incoming": "yes"}, "incoming")` и `({"incoming": 1}, "incoming")`. Строка `({"amount": "-5"}, "amount")` остаётся.
- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_web_api.py`. Ожидается FAIL: `extra="forbid"` отвечает 422 на `incoming`.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** весь набор, ruff, black. Ожидается PASS.
- [ ] **Step 5: commit** `feat: /api/expenses takes «Зачислить» — the incoming flag`

### Task 3: «Зачислить» и «Списать» в экране, панель результата, подпись очереди

**Files:**
- Modify `webapp/src/api.ts:30-38` — `ExpenseInput.incoming?`.
- Create `webapp/src/entry/outgoing.ts`.
- Modify `webapp/src/format.ts` — `sheetAmount`.
- Modify `webapp/src/dashboard/controls.tsx` — `GOOD_BUTTON`.
- Modify `webapp/src/entry/EntryScreen.tsx`:
  - `submit` (281–372) делится на `send` и `submit(sign)`;
  - `sending` и `done`;
  - кнопки шага суммы (571–580);
  - панель (458–501);
  - строка очереди (94);
  - эффекты `MainButton` (385–408) — только у доходов, до Task 4.
- Test:
  - `webapp/src/entry/EntryScreen.test.tsx`;
  - `webapp/src/entry/outgoing.test.ts` (новый);
  - `webapp/src/format.test.ts`.

**Interfaces:**
- Consumes: Task 2 — сервер принимает `incoming`.
- Produces:
  - `ExpenseInput.incoming?: boolean` с комментарием `// true — «Зачислить»: в таблицу минус (у доходов — плюс); нет поля — трата`.
  - `format.ts`: `sheetAmount(value: number, currency: string): string` — D как в таблице:
    - сумы — `money(value)`;
    - другие валюты — `money(value, 2)` только у дробной суммы, иначе `money(value)`;
    - минус — «−», как у `money`.
  - `entry/outgoing.ts`:
    - `INCOME_GROUP = "💰 ДОХОДЫ"`;
    - `type Sign = "spend" | "credit"`;
    - `interface Outgoing { entry: ExpenseInput; title: string; head: string | null }`, где:
      - `title` — строка очереди: «кофе 48 000 UZS»;
      - `head` — первая строка панели, `null` — без неё;
    - `spentHead(value, currency)` → `` `✓ Списано ${sheetAmount(value, currency)} ${currency}` ``;
    - `creditHead(value, currency)` → `` `✓ Зачислено ${sheetAmount(value, currency)} ${currency} — в таблицу ${sheetAmount(-value, currency)}` ``.
  - `controls.tsx`: `GOOD_BUTTON = "h-12 rounded-xl bg-chip-good text-lg font-extrabold text-white disabled:opacity-50"`.
  - `EntryScreen`:
    - `send(out: Outgoing)` — прежний конвейер `submit` без изменений: `inFlight`, `enqueueSending` с `out.title`, `sendExpense`, `finishSending`, ошибки, `CATALOG_FIELDS`. Знак берётся из записи: `sending = out.entry.incoming ? "credit" : "spend"`. Успех — `setDone({ result, head: out.head })`.
    - `submit(sign: Sign)` собирает `ExpenseInput` из полей экрана: `...(sign === "credit" ? { incoming: true } : {})`.
      - `title` — как сейчас.
      - `head`: `null` у `INCOME_GROUP`; иначе `creditHead` для «credit» или `spentHead`.
    - `sending: Sign | null`; `done: { result: ExpenseResult; head: string | null } | null`.

Вид:
- Не доходы — под клавиатурой `<div className="grid grid-cols-2 gap-2">`:
  - «Зачислить» (`GOOD_BUTTON`, `submit("credit")`);
  - «Списать» (`PRIMARY`, `submit("spend")`).

  После этой задачи пара стоит в экране у не-доходов в любом окружении; в Telegram 7.10+ её уберёт Task 4. Обе кнопки недоступны при `value <= 0 || sending !== null`.
- Доходы — как сейчас: без Telegram в экране одна `PRIMARY` «Записать {withCurrency}», в Telegram — `MainButton`.
- Эффекты `MainButton` срабатывают только при `group?.name === INCOME_GROUP`.
- Панель: `done.head` — первой строкой `<p className="text-fact font-extrabold">`. Под ней — строки сервера или «✅ Уже записано: строка N» и «↩️ Отменить», как сейчас.
- Строка очереди: `` `${item.entry.incoming ? "зачисление: " : ""}${item.title}, ${день}` ``.

- [ ] **Step 1: тесты.**

  `format.test.ts`, `sheetAmount`:
  - `(500000, "UZS")` → «500 000»;
  - `(-500000, "UZS")` → «−500 000»;
  - `(-100, "USD")` → «−100»;
  - `(-100.5, "USD")` → «−100,50».

  `outgoing.test.ts` (сравнивать после `.replace(/\s/g, " ")`):
  - `creditHead(48000, "UZS")` → «✓ Зачислено 48 000 UZS — в таблицу −48 000»;
  - `creditHead(12.5, "USD")` → «✓ Зачислено 12,50 USD — в таблицу −12,50»;
  - `spentHead(100, "USD")` → «✓ Списано 100 USD».

  `EntryScreen.test.tsx`:
  - `renderEntry(api, onCatalogChanged = vi.fn(), boot = bootstrapFixture)`.
  - Свой справочник рядом с `OWNER`. Часть 2 берёт его же:

```tsx
// справочник с доходами и счетами: bootstrapFixture общий для девяти файлов
const ALL_GROUPS: Bootstrap = {
  ...bootstrapFixture,
  sources: [...bootstrapFixture.sources, { name: "HUMO 2222 UZS", currency: "UZS" }],
  groups: [
    ...bootstrapFixture.groups,
    {
      name: "💳 СЧЕТА",
      emoji: "💳",
      title: "СЧЕТА",
      subcategories: [
        { name: "переводы", icon: "🔄" },
        { name: "обмен валюты", icon: "💱" },
        { name: "выравнивание", icon: "⚖️" },
      ],
    },
    {
      name: "💰 ДОХОДЫ",
      emoji: "💰",
      title: "ДОХОДЫ",
      subcategories: [{ name: "зарплата", icon: "💵" }],
    },
  ],
};
```

  Правки прежних тестов:
  - все 22 поиска кнопки `/Записать/`, `/Записать 48\s000 UZS/` и `/Записать 12,50 USD/` → `{ name: "Списать" }`: эти тесты — «🍔 ЕДА»;
  - из `writes the amount step big, in the text colour` убрать три строки про `write`: вид кнопок проверяет новый тест.

  Новые тесты (`ЕДА → кофе`, сумма `4`, `8`, `000`):
  - `puts «Зачислить» left in green and «Списать» right`:
    - у родителя «Списать» класс `grid-cols-2`, дети — `[«Зачислить», «Списать»]`;
    - у «Зачислить» — `bg-chip-good`, `text-white`, `text-lg`, `font-extrabold`;
    - у «Списать» — `bg-button`, `text-button-text`, `text-lg`, `font-extrabold`.
  - `turns both buttons off at zero and while one is sending` (`addExpense` не отвечает, пока не вызвать `finish`):
    - до суммы обе `disabled`;
    - после 48 000 обе доступны;
    - после нажатия «Зачислить» обе снова `disabled`;
    - в конце — `await act(async () => finish(writtenFixture))`.
  - `writes «Зачислить» with the sign and says what went into the table`:
    - `sent(api)` совпадает с `{ amount: "48000", incoming: true }`;
    - `findByText("✓ Зачислено 48 000 UZS — в таблицу −48 000")` с классами `text-fact` и `font-extrabold`;
    - строка сервера `/✅ 48 000 UZS/` — ниже.
  - `writes «Списать» without the sign field`:
    - `expect(sent(api)).not.toHaveProperty("incoming")`;
    - «✓ Списано 48 000 UZS».
  - `keeps one «Записать» for incomes` (`ALL_GROUPS`, `ДОХОДЫ → зарплата`):
    - нет «Зачислить» и «Списать»;
    - кнопка `/^Записать 48\s000 UZS$/`;
    - запрос без `incoming`;
    - после записи нет текста `/^✓ (Списано|Зачислено)/`.
  - `names a credit «зачисление» in the queue and keeps old entries spends`:

```tsx
vi.useFakeTimers({ toFake: ["Date"] });
vi.setSystemTime(new Date("2026-10-10T12:00:00Z"));
const today = bootstrapFixture.today;
enqueue(OWNER, { entry: { ...sentLike("old"), day: today }, title: "кофе 48 000 UZS" });
enqueue(OWNER, {
  entry: { ...sentLike("credit"), incoming: true, day: today },
  title: "кофе 48 000 UZS",
});
const addExpense = vi
  .fn<Api["addExpense"]>()
  .mockRejectedValueOnce(new ApiError(0, "network", "Нет связи"))
  .mockResolvedValue(writtenFixture);
renderEntry(fakeApi({ addExpense }));
await waitFor(() => expect(addExpense).toHaveBeenCalledTimes(1));
const queue = screen.getByRole("region", { name: "Не отправлено" });
expect(
  within(queue).getAllByRole("listitem").map((item) => item.textContent?.replace(/\s/g, " ")),
).toEqual(["кофе 48 000 UZS, сегодня", "зачисление: кофе 48 000 UZS, сегодня"]);
await act(async () => {
  window.dispatchEvent(new Event("online"));
});
expect(await screen.findByText("✅ Отправлено из очереди: 2")).toBeTruthy();
// сохранённая до обновления — без признака, то есть трата
expect(addExpense.mock.calls[1]?.[0]).not.toHaveProperty("incoming");
expect(addExpense.mock.calls[2]?.[0].incoming).toBe(true);
```

- [ ] **Step 2:** `cd webapp && npx vitest run src/entry src/format.test.ts`. Ожидается FAIL: нет кнопки «Списать», нет `sheetAmount`.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** `cd webapp && npm test && npm run lint && npm run build`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: «Зачислить» and «Списать» under the sum, the answer names the sign`

### Task 4: родные кнопки Telegram — `MainButton` «Списать» и `SecondaryButton` «Зачислить»

**Files:**
- Modify `webapp/src/telegram.ts`:
  - `BottomButtonParams`;
  - `BottomButton.setParams`;
  - `WebApp.SecondaryButton?`;
  - `secondaryButton`.
- Modify `webapp/src/dashboard/controls.tsx` — `GOOD_COLOR`.
- Modify `webapp/src/entry/EntryScreen.tsx` — эффекты кнопок панели. Комментарий над ними: «Кнопки панели Telegram: «Назад» после первого шага; на сумме — «Списать» и «Зачислить» (с 7.10), у доходов — «Записать»».
- Create `webapp/src/test/telegram.ts` — заглушка для тестов (её же берёт Task 10).
- Test:
  - `webapp/src/telegram.test.ts`;
  - `webapp/src/theme.test.ts`;
  - `webapp/src/entry/EntryScreen.test.tsx`.

**Interfaces:**
- Consumes: Task 3 — `submit(sign)`, `sending`, `INCOME_GROUP`, `ALL_GROUPS`.
- Produces:
  - `interface BottomButtonParams { text?: string; color?: string; text_color?: string; position?: "left" | "right" | "top" | "bottom"; is_active?: boolean; is_visible?: boolean }`, в `BottomButton` — `setParams(params: BottomButtonParams): BottomButton`.
  - `WebApp.SecondaryButton?: BottomButton` — с Bot API 7.10.
  - `secondaryButton(app: WebApp | null): BottomButton | null` — `app.SecondaryButton`, если `app.isVersionAtLeast("7.10")` и кнопка есть; иначе `null`.
  - `GOOD_COLOR = "#146c2e"` — то же значение, что `--color-chip-good`: родной кнопке цвет задаётся строкой.
  - `test/telegram.ts`:
    - `inTelegram(version = true): { main: FakeButton; second: FakeButton }` — `vi.stubGlobal("Telegram", { WebApp })` с:
      - `platform: "ios"`, `isVersionAtLeast: () => version`;
      - `MainButton`, `SecondaryButton` — `FakeButton`: все методы `BottomButton` в виде `vi.fn()`;
      - `BackButton`, `HapticFeedback` — пустые `vi.fn()`.

      Снимать — `vi.unstubAllGlobals()`.
    - `tapPanel(button: FakeButton): Promise<void>` — `act` + последний обработчик из `button.onClick.mock.lastCall`.
    - `lastCall(method: Mock): number` — `Math.max(0, ...method.mock.invocationCallOrder)`.

Правила (`second = secondaryButton(tg)`, `income = group?.name === INCOME_GROUP`, `native = tg !== null && (income || second !== null)`):
- Кнопки в экране (Task 3) — только при `!native`.
- `MainButton` на шаге суммы при `native`:
  - показывается при входе в шаг, прячется при выходе;
  - текст — у доходов «Записать {withCurrency}», иначе «Списать»;
  - нажатие — `submit("spend")`.
- `SecondaryButton` на шаге суммы при `second && !income`:
  - при входе — `setParams({ text: "Зачислить", color: GOOD_COLOR, text_color: "#ffffff", position: "left", is_visible: true })`. Текст — в том же вызове: `is_visible` при пустом тексте бросает исключение.
  - при выходе (смена шага, размонтирование) — `hide()`. Эффект показа не зависит от суммы.
  - нажатие — `submit("credit")`.
- Состояние обеих кнопок при каждом изменении (Ruling 3):
  - ждёт ответа своего знака — `showProgress(false)`;
  - иначе `hideProgress()`, затем `enable()` при `value > 0 && sending === null` или `disable()`.

- [ ] **Step 1: тесты.**

  `telegram.test.ts`, `secondaryButton`:
  - `isVersionAtLeast: (v) => v === "7.10"` и `SecondaryButton` — возвращает эту кнопку;
  - `isVersionAtLeast: () => false` — `null`;
  - нет `SecondaryButton` — `null`;
  - `null` — `null`.

  `theme.test.ts` — `paints Telegram's «Зачислить» with the green badge colour`: `` expect(CSS).toContain(`--color-chip-good: ${GOOD_COLOR};`) ``.

  `EntryScreen.test.tsx` — `describe("EntryScreen in Telegram")`, `afterEach`: `vi.unstubAllGlobals()`, `writeOutbox(OWNER, [])`, `localStorage.clear()`.

```tsx
it("puts «Зачислить» left of «Списать» on Telegram's bar from 7.10", async () => {
  const { main, second } = inTelegram();
  const api = fakeApi();
  const user = userEvent.setup();
  renderEntry(api);
  await fillCoffee(user);
  expect(second.setParams).toHaveBeenCalledWith({
    text: "Зачислить",
    color: GOOD_COLOR,
    text_color: "#ffffff",
    position: "left",
    is_visible: true,
  });
  expect(main.setText).toHaveBeenLastCalledWith("Списать");
  expect(screen.queryByRole("button", { name: "Зачислить" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Списать" })).toBeNull();
  await tapPanel(second);
  expect(sent(api)).toMatchObject({ amount: "48000", incoming: true });
  expect(
    await screen.findByText("✓ Зачислено 48 000 UZS — в таблицу −48 000"),
  ).toBeTruthy();
  expect(second.hide).toHaveBeenCalled(); // форма вернулась к группам
});

it("keeps Telegram's buttons off at zero and while one is sending", async () => {
  const { main, second } = inTelegram();
  const addExpense = vi.fn<Api["addExpense"]>(() => new Promise(() => {}));
  const user = userEvent.setup();
  renderEntry(fakeApi({ addExpense }));
  await user.click(screen.getByRole("button", { name: "ЕДА" }));
  await user.click(screen.getByRole("button", { name: "кофе" }));
  // hideProgress снова включает кнопку — выключаем после него
  for (const button of [main, second]) {
    expect(lastCall(button.disable)).toBeGreaterThan(lastCall(button.hideProgress));
  }
  for (const key of ["4", "8", "000"]) {
    await user.click(screen.getByRole("button", { name: key }));
  }
  for (const button of [main, second]) {
    expect(lastCall(button.enable)).toBeGreaterThan(lastCall(button.disable));
  }
  await tapPanel(main);
  expect(main.showProgress).toHaveBeenLastCalledWith(false);
  expect(second.showProgress).not.toHaveBeenCalled(); // крутится только нажатая
  expect(lastCall(second.disable)).toBeGreaterThan(lastCall(second.enable));
});
```

  Ещё в этом `describe`:
  - `hides «Зачислить» when the sum step closes`:
    - `render(<EntryScreen … wait={noWait} />)`, затем `fillCoffee`;
    - `second.hide` не вызывался;
    - после `unmount()` (другая вкладка) — вызван один раз.
  - `keeps the two buttons in the screen on an older Telegram` — `inTelegram(false)`:
    - «Зачислить» и «Списать» — в экране;
    - `main.show` и `second.setParams` не вызывались.
  - `keeps one «Записать» on Telegram's bar for incomes` — `ALL_GROUPS`, `ДОХОДЫ → зарплата`, 48 000:
    - `main.setText.mock.lastCall?.[0]` совпадает с `/^Записать 48\s000 UZS$/`;
    - `second.setParams` не вызывался.
- [ ] **Step 2:** `cd webapp && npx vitest run src/telegram.test.ts src/theme.test.ts src/entry/EntryScreen.test.tsx`. Ожидается FAIL: нет `secondaryButton` и `GOOD_COLOR`.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** `cd webapp && npm test && npm run lint && npm run build`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: Telegram's bottom bar carries «Зачислить» and «Списать»`

### Task 5: документы, снимки, выкладка части 1

**Files:**
- Modify `README.md`:
  - Usage, шаг 3: «A leading `+` records incoming money: `+20000 refund`.» → «A leading `+`, `-` or `−` records incoming money: `+20000 refund`, `-20000 refund`.»
  - Абзац Mini App, после «…`/undo` and `/fix` work with it)»: «Under the sum there are two buttons side by side: «Зачислить» on the left (green) writes the sum with a minus in the sheet — a top-up, a refund, a correction — and «Списать» on the right (the Telegram button colour) writes it with a plus, like a spending; «💰 ДОХОДЫ» keeps one «Записать» (an income is always plus). In Telegram 7.10+ they are Telegram's own bottom buttons, elsewhere the same buttons in the screen. The answer starts with «✓ Списано 48 000 UZS» or «✓ Зачислено 48 000 UZS — в таблицу −48 000»; a credit waiting in the phone's queue is marked «зачисление».»
- Modify `CLAUDE.md`:
  - Manual Entry: «(`+` prefix = incoming money)» → «(`+`, `-` or `−` prefix = incoming money)».
  - Строка `api.py`: «/api/expenses (entry_row → save_rows, idempotent by entry_id)» → «/api/expenses (entry_row → save_rows, idempotent by entry_id; `incoming` — «Зачислить»: minus in D outside incomes)».
  - Строка `webapp/`, `src/entry`: после «Keypad big — only here…» добавить «; under the sum «Зачислить» (green, minus in D) and «Списать» — Telegram's SecondaryButton/MainButton from 7.10, otherwise in the screen; «💰 ДОХОДЫ» keeps «Записать»; outgoing.ts — the sign and the answer's head».

- [ ] **Step 1:** документы по списку. Commit `docs: «Зачислить» and «Списать»`.
- [ ] **Step 2: снимки** на моках (`cd webapp && npm run dev`), как в прошлых этапах. Что снимать:
  - «Ввод» → «🍔 ЕДА → ☕ кофе» с суммой 48 000;
  - панель после «Зачислить».

  Размеры — 360 px в тёмной теме и 320 px с крупным шрифтом (textZoom ×1,25). Expected:
  - «Зачислить» слева зелёная, «Списать» справа, поровну, слово целиком;
  - «— в таблицу −48 000» видно;
  - без горизонтальной прокрутки.
- [ ] **Step 3: PR** (diff проверен grep-ом из Global Constraints), ревью, мерж. Выкладка: `/health` отвечает 200, в бандле `/app/` есть «Зачислить» и «Списать». Настоящую запись и «Отменить» делает владелец.

---

# Часть 2 — второй PR: перевод «Откуда → Куда»

Ветка `feat/transfers-pair` от main после мержа части 1.

### Task 6: две строки пары — `transfer_rows`

**Files:**
- Create `app/transfers.py`.
- Create `tests/test_transfers.py`.
- Modify `tests/test_statements.py` — тест рядом с `test_transfer_pairs_with_an_already_loaded_half`.

**Interfaces:**
- Consumes: `domain.entry_row`, `domain.SheetRow`, `statements.short`, `statements.FX`.
- Produces: `transfer_rows(amount: float, to_amount: float | None, comment: str, source: str, to_source: str, category: str, subcategory: str, day: date) -> list[SheetRow]`.
  - Строки:
    - `entry_row(amount, False, note("→", source, to_source), source, …)`;
    - `entry_row(amount if to_amount is None else to_amount, True, note("←", to_source, source), to_source, …)`.
  - `note` = `f"{label} {short(card)} {arrow} {short(other)} (свой)"`, затем `f", {comment}"`, если комментарий есть.
  - `label` — «конвертация», если `subcategory == FX[1]`, иначе «перевод».
  - Docstring модуля: снятие и пополнение, комментарии как у SMS и выписок.

- [ ] **Step 1: тесты.** `tests/test_transfers.py`:

```python
"""Перевод между своими картами из «Ввода»: две строки fact одной записью."""

from datetime import date

import pytest

from app.transfers import transfer_rows

DAY = date(2026, 10, 5)


@pytest.mark.parametrize(
    ("to_source", "subcategory", "amount", "to_amount", "comment", "expected"),
    [
        (
            "UZCARD 5656 UZS", "переводы", 500_000, None, "",
            [
                ("VISA 9090 UZS", 500_000, "UZS", "перевод VISA 9090 → UZCARD 5656 (свой)"),
                ("UZCARD 5656 UZS", -500_000, "UZS", "перевод UZCARD 5656 ← VISA 9090 (свой)"),
            ],
        ),
        (
            "VISA 3434 USD", "обмен валюты", 1_210_800, 100, "",
            [
                ("VISA 9090 UZS", 1_210_800, "UZS", "конвертация VISA 9090 → VISA 3434 USD (свой)"),
                ("VISA 3434 USD", -100, "USD", "конвертация VISA 3434 USD ← VISA 9090 (свой)"),
            ],
        ),
        (
            "UZCARD 5656 UZS", "переводы", 500_000, 500_000, "на отпуск",
            [
                ("VISA 9090 UZS", 500_000, "UZS", "перевод VISA 9090 → UZCARD 5656 (свой), на отпуск"),
                ("UZCARD 5656 UZS", -500_000, "UZS", "перевод UZCARD 5656 ← VISA 9090 (свой), на отпуск"),
            ],
        ),
    ],
)
def test_a_transfer_is_a_withdrawal_and_a_top_up(
    to_source: str,
    subcategory: str,
    amount: float,
    to_amount: float | None,
    comment: str,
    expected: list[tuple[str, float, str, str]],
) -> None:
    rows = transfer_rows(
        amount, to_amount, comment, "VISA 9090 UZS", to_source, "💳 СЧЕТА", subcategory, DAY
    )
    assert [(r.source, r.amount, r.currency, r.comment) for r in rows] == expected
    assert {(r.day, r.category, r.subcategory) for r in rows} == {
        (DAY, "💳 СЧЕТА", subcategory)
    }
```

  `tests/test_statements.py`, импорт `from app.transfers import transfer_rows`. Проверено на прототипе: `rows == []`, `already == 2`; для обмена 1 210 800 UZS / 100 USD — тоже.

```python
def test_a_transfer_from_the_app_is_not_written_again() -> None:
    # «Ввод» записал перевод 5-го; выписки видят снятие 5-го и зачисление 6-го
    day = date(2026, 10, 5)
    rows = transfer_rows(
        500_000, None, "", "VISA 9090 UZS", "HUMO 7007 UZS", "💳 СЧЕТА", "переводы", day
    )
    values = fact_values(
        *(
            [serial(r.day), r.category, r.subcategory, r.amount, "", r.comment]
            + [r.currency, r.source]
            for r in rows
        )
    )
    details = "Перевод с VISA UZS KB на HUMO KB через МП"
    out = txn("VISA 9090 UZS", -500_000, details, date(2026, 10, 5))
    inc = txn("HUMO 7007 UZS", 500_000, details, date(2026, 10, 6))
    statements = [
        st.Statement(t.source, [t], (date(2026, 10, 1), STATEMENT_DAY), STATEMENT_DAY)
        for t in (out, inc)
    ]
    plan = st.plan_import(statements, CATALOG, values, {})
    assert (plan.rows, plan.already) == ([], 2)
```

- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_transfers.py tests/test_statements.py`. Ожидается FAIL: `No module named 'app.transfers'`.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** `../venv_dev/bin/black .` — длинные строки таблицы теста black разложит сам. Затем весь набор, ruff, `black --check`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: a transfer between own cards is a withdrawal and a top-up row`

### Task 7: сервер — `to_source` и `to_amount`

**Files:**
- Modify `app/web/schemas.py` — поля `ExpenseIn`; docstring: «… трата, зачисление или перевод между своими картами …».
- Modify `app/web/api.py`:
  - docstring модуля: «запись траты, зачисления или перевода между своими картами (две строки одной записью)»;
  - `FIELD_MESSAGES`;
  - `check_entry`;
  - `add_expense`.
- Test: `tests/test_web_api.py`.

**Interfaces:**
- Consumes: Task 2 — `ExpenseIn.incoming`; Task 6 — `transfer_rows`.
- Produces:
  - `ExpenseIn.to_source: str | None = Field(default=None, min_length=1, max_length=100)` — карта «Куда».
  - `ExpenseIn.to_amount: Decimal | None = Field(default=None, gt=0, le=Decimal(10) ** 12, decimal_places=2)` — «Зачислено» в валюте «Куда».
  - `FIELD_MESSAGES`:
    - `"to_source": "«Куда» — другая своя карта, перевод — только в «💳 СЧЕТА»"`;
    - `"to_amount": "«Зачислено» — больше нуля; в разных валютах — обязательно, в одной — как «Списано»"`.
  - `check_entry` — после прежних проверок:

    | Условие | Поле |
    |---|---|
    | `to_source is None` и `to_amount is not None` | `to_amount` |
    | `to_source` и `incoming` | `incoming` |
    | `to_source` и (группа не `TRANSFERS_GROUP`, или карты нет в `sources`, или она равна `source`) | `to_source` |
    | иначе, `source` из справочника: валюты разные и `to_amount is None` | `to_amount` |
    | иначе: валюта одна, `to_amount` есть и `!= amount` | `to_amount` |

    Docstring: «Поля, которых нет в справочнике бота, дата вне окна и неверная пара».
  - `add_expense`:
    - `day = entry.day or today`;
    - с `to_source` — `rows = transfer_rows(float(entry.amount), None if entry.to_amount is None else float(entry.to_amount), entry.comment, entry.source, entry.to_source, entry.category, entry.subcategory, day)`;
    - иначе — `[entry_row(…, entry.incoming, …)]`;
    - один `save_rows(context, chat, rows, [], fingerprint, notify_failure=False)`.

- [ ] **Step 1: тесты.** `tests/test_web_api.py`, после `expense()`:

```python
def transfer(**changes: Any) -> dict[str, Any]:
    """Перевод «Ввода»: VISA 9090 → UZCARD 5656, 500 000 сумов."""
    body = {
        "source": "VISA 9090 UZS",
        "to_source": "UZCARD 5656 UZS",
        "category": "💳 СЧЕТА",
        "subcategory": "переводы",
        "amount": "500000",
        "comment": "",
    }
    return expense(**{**body, **changes})


def test_a_transfer_writes_two_rows_in_one_write() -> None:
    sheets = FakeSheets()
    application = make_application(sheets)
    response = client_for(application).post(
        "/api/expenses", json=transfer(comment="на отпуск"), headers=auth()
    )
    assert response.status_code == 200
    assert response.json()["rows"] == {"first": 4169, "last": 4170}
    assert sheets.appends == 1
    assert [row[1:4] for row in sheets.rows] == [
        ["💳 СЧЕТА", "переводы", 500000.0],
        ["💳 СЧЕТА", "переводы", -500000.0],
    ]
    assert [row[5:] for row in sheets.rows] == [
        ["перевод VISA 9090 → UZCARD 5656 (свой), на отпуск", "UZS", "VISA 9090 UZS"],
        ["перевод UZCARD 5656 ← VISA 9090 (свой), на отпуск", "UZS", "UZCARD 5656 UZS"],
    ]
    assert sheets.rows[0][0] == sheets.rows[1][0] == TODAY.strftime("%d.%m.%Y")
    # текущая карта «Ввода» — «Куда»
    assert application.user_data[OWNER]["source"] == "UZCARD 5656 UZS"
```

  Ещё:
  - `test_an_exchange_writes_each_side_in_its_currency`:
    - `application.bot_data["subcategories"] = {**SUBCATEGORIES, "💳 СЧЕТА": ["переводы", "обмен валюты"]}`;
    - `transfer(to_source="VISA 3434 USD", subcategory="обмен валюты", amount="1210800", to_amount="100")` → 200;
    - `[(r[3], r[5], r[6]) for r in sheets.rows] == [(1210800.0, "конвертация VISA 9090 → VISA 3434 USD (свой)", "UZS"), (-100.0, "конвертация VISA 3434 USD ← VISA 9090 (свой)", "USD")]`.
  - `test_the_same_sum_twice_is_fine_in_one_currency` — `transfer(to_amount="500000.00")` → 200, две строки.
  - `test_a_transfer_is_repeated_and_undone_as_one_write`:
    - тот же `transfer()` дважды: второй ответ — `("duplicate", {"first": 4169, "last": 4170})`, строк по-прежнему 2;
    - `/api/expenses/undo` → «↩️ Удалил из таблицы: строки 4169–4170.».
  - `test_a_failed_transfer_leaves_no_half_and_the_retry_writes_both`:
    - `FlakySheets` по образцу `test_sheet_failure_is_503_and_the_retry_writes`;
    - первая попытка — 503 и `sheets.rows == []`;
    - повтор — `written`, две строки.
  - `test_bad_transfers_are_named`:

```python
@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"category": "🍔 ЕДА", "subcategory": "кофе"}, "to_source"),
        ({"to_source": "VISA 9090 UZS"}, "to_source"),
        ({"to_source": "MIR 0000 RUB"}, "to_source"),
        ({"to_source": "VISA 3434 USD"}, "to_amount"),  # разные валюты — нужна
        ({"to_amount": "400000"}, "to_amount"),  # одна валюта — только та же
        ({"to_source": "VISA 3434 USD", "to_amount": "0"}, "to_amount"),
        ({"to_source": None, "to_amount": "500000"}, "to_amount"),
        ({"incoming": True}, "incoming"),
    ],
)
def test_bad_transfers_are_named(changes: dict[str, Any], field: str) -> None:
    sheets = FakeSheets()
    response = client_for(make_application(sheets)).post(
        "/api/expenses", json=transfer(**changes), headers=auth()
    )
    assert response.status_code == 422
    assert list(response.json()["error"]["fields"]) == [field]
    assert sheets.rows == []
```

- [ ] **Step 2:** `../venv_dev/bin/python -m pytest -q -p no:cacheprovider tests/test_web_api.py`. Ожидается FAIL: `extra="forbid"` даёт 422 на `to_source`.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** весь набор, ruff, black. Ожидается PASS.
- [ ] **Step 5: commit** `feat: /api/expenses writes a transfer between own cards — to_source and to_amount`

### Task 8: курс и строки «в таблицу» — `entry/transfer.ts`

**Files:**
- Create `webapp/src/entry/transfer.ts`.
- Create `webapp/src/entry/transfer.test.ts`.
- Modify `webapp/src/mocks/fixtures.ts` — `transferSources`, `transferCards` (их берут Task 9 и 10).

**Interfaces:**
- Consumes: `format.money`.
- Produces:
  - `TRANSFERS_GROUP = "💳 СЧЕТА"`, `TRANSFER_SUB = "переводы"`, `EXCHANGE_SUB = "обмен валюты"`.
  - `isTransfer(group: string, sub: string): boolean` — «💳 СЧЕТА» и одна из двух подкатегорий.
  - `balanceOf(cards: CardBalance[] | null, source: string): CardBalance | null` — карта «Остатков на картах», у которой `` `${card.name} ${card.currency}` === source ``.
  - `rateLine(sent: number, sentCurrency: string, got: number, gotCurrency: string): string` — обе суммы больше нуля.
    - Единицы: `UZS` — «сум», `USD` — «$», `RUB` — «₽», остальные — код.
    - Между числом и единицей — неразрывный пробел, как в `format.ts`.

```ts
// дороже та валюта, единица которой стоит больше единиц другой
const [unit, rate, other] =
  sent / got >= 1
    ? [gotCurrency, sent / got, sentCurrency]
    : [sentCurrency, got / sent, gotCurrency];
const number = rate >= 100 ? money(Math.round(rate)) : money(rate, 2);
// → `курс: 1 ${UNIT(unit)} = ${number} ${UNIT(other)}`
```

  - `interface FactLine { card: string; amount: number }` — строка fact, D со знаком.
  - `factLines(from: string | null, to: string, sent: number, got: number, two: boolean): FactLine[]`:
    - `from === null` («не моя карта») — `[{ to, −got }]`;
    - `two` — `[{ from, +sent }, { to, −got }]`;
    - иначе — `[{ from, +sent }, { to, −sent }]`.
  - `fixtures.ts`:
    - `transferSources: Source[]` — VISA 1111 UZS, HUMO 2222 UZS, VISA 3333 USD;
    - `transferCards: CardBalance[]`:
      - «VISA 1111» UZS, `balance: 5_250_000`;
      - «VISA 3333» USD, `balance: 1500.25`;
      - `uzs` — 5 250 000 и 18 003 000; у обеих `bank: null`, `checked: null`, `matches: null`, `stale: false`;
      - у HUMO 2222 строки нет: «на карте» не показывается.

- [ ] **Step 1: тесты** `transfer.test.ts`. Строки сравнивать после `.replace(/\s/g, " ")`: `money` ставит неразрывные пробелы.
  - `it.each` для `rateLine`:
    - `(1_210_800, "UZS", 100, "USD")` → «курс: 1 $ = 12 108 сум»;
    - `(100, "USD", 1_210_000, "UZS")` → «курс: 1 $ = 12 100 сум»;
    - `(8_750, "RUB", 100, "USD")` → «курс: 1 $ = 87,50 ₽»;
    - `(10_000, "RUB", 1_400_000, "UZS")` → «курс: 1 ₽ = 140 сум»;
    - `(100, "EUR", 110, "USD")` → «курс: 1 EUR = 1,10 $».
  - `factLines`:
    - `("VISA 1111 UZS", "HUMO 2222 UZS", 500_000, 0, false)` → `+500 000` и `−500 000`;
    - `("VISA 1111 UZS", "VISA 3333 USD", 1_210_800, 100, true)` → `+1 210 800` и `−100`;
    - `(null, "HUMO 2222 UZS", 0, 300_000, false)` → одна строка `−300 000` у HUMO 2222.
  - `isTransfer`:
    - «💳 СЧЕТА / переводы» и «💳 СЧЕТА / обмен валюты» — `true`;
    - «💳 СЧЕТА / выравнивание» и «🍔 ЕДА / переводы» — `false`.
  - `balanceOf`:
    - `(transferCards, "VISA 3333 USD")?.balance === 1500.25`;
    - HUMO 2222 и `cards = null` — `null`.
- [ ] **Step 2:** `cd webapp && npx vitest run src/entry/transfer.test.ts`. Ожидается FAIL: модуля нет.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** `cd webapp && npm test && npm run lint && npm run build`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: the rate and the rows «в таблицу» of a transfer`

### Task 9: окно карт — `CardSheet`

**Files:**
- Create `webapp/src/entry/CardSheet.tsx`.
- Create `webapp/src/entry/CardSheet.test.tsx`.

**Interfaces:**
- Consumes: Task 8 — `balanceOf`, `transferSources`, `transferCards`; `plan/Sheet`, `moneyText`, `Line`.
- Produces: `CardSheet({ side, sources, chosen, exclude, cards, onPick, onClose })`:
  - `side: "from" | "to"` — заголовок окна «Откуда» или «Куда».
  - `chosen: string | null` — выбранная карта стороны. У `"from"` значение `null` — «не моя карта».
  - `exclude: string | null` — карта другой стороны, её в списке нет.
  - `onPick: (name: string | null) => void` — `null` — «не моя карта».
  - Список `<ul aria-label="Карты">` — карты справочника по порядку, без `exclude`.
    - Каждая — `<button>` `w-full rounded-xl px-4 py-3 text-left`: выбранная — `bg-button text-button-text`, остальные — `bg-section text-text`.
    - Внутри:
      - название — `block text-fact font-extrabold break-words`, у выбранной с «✓ »;
      - под ним — `block text-line` «на карте {moneyText(balance, currency)}», если `balanceOf` дал карту с `balance !== null`.
  - У `"from"` последняя строка — «не моя карта» и под ней «деньги от других людей». Отмечена при `chosen === null`.
  - Карт для выбора нет — `<Line>других карт нет</Line>`.
  - Тап по строке — `onPick`. Окно закрывает вызывающий.

- [ ] **Step 1: тесты** `CardSheet.test.tsx` (`transferSources`, `transferCards`):
  - `lists the cards big with their balance` — `side="to"`, `chosen={null}`, `exclude="VISA 1111 UZS"`:
    - кнопки списка — HUMO 2222 UZS и VISA 3333 USD, по порядку;
    - у VISA 3333 USD — «на карте 1 500,25 $»;
    - у HUMO 2222 строки «на карте» нет;
    - название — `text-fact` и `font-extrabold`;
    - «не моя карта» нет.
  - `marks the chosen card and puts «не моя карта» last for «Откуда»` — `side="from"`, `chosen="VISA 1111 UZS"`, `exclude="HUMO 2222 UZS"`:
    - кнопки по порядку: «✓ VISA 1111 UZS» (класс `bg-button`), VISA 3333 USD, «не моя карта» с «деньги от других людей»;
    - HUMO 2222 нет.
  - `picks a card or «не моя карта»`:
    - тап по VISA 3333 USD → `onPick("VISA 3333 USD")`;
    - тап по «не моя карта» → `onPick(null)`.
  - `says when there is no other card` — `sources` = только VISA 1111 UZS, `side="to"`, `exclude="VISA 1111 UZS"` → «других карт нет».
- [ ] **Step 2:** `cd webapp && npx vitest run src/entry/CardSheet.test.tsx`. Ожидается FAIL.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** `cd webapp && npm test && npm run lint && npm run build`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: the card window of a transfer`

### Task 10: экран «Откуда → Куда» — `TransferScreen`

**Files:**
- Create `webapp/src/entry/EntryFields.tsx` — вынести из шага суммы `EntryScreen.tsx:531-563`:
  - `AmountText`;
  - `CommentField`;
  - `DayField` с `MAX_DAYS_BACK` и `AMOUNT_FITS`.

  Шаг суммы берёт их же, вид прежний: его держат тесты `writes the amount step big…` и `makes a long amount a step smaller…`.
- Create `webapp/src/entry/TransferScreen.tsx`.
- Create `webapp/src/entry/TransferScreen.test.tsx`.
- Modify `webapp/src/api.ts` — `ExpenseInput.to_source?`, `to_amount?`.
- Modify `webapp/src/entry/outgoing.ts` — `Outgoing.card?`.

**Interfaces:**
- Consumes:
  - Task 3 — `Outgoing`, `creditHead`, `sheetAmount`, `PRIMARY`;
  - Task 4 — `test/telegram.ts`;
  - Task 8 — `rateLine`, `factLines`, `balanceOf`, `EXCHANGE_SUB`;
  - Task 9 — `CardSheet`.
- Produces:
  - `ExpenseInput`:
    - `to_source?: string` — перевод между своими картами: карта «Куда»;
    - `to_amount?: string` — «Зачислено» в валюте «Куда»; в одной валюте не отправляется.
  - `Outgoing.card?: string` — карта «Ввода» после записи (у перевода — «Куда»).
  - `EntryFields.tsx`:
    - `AmountText({ text, currency, center = false })` — прежний `<p>` суммы. Выравнивание по центру — только при `center`.
    - `CommentField({ value, onChange })`.
    - `DayField({ day, today, onChange })`: `onChange(null)`, если выбрали сегодня.
  - `TransferScreen({ group, sub, sources, current, cards, today, entryId, busy, onSubmit })`:
    - `current` — «Откуда» по умолчанию.
    - `cards: CardBalance[] | null` — остатки «Сводки».
    - `busy` — запись уходит.
    - `onSubmit: (out: Outgoing) => void`.

Состояние:
- `from: string | null` — `null` означает «не моя карта», начальное значение — `current`.
- `to: string | null` — `null` означает «не выбрана».
- `sent`, `got` — строки клавиатуры.
- `active: "sent" | "got"`, `comment`, `day`, `picking: "from" | "to" | null`.

Правила (Ruling 12–17):
- `two` — валюты обеих выбранных карт есть и разные.
- Поля:
  - `two` — «Списано» (`sent`, валюта «Откуда») и «Зачислено» (`got`, валюта «Куда»), активное — `active`;
  - иначе одно поле «Сумма»: `got` в валюте «Куда», если `from === null`, иначе `sent` в валюте «Откуда».
- Выбор «Откуда» — другая карта или «не моя»: `sent = ""`, `active = "sent"`.
- Выбор «Куда»: если до этого была другая карта — `got = ""`; `active = "got"`.
- Клавиатура пишет в видимое активное поле. `decimals` — валюта поля не «UZS» (неизвестная валюта — как «UZS»).
- Готовность, `ready`: `to !== null` и:
  - при «не моя карта» — `got > 0`;
  - иначе `sent > 0`, а при `two` ещё и `got > 0`.
- Кнопка:
  - подпись — «Зачислить» (`from === null`), «Записать обмен» (`sub.name === EXCHANGE_SUB`) или «Записать перевод»;
  - доступна при `ready && !busy`, в Telegram ещё и при `picking === null`.
- `Outgoing`:
  - своя пара:
    - `entry` — `{ entry_id, source: from, to_source: to, category, subcategory, amount: apiAmount(sent), ...(two ? { to_amount: apiAmount(got) } : {}), comment: comment.trim(), ...(day ? { day } : {}) }`;
    - `title` — `` `${sub.name} ${withCurrency(sent, валюта «Откуда»)} → ${to}` ``;
    - `head` — «✓ Записан перевод» или «✓ Записан обмен»;
    - `card` — `to`.
  - «не моя карта»:
    - `entry` — `{ entry_id, source: to, incoming: true, category, subcategory, amount: apiAmount(got), comment, day? }`;
    - `title` — `` `${sub.name} ${withCurrency(got, валюта «Куда»)}` ``;
    - `head` — `creditHead(got, валюта «Куда»)`;
    - `card` — `to`.
- Telegram (`telegram()`):
  - `MainButton` показывается при монтировании и прячется при размонтировании;
  - текст — подпись кнопки;
  - состояние — по Ruling 3: `busy` → `showProgress(false)`, иначе `hideProgress()`, затем `enable()` или `disable()`;
  - нажатие — `submit`.

  Без Telegram — `PRIMARY` в экране.

Вид, сверху вниз (заголовок рисует `EntryScreen`):
1. Поле карты — `<button>` `flex w-full items-center gap-3 rounded-2xl bg-section p-4 text-left`. Внутри колонка `flex min-w-0 flex-1 flex-col`:
   - подпись «Откуда» / «Куда» — `text-line font-bold`;
   - карта — `text-fact font-extrabold break-words`:
     - нет карты — «выберите карту» с `text-link`;
     - «не моя карта» — она, под ней «деньги от других людей»;
   - «на карте {moneyText}» — `text-line`;
   - справа `<span aria-hidden="true" className="shrink-0 text-fact font-extrabold text-link">›</span>`.
2. Поле суммы — `<button type="button" aria-pressed={активно}>` `flex w-full flex-col items-start gap-1 rounded-2xl bg-section px-4 py-3 text-left`.
   - Рамка: активное — `border-4 border-button`, остальные — `border-2 border-outline`.
   - Подпись `text-line font-bold`: «Списано», «Зачислено» или «Сумма»; у активного — с « — вводите».
   - Ниже — `AmountText`.
3. При `two` и обеих суммах больше нуля — `<p className="text-line font-bold">{rateLine(…)}</p>`.
4. `CommentField`, `DayField`, `Keypad big`.
5. При `ready` — `<Table label="В таблицу" head={["карта", "в таблицу"]} align="lr">`. Строки — `factLines`: `[card, sheetAmount(amount, валюта карты)]`.
6. Кнопка без Telegram; `CardSheet` при `picking`. В окне `exclude` — карта другой стороны; выбор применяет правила выше и закрывает окно (`picking = null`), «✕» — просто закрывает.

- [ ] **Step 1: тесты** `TransferScreen.test.tsx`. Помощники:
  - `show(sub = "переводы", props?)` — рендер с:
    - группой «💳 СЧЕТА» («переводы» 🔄, «обмен валюты» 💱);
    - `transferSources`, `current="VISA 1111 UZS"`, `transferCards`;
    - `today="2026-10-10"`, `entryId="e1"`, `busy={false}`;
    - возвращает `onSubmit`.
  - `pick(user, side, card: RegExp)` — тап по `/^Откуда/` или `/^Куда/`, затем по карте в `dialog` с тем же именем. Регулярка карты — без `^` (`/HUMO 2222 UZS/`): имя выбранной начинается с «✓ ».
  - `keys(user, ...names)`.
  - `factRows()` — `td` таблицы «В таблицу» после `.replace(/\s/g, " ")`.

  Тесты:
  - `starts from the current card and asks where to`:
    - «Откуда» содержит «VISA 1111 UZS» и «на карте 5 250 000»;
    - «Куда» — «выберите карту»;
    - есть «Сумма — вводите»;
    - таблицы «В таблицу» нет;
    - «Записать перевод» `disabled`.
  - `leaves the other side's card out of the window`:
    - окно «Куда» — без VISA 1111 UZS и без «не моя карта»;
    - после выбора HUMO 2222 окно «Откуда» — без HUMO 2222, «не моя карта» последней.
  - `writes one sum for cards of one currency`:
    - Куда HUMO 2222, клавиши `5 0 0 000`;
    - `factRows()` — `[["VISA 1111 UZS", "500 000"], ["HUMO 2222 UZS", "−500 000"]]`;
    - «Записать перевод» → `onSubmit` с:
      - `entry` — `toEqual({ entry_id: "e1", source: "VISA 1111 UZS", to_source: "HUMO 2222 UZS", category: "💳 СЧЕТА", subcategory: "переводы", amount: "500000", comment: "" })`;
      - `title` — «переводы 500 000 UZS → HUMO 2222 UZS»;
      - `head` — «✓ Записан перевод»;
      - `card` — «HUMO 2222 UZS».
  - `asks for both sums across currencies and checks the rate` (`show("обмен валюты")`):
    - Куда VISA 3333 USD;
    - кнопка `/^Зачислено/`: `aria-pressed="true"`, «Зачислено — вводите», класс `border-button`;
    - `1 0 0`, тап `/^Списано/`, `1 2 1 0 8 0 0`;
    - «курс: 1 $ = 12 108 сум»;
    - `factRows()` — `[["VISA 1111 UZS", "1 210 800"], ["VISA 3333 USD", "−100"]]`;
    - «Записать обмен» → `entry.amount: "1210800"`, `entry.to_amount: "100"`, `head` — «✓ Записан обмен».
  - `clears the sum of the card that changed` — `show("обмен валюты")`:
    - Куда VISA 3333 USD, `1 0 0`, «Списано» `1 2 1 0 8 0 0`;
    - Куда → HUMO 2222: в кнопке `/^Сумма/` — «1 210 800» (`within(...).getByText`);
    - Куда → VISA 3333 USD: в `/^Зачислено/` — «0», поле активно, таблицы нет.
  - `credits money from someone else to one card`:
    - Откуда → «не моя карта», Куда HUMO 2222, `3 0 0 000`;
    - `factRows()` — `[["HUMO 2222 UZS", "−300 000"]]`;
    - «Зачислить» → `entry` `toEqual({ entry_id: "e1", source: "HUMO 2222 UZS", incoming: true, category: "💳 СЧЕТА", subcategory: "переводы", amount: "300000", comment: "" })`;
    - `head` — «✓ Зачислено 300 000 UZS — в таблицу −300 000», `card` — «HUMO 2222 UZS».
  - `sends the comment and the date`:
    - «на отпуск» в «Комментарий»;
    - `fireEvent.change` даты на «2026-10-09»;
    - в `entry` — `comment: "на отпуск"`, `day: "2026-10-09"`.
  - `uses Telegram's main button and holds it while the card window is open` (`inTelegram()`, `afterEach` → `vi.unstubAllGlobals()`):
    - `main.show` вызван, `main.setText` — «Записать перевод»;
    - выключена после `hideProgress`;
    - Куда HUMO 2222, `5 0 0 000` → `lastCall(main.enable) > lastCall(main.disable)`;
    - открыть «Куда» → снова `disable` последним;
    - выбрать ту же HUMO 2222 (сумма не стирается) → `enable`;
    - `tapPanel(main)` → `onSubmit` вызван;
    - `rerender` с `busy` → `showProgress(false)`;
    - `unmount` → `main.hide`.
  - `writes the card fields big and bold`:
    - у названия карты в «Откуда» — `text-fact font-extrabold`;
    - у подписи — `text-line font-bold`;
    - у «›» — `text-link`.
- [ ] **Step 2:** `cd webapp && npx vitest run src/entry`. Ожидается FAIL: модуля `TransferScreen` нет. Прежние тесты `EntryScreen` зелёные.
- [ ] **Step 3: реализовать** по Interfaces: сначала вынос `EntryFields` (прогнать `EntryScreen.test.tsx`), затем экран.
- [ ] **Step 4:** `cd webapp && npm test && npm run lint && npm run build`. Ожидается PASS.
- [ ] **Step 5: commit** `feat: the «Откуда → Куда» screen`

### Task 11: «💳 СЧЕТА» → «переводы» открывает перевод

**Files:**
- Modify `webapp/src/entry/EntryScreen.tsx`:
  - `Step` (:55);
  - `CATALOG_FIELDS` (:50);
  - `chooseSub`, `back`;
  - `SourceChips`;
  - шаг `transfer`;
  - `send`;
  - строка «Уже записано» (458–462).
- Modify `webapp/src/mocks/api.ts` — `devBootstrap` для `npm run dev`.
- Test: `webapp/src/entry/EntryScreen.test.tsx`.

**Interfaces:**
- Consumes:
  - Task 3 — `send`, `ALL_GROUPS`;
  - Task 8 — `isTransfer`;
  - Task 10 — `TransferScreen`, `Outgoing.card`.
- Produces:
  - `type Step = "group" | "sub" | "amount" | "transfer"`.
  - `chooseSub(next)` — `isTransfer(group.name, next.name)` ведёт на `"transfer"`, иначе на `"amount"`. Новый `entryId` — как сейчас.
  - `back` — с `"amount"` и `"transfer"` на `"sub"`.
  - Шаг `transfer`:
    - `Header parts={[group.name, \`${sub.icon} ${sub.name}\`]} onBack={tg ? null : back}`;
    - `<TransferScreen key={entryId} … current={source} cards={dashboard?.cards ?? null} today={boot.today} busy={sending !== null} onSubmit={send} />`;
    - `SourceChips` на этом шаге не рисуется.
  - `send` при успехе — `if (out.card) setPicked(out.card)`.
  - `CATALOG_FIELDS = ["source", "to_source", "category", "subcategory"]`.
  - Повтор — `` `✅ Уже записано: ${first === last ? `строка ${first}` : `строки ${first}–${last}`}` ``.
  - `mocks/api.ts`: `bootstrap: () => later(devBootstrap)`:
    - `devBootstrap` — `bootstrapFixture` с картами VISA 1111 UZS (по умолчанию), HUMO 4444 UZS, VISA 2222 USD, VISA 6666 RUB — как в «Остатках» `dashboardFixture`, у HUMO 4444 остатка нет;
    - группы — `bootstrapFixture.groups` + «💳 СЧЕТА» и «💰 ДОХОДЫ», как в `ALL_GROUPS`.

    Тесты берут `fakeApi`, их это не трогает.

- [ ] **Step 1: тесты** `EntryScreen.test.tsx`, справочник `ALL_GROUPS`. Помощник `fillTransfer(user)`: «СЧЕТА» → «переводы» → Куда HUMO 2222 UZS → `5 0 0 000`.
  - `opens «Откуда → Куда» only for «переводы» and «обмен валюты»`:
    - после «СЧЕТА» → «переводы»:
      - заголовок `heading` «💳 СЧЕТА · 🔄 переводы»;
      - кнопка `/^Откуда/`;
      - нет `radiogroup` «Карта» и кнопки «Списать»;
    - «← Назад» → плитки подкатегорий;
    - «выравнивание» → «Зачислить» и «Списать».
  - `writes a transfer, shows it and makes «Куда» the card` — `fillTransfer`, «Записать перевод»:
    - `sent(api)` содержит `{ source: "VISA 9090 UZS", to_source: "HUMO 2222 UZS", category: "💳 СЧЕТА", subcategory: "переводы", amount: "500000" }`, без `to_amount`;
    - «✓ Записан перевод»;
    - `radio` «HUMO 2222 UZS» `checked`.
  - `says «Уже записано: строки N–M» for a repeated transfer` — `addExpense` отвечает `{ status: "duplicate", rows: { first: 4169, last: 4170 }, lines: [] }` → «✅ Уже записано: строки 4169–4170».
  - `asks to choose again when the second card left the catalog` — 422 `{ to_source: "…" }`:
    - «Справочник обновился — выберите заново»;
    - `onCatalogChanged` вызван один раз;
    - видна плитка «ЕДА».
  - `keeps a transfer on the phone with its cards` — дата 2026-10-10T12:00Z, нет связи → в очереди «переводы 500 000 UZS → HUMO 2222 UZS, сегодня».
- [ ] **Step 2:** `cd webapp && npx vitest run src/entry/EntryScreen.test.tsx`. Ожидается FAIL: после «переводы» открывается шаг суммы.
- [ ] **Step 3: реализовать** по Interfaces.
- [ ] **Step 4:** `cd webapp && npm test && npm run lint && npm run build`; весь набор Python. Ожидается PASS.
- [ ] **Step 5: commit** `feat: «💳 СЧЕТА» → «переводы» opens «Откуда → Куда»`

### Task 12: документы, снимки, выкладка части 2

**Files:**
- Modify `README.md`, абзац Mini App, после предложения части 1: ««💳 СЧЕТА» → «переводы» or «обмен валюты» opens «Откуда → Куда» instead of the sum: the current card and the one to choose, each with its balance from «Остатки на картах» (a tap opens the list of the catalog's cards; «Откуда» also has «не моя карта» — money from other people), one sum for cards of one currency or «Списано» and «Зачислено» as in the bank with the rate to check («курс: 1 $ = 12 108 сум»), and the table «карта | в таблицу». «Записать перевод» / «Записать обмен» writes both rows in one write — the withdrawal with a plus, the top-up with a minus, commented like SMS and statements («перевод VISA 1111 → HUMO 2222 (свой)»), one undo for both, «Куда» becomes the current card; «не моя карта» writes one «Зачислить» row on «Куда». A statement with the same sums does not write the pair again.»
- Modify `CLAUDE.md`:
  - дерево — после `statements.py`: `├── transfers.py         # Mini App «Откуда → Куда»: transfer_rows — the withdrawal and the top-up of a transfer between own cards, comments like SMS and statements`;
  - строка `api.py` — к `/api/expenses`: «; `to_source`/`to_amount` — a transfer between own cards in «💳 СЧЕТА»: `transfers.transfer_rows`, two rows of one write»;
  - строка `webapp/`, `src/entry`: «; TransferScreen — «Откуда → Куда» after «💳 СЧЕТА» → «переводы»/«обмен валюты» (CardSheet — the card window, transfer.ts — the rate and the rows «в таблицу», EntryFields — the sum, comment and date shared with the sum step)»;
  - Mini App entry: «→ `domain.entry_row`» → «→ `domain.entry_row` (a transfer — `transfers.transfer_rows`, two rows)».

- [ ] **Step 1:** документы по списку. Commit `docs: a transfer between own cards in one entry`.
- [ ] **Step 2: снимки** на моках (`npm run dev`, `devBootstrap`). Размеры — 360 px в тёмной теме и 320 px с ×1,25. Что снимать:
  - «💳 СЧЕТА → переводы»: VISA 1111 → HUMO 4444, 500 000, таблица;
  - «обмен валюты»: VISA 1111 → VISA 2222 USD, 1 210 800 и 100, курс;
  - окно «Откуда» с «не моя карта».

  Expected:
  - длинные названия карт переносятся, числа целиком;
  - активное поле видно по рамке и «— вводите»;
  - таблица и строка курса не уходят за край;
  - без горизонтальной прокрутки.
- [ ] **Step 3: PR** (diff проверен grep-ом из Global Constraints), ревью, мерж. Выкладка: `/health` отвечает 200, в бандле `/app/` есть «Записать перевод». Настоящий перевод, «Отменить» и «не моя карта» делает владелец.
