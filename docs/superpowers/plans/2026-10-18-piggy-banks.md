# Копилки и вкладки «Сводки» — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Этап 1 — «Сводка» по вкладкам «Сегодня», «Траты», «Накопления», «Впереди»; выбранная вкладка запоминается на телефоне. Этапы 2 (копилки) и 3 (бот и план) дописываются сюда перед началом каждого.

**Architecture:** Только приложение. `webapp/src/dashboard/section.ts` — какие бывают вкладки и выбранная в `localStorage`; `SectionSwitch.tsx` — переключатель, как «Вид» в «Прогнозе»; `DashboardScreen.tsx` показывает карточки выбранной вкладки из того же ответа `GET /api/dashboard`; «Впереди» уходит в свой `Ahead.tsx`. Сервер не меняется.

**Tech Stack:** React 19.3, TypeScript 6.0 (strict, `noUncheckedIndexedAccess`), Vite 8.3, Tailwind 4.3, Vitest 5.0, Testing Library.

**Spec:** `docs/superpowers/specs/2026-10-17-piggy-banks-design.md` (этап 1 — раздел 6.1 и 9, п. 1).

## Global Constraints

- Репозиторий публичный: в коде, тестах, фикстурах и документах — только условные суммы и названия.
- Фронтенд: функциональные компоненты и хуки, TypeScript strict, без `any` и `!`; в `webapp/` перед коммитом `npx prettier --write src && npm run lint && npm test && npm run build`. В этой копии хук `rtk` перехватывает `vitest`, `eslint` и `prettier` и сокращает их вывод: гонять через `rtk proxy …` и смотреть код выхода; отчёт в `webapp/.vitest/` удалить, если появился.
- Вкладки — дословно и в этом порядке: «Сегодня», «Траты», «Накопления», «Впереди»; группа переключателя — `role="radiogroup"` с подписью «Раздел сводки», вкладка — `role="radio"` с `aria-checked`.
- Выбранная вкладка — `bg-bg font-semibold shadow-sm`, остальные — `font-medium text-hint` (высокий контраст: выбор виден не только цветом); новых цветов нет.
- Память телефона: ключ `dashboard-section`, значение — `today`, `spending`, `savings` или `ahead` строкой; нет, испорчено или память закрыта — «Сегодня»; чтение и запись — в `try/catch`.
- Тексты: пустая «Впереди» — «До конца месяца платежей и подписок нет»; без блока прогноза — прежняя заметка «Во вкладке месяца нет блока прогноза — лимит на день не посчитать.»; без вкладки месяца — прежнее «Вкладки месяца нет — бот создаёт её 1-го числа.» без переключателя.
- Коммиты — conventional commits, в конце `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; тело PR заканчивается `🤖 Generated with [Claude Code](https://claude.com/claude-code)`; мерж — `gh pr merge --merge --delete-branch`.

## Review Focus

1. **В памяти телефона значение от старой сборки или мусор** (`calendar`, пусто, JSON) — открывается «Сегодня», ничего не падает (тест в Task 1).
2. **Память телефона закрыта** (приватный режим, запрет сайта) — вкладки переключаются, выбор живёт до закрытия приложения (тест в Task 1).
3. **Ответ без блока прогноза** — «Сегодня» объясняет, почему нет лимита; «Впереди» не говорит «платежей нет», когда список просто не прочитать (тест в Task 2).
4. **«Обновить» и свежий ответ** не сбрасывают выбранную вкладку на «Сегодня» (тест в Task 2).
5. **Узкий телефон (360 px)** — четыре вкладки в одну строку, «Накопления» не обрезана (проверка в превью, Task 2).

---

## Этап 1 — «Сводка» по вкладкам

### Task 1: Вкладки и выбранная в памяти телефона

**Files:**
- Create: `webapp/src/dashboard/section.ts`, `webapp/src/dashboard/SectionSwitch.tsx`
- Test: `webapp/src/dashboard/section.test.ts`, `webapp/src/dashboard/SectionSwitch.test.tsx`

**Interfaces:**
- Produces:
  - `export type Section = "today" | "spending" | "savings" | "ahead";`
  - `export const SECTIONS: readonly { name: Section; title: string }[]` — в порядке «Сегодня», «Траты», «Накопления», «Впереди»;
  - `export const SECTION_KEY = "dashboard-section";`
  - `export function storedSection(): Section` — из `localStorage`, иначе `"today"`;
  - `export function storeSection(section: Section): void` — ошибки памяти глотает;
  - `export default function SectionSwitch(props: { section: Section; onSection: (section: Section) => void })`.

- [ ] **Step 1: Write the failing tests**

`section.test.ts` (`afterEach(() => localStorage.clear())`):

```ts
it("starts on «Сегодня» when nothing is stored", () => {
  expect(storedSection()).toBe("today");
});

it("reads the stored section", () => {
  localStorage.setItem(SECTION_KEY, "savings");
  expect(storedSection()).toBe("savings");
});

it.each(["calendar", "", '{"scope":"UZS"}'])(
  "ignores %j from another build",
  (value) => {
    localStorage.setItem(SECTION_KEY, value);
    expect(storedSection()).toBe("today");
  },
);

it("keeps the choice for the next launch", () => {
  storeSection("ahead");
  expect(localStorage.getItem(SECTION_KEY)).toBe("ahead");
});

it("does without the phone memory when the browser closes it", () => {
  const closed = vi
    .spyOn(window, "localStorage", "get")
    .mockImplementation(() => {
      throw new Error("denied");
    });
  expect(storedSection()).toBe("today");
  expect(() => storeSection("spending")).not.toThrow();
  closed.mockRestore();
});
```

`SectionSwitch.test.tsx`:

```tsx
it("lists the sections and marks the chosen one", () => {
  render(<SectionSwitch section="today" onSection={vi.fn()} />);
  const group = screen.getByRole("radiogroup", { name: "Раздел сводки" });
  const radios = within(group).getAllByRole("radio");
  expect(radios.map((radio) => radio.textContent)).toEqual([
    "Сегодня",
    "Траты",
    "Накопления",
    "Впереди",
  ]);
  const today = screen.getByRole("radio", { name: "Сегодня" });
  expect(today.getAttribute("aria-checked")).toBe("true");
  // выбор виден не только цветом
  expect(today.className).toContain("font-semibold");
  expect(
    screen.getByRole("radio", { name: "Траты" }).getAttribute("aria-checked"),
  ).toBe("false");
});

it("tells which section was pressed", async () => {
  const onSection = vi.fn();
  const user = userEvent.setup();
  render(<SectionSwitch section="today" onSection={onSection} />);
  await user.click(screen.getByRole("radio", { name: "Накопления" }));
  expect(onSection).toHaveBeenCalledWith("savings");
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd webapp && rtk proxy npx vitest run src/dashboard/section.test.ts src/dashboard/SectionSwitch.test.tsx`
Expected: FAIL — `section.ts` и `SectionSwitch.tsx` ещё нет.

- [ ] **Step 3: Implement `section.ts` and `SectionSwitch.tsx`**

`storedSection` принимает только имена из `SECTIONS`. Переключатель — как `Switches` в `forecast/ForecastScreen.tsx`: контейнер `flex gap-1 rounded-xl bg-section p-1`, кнопки `flex-auto whitespace-nowrap rounded-lg px-2 py-1.5 text-sm` плюс классы выбора из Global Constraints — ширина по подписи, чтобы «Накопления» влезла на 360 px.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd webapp && rtk proxy npx vitest run src/dashboard/section.test.ts src/dashboard/SectionSwitch.test.tsx`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add webapp/src/dashboard/section.ts webapp/src/dashboard/section.test.ts webapp/src/dashboard/SectionSwitch.tsx webapp/src/dashboard/SectionSwitch.test.tsx
git commit -m "feat(webapp): «Сводка» sections and the chosen one on the phone"
```

### Task 2: «Сводка» по вкладкам

**Files:**
- Create: `webapp/src/dashboard/Ahead.tsx`
- Modify: `webapp/src/dashboard/DashboardScreen.tsx`, `README.md` (абзац «Mini App», строка про «Сводку», и абзац о заморозке), `CLAUDE.md` (строка `webapp/`)
- Test: `webapp/src/dashboard/DashboardScreen.test.tsx`

**Interfaces:**
- Consumes: Task 1 — `Section`, `storedSection`, `storeSection`, `SectionSwitch`.
- Produces: `export default function Ahead({ data }: { data: Dashboard })` — «Впереди» и «Подписки» (со своим `STATE_TEXT`), заметка без блока прогноза или «До конца месяца платежей и подписок нет».

Раскладка `DashboardScreen` (состояние — `useState<Section>(storedSection)`, выбор — `setSection` и `storeSection`):

| Вкладка | Что | Без блока прогноза |
|---|---|---|
| `today` | `Today`, `Cards`, `Sums`, `CardBalances` | сверху заметка |
| `spending` | `CategoryChart`, «Траты по дням» | как есть |
| `savings` | `CushionCard`, `SavingsCard` | как есть |
| `ahead` | `Ahead` | заметка вместо списков |

Заголовок и «Обновить» — над переключателем; скелет и ошибка — как сейчас; `no_month_tab` — только сообщение, без переключателя.

- [ ] **Step 1: Rewrite the screen tests for sections**

В `DashboardScreen.test.tsx`: `afterEach(() => localStorage.clear())` и помощник

```ts
async function show(user: ReturnType<typeof userEvent.setup>, name: string) {
  await user.click(await screen.findByRole("radio", { name }));
}
```

Прежние проверки переезжают на свои вкладки (суммы и тексты фикстуры — прежние):

- «shows what is left today, the plan by groups and the chart» делится на три теста:
  - `"opens on «Сегодня» with what is left today"` — «Можно сегодня», «380 000», `/лимит 500 000 · потрачено 120 000/`, `aria-checked="true"` у «Сегодня», `queryByRole("region", { name: "План-факт по категориям" })` — `null`;
  - `"«Траты» shows the plan by groups and the daily chart"` — после `show(user, "Траты")`: `[data-status="over"]`, картинка «Траты по дням: 9 дней, сверх лимита — 2», «за 9 дней при плане 4,05 млн», «запас 1,10 млн», «можно 500 000 в день», «план 450 000 в день»; «Можно сегодня» — `null`;
  - `"«Впереди» shows the list and the subscriptions"` — «−380,00 USD» с классом `text-payment`, `/ждём/`.
- «puts the daily spending right after the plan vs fact chart» → `"each section has its own cards"`: заголовки `h2` по вкладкам — «Сегодня» `["Остатки на картах"]`, «Траты» `["План-факт по категориям", "Траты по дням"]`, «Накопления» `["🛟 Подушка безопасности", "Норма сбережений"]`, «Впереди» `["Впереди", "Подписки"]`.
- На «Траты» перед проверками: «says how far the finished days went over the plan», «says when the finished days match the plan exactly», «opens a group to show its subcategories», вторая половина «shows the cards now and where the money went this month» (регион «План-факт по категориям») и «лимита нет» в «says there is no limit today when the month is behind the plan».
- На «Впереди»: «marks a row of the list that is late».
- «keeps the cards and the chart without the forecast block»: «Сегодня» — регион «Остатки на картах» и заметка «Во вкладке месяца нет блока прогноза — лимит на день не посчитать.»; «Траты» — регион «План-факт по категориям»; «Впереди» — та же заметка, «До конца месяца платежей и подписок нет» — `null`.
- «explains a missing month tab» — ещё `queryByRole("radiogroup", { name: "Раздел сводки" })` — `null`.

Новые тесты:

```tsx
it("switches sections and keeps the choice for the next launch", async () => {
  const user = userEvent.setup();
  const first = render(<DashboardScreen api={fakeApi()} />);
  await show(user, "Накопления");
  expect(screen.getByRole("heading", { name: "🛟 Подушка безопасности" })).toBeTruthy();
  expect(screen.queryByText("Можно сегодня")).toBeNull();
  expect(localStorage.getItem("dashboard-section")).toBe("savings");
  first.unmount();
  render(<DashboardScreen api={fakeApi()} />);
  const savings = await screen.findByRole("radio", { name: "Накопления" });
  expect(savings.getAttribute("aria-checked")).toBe("true");
});

it("keeps the chosen section when the numbers are read anew", async () => {
  const user = userEvent.setup();
  render(<DashboardScreen api={fakeApi()} />);
  await show(user, "Траты");
  await user.click(screen.getByRole("button", { name: "Обновить" }));
  expect(
    await screen.findByRole("region", { name: "План-факт по категориям" }),
  ).toBeTruthy();
  expect(
    screen.getByRole("radio", { name: "Траты" }).getAttribute("aria-checked"),
  ).toBe("true");
});

it("«Впереди» says when nothing is ahead", async () => {
  const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
    ...dashboardFixture,
    upcoming: [],
    subscriptions: [],
  });
  const user = userEvent.setup();
  render(<DashboardScreen api={fakeApi({ dashboard })} />);
  await show(user, "Впереди");
  expect(screen.getByText("До конца месяца платежей и подписок нет")).toBeTruthy();
});
```

- [ ] **Step 2: Run the screen tests to verify they fail**

Run: `cd webapp && rtk proxy npx vitest run src/dashboard/DashboardScreen.test.tsx`
Expected: FAIL — переключателя «Раздел сводки» на экране ещё нет.

- [ ] **Step 3: Split the screen into sections**

`Content` становится разводкой по вкладкам (таблица выше); список «Впереди» и «Подписки» с `STATE_TEXT` переезжают в `Ahead.tsx` без изменений вида.

- [ ] **Step 4: Run the screen tests to verify they pass**

Run: `cd webapp && rtk proxy npx vitest run src/dashboard src/App.test.tsx`
Expected: PASS; `App.test.tsx` не меняется — по умолчанию открыта «Сегодня».

- [ ] **Step 5: Update the docs**

`README.md`, абзац «Mini App»: «Сводка» — четыре вкладки, что на какой (как в таблице выше; «Накопления» — подушка и норма), выбранная запоминается на телефоне; абзац о заморозке — «Сводка» → вкладка «Накопления». `CLAUDE.md`, строка `webapp/`: `src/dashboard` — «Сводка» по вкладкам, `section.ts` — выбранная вкладка в `localStorage` (`dashboard-section`).

- [ ] **Step 6: Check the whole app and a narrow phone**

Run: `cd webapp && rtk proxy npx prettier --write src && rtk proxy npm run lint && rtk proxy npm test && npm run build`
Expected: все тесты зелёные, lint и build без ошибок.

Превью (`npm run dev` — моки) на ширине 360 px: четыре вкладки в одну строку, «Накопления» видна целиком, выбранная — жирная на светлой и тёмной теме.

- [ ] **Step 7: Commit**

```bash
git add webapp/src/dashboard README.md CLAUDE.md
git commit -m "feat(webapp): «Сводка» in four sections"
```

---

## Этап 2 — копилки

**Goal:** Копилки на листах «Копилки» и «Журнал копилок», их остатки, траты по статье, план взносов, 10 % и подушка за вычетом копилок — во вкладке «Накопления» с окнами копилки и «Отложил…»; бот фоном пишет на лист «Потрачено», «В этом месяце», итоги окон и новые сроки.

**Architecture:** `app/services/piggy.py` — без сети: разбор листов (`Book`), окна и траты из `fact` (`balances`), план взносов, 10 % и подушка (`savings_screen`), раскладка «Отложил…» (`distribute`). `GoogleSheetsService` читает оба листа через `get_values` (копии и их сброс — как у остальных листов) и пишет строки списка и журнала. `GET /api/savings` собирает экран тем же путём, что «Сводка»; правки — `POST/PUT /api/piggy…` под своим замком, с заметкой второму и флагом фоновой записи. Фоновая запись — `app/services/piggy_sync.py` из той же задачи, что «Пришло к утру». Подушка и норма уходят из ответа «Сводки» в ответ «Накоплений».

**Spec:** разделы 3, 4.1–4.6, 4.8, 5, 6.2–6.4 и 9 п. 2 (без 4.7 — это этап 3).

### Global Constraints этапа 2

- Python: type hints на всех сигнатурах, black + ruff; из корня репозитория `black . && ruff check . && pytest -q` (в этой копии — `../venv_dev/bin/…`).
- Листы (спека 3; имена — `config.PIGGY_SHEET_NAME = "Копилки"`, `config.PIGGY_JOURNAL_NAME = "Журнал копилок"`):
  - «Копилки» A:N — `ID | Копилка | Цель | Валюта | Срок | Повтор | Группа | Подкатегория | Отложено | Потрачено | Осталось | В этом месяце | Кто | Закрыта`;
  - «Журнал копилок» A:H — `Дата | ID | Копилка | Сумма | Валюта | Что | Кто | Ключ`. **Ruling:** в журнал добавлена колонка «Валюта» (E) — без неё смену валюты копилки руками (спека 3.3) не заметить. Формула I — по колонкам D и B журнала.
  - Значения — `RAW`, дата — текстом «ДД.ММ.ГГГГ»; формулы I и K — `USER_ENTERED`, разделитель `;`, без номера строки:
    `=SUMIFS('Журнал копилок'!$D:$D;'Журнал копилок'!$B:$B;INDEX($A:$A;ROW());'Журнал копилок'!$D:$D;">0")` и `=INDEX($I:$I;ROW())-INDEX($J:$J;ROW())`.
  - Строки бот не удаляет. ID — тем же генератором, что у «Календаря» (6 символов, первый — буква).
- Повтор — `один раз` / `каждый год` (без учёта регистра); «Что» — `отложил`, `потратил`, `потрачено в окне`, `списал остаток`, `закрыта`, `исправление`, `не из копилки`; ключ итога окна — `окно:<ДД.ММ.ГГГГ>`; ручная отметка — слова «не из копилки» в комментарии строки `fact` без учёта регистра.
- Окно трат: «каждый год» — [срок − 60 дней; срок + 30]; «один раз» — [день первой строки журнала копилки; срок + 30].
- План: на экране 12 месяцев с текущего календарного; сроки «каждый год» — на 24 месяца вперёд; суммы — вверх: сумы до 1 000, доллары и рубли до 1. **Ruling:** день денег для всех месяцев плана — по списку текущей вкладки (спека позволяет для месяцев без вкладки; скрытая вкладка следующего месяца не читается — лишний запрос ради редкого расхождения).
- 10 % — `config.SAVE_SHARE`; подушка на 1-е < 1 месяца — вся доля в подушку; от 1 до `config.CUSHION_MONTHS` — пополам; от `CUSHION_MONTHS` — вся долгим целям.
- Подушка — «Заморожено» − Σ остатков копилок на вечер вчера в валюте заморозки по курсу таблицы; меньше нуля — 0 и `over`. **Ruling:** `CushionOut` сохраняет нынешние поля (`uzs`, `monthly`, `short_uzs` нужны `CushionCard`) и получает `frozen`, `piggies`, `over` — спека 5.1 их не перечисляла.
- Тексты API (спека 5.3) и экрана (6.2–6.4) — дословно. Заметки второму — без рода, как у календаря: «🐷 Иван — новая копилка: …», «🐷 Иван — копилка изменена: …», «🐷 Иван — копилка закрыта: Новый год (остаток — в подушку)», «🐷 Иван — отложил: Новый год 6 000 000 UZS, Отпуск 210 USD», «🐷 Иван — не из копилки: 18.12 ПРАЗДНИК / сезонный 2 400 000».

### Review Focus этапа 2

1. **Лист правили руками** — удалили или сдвинули строку списка, вписали срок и текстом, и датой, сменили валюту при непустом журнале: деньги копилки не теряются, битая строка видна с номером (тесты в Task 3 и Task 6).
2. **Одна трата на две копилки одной статьи и возврат в окне** — не списывается дважды, остаток не уходит ниже нуля (тесты в Task 4).
3. **Взнос 31-го после зарплаты 30-го** — идёт в план своего месяца; второе «Отложил всё» ничего не задваивает (тесты в Task 5 и Task 7).
4. **Двое одновременно заводят первую копилку** — листы создаются один раз, ни одна строка не теряется (тест в Task 7).
5. **Фоновая запись упала посередине** (итог окна записан, срок не перенесён) — следующий запуск доделывает, итог не пишется дважды (тест в Task 8).

---

### Task 3: Разбор листов копилок

**Files:**
- Create: `app/services/piggy.py`, `tests/test_piggy.py`
- Modify: `app/config.py` (имена листов рядом с `CALENDAR_SHEET_NAME`)

**Interfaces:**
- Produces (`app/services/piggy.py`):
  - `LIST_HEADER: list[str]`, `JOURNAL_HEADER: list[str]` — шапки из Global Constraints;
  - `SAVED, SPENT, WINDOW, WRITE_OFF, CLOSED, FIX, NOT_FROM` — строки «Что»;
  - `@dataclass(frozen=True) class Piggy: id: str; row: int; name: str; target: float | None; currency: str; deadline: date | None; yearly: bool; group: str | None; subcategory: str | None; author: str; closed: date | None; error: str | None = None` — `row` — номер строки листа, `error` — что не так (для лога и признака битой строки);
  - `@dataclass(frozen=True) class Move: day: date; piggy_id: str; name: str; amount: float; currency: str; kind: str; who: str; key: str`;
  - `@dataclass(frozen=True) class Book: piggies: tuple[Piggy, ...]; moves: tuple[Move, ...]` и метод `moves_of(self, piggy_id: str) -> tuple[Move, ...]`;
  - `def read_book(list_rows: list[list[Any]], journal_rows: list[list[Any]], currencies: Collection[str], groups: Collection[str], subcategories: Mapping[str, Sequence[str]]) -> Book`.

- [ ] **Step 1: Write the failing tests** — `tests/test_piggy.py`, строки листов — списками ячеек с шапкой, даты — и текстом, и серийным числом (`to_day` из `analytics_service`):
  - `test_reads_a_piggy_and_its_moves` — строка `["p3k9za", "Новый год", 12000000, "UZS", "20.12.2026", "каждый год", "🎉 ПРАЗДНИК", "сезонный", "", "", "", "", "Иван", ""]` даёт `Piggy(id="p3k9za", row=2, target=12_000_000.0, deadline=date(2026, 12, 20), yearly=True, …, error=None)`; две строки журнала — два `Move` с суммой, валютой и «Что».
  - `test_long_goal_has_no_deadline_and_maybe_no_target` — пустые C, E, F, G, H: `target is None`, `deadline is None`, `yearly is False`, `error is None`.
  - `test_broken_rows_keep_their_row_number` (параметры): нет названия; валюта `EUR` не из карт; цель `"abc"`; цель `0`; срок `"завтра"`; повтор `"раз в год"`; «каждый год» без срока; группа не из справочника; группа «💰 ДОХОДЫ»; подкатегория чужой группы — `error` не `None`, `row` — номер строки.
  - `test_a_new_currency_with_moves_is_broken` — валюта в списке `USD`, в журнале строки копилки в `UZS` → `error`.
  - `test_a_copied_id_keeps_the_first_row` — у двух строк один ID: в `piggies` первая, вторая — в лог (`caplog`).
  - `test_moves_of_an_unknown_piggy_are_left_out` — строка журнала с ID без копилки не попадает в `moves_of` ни одной копилки и пишется в лог.
  - `test_a_closed_piggy_is_marked` — дата в N → `closed`.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy.py -q` — FAIL: модуля нет.
- [ ] **Step 3:** `read_book` — строка 1 листа пропускается; ячейки обрезаются; сумма и цель — числа таблицы или текст с запятой; строка журнала без даты, ID или суммы пропускается с `logger.warning`.
- [ ] **Step 4:** тот же запуск — PASS.
- [ ] **Step 5:** `black . && ruff check . && pytest -q`; commit `feat(piggy): read the piggy banks and their journal`.

### Task 4: Траты из копилки и остатки

**Files:** Modify `app/services/piggy.py`; Test `tests/test_piggy.py`

**Interfaces:**
- Consumes: Task 3 — `Piggy`, `Move`, `Book`.
- Produces:
  - `def fact_key(day: date, group: str, subcategory: str, amount: float, currency: str, source: str, comment: str) -> str` — комментарий без префикса «AI:» и пробелов по краям, сумма — до копеек;
  - `def row_keys(frame: pd.DataFrame) -> list[str]` — ключи строк фрейма по порядку, у повторов — `#2`, `#3`…;
  - `def fingerprint(key: str) -> str` — первые 10 знаков `sha1` (для кнопки в чате, этап 3);
  - `@dataclass(frozen=True) class Window: start: date; end: date; deadline: date`;
  - `def open_window(piggy: Piggy, moves: Sequence[Move]) -> Window | None` — окно нынешнего срока, если у копилки есть срок и статья, а итога этого окна (`окно:<срок>`) в журнале нет;
  - `@dataclass(frozen=True) class Spend: key: str; day: date; group: str; subcategory: str; amount: float` — сумма в валюте копилки;
  - `@dataclass(frozen=True) class Balance: saved: float; spent: float; left: float; collected: float; spends: tuple[Spend, ...]`;
  - `def balances(book: Book, frame: pd.DataFrame, rates: Rates, until: date) -> dict[str, Balance]` — по копилкам, у которых есть ID.

Правила `balances` (спека 4.2, 4.6):
- журнал — строки по `until`; `saved` — сумма плюсовых, `spent` — минус сумма минусовых плюс траты открытого окна, `left = saved − spent`;
- траты окна — `expenses(frame)` той же группы и подкатегории, день в окне и не позже `until`, без отмеченных (ключ строки — в журнале с «не из копилки» у любой копилки, или в комментарии «не из копилки»), сумма — `rates.convert` в валюту копилки (нет курса — строка пропускается);
- строки идут по дням; строка достаётся копилке, в окне которой её день, с ближайшим к дню сроком (при равенстве — более ранний); у копилки кончился остаток (сумма её журнала по `until`) — остаток строки идёт следующей, потом — в обычные траты; возврат уменьшает траты той же копилки, итог окна не меньше нуля;
- `collected = left` + траты открытого окна + минусовые «потратил» и «списал остаток», чей день внутри открытого окна.

- [ ] **Step 1: Write the failing tests** — фрейм — через `transactions_frame([FACT_HEADER, *rows], RATES)` и `fact_row` из `tests/test_day_budget.py`, как в `tests/test_savings.py`:
  - `test_spending_in_the_window_comes_from_the_piggy` — «Новый год» 12 млн отложено, трата «сезонный» 2,4 млн 18.12: `spent == 2_400_000`, `left == 9_600_000`, `collected == 12_000_000`;
  - `test_spending_outside_the_window_is_ordinary` — та же трата 01.09 → `spent == 0`;
  - `test_spending_over_the_balance_is_ordinary` — отложено 1 млн, трата 2,4 млн → `spent == 1_000_000`, `left == 0`;
  - `test_a_refund_lowers_the_spending` — трата 2,4 млн и возврат −0,4 млн → `spent == 2_000_000`;
  - `test_one_article_two_piggies_never_twice` — две копилки «день рождения» со сроками 10.03 и 20.08, трата 05.03 на 3 млн при остатках 2 млн и 5 млн → первой 2 млн, второй 1 млн; сумма списаний равна трате;
  - `test_marked_spending_is_not_from_the_piggy` — ключ строки в журнале с «не из копилки» → 0; то же для «не из копилки» в комментарии;
  - `test_another_currency_by_the_table_rate` — копилка в USD, трата в сумах 1 200 000 при курсе 12 000 → `spent == 100.0`;
  - `test_a_frozen_window_is_not_counted_again` — строка журнала «потрачено в окне» −2,4 млн с ключом `окно:20.12.2026` и та же трата → `spent == 2_400_000` один раз;
  - `test_a_closed_window_counts_until_it_is_frozen` — сегодня 25.01 (окно закрылось 19.01), итога в журнале ещё нет → трата всё равно в `spent`;
  - `test_collected_keeps_what_was_already_spent` — «Отпуск», «потратил» −10 млн 15.04 при сроке 01.06 → `collected` не уменьшился;
  - `test_fact_keys_number_repeats_and_ignore_ai` — две одинаковые строки → `…` и `…#2`; комментарий «AI: кафе» и «кафе» → один ключ.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy.py -q` — FAIL: новых имён нет.
- [ ] **Step 3:** реализовать по правилам выше.
- [ ] **Step 4:** тот же запуск — PASS.
- [ ] **Step 5:** `black . && ruff check . && pytest -q`; commit `feat(piggy): spending of the article from the piggy bank`.

### Task 5: План взносов, 10 %, подушка и «хватает ли»

**Files:** Modify `app/services/piggy.py`; Test `tests/test_piggy.py`

**Interfaces:**
- Consumes: Task 3–4; `read_forecast` (`app/services/day_budget.py`), `DayBudget`, `Cushion` и `monthly_spending`, `month_savings`, `full_months` (`app/services/savings.py`).
- Produces:
  - `def money_day(rows: list[list[Any]] | None) -> int | None` — день последнего поступления (+) списка вкладки;
  - `@dataclass(frozen=True) class MonthPlan: month: date; total: float; items: dict[str, float]; cushion: float; goals: dict[str, float]; ready: tuple[str, ...]` — в сумах; `items` — копилки со сроком, `goals` — долгие цели;
  - `@dataclass(frozen=True) class PiggyState: piggy: Piggy; balance: Balance; status: str; this_month: float; left_this_month: float; short: float; leftover: float; history: tuple[Move, ...]` — суммы в валюте копилки; `status` — `on_track | late | no_target | done | leftover | goal | broken`;
  - `@dataclass(frozen=True) class Enough: income: float; save: float; life: float; ordinary: float; gap: float`;
  - `@dataclass(frozen=True) class Screen: today: date; piggies: tuple[PiggyState, ...]; plan: tuple[MonthPlan, ...]; cushion: Cushion | None; frozen: float | None; envelopes: float; over: float; share_to: str; enough: Enough | None`;
  - `def savings_screen(book: Book, frame: pd.DataFrame, rows: list[list[Any]] | None, today: date, rates: Rates, budget: DayBudget | None, share: float, cushion_months: int) -> Screen`;
  - `def distribute(screen: Screen, amount: float, currency: str, rates: Rates) -> list[tuple[str, float]]` — пары (ID, сумма в валюте копилки) для «Отложил…».

Правила (спека 4.3–4.5, 4.8):
- первый месяц плана — текущий календарный (**Ruling:** спека п. 2 после разбора проверки — взнос идёт в план своего месяца); месяц успевает к сроку, если его день денег (`money_day`, обрезанный по длине месяца; нет — последний день) не позже срока;
- нужда ближайшего срока — цель минус `collected` на конец прошлого месяца (`balances(..., until=последний день прошлого месяца)`); сроки «каждый год» — до `today + 24 месяца`, у следующих лет — вся цель; без успевающих месяцев — `late`, `short` = нужда;
- сумма месяца — ступеньками (`perf`-проверка в разговоре: пример спеки даёт 6,0 / 6,0 / 5,0 × 6 / 3,5); внутри месяца — ближайший срок первым; `this_month` — доля текущего месяца в валюте копилки, вверх; `left_this_month = max(this_month − сумма плюсовых строк журнала с 1-го числа, 0)`;
- доля 10 % месяца — `share` от поступлений (+) списка вкладки; делится по подушке на 1-е (подушка — «Заморожено» на начало месяца `budget.frozen − budget.frozen_change` минус остатки копилок на конец прошлого месяца, в месяцах `monthly_spending`); долгим целям — поровну, у кого `collected < target` или цели нет; их «осталось» — как у копилок; для следующих месяцев плана подушка растёт на свою часть;
- подушка экрана — `budget.frozen` минус остатки копилок по `until = today − 1 день`, в валюте заморозки; меньше нуля — `Cushion` с 0 и `over`; `envelopes` — сумма остатков;
- статусы: ошибка строки — `broken`; без срока — `goal`; срок без цели — `no_target`; `collected ≥ target` — `done`; «один раз» после окна с остатком — `leftover`; иначе `late` или `on_track`; `leftover` у «каждый год» — остаток после последнего итога окна, пока после него нет ни одного «отложил»;
- «хватает ли» — полные месяцы `full_months(today, 6)` не раньше первого месяца трат; `income` — средний доход, `save` — `items` и `goals` текущего месяца плюс `share × income`, `life = income − save`, `ordinary` — средние траты без статей копилок «каждый год», `gap = life − ordinary`; месяцев нет — `None`;
- `distribute` — копилкам со сроком в порядке экрана их `left_this_month`, потом долгим целям их `left_this_month`, остальное не пишется; суммы — в валюте копилки по курсу таблицы, до копеек.

- [ ] **Step 1: Write the failing tests:**
  - `test_the_spec_example_steps_down` — сегодня 17.10.2026, список: поступления 15-го и 30-го; «Новый год» 12 млн к 20.12 каждый год, «Отпуск» 2 500 USD к 01.06.2027 каждый год, курс 12 000 → `total` по месяцам: октябрь и ноябрь 6 000 000 (всё «Новому году»), декабрь–май 5 000 000 («Отпуску»), июнь 3 500 000;
  - `test_a_deadline_before_any_money_day_is_late` — срок 25.10 при дне денег 30 → `late`, `short` = цель;
  - `test_a_contribution_after_the_money_day_counts_for_its_month` — сегодня 31.10, «отложил» 31.10 на всю долю октября → `left_this_month == 0`, `this_month` не изменился;
  - `test_rounds_up` — доля 5 999 001 → 6 000 000; в USD 210,2 → 211;
  - `test_no_target_asks_for_a_sum` и `test_done_when_collected_reaches_the_target`;
  - `test_the_share_goes_to_the_cushion_under_a_month`, `test_the_share_splits_between_a_month_and_the_target`, `test_the_share_goes_to_goals_from_the_target`, `test_a_goal_at_its_target_passes_its_part`;
  - `test_the_cushion_is_frozen_minus_piggies_as_of_last_night` — «отложил» сегодня в подушку не вычитается; `test_piggies_over_the_frozen_money_warn` — `over > 0`, месяцев 0;
  - `test_enough_compares_money_for_life_with_ordinary_spending` — `gap` по условным цифрам;
  - `test_distribute_by_deadlines_then_goals` — 10 млн при «осталось» 6 млн и 210 USD и долгой цели 1,5 млн → 6 000 000, 210, 1 500 000, остальное не пишется.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy.py -q` — FAIL.
- [ ] **Step 3:** реализовать по правилам выше.
- [ ] **Step 4:** тот же запуск — PASS.
- [ ] **Step 5:** `black . && ruff check . && pytest -q`; commit `feat(piggy): the contribution plan, the 10 % share and the cushion`.

### Task 6: Листы копилок в таблице

**Files:** Modify `app/services/google_sheets.py`; Test `tests/test_piggy_sheet.py`

**Interfaces:**
- Produces (`GoogleSheetsService`):
  - `get_piggy(self, max_age: float = 0.0) -> tuple[list[list[Any]], list[list[Any]]]` — оба листа через `get_values`; листа нет — пустой список;
  - `ensure_piggy_sheets(self) -> None` — создаёт недостающие листы с шапкой (`add_worksheet`, `append_rows([HEADER], RAW)`), кладёт их в `_worksheets`;
  - `add_piggy(self, row: Sequence[Any]) -> int` — строка A:N значениями (`RAW`, `table_range="A:N"`, без повтора), номер строки из `appended_rows`, затем формулы I и K этой строки (`values_batch_update`, `USER_ENTERED`); возвращает номер строки;
  - `update_piggy(self, piggy_id: str, cells: Mapping[str, Any]) -> bool` — строка по ID в колонке A заново, пишутся только переданные колонки (`"B"…"H"`, `"M"`, `"N"`) одним `values_batch_update` `RAW` с повтором; строки нет — `False`;
  - `add_piggy_moves(self, rows: Sequence[Sequence[Any]]) -> None` — строки журнала A:H (`RAW`, `table_range="A:H"`, без повтора).
  - Все записи зовут `_written()`.

- [ ] **Step 1: Write the failing tests** — двойник таблицы со словарём «имя → лист» по образцу `CalendarSpreadsheet` и `CalendarWorksheet` (`tests/test_calendar_sheet.py`), `append_rows` возвращает `{"updates": {"updatedRange": "'Копилки'!A5:N5"}}`:
  - `test_creates_both_sheets_once_with_headers`;
  - `test_a_new_piggy_gets_row_independent_formulas` — после `add_piggy` в `values_batch_update` — I5 и K5 с формулами из Global Constraints, `USER_ENTERED`;
  - `test_update_finds_the_row_by_id_after_a_shift` — строку выше удалили руками → правка ложится на строку с этим ID; ID нет — `False`;
  - `test_moves_are_appended_as_values`;
  - `test_reads_both_sheets_and_keeps_a_copy_until_a_write` — `get_piggy(max_age=60)` второй раз не читает; после `add_piggy_moves` читает заново; листа нет — `([], [])`.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy_sheet.py -q` — FAIL.
- [ ] **Step 3:** реализовать.
- [ ] **Step 4:** тот же запуск — PASS.
- [ ] **Step 5:** `black . && ruff check . && pytest -q`; commit `feat(sheets): the piggy bank sheets`.

### Task 7: API «Накоплений»

**Files:**
- Modify: `app/services/analytics_service.py` (`savings`), `app/services/dashboard.py` и `app/web/api.py`, `app/web/schemas.py` (подушка и норма уходят из «Сводки», места — `dashboard.py:117-118, 258, 270-271, 285-288`, `api.py:360, 429-430`, `schemas.py:263-264`), `tests/test_dashboard.py`, `tests/test_web_api.py`
- Create: `tests/test_web_piggy.py`

**Interfaces:**
- Consumes: Task 3–6.
- Produces:
  - `AnalyticsService.savings(self, today: date | None = None, max_age: float = 0.0) -> piggy.Screen | None` — `read_month_sheets(..., calendar=False)`, `gs.get_piggy(max_age)`, `read_book`, `day_budget`, `savings_screen`; нет вкладки месяца — `None`;
  - схемы `SavingsScreenOut` (поля спеки 5.1 + `rates`), `PiggyOut`, `MonthPlanOut`, `EnoughOut`, `CushionOut` с `frozen`, `piggies`, `over`; входы `PiggyIn` (`entry_id`, `name`, `target`, `currency`, `deadline`, `repeat: Literal["once", "yearly"]`, `group`, `subcategory`), `MoveIn` (`entry_id`, `kind: Literal["saved", "spent", "write_off"]`, `amount`), `SaveIn` (`entry_id`, `amount`, `currency`), `ExcludeIn` (`key`);
  - роуты `GET /api/savings`, `POST /api/piggy`, `PUT /api/piggy/{piggy_id}`, `POST /api/piggy/{piggy_id}/close`, `POST /api/piggy/{piggy_id}/moves`, `POST /api/piggy/save`, `POST /api/piggy/{piggy_id}/exclude`; замок `PIGGY_LOCK = "piggy_lock"`, повторы — `PIGGY_ENTRIES = "piggy_entries"`; каждый ответ — свежий `SavingsScreenOut`; после записи — `mark_dirty(bot_data)` и `notify_others`.

- [ ] **Step 1: Write the failing tests** (`tests/test_web_piggy.py`, двойник аналитики и таблицы — по образцу `tests/test_web_calendar.py`):
  - `test_savings_screen_shape` — копилки, план, подушка, `rates`;
  - `test_create_writes_the_row_and_tells_the_other` — строка списка, заметка второму «🐷 Иван — новая копилка: Новый год 12 000 000 UZS к 20.12, каждый год»;
  - `test_two_first_piggies_create_the_sheets_once` — два одновременных `POST` → листы созданы один раз, обе строки на месте;
  - `test_a_repeated_entry_writes_once`;
  - `test_fields_are_checked` (параметры по спеке 5.3: название, цель, валюта, срок, «каждый год» без срока, статья) — `422` с текстом поля;
  - `test_currency_of_a_piggy_with_moves_cannot_change` — `422` «Валюту не поменять: в копилке есть движения. Закройте её и заведите новую»;
  - `test_spent_more_than_the_piggy_holds` — `422` «В копилке только 1 000 000 UZS»;
  - `test_close_returns_the_rest_to_the_cushion` — N — сегодня, журнал «закрыта» на весь остаток;
  - `test_save_distributes_one_sum`; `test_exclude_marks_a_spending` (журнал «не из копилки», сумма 0, ключ);
  - `test_unknown_piggy_is_404`, `test_sheet_down_is_503`, `test_writes_mark_the_background_sync`;
  - в `tests/test_dashboard.py` и `tests/test_web_api.py` — проверки подушки и нормы переезжают в `test_web_piggy.py`.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_web_piggy.py -q` — FAIL.
- [ ] **Step 3:** реализовать по образцу календаря (`add_record`, `check_record`, `calendar_lock`, `author_name`, `notify_others`, `fresh_forecast`).
- [ ] **Step 4:** `../venv_dev/bin/pytest -q` — PASS целиком.
- [ ] **Step 5:** `black . && ruff check . && pytest -q`; commit `feat(web): the «Накопления» API`.

### Task 8: Фоновая запись копилок

**Files:**
- Create: `app/services/piggy_sync.py`, `tests/test_piggy_sync.py`
- Modify: `app/handlers/receipts.py` (`sync_now`), `tests/test_receipts_job.py`

**Interfaces:**
- Consumes: Task 3–6.
- Produces: `def sync_piggy(gs: GoogleSheetsService, today: date) -> int` — число записанных ячеек и строк.

Правила (спека 5.5): листов нет — 0; читает листы заново (`get_piggy(0)`), `fact` и курсы с копией экрана; для каждой копилки, чьё окно закрылось (сегодня > срок + 30) и без строки `окно:<срок>`, — строка журнала «потрачено в окне» (минус ушедшее, «Кто» — «бот»), затем у «каждый год» — срок на год вперёд, только если в E та же дата, что в этом чтении; «Потрачено» (J) и «В этом месяце» (L) — только изменившиеся ячейки строк с тем же ID; пустые формулы I и K — восстанавливает. `sync_now` зовёт её после `sync_receipts` в своём `try/except` (ошибка — в лог, счёт прежний), под тем же замком.

- [ ] **Step 1: Write the failing tests:**
  - `test_a_closed_window_is_written_once_then_the_deadline_moves`;
  - `test_a_crash_between_the_two_writes_is_finished_next_time` — итог есть, срок прежний → следующий запуск только переносит срок;
  - `test_the_deadline_edited_by_hand_meanwhile_stays`;
  - `test_writes_only_changed_bot_cells`; `test_restores_missing_formulas`;
  - в `tests/test_receipts_job.py` — `test_a_piggy_failure_does_not_stop_the_receipts`.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy_sync.py tests/test_receipts_job.py -q` — FAIL.
- [ ] **Step 3:** реализовать.
- [ ] **Step 4:** тот же запуск — PASS.
- [ ] **Step 5:** `black . && ruff check . && pytest -q`; commit `feat(piggy): the background writes`.

### Task 9: «Накопления» в приложении

**Files:**
- Modify: `webapp/src/api.ts`, `webapp/src/mocks/api.ts`, `webapp/src/mocks/fixtures.ts`, `webapp/src/test/fakeApi.ts`, `webapp/src/App.tsx` (прогрев `savings` вместе с прогнозом и планом), `webapp/src/entry/EntryScreen.tsx` и `webapp/src/plan/PlanScreen.tsx` (ключ `savings` в сбрасываемых записью), `webapp/src/dashboard/DashboardScreen.tsx` (вкладка «Накопления» — свой ответ; «Обновить» обновляет и его), `webapp/src/dashboard/CushionCard.tsx`
- Create: `webapp/src/dashboard/SavingsTab.tsx`, `webapp/src/dashboard/PiggyCard.tsx`, `webapp/src/dashboard/YearPlan.tsx` и тесты к ним

**Interfaces:**
- Produces (`api.ts`): типы `SavingsScreen`, `PiggyState`, `MonthPlan`, `Enough`, `PiggyInput`, `MoveInput`; методы `savings(read?)`, `addPiggy(input)`, `updatePiggy(id, input)`, `closePiggy(id)`, `movePiggy(id, input)`, `savePiggies(input)`, `excludeSpend(id, key)` — все возвращают `SavingsScreen`; у `Dashboard` больше нет `cushion` и `savings`.
- `SavingsTab({ api }: { api: Api })` — `useResource(store, "savings", …)`; сверху вниз по спеке 6.2.

- [ ] **Step 1: Write the failing tests** (`SavingsTab.test.tsx`, `PiggyCard.test.tsx`, `YearPlan.test.tsx`, `CushionCard.test.tsx`, `DashboardScreen.test.tsx`, `App.test.tsx`) — тексты спеки 6.2 дословно: «В октябре отложить», «в копилки по плану 6,0 млн, осталось 6,0 млн», «10 % от поступлений ≈ 4,0 млн — в подушку», «накоплено 0 из 12,0 млн», «к 20.12 · каждый год», «в этом месяце 6,0 млн», «успеваем», «к сроку не успеть — не хватает 3,0 млн», «сумма?», «собрано ✓», «Проверьте строку 5 на листе «Копилки»», «Заведите копилку на Новый год, отпуск или другую большую трату — бот разложит взносы по месяцам», «1 100 $ = заморожено 1 500 $ − копилки 400 $ (на вечер вчера)», «В копилках на 300 $ больше, чем на долларовой карте, — отложите разницу или поправьте копилки», три месяца плана и «Весь год»; прогрев `savings` после «Сводки»; запись траты сбрасывает `savings`.
- [ ] **Step 2:** `cd webapp && rtk proxy npx vitest run src/dashboard src/App.test.tsx` — FAIL.
- [ ] **Step 3:** реализовать.
- [ ] **Step 4:** тот же запуск — PASS.
- [ ] **Step 5:** `npx prettier --write src && npm run lint && npm test && npm run build`; commit `feat(webapp): piggy banks in «Накопления»`.

### Task 10: Окна копилки и «Отложил…»

**Files:**
- Create: `webapp/src/fields.tsx` (из `forecast/RecordSheet.tsx` выносятся `cardCurrencies`, `chip`, `segment`, `FieldError` — RecordSheet импортирует их оттуда), `webapp/src/dashboard/PiggySheet.tsx`, `webapp/src/dashboard/SaveSheet.tsx` и тесты
- Modify: `webapp/src/dashboard/SavingsTab.tsx`, `README.md`, `CLAUDE.md`

**Interfaces:**
- Consumes: Task 9 — методы `api` и `SavingsScreen`.
- Produces: `PiggySheet({ api, boot, data, piggy, onSaved, onClose })`, `SaveSheet({ api, data, onSaved, onClose })`.

- [ ] **Step 1: Write the failing tests** — по спеке 6.3–6.4: создание (название, цель и валюта, срок или «Без срока», повтор только со сроком, статья необязательно), подсказка плана, «Сохранить», «Отложил…» и «Потратил…» с суммой «осталось в этом месяце» по умолчанию, «Закрыть копилку» → «Закрыть — остаток вернётся в подушку», «История» и «Не из копилки», ошибки полей под полями, один запрос на двойное нажатие; «Отложил…» — раскладка до записи, как в ответе сервера.
- [ ] **Step 2:** `cd webapp && rtk proxy npx vitest run src/dashboard src/forecast` — FAIL.
- [ ] **Step 3:** реализовать; README — абзац о копилках (листы, план, 10 %, подушка, окна), CLAUDE.md — `piggy.py`, `piggy_sync.py`, листы.
- [ ] **Step 4:** `npx prettier --write src && npm run lint && npm test && npm run build` и `black . && ruff check . && pytest -q` — всё зелёное; превью 360 px.
- [ ] **Step 5:** commit `feat(webapp): the piggy bank and «Отложил…» windows`.

---

## Этап 3 — бот и «Отложить за месяц»

**Goal:** Бот говорит о копилках в чате — после поступления «🐷 В копилки по плану ноября осталось: …» с «Отложил всё» и «Отложил часть…», после траты из копилки «🐷 Из копилки …» с «Не из копилки»; «🎯 Отложить за месяц» получает сумму копилок — в новой вкладке сам, в «Плане» подсказкой с «Применить»; цель может быть меньше нуля.

**Architecture:** `piggy.py` считает «Отложить за месяц» (4.7) для каждого месяца плана — `Screen.goals`. `GET /api/savings` отдаёт цель текущего месяца, «Накопления» показывают расхождение с Q20. `GoogleSheetsService._ensure_dates` пишет Q:R строки цели тем же запросом, что даты, — сумму даёт `piggy_sync.goal_cells`. «План» получает подсказку цели и статей месяца трат, сервер принимает цель меньше нуля. Новый `app/handlers/piggy.py` — сообщения «🐷», кнопки `piggy:` и хелперы журнала и заметок, вынесенные из `app/web/api.py` (иначе круговой импорт: `api.py` сам импортирует обработчики). Приложение по `?open=save` открывает «Накопления» и «Отложил…».

**Spec:** разделы 4.7, 6.2 п. 1, 6.4 (`?open=save`), 7, 8 и 9 п. 3.

### Global Constraints этапа 3

- Всё из «Global Constraints этапа 2» (Python, листы, тексты API, коммиты и PR).
- **«Отложить за месяц» месяца M** (4.7), в сумах: взносы копилок со сроком в M (`MonthPlan.total`) + доля 10 % (`cushion` + Σ `goals`) − траты из копилок в M. Траты из копилок: в текущем месяце — уже потраченное из копилок в M по сегодня (траты окна из fact и строки журнала «потратил» и «списал остаток» с днём в M); у копилок со сроком в M — плюс ещё не потраченная часть «собрано к сроку» (цель, у «к сроку не успеть» — собранное, минус уже потраченное к этому сроку); в будущих месяцах — только второе. Может быть меньше нуля. В таблицу и в «Применить» — сумы, округлённые до тысячи.
- **Q20** пишется только во вкладку, которой бот в этот же раз ставит даты M1:M2, тем же `values_batch_update` (RAW: Q — число, R — `UZS`), и только когда есть открытая копилка; строка — по подписи «Отложить за месяц» (`plan.read_plan(rows).goal.row`); сбой расчёта — даты пишутся без Q:R, сбой в лог. Q20 вкладки, где даты уже верны, бот не трогает: это ячейка владельца.
- Тексты бота — дословно (спека 7.1–7.3): «🐷 В копилки по плану ноября осталось: «Новый год» 6 000 000 UZS, «Отпуск» 210 USD»; кнопки «Отложил всё» и «Отложил часть…»; «🐷 Записал: «Новый год» 6 000 000 UZS, «Отпуск» 210 USD»; «Уже записано»; «Подсказка за октябрь устарела: в ноябре свой план»; «🐷 Из копилки «Новый год»: −2 400 000 UZS, осталось 3 600 000»; трата больше остатка — «🐷 Из копилки «Новый год»: 600 000 UZS — всё, что было, остальное — обычные траты»; «Не из копилки»; «🐷 Не из копилки — учтено»; «Уже учтено»; «Запись изменилась — отметьте в «Накоплениях»». Суммы — как `amount_text` этапа 2.
- `callback_data` не длиннее 64 байт: `piggy:all:<ГГГГ-ММ>` и `piggy:no:<ID>:<отпечаток из 10 знаков>`; `CallbackQueryHandler(..., pattern=r"^piggy:")` — до общего обработчика кнопок в `main.py`. Обработчик не опирается на `user_data` (после перезапуска её нет): всё пересчитывает по таблице заново под `PIGGY_LOCK` — тем же замком, что API.
- Сообщения «🐷» — отдельные `send_message` в чат записавшего, после меню под сводкой, без `track_message` (следующая запись их не удаляет); их сбой — только в лог, сводка и ответ записи не меняются.
- Обычная трата без статьи копилки не добавляет чтений таблицы: статьи открытых копилок — в `bot_data["piggy_articles"]` (время чтения и множество пар), обновляются любым чтением листов копилок, сами — не чаще раза в 10 минут.
- «Отложил часть…» — `InlineKeyboardButton(web_app=WebAppInfo(app_url(user_id) + "&open=save"))`; без `WEBAPP_URL` этой кнопки нет.
- «План»: цель меньше нуля — строка «💰 Из накоплений» с суммой без минуса; ручной ввод — от нуля; «Применить» ставит любое число до ±10¹²; окно цели без ввода отрицательную цель не трогает.

### Review Focus этапа 3

1. **«Отложил всё» после перезапуска, дважды, после «Отложил часть…», в следующем месяце** — журнал не задваивается, старая подсказка говорит, что устарела (тесты в Task 14).
2. **Сбой между копией вкладки и датами** — следующий запуск ставит даты и Q20 одним запросом; вкладку с верными датами (Q20 владельца) бот не трогает (тесты в Task 12).
3. **Отрицательная цель проходит «План» насквозь** — видна как «Из накоплений», «Готово» без ввода её не затирает, сохранение уходит со знаком, сервер принимает (тесты в Task 13).
4. **«Не из копилки» после /undo или правки строки** — «Запись изменилась…», журнал не меняется; две копилки одной статьи — одно нажатие снимает трату с обеих (тесты в Task 15).
5. **Обычная запись не медленнее и не ломается** — трата без статьи копилки не читает листы копилок; сбой расчёта «🐷» оставляет сводку прежней (тесты в Task 14 и 15).

### Task 11: «Отложить за месяц» в модели и «Накоплениях»

**Files:**
- Modify: `app/services/piggy.py`, `app/web/schemas.py`, `app/web/api.py` (`savings_screen_out`), `webapp/src/api.ts`, `webapp/src/mocks/fixtures.ts`, `webapp/src/dashboard/SavingsTab.tsx`
- Test: `tests/test_piggy.py`, `tests/test_web_piggy.py`, `webapp/src/dashboard/SavingsTab.test.tsx`

**Interfaces:**
- Produces (`piggy.py`): `FromPiggy(piggy_id, name, group, subcategory, currency, amount, uzs)` — траты из копилки в месяце (`amount` — в валюте копилки, `uzs` — в сумах); `MonthGoal(month, piggies, share, spent, total, from_piggies: tuple[FromPiggy, ...])` — сумы, `total = piggies + share − spent`; `Screen.goals: tuple[MonthGoal, ...] = ()` — по месяцу плана, в том же порядке; `goal_of(screen, month: date) -> MonthGoal | None`.
- Produces (API): `SavingsGoalOut {suggested, in_plan, piggies, share, spent}` — сумы, `suggested` — до тысячи, `in_plan` — Q20 текущей вкладки в сумах по курсу таблицы (`null` — строки цели нет); `SavingsScreenOut.goal: SavingsGoalOut | None` — `null`, когда открытых копилок нет.
- Produces (TS): `SavingsGoal`, `SavingsScreen.goal: SavingsGoal | null`.

- [ ] **Step 1: Write the failing tests**
  - `test_the_month_goal_adds_the_share_and_takes_the_spending` — «Новый год» 12 млн к 20.12, сегодня 17.10, поступлений в списке 12 млн: октябрь — `(6_000_000, 1_200_000, 0, 7_200_000)` (`piggies, share, spent, total`), ноябрь — `7_200_000`, декабрь — `1_000_000 + 1_200_000 − 12_000_000 = −9_800_000`, в `from_piggies` декабря — «Новый год» `12_000_000`.
  - `test_gifts_before_the_deadline_month_lower_the_goal` — сегодня 15.11, отложено 6 млн 30.10, подарки 2,4 млн 10.11: ноябрь — `6_000_000 + 1_200_000 − 2_400_000 = 4_800_000`, декабрь — `1_000_000 + 1_200_000 − 9_600_000 = −7_400_000`.
  - `test_a_late_piggy_spends_only_what_it_collected` — у «к сроку не успеть» в месяц срока из копилки уходит собранное, а не цель.
  - `test_savings_answer_has_the_goal_and_the_plan_cell` (API) — `goal["suggested"] == 7_200_000`, `goal["in_plan"]` — Q20 вкладки в сумах; без копилок — `goal is None`.
  - vitest: «В плане месяца «Отложить» — 2,00 млн, по копилкам нужно 7,20 млн: поправьте в «Плане»» при расхождении от 1 000 сумов; совпадает или `goal: null` — строки нет.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy.py tests/test_web_piggy.py -q` и `cd webapp && rtk proxy npx vitest run src/dashboard` — FAIL.
- [ ] **Step 3:** реализовать: `goals` считает `savings_screen` по уже готовым `plan`, `now`, `late` и журналу.
- [ ] **Step 4:** те же запуски — PASS; `black . && ruff check . && pytest -q`, `npx prettier --write src && npm run lint && npm test && npm run build`.
- [ ] **Step 5:** commit `feat(piggy): «Отложить за месяц» from the piggy banks`.

### Task 12: Q20 новой вкладки

**Files:**
- Modify: `app/services/google_sheets.py` (`_ensure_dates`, `ensure_month_tab`, `ensure_next_month_tab`), `app/services/piggy_sync.py` (`goal_cells`), `app/handlers/month.py` (`month_tab_job`), `app/web/api.py` (`create_next_plan`)
- Test: `tests/test_month.py`, `tests/test_piggy_sync.py`, `tests/test_web_plan.py`

**Interfaces:**
- Consumes: Task 11 — `Screen.goals`, `goal_of`.
- Produces: `GoalCells = Callable[[list[list[Any]], date], tuple[int, float] | None]` (`google_sheets.py`) — по значениям вкладки (UNFORMATTED) и её первому дню: строка цели и сумма в сумах или `None`; `_ensure_dates(title, first, last, goal: GoalCells | None = None) -> bool`; `ensure_month_tab(day, goal: GoalCells | None = None)`; `ensure_next_month_tab(today, goal: GoalCells | None = None)` — `goal` только для следующей вкладки; `piggy_sync.goal_cells(gs, today, currencies, groups, subcategories) -> GoalCells`.

- [ ] **Step 1: Write the failing tests**
  - `test_month.py`: даты неверны и есть `goal` → один `values_batch_update` с `'<вкладка>'!M1:M2` и `'<вкладка>'!Q20:R20` = `[[7_200_000, "UZS"]]`; даты верны → `goal` не вызывается, записи нет; `goal` упал → даты записаны без Q:R; `goal=None` — как сейчас (прежние `assert data == [...]` не меняются).
  - `test_piggy_sync.py`: `goal_cells` на копии вкладки со строкой цели 20 и «Новым годом» → `(20, 7_200_000)`; без открытых копилок → `None`; вкладка без подписи «Отложить за месяц» → `None`.
  - `test_web_plan.py`: «Создать план на ноябрь» при «Новом годе» пишет в скрытую вкладку Q:R строки цели `7_200_000`, `"UZS"` тем же запросом, что даты.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_month.py tests/test_piggy_sync.py tests/test_web_plan.py -q` — FAIL.
- [ ] **Step 3:** реализовать; `month_tab_job` и `create_next_plan` строят `goal_cells` по каталогу из `bot_data` (как `catalog()` в `api.py`).
- [ ] **Step 4:** те же — PASS; `black . && ruff check . && pytest -q`.
- [ ] **Step 5:** commit `feat(piggy): the new month tab gets «Отложить за месяц»`.

### Task 13: «План» — цель меньше нуля, «Применить», подсказки статей

**Files:**
- Modify: `app/web/schemas.py` (`GoalIn.amount` — `ge=-MAX_SUM`; `PlanGoalHintOut`, `PlanPiggyHintOut`; `PlanOut.goal_hint`, `PlanOut.piggy_hints`), `app/web/api.py` (`month_plan`, `save_month_plan`, `create_next_plan` — подсказки в ответе), `webapp/src/api.ts`, `webapp/src/plan/draft.ts` (`applyGoal`), `webapp/src/plan/GoalSheet.tsx`, `webapp/src/plan/PlanScreen.tsx`, `webapp/src/mocks/fixtures.ts`
- Create: `webapp/src/plan/PiggyHints.tsx`
- Test: `tests/test_web_plan.py`, `webapp/src/plan/draft.test.ts`, `webapp/src/plan/PlanScreen.test.tsx`

**Interfaces:**
- Consumes: Task 11 — `MonthGoal`, `FromPiggy`, `goal_of`.
- Produces: `PlanGoalHintOut {suggested, piggies, share, spent}` (сумы, `suggested` — до тысячи); `PlanPiggyHintOut {piggy, month, group, subcategory, currency, amount, need, key}` — трата из копилки со статьёй в месяце плана: `amount` — в валюте копилки, `need` — сколько не хватает плану статьи, `key` — статья вкладки (`article_key`) или `null`, если строки нет (тогда «Применить» добавляет её, как «＋ статья»); подсказка есть, пока план статьи меньше `amount`. Без открытых копилок — `goal_hint: null`, `piggy_hints: []`. TS: `applyGoal(draft, goal: PlanGoal, amount: number): Draft` — правка цели с текстом суммы со знаком и валютой `UZS`.

- [ ] **Step 1: Write the failing tests**
  - pytest: сохранение цели `−9 800 000` → 200, в Q строки цели `−9800000`; `GET /api/plan?month=2026-12` при «Новом годе» → `goal_hint.suggested == −9_800_000`, `piggy_hints[0]` — «Новый год», `🎉 ПРАЗДНИК` / `сезонный`, `amount == 12_000_000`, `need == 12_000_000`.
  - vitest: цель `−9 800 000` — строка «💰 Из накоплений» и «9 800 000 UZS»; окно цели и «Готово» без ввода — черновик не меняется; подсказка «По копилкам — −9,80 млн: копилки 1,00 млн + 10 % 1,20 млн − из копилок 12,00 млн» и «Применить» → в сохранении `goal.value.amount == "-9800000"`; подсказка статьи «Из копилки «Новый год» в декабре — 12,00 млн: заложите в статью «ПРАЗДНИК / сезонный»» и «Применить» → план статьи (или новая строка) +12 000 000 и цель по подсказке; подсказка уходит, когда плана статьи хватает.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_web_plan.py -q` и `cd webapp && rtk proxy npx vitest run src/plan` — FAIL.
- [ ] **Step 3:** реализовать.
- [ ] **Step 4:** те же — PASS; полные проверки Python и webapp.
- [ ] **Step 5:** commit `feat(plan): a goal below zero and the piggy bank hints`.

### Task 14: «🐷» после поступления, «Отложил всё», `?open=save`

**Files:**
- Create: `app/handlers/piggy.py`, `tests/test_piggy_bot.py`
- Modify: `app/web/api.py` (`move_row`, `piggy_note`, `notify_others`, `author_name` переезжают в `app/handlers/piggy.py`, `api.py` их импортирует), `app/handlers/messages.py` (`save_rows`), `app/handlers/fix.py`, `app/handlers/statement_import.py`, `app/main.py`, `webapp/src/App.tsx`, `webapp/src/dashboard/DashboardScreen.tsx`, `webapp/src/dashboard/SavingsTab.tsx`
- Test: `tests/test_messages.py`, `tests/test_fix.py`, `tests/test_statement_import.py`, `webapp/src/App.test.tsx`, `webapp/src/dashboard/SavingsTab.test.tsx`

**Interfaces:**
- Consumes: этап 2 — `AnalyticsService.savings`, `PIGGY_LOCK`, `mark_dirty`, `SAVED`; `common.app_url`.
- Produces (`app/handlers/piggy.py`): `async income_prompt(context, chat_id: int, user_id: int, rows: Sequence[SheetRow]) -> None` — «🐷» после поступления: копилки и долгие цели с «осталось в этом месяце» больше нуля, ноль у всех — сообщения нет; `async piggy_button(update, context) -> None` — `piggy:all:` (здесь) и `piggy:no:` (Task 15); вынесенные из `api.py` `move_row`, `piggy_note`, `notify_others`, `author_name`.
- Produces (webapp): `DashboardScreen` и `SavingsTab` получают `openSave: boolean`; `App` читает `open=save` из адреса один раз и открывает «Сводку».

- [ ] **Step 1: Write the failing tests**
  - `test_piggy_bot.py`: после записи поступления — «🐷 В копилки по плану октября осталось: «Новый год» 6 000 000 UZS», кнопки `piggy:all:2026-10` и web_app с адресом на `&open=save`; без поступления и без «осталось» — сообщения нет; расчёт упал — сообщения нет, сводка та же.
  - «Отложил всё»: журнал получает «отложил» на «осталось в этом месяце», сообщение — «🐷 Записал: «Новый год» 6 000 000 UZS» без кнопок, второму — «🐷 Иван — отложил: Новый год 6 000 000 UZS»; второе нажатие — «Уже записано», журнал не растёт; кнопка прошлого месяца — «Подсказка за сентябрь устарела: в октябре свой план» и «Отложил часть…»; пустая `user_data` (после перезапуска) — то же поведение.
  - Пути записи: `save_rows` с поступлением зовёт `income_prompt` один раз и после меню; `/fix` в доходы — тоже; выписка с поступлением этого месяца — тоже, прошлого месяца — нет.
  - vitest: `?open=save` открывает «Сводку» → «Накопления» и окно «Отложил», когда есть что откладывать.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy_bot.py tests/test_messages.py tests/test_fix.py tests/test_statement_import.py -q` и `cd webapp && rtk proxy npx vitest run src/App.test.tsx src/dashboard` — FAIL.
- [ ] **Step 3:** реализовать; `piggy_button` в `main.py` — до общего обработчика.
- [ ] **Step 4:** те же — PASS; полные проверки.
- [ ] **Step 5:** commit `feat(bot): «🐷» after an income and «Отложил всё»`.

### Task 15: «🐷 Из копилки» после траты и «Не из копилки»

**Files:**
- Modify: `app/handlers/piggy.py`, `app/handlers/messages.py` (`save_rows`: строки «🐷» в `SaveResult.lines` — для ответа приложению), `app/services/piggy.py` (`row_prefix`), `README.md`, `CLAUDE.md`
- Test: `tests/test_piggy_bot.py`, `tests/test_piggy.py`, `tests/test_web_api.py`

**Interfaces:**
- Consumes: Task 14 — `piggy_button`; этап 2 — `fact_key`, `fingerprint`, `NOT_FROM`.
- Produces: `row_prefix(day, group, subcategory, amount, currency, source) -> str` (`piggy.py`) — ключ строки fact без комментария: трата копилки — та, чей ключ с него начинается (комментарий Sheets мог поменять при записи); `piggy_articles(bot_data, gs) -> set[tuple[str, str]]` (`app/handlers/piggy.py`); `async spend_note(context, chat_id: int, rows: Sequence[SheetRow]) -> list[str]` — строки «🐷 Из копилки…» для ответа приложению и сообщение в чат: одна строка записи — с кнопкой `piggy:no:<ID>:<отпечаток>`, несколько — по строке на копилку без кнопки.

- [ ] **Step 1: Write the failing tests**
  - трата 2 400 000 по «ПРАЗДНИК / сезонный» в окне «Нового года» с остатком 6 000 000 → «🐷 Из копилки «Новый год»: −2 400 000 UZS, осталось 3 600 000» и кнопка `piggy:no:p3k9za:<fingerprint(key)>`; трата больше остатка → «…: 600 000 UZS — всё, что было, остальное — обычные траты»; две копилки одной статьи — обе строки одним сообщением; пачка строк — по строке на копилку без кнопки; трата другой статьи — листы копилок не читаются сверх `piggy_articles`; `add_expense` приложения — строка «🐷…» в `lines` ответа.
  - «Не из копилки»: журнал получает «не из копилки» с ключом, сообщение — «🐷 Не из копилки — учтено», второму — заметка 7.3; второе нажатие — «Уже учтено»; строку правили или отменили — «Запись изменилась — отметьте в «Накоплениях»», журнал не меняется; две копилки — после нажатия трата не уходит ни в одну.
- [ ] **Step 2:** `../venv_dev/bin/pytest tests/test_piggy_bot.py tests/test_piggy.py tests/test_web_api.py -q` — FAIL.
- [ ] **Step 3:** реализовать; README — абзац о сообщениях «🐷» и «Отложить за месяц», CLAUDE.md — `app/handlers/piggy.py` и Q20 новой вкладки.
- [ ] **Step 4:** те же — PASS; полные проверки Python и webapp.
- [ ] **Step 5:** commit `feat(bot): «🐷 Из копилки» and «Не из копилки»`.
