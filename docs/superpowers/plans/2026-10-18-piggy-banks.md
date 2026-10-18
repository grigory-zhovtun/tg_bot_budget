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
  - `"«Впереди» shows the list and the subscriptions"` — «−650,00 USD» с классом `text-payment`, `/ждём/`.
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
