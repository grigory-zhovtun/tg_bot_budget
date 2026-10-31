import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  type Api,
  type Bootstrap,
  type Dashboard,
  type ExpenseInput,
  type ExpenseResult,
} from "../api";
import {
  bootstrapFixture,
  dashboardFixture,
  savingsFixture,
  writtenFixture,
} from "../mocks/fixtures";
import { GOOD_COLOR } from "../dashboard/controls";
import { Store, StoreProvider } from "../store";
import { fakeApi } from "../test/fakeApi";
import { inTelegram, lastCall, tapPanel } from "../test/telegram";
import { nb } from "../test/text";
import EntryScreen from "./EntryScreen";
import { enqueue, readOutbox, writeOutbox } from "./outbox";

type User = ReturnType<typeof userEvent.setup>;

const OWNER = bootstrapFixture.user.id;

// справочник с доходами и счетами: bootstrapFixture общий для девяти файлов
const ALL_GROUPS: Bootstrap = {
  ...bootstrapFixture,
  sources: [
    ...bootstrapFixture.sources,
    { name: "HUMO 2222 UZS", currency: "UZS" },
  ],
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

// повторы без пауз: тесты не ждут 2 и 5 секунд
const noWait = () => Promise.resolve();

function renderEntry(
  api: Api,
  onCatalogChanged: () => void = vi.fn(),
  boot: Bootstrap = bootstrapFixture,
): void {
  render(
    <EntryScreen
      api={api}
      boot={boot}
      onCatalogChanged={onCatalogChanged}
      wait={noWait}
    />,
  );
}

async function fillCoffee(user: User): Promise<void> {
  await user.click(screen.getByRole("button", { name: "ЕДА" }));
  await user.click(screen.getByRole("button", { name: "кофе" }));
  for (const key of ["4", "8", "000"]) {
    await user.click(screen.getByRole("button", { name: key }));
  }
}

/** Строка плана на плитке: «⚠️ сверх на 300 000»; null — плитка без плана. */
function tilePlan(name: string): string | null {
  const id = screen
    .getByRole("button", { name })
    .getAttribute("aria-describedby");
  const note = id ? document.getElementById(id) : null;
  return note ? (note.textContent ?? "") : null;
}

function sentLike(id: string): ExpenseInput {
  return {
    entry_id: id,
    source: "VISA 9090 UZS",
    category: "🍔 ЕДА",
    subcategory: "кофе",
    amount: "48000",
    comment: "",
  };
}

function sent(api: Api, call = 0): ExpenseInput {
  const input = vi.mocked(api.addExpense).mock.calls[call]?.[0];
  if (!input) throw new Error("addExpense was not called");
  return input;
}

describe("EntryScreen", () => {
  afterEach(() => {
    vi.useRealTimers();
    writeOutbox(OWNER, []);
    localStorage.clear();
  });

  it("records an expense and comes back to the groups", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.type(screen.getByLabelText("Комментарий"), "латте");
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(sent(api)).toMatchObject({
      source: "VISA 9090 UZS",
      category: "🍔 ЕДА",
      subcategory: "кофе",
      amount: "48000",
      comment: "латте",
    });
    expect(sent(api).day).toBeUndefined(); // дату ставит бот
    expect(await screen.findByText(/✅ 48 000 UZS/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "ЕДА" })).toBeTruthy();
  });

  it("dates the entry by the bot's today, not by the phone clock", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-10-11T03:00:00Z"));
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    expect(screen.getByText("Дата: Сегодня")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Списать" }));
    // не отправляем день вообще: приложение могло пролежать открытым до утра,
    // и сегодняшнее число знает только бот
    expect(sent(api).day).toBeUndefined();
  });

  it("sends the date the user picked", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    fireEvent.change(screen.getByLabelText(/Дата/), {
      target: { value: "2026-10-09" },
    });
    expect(screen.getByText("Дата: Вчера")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(sent(api).day).toBe("2026-10-09");
  });

  it("sends again by itself with the same entry id when the sheet is down", async () => {
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValueOnce(
        new ApiError(
          503,
          "sheets_unavailable",
          "Таблица не ответила — попробуйте ещё раз",
        ),
      )
      .mockResolvedValueOnce(writtenFixture);
    const api = fakeApi({ addExpense });
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(await screen.findByText(/✅ 48 000 UZS/)).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
    expect(sent(api, 1).entry_id).toBe(sent(api, 0).entry_id);
  });

  it("keeps the expense on the phone when there is no connection", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-10-10T12:00:00Z"));
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValue(
        new ApiError(0, "network", "Нет связи с сервером — проверьте интернет"),
      );
    const api = fakeApi({ addExpense });
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(
      await screen.findByText("📤 Трата сохранена на телефоне"),
    ).toBeTruthy();
    expect(addExpense).toHaveBeenCalledTimes(3);
    const queue = screen.getByRole("region", { name: "Не отправлено" });
    expect(queue.textContent).toContain("📤 Не отправлено: 1");
    expect(queue.textContent?.replace(/\s/g, " ")).toContain(
      "кофе 48 000 UZS, сегодня",
    );
    // форма — снова с групп; дата в очереди — сегодня по боту
    expect(screen.getByRole("button", { name: "ЕДА" })).toBeTruthy();
    expect(readOutbox(OWNER)[0]?.entry.day).toBe(bootstrapFixture.today);
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("says «Зачисление сохранено» when a credit stays on the phone", async () => {
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValue(
        new ApiError(0, "network", "Нет связи с сервером — проверьте интернет"),
      );
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Зачислить" }));
    expect(
      await screen.findByText("📤 Зачисление сохранено на телефоне"),
    ).toBeTruthy();
    expect(screen.queryByText("📤 Трата сохранена на телефоне")).toBeNull();
  });

  it("dates a kept expense by the bot's day at the tap", async () => {
    // приложение открыли 10-го, трату ввели в 00:30 по Ташкенту уже 12-го
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-10-11T19:30:00Z"));
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValue(new ApiError(0, "network", "Нет связи"));
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    await screen.findByText("📤 Трата сохранена на телефоне");
    expect(readOutbox(OWNER)[0]?.entry.day).toBe("2026-10-12");
    // сам запрос — без даты: её ставит бот
    expect(addExpense.mock.calls[0]?.[0].day).toBeUndefined();
  });

  it("keeps the expense on the phone while it is being sent", async () => {
    // приложение закрыли во время повторов — трата уйдёт при следующем открытии
    const addExpense = vi.fn<Api["addExpense"]>(() => new Promise(() => {}));
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(
      readOutbox(OWNER).map((item) => item.title.replace(/\s/g, " ")),
    ).toEqual(["кофе 48 000 UZS"]);
    expect(screen.queryByRole("region", { name: "Не отправлено" })).toBeNull();
  });

  it("sends the queue when the phone is back online", async () => {
    enqueue(OWNER, {
      entry: { ...sentLike("q1"), day: bootstrapFixture.today },
      title: "кофе 48 000 UZS",
    });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValueOnce(new ApiError(0, "network", "Нет связи"))
      .mockResolvedValueOnce(writtenFixture);
    renderEntry(fakeApi({ addExpense }));
    await waitFor(() => expect(addExpense).toHaveBeenCalledTimes(1));
    expect(
      screen.getByRole("region", { name: "Не отправлено" }).textContent,
    ).toContain("📤 Не отправлено: 1");
    await act(async () => {
      window.dispatchEvent(new Event("online"));
    });
    expect(await screen.findByText("✅ Отправлено из очереди: 1")).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Не отправлено" })).toBeNull();
    expect(addExpense.mock.calls[1]?.[0].entry_id).toBe("q1");
  });

  it("shows why the server refused and deletes after a second tap", async () => {
    enqueue(OWNER, {
      entry: { ...sentLike("q2"), day: "2026-08-01" },
      title: "кофе 48 000 UZS",
    });
    const day = "Дата — не раньше чем 31 день назад и не позже сегодня";
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValue(
        new ApiError(422, "validation", "Проверьте введённые данные", { day }),
      );
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    const queue = await screen.findByRole("region", { name: "Не отправлено" });
    expect(await within(queue).findByText(`⚠️ ${day}`)).toBeTruthy();
    await user.click(within(queue).getByRole("button", { name: "Удалить" }));
    expect(readOutbox(OWNER)).toHaveLength(1);
    await user.click(
      within(queue).getByRole("button", { name: "Точно удалить" }),
    );
    expect(screen.queryByRole("region", { name: "Не отправлено" })).toBeNull();
    expect(readOutbox(OWNER)).toEqual([]);
  });

  it("keeps the expense unmarked when the login expired", async () => {
    enqueue(OWNER, {
      entry: { ...sentLike("q3"), day: bootstrapFixture.today },
      title: "кофе 48 000 UZS",
    });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValue(
        new ApiError(401, "unauthorized", "Откройте приложение заново из бота"),
      );
    renderEntry(fakeApi({ addExpense }));
    expect((await screen.findByRole("alert")).textContent).toContain(
      "Откройте приложение заново из бота",
    );
    const queue = screen.getByRole("region", { name: "Не отправлено" });
    expect(queue.textContent).not.toContain("⚠️");
    expect(within(queue).queryByRole("button", { name: "Удалить" })).toBeNull();
    // до нового открытия приложения больше не шлём
    await act(async () => {
      window.dispatchEvent(new Event("online"));
    });
    expect(addExpense).toHaveBeenCalledTimes(1);
    expect(readOutbox(OWNER)[0]?.error).toBeUndefined();
  });

  it("sends the queue when the app is back on the screen", async () => {
    enqueue(OWNER, {
      entry: { ...sentLike("q4"), day: bootstrapFixture.today },
      title: "кофе 48 000 UZS",
    });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValueOnce(new ApiError(0, "network", "Нет связи"))
      .mockResolvedValueOnce(writtenFixture);
    renderEntry(fakeApi({ addExpense }));
    await waitFor(() => expect(addExpense).toHaveBeenCalledTimes(1));
    Object.defineProperty(document, "visibilityState", {
      value: "visible",
      configurable: true,
    });
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"));
    });
    expect(await screen.findByText("✅ Отправлено из очереди: 1")).toBeTruthy();
  });

  it("waits for «Отправить сейчас» and says when there is no connection", async () => {
    enqueue(OWNER, {
      entry: { ...sentLike("q5"), day: bootstrapFixture.today },
      title: "кофе 48 000 UZS",
    });
    let fail: (reason: unknown) => void = () => undefined;
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValueOnce(new ApiError(0, "network", "Нет связи"))
      .mockImplementationOnce(
        () =>
          new Promise((_resolve, reject) => {
            fail = reject;
          }),
      );
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await waitFor(() => expect(addExpense).toHaveBeenCalledTimes(1));
    const button = await screen.findByRole("button", {
      name: "Отправить сейчас",
    });
    await waitFor(() => expect(button.hasAttribute("disabled")).toBe(false));
    await user.click(button);
    expect(button.hasAttribute("disabled")).toBe(true);
    expect(button.getAttribute("aria-busy")).toBe("true");
    await act(async () => {
      fail(new ApiError(0, "network", "Нет связи"));
    });
    expect(
      await screen.findByText("📵 Связи нет — трата ждёт на телефоне"),
    ).toBeTruthy();
  });

  it("says a credit waits on the phone when there is still no connection", async () => {
    enqueue(OWNER, {
      entry: { ...sentLike("q8"), incoming: true, day: bootstrapFixture.today },
      title: "кофе 48 000 UZS",
    });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValue(new ApiError(0, "network", "Нет связи"));
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await waitFor(() => expect(addExpense).toHaveBeenCalledTimes(1));
    const button = await screen.findByRole("button", {
      name: "Отправить сейчас",
    });
    await waitFor(() => expect(button.hasAttribute("disabled")).toBe(false));
    await user.click(button);
    expect(
      await screen.findByText("📵 Связи нет — зачисление ждёт на телефоне"),
    ).toBeTruthy();
  });

  it("drops «Отменить» once the queue sent an expense", async () => {
    // сервер отменяет последнюю запись — ею стала трата из очереди
    const user = userEvent.setup();
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockResolvedValue(writtenFixture);
    renderEntry(fakeApi({ addExpense }));
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(
      await screen.findByRole("button", { name: "↩️ Отменить" }),
    ).toBeTruthy();
    enqueue(OWNER, {
      entry: { ...sentLike("q6"), day: bootstrapFixture.today },
      title: "кофе 48 000 UZS",
    });
    await act(async () => {
      window.dispatchEvent(new Event("online"));
    });
    expect(await screen.findByText("✅ Отправлено из очереди: 1")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "↩️ Отменить" })).toBeNull();
  });

  it("holds «Отменить» while the queue is being sent", async () => {
    const user = userEvent.setup();
    let finish: (result: ExpenseResult) => void = () => undefined;
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockResolvedValueOnce(writtenFixture)
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finish = resolve;
          }),
      );
    renderEntry(fakeApi({ addExpense }));
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    const undo = await screen.findByRole("button", { name: "↩️ Отменить" });
    enqueue(OWNER, {
      entry: { ...sentLike("q7"), day: bootstrapFixture.today },
      title: "кофе 48 000 UZS",
    });
    await act(async () => {
      window.dispatchEvent(new Event("online"));
    });
    expect(undo.hasAttribute("disabled")).toBe(true);
    await act(async () => {
      finish(writtenFixture);
    });
  });

  it("sends one request when the button is tapped twice", async () => {
    let finish: (result: ExpenseResult) => void = () => undefined;
    const addExpense = vi.fn<Api["addExpense"]>(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await fillCoffee(user);
    const button = screen.getByRole("button", { name: "Списать" });
    fireEvent.click(button);
    fireEvent.click(button);
    expect(addExpense).toHaveBeenCalledTimes(1);
    finish(writtenFixture);
    expect(await screen.findByText(/✅ 48 000 UZS/)).toBeTruthy();
  });

  it("reloads the catalog when the bot no longer has the subcategory", async () => {
    const addExpense = vi.fn<Api["addExpense"]>().mockRejectedValue(
      new ApiError(422, "validation", "Проверьте введённые данные", {
        subcategory: "Такой подкатегории нет — обновите приложение",
      }),
    );
    const onCatalogChanged = vi.fn();
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }), onCatalogChanged);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(
      await screen.findByText("Справочник обновился — выберите заново"),
    ).toBeTruthy();
    expect(onCatalogChanged).toHaveBeenCalledOnce();
    expect(screen.getByRole("button", { name: "ЕДА" })).toBeTruthy();
  });

  it("takes cents on a dollar card", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await user.click(screen.getByRole("radio", { name: "VISA 3434 USD" }));
    await user.click(screen.getByRole("button", { name: "ЕДА" }));
    await user.click(screen.getByRole("button", { name: "кофе" }));
    for (const key of ["1", "2", ",", "5"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(sent(api)).toMatchObject({
      source: "VISA 3434 USD",
      amount: "12.5",
    });
  });

  it("undoes the last write from the summary", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    await user.click(
      await screen.findByRole("button", { name: "↩️ Отменить" }),
    );
    expect(api.undo).toHaveBeenCalledOnce();
    expect(
      await screen.findByText("↩️ Удалил из таблицы: строка 4169."),
    ).toBeTruthy();
  });

  it("undoes once even when «Отменить» is tapped twice", async () => {
    let finish: (result: { message: string }) => void = () => undefined;
    const undo = vi.fn<Api["undo"]>(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const user = userEvent.setup();
    renderEntry(fakeApi({ undo }));
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    const button = await screen.findByRole("button", { name: "↩️ Отменить" });
    fireEvent.click(button);
    fireEvent.click(button);
    expect(undo).toHaveBeenCalledTimes(1);
    finish({ message: "↩️ Удалил из таблицы: строка 4169." });
    expect(
      await screen.findByText("↩️ Удалил из таблицы: строка 4169."),
    ).toBeTruthy();
  });

  it("shows what is left in the plan on the tiles", async () => {
    const user = userEvent.setup();
    renderEntry(fakeApi());
    await waitFor(() => {
      expect(tilePlan("ЕДА")).toBe(nb("⚠️ сверх на 300 000"));
    });
    await user.click(screen.getByRole("button", { name: /ЕДА/ }));
    expect(tilePlan("кофе")).toBe(nb("осталось 50 000"));
    expect(tilePlan("кафе")).toBe(nb("⚠️ сверх на 350 000"));
  });

  it("works without the plan when the sheet does not answer", async () => {
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockRejectedValue(new ApiError(503, "sheets_unavailable", "нет"));
    const user = userEvent.setup();
    renderEntry(fakeApi({ dashboard }));
    await user.click(screen.getByRole("button", { name: /ЕДА/ }));
    expect(screen.getByRole("button", { name: /кофе/ })).toBeTruthy();
    expect(screen.queryByText(/осталось|сверх/)).toBeNull();
  });

  it("updates what is left after a write", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    await screen.findByText(/✅ 48 000 UZS/);
    expect(api.dashboard).toHaveBeenCalledTimes(2);
  });

  it("makes «Накопления» ask the table again after a write", async () => {
    const store = new Store();
    store.put("savings", savingsFixture);
    const api = fakeApi();
    const user = userEvent.setup();
    render(
      <StoreProvider value={store}>
        <EntryScreen
          api={api}
          boot={bootstrapFixture}
          onCatalogChanged={vi.fn()}
        />
      </StoreProvider>,
    );
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    await screen.findByText(/✅ 48 000 UZS/);
    // трата по статье копилки меняет её остаток — вкладка спросит сервер заново
    expect(store.get("savings").at).toBe(0);
  });

  it("keeps the fresh plan when an older answer comes late", async () => {
    let answerFirst: (value: Dashboard) => void = () => undefined;
    const fresh: Dashboard = {
      ...dashboardFixture,
      groups: dashboardFixture.groups.map((group) =>
        group.name === "🍔 ЕДА" ? { ...group, fact: 3_400_000 } : group,
      ),
    };
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockImplementationOnce(
        () =>
          new Promise<Dashboard>((resolve) => {
            answerFirst = resolve;
          }),
      )
      .mockResolvedValueOnce(fresh);
    const user = userEvent.setup();
    renderEntry(fakeApi({ dashboard }));
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    await waitFor(() => {
      expect(tilePlan("ЕДА")).toBe(nb("⚠️ сверх на 400 000"));
    });
    await act(async () => {
      answerFirst(dashboardFixture);
    });
    expect(tilePlan("ЕДА")).toBe(nb("⚠️ сверх на 400 000"));
  });

  it("hides the plan it could not refresh after a write", async () => {
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockResolvedValueOnce(dashboardFixture)
      .mockRejectedValueOnce(new ApiError(503, "sheets_unavailable", "нет"));
    const user = userEvent.setup();
    renderEntry(fakeApi({ dashboard }));
    await waitFor(() => {
      expect(tilePlan("ЕДА")).toBe(nb("⚠️ сверх на 300 000"));
    });
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    await screen.findByText(/✅ 48 000 UZS/);
    await waitFor(() => {
      expect(tilePlan("ЕДА")).toBeNull();
    });
    expect(screen.queryByText(/^(осталось|⚠️ сверх на|вне плана)$/)).toBeNull();
  });

  it("updates what is left after an undo", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    await user.click(await screen.findByRole("button", { name: /Отменить/ }));
    await screen.findByText(/Удалил из таблицы/);
    expect(api.dashboard).toHaveBeenCalledTimes(3);
  });

  it("follows the default card of a fresh catalog until a card is picked", () => {
    const api = fakeApi();
    const changed = vi.fn();
    const { rerender } = render(
      <EntryScreen
        api={api}
        boot={bootstrapFixture}
        onCatalogChanged={changed}
      />,
    );
    // справочник с прошлого раза, а свежий уже знает другую карту по умолчанию
    rerender(
      <EntryScreen
        api={api}
        boot={{ ...bootstrapFixture, default_source: "VISA 3434 USD" }}
        onCatalogChanged={changed}
      />,
    );
    expect(
      screen.getByRole("radio", { name: "VISA 3434 USD", checked: true }),
    ).toBeTruthy();
  });

  it("keeps the card the user picked when the catalog is read again", async () => {
    const api = fakeApi();
    const changed = vi.fn();
    const user = userEvent.setup();
    const { rerender } = render(
      <EntryScreen
        api={api}
        boot={bootstrapFixture}
        onCatalogChanged={changed}
      />,
    );
    await user.click(screen.getByRole("radio", { name: "VISA 3434 USD" }));
    rerender(
      <EntryScreen
        api={api}
        boot={{ ...bootstrapFixture }}
        onCatalogChanged={changed}
      />,
    );
    expect(
      screen.getByRole("radio", { name: "VISA 3434 USD", checked: true }),
    ).toBeTruthy();
  });

  it("shows the cards as big bold buttons", () => {
    renderEntry(fakeApi());
    const card = screen.getByRole("radio", { name: "VISA 9090 UZS" });
    expect(card.className).toContain("text-line");
    expect(card.className).toContain("font-extrabold");
    expect(card.className).not.toContain("text-sm");
  });

  it("writes the amount step big, in the text colour", async () => {
    const user = userEvent.setup();
    renderEntry(fakeApi());
    await fillCoffee(user);
    const back = screen.getByRole("button", { name: "← Назад" });
    expect(back.className).toContain("text-line");
    expect(back.className).toContain("font-bold");
    const title = screen.getByRole("heading", {
      name: "🍔 ЕДА · ☕ кофе",
    });
    expect(title.className).toContain("text-fact");
    // длинная пара переносится только между группой и подкатегорией
    expect(
      [...title.querySelectorAll(".whitespace-nowrap")].map(
        (part) => part.textContent,
      ),
    ).toEqual(["🍔 ЕДА", "☕ кофе"]);
    const amount = screen.getByText("48 000");
    expect(amount.className).toContain("whitespace-nowrap");
    expect(amount.parentElement?.className).toContain("text-big");
    expect(amount.parentElement?.className).not.toContain("text-big-long");
    const currency = screen.getByText("UZS");
    expect(currency.className).not.toContain("text-hint");
    for (const text of ["Комментарий", "Дата: Сегодня"]) {
      const label = screen.getByText(text).closest("label");
      expect(label?.className).toContain("text-line");
      expect(label?.className).toContain("font-bold");
      expect(label?.className).not.toContain("text-hint");
    }
    expect(screen.getByLabelText("Комментарий").className).toContain("text-lg");
    const five = screen.getByRole("button", { name: "5" });
    expect(five.className).toContain("text-[28px]");
  });

  it("makes a long amount a step smaller so it stays on one line", async () => {
    const user = userEvent.setup();
    renderEntry(fakeApi());
    await fillCoffee(user);
    for (const key of ["000", "000"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    // число не рвётся между группами цифр, валюта может уйти строкой ниже
    const amount = screen.getByText("48 000 000 000");
    expect(amount.className).toContain("whitespace-nowrap");
    expect(amount.parentElement?.className).toContain("text-big-long");
  });

  it("writes the answer and «Отменить» at 17 px", async () => {
    const user = userEvent.setup();
    renderEntry(fakeApi());
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    const answer = await screen.findByRole("status");
    expect(answer.className).toContain("text-line");
    expect(answer.className).not.toContain("text-sm");
    const undo = screen.getByRole("button", { name: "↩️ Отменить" });
    expect(undo.className).toContain("text-line");
    expect(undo.className).toContain("font-bold");
    expect(undo.className).toContain("min-h-11"); // палец попадает с первого раза
    await user.click(undo);
    const notice = await screen.findByText(
      "↩️ Удалил из таблицы: строка 4169.",
    );
    expect(notice.className).toContain("text-line");
  });

  it("puts «Зачислить» left in green and «Списать» right", async () => {
    const user = userEvent.setup();
    renderEntry(fakeApi());
    await fillCoffee(user);
    const spend = screen.getByRole("button", { name: "Списать" });
    const row = spend.parentElement;
    expect(row?.className).toContain("grid-cols-2");
    expect(
      [...(row?.children ?? [])].map((child) => child.textContent),
    ).toEqual(["Зачислить", "Списать"]);
    const credit = screen.getByRole("button", { name: "Зачислить" });
    for (const name of [
      "bg-chip-good",
      "text-white",
      "text-lg",
      "font-extrabold",
    ]) {
      expect(credit.className).toContain(name);
    }
    for (const name of [
      "bg-button",
      "text-button-text",
      "text-lg",
      "font-extrabold",
    ]) {
      expect(spend.className).toContain(name);
    }
    // в половине ряда слово при крупном шрифте переносится, а не обрезается
    for (const button of [credit, spend]) {
      const classes = button.className.split(" ");
      expect(classes).toContain("min-h-12");
      expect(classes).toContain("px-1");
      expect(classes).not.toContain("h-12");
    }
  });

  it("turns both buttons off at zero and while one is sending", async () => {
    let finish: (result: ExpenseResult) => void = () => undefined;
    const addExpense = vi.fn<Api["addExpense"]>(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await user.click(screen.getByRole("button", { name: "ЕДА" }));
    await user.click(screen.getByRole("button", { name: "кофе" }));
    const off = () =>
      ["Зачислить", "Списать"].map((name) =>
        screen.getByRole("button", { name }).hasAttribute("disabled"),
      );
    expect(off()).toEqual([true, true]);
    for (const key of ["4", "8", "000"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    expect(off()).toEqual([false, false]);
    await user.click(screen.getByRole("button", { name: "Зачислить" }));
    expect(off()).toEqual([true, true]);
    await act(async () => finish(writtenFixture));
  });

  it("sends only «Зачислить» when «Списать» is tapped right after it", async () => {
    const addExpense = vi.fn<Api["addExpense"]>(() => new Promise(() => {}));
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await fillCoffee(user);
    fireEvent.click(screen.getByRole("button", { name: "Зачислить" }));
    fireEvent.click(screen.getByRole("button", { name: "Списать" }));
    expect(addExpense).toHaveBeenCalledOnce();
    expect(addExpense.mock.calls[0]?.[0].incoming).toBe(true);
  });

  it("writes «Зачислить» with the sign and says what went into the table", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Зачислить" }));
    expect(sent(api)).toMatchObject({ amount: "48000", incoming: true });
    const head = await screen.findByText(
      "✓ Зачислено 48 000 UZS — в таблицу −48 000",
    );
    expect(head.className).toContain("text-fact");
    expect(head.className).toContain("font-extrabold");
    // строки сервера — под первой строкой
    const line = screen.getByText(/✅ 48 000 UZS/);
    expect(
      head.compareDocumentPosition(line) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it("writes «Списать» without the sign field", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    expect(sent(api)).not.toHaveProperty("incoming");
    expect(await screen.findByText("✓ Списано 48 000 UZS")).toBeTruthy();
  });

  it("keeps one «Записать» for incomes", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api, vi.fn(), ALL_GROUPS);
    await user.click(screen.getByRole("button", { name: "ДОХОДЫ" }));
    await user.click(screen.getByRole("button", { name: "зарплата" }));
    for (const key of ["4", "8", "000"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    expect(screen.queryByRole("button", { name: "Зачислить" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Списать" })).toBeNull();
    await user.click(
      screen.getByRole("button", { name: /^Записать 48\s000 UZS$/ }),
    );
    expect(sent(api)).not.toHaveProperty("incoming");
    expect(await screen.findByText(/✅ 48 000 UZS/)).toBeTruthy();
    expect(screen.queryByText(/^✓ (Списано|Зачислено)/)).toBeNull();
  });

  it("names a credit «зачисление» in the queue and keeps old entries spends", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-10-10T12:00:00Z"));
    const today = bootstrapFixture.today;
    enqueue(OWNER, {
      entry: { ...sentLike("old"), day: today },
      title: "кофе 48 000 UZS",
    });
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
      within(queue)
        .getAllByRole("listitem")
        .map((item) => item.textContent?.replace(/\s/g, " ")),
    ).toEqual([
      "кофе 48 000 UZS, сегодня",
      "зачисление: кофе 48 000 UZS, сегодня",
    ]);
    await act(async () => {
      window.dispatchEvent(new Event("online"));
    });
    expect(await screen.findByText("✅ Отправлено из очереди: 2")).toBeTruthy();
    // сохранённая до обновления — без признака, то есть трата
    expect(addExpense.mock.calls[1]?.[0]).not.toHaveProperty("incoming");
    expect(addExpense.mock.calls[2]?.[0].incoming).toBe(true);
  });

  it("writes an error at 17 px", async () => {
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValue(new ApiError(400, "bad_amount", "Сумма не читается"));
    const user = userEvent.setup();
    renderEntry(fakeApi({ addExpense }));
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: "Списать" }));
    const alert = await screen.findByRole("alert");
    expect(alert.className).toContain("text-line");
    expect(alert.className).not.toContain("text-sm");
  });
});

describe("EntryScreen in Telegram", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    writeOutbox(OWNER, []);
    localStorage.clear();
  });

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
    // текст — в том же вызове, что показ: кадра с прежним текстом нет
    expect(main.setParams).toHaveBeenLastCalledWith({
      text: "Списать",
      is_visible: true,
    });
    expect(main.show).not.toHaveBeenCalled();
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
      expect(lastCall(button.disable)).toBeGreaterThan(
        lastCall(button.hideProgress),
      );
    }
    for (const key of ["4", "8", "000"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    for (const button of [main, second]) {
      expect(lastCall(button.enable)).toBeGreaterThan(lastCall(button.disable));
    }
    await tapPanel(main);
    expect(addExpense.mock.calls[0]?.[0]).not.toHaveProperty("incoming");
    expect(main.showProgress).toHaveBeenLastCalledWith(false);
    expect(second.showProgress).not.toHaveBeenCalled(); // крутится только нажатая
    expect(lastCall(second.disable)).toBeGreaterThan(lastCall(second.enable));
  });

  it("hides «Зачислить» when the sum step closes", async () => {
    const { second } = inTelegram();
    const user = userEvent.setup();
    const { unmount } = render(
      <EntryScreen
        api={fakeApi()}
        boot={bootstrapFixture}
        onCatalogChanged={vi.fn()}
        wait={noWait}
      />,
    );
    await fillCoffee(user);
    expect(second.hide).not.toHaveBeenCalled(); // сумма набирается — кнопка на месте
    unmount(); // другая вкладка
    expect(second.hide).toHaveBeenCalledOnce();
  });

  it("keeps the two buttons in the screen on an older Telegram", async () => {
    const { main, second } = inTelegram(false);
    const user = userEvent.setup();
    renderEntry(fakeApi());
    await fillCoffee(user);
    expect(screen.getByRole("button", { name: "Зачислить" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Списать" })).toBeTruthy();
    expect(main.setParams).not.toHaveBeenCalled(); // панель не показана
    expect(second.setParams).not.toHaveBeenCalled();
  });

  it("keeps one «Записать» on Telegram's bar for incomes", async () => {
    const { main, second } = inTelegram();
    const user = userEvent.setup();
    renderEntry(fakeApi(), vi.fn(), ALL_GROUPS);
    await user.click(screen.getByRole("button", { name: "ДОХОДЫ" }));
    await user.click(screen.getByRole("button", { name: "зарплата" }));
    for (const key of ["4", "8", "000"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    expect(main.setParams.mock.lastCall?.[0]).toEqual({
      text: expect.stringMatching(/^Записать 48\s000 UZS$/),
      is_visible: true,
    });
    expect(second.setParams).not.toHaveBeenCalled();
  });

  // 2. после записи панель прячется без крутилки: иначе она осталась бы на кнопке
  it.each(["main", "second"] as const)(
    "stops the %s button spinning once the sum step closes",
    async (which) => {
      const buttons = inTelegram();
      let finish: (result: ExpenseResult) => void = () => undefined;
      const addExpense = vi.fn<Api["addExpense"]>(
        () =>
          new Promise((resolve) => {
            finish = resolve;
          }),
      );
      const user = userEvent.setup();
      renderEntry(fakeApi({ addExpense }));
      await fillCoffee(user);
      const pressed = buttons[which];
      await tapPanel(pressed);
      expect(pressed.showProgress).toHaveBeenCalled(); // крутится, пока ждёт
      await act(async () => finish(writtenFixture));
      expect(screen.getByRole("button", { name: "ЕДА" })).toBeTruthy(); // шаг ушёл
      expect(lastCall(pressed.hideProgress)).toBeGreaterThan(
        lastCall(pressed.showProgress),
      );
    },
  );
});
