import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  type Api,
  type Dashboard,
  type ExpenseInput,
  type ExpenseResult,
} from "../api";
import {
  bootstrapFixture,
  dashboardFixture,
  writtenFixture,
} from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import EntryScreen from "./EntryScreen";

type User = ReturnType<typeof userEvent.setup>;

function renderEntry(api: Api, onCatalogChanged: () => void = vi.fn()): void {
  render(
    <EntryScreen
      api={api}
      boot={bootstrapFixture}
      onCatalogChanged={onCatalogChanged}
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

function sent(api: Api, call = 0): ExpenseInput {
  const input = vi.mocked(api.addExpense).mock.calls[call]?.[0];
  if (!input) throw new Error("addExpense was not called");
  return input;
}

describe("EntryScreen", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("records an expense and comes back to the groups", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.type(screen.getByLabelText("Комментарий"), "латте");
    await user.click(
      screen.getByRole("button", { name: /Записать 48\s000 UZS/ }),
    );
    expect(sent(api)).toMatchObject({
      source: "VISA 9120 UZS",
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
    await user.click(screen.getByRole("button", { name: /Записать/ }));
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
    await user.click(screen.getByRole("button", { name: /Записать/ }));
    expect(sent(api).day).toBe("2026-10-09");
  });

  it("keeps the input and the entry id when the sheet is down", async () => {
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
    await user.click(screen.getByRole("button", { name: /Записать/ }));
    expect((await screen.findByRole("alert")).textContent).toBe(
      "Таблица не ответила — попробуйте ещё раз",
    );
    await user.click(
      screen.getByRole("button", { name: /Записать 48\s000 UZS/ }),
    );
    expect(sent(api, 1).entry_id).toBe(sent(api, 0).entry_id);
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
    const button = screen.getByRole("button", { name: /Записать/ });
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
    await user.click(screen.getByRole("button", { name: /Записать/ }));
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
    await user.click(screen.getByRole("radio", { name: "VISA 4058 USD" }));
    await user.click(screen.getByRole("button", { name: "ЕДА" }));
    await user.click(screen.getByRole("button", { name: "кофе" }));
    for (const key of ["1", "2", ",", "5"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    await user.click(
      screen.getByRole("button", { name: /Записать 12,50 USD/ }),
    );
    expect(sent(api)).toMatchObject({
      source: "VISA 4058 USD",
      amount: "12.5",
    });
  });

  it("undoes the last write from the summary", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: /Записать/ }));
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
    await user.click(screen.getByRole("button", { name: /Записать/ }));
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
    expect(await screen.findByText("сверх на 300 000")).toBeTruthy(); // ЕДА
    await user.click(screen.getByRole("button", { name: /ЕДА/ }));
    expect(await screen.findByText("осталось 50 000")).toBeTruthy(); // кофе
    expect(screen.getByText("сверх на 350 000")).toBeTruthy(); // кафе
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
    await user.click(screen.getByRole("button", { name: /Записать/ }));
    await screen.findByText(/✅ 48 000 UZS/);
    expect(api.dashboard).toHaveBeenCalledTimes(2);
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
    await user.click(screen.getByRole("button", { name: /Записать/ }));
    expect(await screen.findByText("сверх на 400 000")).toBeTruthy();
    await act(async () => {
      answerFirst(dashboardFixture);
    });
    expect(screen.getByText("сверх на 400 000")).toBeTruthy();
    expect(screen.queryByText("сверх на 300 000")).toBeNull();
  });

  it("hides the plan it could not refresh after a write", async () => {
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockResolvedValueOnce(dashboardFixture)
      .mockRejectedValueOnce(new ApiError(503, "sheets_unavailable", "нет"));
    const user = userEvent.setup();
    renderEntry(fakeApi({ dashboard }));
    expect(await screen.findByText("сверх на 300 000")).toBeTruthy();
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: /Записать/ }));
    await screen.findByText(/✅ 48 000 UZS/);
    await waitFor(() => {
      expect(screen.queryByText(/^(осталось|сверх на)/)).toBeNull();
    });
  });

  it("updates what is left after an undo", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    renderEntry(api);
    await fillCoffee(user);
    await user.click(screen.getByRole("button", { name: /Записать/ }));
    await user.click(await screen.findByRole("button", { name: /Отменить/ }));
    await screen.findByText(/Удалил из таблицы/);
    expect(api.dashboard).toHaveBeenCalledTimes(3);
  });
});
