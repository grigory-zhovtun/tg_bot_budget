import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  type Api,
  type ExpenseInput,
  type ExpenseResult,
} from "../api";
import { bootstrapFixture, writtenFixture } from "../mocks/fixtures";
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
      day: "2026-10-10",
    });
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
    expect(sent(api).day).toBe("2026-10-10");
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
});
