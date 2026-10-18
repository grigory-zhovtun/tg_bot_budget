import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type Piggy } from "../api";
import { bootstrapFixture, savingsFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import PiggySheet from "./PiggySheet";

type User = ReturnType<typeof userEvent.setup>;

function piggy(index: number, changes: Partial<Piggy> = {}): Piggy {
  const found = savingsFixture.piggies[index];
  if (!found) throw new Error("fixture");
  return { ...found, ...changes };
}

function open(api: Api, current: Piggy | null, onSaved = vi.fn()) {
  render(
    <PiggySheet
      api={api}
      boot={bootstrapFixture}
      data={savingsFixture}
      piggy={current}
      onSaved={onSaved}
      onClose={vi.fn()}
    />,
  );
  return onSaved;
}

async function keys(user: User, ...pressed: string[]): Promise<void> {
  for (const key of pressed) {
    await user.click(screen.getByRole("button", { name: key }));
  }
}

describe("PiggySheet", () => {
  it("creates a piggy with a target, a yearly deadline and an article", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    const onSaved = open(api, null);
    expect(screen.getByRole("dialog", { name: "Новая копилка" })).toBeTruthy();
    await user.type(screen.getByLabelText("Название"), "Кафе с друзьями");
    await keys(user, "1", "2", "000", "000");
    fireEvent.change(screen.getByLabelText("Срок"), {
      target: { value: "2026-12-20" },
    });
    await user.click(screen.getByRole("radio", { name: "Каждый год" }));
    await user.click(screen.getByRole("button", { name: "🍔 ЕДА" }));
    await user.click(screen.getByRole("button", { name: "кафе" }));
    await user.click(screen.getByRole("button", { name: "Создать" }));
    expect(api.addPiggy).toHaveBeenCalledWith({
      entry_id: expect.any(String),
      name: "Кафе с друзьями",
      target: "12000000",
      currency: "UZS",
      deadline: "2026-12-20",
      repeat: "yearly",
      group: "🍔 ЕДА",
      subcategory: "кафе",
    });
    await vi.waitFor(() =>
      expect(onSaved).toHaveBeenCalledWith(savingsFixture),
    );
  });

  it("a long goal may have neither a deadline nor a target", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    open(api, null);
    await user.type(screen.getByLabelText("Название"), "Машина");
    await user.click(screen.getByRole("radio", { name: "USD" }));
    await user.click(screen.getByRole("button", { name: "Без срока" }));
    // «каждый год» — только со сроком
    expect(screen.queryByRole("radio", { name: "Каждый год" })).toBeNull();
    await user.click(screen.getByRole("button", { name: "Без цели" }));
    await user.click(screen.getByRole("button", { name: "Создать" }));
    expect(api.addPiggy).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "Машина",
        target: null,
        currency: "USD",
        deadline: null,
        repeat: "once",
        group: null,
        subcategory: null,
      }),
    );
  });

  it("saves the changes of a piggy", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    open(api, piggy(0));
    const name = screen.getByLabelText("Название");
    await user.clear(name);
    await user.type(name, "Праздники");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.updatePiggy).toHaveBeenCalledWith(
      "p3k9za",
      expect.objectContaining({ name: "Праздники", target: "12000000" }),
    );
  });

  it("tells this month's part and the next ones", () => {
    open(fakeApi(), piggy(0));
    expect(
      screen.getByText("В этом месяце 6,00 млн, в ноябре — 6,00 млн"),
    ).toBeTruthy();
  });

  it("names the months of a longer run in the piggy's currency", () => {
    open(fakeApi(), piggy(1));
    expect(
      screen.getByText(
        "В этом месяце ничего, с декабря по май — по 417 $ в месяц",
      ),
    ).toBeTruthy();
  });

  it("sets this month's part aside by default", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    open(api, piggy(0));
    await user.click(screen.getByRole("button", { name: "Отложил…" }));
    expect(screen.getByLabelText("Сумма").textContent).toContain(
      "6\u00a0000\u00a0000",
    );
    await user.click(screen.getByRole("button", { name: "Записать" }));
    expect(api.movePiggy).toHaveBeenCalledWith("p3k9za", {
      entry_id: expect.any(String),
      kind: "saved",
      amount: "6000000",
    });
  });

  it("spends from the piggy", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    open(api, piggy(1));
    await user.click(screen.getByRole("button", { name: "Потратил…" }));
    await keys(user, "1", "5", "0");
    await user.click(screen.getByRole("button", { name: "Записать" }));
    expect(api.movePiggy).toHaveBeenCalledWith(
      "tr0001",
      expect.objectContaining({ kind: "spent", amount: "150" }),
    );
  });

  it("writes off what is left after the deadline", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    open(api, piggy(1, { status: "leftover", leftover: 300, balance: 300 }));
    await user.click(
      screen.getByRole("button", { name: "Списать как потраченное" }),
    );
    expect(api.movePiggy).toHaveBeenCalledWith(
      "tr0001",
      expect.objectContaining({ kind: "write_off", amount: "300" }),
    );
  });

  it("closes on the second press", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    open(api, piggy(0));
    await user.click(screen.getByRole("button", { name: "Закрыть копилку" }));
    expect(api.closePiggy).not.toHaveBeenCalled();
    await user.click(
      screen.getByRole("button", {
        name: "Закрыть — остаток вернётся в подушку",
      }),
    );
    expect(api.closePiggy).toHaveBeenCalledWith("p3k9za");
  });

  it("lists the history and takes a spending off the piggy", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    open(
      api,
      piggy(0, {
        history: [
          {
            day: "2026-10-05",
            amount: 6_000_000,
            kind: "отложил",
            who: "Иван",
          },
        ],
        spends: [
          {
            key: "2026-10-08|🎉 ПРАЗДНИК|сезонный|400000.00|UZS|VISA|",
            day: "2026-10-08",
            group: "🎉 ПРАЗДНИК",
            subcategory: "сезонный",
            amount: 400_000,
          },
        ],
      }),
    );
    expect(screen.getByText("05.10 отложил 6,00 млн")).toBeTruthy();
    expect(screen.getByText("08.10 ПРАЗДНИК / сезонный 400 000")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Не из копилки" }));
    expect(api.excludeSpend).toHaveBeenCalledWith(
      "p3k9za",
      "2026-10-08|🎉 ПРАЗДНИК|сезонный|400000.00|UZS|VISA|",
    );
  });

  it("puts the server's field errors under the fields", async () => {
    const api = fakeApi({
      addPiggy: vi.fn<Api["addPiggy"]>().mockRejectedValue(
        new ApiError(422, "validation", "Проверьте введённые данные", {
          deadline: "Срок — не раньше сегодня и не дальше чем через 5 лет",
        }),
      ),
    });
    const user = userEvent.setup();
    open(api, null);
    await user.type(screen.getByLabelText("Название"), "Новый год");
    await keys(user, "5", "000");
    await user.click(screen.getByRole("button", { name: "Создать" }));
    expect(
      await screen.findByText(
        "Срок — не раньше сегодня и не дальше чем через 5 лет",
      ),
    ).toBeTruthy();
  });

  it("sends one request when «Создать» is tapped twice", async () => {
    const api = fakeApi({
      addPiggy: vi
        .fn<Api["addPiggy"]>()
        .mockReturnValue(new Promise(() => undefined)),
    });
    const user = userEvent.setup();
    open(api, null);
    await user.type(screen.getByLabelText("Название"), "Новый год");
    await keys(user, "5", "000");
    const create = screen.getByRole("button", { name: "Создать" });
    fireEvent.click(create);
    fireEvent.click(create);
    expect(api.addPiggy).toHaveBeenCalledTimes(1);
  });
});
