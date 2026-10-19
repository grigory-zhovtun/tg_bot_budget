import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { bootstrapFixture, savingsFixture } from "../mocks/fixtures";
import { ApiError, type Api, type SavingsScreen } from "../api";
import { fakeApi } from "../test/fakeApi";
import { nb } from "../test/text";
import SaveSheet from "./SaveSheet";

/** Клетки строки «Куда ляжет» — по названию копилки или «подушка». */
function cells(name: string): string[] {
  const table = screen.getByRole("table", { name: "Куда ляжет" });
  const row = within(table).getByText(name).closest("tr");
  if (!row) throw new Error(`no row for ${name}`);
  return [...row.querySelectorAll("td")].map((cell) => cell.textContent ?? "");
}

function show(data: SavingsScreen = savingsFixture) {
  render(
    <SaveSheet
      api={fakeApi()}
      boot={bootstrapFixture}
      data={data}
      onSaved={vi.fn()}
      onClose={vi.fn()}
    />,
  );
}

describe("SaveSheet", () => {
  it("offers this month's part and shows where it goes", () => {
    show();
    expect(screen.getByLabelText("Сумма").textContent).toContain(
      "4\u00a0000\u00a0000",
    );
    const table = screen.getByRole("table", { name: "Куда ляжет" });
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((cell) => cell.textContent),
    ).toEqual(["копилка", "сумма"]);
    expect(cells("Новый год")).toEqual(["Новый год", nb("4 000 000")]);
    expect(screen.queryByText("подушка")).toBeNull();
    expect(screen.queryByText(/в журнал не пишется/)).toBeNull();
  });

  it("a bigger sum leaves the rest to the cushion", async () => {
    const user = userEvent.setup();
    show();
    await user.click(screen.getByRole("button", { name: "000" }));
    // взнос «Нового года», потом «Отпуску» — его «не хватит» 500 $, потом подушка
    expect(cells("Новый год")).toEqual(["Новый год", nb("6 000 000")]);
    expect(cells("Отпуск")).toEqual(["Отпуск", nb("500 $")]);
    expect(cells("подушка")).toEqual(["подушка", nb("3 988 000 000")]);
    expect(screen.getByText("подушка — в журнал не пишется")).toBeTruthy();
  });

  it("puts it all into the cushion when no piggy needs it", () => {
    show({
      ...savingsFixture,
      piggies: savingsFixture.piggies.map((piggy) => ({
        ...piggy,
        left_this_month: 0,
        status: piggy.status === "short" ? "on_track" : piggy.status,
      })),
    });
    const table = screen.getByRole("table", { name: "Куда ляжет" });
    expect(within(table).getAllByRole("row")).toHaveLength(2); // шапка и подушка
    expect(cells("подушка")).toEqual(["подушка", nb("4 000 000")]);
    expect(screen.getByText("подушка — в журнал не пишется")).toBeTruthy();
  });

  it("shows cents of a dollar part only when it has them", async () => {
    const user = userEvent.setup();
    show({
      ...savingsFixture,
      piggies: savingsFixture.piggies.map((piggy) =>
        piggy.name === "Отпуск" ? { ...piggy, short: 500.5 } : piggy,
      ),
    });
    await user.click(screen.getByRole("button", { name: "000" }));
    expect(cells("Отпуск")).toEqual(["Отпуск", nb("500,50 $")]);
  });

  it("writes the button big", () => {
    show();
    const write = screen.getByRole("button", { name: "Записать" });
    expect(write.className).toContain("text-lg");
    expect(write.className).toContain("font-extrabold");
  });

  it("writes the server's refusal under the sum big", async () => {
    const savePiggies = vi.fn<Api["savePiggies"]>().mockRejectedValue(
      new ApiError(422, "validation", "Проверьте введённые данные", {
        amount: "Сумма больше, чем можно отложить",
      }),
    );
    const user = userEvent.setup();
    render(
      <SaveSheet
        api={fakeApi({ savePiggies })}
        boot={bootstrapFixture}
        data={savingsFixture}
        onSaved={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Записать" }));
    const under = await screen.findByText("Сумма больше, чем можно отложить");
    expect(under.className).toContain("text-line");
    expect(under.className).toContain("font-bold");
    expect(under.className).not.toContain("text-sm");
  });

  it("writes the currencies in the text colour, not pale", () => {
    show();
    const dollars = screen.getByRole("radio", { name: "USD" });
    expect(dollars.className).toContain("text-line");
    expect(dollars.className).not.toContain("text-hint");
  });

  it("records the sum in one request", async () => {
    const api = fakeApi();
    const onSaved = vi.fn();
    const user = userEvent.setup();
    render(
      <SaveSheet
        api={api}
        boot={bootstrapFixture}
        data={savingsFixture}
        onSaved={onSaved}
        onClose={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Записать" }));
    expect(api.savePiggies).toHaveBeenCalledWith({
      entry_id: expect.any(String),
      amount: "4000000",
      currency: "UZS",
    });
    await vi.waitFor(() =>
      expect(onSaved).toHaveBeenCalledWith(savingsFixture),
    );
  });
});
