import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { bootstrapFixture, savingsFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import SaveSheet from "./SaveSheet";

describe("SaveSheet", () => {
  it("offers this month's part and shows where it goes", () => {
    render(
      <SaveSheet
        api={fakeApi()}
        boot={bootstrapFixture}
        data={savingsFixture}
        onSaved={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByLabelText("Сумма").textContent).toContain(
      "6\u00a0000\u00a0000",
    );
    expect(screen.getByText("«Новый год» — 6,00 млн")).toBeTruthy();
    expect(screen.queryByText(/Подушке/)).toBeNull();
  });

  it("a bigger sum leaves the rest to the cushion", async () => {
    const user = userEvent.setup();
    render(
      <SaveSheet
        api={fakeApi()}
        boot={bootstrapFixture}
        data={savingsFixture}
        onSaved={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("button", { name: "000" }));
    expect(screen.getByText("«Новый год» — 6,00 млн")).toBeTruthy();
    expect(
      screen.getByText("Подушке — 5994,00 млн, в журнал не пишется"),
    ).toBeTruthy();
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
      amount: "6000000",
      currency: "UZS",
    });
    await vi.waitFor(() =>
      expect(onSaved).toHaveBeenCalledWith(savingsFixture),
    );
  });
});
