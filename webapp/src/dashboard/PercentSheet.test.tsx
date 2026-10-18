import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { Api } from "../api";
import { savingsFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import PercentSheet from "./PercentSheet";

function month() {
  if (!savingsFixture.month) throw new Error("fixture");
  return savingsFixture.month;
}

describe("PercentSheet", () => {
  it("steps the percent and shows what it gives", async () => {
    const preview = {
      ...savingsFixture,
      month: { ...month(), percent: 26, capacity: 10_400_000 },
    };
    const api = fakeApi({
      savings: vi.fn<Api["savings"]>().mockResolvedValue(preview),
    });
    const user = userEvent.setup();
    render(
      <PercentSheet
        api={api}
        data={savingsFixture}
        onSaved={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByText("25 %")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Больше на 1 %" }));
    expect(screen.getByText("26 %")).toBeTruthy();
    await vi.waitFor(() =>
      expect(api.savings).toHaveBeenCalledWith(false, { percent: 26 }),
    );
    expect(await screen.findByText("в месяц ≈ 10,40 млн")).toBeTruthy();
  });

  it("lists what each piggy will have by its deadline", () => {
    render(
      <PercentSheet
        api={fakeApi()}
        data={savingsFixture}
        onSaved={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    const list = screen.getByRole("list", { name: "К срокам" });
    const rows = within(list).getAllByRole("listitem");
    // суммы с неразрывными пробелами — сравниваем по словам
    expect(rows.map((row) => row.textContent?.replace(/\s+/g, " "))).toEqual([
      "Новый год · 20.1212,00 млн из 12,00 млн ✓",
      "Отпуск · 01.062 000 $ из 2 500 $",
    ]);
    expect(screen.getByText("Чтобы успеть всё к срокам — 30 %")).toBeTruthy();
  });

  it("saves a quick choice", async () => {
    const api = fakeApi();
    const onSaved = vi.fn();
    const user = userEvent.setup();
    render(
      <PercentSheet
        api={api}
        data={savingsFixture}
        onSaved={onSaved}
        onClose={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("radio", { name: "5 %" }));
    await user.click(screen.getByRole("button", { name: "Сохранить 5 %" }));
    expect(api.setPercent).toHaveBeenCalledWith(5);
    await vi.waitFor(() =>
      expect(onSaved).toHaveBeenCalledWith(savingsFixture),
    );
  });
});
