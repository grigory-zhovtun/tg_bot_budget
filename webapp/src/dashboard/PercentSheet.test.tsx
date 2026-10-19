import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { Api, SavingsScreen } from "../api";
import { savingsFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import { nb } from "../test/text";
import PercentSheet from "./PercentSheet";

function show(data: SavingsScreen = savingsFixture) {
  render(
    <PercentSheet
      api={fakeApi()}
      data={data}
      onSaved={vi.fn()}
      onClose={vi.fn()}
    />,
  );
}

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
    expect(await screen.findByText("в месяц ≈ 10 400 000")).toBeTruthy();
  });

  it("lists what each piggy will have by its deadline", () => {
    show();
    expect(screen.getByText("в месяц ≈ 10 000 000").className).toContain(
      "text-line",
    );
    const list = screen.getByRole("list", { name: "К срокам" });
    const rows = within(list).getAllByRole("listitem");
    expect(
      rows.map((row) =>
        [...row.querySelectorAll("p")].map((line) => line.textContent),
      ),
    ).toEqual([
      ["Новый год, к 20.12", nb("будет 12 000 000"), nb("из 12 000 000 ✓")],
      ["Отпуск, к 01.06", nb("⚠️ будет 2 000 $"), nb("из 2 500 $")],
    ]);
  });

  it("warns as the summary does: deadlines and money for life", () => {
    show();
    const deadlines = screen.getByText("⚠️ К срокам нужно 30 %");
    expect(deadlines.className).toContain("text-danger");
    expect(screen.getByText("⚠️ На жизнь не хватит")).toBeTruthy();
    expect(screen.getByText("останется 31 800 000")).toBeTruthy();
    expect(screen.getByText("обычные траты 34 000 000")).toBeTruthy();
    expect(screen.getByText("разница 2 200 000")).toBeTruthy();
  });

  it("says so when the percent is enough for both", () => {
    const month = savingsFixture.month;
    if (!month || !savingsFixture.enough) throw new Error("fixture");
    show({
      ...savingsFixture,
      month: { ...month, percent: 30 },
      enough: { ...savingsFixture.enough, gap: 500_000 },
    });
    expect(screen.getByText("✓ Успеваем всё к срокам")).toBeTruthy();
    expect(screen.getByText("✓ На жизнь хватает")).toBeTruthy();
    expect(screen.getByText("запас 500 000")).toBeTruthy();
    expect(screen.queryByText(/разница/)).toBeNull();
  });

  it("writes the quick choice and the button big", () => {
    show();
    const quick = screen.getByRole("radio", { name: "5 %" });
    expect(quick.className).toContain("text-line");
    expect(quick.className).toContain("font-extrabold");
    const save = screen.getByRole("button", { name: "Сохранить 25 %" });
    expect(save.className).toContain("text-lg");
    expect(save.className).toContain("font-extrabold");
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
