import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import MonthNav, { monthTitle } from "./MonthNav";

describe("MonthNav", () => {
  it("names a month of another year with the year", () => {
    expect(monthTitle("2026-10", "2026-11")).toBe("Октябрь");
    expect(monthTitle("2025-12", "2026-01")).toBe("Декабрь 2025");
  });

  it("turns over the new year in both directions", async () => {
    const onMonth = vi.fn();
    const user = userEvent.setup();
    const { rerender } = render(
      <MonthNav
        month="2026-01"
        current="2026-03"
        earliest="2025-06"
        onMonth={onMonth}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Предыдущий месяц" }));
    expect(onMonth).toHaveBeenLastCalledWith("2025-12");
    rerender(
      <MonthNav
        month="2025-12"
        current="2026-03"
        earliest="2025-06"
        onMonth={onMonth}
      />,
    );
    expect(screen.getByRole("heading").textContent).toBe("Декабрь 2025");
    await user.click(screen.getByRole("button", { name: "Следующий месяц" }));
    expect(onMonth).toHaveBeenLastCalledWith("2026-01");
    await user.click(screen.getByRole("button", { name: "Вернуться в март" }));
    expect(onMonth).toHaveBeenLastCalledWith("2026-03");
  });

  it("stops at the earliest month and lets go back before it is known", () => {
    const { rerender } = render(
      <MonthNav
        month="2026-08"
        current="2026-10"
        earliest="2026-08"
        onMonth={vi.fn()}
      />,
    );
    const back = () => screen.getByRole("button", { name: "Предыдущий месяц" });
    expect(back()).toHaveProperty("disabled", true);
    rerender(
      <MonthNav
        month="2026-08"
        current="2026-10"
        earliest={null}
        onMonth={vi.fn()}
      />,
    );
    expect(back()).toHaveProperty("disabled", false);
  });

  it("has arrows a finger can hit and a bold month name", () => {
    render(
      <MonthNav
        month="2026-10"
        current="2026-10"
        earliest={null}
        onMonth={vi.fn()}
      />,
    );
    // 44 px — не меньше пальца
    expect(
      screen.getByRole("button", { name: "Следующий месяц" }).className,
    ).toContain("size-11");
    expect(screen.getByRole("heading").className).toContain("font-extrabold");
  });
});
