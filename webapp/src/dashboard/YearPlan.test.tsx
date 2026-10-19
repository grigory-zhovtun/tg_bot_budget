import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import YearPlan from "./YearPlan";

const NAMES = { ny: "Новый год", trip: "Отпуск" };

describe("YearPlan", () => {
  it("puts every month's total in big text and each part on its own line", () => {
    render(
      <YearPlan
        names={NAMES}
        plan={[
          {
            month: "2026-12",
            total: 5_000_000,
            items: [{ id: "trip", amount: 5_000_000 }],
            cushion: 4_000_000,
            goals: [{ id: "ny", amount: 1_000_000 }],
            ready: ["ny"],
          },
        ]}
      />,
    );
    const month = screen.getByText("Декабрь — 10 000 000");
    expect(month.className).toContain("text-fact");
    const item = month.closest("li");
    if (!item) throw new Error("no month");
    expect(
      [...item.querySelectorAll("p")].map((line) => line.textContent),
    ).toEqual(
      [
        "Декабрь — 10 000 000",
        "Отпуск 5 000 000",
        "подушка 4 000 000",
        "Новый год 1 000 000",
        "✓ Новый год — собрано",
      ].map((line) => line.replace(/(\d) (?=\d)/g, "$1\u00a0")),
    );
    expect(within(item).getByText("✓ Новый год — собрано").className).toContain(
      "text-ok",
    );
  });
});
