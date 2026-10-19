import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import PlanEvents from "./PlanEvents";

describe("PlanEvents", () => {
  it("draws the conflict ring inside the row so the list frame does not clip it", () => {
    render(
      <PlanEvents
        rows={[
          {
            slot: 0,
            name: "Аванс",
            day: 15,
            amount: 5_000_000,
            currency: "UZS",
            edited: false,
            conflict: true,
            note: null,
          },
        ]}
        onPick={vi.fn()}
      />,
    );
    const row = screen.getByRole("button");
    expect(row.getAttribute("data-conflict")).toBe("true");
    expect(row.className).toContain("ring-2");
    expect(row.className).toContain("ring-inset");
  });
});
