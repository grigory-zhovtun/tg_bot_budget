import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { DaySpend } from "../api";
import { dashboardFixture } from "../mocks/fixtures";
import SpendingChart from "./SpendingChart";

function show(
  spending: DaySpend[] = dashboardFixture.spending,
  today = "2026-10-10",
  limit: number | null = 500_000,
) {
  return render(
    <SpendingChart
      spending={spending}
      today={today}
      limit={limit}
      planPerDay={450_000}
    />,
  );
}

describe("SpendingChart", () => {
  it("leads with the margin left by the finished days", () => {
    show();
    const verdict = screen.getByText("✓ Запас 1 100 000");
    expect(verdict.className).toContain("text-fact");
    expect(verdict.className).toContain("text-ok");
    expect(screen.getByText("потрачено 2 950 000 за 9 дней")).toBeTruthy();
    expect(screen.getByText("по плану 4 050 000")).toBeTruthy();
  });

  it("says how far the finished days went over the plan", () => {
    show(
      [
        { day: "2026-10-01", spent: 900_000, limit: 450_000 },
        { day: "2026-10-02", spent: 800_000, limit: 435_000 },
        { day: "2026-10-03", spent: 0, limit: 420_000 },
      ],
      "2026-10-03",
    );
    expect(screen.getByText("⚠️ Сверх на 800 000").className).toContain(
      "text-danger",
    );
  });

  it("says when the finished days match the plan exactly", () => {
    show(
      [
        { day: "2026-10-01", spent: 450_000, limit: 450_000 },
        { day: "2026-10-02", spent: 0, limit: 450_000 },
      ],
      "2026-10-02",
    );
    expect(screen.getByText("✓ Ровно по плану")).toBeTruthy();
  });

  it("labels the chart in big whole numbers", () => {
    const { container } = show();
    const labels = [...container.querySelectorAll("svg text")];
    expect(labels.length).toBeGreaterThan(0);
    for (const label of labels) {
      expect(label.getAttribute("font-size")).toBe("15");
    }
    expect(screen.getByText("можно 500 000 в день")).toBeTruthy();
    expect(
      screen.getByText("план 450 000 в день").closest("span")?.className,
    ).toContain("whitespace-nowrap");
  });

  it("keeps a long label inside the chart", () => {
    show(dashboardFixture.spending.slice(0, 4), "2026-10-15", 12_345_678);
    const label = screen.getByText("можно 12 345 678 в день");
    // середина месяца, длинная сумма: подпись прижата к правому краю, а не уходит за него
    expect(label.getAttribute("text-anchor")).toBe("end");
    expect(label.getAttribute("x")).toBe("318");
  });
});
