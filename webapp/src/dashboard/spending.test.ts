import { describe, expect, it } from "vitest";
import type { DaySpend } from "../api";
import { spendingBars, spendingSummary } from "./spending";

const day = (n: number, spent: number, limit: number): DaySpend => ({
  day: `2026-10-${String(n).padStart(2, "0")}`,
  spent,
  limit,
});

const SIZE = { width: 310, top: 10, bottom: 110 }; // 31 день по 10, высота 100

describe("spendingBars", () => {
  const spending = [day(1, 500, 400), day(2, 300, 380), day(3, 100, 350)];

  it("colors each past day by its own limit, today apart, the rest as allowance", () => {
    const { bars } = spendingBars(spending, "2026-10-03", 350, 400, SIZE);
    expect(bars.map((bar) => bar.state)).toEqual([
      "over",
      "ok",
      "today",
      ...Array<string>(28).fill("ahead"),
    ]);
    expect(bars.slice(3).every((bar) => bar.value === 350)).toBe(true);
    expect(bars.slice(3).every((bar) => bar.tick === null)).toBe(true);
  });

  it("scales bars, limit ticks and the plan line to the tallest of them", () => {
    const { bars, planY } = spendingBars(
      spending,
      "2026-10-03",
      350,
      400,
      SIZE,
    );
    const [first, second] = bars;
    expect([first?.y, first?.height, first?.tick]).toEqual([10, 100, 30]);
    expect([second?.x, second?.width]).toEqual([11.7, 6.6]);
    expect(planY).toBe(30);
  });

  it("keeps a refund day at zero", () => {
    const { bars } = spendingBars(
      [day(1, -50, 400), day(2, 0, 410)],
      "2026-10-02",
      410,
      400,
      SIZE,
    );
    expect([bars[0]?.state, bars[0]?.height]).toEqual(["ok", 0]);
  });

  it("cuts a day far above the others so they stay readable", () => {
    const { bars } = spendingBars(
      [day(1, 100, 150), day(2, 120, 150), day(3, 1000, 150)],
      "2026-10-03",
      150,
      100,
      SIZE,
    );
    // шкала — до двух вторых по величине: 240
    expect(bars.map((bar) => bar.cut).slice(0, 3)).toEqual([
      false,
      false,
      true,
    ]);
    expect([bars[2]?.y, bars[2]?.value]).toEqual([10, 1000]);
    expect(bars[1]?.height).toBe(50);
  });

  it("leaves no allowance when the limit is gone", () => {
    const { bars } = spendingBars(
      [day(1, 900, 100), day(2, 50, -20)],
      "2026-10-02",
      -20,
      100,
      SIZE,
    );
    expect(bars.map((bar) => bar.state)).toEqual(["over", "today"]);
    expect(bars[1]?.tick).toBe(110); // лимит ниже нуля — чёрточка на нуле
  });

  it("counts the days of a short month", () => {
    const { bars } = spendingBars(
      [{ day: "2026-11-01", spent: 0, limit: 300 }],
      "2026-11-01",
      300,
      300,
      SIZE,
    );
    expect(bars).toHaveLength(30);
  });
});

describe("spendingSummary", () => {
  it("adds up the finished days against the plan", () => {
    const spending = [day(1, 500, 400), day(2, 300, 380), day(3, 100, 350)];
    expect(spendingSummary(spending, "2026-10-03", 400)).toEqual({
      days: 2,
      spent: 800,
      plan: 800,
    });
  });

  it("has nothing to add up on the first day", () => {
    expect(spendingSummary([day(1, 100, 400)], "2026-10-01", 400)).toBeNull();
  });
});
