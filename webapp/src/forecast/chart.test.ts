import { describe, expect, it } from "vitest";
import { geometry, labelDays } from "./chart";

const BOX = { width: 320, top: 10, bottom: 110, left: 8, right: 8 };

describe("forecast chart geometry", () => {
  it("puts today at the left edge and the last day at the right one", () => {
    const days = [
      { day: 15, plan: 100, pace: 100 },
      { day: 31, plan: 50, pace: 20 },
    ];
    const chart = geometry(days, BOX);
    expect([chart.x(15), chart.x(31)]).toEqual([8, 312]);
    expect(chart.slot).toBe(19); // 304 точки на 16 промежутков
  });

  it("puts zero inside the chart when a line goes below it", () => {
    const chart = geometry(
      [
        { day: 15, plan: 100, pace: 100 },
        { day: 16, plan: 50, pace: -100 },
      ],
      BOX,
    );
    expect(chart.below).toBe(true);
    expect([chart.y(100), chart.zero, chart.y(-100)]).toEqual([10, 60, 110]);
    expect(chart.pace).toBe("M8,10 L312,110");
  });

  it("keeps zero at the bottom when nothing is below it", () => {
    const chart = geometry([{ day: 15, plan: 100, pace: 50 }], BOX);
    expect(chart.below).toBe(false);
    expect(chart.zero).toBe(110);
  });

  it("draws one point in the middle on the last day of the month", () => {
    const chart = geometry([{ day: 31, plan: 100, pace: 100 }], BOX);
    expect(chart.x(31)).toBe(160);
    expect(chart.plan).toBe("M160,10");
  });

  it("labels today, every fifth day and the last one", () => {
    expect(labelDays(15, 31)).toEqual([15, 20, 25, 31]);
    expect(labelDays(1, 30)).toEqual([1, 5, 10, 15, 20, 25, 30]);
    expect(labelDays(29, 31)).toEqual([29, 31]);
    expect(labelDays(31, 31)).toEqual([31]);
  });
});
