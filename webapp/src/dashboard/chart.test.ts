import { describe, expect, it } from "vitest";
import { chartPaths } from "./chart";

describe("chartPaths", () => {
  it("draws the plan for the whole month and the fact up to today", () => {
    const points = [
      { day: "2026-10-01", plan: 100, fact: 90 },
      { day: "2026-10-02", plan: 80, fact: 85 },
      { day: "2026-10-03", plan: 60, fact: null },
    ];
    const paths = chartPaths(points, 200, 100);
    expect(paths.plan).toBe("M0.0,0.0 L100.0,50.0 L200.0,100.0");
    expect(paths.fact).toBe("M0.0,25.0 L100.0,37.5");
    expect([paths.min, paths.max]).toEqual([60, 100]);
  });
  it("handles an empty table", () => {
    expect(chartPaths([], 200, 100)).toEqual({
      plan: "",
      fact: "",
      min: 0,
      max: 0,
    });
  });
});
