import { describe, expect, it } from "vitest";
import { dashboardFixture } from "../mocks/fixtures";
import { planIndex, planLine } from "./plan";

describe("plan on the tiles", () => {
  it("says what is left, what is over and what is outside the plan", () => {
    expect(planLine({ plan: 500_000, fact: 300_000 })).toEqual({
      text: "осталось 200\u00a0000",
      status: "ok",
      filled: 0.6,
    });
    expect(planLine({ plan: 400_000, fact: 450_000 })?.text).toBe(
      "сверх на 50\u00a0000",
    );
    expect(planLine({ plan: 0, fact: 120_000 })?.text).toBe(
      "вне плана · 120\u00a0000",
    );
    expect(planLine({ plan: 0, fact: 0 })).toBeNull();
    expect(planLine(undefined)).toBeNull();
  });

  it("finds groups and subcategories of the month", () => {
    const index = planIndex(dashboardFixture);
    expect(index.group("🍔 ЕДА")).toEqual({ plan: 3_000_000, fact: 3_300_000 });
    expect(index.sub("🍔 ЕДА", "кофе")).toEqual({
      plan: 300_000,
      fact: 250_000,
    });
    expect(index.sub("🍔 ЕДА", "кальян")).toBeUndefined();
  });

  it("knows nothing without the month tab", () => {
    const index = planIndex({ ...dashboardFixture, status: "no_month_tab" });
    expect(index.group("🍔 ЕДА")).toBeUndefined();
  });
});
