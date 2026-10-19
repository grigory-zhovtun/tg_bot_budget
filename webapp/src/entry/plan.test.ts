import { describe, expect, it } from "vitest";
import { dashboardFixture } from "../mocks/fixtures";
import { planIndex, planLine } from "./plan";

describe("plan on the tiles", () => {
  it("says what is left, what is over and what is outside the plan", () => {
    expect(planLine({ plan: 500_000, fact: 300_000 })).toEqual({
      head: "осталось",
      sum: "200\u00a0000",
      status: "ok",
      filled: 0.6,
    });
    expect(planLine({ plan: 400_000, fact: 450_000 })).toMatchObject({
      head: "⚠️ сверх на",
      sum: "50\u00a0000",
    });
    expect(planLine({ plan: 0, fact: 120_000 })).toMatchObject({
      head: "вне плана",
      sum: "120\u00a0000",
    });
    expect(planLine({ plan: 0, fact: 0 })).toBeNull();
    expect(planLine(undefined)).toBeNull();
  });

  it("writes the sum whole, without «млн»", () => {
    expect(planLine({ plan: 20_000_000, fact: 7_654_322 })?.sum).toBe(
      "12\u00a0345\u00a0678",
    );
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
