import { describe, expect, it } from "vitest";
import {
  capital,
  days,
  monthDative,
  monthIn,
  monthName,
  monthRange,
  monthShort,
  nextMonth,
  plural,
} from "./months";

describe("month names", () => {
  it("names months the Russian way", () => {
    expect(monthName("2026-09")).toBe("сентябрь");
    expect(monthShort("2026-09")).toBe("сен");
    expect(monthDative("2026-09")).toBe("сентябрю");
    expect(capital("октябрь")).toBe("Октябрь");
    expect(monthRange(["2026-07", "2026-08", "2026-09"])).toBe("июль–сентябрь");
  });

  it("puts numbers in the right form", () => {
    expect(
      [1, 2, 5, 11, 21, 22, 112].map((n) => plural(n, "a", "b", "c")),
    ).toEqual(["a", "b", "c", "c", "a", "b", "c"]);
    expect(days(10)).toBe("10 дней");
    expect(days(1)).toBe("1 день");
  });
  it("knows the next month and says «в ноябре»", () => {
    expect(nextMonth("2026-10")).toBe("2026-11");
    expect(nextMonth("2026-12")).toBe("2027-01");
    expect(monthIn("2026-11")).toBe("в ноябре");
  });
});
