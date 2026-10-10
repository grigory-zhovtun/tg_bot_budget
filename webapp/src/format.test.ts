import { describe, expect, it } from "vitest";
import { dayLabel, money, shiftDay, short, withCurrency } from "./format";

describe("money", () => {
  it("groups thousands like the bot", () => {
    expect(money(48000)).toBe("48 000");
  });
  it("keeps cents for currency cards", () => {
    expect(money(1824.86, 2)).toBe("1 824,86");
    expect(withCurrency(12.5, "USD")).toBe("12,50 USD");
    expect(withCurrency(48000, "UZS")).toBe("48 000 UZS");
  });
  it("marks negatives with a minus sign", () => {
    expect(money(-1500)).toBe("−1 500");
  });
  it("shortens millions", () => {
    expect(short(8_520_000)).toBe("8,52 млн");
    expect(short(-8_520_000)).toBe("−8,52 млн");
    expect(short(650_000)).toBe("650 000");
  });
});

describe("days", () => {
  it("shifts across months", () => {
    expect(shiftDay("2026-10-01", -1)).toBe("2026-09-30");
  });
  it("names today and yesterday", () => {
    expect(dayLabel("2026-10-10", "2026-10-10")).toBe("Сегодня");
    expect(dayLabel("2026-10-09", "2026-10-10")).toBe("Вчера");
    expect(dayLabel("2026-10-01", "2026-10-10")).toBe("01.10");
  });
});
