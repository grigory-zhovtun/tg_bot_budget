import { describe, expect, it } from "vitest";
import {
  dayLabel,
  deviceDay,
  daysBetween,
  inCurrency,
  longDay,
  itemAmount,
  money,
  moneyText,
  shiftDay,
  short,
  signedMoney,
  withCurrency,
} from "./format";

/** Пробелы в ожидаемых строках — неразрывные, как в форматтерах. */
const nb = (text: string) => text.replace(/ /g, "\u00a0");

describe("money", () => {
  it("groups thousands like the bot", () => {
    expect(money(48000)).toBe("48 000");
  });
  it("keeps cents for currency cards", () => {
    expect(money(1312.45, 2)).toBe("1 312,45");
    expect(withCurrency(12.5, "USD")).toBe("12,50 USD");
    expect(withCurrency(48000, "UZS")).toBe("48 000 UZS");
  });
  it("marks negatives with a minus sign", () => {
    expect(money(-1500)).toBe("−1 500");
  });
  it("shortens millions", () => {
    expect(short(8_520_000)).toBe("8,52 млн");
    expect(short(-8_520_000)).toBe("−8,52 млн");
    expect(short(640_000)).toBe("640 000");
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

describe("itemAmount", () => {
  it("shows incomes in sums and payments in their currency", () => {
    expect(itemAmount(3_150_000, "UZS", 3_150_000)).toBe("+3,15 млн");
    expect(itemAmount(-380, "USD", -4_560_000)).toBe("−380,00 USD");
  });
});

describe("longDay", () => {
  it("names the day with the month", () => {
    expect(longDay("2026-10-18")).toBe("18 октября");
    expect(longDay("2027-03-01")).toBe("1 марта");
  });
});

describe("daysBetween", () => {
  it("counts whole days from one ISO date to another", () => {
    expect(daysBetween("2026-10-09", "2026-10-15")).toBe(6);
    expect(daysBetween("2026-10-15", "2026-10-15")).toBe(0);
    expect(daysBetween("2026-09-28", "2026-10-03")).toBe(5);
  });
});

describe("deviceDay", () => {
  it("is the date on the phone clock, not in UTC", () => {
    expect(deviceDay(new Date(2026, 9, 17, 0, 30))).toBe("2026-10-17");
    expect(deviceDay(new Date(2026, 0, 5, 23, 59))).toBe("2026-01-05");
  });
});

describe("inCurrency", () => {
  it("puts the sign after the number like the bot", () => {
    expect(inCurrency(380, "USD")).toBe(nb("380 $"));
    expect(inCurrency(75_000, "RUB")).toBe(nb("75 000 ₽"));
    expect(inCurrency(50, "EUR")).toBe(nb("50 EUR"));
    expect(inCurrency(48_000, "UZS")).toBe(nb("48 000"));
    expect(inCurrency(1312.45, "USD", true)).toBe(nb("1 312,45 $"));
  });
});

describe("moneyText", () => {
  it("keeps cents only for a fractional amount in a currency", () => {
    expect(moneyText(1500.25, "USD")).toBe(nb("1 500,25 $"));
    expect(moneyText(1000, "USD")).toBe(nb("1 000 $"));
    expect(moneyText(5_250_000.4, "UZS")).toBe(nb("5 250 000"));
  });
  it("never shows a minus zero", () => {
    expect(moneyText(-0.001, "USD")).toBe(nb("0 $"));
  });
});

describe("signedMoney", () => {
  it("marks incomes with a plus and payments with a minus", () => {
    expect(signedMoney(2_000_000, "UZS")).toBe(nb("+2 000 000"));
    expect(signedMoney(-380, "USD")).toBe(nb("−380 $"));
    expect(signedMoney(-380.5, "USD")).toBe(nb("−380,50 $"));
  });
});
