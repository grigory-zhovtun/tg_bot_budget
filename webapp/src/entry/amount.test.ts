import { describe, expect, it } from "vitest";
import {
  amountValue,
  apiAmount,
  displayAmount,
  MAX_WHOLE_DIGITS,
  press,
  type Key,
} from "./amount";

function typed(keys: Key[], decimals = false): string {
  return keys.reduce((text, key) => press(text, key, decimals), "");
}

describe("amount keypad", () => {
  it("builds sums with 000", () => {
    expect(typed(["4", "8", "000"])).toBe("48000");
  });
  it("ignores leading zeros and 000 at the start", () => {
    expect(typed(["0", "0", "000", "5"])).toBe("5");
  });
  it("allows a comma and two decimals only on currency cards", () => {
    expect(typed(["1", "2", ",", "5", "0", "9"], true)).toBe("12,50");
    expect(typed(["1", "2", ","])).toBe("12");
    expect(typed([",", "5"], true)).toBe("0,5");
  });
  it("erases the last character", () => {
    expect(typed(["1", "2", "⌫"])).toBe("1");
  });
  it("stops at the longest sum the bot accepts", () => {
    const nines = Array<Key>(MAX_WHOLE_DIGITS + 1).fill("9");
    expect(typed(nines)).toHaveLength(MAX_WHOLE_DIGITS);
  });
  it("converts for the API and the screen", () => {
    expect(apiAmount("12,5")).toBe("12.5");
    expect(apiAmount("12,")).toBe("12");
    expect(amountValue("12,5")).toBe(12.5);
    expect(amountValue("")).toBe(0);
    expect(displayAmount("48000")).toBe("48 000");
    expect(displayAmount("")).toBe("0");
  });
});
