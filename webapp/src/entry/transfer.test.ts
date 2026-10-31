import { describe, expect, it } from "vitest";
import { transferCards } from "../mocks/fixtures";
import { balanceOf, factLines, isTransfer, rateLine } from "./transfer";

/** Неразрывные пробелы money — обычными, для сравнения. */
const plain = (text: string) => text.replace(/\s/g, " ");

describe("rateLine", () => {
  it.each([
    [1_210_800, "UZS", 100, "USD", "курс: 1 $ = 12 108 сум"],
    [100, "USD", 1_210_000, "UZS", "курс: 1 $ = 12 100 сум"],
    [8_750, "RUB", 100, "USD", "курс: 1 $ = 87,50 ₽"],
    [10_000, "RUB", 1_400_000, "UZS", "курс: 1 ₽ = 140 сум"],
    [100, "EUR", 110, "USD", "курс: 1 EUR = 1,10 $"],
  ])(
    "puts the dearer currency first: %d %s for %d %s",
    (sent, sentCurrency, got, gotCurrency, line) => {
      expect(plain(rateLine(sent, sentCurrency, got, gotCurrency))).toBe(line);
    },
  );
});

describe("factLines", () => {
  it("writes one sum to both cards of one currency", () => {
    expect(
      factLines("VISA 1111 UZS", "HUMO 2222 UZS", 500_000, 0, false),
    ).toEqual([
      { card: "VISA 1111 UZS", amount: 500_000 },
      { card: "HUMO 2222 UZS", amount: -500_000 },
    ]);
  });

  it("writes each side in its currency across currencies", () => {
    expect(
      factLines("VISA 1111 UZS", "VISA 3333 USD", 1_210_800, 100, true),
    ).toEqual([
      { card: "VISA 1111 UZS", amount: 1_210_800 },
      { card: "VISA 3333 USD", amount: -100 },
    ]);
  });

  it("writes one minus row for money from someone else", () => {
    expect(factLines(null, "HUMO 2222 UZS", 0, 300_000, false)).toEqual([
      { card: "HUMO 2222 UZS", amount: -300_000 },
    ]);
  });
});

describe("isTransfer", () => {
  it("opens «Откуда → Куда» only for «переводы» and «обмен валюты»", () => {
    expect(isTransfer("💳 СЧЕТА", "переводы")).toBe(true);
    expect(isTransfer("💳 СЧЕТА", "обмен валюты")).toBe(true);
    expect(isTransfer("💳 СЧЕТА", "выравнивание")).toBe(false);
    expect(isTransfer("🍔 ЕДА", "переводы")).toBe(false);
  });
});

describe("balanceOf", () => {
  it("finds the card of the catalog in «Остатки на картах»", () => {
    expect(balanceOf(transferCards, "VISA 3333 USD")?.balance).toBe(1500.25);
    expect(balanceOf(transferCards, "HUMO 2222 UZS")).toBeNull();
    expect(balanceOf(null, "VISA 1111 UZS")).toBeNull();
  });
});
