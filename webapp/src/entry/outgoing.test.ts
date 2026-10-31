import { describe, expect, it } from "vitest";
import { creditHead, spentHead } from "./outgoing";

/** Неразрывные пробелы форматтеров — обычными, для сравнения. */
const plain = (text: string) => text.replace(/\s/g, " ");

describe("the answer's first line", () => {
  it("says what went into the table for «Зачислить»", () => {
    expect(plain(creditHead(48000, "UZS"))).toBe(
      "✓ Зачислено 48 000 UZS — в таблицу −48 000",
    );
    expect(plain(creditHead(12.5, "USD"))).toBe(
      "✓ Зачислено 12,50 USD — в таблицу −12,50",
    );
  });

  it("says «Списано» for «Списать»", () => {
    expect(plain(spentHead(100, "USD"))).toBe("✓ Списано 100 USD");
  });
});
