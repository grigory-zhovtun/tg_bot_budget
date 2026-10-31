import { describe, expect, it } from "vitest";
import type { ExpenseInput } from "../api";
import { creditHead, keptNotice, spentHead, waitingNotice } from "./outgoing";

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

const COFFEE: ExpenseInput = {
  entry_id: "e1",
  source: "VISA 1111 UZS",
  category: "🍔 ЕДА",
  subcategory: "кофе",
  amount: "48000",
  comment: "",
};
const CREDIT: ExpenseInput = { ...COFFEE, incoming: true };
const TRANSFER: ExpenseInput = {
  ...COFFEE,
  to_source: "HUMO 2222 UZS",
  category: "💳 СЧЕТА",
  subcategory: "переводы",
  amount: "500000",
};
const EXCHANGE: ExpenseInput = {
  ...TRANSFER,
  to_source: "VISA 3333 USD",
  subcategory: "обмен валюты",
  amount: "1210800",
  to_amount: "100",
};

describe("what the phone's queue calls an entry", () => {
  it.each([
    [COFFEE, "📤 Трата сохранена на телефоне"],
    [CREDIT, "📤 Зачисление сохранено на телефоне"],
    [TRANSFER, "📤 Перевод сохранён на телефоне"],
    [EXCHANGE, "📤 Обмен сохранён на телефоне"],
  ])("says what was kept on the phone: %o", (entry, notice) => {
    expect(keptNotice(entry)).toBe(notice);
  });

  it("names what waits when every entry is of one kind", () => {
    expect(waitingNotice([TRANSFER, TRANSFER])).toBe(
      "📵 Связи нет — перевод ждёт на телефоне",
    );
    expect(waitingNotice([EXCHANGE])).toBe(
      "📵 Связи нет — обмен ждёт на телефоне",
    );
    expect(waitingNotice([CREDIT])).toBe(
      "📵 Связи нет — зачисление ждёт на телефоне",
    );
    expect(waitingNotice([COFFEE])).toBe(
      "📵 Связи нет — трата ждёт на телефоне",
    );
  });

  it("keeps «трата» for a queue of different entries", () => {
    expect(waitingNotice([CREDIT, TRANSFER])).toBe(
      "📵 Связи нет — трата ждёт на телефоне",
    );
  });
});
