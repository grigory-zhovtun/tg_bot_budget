import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { CardBalance } from "../api";
import { nb } from "../test/text";
import CardBalances from "./CardBalances";

const TODAY = "2026-10-15";

function card(changes: Partial<CardBalance> = {}): CardBalance {
  return {
    name: "VISA 1111",
    currency: "UZS",
    balance: 5_250_000.4,
    uzs: 5_250_000.4,
    bank: 5_250_000.4,
    checked: TODAY,
    matches: true,
    stale: false,
    ...changes,
  };
}

function show(cards: CardBalance[]) {
  render(<CardBalances cards={cards} today={TODAY} />);
}

/** Клетки строки таблицы — по названию карты. */
function cells(name: string): string[] {
  const row = screen.getByText(name).closest("tr");
  if (!row) throw new Error(`no row for ${name}`);
  return [...row.querySelectorAll("td")].map((cell) => cell.textContent ?? "");
}

describe("CardBalances", () => {
  it("lists every card in a table with the total in sums", () => {
    show([
      card(),
      card({
        name: "VISA 2222",
        currency: "USD",
        balance: 1500.25,
        uzs: 18_003_000,
        bank: 1500.24,
      }),
      card({
        name: "МИР 3333",
        currency: "RUB",
        balance: 2000.5,
        uzs: 300_075,
        bank: 2000.5,
      }),
    ]);
    const table = screen.getByRole("table");
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((cell) => cell.textContent),
    ).toEqual(["карта", "остаток", "сверка"]);
    expect(cells("VISA 1111")).toEqual(["VISA 1111", nb("5 250 000"), "✓"]);
    expect(cells("VISA 2222")).toEqual(["VISA 2222", nb("1 500,25 $"), "✓"]);
    expect(cells("МИР 3333")).toEqual(["МИР 3333", nb("2 000,50 ₽"), "✓"]);
    const total = screen.getByText("Всего ≈ 23 553 075 сум");
    expect(total.className).toContain("text-fact");
  });

  it("marks a card that matches the bank and says how old the check is", () => {
    show([
      card(),
      card({ name: "VISA 6666", checked: "2026-10-09" }),
      card({ name: "HUMO 4444", checked: "2026-10-07", stale: true }),
      card({ name: "МИР 3333", checked: null, stale: true }),
    ]);
    expect(cells("VISA 6666")[2]).toBe("✓ 6 дн.");
    // сверке больше недели — отметка жёлтая, как раньше
    expect(screen.getByText("✓ 8 дн.").className).toContain("text-warn");
    // остаток банка без времени сверки — вписан давно, тоже жёлтым
    expect(screen.getByText("✓ давно").className).toContain("text-warn");
  });

  it("warns under the table when the bank shows another balance", () => {
    show([
      card({
        name: "HUMO 4444",
        balance: 300_000,
        uzs: 300_000,
        bank: 299_995,
        matches: false,
        checked: "2026-10-14",
      }),
    ]);
    expect(cells("HUMO 4444")[2]).toBe("⚠️");
    const warning = screen.getByText("⚠️ HUMO 4444 в банке 299 995");
    expect(warning.className).toContain("text-danger");
    expect(screen.getByText("сверено вчера")).toBeTruthy();
  });

  it("has no mark for a card that was never checked", () => {
    show([card({ bank: null, checked: null, matches: null })]);
    expect(cells("VISA 1111")[2]).toBe("");
    expect(screen.queryByText(/в банке/)).toBeNull();
  });

  it("shows a dash for a broken balance cell and leaves it out of the total", () => {
    show([
      card(),
      card({
        name: "VISA 7777",
        balance: null,
        uzs: null,
        bank: null,
        checked: null,
        matches: null,
      }),
    ]);
    expect(cells("VISA 7777")[1]).toBe("—");
    expect(screen.getByText("Всего ≈ 5 250 000 сум")).toBeTruthy();
  });

  it("says which currency is missing from the total without a rate", () => {
    show([
      card(),
      card({
        name: "VISA 2222",
        currency: "USD",
        balance: 1500.25,
        uzs: null,
        bank: 1500.25,
      }),
    ]);
    expect(screen.getByText("Всего ≈ 5 250 000 сум")).toBeTruthy();
    expect(screen.getByText("без USD — нет курса")).toBeTruthy();
  });
});
