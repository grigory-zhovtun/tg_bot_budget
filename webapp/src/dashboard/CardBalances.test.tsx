import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { CardBalance } from "../api";
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

/** Строка карты в списке — по её названию. */
function row(name: string): HTMLElement {
  const item = screen.getByText(name).closest("li");
  if (!item) throw new Error(`no row for ${name}`);
  return item;
}

describe("CardBalances", () => {
  it("lists every card in its currency with the total in sums", () => {
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
    expect(within(row("VISA 1111")).getByText("5 250 000 сум")).toBeTruthy();
    expect(within(row("VISA 2222")).getByText("1 500,25 USD")).toBeTruthy();
    expect(within(row("МИР 3333")).getByText("2 000,50 RUB")).toBeTruthy();
    expect(screen.getByText("Всего ≈ 23,55 млн сум")).toBeTruthy();
  });

  it("marks a card that matches the bank and says how old the check is", () => {
    show([
      card(),
      card({ name: "VISA 6666", checked: "2026-10-09" }),
      card({ name: "HUMO 4444", checked: "2026-10-07", stale: true }),
      card({ name: "МИР 3333", checked: null }),
    ]);
    expect(within(row("VISA 1111")).getByText("✓")).toBeTruthy();
    expect(within(row("VISA 6666")).getByText("✓ 6 дн.")).toBeTruthy();
    // сверке больше недели — отметка жёлтая
    const old = within(row("HUMO 4444")).getByText("✓ 8 дн.");
    expect(old.className).toContain("text-warn");
    expect(within(row("МИР 3333")).getByText("✓")).toBeTruthy();
  });

  it("shows the bank balance when the table differs from it", () => {
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
    const humo = row("HUMO 4444");
    expect(
      within(humo).getByText("в банке 299 995 сум · сверено вчера"),
    ).toBeTruthy();
    expect(within(humo).queryByText(/✓/)).toBeNull();
  });

  it("has no mark for a card that was never checked", () => {
    show([card({ bank: null, checked: null, matches: null })]);
    expect(within(row("VISA 1111")).queryByText(/✓|в банке/)).toBeNull();
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
    expect(within(row("VISA 7777")).getByText("—")).toBeTruthy();
    expect(screen.getByText("Всего ≈ 5,25 млн сум")).toBeTruthy();
  });
});
