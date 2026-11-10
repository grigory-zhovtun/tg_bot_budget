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

/** Строка карты — по её названию. */
function row(name: string): HTMLElement {
  const item = screen.getByText(name).closest("li");
  if (!item) throw new Error(`no row for ${name}`);
  return item;
}

describe("CardBalances", () => {
  it("lists every card in a row with the total in sums", () => {
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
    expect(screen.getAllByRole("listitem")).toHaveLength(3);
    expect(within(row("VISA 1111")).getByText("5 250 000")).toBeTruthy();
    expect(within(row("VISA 2222")).getByText("1 500,25 $")).toBeTruthy();
    expect(within(row("МИР 3333")).getByText("2 000,50 ₽")).toBeTruthy();
    const total = screen.getByText("Всего ≈ 23 553 075 сум");
    expect(total.className).toContain("text-fact");
  });

  it("puts the currency of a card into a round sign of its own colour", () => {
    show([
      card(),
      card({ name: "VISA 2222", currency: "USD" }),
      card({ name: "МИР 3333", currency: "RUB" }),
      card({ name: "EURO 4444", currency: "EUR" }),
    ]);
    const sign = (name: string) => row(name).querySelector("[aria-hidden]");
    expect(sign("VISA 1111")?.textContent).toBe("сум");
    expect(sign("VISA 1111")?.className).toContain("bg-round-sums");
    expect(sign("VISA 2222")?.textContent).toBe("$");
    expect(sign("VISA 2222")?.className).toContain("bg-round-dollars");
    expect(sign("МИР 3333")?.textContent).toBe("₽");
    expect(sign("МИР 3333")?.className).toContain("bg-round-rubles");
    // валюта без своего знака — кодом, на сером
    expect(sign("EURO 4444")?.textContent).toBe("EUR");
    expect(sign("EURO 4444")?.className).toContain("bg-chip-off");
    // знак на круге — белый и жирный
    expect(sign("VISA 1111")?.className).toContain("text-white");
  });

  it("says under the name when the card was checked with the bank", () => {
    show([
      card(),
      card({ name: "VISA 6666", checked: "2026-10-09" }),
      card({ name: "HUMO 4444", checked: "2026-10-07", stale: true }),
      card({ name: "МИР 3333", checked: null, stale: true }),
    ]);
    const today = within(row("VISA 1111")).getByText("✓ сверено сегодня");
    expect(today.className).toContain("text-ok");
    expect(
      within(row("VISA 6666")).getByText("✓ сверено 6 дн. назад"),
    ).toBeTruthy();
    // давняя сверка — жёлтым и с числом дней: цвет не единственный знак
    const stale = within(row("HUMO 4444")).getByText("✓ сверено 8 дн. назад");
    expect(stale.className).toContain("text-warn");
    expect(
      within(row("МИР 3333")).getByText("✓ сверено давно").className,
    ).toContain("text-warn");
  });

  it("warns in the row when the bank shows another balance", () => {
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
    const warning = within(row("HUMO 4444")).getByText("⚠️ в банке 299 995");
    expect(warning.className).toContain("text-danger");
    expect(within(row("HUMO 4444")).getByText("сверено вчера")).toBeTruthy();
  });

  it("says nothing about a check for a card that was never checked", () => {
    show([card({ bank: null, checked: null, matches: null })]);
    expect(row("VISA 1111").textContent).toBe(`сумVISA 1111${nb("5 250 000")}`);
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
    expect(within(row("VISA 7777")).getByText("—")).toBeTruthy();
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

  it("titles the block in the link colour, as every block of the summary", () => {
    show([card()]);
    expect(
      screen.getByRole("heading", { name: "Остатки на картах" }).className,
    ).toContain("text-link");
  });
});
