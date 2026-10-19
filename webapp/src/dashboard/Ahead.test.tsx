import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Dashboard } from "../api";
import { dashboardFixture } from "../mocks/fixtures";
import { nb } from "../test/text";
import Ahead from "./Ahead";

function show(changes: Partial<Dashboard> = {}) {
  render(<Ahead data={{ ...dashboardFixture, ...changes }} />);
}

/** Пункты списка: название, день и сумма — как их видно. */
function items(label: string): string[][] {
  const list = screen.getByRole("list", { name: label });
  return within(list)
    .getAllByRole("listitem")
    .map((item) => [
      item.querySelector("p")?.textContent ?? "",
      ...[...(item.querySelector("div")?.children ?? [])].map(
        (cell) => cell.textContent ?? "",
      ),
    ]);
}

describe("Ahead", () => {
  it("lists the payments ahead with their day and signed sum", () => {
    show();
    expect(items("Платежи и поступления")).toEqual([
      ["Аванс", "15-го", nb("+2 000 000")],
      ["Аренда", "18-го", nb("−380 $")],
    ]);
    // поступление — зелёным, платёж — оранжевым, как в календаре
    expect(screen.getByText("+2 000 000").className).toContain("text-earned");
    expect(screen.getByText("−380 $").className).toContain("text-payment");
    expect(screen.queryByText(/день прошёл/)).toBeNull();
  });

  it("marks a row that is late and explains the mark", () => {
    show({
      upcoming: [
        {
          name: "ЗП",
          day: 5,
          amount: 10_000_000,
          currency: "UZS",
          uzs: 10_000_000,
        },
      ],
    });
    expect(items("Платежи и поступления")).toEqual([
      ["⏳ ЗП", "5-го", nb("+10 000 000")],
    ]);
    expect(screen.getByText("⏳ — день прошёл, записи ещё нет")).toBeTruthy();
  });

  it("lists the subscriptions with a mark and explains the marks it uses", () => {
    show();
    expect(items("Подписки")).toEqual([
      ["✓ Render.com", "1-го", nb("312 400")],
      ["⏳ Google One", "14-го", nb("198 700")],
    ]);
    expect(screen.getByText("✓ списано")).toBeTruthy();
    expect(screen.getByText("⏳ ждём")).toBeTruthy();
    expect(screen.queryByText("⚠️ дважды")).toBeNull();
  });

  it("keeps a long name on its own line and a currency in its sign", () => {
    show({
      subscriptions: [
        {
          name: "Longname Trading Company",
          day: 20,
          amount: 15.5,
          currency: "USD",
          uzs: 186_000,
          state: "twice",
        },
        {
          name: "Music Box",
          day: 3,
          amount: 41_000,
          currency: "UZS",
          uzs: 41_000,
          state: "missed",
        },
      ],
    });
    expect(items("Подписки")).toEqual([
      ["⚠️ Longname Trading Company", "20-го", nb("15,50 $")],
      ["❓ Music Box", "3-го", nb("41 000")],
    ]);
    expect(screen.getByText("⚠️ дважды")).toBeTruthy();
    expect(screen.getByText("❓ не было")).toBeTruthy();
  });

  it("says when nothing is ahead", () => {
    show({ upcoming: [], subscriptions: [] });
    expect(
      screen.getByText("До конца месяца платежей и подписок нет").className,
    ).toContain("text-line");
  });

  it("explains a missing forecast block instead of an empty list", () => {
    show({ status: "no_forecast" });
    expect(screen.getByText("⚠️ Нет блока прогноза")).toBeTruthy();
    expect(screen.queryByRole("list")).toBeNull();
  });
});
