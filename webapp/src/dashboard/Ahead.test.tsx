import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
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
    // на полосах цветной текст не держит 7:1 — суммы цветом текста, смысл в знаке
    for (const sum of ["+2 000 000", "−380 $"]) {
      expect(screen.getByText(sum).className).not.toMatch(
        /text-(earned|payment)/,
      );
    }
    expect(screen.queryByText(/день прошёл/)).toBeNull();
  });

  it("keeps two rows with the same day and name apart", () => {
    // две строки прошлого месяца с одним названием — обе «… за октябрь, остаток»
    const errors = vi.spyOn(console, "error").mockImplementation(() => {});
    const rest = {
      name: "Фриланс за октябрь, остаток",
      day: 1,
      amount: 200,
      currency: "USD",
      uzs: 2_400_000,
    };
    show({ upcoming: [rest, { ...rest, amount: 300, uzs: 3_600_000 }] });
    expect(items("Платежи и поступления")).toHaveLength(2);
    expect(
      errors.mock.calls.some((call) => String(call[0]).includes("same key")),
    ).toBe(false);
    errors.mockRestore();
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
