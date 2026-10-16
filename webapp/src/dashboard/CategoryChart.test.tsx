import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { Category } from "../api";
import CategoryChart from "./CategoryChart";

/** textContent — как есть: пробелы между разрядами неразрывные, как в money(). */
const nb = (text: string) => text.replace(/(\d) (?=\d)/g, "$1\u00a0");

// за месяц потрачено 8 400 000 из плана 12 500 000; самая длинная полоса —
// план дома 6 000 000, по ней шкала у всех групп
const CATEGORIES: Category[] = [
  {
    name: "🏚️ ДОМ",
    plan: 6_000_000,
    fact: 4_500_000,
    items: [
      { name: "продукты", icon: "🛒", plan: 3_500_000, fact: 3_000_000 },
      { name: "квартплата", icon: "", plan: 2_500_000, fact: 1_500_000 },
    ],
  },
  {
    name: "🍔 ЕДА",
    plan: 3_000_000,
    fact: 3_300_000,
    items: [{ name: "кафе", icon: "🍽️", plan: 3_000_000, fact: 3_300_000 }],
  },
  {
    name: "🎉 ПРАЗДНИК",
    plan: 0,
    fact: 600_000,
    items: [{ name: "подарки", icon: "🎁", plan: 0, fact: 600_000 }],
  },
  {
    name: "🚌 ТРАНСПОРТ",
    plan: 3_500_000,
    fact: 0,
    items: [{ name: "такси", icon: "🚕", plan: 3_500_000, fact: 0 }],
  },
];

function show(today = "2026-10-15") {
  return render(
    <CategoryChart categories={CATEGORIES} untracked={[]} today={today} />,
  );
}

describe("CategoryChart", () => {
  it("shows the month against the plan and every group with its share", () => {
    show();
    expect(screen.getByText("8,40 млн")).toBeTruthy();
    // план — на весь месяц, а не на прошедшие дни
    expect(
      screen.getByText("за 1–15 октября · план на месяц 12,50 млн"),
    ).toBeTruthy();
    expect(
      screen.getAllByRole("button").map((button) => button.textContent),
    ).toEqual([
      "🏚️ ДОМ4,50 млн из 6,00 млн · 54 %",
      "🍔 ЕДА3,30 млн из 3,00 млн · 39 %",
      nb("🎉 ПРАЗДНИК600 000 вне плана · 7 %"),
      nb("🚌 ТРАНСПОРТ0 из 3,50 млн"),
    ]);
  });

  it("draws the spending and the plan on one scale, coloured by the plan", () => {
    const { container } = show();
    const bars = [...container.querySelectorAll<HTMLElement>("[data-status]")];
    expect(bars.map((bar) => bar.dataset.status)).toEqual([
      "ok",
      "over",
      "off",
      "ok",
    ]);
    const width = (bar: HTMLElement, part: string) =>
      bar
        .querySelector<HTMLElement>(`[data-${part}]`)
        ?.style.getPropertyValue(part === "fact" ? "width" : "left") ?? null;
    expect(bars.map((bar) => width(bar, "fact"))).toEqual([
      "75%",
      "55%",
      "10%",
      null, // без трат полосы нет
    ]);
    expect(bars.map((bar) => width(bar, "plan"))).toEqual([
      "100%",
      "50%",
      null, // без плана черты нет
      "58.3%",
    ]);
  });

  it("opens a group to show its subcategories against their plans", async () => {
    const user = userEvent.setup();
    show();
    expect(screen.queryByText(/продукты/)).toBeNull();
    const home = screen.getByRole("button", { name: /ДОМ/ });
    await user.click(home);
    expect(home.getAttribute("aria-expanded")).toBe("true");
    const food = screen.getByText("🛒 продукты").closest("li");
    if (!food) throw new Error("no row for продукты");
    expect(within(food).getByText("3,00 млн из 3,50 млн")).toBeTruthy();
    expect(screen.getByText("квартплата")).toBeTruthy();
  });

  it("explains the bar and names the spending the month tab does not count", () => {
    render(
      <CategoryChart
        categories={CATEGORIES}
        untracked={[
          {
            group: "🍔 ЕДА",
            subcategory: "кафе",
            currency: "USD",
            amount: 42_000,
          },
        ]}
        today="2026-10-15"
      />,
    );
    expect(
      screen.getByText("Полоса — сколько потрачено, черта — план"),
    ).toBeTruthy();
    expect(
      screen.getByText(
        "⚠️ Во вкладке месяца не учтено: 🍔 ЕДА / кафе (USD) — 42 000 сум. " +
          "Добавьте статью во вкладке «План».",
      ),
    ).toBeTruthy();
  });

  it("speaks of one day on the first of the month", () => {
    show("2026-10-01");
    expect(
      screen.getByText("за 1 октября · план на месяц 12,50 млн"),
    ).toBeTruthy();
  });
});
