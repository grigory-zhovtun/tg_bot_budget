import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { Category } from "../api";
import { nb } from "../test/text";
import CategoryChart from "./CategoryChart";

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
  it("shows the month against the plan and every group with its plan share", () => {
    show();
    const summary = screen.getByText("Потрачено 8 400 000");
    expect(summary.className).toContain("text-fact");
    // план — на весь месяц, а не на прошедшие дни
    expect(screen.getByText("за 1–15 октября")).toBeTruthy();
    expect(screen.getByText("план на месяц 12 500 000")).toBeTruthy();
    expect(
      screen.getAllByRole("button").map((button) => button.textContent),
    ).toEqual([
      nb("🏚️ ДОМ✓ 75 %"),
      nb("🍔 ЕДА⚠️ 110 %"),
      "🎉 ПРАЗДНИК",
      nb("🚌 ТРАНСПОРТ✓ 0 %"),
    ]);
    expect(screen.getByText("4 500 000 из 6 000 000")).toBeTruthy();
    expect(screen.getByText("600 000 вне плана")).toBeTruthy();
    expect(screen.getByText("0 из 3 500 000")).toBeTruthy();
  });

  it("paints the plan share badge like the bot's statuses", () => {
    render(
      <CategoryChart
        categories={[
          { name: "🍔 ЕДА", plan: 1_000_000, fact: 1_100_000, items: [] },
          { name: "☕ КОФЕ", plan: 1_000_000, fact: 860_000, items: [] },
          { name: "🚕 ТАКСИ", plan: 1_000_000, fact: 410_000, items: [] },
        ]}
        untracked={[]}
        today="2026-10-15"
      />,
    );
    expect(screen.getByText("⚠️ 110 %").className).toContain("bg-chip-bad");
    // от 80 % плана — жёлтая, без знака
    expect(screen.getByText("86 %").className).toContain("bg-chip-mid");
    expect(screen.getByText("✓ 41 %").className).toContain("bg-chip-good");
  });

  it("lets a badge move under a long name instead of off the screen", () => {
    show();
    const row = screen.getByRole("button", { name: /ДОМ/ });
    expect(row.className).toContain("flex-wrap");
  });

  it("writes nothing under a group without plan and spending", () => {
    render(
      <CategoryChart
        categories={[
          { name: "🎁 ПОДАРКИ", plan: 0, fact: 0, items: [] },
          { name: "🍔 ЕДА", plan: 1_000_000, fact: 100_000, items: [] },
        ]}
        untracked={[]}
        today="2026-10-15"
      />,
    );
    expect(screen.queryByText(/вне плана/)).toBeNull();
    expect(screen.getByRole("button", { name: "🎁 ПОДАРКИ" })).toBeTruthy();
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
    expect(within(food).getByText("3 000 000 из 3 500 000")).toBeTruthy();
    expect(within(food).getByText("86 %")).toBeTruthy();
    expect(screen.getByText("квартплата")).toBeTruthy();
  });

  it("explains the bar and lists the spending the month tab does not count", () => {
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
    expect(screen.getByText("полоса — потрачено, черта — план")).toBeTruthy();
    const note = screen.getByRole("region", { name: "Нет в плане месяца" });
    expect(within(note).getByText("⚠️ Нет в плане месяца")).toBeTruthy();
    const row = within(note).getByText("кафе (USD)").closest("tr");
    expect(row?.textContent).toBe(nb("кафе (USD)42 000"));
    expect(within(note).getByText("добавьте во вкладке «План»")).toBeTruthy();
  });

  it("speaks of one day on the first of the month", () => {
    show("2026-10-01");
    expect(screen.getByText("за 1 октября")).toBeTruthy();
  });
});
