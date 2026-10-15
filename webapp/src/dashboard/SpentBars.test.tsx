import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { SpentGroup } from "../api";
import SpentBars from "./SpentBars";

/** textContent — как есть: пробелы между разрядами неразрывные, как в money(). */
const nb = (text: string) => text.replace(/(\d) (?=\d)/g, "$1\u00a0");

// за месяц 8 520 000: дом 70 %, еда 29 %, такси меньше процента
const GROUPS: SpentGroup[] = [
  {
    name: "🏚️ ДОМ",
    amount: 6_000_000,
    items: [
      { name: "продукты", icon: "🛒", amount: 3_500_000 },
      { name: "квартплата", icon: "", amount: 2_500_000 },
    ],
  },
  {
    name: "🍔 ЕДА",
    amount: 2_500_000,
    items: [
      { name: "кофе", icon: "☕", amount: 1_300_000 },
      { name: "кафе", icon: "🍽️", amount: 1_200_000 },
    ],
  },
  {
    name: "🚌 ТРАНСПОРТ",
    amount: 20_000,
    items: [{ name: "такси", icon: "🚕", amount: 20_000 }],
  },
];

describe("SpentBars", () => {
  it("shows the month total and every group with its share", () => {
    render(<SpentBars groups={GROUPS} untracked={[]} today="2026-10-15" />);
    expect(screen.getByText("8,52 млн")).toBeTruthy();
    expect(screen.getByText("за 1–15 октября")).toBeTruthy();
    const labels = screen
      .getAllByRole("button")
      .map((button) => button.textContent);
    expect(labels).toEqual([
      "🏚️ ДОМ6,00 млн · 70 %",
      "🍔 ЕДА2,50 млн · 29 %",
      nb("🚌 ТРАНСПОРТ20 000 · <1 %"),
    ]);
  });

  it("draws each bar against the biggest group", () => {
    const { container } = render(
      <SpentBars groups={GROUPS} untracked={[]} today="2026-10-15" />,
    );
    const bars = [...container.querySelectorAll<HTMLElement>("[data-bar]")];
    expect(bars.map((bar) => bar.style.width)).toEqual([
      "100%",
      "41.7%",
      "0.3%",
    ]);
  });

  it("opens a group to show where its money went", async () => {
    const user = userEvent.setup();
    render(<SpentBars groups={GROUPS} untracked={[]} today="2026-10-15" />);
    expect(screen.queryByText(/кофе/)).toBeNull();
    const food = screen.getByRole("button", { name: /ЕДА/ });
    await user.click(food);
    expect(food.getAttribute("aria-expanded")).toBe("true");
    const coffee = screen.getByText("☕ кофе").closest("li");
    if (!coffee) throw new Error("no coffee row");
    expect(within(coffee).getByText("1,30 млн")).toBeTruthy();
    await user.click(food);
    expect(screen.queryByText(/кофе/)).toBeNull();
  });

  it("names the spending the month tab does not count", () => {
    render(
      <SpentBars
        groups={GROUPS}
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
      screen.getByText(
        "⚠️ Во вкладке месяца не учтено: 🍔 ЕДА / кафе (USD) — 42 000 сум. " +
          "Добавьте статью во вкладке «План».",
      ),
    ).toBeTruthy();
  });

  it("speaks of one day on the first of the month", () => {
    render(<SpentBars groups={GROUPS} untracked={[]} today="2026-10-01" />);
    expect(screen.getByText("за 1 октября")).toBeTruthy();
  });
});
