import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Savings } from "../api";
import { savingsFixture } from "../mocks/fixtures";
import { nb } from "../test/text";
import SavingsCard from "./SavingsCard";

function fixture(): Savings {
  const savings = savingsFixture.savings;
  if (!savings) throw new Error("fixture");
  return savings;
}

describe("SavingsCard", () => {
  it("shows the savings rate of three months and a bar for each month", () => {
    render(<SavingsCard savings={fixture()} />);
    // минус — красной плашкой со знаком
    expect(screen.getByText("⚠️ −10 %").className).toContain("bg-chip-bad");
    expect(screen.getByText("за июль–сентябрь")).toBeTruthy();
    expect(screen.getByText("пришло 120 000 000")).toBeTruthy();
    expect(screen.getByText("потрачено 132 000 000")).toBeTruthy();
    const months = screen.getAllByRole("img");
    expect(months.map((item) => item.getAttribute("aria-label"))).toEqual(
      [
        "апрель: пришло 40 000 000, потрачено 30 000 000, +25 %",
        "май: пришло 0, потрачено 35 000 000, дохода не было",
        "июнь: пришло 50 000 000, потрачено 48 000 000, +4 %",
        "июль: пришло 30 000 000, потрачено 60 000 000, −100 %",
        "август: пришло 45 000 000, потрачено 36 000 000, +20 %",
        "сентябрь: пришло 45 000 000, потрачено 36 000 000, +20 %",
      ].map(nb),
    );
    // от 10 % — мятным, меньше — жёлтым, минус — оранжевым; подписи крупнее
    const april = months[0];
    const june = months[2];
    const july = months[3];
    if (!april || !june || !july) throw new Error("months");
    // над столбиком — число без «%»: крупно и в свою колонку; «%» — в подписи
    const label = within(april).getByText("+25");
    expect(label.className).toContain("text-ok");
    expect(label.className).toContain("text-[15px]");
    expect(within(june).getByText("+4").className).toContain("text-warn");
    expect(within(july).getByText("−100").className).toContain("text-danger");
    expect(screen.getByText("числа над столбиками — в %")).toBeTruthy();
    const may = months[1];
    if (!may) throw new Error("may");
    expect(within(may).getByText("—")).toBeTruthy();
    // траты без дохода — запас проели: столбик вниз, а не «нет данных»
    expect(may.querySelector('[data-bar="down"]')).not.toBeNull();
    // маленькая доля всё равно видна
    expect((june.querySelector("[data-bar]") as HTMLElement).style.height).toBe(
      "max(2%, 3px)",
    );
  });

  it("keeps a huge share short and paints by the rounded number", () => {
    render(
      <SavingsCard
        savings={{
          rate: 0.096,
          months: [
            { month: "2026-09", income: 1_000_000, spent: 80e6, rate: -79 },
          ],
        }}
      />,
    );
    expect(screen.getByText("< −999")).toBeTruthy();
    // 9,6 % пишется «10 %» — и плашка как у 10 %
    expect(screen.getByText("✓ 10 %").className).toContain("bg-chip-good");
  });

  it("paints a small positive rate yellow", () => {
    render(<SavingsCard savings={{ ...fixture(), rate: 0.05 }} />);
    expect(screen.getByText("5 %").className).toContain("bg-chip-mid");
  });

  it("says when there was no income for three months", () => {
    render(
      <SavingsCard
        savings={{
          rate: null,
          months: fixture().months.map((item) => ({
            ...item,
            income: 0,
            rate: null,
          })),
        }}
      />,
    );
    expect(screen.getByText("за июль–сентябрь дохода не было")).toBeTruthy();
    expect(screen.queryByText(/✓|⚠️/)).toBeNull();
  });
});
