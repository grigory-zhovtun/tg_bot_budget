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

/** Июль–сентябрь в плюсе: пришло 120 млн, потрачено 90 млн. */
function plus(): Savings {
  const months = fixture().months.map((item, index) =>
    index < 3 ? item : { ...item, income: 40_000_000, spent: 30_000_000 },
  );
  return { ...fixture(), rate: 0.25, months, worst: null };
}

describe("SavingsCard", () => {
  it("says why three months are in minus", () => {
    render(<SavingsCard savings={fixture()} />);
    // минус — красной плашкой со знаком, в одной строке с заголовком
    const chip = screen.getByText("⚠️ −10 %");
    expect(chip.className).toContain("bg-chip-bad");
    const row = screen.getByRole("heading", {
      name: "Норма сбережений",
    }).parentElement;
    expect(row?.className).toContain("justify-between");
    expect(row?.contains(chip)).toBe(true);
    expect(
      screen.getByText("за июль–сентябрь траты больше дохода на 12 000 000"),
    ).toBeTruthy();
    expect(
      screen.getByText(
        "больше всего — июль: потрачено 60 000 000, пришло 30 000 000",
      ),
    ).toBeTruthy();
    expect(screen.getByText("за полгода −17 %")).toBeTruthy();
    expect(screen.getByText("вверх — отложили, вниз — проели")).toBeTruthy();
    expect(screen.queryByText("числа над столбиками — в %")).toBeNull();
  });

  it("draws six months and the current one dashed", () => {
    render(<SavingsCard savings={fixture()} />);
    const months = screen.getAllByRole("img");
    expect(months).toHaveLength(7);
    expect(months.map((item) => item.getAttribute("aria-label"))).toEqual(
      [
        "апрель: пришло 40 000 000, потрачено 30 000 000, +25 %",
        "май: пришло 0, потрачено 35 000 000, дохода не было",
        "июнь: пришло 50 000 000, потрачено 48 000 000, +4 %",
        "июль: пришло 30 000 000, потрачено 60 000 000, −100 %",
        "август: пришло 45 000 000, потрачено 36 000 000, +20 %",
        "сентябрь: пришло 45 000 000, потрачено 36 000 000, +20 %",
        "октябрь пока: пришло 16 000 000, потрачено 12 000 000, +25 %",
      ].map(nb),
    );
    const [april, may, , , , , now] = months;
    if (!april || !may || !now) throw new Error("months");
    // чисел над столбиками нет — подпись под столбиком, месяц
    expect(within(april).queryByText("+25")).toBeNull();
    expect(within(april).getByText("апр")).toBeTruthy();
    // траты без дохода — запас проели: столбик вниз, а не «нет данных»
    expect(may.querySelector('[data-bar="down"]')).not.toBeNull();
    // текущий — пунктиром, с подписью «пока»
    expect(now.hasAttribute("data-current")).toBe(true);
    expect(within(now).getByText("пока")).toBeTruthy();
    const bar = now.querySelector("[data-bar]");
    expect(bar?.className).toContain("border-dashed");
    expect(bar?.className).toContain("border-2");
    expect(bar?.className).not.toContain("bg-");
    expect(screen.getByText("октябрь пока +25 %")).toBeTruthy();
  });

  it("says the share saved in plus", () => {
    render(<SavingsCard savings={plus()} />);
    expect(
      screen.getByText("за июль–сентябрь отложено 30 000 000 — 25 % дохода"),
    ).toBeTruthy();
    expect(screen.queryByText(/больше всего/)).toBeNull();
  });

  it("says when there was no income", () => {
    render(
      <SavingsCard
        savings={{
          rate: null,
          half_rate: null,
          worst: null,
          current: null,
          months: fixture().months.map((item) => ({
            ...item,
            income: 0,
            rate: null,
          })),
        }}
      />,
    );
    expect(screen.getByText("за июль–сентябрь дохода не было")).toBeTruthy();
    expect(screen.getByText("за полгода дохода не было")).toBeTruthy();
    expect(screen.queryByText(/✓|⚠️/)).toBeNull();
    expect(screen.getAllByRole("img")).toHaveLength(6); // без текущего месяца
  });

  it("says the month so far has no income yet", () => {
    render(
      <SavingsCard
        savings={{
          ...fixture(),
          current: {
            month: "2026-10",
            income: 0,
            spent: 3_000_000,
            rate: null,
          },
        }}
      />,
    );
    expect(screen.getByText("октябрь пока: дохода ещё не было")).toBeTruthy();
  });

  it("keeps a huge share short and paints by the rounded number", () => {
    render(
      <SavingsCard
        savings={{
          rate: 0.096,
          half_rate: 0.096,
          worst: null,
          current: null,
          months: [
            { month: "2026-09", income: 1_000_000, spent: 80e6, rate: -79 },
          ],
        }}
      />,
    );
    // 9,6 % пишется «10 %» — и плашка как у 10 %
    expect(screen.getByText("✓ 10 %").className).toContain("bg-chip-good");
  });

  it("paints a small positive rate yellow", () => {
    render(<SavingsCard savings={{ ...fixture(), rate: 0.05 }} />);
    expect(screen.getByText("5 %").className).toContain("bg-chip-mid");
  });
});
