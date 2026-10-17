import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Savings } from "../api";
import { dashboardFixture } from "../mocks/fixtures";
import SavingsCard from "./SavingsCard";

function fixture(): Savings {
  const savings = dashboardFixture.savings;
  if (!savings) throw new Error("fixture");
  return savings;
}

describe("SavingsCard", () => {
  it("shows the savings rate of three months and a bar for each month", () => {
    render(<SavingsCard savings={fixture()} />);
    // минус — оранжевым, без красного текста
    expect(screen.getByText("−10 %").className).toContain("text-danger");
    expect(
      screen.getByText(
        "за июль–сентябрь: пришло 120,00 млн, потрачено 132,00 млн",
      ),
    ).toBeTruthy();
    const months = screen.getAllByRole("img");
    expect(months.map((item) => item.getAttribute("aria-label"))).toEqual([
      "апрель: пришло 40,00 млн, потрачено 30,00 млн, +25 %",
      "май: пришло 0, потрачено 35,00 млн, дохода не было",
      "июнь: пришло 50,00 млн, потрачено 48,00 млн, +4 %",
      "июль: пришло 30,00 млн, потрачено 60,00 млн, −100 %",
      "август: пришло 45,00 млн, потрачено 36,00 млн, +20 %",
      "сентябрь: пришло 45,00 млн, потрачено 36,00 млн, +20 %",
    ]);
    // от 10 % — мятным, меньше — жёлтым, минус — оранжевым
    const april = months[0];
    const june = months[2];
    const july = months[3];
    if (!april || !june || !july) throw new Error("months");
    expect(within(april).getByText("+25 %").className).toContain("text-ok");
    expect(within(june).getByText("+4 %").className).toContain("text-warn");
    expect(within(july).getByText("−100 %").className).toContain("text-danger");
    expect(within(months[1] as HTMLElement).getByText("—")).toBeTruthy();
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
  });
});
