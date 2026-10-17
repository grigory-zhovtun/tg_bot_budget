import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Cushion } from "../api";
import { dashboardFixture } from "../mocks/fixtures";
import CushionCard from "./CushionCard";

function fixture(): Cushion {
  const cushion = dashboardFixture.cushion;
  if (!cushion) throw new Error("fixture");
  return cushion;
}

describe("CushionCard", () => {
  it("shows how many months of spending the cushion lasts", () => {
    render(<CushionCard cushion={fixture()} />);
    // меньше трёх месяцев — оранжевым, как предупреждение
    expect(screen.getByText("0,3 мес.").className).toContain("text-danger");
    const bar = screen.getByRole("progressbar", {
      name: "Подушка: 0,3 из 6 месяцев",
    });
    expect(bar.getAttribute("aria-valuenow")).toBe("0.3");
    expect(bar.getAttribute("aria-valuemax")).toBe("6");
    expect(
      screen.getByText(
        "1 000,00 USD ≈ 12,00 млн — при тратах 40,00 млн в месяц",
      ),
    ).toBeTruthy();
    expect(
      screen.getByText("до цели в 6 мес. не хватает ≈ 228,00 млн ($19 000)"),
    ).toBeTruthy();
  });

  it("is yellow from three months and mint at the target", () => {
    const { unmount } = render(
      <CushionCard cushion={{ ...fixture(), months: 4.2, short_uzs: 72e6 }} />,
    );
    expect(screen.getByText("4,2 мес.").className).toContain("text-warn");
    unmount();
    render(
      <CushionCard
        cushion={{ ...fixture(), months: 6.5, short_uzs: 0, short: 0 }}
      />,
    );
    expect(screen.getByText("6,5 мес.").className).toContain("text-ok");
    expect(screen.getByText("цель в 6 мес. достигнута")).toBeTruthy();
  });

  it("says why there are no months before any spending", () => {
    render(
      <CushionCard cushion={{ ...fixture(), months: null, monthly: 0 }} />,
    );
    expect(screen.getByText("—")).toBeTruthy();
    expect(screen.getByText("трат за 3 полных месяца ещё нет")).toBeTruthy();
    expect(screen.queryByRole("progressbar")).toBeNull();
  });

  it("keeps the bar value within its scale", () => {
    render(
      <CushionCard
        cushion={{ ...fixture(), months: 12, short_uzs: 0, short: 0 }}
      />,
    );
    const bar = screen.getByRole("progressbar");
    expect(bar.getAttribute("aria-valuenow")).toBe("6");
    expect(bar.getAttribute("aria-valuetext")).toBe("12,0 из 6 месяцев");
  });

  it("is not mint until nothing is short of the target", () => {
    // 5,96 месяца пишется «6,0», но до цели ещё не хватает — жёлтым
    render(
      <CushionCard
        cushion={{ ...fixture(), months: 5.96, short_uzs: 2_400_000 }}
      />,
    );
    expect(screen.getByText("6,0 мес.").className).toContain("text-warn");
  });
});
