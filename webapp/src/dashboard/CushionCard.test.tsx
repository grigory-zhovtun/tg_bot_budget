import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Cushion } from "../api";
import { savingsFixture } from "../mocks/fixtures";
import CushionCard from "./CushionCard";

/** Подушка без копилок: «Заморожено» 1000 $ при тратах 40 млн в месяц. */
function fixture(): Cushion {
  const cushion = savingsFixture.cushion;
  if (!cushion) throw new Error("fixture");
  return {
    ...cushion,
    amount: 1000,
    uzs: 12_000_000,
    months: 0.3,
    short_uzs: 228_000_000,
    short: 19_000,
    frozen: 1000,
    piggies: 0,
  };
}

describe("CushionCard", () => {
  it("shows how many months of spending the cushion lasts", () => {
    render(<CushionCard cushion={fixture()} />);
    const heading = screen.getByRole("heading");
    expect(heading.textContent).toBe("🛟 Подушка");
    // при крупном шрифте плашка уходит на следующую строку, а не за экран
    expect(heading.parentElement?.className).toContain("flex-wrap");
    // меньше трёх месяцев — красной плашкой со знаком
    expect(screen.getByText("⚠️ 0,3 мес.").className).toContain("bg-chip-bad");
    const bar = screen.getByRole("progressbar", {
      name: "Подушка: 0,3 из 6 месяцев",
    });
    expect(bar.getAttribute("aria-valuenow")).toBe("0.3");
    expect(bar.getAttribute("aria-valuemax")).toBe("6");
    expect(screen.getByText("1 000 $ ≈ 12 000 000 сум")).toBeTruthy();
    expect(screen.getByText("траты в месяц 40 000 000")).toBeTruthy();
    expect(screen.getByText("Не хватает ≈ 228 000 000").className).toContain(
      "text-fact",
    );
    expect(screen.getByText("до цели 6 месяцев, это 19 000 $")).toBeTruthy();
  });

  it("is a yellow badge from three months and a green one at the target", () => {
    const { unmount } = render(
      <CushionCard cushion={{ ...fixture(), months: 4.2, short_uzs: 72e6 }} />,
    );
    expect(screen.getByText("4,2 мес.").className).toContain("bg-chip-mid");
    unmount();
    render(
      <CushionCard
        cushion={{ ...fixture(), months: 6.5, short_uzs: 0, short: 0 }}
      />,
    );
    expect(screen.getByText("✓ 6,5 мес.").className).toContain("bg-chip-good");
    expect(screen.getByText("✓ Цель 6 мес. достигнута")).toBeTruthy();
  });

  it("says why there are no months before any spending", () => {
    render(
      <CushionCard cushion={{ ...fixture(), months: null, monthly: 0 }} />,
    );
    expect(screen.queryByText(/мес\./)).toBeNull();
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

  it("is not green until nothing is short of the target", () => {
    // 5,96 месяца пишется «6,0», но до цели ещё не хватает — жёлтым
    render(
      <CushionCard
        cushion={{ ...fixture(), months: 5.96, short_uzs: 2_400_000 }}
      />,
    );
    expect(screen.getByText("6,0 мес.").className).toContain("bg-chip-mid");
  });
});
