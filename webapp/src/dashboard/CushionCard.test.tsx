import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Cushion } from "../api";
import CushionCard from "./CushionCard";

/** 3 000 $ на карте, квартплата 18.10 700 $ и копилки 1 000 $: подушка 1 300 $ —
 * 1,3 месяца жизни по 12 млн, цель 6 месяцев. */
function fixture(changes: Partial<Cushion> = {}): Cushion {
  return {
    amount: 1300,
    currency: "USD",
    uzs: 15_600_000,
    monthly: 12_000_000,
    months: 1.3,
    target_months: 6,
    short_uzs: 56_400_000,
    short: 4700,
    frozen: 2300,
    piggies: 1000,
    over: 0,
    card: 3000,
    payments: [{ day: "2026-10-18", name: "Квартплата", amount: 700 }],
    lacking: 0,
    lacking_on: null,
    life_months: ["2026-07", "2026-08", "2026-09"],
    life_groups: ["🏚️ ДОМ", "🌛 ЕЖЕМЕСЯЧНО"],
    steps: [
      { months: 3, short: 1700, short_uzs: 20_400_000 },
      { months: 6, short: 4700, short_uzs: 56_400_000 },
    ],
    ...changes,
  };
}

/** Строки таблицы «Расчёт подушки», пробелы внутри чисел — обычные. */
function counted(): (string | undefined)[] {
  return within(screen.getByRole("table", { name: "Расчёт подушки" }))
    .getAllByRole("row")
    .map((row) => row.textContent?.replace(/\s/g, " "));
}

describe("CushionCard", () => {
  it("shows the cushion now and how it is counted", () => {
    render(<CushionCard today="2026-10-10" cushion={fixture()} />);
    const heading = screen.getByRole("heading");
    expect(heading.textContent).toBe("🛟 Подушка");
    // при крупном шрифте плашка уходит на следующую строку, а не за экран
    expect(heading.parentElement?.className).toContain("flex-wrap");
    expect(screen.getByText("1,3 мес.").className).toContain("bg-chip-mid");
    // главная цифра — подушка; та же сумма — последней строкой расчёта
    expect(screen.getByText("1 300 $", { selector: "p" }).className).toContain(
      "text-big",
    );
    expect(screen.getByText("в подушке сейчас")).toBeTruthy();
    expect(screen.getByText("≈ 15 600 000 сум")).toBeTruthy();
    expect(counted()).toEqual([
      "доллары на карте3 000 $",
      "Квартплата 18.10−700 $",
      "копилки−1 000 $",
      "подушка1 300 $",
    ]);
    expect(screen.getByText("Месяц жизни 12 000 000")).toBeTruthy();
    expect(screen.getByText("в среднем за июль–сентябрь")).toBeTruthy();
    expect(
      screen.getByText("обязательные: 🏚️ ДОМ, 🌛 ЕЖЕМЕСЯЧНО"),
    ).toBeTruthy();
    expect(screen.getByText("До 3 месяцев — ещё 1 700 $").className).toContain(
      "text-fact",
    );
    expect(screen.getByText("≈ 20 400 000 сум")).toBeTruthy();
    expect(screen.getByText("цель 6 месяцев — ещё 4 700 $")).toBeTruthy();
  });

  it("colours the months by the steps", () => {
    const low = render(
      <CushionCard
        today="2026-10-10"
        cushion={fixture({
          months: 0.8,
          steps: [
            { months: 1, short: 200, short_uzs: 2_400_000 },
            { months: 3, short: 2200, short_uzs: 26_400_000 },
            { months: 6, short: 5200, short_uzs: 62_400_000 },
          ],
        })}
      />,
    );
    expect(screen.getByText("⚠️ 0,8 мес.").className).toContain("bg-chip-bad");
    expect(screen.getByText("До 1 месяца — ещё 200 $")).toBeTruthy();
    expect(screen.getByText("3 месяца — ещё 2 200 $")).toBeTruthy();
    low.unmount();
    const mid = render(
      <CushionCard
        today="2026-10-10"
        cushion={fixture({
          months: 3.5,
          steps: [{ months: 6, short: 2500, short_uzs: 30_000_000 }],
        })}
      />,
    );
    expect(screen.getByText("3,5 мес.").className).toContain("bg-chip-good");
    expect(screen.getByText("До цели 6 месяцев — ещё 2 500 $")).toBeTruthy();
    mid.unmount();
    render(
      <CushionCard
        today="2026-10-10"
        cushion={fixture({ months: 6, steps: [] })}
      />,
    );
    expect(screen.getByText("✓ 6,0 мес.").className).toContain("bg-chip-good");
    expect(screen.getByText("✓ Цель 6 мес. достигнута")).toBeTruthy();
  });

  it("marks one and three months on the bar", () => {
    render(<CushionCard today="2026-10-10" cushion={fixture()} />);
    const bar = screen.getByRole("progressbar", {
      name: "Подушка: 1,3 из 6 месяцев",
    });
    expect(bar.getAttribute("aria-valuenow")).toBe("1.3");
    expect(bar.getAttribute("aria-valuemax")).toBe("6");
    const scale = screen.getByTestId("cushion-scale");
    expect(
      Array.from(scale.querySelectorAll("span")).map(
        (mark) => mark.textContent,
      ),
    ).toEqual(["1", "3", "6 мес."]);
  });

  it("keeps the bar value within its scale", () => {
    render(
      <CushionCard
        today="2026-10-10"
        cushion={fixture({ months: 12, steps: [] })}
      />,
    );
    const bar = screen.getByRole("progressbar");
    expect(bar.getAttribute("aria-valuenow")).toBe("6");
    expect(bar.getAttribute("aria-valuetext")).toBe("12,0 из 6 месяцев");
  });

  it("says when there are no mandatory groups", () => {
    render(
      <CushionCard
        today="2026-10-10"
        cushion={fixture({ life_groups: [], months: null, monthly: 0 })}
      />,
    );
    expect(
      screen.getByText("обязательных групп нет в справочнике"),
    ).toBeTruthy();
    expect(screen.queryByText(/мес\./)).toBeNull();
    expect(screen.queryByRole("progressbar")).toBeNull();
    expect(screen.queryByText(/— ещё/)).toBeNull();
    expect(screen.queryByText(/Месяц жизни/)).toBeNull();
  });

  it("says why there are no months before any spending", () => {
    render(
      <CushionCard
        today="2026-10-10"
        cushion={fixture({ life_months: [], months: null, monthly: 0 })}
      />,
    );
    expect(screen.getByText("трат за 3 полных месяца ещё нет")).toBeTruthy();
    expect(screen.queryByRole("progressbar")).toBeNull();
  });

  it("warns when the piggies are over the card", () => {
    render(
      <CushionCard
        today="2026-10-10"
        cushion={fixture({ amount: 0, uzs: 0, over: 300 })}
      />,
    );
    expect(screen.getByText("⚠️ Копилки больше карты")).toBeTruthy();
    expect(screen.getByText("разница 300 $")).toBeTruthy();
  });
});
