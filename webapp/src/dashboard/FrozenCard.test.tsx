import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Frozen } from "../api";
import FrozenCard from "./FrozenCard";

/** 3 000 $ на карте, квартплата 18 ноября 700 $ — в сроке: заморожено 2 300 $. */
function fixture(changes: Partial<Frozen> = {}): Frozen {
  return {
    amount: 2300,
    currency: "USD",
    uzs: 27_600_000,
    change: 0,
    total: 3000,
    payments: [{ day: "2026-11-18", name: "Квартплата", amount: 700 }],
    piggies: 1000,
    cushion: 1300,
    over: 0,
    lacking: 0,
    lacking_on: null,
    target: "до зарплаты 30 ноября",
    ...changes,
  };
}

/** Строки карточки по порядку: тексты её детей, пробелы внутри чисел — обычные. */
function lines(): string[] {
  const card = screen.getByRole("region", { name: "Заморожено" });
  return Array.from(card.children).map(
    (child) => child.textContent?.replace(/\s/g, " ") ?? "",
  );
}

describe("FrozenCard", () => {
  it("shows what the frozen dollars keep and where the rest goes", () => {
    render(<FrozenCard frozen={fixture()} today="2026-10-10" />);
    expect(lines()).toEqual([
      "🧊 Заморожено 2 300 $",
      "не трогаем:",
      "подушка1 300 $копилки1 000 $",
      "на платежи до зарплаты 30 ноября:",
      "Квартплата 18.11 — 700 $",
      "всего на карте 3 000 $",
    ]);
    expect(screen.getByRole("table", { name: "Не трогаем" })).toBeTruthy();
  });

  it("shows no breakdown without the piggies", () => {
    render(
      <FrozenCard
        frozen={fixture({ piggies: null, cushion: null })}
        today="2026-10-10"
      />,
    );
    expect(screen.queryByText("не трогаем:")).toBeNull();
    expect(screen.queryByRole("table")).toBeNull();
    expect(lines()[0]).toBe("🧊 Заморожено 2 300 $");
  });

  it("warns when the piggies are over the card", () => {
    const over = fixture({
      amount: 1200,
      piggies: 1500,
      cushion: 0,
      over: 300,
    });
    render(<FrozenCard frozen={over} today="2026-10-10" />);
    expect(screen.getByText("⚠️ Копилки больше карты")).toBeTruthy();
    expect(lines()).toContain("разница 300 $");
    expect(screen.getByText("отложите разницу")).toBeTruthy();
    expect(screen.getByText("или поправьте копилки")).toBeTruthy();
  });

  it("says what to buy when the dollars are short", () => {
    const short = fixture({
      amount: -400,
      total: 300,
      piggies: 0,
      cushion: 0,
      lacking: 400,
      lacking_on: "2026-11-18",
    });
    render(<FrozenCard frozen={short} today="2026-10-10" />);
    expect(lines()).toContain("🧊 Заморожено −400 $");
    expect(screen.getByText("⚠️ Долларов не хватает на платежи")).toBeTruthy();
    expect(lines()).toContain("купите 400 $ до 18.11");
    // без дня — без «до …»
    const unknown = fixture({ amount: -400, lacking: 400, piggies: null });
    render(
      <FrozenCard frozen={{ ...unknown, cushion: null }} today="2026-10-10" />,
    );
    expect(screen.getByText(/^купите 400\s\$$/)).toBeTruthy();
  });

  it("says to buy today when the payment day has passed", () => {
    const late = fixture({
      amount: -600,
      total: 1000,
      piggies: 0,
      cushion: 0,
      lacking: 600,
      lacking_on: "2026-10-08",
    });
    render(<FrozenCard frozen={late} today="2026-10-10" />);
    expect(lines()).toContain("купите 600 $ сегодня");
  });
});
