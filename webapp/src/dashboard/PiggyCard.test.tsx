import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Piggy } from "../api";
import { savingsFixture } from "../mocks/fixtures";
import PiggyCard from "./PiggyCard";

function piggy(changes: Partial<Piggy> = {}): Piggy {
  const first = savingsFixture.piggies[0];
  if (!first) throw new Error("fixture");
  return { ...first, ...changes };
}

function card(changes: Partial<Piggy>) {
  return render(
    <ul>
      <PiggyCard piggy={piggy(changes)} />
    </ul>,
  );
}

describe("PiggyCard", () => {
  it("says when the deadline cannot be met", () => {
    card({ status: "late", short: 3_000_000 });
    expect(
      screen.getByText("к сроку не успеть — не хватает 3,00 млн").className,
    ).toContain("text-danger");
  });

  it("marks a collected piggy and this month's part set aside", () => {
    card({ status: "done", collected: 12_000_000, left_this_month: 0 });
    expect(screen.getByText("собрано ✓")).toBeTruthy();
    expect(
      screen.getByText("в этом месяце 6,00 млн — отложено ✓"),
    ).toBeTruthy();
  });

  it("says what is left after a one-time deadline", () => {
    card({ status: "leftover", repeat: "once", leftover: 1_000_000 });
    expect(screen.getByText("срок прошёл, осталось 1,00 млн")).toBeTruthy();
  });

  it("names what is left from last year", () => {
    card({ leftover: 500_000 });
    expect(screen.getByText("осталось с прошлого раза 500 000")).toBeTruthy();
  });

  it("shows what is in the piggy after spending from it", () => {
    card({ collected: 12_000_000, balance: 9_600_000, spent: 2_400_000 });
    expect(screen.getByText("накоплено 12,00 млн из 12,00 млн")).toBeTruthy();
    expect(screen.getByText("в копилке 9,60 млн")).toBeTruthy();
    const bar = screen.getByRole("progressbar", { name: "Новый год: собрано" });
    expect(bar.getAttribute("aria-valuenow")).toBe("100");
  });

  it("shows the importance number", () => {
    card({ order: 1 });
    expect(screen.getByLabelText("Важность 1").textContent).toBe("1");
  });

  it("says what a short piggy will have by the deadline", () => {
    card({ status: "short", forecast: 3_000_000, short: 9_000_000 });
    expect(
      screen.getByText(
        "⚠ к 20.12 будет 3,00 млн из 12,00 млн — не хватит 9,00 млн",
      ).className,
    ).toContain("text-danger");
  });
});
