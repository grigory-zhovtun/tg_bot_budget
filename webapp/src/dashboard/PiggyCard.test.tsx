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
    expect(screen.getByText("⚠️ К сроку не успеть").className).toContain(
      "text-danger",
    );
    expect(screen.getByText("не хватает 3 000 000")).toBeTruthy();
  });

  it("marks a collected piggy and this month's part set aside", () => {
    card({ status: "done", collected: 12_000_000, left_this_month: 0 });
    expect(screen.getByText("✓ собрано").className).toContain("bg-chip-good");
    expect(screen.getByText("в этом месяце 6 000 000")).toBeTruthy();
    expect(screen.getByText("✓ отложено").className).toContain("text-ok");
  });

  it("says what is left after a one-time deadline", () => {
    card({ status: "leftover", repeat: "once", leftover: 1_000_000 });
    expect(screen.getByText("⚠️ Срок прошёл")).toBeTruthy();
    expect(screen.getByText("осталось 1 000 000")).toBeTruthy();
  });

  it("names what is left from last year", () => {
    card({ leftover: 500_000 });
    expect(screen.getByText("осталось с прошлого раза 500 000")).toBeTruthy();
  });

  it("shows what is in the piggy after spending from it", () => {
    card({ collected: 12_000_000, balance: 9_600_000, spent: 2_400_000 });
    expect(screen.getByText("12 000 000", { selector: "strong" })).toBeTruthy();
    expect(screen.getByText("в копилке 9 600 000")).toBeTruthy();
    const bar = screen.getByRole("progressbar", { name: "Новый год: собрано" });
    expect(bar.getAttribute("aria-valuenow")).toBe("100");
  });

  it("shows the importance number next to a big name", () => {
    card({ order: 1 });
    expect(screen.getByLabelText("Важность 1").textContent).toBe("1");
    expect(screen.getByRole("heading").className).toContain("text-fact");
  });

  it("says what a short piggy will have by the deadline", () => {
    card({ status: "short", forecast: 3_000_000, short: 9_000_000 });
    expect(screen.getByText("⚠️ Не хватит 9 000 000").className).toContain(
      "text-danger",
    );
    expect(
      screen.getByText("к 20.12 будет 3 000 000 из 12 000 000"),
    ).toBeTruthy();
  });
});
