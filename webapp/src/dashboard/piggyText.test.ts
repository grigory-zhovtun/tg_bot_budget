import { describe, expect, it } from "vitest";
import { savingsFixture } from "../mocks/fixtures";
import { nb } from "../test/text";
import { forecastLines } from "./piggyText";

function piggy(name: string) {
  const found = savingsFixture.piggies.find((one) => one.name === name);
  if (!found) throw new Error(`no piggy ${name}`);
  return found;
}

describe("forecastLines", () => {
  it("says what the piggy will have and of what, whole", () => {
    expect(forecastLines(piggy("Новый год"))).toEqual({
      will: nb("будет 12 000 000"),
      of: nb("из 12 000 000 ✓"),
    });
  });

  it("warns when the piggy will not make it", () => {
    expect(forecastLines(piggy("Отпуск"))).toEqual({
      will: nb("⚠️ будет 2 000 $"),
      of: nb("из 2 500 $"),
    });
  });

  it("puts the deadline before «будет»", () => {
    expect(forecastLines(piggy("Новый год"), "к 20.12").will).toBe(
      nb("к 20.12 будет 12 000 000"),
    );
    expect(forecastLines(piggy("Отпуск"), "к 01.06").will).toBe(
      nb("⚠️ к 01.06 будет 2 000 $"),
    );
  });

  it("shows cents of a dollar piggy only when it has them", () => {
    const vacation = { ...piggy("Отпуск"), forecast: 2000.5 };
    expect(forecastLines(vacation).will).toBe(nb("⚠️ будет 2 000,50 $"));
  });
});
