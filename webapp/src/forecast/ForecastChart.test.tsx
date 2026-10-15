import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import ForecastChart from "./ForecastChart";

const DAYS = [
  { day: 15, plan: 100, pace: 50 },
  { day: 16, plan: 60, pace: -20 },
  { day: 17, plan: 30, pace: -60 },
];

describe("ForecastChart", () => {
  it("draws both lines, red below zero and a tap zone for every day", () => {
    const onSelect = vi.fn();
    const { container } = render(
      <ForecastChart
        days={DAYS}
        marks={[{ day: 17, up: false }]}
        selected={15}
        label="До конца месяца"
        onSelect={onSelect}
      />,
    );
    expect(screen.getByRole("img", { name: "До конца месяца" })).toBeTruthy();
    expect(container.querySelectorAll("[data-day]")).toHaveLength(3);
    expect(container.querySelector("g[clip-path]")).not.toBeNull();
    expect(screen.getByText("▼")).toBeTruthy();
    const zone = container.querySelector('[data-day="16"]');
    if (!zone) throw new Error("no tap zone");
    fireEvent.click(zone);
    expect(onSelect).toHaveBeenCalledWith(16);
  });

  it("has no red part when the money stays above zero", () => {
    const { container } = render(
      <ForecastChart
        days={[{ day: 31, plan: 10, pace: 5 }]}
        marks={[]}
        selected={31}
        label="До конца месяца"
        onSelect={() => undefined}
      />,
    );
    expect(container.querySelector("g[clip-path]")).toBeNull();
    expect(container.querySelectorAll("[data-day]")).toHaveLength(1);
  });
});
