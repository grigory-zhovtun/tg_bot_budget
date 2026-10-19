import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import Keypad from "./Keypad";

describe("Keypad", () => {
  it("keeps its size in the windows", () => {
    render(<Keypad decimals={false} onKey={vi.fn()} />);
    const key = screen.getByRole("button", { name: "5" });
    expect(key.className).toContain("text-2xl");
    expect(key.className).toContain("font-medium");
    expect(key.className).toContain("h-14");
  });

  it("is bigger and bolder when asked", () => {
    render(<Keypad decimals={false} onKey={vi.fn()} big />);
    const key = screen.getByRole("button", { name: "5" });
    expect(key.className).toContain("text-[28px]");
    expect(key.className).toContain("font-extrabold");
    expect(key.className).toContain("h-16");
    expect(key.className).not.toContain("text-2xl");
  });
});
