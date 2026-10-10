import { describe, expect, it } from "vitest";
import { filled, statusOf } from "./status";

describe("plan status", () => {
  it("follows the bot's thresholds", () => {
    expect(statusOf(100, 79)).toBe("ok");
    expect(statusOf(100, 80)).toBe("warn");
    expect(statusOf(100, 100)).toBe("warn");
    expect(statusOf(100, 101)).toBe("over");
    expect(statusOf(0, 50)).toBe("off");
  });
  it("fills the bar up to the plan", () => {
    expect(filled(200, 50)).toBe(0.25);
    expect(filled(100, 150)).toBe(1);
    expect(filled(0, 10)).toBe(1);
    expect(filled(0, 0)).toBe(0);
  });
});
