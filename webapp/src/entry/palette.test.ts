import { describe, expect, it } from "vitest";
import { groupColor, tint } from "./palette";

describe("group colours", () => {
  it("gives groups a colour by meaning", () => {
    expect(groupColor("🍔 ЕДА", 0)).toBe("#ff8a3d");
    expect(groupColor("👶 ДЕТИ", 5)).toBe("#ec5ca0");
    expect(groupColor("💰 ДОХОДЫ", 9)).toBe("#3fb86b");
    expect(groupColor("🚧 РАЗНОЕ", 2)).toBe("#8a96a3");
  });

  it("falls back to the palette by position for unknown groups", () => {
    const first = groupColor("🪐 КОСМОС", 0);
    const second = groupColor("🛸 ПРОЧЕЕ НОВОЕ", 1);
    expect(first).toMatch(/^#[0-9a-f]{6}$/);
    expect(second).not.toBe(first);
  });

  it("makes a translucent tint that works on dark and light themes", () => {
    expect(tint("#ff8a3d", 0.14)).toBe("rgba(255, 138, 61, 0.14)");
  });
});
