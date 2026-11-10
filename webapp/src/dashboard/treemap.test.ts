import { describe, expect, it } from "vitest";
import { treemap, type Rect } from "./treemap";

const area = (rect: Rect) => rect.w * rect.h;

function overlap(a: Rect, b: Rect): number {
  const wide = Math.min(a.x + a.w, b.x + b.w) - Math.max(a.x, b.x);
  const tall = Math.min(a.y + a.h, b.y + b.h) - Math.max(a.y, b.y);
  return wide > 1e-9 && tall > 1e-9 ? wide * tall : 0;
}

describe("treemap", () => {
  it("gives every weight its share of the field", () => {
    const weights = [6, 3, 2, 1];
    const rects = treemap(weights, 1.15, 1);
    rects.forEach((rect, index) => {
      expect(area(rect)).toBeCloseTo((weights[index] ?? 0) / 12);
    });
  });

  it("fills the field without overlaps or going out of it", () => {
    const rects = treemap(
      [5.3, 4.2, 2.2, 0.9, 0.83, 0.74, 0.74, 0.74],
      1.15,
      1,
    );
    expect(rects.reduce((sum, rect) => sum + area(rect), 0)).toBeCloseTo(1);
    for (const rect of rects) {
      expect(rect.x).toBeGreaterThanOrEqual(-1e-9);
      expect(rect.y).toBeGreaterThanOrEqual(-1e-9);
      expect(rect.x + rect.w).toBeLessThanOrEqual(1 + 1e-9);
      expect(rect.y + rect.h).toBeLessThanOrEqual(1 + 1e-9);
    }
    rects.forEach((a, i) =>
      rects.slice(i + 1).forEach((b) => expect(overlap(a, b)).toBe(0)),
    );
  });

  it("keeps tiles close to squares instead of thin strips", () => {
    const rects = treemap(
      [5.3, 4.2, 2.2, 0.9, 0.83, 0.74, 0.74, 0.74],
      1.15,
      1,
    );
    for (const rect of rects) {
      const wide = rect.w * 1.15;
      expect(Math.max(wide / rect.h, rect.h / wide)).toBeLessThan(3.2);
    }
  });

  it("gives one weight the whole field", () => {
    expect(treemap([7], 2, 1)).toEqual([{ x: 0, y: 0, w: 1, h: 1 }]);
  });

  it("leaves a weight of zero or less without a tile", () => {
    const [first, none, minus] = treemap([4, 0, -2], 1, 1);
    expect(first).toEqual({ x: 0, y: 0, w: 1, h: 1 });
    expect(none).toEqual({ x: 0, y: 0, w: 0, h: 0 });
    expect(minus).toEqual({ x: 0, y: 0, w: 0, h: 0 });
  });

  it("has nothing to lay out without weights or a field", () => {
    expect(treemap([], 1, 1)).toEqual([]);
    expect(treemap([0, 0], 1, 1).every((rect) => area(rect) === 0)).toBe(true);
    expect(treemap([1], 0, 1)).toEqual([{ x: 0, y: 0, w: 0, h: 0 }]);
  });
});
