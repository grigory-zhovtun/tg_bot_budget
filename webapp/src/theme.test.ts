/// <reference types="node" />
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { GOOD_COLOR } from "./dashboard/controls";

// vitest отдаёт CSS пустым даже с ?raw — читаем файл; тесты идут из папки webapp
const CSS = readFileSync(resolve(process.cwd(), "src/index.css"), "utf-8");

/* Цвета текста — из index.css: светлый — запасной в var(--…, #…), тёмный — из блока data-scheme="dark". */

const LIGHT_BACKGROUNDS = ["#ffffff", "#f1f3f5", "#efeff3", "#f0f0f0"];
const DARK_BACKGROUNDS = [
  "#17212b",
  "#232e3c",
  "#212121",
  "#2c2c2e",
  "#18222d",
];

interface Pair {
  light: string;
  dark: string;
}

function palette(): Record<string, Pair> {
  const block =
    /:root\[data-scheme="dark"\]\s*\{([^}]*)\}/.exec(CSS)?.[1] ?? "";
  const dark = new Map(
    [...block.matchAll(/(--[a-z-]+):\s*(#[0-9a-f]{6})/g)].map((m) => [
      m[1],
      m[2],
    ]),
  );
  const found: Record<string, Pair> = {};
  for (const m of CSS.matchAll(
    /--color-([a-z]+):\s*var\((--[a-z-]+),\s*(#[0-9a-f]{6})\)/g,
  )) {
    const [, name = "", variable = "", light = ""] = m;
    found[name] = { light, dark: dark.get(variable) ?? "" };
  }
  return found;
}

function luminance(hex: string): number {
  const channel = (offset: number) => {
    const c = parseInt(hex.slice(offset, offset + 2), 16) / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * channel(1) + 0.7152 * channel(3) + 0.0722 * channel(5);
}

/** Контраст по WCAG: 4,5 — норма для обычного текста, 7 — повышенная. */
function contrast(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return ((hi ?? 0) + 0.05) / ((lo ?? 0) + 0.05);
}

/** Худший контраст цвета на фонах темы. */
function worst(color: string, backgrounds: string[]): number {
  return Math.min(...backgrounds.map((bg) => contrast(color, bg)));
}

/** Оттенок в градусах: красный — около 0, оранжевый — 20–40. */
function hue(hex: string): number {
  const [r, g, b] = [1, 3, 5].map((o) => parseInt(hex.slice(o, o + 2), 16));
  const max = Math.max(r ?? 0, g ?? 0, b ?? 0);
  const min = Math.min(r ?? 0, g ?? 0, b ?? 0);
  if (max === min) return 0;
  const d = max - min;
  if (max === r) return (60 * (((g ?? 0) - (b ?? 0)) / d) + 360) % 360;
  if (max === g) return 60 * (((b ?? 0) - (r ?? 0)) / d + 2);
  return 60 * (((r ?? 0) - (g ?? 0)) / d + 4);
}

describe("theme colors", () => {
  const colors = palette();

  it("keeps every text color at 7:1 on light and dark Telegram themes", () => {
    for (const name of [
      "hint",
      "danger",
      "ok",
      "warn",
      "link",
      "spent",
      "earned",
      "planned",
    ]) {
      const pair = colors[name];
      expect(pair, name).toBeDefined();
      if (!pair) continue;
      expect(
        worst(pair.light, LIGHT_BACKGROUNDS),
        `${name} light`,
      ).toBeGreaterThanOrEqual(7);
      expect(
        worst(pair.dark, DARK_BACKGROUNDS),
        `${name} dark`,
      ).toBeGreaterThanOrEqual(7);
    }
  });

  it("keeps payment marks above 6:1", () => {
    const pair = colors.payment;
    expect(pair).toBeDefined();
    if (!pair) return;
    expect(worst(pair.light, LIGHT_BACKGROUNDS)).toBeGreaterThanOrEqual(6);
    expect(worst(pair.dark, DARK_BACKGROUNDS)).toBeGreaterThanOrEqual(6);
  });

  it("keeps white text on status badges above 6:1", () => {
    const badges = [
      ...CSS.matchAll(/--color-chip-([a-z]+):\s*(#[0-9a-f]{6})/g),
    ].map((m) => [m[1] ?? "", m[2] ?? ""] as const);
    expect(badges.map(([name]) => name)).toEqual(["bad", "mid", "good"]);
    for (const [name, color] of badges) {
      expect(contrast("#ffffff", color), name).toBeGreaterThanOrEqual(6);
    }
  });

  it("paints Telegram's «Зачислить» with the green badge colour", () => {
    // родной кнопке цвет задаётся строкой — он тот же, что у зелёной плашки
    expect(CSS).toContain(`--color-chip-good: ${GOOD_COLOR};`);
  });

  it("warns in orange, never in red text", () => {
    const pair = colors.danger;
    expect(pair).toBeDefined();
    if (!pair) return;
    expect(hue(pair.light)).toBeGreaterThanOrEqual(20);
    expect(hue(pair.dark)).toBeGreaterThanOrEqual(20);
  });
});
