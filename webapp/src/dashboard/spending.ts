import type { DaySpend } from "../api";

/** Прошедший день сверх своего лимита или в нём, сегодня или день впереди. */
export type BarState = "over" | "ok" | "today" | "ahead";

export interface Bar {
  day: number; // число месяца
  state: BarState;
  value: number; // траты дня; у дня впереди — сколько можно тратить в день
  x: number;
  y: number;
  width: number;
  height: number;
  cut: boolean; // выше шкалы: столбик срезан сверху, его сумма подписана
  tick: number | null; // y чёрточки лимита дня; у дней впереди её нет
}

export interface Size {
  width: number;
  top: number; // верх шкалы
  bottom: number; // нулевая линия
}

/** День выше двух вторых по величине срезается, чтобы остальные не сплющились. */
export const CUT_RATIO = 2;

const BAR_SHARE = 0.66;

const round = (value: number) => Math.round(value * 10) / 10; // SVG хватит десятых

function dayOf(isoDay: string): number {
  return Number(isoDay.slice(8, 10));
}

export function daysInMonth(isoDay: string): number {
  const [year = 0, month = 0] = isoDay.split("-").map(Number);
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

/** Столбики месяца: траты по сегодня, впереди — лимит на день, если он есть. */
export function spendingBars(
  spending: DaySpend[],
  today: string,
  limit: number | null,
  planPerDay: number | null,
  { width, top, bottom }: Size,
): { bars: Bar[]; planY: number | null } {
  const now = dayOf(today);
  const ahead = limit !== null && limit > 0 ? limit : 0;
  const spent = spending.map((day) => Math.max(day.spent, 0));
  const [first = 0, second = 0] = [...spent].sort((a, b) => b - a);
  const tallest =
    second > 0 && first > CUT_RATIO * second ? CUT_RATIO * second : first;
  const peak =
    Math.max(
      tallest,
      ...spending.map((day) => Math.max(day.limit, 0)),
      planPerDay ?? 0,
      ahead,
    ) || 1;
  const slot = width / daysInMonth(today);
  const barWidth = slot * BAR_SHARE;
  const y = (value: number) =>
    round(bottom - (Math.min(value, peak) / peak) * (bottom - top));
  const bar = (
    day: number,
    state: BarState,
    value: number,
    tick: number | null,
  ) => ({
    day,
    state,
    value,
    x: round((day - 1) * slot + (slot - barWidth) / 2),
    y: y(value),
    width: round(barWidth),
    height: round(bottom - y(value)),
    cut: value > peak,
    tick,
  });
  const bars: Bar[] = spending.map((day, index) => {
    const number = dayOf(day.day);
    const value = spent[index] ?? 0;
    const state =
      number === now
        ? "today"
        : day.spent > Math.max(day.limit, 0)
          ? "over"
          : "ok";
    return bar(number, state, value, y(Math.max(day.limit, 0)));
  });
  if (ahead > 0) {
    for (let number = now + 1; number <= daysInMonth(today); number += 1) {
      bars.push(bar(number, "ahead", ahead, null));
    }
  }
  const planY = planPerDay !== null && planPerDay > 0 ? y(planPerDay) : null;
  return { bars, planY };
}

export interface Summary {
  days: number;
  spent: number;
  plan: number;
}

/** Прошедшие дни месяца против плана на день; в первый день считать нечего. */
export function spendingSummary(
  spending: DaySpend[],
  today: string,
  planPerDay: number | null,
): Summary | null {
  const done = spending.filter((day) => day.day < today);
  if (done.length === 0 || planPerDay === null) return null;
  return {
    days: done.length,
    spent: done.reduce((sum, day) => sum + day.spent, 0),
    plan: planPerDay * done.length,
  };
}
