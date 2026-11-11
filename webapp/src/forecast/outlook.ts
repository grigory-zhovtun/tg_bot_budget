/** Карточка «Деньги до конца месяца»: три точки остатка и темп трат в день. */

import type { Forecast, ForecastCurrency } from "../api";
import { longDay } from "../format";
import {
  TOTAL,
  amount,
  isoDay,
  pillValue,
  viewCurrencies,
  viewDays,
  viewDips,
  type View,
} from "./view";

/** Остаток на вечер дня: по плану и если тратить как сейчас. */
export interface OutlookPoint {
  day: number;
  plan: number;
  pace: number;
}

/** С какого дня денег не хватит и по какой линии: план важнее темпа. */
export interface Shortage {
  line: "plan" | "pace";
  start: number;
}

export interface Outlook {
  today: number;
  now: number; // деньги сейчас
  low: OutlookPoint | null; // меньше всего по плану; null — это сегодня или конец
  end: OutlookPoint;
  short: Shortage | null;
}

/** Траты в день: по плану и как идут сейчас. */
export interface SpendPace {
  plan: number;
  pace: number;
}

export interface Verdict {
  tone: "fast" | "slow" | "even";
  text: string;
}

/**
 * Сумма валют вида в его числах: одна валюта — как есть, «Всего» — по курсам
 * таблицы (валюта без курса не в счёт), в долларах — делим на курс доллара.
 */
function added(
  data: Forecast,
  view: View,
  pick: (line: ForecastCurrency) => number,
): number | null {
  const lines = viewCurrencies(data, view);
  if (view.scope !== TOTAL) {
    const line = lines[0];
    return line ? pick(line) : null;
  }
  const rate = (code: string) => (code === "UZS" ? 1 : data.rates[code]);
  const unit = view.unit === "usd" ? rate("USD") : 1;
  if (!unit) return null;
  let sum = 0;
  for (const line of lines) {
    const toUzs = rate(line.code);
    if (line.in_total && toUzs) sum += pick(line) * toUzs;
  }
  return sum / unit;
}

/** Три точки вида: сейчас, меньше всего и конец месяца; дней впереди нет — null. */
export function outlook(data: Forecast, view: View): Outlook | null {
  const days = viewDays(data, view);
  const first = days[0];
  const end = days[days.length - 1];
  const now = added(data, view, (line) => line.now);
  if (!first || !end || now === null) return null;
  let low = first;
  for (const point of days) {
    if (point.plan < low.plan) low = point;
  }
  const dips = viewDips(data, view);
  const short: Shortage | null = dips.plan
    ? { line: "plan", start: dips.plan.start }
    : dips.pace
      ? { line: "pace", start: dips.pace.start }
      : null;
  return {
    today: first.day,
    now,
    low: low === first || low === end ? null : low,
    end,
    short,
  };
}

/** Траты в день в числах вида; тратить нечего ни так, ни так — null. */
export function spendPace(data: Forecast, view: View): SpendPace | null {
  const plan = added(data, view, (line) => line.per_day_plan);
  const pace = added(data, view, (line) => line.per_day_pace);
  if (plan === null || pace === null) return null;
  if (Math.round(plan) === 0 && Math.round(pace) === 0) return null;
  return { plan, pace };
}

/** «✓ Денег хватает на весь месяц» или с какого дня не хватит. */
export function shortageText(short: Shortage | null, today: string): string {
  if (!short) return "✓ Денег хватает на весь месяц";
  const lead = short.line === "plan" ? "Денег" : "При таком темпе денег";
  if (short.start === Number(today.slice(8, 10))) {
    return short.line === "plan"
      ? "Денег не хватает уже сегодня"
      : `${lead} не хватит уже сегодня`;
  }
  return `${lead} не хватит с ${longDay(isoDay(today, short.start))}`;
}

/** Сумма точки: сумы от миллиона — «31,0 млн», остальное — целиком с единицей. */
export function sumText(value: number, code: string): string {
  return code === "UZS" && Math.abs(value) >= 1_000_000
    ? `${pillValue(value, code)} млн`
    : amount(value, code);
}

/** Траты в день: «1,24 млн», «470 тыс.», «400 сум», «$5». */
export function perDayText(value: number, code: string): string {
  if (code !== "UZS") return amount(value, code);
  const thousands = Math.round(value / 1_000);
  if (thousands >= 1_000) {
    return `${(value / 1_000_000).toFixed(2).replace(".", ",")} млн`;
  }
  return thousands >= 1 ? `${thousands} тыс.` : amount(value, code);
}

/** Темп против плана; разница меньше тысячи сумов или единицы валюты — по плану. */
export function paceVerdict(pace: SpendPace, code: string): Verdict {
  const diff = pace.pace - pace.plan;
  if (Math.abs(diff) < (code === "UZS" ? 1_000 : 0.5)) {
    return { tone: "even", text: "Траты идут по плану" };
  }
  const by = perDayText(Math.abs(diff), code);
  return diff > 0
    ? { tone: "fast", text: `Быстрее плана на ${by} в день` }
    : { tone: "slow", text: `Медленнее плана на ${by} в день` };
}
