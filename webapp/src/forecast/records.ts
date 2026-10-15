/** Записи календаря на экране: правило словами, дни правила, суммы клеток, подсказки. */

import type {
  AmountHint,
  Forecast,
  ForecastArticle,
  ForecastRecord,
  Repeat,
} from "../api";
import { money } from "../format";
import { TOTAL, dayOf, isoDay, viewCurrencies, type View } from "./view";

/** Группы не трат: в окне записи их нет, API их не примет. */
export const CLOSED_GROUPS = ["💰 ДОХОДЫ", "💳 СЧЕТА"];
export const WEEKDAY_NAMES = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]; // ISO: пн = 1
export const REPEAT_NAMES: Record<Repeat, string> = {
  once: "Разово",
  daily: "Каждый день",
  weekdays: "По дням недели",
  monthly: "Раз в месяц",
};
const CENT = 0.01; // как на сервере: записи статьи больше остатка больше чем на копейку

/** «2026-10-15» → 4: пн = 1 … вс = 7. */
export function isoWeekday(iso: string): number {
  const day = new Date(`${iso}T00:00:00Z`).getUTCDay();
  return day === 0 ? 7 : day;
}

/** «👶 ДЕТИ» → «ДЕТИ». */
export function groupTitle(group: string): string {
  return group.replace(/^[^\p{L}\p{N}]+/u, "");
}

/** «разово», «каждый день», «пн ср», «15-го каждого месяца». */
export function ruleText(
  repeat: Repeat,
  weekdays: number[],
  date: string,
): string {
  if (repeat === "once") return "разово";
  if (repeat === "daily") return "каждый день";
  if (repeat === "weekdays") {
    return [...weekdays]
      .sort((a, b) => a - b)
      .map((day) => WEEKDAY_NAMES[day - 1] ?? "")
      .join(" ");
  }
  return `${dayOf(date)}-го каждого месяца`;
}

/** Дни правила с today по end (последний день того же месяца) — как вхождения на сервере. */
export function ruleDays(
  repeat: Repeat,
  weekdays: number[],
  date: string,
  today: string,
  end: string,
): number[] {
  if (date > end) return [];
  if (repeat === "once") return date >= today ? [dayOf(date)] : [];
  const last = dayOf(end);
  const found: number[] = [];
  for (let day = dayOf(date > today ? date : today); day <= last; day += 1) {
    const falls =
      repeat === "daily" ||
      (repeat === "weekdays" &&
        weekdays.includes(isoWeekday(isoDay(today, day)))) ||
      (repeat === "monthly" && day === Math.min(dayOf(date), last));
    if (falls) found.push(day);
  }
  return found;
}

/** Сколько записей статьи станет больше остатка: before — вклад правки до неё, after — после. */
export function overBy(
  article: ForecastArticle | undefined,
  before: number,
  after: number,
): number {
  const excess = (article?.booked ?? 0) - before + after - (article?.rest ?? 0);
  return excess > CENT ? excess : 0;
}

/** Недели сетки с понедельника: с недели сегодня по неделю последнего дня; null — не этот месяц. */
export function gridWeeks(today: string, end: string): (number | null)[][] {
  const last = dayOf(end);
  const weeks: (number | null)[][] = [];
  for (
    let monday = dayOf(today) - isoWeekday(today) + 1;
    monday <= last;
    monday += 7
  ) {
    const week: (number | null)[] = [];
    for (let day = monday; day < monday + 7; day += 1) {
      week.push(day >= 1 && day <= last ? day : null);
    }
    weeks.push(week);
  }
  return weeks;
}

export interface DayRecord extends ForecastRecord {
  currency: string;
}

/** Записи дня в виде — каждая со своей валютой. */
export function dayRecords(
  data: Forecast,
  view: View,
  day: number,
): DayRecord[] {
  return viewCurrencies(data, view).flatMap((line) =>
    line.records
      .filter((record) => record.day === day)
      .map((record) => ({ ...record, currency: line.code })),
  );
}

/** Открытое у записей дня в единицах вида: во «Всего» — по курсам таблицы. */
export function dayRecordSum(data: Forecast, view: View, day: number): number {
  const records = dayRecords(data, view, day);
  if (view.scope !== TOTAL) {
    return records.reduce((sum, record) => sum + record.open, 0);
  }
  let uzs = 0;
  for (const record of records) {
    const rate = record.currency === "UZS" ? 1 : data.rates[record.currency];
    if (rate) uzs += record.open * rate; // без курса валюта не во «Всего»
  }
  if (view.unit !== "usd") return uzs;
  const dollar = data.rates.USD;
  return dollar ? uzs / dollar : 0;
}

/** Сумма записей в клетке: сумы — «350к», «1,2м»; остальное — целыми. */
export function cellAmount(value: number, code: string): string {
  if (code !== "UZS") return money(value);
  if (value >= 999_500) {
    return `${(value / 1_000_000).toFixed(1).replace(".", ",")}м`;
  }
  if (value >= 1_000) return `${Math.round(value / 1_000)}к`;
  return money(value);
}

export interface HintChip {
  label: string;
  value: number;
}

/** Подсказки суммы: «обычно» и «в прошлый раз», у «каждый день» — «в среднем в день». */
export function hintChips(
  hint: AmountHint | undefined,
  repeat: Repeat,
): HintChip[] {
  if (!hint) return [];
  const chips: [string, number | null][] =
    repeat === "daily"
      ? [["в среднем в день", hint.per_day]]
      : [
          ["обычно", hint.typical],
          ["в прошлый раз", hint.last],
        ];
  return chips.flatMap(([label, value]) =>
    value !== null && value > 0
      ? [{ label: `${label} ${money(value)}`, value }]
      : [],
  );
}
