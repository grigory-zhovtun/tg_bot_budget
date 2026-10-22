/** Вид экрана «Календарь»: что показать, числа и подписи вида, память выбора. */

import type {
  DayArticle,
  Forecast,
  ForecastCurrency,
  ForecastDay,
  ForecastDip,
  ForecastDips,
  ForecastEvent,
  ForecastPast,
} from "../api";
import { longDay, money } from "../format";

export const TOTAL = "total";
export const VIEW_KEY = "forecast-view";

/** «Всего» в сумах или долларах, или одна валюта (её код). */
export interface View {
  scope: string;
  unit: "uzs" | "usd";
}

export const DEFAULT_VIEW: View = { scope: TOTAL, unit: "uzs" };

const NO_DIPS: ForecastDips = { plan: null, pace: null };
const SIGNS: Record<string, string> = { UZS: "сум", USD: "$", RUB: "₽" };
const SUBJECTS: Record<string, string> = {
  UZS: "сумы",
  USD: "доллары",
  RUB: "рубли",
};
const IN_CURRENCY: Record<string, string> = {
  UZS: "в сумах",
  USD: "в долларах",
  RUB: "в рублях",
};
const WEEKDAYS_LONG = [
  "Воскресенье",
  "Понедельник",
  "Вторник",
  "Среда",
  "Четверг",
  "Пятница",
  "Суббота",
];

/** «2026-10-15» → 15. */
export function dayOf(iso: string): number {
  return Number(iso.slice(8, 10));
}

/** День месяца сегодняшней даты: «2026-10-15» и 17 → «2026-10-17». */
export function isoDay(today: string, day: number): string {
  return `${today.slice(0, 7)}-${String(day).padStart(2, "0")}`;
}

/** 25 → «25.10». */
export function shortDate(day: number, today: string): string {
  return `${String(day).padStart(2, "0")}.${today.slice(5, 7)}`;
}

function weekday(iso: string): number {
  return new Date(`${iso}T00:00:00Z`).getUTCDay();
}

/** «Суббота, 17 октября». */
export function dayTitle(iso: string): string {
  return `${WEEKDAYS_LONG[weekday(iso)] ?? ""}, ${longDay(iso)}`;
}

/** Кнопки переключателя: «Всего» и валюты ответа. */
export function scopes(data: Forecast): string[] {
  return [TOTAL, ...data.currencies.map((line) => line.code)];
}

/** «Всего» в долларах — только если у таблицы есть курс доллара. */
export function hasDollars(data: Forecast): boolean {
  return (data.total?.usd.length ?? 0) > 0;
}

/** Валюта чисел вида: у «Всего» — UZS или USD. */
export function viewCode(view: View): string {
  if (view.scope !== TOTAL) return view.scope;
  return view.unit === "usd" ? "USD" : "UZS";
}

/** Валюты вида: во «Всего» — все, иначе одна. */
export function viewCurrencies(data: Forecast, view: View): ForecastCurrency[] {
  return view.scope === TOTAL
    ? data.currencies
    : data.currencies.filter((line) => line.code === view.scope);
}

/** Остаток на вечер по дням в числах вида. */
export function viewDays(data: Forecast, view: View): ForecastDay[] {
  if (view.scope !== TOTAL) return viewCurrencies(data, view)[0]?.days ?? [];
  return (view.unit === "usd" ? data.total?.usd : data.total?.uzs) ?? [];
}

/** Дни по факту в числах вида: с 1-го по сегодня (сегодня — что уже было). */
export function viewPast(data: Forecast, view: View): ForecastPast[] {
  if (view.scope !== TOTAL) return viewCurrencies(data, view)[0]?.past ?? [];
  return (
    (view.unit === "usd" ? data.total?.past_usd : data.total?.past_uzs) ?? []
  );
}

export function viewDips(data: Forecast, view: View): ForecastDips {
  if (view.scope !== TOTAL) {
    return viewCurrencies(data, view)[0]?.below_zero ?? NO_DIPS;
  }
  return data.total?.below_zero ?? NO_DIPS;
}

export interface DayEvent extends ForecastEvent {
  currency: string;
}

/** События дня в виде — каждое со своей валютой. */
export function dayEvents(data: Forecast, view: View, day: number): DayEvent[] {
  return viewCurrencies(data, view).flatMap((line) =>
    line.events
      .filter((event) => event.day === day)
      .map((event) => ({ ...event, currency: line.code })),
  );
}

/** Отметки графика: ▲ в дни поступлений, ▼ в дни платежей и открытых разовых записей. */
export function marks(
  data: Forecast,
  view: View,
): { day: number; up: boolean }[] {
  const found: { day: number; up: boolean }[] = [];
  const add = (day: number, up: boolean) => {
    if (!found.some((mark) => mark.day === day && mark.up === up)) {
      found.push({ day, up });
    }
  };
  for (const line of viewCurrencies(data, view)) {
    for (const event of line.events) add(event.day, event.amount > 0);
    for (const record of line.records) {
      if (record.repeat === "once" && record.open > 0) add(record.day, false);
    }
  }
  return found.sort((a, b) => a.day - b.day);
}

/** «9 060 000 сум», «−$500», «20 000 ₽», «300 EUR» — целыми. */
export function amount(value: number, code: string): string {
  if (code === "USD") {
    const sign = value < 0 && Math.round(value) !== 0 ? "−" : "";
    return `${sign}$${money(Math.abs(value))}`;
  }
  return `${money(value)} ${SIGNS[code] ?? code}`;
}

/** Со знаком: «+2 500 000 сум», «−$500». */
export function signed(value: number, code: string): string {
  return `${value > 0 ? "+" : ""}${amount(value, code)}`;
}

/** Число на плашке: сумы — миллионы с одним знаком, остальное — целыми. */
export function pillValue(value: number, code: string): string {
  if (code !== "UZS") return money(value);
  const millions = (Math.abs(value) / 1_000_000).toFixed(1).replace(".", ",");
  return `${value < 0 && millions !== "0,0" ? "−" : ""}${millions}`;
}

/** В чём числа плашек: «млн сум», «$», «₽». */
export function pillUnit(code: string): string {
  return code === "UZS" ? "млн сум" : (SIGNS[code] ?? code);
}

/** Подпись графика: «в сумах, по курсу таблицы» у «Всего», «в рублях» у валюты. */
export function chartNote(view: View): string {
  const code = viewCode(view);
  const unit = IN_CURRENCY[code] ?? `в ${code}`;
  return view.scope === TOTAL ? `${unit}, по курсу таблицы` : unit;
}

/** «⚠️ По темпу сумы уйдут в минус 25.10, вернутся 30.10» — по линии в минусе. */
export function warnings(data: Forecast, view: View): string[] {
  const dips = viewDips(data, view);
  const subject =
    view.scope === TOTAL ? "деньги" : (SUBJECTS[view.scope] ?? view.scope);
  const today = dayOf(data.today);
  const lines: [string, ForecastDip | null][] = [
    ["По плану", dips.plan],
    ["По темпу", dips.pace],
  ];
  return lines.flatMap(([line, dip]) => {
    if (!dip) return [];
    const when =
      dip.start === today
        ? "в минусе уже сегодня"
        : `уйдут в минус ${shortDate(dip.start, data.today)}`;
    const back =
      dip.until !== null
        ? `, вернутся ${shortDate(dip.until, data.today)}`
        : "";
    return [`⚠️ ${line} ${subject} ${when}${back}`];
  });
}

/** Сумма валют в числах вида: во «Всего» — по курсу таблицы, без валют без курса. */
function inView(
  data: Forecast,
  view: View,
  pick: (line: ForecastCurrency) => number,
): number {
  const lines = viewCurrencies(data, view);
  if (view.scope !== TOTAL)
    return lines.reduce((sum, line) => sum + pick(line), 0);
  const sums = lines.reduce((sum, line) => {
    const rate = line.code === "UZS" ? 1 : data.rates[line.code];
    return line.in_total && rate ? sum + pick(line) * rate : sum;
  }, 0);
  const dollar = data.rates.USD;
  return view.unit === "usd" && dollar ? sums / dollar : sums;
}

/**
 * Строка обычных трат дня по плану в числах вида: «обычные траты ≈ 470 000 сум
 * (по плану)». Сегодня — сколько от них осталось или что они уже потрачены;
 * во «Всего» — чистым остатком: перерасход сумов не прячется за остатком рублей.
 */
export function ordinary(
  data: Forecast,
  view: View,
  day: number,
): string | null {
  const code = viewCode(view);
  const planned = inView(data, view, (line) => line.per_day_plan);
  if (day !== dayOf(data.today)) {
    return Math.round(planned) > 0
      ? `обычные траты ≈ ${amount(planned, code)} (по плану)`
      : null;
  }
  const left = inView(
    data,
    view,
    (line) => line.per_day_plan - line.spent_today,
  );
  if (Math.round(left) > 0) {
    return `на обычные траты осталось ≈ ${amount(left, code)} (по плану)`;
  }
  return Math.round(planned) > 0
    ? "обычные траты на сегодня по плану уже потрачены"
    : null;
}

export interface ShownArticle extends DayArticle {
  currency: string;
}

/**
 * Статьи дня в виде, каждая в своей валюте: траты от больших (по сумам) и
 * доходы. Во «Всего» — только валюты итога дня, как его суммы.
 */
export function dayArticles(
  data: Forecast,
  view: View,
  day: number,
): { spent: ShownArticle[]; earned: ShownArticle[] } {
  const lines = viewCurrencies(data, view).filter(
    (line) => view.scope !== TOTAL || line.in_total,
  );
  const all = lines.flatMap((line) =>
    (line.past.find((point) => point.day === day)?.articles ?? []).map(
      (article) => ({ ...article, currency: line.code }),
    ),
  );
  const biggest = (a: ShownArticle, b: ShownArticle) => b.uzs - a.uzs;
  return {
    spent: all.filter((article) => !article.income).sort(biggest),
    earned: all.filter((article) => article.income).sort(biggest),
  };
}

/** «120 000 сум», «$5»; возврат — «+50 000 сум возврат». */
export function articleText(article: ShownArticle): string {
  return article.amount < 0 && !article.income
    ? `+${amount(-article.amount, article.currency)} возврат`
    : amount(article.amount, article.currency);
}

/** «минус 25–29.10», «минус 25.10» (один день) или «минус с 25.10» (не вернётся). */
export function minusRange(dip: ForecastDip, today: string): string {
  if (dip.until === null) return `минус с ${shortDate(dip.start, today)}`;
  const last = dip.until - 1;
  return last === dip.start
    ? `минус ${shortDate(last, today)}`
    : `минус ${dip.start}–${shortDate(last, today)}`;
}

export interface CurrencyRow {
  code: string;
  now: string;
  end: string; // по плану на последний день
  pace: string; // «по темпу …» или «минус …»
  minus: boolean;
  counted: boolean; // во «Всего»
}

/** «По валютам»: деньги сейчас → по плану на конец месяца, справа — темп или минус. */
export function currencyRows(data: Forecast): CurrencyRow[] {
  return data.currencies.map((line) => {
    const last = line.days[line.days.length - 1];
    const dip = line.below_zero.pace;
    return {
      code: line.code,
      now: amount(line.now, line.code),
      end: last ? amount(last.plan, line.code) : "",
      pace: dip
        ? minusRange(dip, data.today)
        : `по темпу ${last ? amount(last.pace, line.code) : ""}`,
      minus: dip !== null,
      counted: line.in_total,
    };
  });
}

/** Вид из памяти телефона как есть; нет или испорчен — «Всего, сум». */
export function storedView(): View {
  try {
    const text = localStorage.getItem(VIEW_KEY);
    if (!text) return DEFAULT_VIEW;
    const value: unknown = JSON.parse(text);
    if (typeof value !== "object" || value === null || !("scope" in value)) {
      return DEFAULT_VIEW;
    }
    const scope: unknown = value.scope;
    const unit: unknown = "unit" in value ? value.unit : "uzs";
    if (typeof scope !== "string") return DEFAULT_VIEW;
    return { scope, unit: unit === "usd" ? "usd" : "uzs" };
  } catch {
    return DEFAULT_VIEW;
  }
}

/** Вид, подходящий к ответу: нет такой валюты — «Всего», доллары — если есть курс. */
export function fitView(view: View, data: Forecast): View {
  if (!scopes(data).includes(view.scope)) return DEFAULT_VIEW;
  if (view.unit === "usd" && !hasDollars(data)) {
    return { scope: view.scope, unit: "uzs" };
  }
  return view;
}

/** Вид из памяти телефона, если он подходит к ответу; иначе «Всего, сум». */
export function loadView(data: Forecast): View {
  return fitView(storedView(), data);
}

export function storeView(view: View): void {
  try {
    localStorage.setItem(VIEW_KEY, JSON.stringify(view));
  } catch {
    // память недоступна (приватный режим): вид живёт до закрытия приложения
  }
}
