/** Названия месяцев и числа для подсказок плана: «сен», «сентябрю», «11 дней». */

const NOMINATIVE = [
  "январь",
  "февраль",
  "март",
  "апрель",
  "май",
  "июнь",
  "июль",
  "август",
  "сентябрь",
  "октябрь",
  "ноябрь",
  "декабрь",
];
const DATIVE = [
  "январю",
  "февралю",
  "марту",
  "апрелю",
  "маю",
  "июню",
  "июлю",
  "августу",
  "сентябрю",
  "октябрю",
  "ноябрю",
  "декабрю",
];
const SHORT = [
  "янв",
  "фев",
  "мар",
  "апр",
  "май",
  "июн",
  "июл",
  "авг",
  "сен",
  "окт",
  "ноя",
  "дек",
];

const PREPOSITIONAL = [
  "январе",
  "феврале",
  "марте",
  "апреле",
  "мае",
  "июне",
  "июле",
  "августе",
  "сентябре",
  "октябре",
  "ноябре",
  "декабре",
];

/** «2026-09» → 8. */
function index(month: string): number {
  return Number(month.slice(5, 7)) - 1;
}

export function monthName(month: string): string {
  return NOMINATIVE[index(month)] ?? month;
}

export function monthShort(month: string): string {
  return SHORT[index(month)] ?? month;
}

export function monthDative(month: string): string {
  return DATIVE[index(month)] ?? month;
}

export function capital(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** «июль–сентябрь». */
export function monthRange(months: string[]): string {
  const first = months[0];
  const last = months[months.length - 1];
  return first && last ? `${monthName(first)}–${monthName(last)}` : "";
}

/** Русское число: 1 день, 2 дня, 5 дней, 11 дней, 21 день. */
export function plural(
  count: number,
  one: string,
  few: string,
  many: string,
): string {
  const tens = count % 100;
  const units = count % 10;
  if (tens >= 11 && tens <= 14) return many;
  if (units === 1) return one;
  if (units >= 2 && units <= 4) return few;
  return many;
}

export function days(count: number): string {
  return `${count} ${plural(count, "день", "дня", "дней")}`;
}

/** «в ноябре». */
export function monthIn(month: string): string {
  return `в ${PREPOSITIONAL[index(month)] ?? month}`;
}

/** «2026-12» → «2027-01». */
export function nextMonth(month: string): string {
  const year = Number(month.slice(0, 4));
  const number = Number(month.slice(5, 7));
  return number === 12
    ? `${year + 1}-01`
    : `${year}-${String(number + 1).padStart(2, "0")}`;
}
