/** Деньги и даты как в боте: пробелы между разрядами, «млн», «Сегодня». */

const NBSP = " ";

export function money(value: number, fraction = 0): string {
  const fixed = Math.abs(value).toFixed(fraction);
  const [whole = "0", cents] = fixed.split(".");
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, NBSP);
  const sign = value < 0 && Number(fixed) !== 0 ? "−" : "";
  return sign + (cents ? `${grouped},${cents}` : grouped);
}

/** «8,52 млн» от миллиона, иначе «650 000». */
export function short(value: number): string {
  if (Math.abs(value) >= 1_000_000) {
    const millions = (Math.abs(value) / 1_000_000).toFixed(2).replace(".", ",");
    return `${value < 0 ? "−" : ""}${millions} млн`;
  }
  return money(value);
}

/** «48 000 UZS» или «12,50 USD». */
export function withCurrency(value: number, currency: string): string {
  return `${money(value, currency === "UZS" ? 0 : 2)} ${currency}`;
}

/** Сколько дней от from до to (ISO-даты): «2026-10-09» → «2026-10-15» — 6. */
export function daysBetween(from: string, to: string): number {
  const day = 24 * 60 * 60 * 1000;
  return Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / day,
  );
}

/** «2026-10-10» ± дни → ISO-дата; считаем в UTC, без часовых поясов. */
export function shiftDay(isoDay: string, days: number): string {
  const date = new Date(`${isoDay}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

const GENITIVE = [
  "января",
  "февраля",
  "марта",
  "апреля",
  "мая",
  "июня",
  "июля",
  "августа",
  "сентября",
  "октября",
  "ноября",
  "декабря",
];

/** «2026-10-18» → «18 октября». */
export function longDay(isoDay: string): string {
  const [, month = "", day = ""] = isoDay.split("-");
  return `${Number(day)} ${GENITIVE[Number(month) - 1] ?? month}`;
}

/** «Сегодня», «Вчера» или «08.10». */
export function dayLabel(isoDay: string, today: string): string {
  if (isoDay === today) return "Сегодня";
  if (isoDay === shiftDay(today, -1)) return "Вчера";
  const [, month = "", day = ""] = isoDay.split("-");
  return `${day}.${month}`;
}

/** Пункт жёлтого списка: «+2,72 млн» или «−650,00 USD». */
export function itemAmount(
  amount: number,
  currency: string,
  uzs: number,
): string {
  const sign = amount > 0 ? "+" : "−";
  return currency === "UZS"
    ? `${sign}${short(Math.abs(uzs))}`
    : `${sign}${withCurrency(Math.abs(amount), currency)}`;
}
