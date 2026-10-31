/** Деньги и даты как в боте: пробелы между разрядами, «млн», «Сегодня». */

const NBSP = " ";

export function money(value: number, fraction = 0): string {
  const fixed = Math.abs(value).toFixed(fraction);
  const [whole = "0", cents] = fixed.split(".");
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, NBSP);
  const sign = value < 0 && Number(fixed) !== 0 ? "−" : "";
  return sign + (cents ? `${grouped},${cents}` : grouped);
}

/** «8,52 млн» от миллиона, иначе «640 000». */
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

const SYMBOLS: Record<string, string> = { USD: "$", RUB: "₽" };

/** Как в боте (app/rich.py): «380 $», «75 000 ₽», «50 EUR»; сумы — без единицы. */
export function inCurrency(
  amount: number,
  currency: string,
  cents = false,
): string {
  const number = money(amount, cents ? 2 : 0);
  if (currency === "UZS") return number;
  return `${number}${NBSP}${SYMBOLS[currency] ?? currency}`;
}

/** Сумма карты или копилки: копейки — только у дробной суммы не в сумах. */
export function moneyText(amount: number, currency: string): string {
  const whole = Number.isInteger(Math.round(amount * 100) / 100);
  return inCurrency(amount, currency, currency !== "UZS" && !whole);
}

/** Сумма D как в таблице, без единицы: сумы — целиком, другие валюты — с копейками
 * только у дробной суммы; минус — «−»: «−500 000», «−100», «−100,50». */
export function sheetAmount(value: number, currency: string): string {
  const whole = Number.isInteger(Math.round(value * 100) / 100);
  return money(value, currency !== "UZS" && !whole ? 2 : 0);
}

/** Строка списка со знаком: «+2 000 000», «−380 $», «−380,50 $». */
export function signedMoney(amount: number, currency: string): string {
  return `${amount < 0 ? "−" : "+"}${moneyText(Math.abs(amount), currency)}`;
}

/** «15-го» не рвётся по дефису: неразрывный дефис. */
export function glued(text: string): string {
  return text.replace(/(\d)-/g, "$1\u2011");
}

/** Сколько дней от from до to (ISO-даты): «2026-10-09» → «2026-10-15» — 6. */
export function daysBetween(from: string, to: string): number {
  const day = 24 * 60 * 60 * 1000;
  return Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / day,
  );
}

/** Сегодня по часам телефона: «2026-10-17». */
export function deviceDay(now: Date = new Date()): string {
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
}

/** Сегодня по боту сейчас: часы телефона, сдвинутые на offset бота от UTC (минут).
 * Без сдвига (справочник из старой копии) или с часами позади — today справочника. */
export function botDay(
  today: string,
  offset: number | undefined,
  now: number = Date.now(),
): string {
  if (offset === undefined) return today;
  const day = new Date(now + offset * 60_000).toISOString().slice(0, 10);
  return day > today ? day : today;
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

/** Пункт жёлтого списка: «+3,15 млн» или «−380,00 USD». */
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
