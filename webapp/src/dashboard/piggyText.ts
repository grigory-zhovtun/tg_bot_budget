/** Тексты «Накоплений»: суммы копилок, сроки, процент, «хватает ли». */
import type { Piggy } from "../api";
import { money, moneyText, short } from "../format";

/** «В октябре отложить». */
export const MONTHS_IN = [
  "январе", "феврале", "марте", "апреле", "мае", "июне",
  "июле", "августе", "сентябре", "октябре", "ноябре", "декабре",
]; // prettier-ignore

/** «с декабря по май». */
export const MONTHS_FROM = [
  "января", "февраля", "марта", "апреля", "мая", "июня",
  "июля", "августа", "сентября", "октября", "ноября", "декабря",
]; // prettier-ignore

/** «Ноябрь — 10,00 млн: …». */
export const MONTH_NAMES = [
  "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
  "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]; // prettier-ignore

/** «2026-10» → 9 (номер месяца с нуля). */
export function monthIndex(isoMonth: string): number {
  return Number(isoMonth.slice(5, 7)) - 1;
}

/** Сумма копилки: сумы — «6,00 млн» или «500 000», доллары — «400 $». */
export function piggyMoney(value: number, currency: string): string {
  if (currency === "UZS") return short(value);
  const digits = Number.isInteger(value) ? money(value) : money(value, 2);
  return currency === "USD" ? `${digits} $` : `${digits} ${currency}`;
}

/** «к 20.12, каждый год», «до 01.08.2027», «без срока». */
export function whenText(piggy: Piggy): string {
  if (!piggy.deadline) return "без срока";
  const [year = "", month = "", day = ""] = piggy.deadline.split("-");
  return piggy.repeat === "yearly"
    ? `к ${day}.${month}, каждый год`
    : `до ${day}.${month}.${year}`;
}

/** «25», «2,5» — процент с поступлений. */
export function percentText(value: number): string {
  return String(value).replace(".", ",");
}

/** «2026-12-20» → «20.12». */
export function dayMonth(deadline: string): string {
  const [, month = "", day = ""] = deadline.split("-");
  return `${day}.${month}`;
}

/** Что копилка успеет к сроку, двумя строками: «будет 12 000 000» / «из 12 000 000 ✓»;
 * не успевает — «⚠️ будет 2 000 $» / «из 2 500 $». when — «к 20.12» перед «будет». */
export function forecastLines(
  piggy: Piggy,
  when = "",
): { will: string; of: string } {
  const sum = (value: number) => moneyText(value, piggy.currency);
  const short = piggy.status === "short";
  const head = `${short ? "⚠️ " : ""}${when ? `${when} ` : ""}`;
  return {
    will: `${head}будет ${sum(piggy.forecast ?? 0)}`,
    of: `из ${sum(piggy.target ?? 0)}${short ? "" : " ✓"}`,
  };
}
