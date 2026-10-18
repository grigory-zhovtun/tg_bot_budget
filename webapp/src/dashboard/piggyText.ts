/** Тексты «Накоплений»: суммы копилок, сроки, доля 10 %, «хватает ли». */
import type { Enough, Piggy, SavingsMonth } from "../api";
import { money, short } from "../format";

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

/** «к 20.12 · каждый год», «до 01.08.2027», «без срока». */
export function whenText(piggy: Piggy): string {
  if (!piggy.deadline) return "без срока";
  const [year = "", month = "", day = ""] = piggy.deadline.split("-");
  return piggy.repeat === "yearly"
    ? `к ${day}.${month} · каждый год`
    : `до ${day}.${month}.${year}`;
}

/** Куда идёт доля 10 %: «в подушку», «пополам: подушка и «Машина»». */
export function shareText(
  month: SavingsMonth,
  names: Record<string, string>,
): string {
  const goals = month.goals
    .map((goal) => `«${names[goal.id] ?? goal.id}»`)
    .join(", ");
  if (!goals) return "в подушку";
  return month.share_to === "goals"
    ? `на ${goals}`
    : `пополам: подушка и ${goals}`;
}

/** «на жизнь остаётся …, обычные траты — …: не хватает …» или «…: хватает, запас …». */
export function enoughText(enough: Enough): string {
  const head = `на жизнь остаётся ${short(enough.life)}, обычные траты — ${short(enough.ordinary)}`;
  return enough.gap < 0
    ? `${head}: не хватает ${short(-enough.gap)}`
    : `${head}: хватает, запас ${short(enough.gap)}`;
}
