/** Перевод между своими картами: курс для проверки и строки «в таблицу». */

import type { CardBalance } from "../api";
import { money } from "../format";

export const TRANSFERS_GROUP = "💳 СЧЕТА";
export const TRANSFER_SUB = "переводы";
export const EXCHANGE_SUB = "обмен валюты";

const NBSP = " ";
const UNITS: Record<string, string> = { UZS: "сум", USD: "$", RUB: "₽" };
const unit = (currency: string) => UNITS[currency] ?? currency;

/** «💳 СЧЕТА» → «переводы» или «обмен валюты» открывают «Откуда → Куда». */
export function isTransfer(group: string, sub: string): boolean {
  return (
    group === TRANSFERS_GROUP && (sub === TRANSFER_SUB || sub === EXCHANGE_SUB)
  );
}

/** Остаток карты справочника («VISA 1111 UZS») из «Остатков на картах»; нет — null. */
export function balanceOf(
  cards: CardBalance[] | null,
  source: string,
): CardBalance | null {
  return (
    cards?.find((card) => `${card.name} ${card.currency}` === source) ?? null
  );
}

/** «курс: 1 $ = 12 108 сум» — из двух введённых сумм, только для проверки. Единица —
 * та валюта, что дороже; число целиком, меньше 100 — с двумя знаками. */
export function rateLine(
  sent: number,
  sentCurrency: string,
  got: number,
  gotCurrency: string,
): string {
  // дороже та валюта, единица которой стоит больше единиц другой
  const [one, rate, other]: [string, number, string] =
    sent / got >= 1
      ? [gotCurrency, sent / got, sentCurrency]
      : [sentCurrency, got / sent, gotCurrency];
  const number = rate >= 100 ? money(Math.round(rate)) : money(rate, 2);
  return `курс: 1${NBSP}${unit(one)} = ${number}${NBSP}${unit(other)}`;
}

/** Строка fact пары: карта и D со знаком, как в таблице. */
export interface FactLine {
  card: string;
  amount: number;
}

/** Строки «в таблицу»: снятие плюсом, пополнение минусом (в одной валюте — та же
 * сумма); «не моя карта» (from — null) — одна строка «Куда». */
export function factLines(
  from: string | null,
  to: string,
  sent: number,
  got: number,
  two: boolean,
): FactLine[] {
  if (from === null) return [{ card: to, amount: -got }];
  return [
    { card: from, amount: sent },
    { card: to, amount: -(two ? got : sent) },
  ];
}
