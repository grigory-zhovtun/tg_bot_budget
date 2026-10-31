/** Запись «Ввода» перед отправкой: знак, строка очереди, первая строка ответа. */

import type { ExpenseInput } from "../api";
import { sheetAmount } from "../format";

/** Доход всегда с плюсом: у него одна кнопка «Записать», без знака. */
export const INCOME_GROUP = "💰 ДОХОДЫ";

/** «Списать» — в таблицу плюс, «Зачислить» — минус. */
export type Sign = "spend" | "credit";

export interface Outgoing {
  entry: ExpenseInput;
  title: string; // строка очереди: «кофе 48 000 UZS»
  head: string | null; // первая строка панели; null — без неё
  card?: string; // карта «Ввода» после записи (у перевода — «Куда»)
}

/** «✓ Списано 48 000 UZS». */
export function spentHead(value: number, currency: string): string {
  return `✓ Списано ${sheetAmount(value, currency)} ${currency}`;
}

/** «✓ Зачислено 48 000 UZS — в таблицу −48 000»: знак таблицы виден только здесь. */
export function creditHead(value: number, currency: string): string {
  return `✓ Зачислено ${sheetAmount(value, currency)} ${currency} — в таблицу ${sheetAmount(-value, currency)}`;
}
