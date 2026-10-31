/** Запись «Ввода» перед отправкой: знак, строка очереди, первая строка ответа. */

import type { ExpenseInput } from "../api";
import { sheetAmount } from "../format";
import { EXCHANGE_SUB } from "./transfer";

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

/** Как очередь на телефоне называет запись. */
type Kind = "spend" | "credit" | "transfer" | "exchange";

function kindOf(entry: ExpenseInput): Kind {
  if (entry.to_source !== undefined) {
    return entry.subcategory === EXCHANGE_SUB ? "exchange" : "transfer";
  }
  return entry.incoming ? "credit" : "spend";
}

const KEPT: Record<Kind, string> = {
  spend: "📤 Трата сохранена на телефоне",
  credit: "📤 Зачисление сохранено на телефоне",
  transfer: "📤 Перевод сохранён на телефоне",
  exchange: "📤 Обмен сохранён на телефоне",
};

const WAITS: Record<Kind, string> = {
  spend: "трата",
  credit: "зачисление",
  transfer: "перевод",
  exchange: "обмен",
};

/** Запись не ушла и ждёт связи: «📤 Перевод сохранён на телефоне». */
export function keptNotice(entry: ExpenseInput): string {
  return KEPT[kindOf(entry)];
}

/** «Отправить сейчас» без связи: записи одного вида — по виду («📵 Связи нет —
 * перевод ждёт на телефоне»), разных — «трата», как было. */
export function waitingNotice(entries: ExpenseInput[]): string {
  const [first, ...rest] = entries.map(kindOf);
  const kind =
    first !== undefined && rest.every((other) => other === first)
      ? first
      : "spend";
  return `📵 Связи нет — ${WAITS[kind]} ждёт на телефоне`;
}
