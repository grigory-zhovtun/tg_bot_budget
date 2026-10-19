import type { PlanEvent } from "../api";
import { moneyText } from "../format";

function dayMonth(isoDay: string): string {
  return `${isoDay.slice(8, 10)}.${isoDay.slice(5, 7)}`;
}

/** Подпись под строкой списка: что по ней пришло — числа целиком, как в «Сводке»;
 * null — подписи нет. */
export function receiptHint(event: PlanEvent, month: string): string | null {
  const { amount, currency, day, received, received_on: on, status } = event;
  if (amount === null || currency === null || day === null) return null;
  const verb = amount > 0 ? "пришло" : "оплачено";
  const plain = (value: number) => moneyText(Math.abs(value), currency);
  if (status === "done" && typeof received === "number" && on) {
    return `✓ ${verb} ${plain(received)}, ${dayMonth(on)}`;
  }
  if (status === "partial" && typeof received === "number") {
    return `${verb} ${plain(received)} из ${plain(amount)}`;
  }
  if (status === "waiting") {
    return `⏳ ждём с ${String(day).padStart(2, "0")}.${month.slice(5, 7)}`;
  }
  return null;
}
