import type { PlanEvent } from "../api";
import { short, withCurrency } from "../format";

function plain(value: number, currency: string): string {
  return currency === "UZS"
    ? short(Math.abs(value))
    : withCurrency(Math.abs(value), currency);
}

function dayMonth(isoDay: string): string {
  return `${isoDay.slice(8, 10)}.${isoDay.slice(5, 7)}`;
}

/** Подпись под строкой списка: что по ней пришло; null — подписи нет. */
export function receiptHint(event: PlanEvent, month: string): string | null {
  const { amount, currency, day, received, received_on: on, status } = event;
  if (amount === null || currency === null || day === null) return null;
  const verb = amount > 0 ? "пришло" : "оплачено";
  if (status === "done" && typeof received === "number" && on) {
    return `✓ ${verb} ${plain(received, currency)} · ${dayMonth(on)}`;
  }
  if (status === "partial" && typeof received === "number") {
    return `${verb} ${plain(received, currency)} из ${plain(amount, currency)}`;
  }
  if (status === "waiting") {
    return `⏳ ждём с ${String(day).padStart(2, "0")}.${month.slice(5, 7)}`;
  }
  return null;
}
