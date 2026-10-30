import { ApiError, type Api, type ExpenseInput } from "../api";
import { retryable } from "./send";

const KEY = "budget.outbox.v1";

/** Трата, которую не удалось отправить: ждёт связи на телефоне. error — сервер
 * отказал по данным: больше не отправляется сама, её можно удалить. */
export interface Pending {
  entry: ExpenseInput;
  title: string; // «кофе 48 000 UZS» — для строки очереди
  error?: string;
}

// память недоступна (приватный режим): очередь живёт до закрытия приложения
let memory: Pending[] = [];

export function readOutbox(): Pending[] {
  try {
    const raw = localStorage.getItem(KEY);
    return raw ? (JSON.parse(raw) as Pending[]) : memory;
  } catch {
    return memory;
  }
}

export function writeOutbox(items: Pending[]): void {
  memory = items;
  try {
    if (items.length > 0) localStorage.setItem(KEY, JSON.stringify(items));
    else localStorage.removeItem(KEY);
  } catch {
    // память недоступна — очередь в memory
  }
}

/** Положить трату в очередь; то же нажатие (entry_id) — одной строкой. */
export function enqueue(item: Pending): Pending[] {
  const id = item.entry.entry_id;
  const items = [
    ...readOutbox().filter((pending) => pending.entry.entry_id !== id),
    item,
  ];
  writeOutbox(items);
  return items;
}

export function dropPending(entryId: string): Pending[] {
  const items = readOutbox().filter((item) => item.entry.entry_id !== entryId);
  writeOutbox(items);
  return items;
}

/** Отправить очередь по порядку, по одной попытке: нет связи — остановиться до
 * следующего раза; сервер отказал по данным — пометить и идти дальше. */
export async function flushOutbox(
  api: Api,
): Promise<{ sent: number; items: Pending[] }> {
  let sent = 0;
  for (const item of readOutbox()) {
    if (item.error) continue;
    const id = item.entry.entry_id;
    try {
      await api.addExpense(item.entry);
      sent += 1;
      dropPending(id);
    } catch (error) {
      if (retryable(error)) break;
      const message =
        error instanceof ApiError ? error.message : "Не получилось записать";
      writeOutbox(
        readOutbox().map((pending) =>
          pending.entry.entry_id === id
            ? { ...pending, error: message }
            : pending,
        ),
      );
    }
  }
  return { sent, items: readOutbox() };
}
