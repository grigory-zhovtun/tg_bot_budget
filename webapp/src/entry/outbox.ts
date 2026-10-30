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

/** Что дала отправка очереди: items — что осталось (без отправляемых прямо сейчас);
 * stopped — вход устарел: до нового открытия приложения не отправляем. */
export interface Flushed {
  sent: number;
  items: Pending[];
  stopped: ApiError | null;
}

// у каждого человека своя очередь: телефоном могут пользоваться оба
const key = (user: number) => `${KEY}.${user}`;
// память недоступна (приватный режим): очередь живёт до закрытия приложения
const memory = new Map<number, Pending[]>();
// траты, которые сейчас отправляются: второй раз не шлём, строкой очереди не показываем
const sending = new Set<string>();
let running: Promise<Flushed> | null = null; // одна отправка очереди за раз

function isPending(value: unknown): value is Pending {
  return (
    typeof value === "object" &&
    value !== null &&
    "title" in value &&
    typeof value.title === "string" &&
    "entry" in value &&
    typeof value.entry === "object" &&
    value.entry !== null &&
    "entry_id" in value.entry &&
    typeof value.entry.entry_id === "string"
  );
}

export function readOutbox(user: number): Pending[] {
  try {
    const raw = localStorage.getItem(key(user));
    if (raw === null) return memory.get(user) ?? [];
    const value: unknown = JSON.parse(raw);
    return Array.isArray(value) ? value.filter(isPending) : [];
  } catch {
    return memory.get(user) ?? [];
  }
}

export function writeOutbox(user: number, items: Pending[]): void {
  memory.set(user, items);
  try {
    if (items.length > 0)
      localStorage.setItem(key(user), JSON.stringify(items));
    else localStorage.removeItem(key(user));
  } catch {
    // не записалось (память полна) — старый список не должен победить свежий
    try {
      localStorage.removeItem(key(user));
    } catch {
      // память недоступна — очередь в memory
    }
  }
}

/** Положить трату в очередь; то же нажатие (entry_id) — одной строкой. */
export function enqueue(user: number, item: Pending): Pending[] {
  const id = item.entry.entry_id;
  const items = [
    ...readOutbox(user).filter((pending) => pending.entry.entry_id !== id),
    item,
  ];
  writeOutbox(user, items);
  return items;
}

export function dropPending(user: number, entryId: string): Pending[] {
  const items = readOutbox(user).filter(
    (item) => item.entry.entry_id !== entryId,
  );
  writeOutbox(user, items);
  return items;
}

/** Очередь без трат, которые отправляются прямо сейчас, — для строк «Не отправлено». */
export function unsent(user: number): Pending[] {
  return readOutbox(user).filter((item) => !sending.has(item.entry.entry_id));
}

/** Трата ложится на телефон до первой попытки: закрыли приложение во время повторов —
 * она отправится при следующем открытии. */
export function enqueueSending(user: number, item: Pending): void {
  sending.add(item.entry.entry_id);
  enqueue(user, item);
}

/** Попытки кончились: keep — оставить в очереди (нет связи), иначе убрать (записана
 * или сервер отказал — экран это покажет сам). */
export function finishSending(
  user: number,
  entryId: string,
  keep: boolean,
): void {
  sending.delete(entryId);
  if (!keep) dropPending(user, entryId);
}

/** 401 и 403 — не отказ по данным: вход устарел, трата ждёт нового открытия. */
export function stopsQueue(error: unknown): error is ApiError {
  return (
    error instanceof ApiError && (error.status === 401 || error.status === 403)
  );
}

/** Почему сервер отказал: причина поля («Дата — не раньше чем…»), без поля — ответ. */
function refusal(error: unknown): string {
  if (!(error instanceof ApiError)) return "Не получилось записать";
  return Object.values(error.fields)[0] ?? error.message;
}

async function send(api: Api, user: number): Promise<Flushed> {
  let sent = 0;
  let stopped: ApiError | null = null;
  for (const item of readOutbox(user)) {
    const id = item.entry.entry_id;
    if (item.error || sending.has(id)) continue;
    sending.add(id);
    try {
      await api.addExpense(item.entry);
      sent += 1;
      dropPending(user, id);
    } catch (error) {
      if (retryable(error)) break;
      if (stopsQueue(error)) {
        stopped = error;
        break;
      }
      const reason = refusal(error);
      writeOutbox(
        user,
        readOutbox(user).map((pending) =>
          pending.entry.entry_id === id
            ? { ...pending, error: reason }
            : pending,
        ),
      );
    } finally {
      sending.delete(id);
    }
  }
  return { sent, items: unsent(user), stopped };
}

/** Отправить очередь по порядку, по одной попытке: нет связи — остановиться до
 * следующего раза; сервер отказал по данным — пометить и идти дальше. Вторая
 * отправка, пока идёт первая, ждёт её ответа. */
export function flushOutbox(api: Api, user: number): Promise<Flushed> {
  running ??= send(api, user).finally(() => {
    running = null;
  });
  return running;
}
