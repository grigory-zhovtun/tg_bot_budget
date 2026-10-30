import {
  ApiError,
  type Api,
  type ExpenseInput,
  type ExpenseResult,
} from "../api";

// пауза перед второй и третьей попыткой: связь телефона часто возвращается за секунды
export const RETRY_DELAYS = [2_000, 5_000];

export type Wait = (ms: number) => Promise<void>;

export const sleep: Wait = (ms) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/** Тот же запрос можно послать снова: нет связи или долгий ответ (статус 0), сбой
 * сервера или таблицы (5xx), запись ещё сохраняется. Ошибки данных — нельзя. */
export function retryable(error: unknown): boolean {
  if (!(error instanceof ApiError)) return false;
  return (
    error.status === 0 || error.status >= 500 || error.code === "in_progress"
  );
}

/** Трата с повторами: тот же entry_id — сервер не запишет её дважды, а на повтор
 * уже записанной ответит «уже записано». */
export async function sendExpense(
  api: Api,
  entry: ExpenseInput,
  wait: Wait = sleep,
): Promise<ExpenseResult> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await api.addExpense(entry);
    } catch (error) {
      const delay = RETRY_DELAYS[attempt];
      if (!retryable(error) || delay === undefined) throw error;
      await wait(delay);
    }
  }
}
