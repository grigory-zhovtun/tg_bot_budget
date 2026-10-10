/** Клиент API бота: заголовок авторизации, типы ответов, ошибки. */

export interface Source {
  name: string;
  currency: string;
}

export interface Subcategory {
  name: string;
  icon: string;
}

export interface Group {
  name: string;
  emoji: string;
  title: string;
  subcategories: Subcategory[];
}

export interface Bootstrap {
  user: { id: number; first_name: string };
  bot_username: string;
  today: string;
  default_source: string | null;
  sources: Source[];
  groups: Group[];
}

export interface ExpenseInput {
  entry_id: string;
  source: string;
  category: string;
  subcategory: string;
  amount: string;
  comment: string;
  day?: string; // нет — сегодня по часам бота
}

export interface ExpenseResult {
  status: "written" | "duplicate";
  rows: { first: number; last: number };
  lines: string[];
}

export interface UndoResult {
  message: string;
}

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly fields: Record<string, string>;

  constructor(
    status: number,
    code: string,
    message: string,
    fields: Record<string, string> = {},
  ) {
    super(message);
    this.status = status;
    this.code = code;
    this.fields = fields;
  }
}

/** «tma <initData>», если Telegram передал подпись, иначе токен из ссылки кнопки. */
export function authHeader(initData: string, search: string): string | null {
  if (initData) return `tma ${initData}`;
  const launch = new URLSearchParams(search).get("launch");
  return launch ? `Launch ${launch}` : null;
}

interface ErrorBody {
  error: { code: string; message: string; fields?: Record<string, string> };
}

function isErrorBody(body: unknown): body is ErrorBody {
  if (typeof body !== "object" || body === null || !("error" in body)) {
    return false;
  }
  const error: unknown = body.error;
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    "message" in error
  );
}

// Таблица Google иногда думает долго, но не бесконечно ждём ответа
export const REQUEST_TIMEOUT_MS = 25_000;

export async function request<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const auth = authHeader(
    window.Telegram?.WebApp?.initData ?? "",
    window.location.search,
  );
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (auth) headers.Authorization = auth;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetch(`/api${path}`, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch {
    throw controller.signal.aborted
      ? new ApiError(
          0,
          "timeout",
          "Сервер долго не отвечает — попробуйте ещё раз",
        )
      : new ApiError(0, "network", "Нет связи с сервером — проверьте интернет");
  } finally {
    clearTimeout(timer);
  }
  const body: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    if (isErrorBody(body)) {
      const { code, message, fields } = body.error;
      throw new ApiError(response.status, code, message, fields ?? {});
    }
    throw new ApiError(
      response.status,
      "server",
      "Сервер не ответил — попробуйте ещё раз",
    );
  }
  return body as T;
}

export const api = {
  bootstrap: () => request<Bootstrap>("/bootstrap"),
  addExpense: (entry: ExpenseInput) =>
    request<ExpenseResult>("/expenses", {
      method: "POST",
      body: JSON.stringify(entry),
    }),
  undo: () => request<UndoResult>("/expenses/undo", { method: "POST" }),
};

export type Api = typeof api;
