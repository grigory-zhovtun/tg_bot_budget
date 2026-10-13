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

export interface PlanItem {
  name: string;
  icon: string;
  plan: number;
  fact: number;
}

export interface GroupPlan {
  name: string;
  plan: number;
  fact: number;
  items: PlanItem[];
}

/** День месяца: траты без платежей из списка и лимит, с которым день начался. */
export interface DaySpend {
  day: string;
  spent: number;
  limit: number;
}

export interface ForecastItem {
  name: string;
  day: number;
  amount: number;
  currency: string;
  uzs: number;
}

export type SubscriptionState = "charged" | "twice" | "expected" | "missed";

export interface Subscription {
  name: string;
  day: number;
  amount: number;
  currency: string;
  uzs: number;
  state: SubscriptionState;
}

/** Прогноз по сумам: когда кончатся и сколько долларов обменять до поступления. */
export interface Runway {
  balance: number;
  pace: number;
  last_day: string | null; // последний день, когда сумы ещё не в минусе
  short: number; // сколько сумов не хватит в самой низкой точке; 0 — хватает
  until: string | null; // поступление, которое закроет минус; null — конец месяца
  dollars: number | null; // сколько долларов обменять; null — нет курса
}

export interface Dashboard {
  status: "ok" | "no_month_tab" | "no_forecast";
  month: string;
  today: string;
  limit: number | null;
  spent_today: number | null;
  left_today: number | null;
  plan_per_day: number | null;
  days_left: number | null;
  balance: number | null;
  planned_balance: number | null;
  frozen: {
    amount: number;
    currency: string;
    uzs: number;
    change: number;
  } | null;
  groups: GroupPlan[];
  spending: DaySpend[]; // с 1-го числа по сегодня
  upcoming: ForecastItem[];
  subscriptions: Subscription[];
  runway: Runway | null;
}

export interface PlanArticle {
  key: string;
  kind: "spend" | "income";
  group: string;
  subcategory: string;
  icon: string;
  currency: string;
  plan: number;
  average: number;
  last: number;
  current: number;
  editable: boolean;
}

export interface PlanEvent {
  slot: number;
  name: string | null;
  day: number | null;
  amount: number | null; // со знаком: «+» поступление, «−» платёж
  currency: string | null;
}

export interface PlanGoal {
  amount: number;
  currency: string;
}

export interface PlanHistory {
  average: string[];
  last: string;
  current: string;
  current_days: number;
}

/** Пара справочника, которой нет во вкладке, — для «＋ статья». */
export interface PlanAddable {
  kind: "spend" | "income";
  group: string;
  subcategory: string;
  icon: string;
  currency: string;
  average: number;
  last: number;
  current: number;
}

export interface MonthPlan {
  month: string;
  title: string;
  status: "ok" | "not_created" | "no_month_tab";
  today: string;
  rates: Record<string, number>;
  history: PlanHistory | null;
  rows: PlanArticle[];
  events: PlanEvent[] | null;
  goal: PlanGoal | null;
  addable: PlanAddable[];
  saved: number | null;
}

export interface EventValue {
  name: string;
  day: number;
  amount: string; // «1000000», «-650.5»
  currency: string;
}

export interface PlanSave {
  month: string;
  rows: { key: string; base: number; plan: string }[];
  events: {
    slot: number;
    base: Omit<PlanEvent, "slot">;
    value: EventValue | null;
  }[];
  goal: {
    base: PlanGoal;
    value: { amount: string; currency: string };
  } | null;
  new_rows: {
    group: string;
    subcategory: string;
    currency: string;
    plan: string;
  }[];
}

export interface PlanConflict {
  plan: MonthPlan;
  conflicts: string[];
}

/** Пункт совета; apply — новая сумма статьи для «Применить». */
export interface AdvicePoint {
  text: string;
  apply: { key: string; plan: number } | null;
}

/** Черновик для советника: значения, как на экране, без того, что было. */
export interface PlanAdviceRequest {
  month: string;
  rows: { key: string; plan: string }[];
  new_rows: PlanSave["new_rows"];
  events: { slot: number; value: EventValue | null }[];
  goal: { amount: string; currency: string } | null;
}

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly fields: Record<string, string>;
  readonly body: unknown; // всё тело ответа — у 409 там свежий план

  constructor(
    status: number,
    code: string,
    message: string,
    fields: Record<string, string> = {},
    body: unknown = null,
  ) {
    super(message);
    this.status = status;
    this.code = code;
    this.fields = fields;
    this.body = body;
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
// Советник: сервер ждёт модель до 60 с, ещё время — на чтение таблицы
export const ADVICE_TIMEOUT_MS = 100_000;

export async function request<T>(
  path: string,
  init: RequestInit = {},
  timeoutMs = REQUEST_TIMEOUT_MS,
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
  const timer = setTimeout(() => controller.abort(), timeoutMs);
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
      throw new ApiError(response.status, code, message, fields ?? {}, body);
    }
    throw new ApiError(
      response.status,
      "server",
      "Сервер не ответил — попробуйте ещё раз",
    );
  }
  return body as T;
}

/** 409 сохранения плана: свежий план и что поменялось после открытия. */
export function planConflict(error: unknown): PlanConflict | null {
  if (!(error instanceof ApiError) || error.code !== "plan_changed")
    return null;
  const body = error.body;
  if (
    typeof body !== "object" ||
    body === null ||
    !("plan" in body) ||
    !("conflicts" in body)
  ) {
    return null;
  }
  return {
    plan: body.plan as MonthPlan,
    conflicts: body.conflicts as string[],
  };
}

export const api = {
  bootstrap: () => request<Bootstrap>("/bootstrap"),
  addExpense: (entry: ExpenseInput) =>
    request<ExpenseResult>("/expenses", {
      method: "POST",
      body: JSON.stringify(entry),
    }),
  undo: () => request<UndoResult>("/expenses/undo", { method: "POST" }),
  dashboard: () => request<Dashboard>("/dashboard"),
  plan: (month: string) =>
    request<MonthPlan>(`/plan?month=${encodeURIComponent(month)}`),
  savePlan: (input: PlanSave) =>
    request<MonthPlan>("/plan", {
      method: "POST",
      body: JSON.stringify(input),
    }),
  createNextPlan: () => request<MonthPlan>("/plan/next", { method: "POST" }),
  planAdvice: (input: PlanAdviceRequest) =>
    request<{ points: AdvicePoint[] }>(
      "/plan/advice",
      { method: "POST", body: JSON.stringify(input) },
      ADVICE_TIMEOUT_MS,
    ),
};

export type Api = typeof api;
