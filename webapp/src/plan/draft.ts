/** Черновик плана: правки поверх плана из таблицы, итог, запрос на сохранение. */

import type {
  EventValue,
  MonthPlan,
  PlanAddable,
  PlanArticle,
  PlanEvent,
  PlanAdviceRequest,
  PlanGoal,
  PlanSave,
} from "../api";
import { amountValue, apiAmount } from "../entry/amount";

const EPSILON = 0.005; // суммы сравниваем до копейки, как сервер
const STORAGE = "plan-draft";

/** Правка статьи: base — план, который видели при открытии; text — как на клавиатуре. */
export interface RowEdit {
  base: number;
  text: string;
}

/** Новое содержимое строки списка: сумма без знака, знак — «Поступление» или «Платёж». */
export interface EventDraft {
  name: string;
  day: number;
  income: boolean;
  text: string;
  currency: string;
}

export interface EventEdit {
  base: PlanEvent;
  value: EventDraft | null; // null — очистить строку
}

export interface GoalEdit {
  base: PlanGoal;
  text: string;
  currency: string;
}

/** Новая статья: пары во вкладке ещё нет; text — сумма как на клавиатуре. */
export interface AddedRow {
  kind: "spend" | "income";
  group: string;
  subcategory: string;
  icon: string;
  currency: string;
  text: string;
}

export interface Draft {
  month: string;
  rows: Record<string, RowEdit>;
  events: Record<string, EventEdit>; // ключ — номер строки списка
  goal: GoalEdit | null;
  added: Record<string, AddedRow>; // ключ — «подкатегория|ВАЛЮТА»
}

export interface PlanTotals {
  incomes: number;
  spend: number;
  goal: number;
  incomeRows: number;
  free: number;
}

export function emptyDraft(month: string): Draft {
  return { month, rows: {}, events: {}, goal: null, added: {} };
}

export function changeCount(draft: Draft): number {
  return (
    Object.keys(draft.rows).length +
    Object.keys(draft.events).length +
    (draft.goal ? 1 : 0) +
    Object.keys(draft.added).length
  );
}

function close(left: number | null, right: number | null): boolean {
  if (left === null || right === null) return left === right;
  return Math.abs(left - right) < EPSILON;
}

function without<T>(record: Record<string, T>, key: string): Record<string, T> {
  return Object.fromEntries(
    Object.entries(record).filter(([name]) => name !== key),
  );
}

export function eventAmount(value: EventDraft): number {
  return (value.income ? 1 : -1) * amountValue(value.text);
}

function sameEvent(event: PlanEvent, value: EventDraft | null): boolean {
  if (value === null) return event.name === null;
  return (
    event.name === value.name &&
    event.day === value.day &&
    event.currency === value.currency &&
    close(event.amount, eventAmount(value))
  );
}

function sameCells(left: PlanEvent, right: PlanEvent): boolean {
  return (
    left.name === right.name &&
    left.day === right.day &&
    left.currency === right.currency &&
    close(left.amount, right.amount)
  );
}

/** Сумма статьи в черновике; правка, равная плану, убирается. */
export function editRow(
  draft: Draft,
  article: PlanArticle,
  text: string,
): Draft {
  const base = draft.rows[article.key]?.base ?? article.plan;
  const rows = close(amountValue(text), base)
    ? without(draft.rows, article.key)
    : { ...draft.rows, [article.key]: { base, text } };
  return { ...draft, rows };
}

/** Строка списка в черновике; event — строка из плана, а не из черновика. */
export function editEvent(
  draft: Draft,
  event: PlanEvent,
  value: EventDraft | null,
): Draft {
  const key = String(event.slot);
  const base = draft.events[key]?.base ?? event;
  const events = sameEvent(base, value)
    ? without(draft.events, key)
    : { ...draft.events, [key]: { base, value } };
  return { ...draft, events };
}

export function editGoal(
  draft: Draft,
  goal: PlanGoal,
  text: string,
  currency: string,
): Draft {
  const base = draft.goal?.base ?? goal;
  const same =
    currency === base.currency && close(amountValue(text), base.amount);
  return { ...draft, goal: same ? null : { base, text, currency } };
}

export function pairKey(subcategory: string, currency: string): string {
  return `${subcategory}|${currency}`;
}

export function addRow(draft: Draft, item: PlanAddable, text: string): Draft {
  const row: AddedRow = {
    kind: item.kind,
    group: item.group,
    subcategory: item.subcategory,
    icon: item.icon,
    currency: item.currency,
    text,
  };
  const key = pairKey(item.subcategory, item.currency);
  return { ...draft, added: { ...draft.added, [key]: row } };
}

export function removeRow(draft: Draft, key: string): Draft {
  return { ...draft, added: without(draft.added, key) };
}

export function rowPlan(draft: Draft, article: PlanArticle): number {
  const edit = draft.rows[article.key];
  return edit ? amountValue(edit.text) : article.plan;
}

export function shownEvents(plan: MonthPlan, draft: Draft): PlanEvent[] {
  return (plan.events ?? []).map((event) => {
    const edit = draft.events[String(event.slot)];
    if (!edit) return event;
    if (edit.value === null) {
      return {
        slot: event.slot,
        name: null,
        day: null,
        amount: null,
        currency: null,
      };
    }
    return {
      slot: event.slot,
      name: edit.value.name,
      day: edit.value.day,
      amount: eventAmount(edit.value),
      currency: edit.value.currency,
    };
  });
}

export function shownGoal(plan: MonthPlan, draft: Draft): PlanGoal | null {
  if (!plan.goal) return null;
  return draft.goal
    ? { amount: amountValue(draft.goal.text), currency: draft.goal.currency }
    : plan.goal;
}

/** Итог по черновику в сумах — как у сервера: поступления − расходы − цель. */
export function planTotals(plan: MonthPlan, draft: Draft): PlanTotals {
  const uzs = (amount: number, currency: string | null): number =>
    amount * (plan.rates[currency ?? ""] ?? 0);
  let spend = 0;
  let incomeRows = 0;
  for (const row of plan.rows) {
    const value = uzs(rowPlan(draft, row), row.currency);
    if (row.kind === "spend") spend += value;
    else incomeRows += value;
  }
  for (const row of Object.values(draft.added)) {
    const value = uzs(amountValue(row.text), row.currency);
    if (row.kind === "spend") spend += value;
    else incomeRows += value;
  }
  let incomes = 0;
  for (const event of shownEvents(plan, draft)) {
    if (event.name && event.amount !== null && event.amount > 0) {
      incomes += uzs(event.amount, event.currency);
    }
  }
  const shown = shownGoal(plan, draft);
  const goal = shown ? uzs(shown.amount, shown.currency) : 0;
  return { incomes, spend, goal, incomeRows, free: incomes - spend - goal };
}

function eventValue(value: EventDraft): EventValue {
  return {
    name: value.name,
    day: value.day,
    amount: `${value.income ? "" : "-"}${apiAmount(value.text)}`,
    currency: value.currency,
  };
}

export function toSave(draft: Draft): PlanSave {
  return {
    month: draft.month,
    rows: Object.entries(draft.rows).map(([key, edit]) => ({
      key,
      base: edit.base,
      plan: apiAmount(edit.text || "0"),
    })),
    events: Object.values(draft.events).map((edit) => ({
      slot: edit.base.slot,
      base: {
        name: edit.base.name,
        day: edit.base.day,
        amount: edit.base.amount,
        currency: edit.base.currency,
      },
      value: edit.value ? eventValue(edit.value) : null,
    })),
    goal: draft.goal
      ? {
          base: draft.goal.base,
          value: {
            amount: apiAmount(draft.goal.text || "0"),
            currency: draft.goal.currency,
          },
        }
      : null,
    new_rows: Object.values(draft.added).map((row) => ({
      group: row.group,
      subcategory: row.subcategory,
      currency: row.currency,
      plan: apiAmount(row.text || "0"),
    })),
  };
}

/** Черновик для советника: то же, что сохраняем, но без базы. */
export function toAdvice(draft: Draft): PlanAdviceRequest {
  const save = toSave(draft);
  return {
    month: save.month,
    rows: save.rows.map(({ key, plan }) => ({ key, plan })),
    new_rows: save.new_rows,
    events: save.events.map(({ slot, value }) => ({ slot, value })),
    goal: save.goal ? save.goal.value : null,
  };
}

/** Черновик поверх свежего плана: правки остаются, база — новые значения.
 * conflicts — где значение в таблице поменялось после начала правок. */
export function rebase(
  draft: Draft,
  fresh: MonthPlan,
): { draft: Draft; conflicts: string[] } {
  const conflicts: string[] = [];
  let next = emptyDraft(draft.month);
  for (const [key, edit] of Object.entries(draft.rows)) {
    const article = fresh.rows.find((row) => row.key === key && row.editable);
    if (!article) continue;
    if (!close(article.plan, edit.base)) conflicts.push(key);
    next = editRow(next, article, edit.text);
  }
  for (const [key, edit] of Object.entries(draft.events)) {
    const event = (fresh.events ?? []).find(
      (item) => String(item.slot) === key,
    );
    if (!event) continue;
    if (!sameCells(event, edit.base)) conflicts.push(`event:${key}`);
    next = editEvent(next, event, edit.value);
  }
  if (draft.goal && fresh.goal) {
    const { base } = draft.goal;
    if (
      base.currency !== fresh.goal.currency ||
      !close(base.amount, fresh.goal.amount)
    ) {
      conflicts.push("goal");
    }
    next = editGoal(next, fresh.goal, draft.goal.text, draft.goal.currency);
  }
  for (const [key, row] of Object.entries(draft.added)) {
    const article = fresh.rows.find((item) => item.key === key);
    if (article) {
      // пару успели добавить во вкладку: дальше это правка существующей статьи
      if (!close(article.plan, amountValue(row.text))) conflicts.push(key);
      if (article.editable) next = editRow(next, article, row.text);
      continue;
    }
    const offered = fresh.addable.some(
      (item) => pairKey(item.subcategory, item.currency) === key,
    );
    if (offered) next = { ...next, added: { ...next.added, [key]: row } };
  }
  return { draft: next, conflicts };
}

/** Сумма для клавиатуры: копейки только у валюты; 0 — пусто. */
export function amountText(value: number, decimals: boolean): string {
  if (value <= 0) return "";
  if (!decimals) return String(Math.round(value));
  return value
    .toFixed(2)
    .replace(/\.?0+$/, "")
    .replace(".", ",");
}

function storageKey(userId: number, month: string): string {
  return `${STORAGE}:${userId}:${month}`;
}

type StoredDraft = Omit<Draft, "added"> & { added?: Record<string, AddedRow> };

function isStoredDraft(value: unknown, month: string): value is StoredDraft {
  return (
    typeof value === "object" &&
    value !== null &&
    "month" in value &&
    value.month === month &&
    "rows" in value &&
    typeof value.rows === "object" &&
    value.rows !== null &&
    "events" in value &&
    typeof value.events === "object" &&
    value.events !== null &&
    "goal" in value &&
    (!("added" in value) ||
      (typeof value.added === "object" && value.added !== null))
  );
}

export function loadDraft(userId: number, month: string): Draft | null {
  try {
    const text = localStorage.getItem(storageKey(userId, month));
    if (!text) return null;
    const value: unknown = JSON.parse(text);
    if (!isStoredDraft(value, month)) return null;
    const draft: Draft = { ...value, added: value.added ?? {} };
    return changeCount(draft) > 0 ? draft : null;
  } catch {
    return null;
  }
}

export function storeDraft(userId: number, draft: Draft): void {
  try {
    const key = storageKey(userId, draft.month);
    if (changeCount(draft) === 0) localStorage.removeItem(key);
    else localStorage.setItem(key, JSON.stringify(draft));
  } catch {
    // память недоступна (приватный режим): черновик живёт до закрытия приложения
  }
}
