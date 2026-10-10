/** Остаток по плану на плитках ввода: из тех же данных, что «Сводка». */

import type { Dashboard } from "../api";
import { filled, statusOf, type Status } from "../dashboard/status";
import { short } from "../format";

export interface PlanFact {
  plan: number;
  fact: number;
}

export interface PlanLine {
  text: string;
  status: Status;
  filled: number;
}

export interface PlanIndex {
  group(name: string): PlanFact | undefined;
  sub(group: string, name: string): PlanFact | undefined;
}

/** «осталось 200 000», «сверх на 50 000», «вне плана · 120 000»; null — нечего показать. */
export function planLine(value: PlanFact | undefined): PlanLine | null {
  if (!value) return null;
  const { plan, fact } = value;
  if (plan <= 0 && fact <= 0) return null;
  const status = statusOf(plan, fact);
  const share = filled(plan, fact);
  if (plan <= 0) {
    return { text: `вне плана · ${short(fact)}`, status, filled: share };
  }
  const text =
    fact > plan
      ? `сверх на ${short(fact - plan)}`
      : `осталось ${short(plan - fact)}`;
  return { text, status, filled: share };
}

export function planIndex(dashboard: Dashboard | null): PlanIndex {
  const groups = new Map<string, PlanFact>();
  const subs = new Map<string, PlanFact>();
  if (dashboard && dashboard.status !== "no_month_tab") {
    for (const group of dashboard.groups) {
      groups.set(group.name, { plan: group.plan, fact: group.fact });
      for (const item of group.items) {
        subs.set(`${group.name}\n${item.name}`, {
          plan: item.plan,
          fact: item.fact,
        });
      }
    }
  }
  return {
    group: (name) => groups.get(name),
    sub: (group, name) => subs.get(`${group}\n${name}`),
  };
}
