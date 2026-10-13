import type { Api } from "../api";
import {
  bootstrapFixture,
  dashboardFixture,
  nextPlanFixture,
  notCreatedFixture,
  planFixture,
  writtenFixture,
} from "./fixtures";

function later<T>(value: T): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), 300));
}

/** API без сервера для npm run dev: тестовые данные с задержкой. */
let nextCreated = false; // мок-режим: созданный план следующего месяца живёт до перезагрузки

export const mockApi: Api = {
  bootstrap: () => later(bootstrapFixture),
  addExpense: () => later(writtenFixture),
  undo: () => later({ message: "↩️ Удалил из таблицы: строка 4169." }),
  dashboard: () => later(dashboardFixture),
  plan: (month: string) => {
    if (month !== nextPlanFixture.month) return later(planFixture);
    return later(nextCreated ? nextPlanFixture : notCreatedFixture);
  },
  createNextPlan: () => {
    nextCreated = true;
    return later(nextPlanFixture);
  },
  savePlan: () => later({ ...planFixture, saved: 1 }),
};
