import type { Api } from "../api";
import {
  adviceFixture,
  bootstrapFixture,
  dashboardFixture,
  forecastFixture,
  nextPlanFixture,
  notCreatedFixture,
  planFixture,
  savingsFixture,
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
  forecast: () => later(forecastFixture),
  // мок-режим: запись не сохраняется, приходит тот же прогноз
  addRecord: () => later(forecastFixture),
  updateRecord: () => later(forecastFixture),
  deleteRecord: () => later(forecastFixture),
  plan: (month: string) => {
    if (month !== nextPlanFixture.month) return later(planFixture);
    return later(nextCreated ? nextPlanFixture : notCreatedFixture);
  },
  createNextPlan: () => {
    nextCreated = true;
    return later(nextPlanFixture);
  },
  savePlan: () => later({ ...planFixture, saved: 1 }),
  planAdvice: () => later(adviceFixture),
  // мок-режим: правки копилок не сохраняются, приходит тот же экран
  savings: () => later(savingsFixture),
  addPiggy: () => later(savingsFixture),
  updatePiggy: () => later(savingsFixture),
  closePiggy: () => later(savingsFixture),
  movePiggy: () => later(savingsFixture),
  savePiggies: () => later(savingsFixture),
  setPercent: () => later(savingsFixture),
  setOrder: () => later(savingsFixture),
  excludeSpend: () => later(savingsFixture),
};
