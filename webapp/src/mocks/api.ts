import type { Api, Bootstrap, SavingsScreen } from "../api";
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

// npm run dev: карты — как в «Остатках» сводки (у HUMO 4444 остатка нет), группы —
// с «💳 СЧЕТА» и «💰 ДОХОДЫ»: виден перевод «Откуда → Куда» и «Записать» доходов.
// bootstrapFixture общий для тестов — его не трогаем
const devBootstrap: Bootstrap = {
  ...bootstrapFixture,
  default_source: "VISA 1111 UZS",
  sources: [
    { name: "VISA 1111 UZS", currency: "UZS" },
    { name: "HUMO 4444 UZS", currency: "UZS" },
    { name: "VISA 2222 USD", currency: "USD" },
    { name: "VISA 6666 RUB", currency: "RUB" },
  ],
  groups: [
    ...bootstrapFixture.groups,
    {
      name: "💳 СЧЕТА",
      emoji: "💳",
      title: "СЧЕТА",
      subcategories: [
        { name: "переводы", icon: "🔄" },
        { name: "обмен валюты", icon: "💱" },
        { name: "выравнивание", icon: "⚖️" },
      ],
    },
    {
      name: "💰 ДОХОДЫ",
      emoji: "💰",
      title: "ДОХОДЫ",
      subcategories: [{ name: "зарплата", icon: "💵" }],
    },
  ],
};

/** API без сервера для npm run dev: тестовые данные с задержкой. */
let nextCreated = false; // мок-режим: созданный план следующего месяца живёт до перезагрузки

// мок-режим: подушка меньше месяца — «Отложил…» отдаёт ей сначала её часть
const underMonth: SavingsScreen = {
  ...savingsFixture,
  month: savingsFixture.month && {
    ...savingsFixture.month,
    left: 3_000_000,
    cushion: 1_000_000,
    cushion_saved: 400_000,
    cushion_room: 600_000,
  },
};

export const mockApi: Api = {
  bootstrap: () => later(devBootstrap),
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
  savings: () => later(underMonth),
  addPiggy: () => later(savingsFixture),
  updatePiggy: () => later(savingsFixture),
  closePiggy: () => later(savingsFixture),
  movePiggy: () => later(savingsFixture),
  savePiggies: () => later(savingsFixture),
  setPercent: () => later(savingsFixture),
  setOrder: () => later(savingsFixture),
  setLife: () => later(savingsFixture),
  excludeSpend: () => later(savingsFixture),
};
