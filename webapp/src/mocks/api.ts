import type { Api } from "../api";
import { bootstrapFixture, dashboardFixture, writtenFixture } from "./fixtures";

function later<T>(value: T): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), 300));
}

/** API без сервера для npm run dev: тестовые данные с задержкой. */
export const mockApi: Api = {
  bootstrap: () => later(bootstrapFixture),
  addExpense: () => later(writtenFixture),
  undo: () => later({ message: "↩️ Удалил из таблицы: строка 4169." }),
  dashboard: () => later(dashboardFixture),
};
