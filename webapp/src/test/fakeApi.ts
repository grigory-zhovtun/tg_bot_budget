import { vi } from "vitest";
import type { Api } from "../api";
import {
  adviceFixture,
  bootstrapFixture,
  dashboardFixture,
  forecastFixture,
  nextPlanFixture,
  notCreatedFixture,
  planFixture,
  writtenFixture,
} from "../mocks/fixtures";

/** API для тестов компонентов: ответы фикстур, любой метод можно подменить. */
export function fakeApi(overrides: Partial<Api> = {}): Api {
  return {
    bootstrap: vi.fn<Api["bootstrap"]>().mockResolvedValue(bootstrapFixture),
    addExpense: vi.fn<Api["addExpense"]>().mockResolvedValue(writtenFixture),
    undo: vi
      .fn<Api["undo"]>()
      .mockResolvedValue({ message: "↩️ Удалил из таблицы: строка 4169." }),
    dashboard: vi.fn<Api["dashboard"]>().mockResolvedValue(dashboardFixture),
    forecast: vi.fn<Api["forecast"]>().mockResolvedValue(forecastFixture),
    plan: vi
      .fn<Api["plan"]>()
      .mockImplementation((month) =>
        Promise.resolve(
          month === notCreatedFixture.month ? notCreatedFixture : planFixture,
        ),
      ),
    savePlan: vi
      .fn<Api["savePlan"]>()
      .mockResolvedValue({ ...planFixture, saved: 1 }),
    createNextPlan: vi
      .fn<Api["createNextPlan"]>()
      .mockResolvedValue(nextPlanFixture),
    planAdvice: vi.fn<Api["planAdvice"]>().mockResolvedValue(adviceFixture),
    ...overrides,
  };
}
