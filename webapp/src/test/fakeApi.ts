import { vi } from "vitest";
import type { Api } from "../api";
import {
  bootstrapFixture,
  dashboardFixture,
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
    plan: vi.fn<Api["plan"]>().mockResolvedValue(planFixture),
    savePlan: vi
      .fn<Api["savePlan"]>()
      .mockResolvedValue({ ...planFixture, saved: 1 }),
    ...overrides,
  };
}
