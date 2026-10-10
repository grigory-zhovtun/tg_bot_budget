import { vi } from "vitest";
import type { Api } from "../api";
import { bootstrapFixture, writtenFixture } from "../mocks/fixtures";

/** API для тестов компонентов: ответы фикстур, любой метод можно подменить. */
export function fakeApi(overrides: Partial<Api> = {}): Api {
  return {
    bootstrap: vi.fn<Api["bootstrap"]>().mockResolvedValue(bootstrapFixture),
    addExpense: vi.fn<Api["addExpense"]>().mockResolvedValue(writtenFixture),
    undo: vi
      .fn<Api["undo"]>()
      .mockResolvedValue({ message: "↩️ Удалил из таблицы: строка 4169." }),
    ...overrides,
  };
}
