import { describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type ExpenseInput } from "../api";
import { writtenFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import { retryable, sendExpense } from "./send";

const ENTRY: ExpenseInput = {
  entry_id: "e1",
  source: "VISA 9090 UZS",
  category: "🍔 ЕДА",
  subcategory: "кофе",
  amount: "48000",
  comment: "",
};

const noWait = () => Promise.resolve();

describe("sendExpense", () => {
  it("tries again after a lost connection with the same entry id", async () => {
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValueOnce(
        new ApiError(0, "network", "Нет связи с сервером — проверьте интернет"),
      )
      .mockResolvedValueOnce(writtenFixture);
    const result = await sendExpense(fakeApi({ addExpense }), ENTRY, noWait);
    expect(result).toBe(writtenFixture);
    expect(addExpense.mock.calls.map(([sent]) => sent.entry_id)).toEqual([
      "e1",
      "e1",
    ]);
  });

  it("waits 2 s and then 5 s and gives up after three attempts", async () => {
    const waits: number[] = [];
    const down = new ApiError(
      503,
      "sheets_unavailable",
      "Таблица не ответила — попробуйте ещё раз",
    );
    const addExpense = vi.fn<Api["addExpense"]>().mockRejectedValue(down);
    await expect(
      sendExpense(fakeApi({ addExpense }), ENTRY, async (ms) => {
        waits.push(ms);
      }),
    ).rejects.toBe(down);
    expect(addExpense).toHaveBeenCalledTimes(3);
    expect(waits).toEqual([2000, 5000]);
  });

  it("does not repeat a request the server refused", async () => {
    const refused = new ApiError(422, "validation", "Проверьте данные", {
      subcategory: "нет в справочнике",
    });
    const addExpense = vi.fn<Api["addExpense"]>().mockRejectedValue(refused);
    await expect(
      sendExpense(fakeApi({ addExpense }), ENTRY, noWait),
    ).rejects.toBe(refused);
    expect(addExpense).toHaveBeenCalledTimes(1);
  });
});

describe("retryable", () => {
  it("repeats lost connections, long answers, server failures and a write in progress", () => {
    expect(retryable(new ApiError(0, "timeout", "долго"))).toBe(true);
    expect(retryable(new ApiError(502, "server", "сбой"))).toBe(true);
    expect(retryable(new ApiError(409, "in_progress", "сохраняется"))).toBe(
      true,
    );
    expect(retryable(new ApiError(409, "plan_changed", "план поменяли"))).toBe(
      false,
    );
    expect(retryable(new ApiError(401, "auth", "войдите"))).toBe(false);
    expect(retryable(new Error("ошибка в коде"))).toBe(false);
  });
});
