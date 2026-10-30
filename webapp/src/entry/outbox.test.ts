import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type ExpenseInput } from "../api";
import { writtenFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import {
  dropPending,
  enqueue,
  flushOutbox,
  readOutbox,
  writeOutbox,
} from "./outbox";

function entry(id: string): ExpenseInput {
  return {
    entry_id: id,
    source: "VISA 9090 UZS",
    category: "🍔 ЕДА",
    subcategory: "кофе",
    amount: "48000",
    comment: "",
    day: "2026-10-30",
  };
}

afterEach(() => {
  writeOutbox([]);
  localStorage.clear();
  vi.restoreAllMocks();
});

describe("outbox", () => {
  it("keeps the expense on the phone between openings", () => {
    enqueue({ entry: entry("a"), title: "кофе 48 000 UZS" });
    expect(
      JSON.parse(localStorage.getItem("budget.outbox.v1") ?? "[]"),
    ).toEqual([{ entry: entry("a"), title: "кофе 48 000 UZS" }]);
    expect(readOutbox().map((item) => item.entry.entry_id)).toEqual(["a"]);
    // то же нажатие второй раз — одна строка
    enqueue({ entry: entry("a"), title: "кофе 48 000 UZS" });
    expect(readOutbox()).toHaveLength(1);
    expect(dropPending("a")).toEqual([]);
    expect(localStorage.getItem("budget.outbox.v1")).toBeNull();
  });

  it("lives until the app is closed when the phone storage is off", () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("private mode");
    });
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("private mode");
    });
    enqueue({ entry: entry("a"), title: "кофе" });
    expect(readOutbox().map((item) => item.entry.entry_id)).toEqual(["a"]);
  });

  it("sends in order, stops without a connection and keeps the rest", async () => {
    enqueue({ entry: entry("a"), title: "a" });
    enqueue({ entry: entry("b"), title: "b" });
    enqueue({ entry: entry("c"), title: "c" });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockResolvedValueOnce({ ...writtenFixture, status: "duplicate" })
      .mockRejectedValueOnce(new ApiError(0, "network", "Нет связи"));
    const done = await flushOutbox(fakeApi({ addExpense }));
    // «уже записано» — тоже отправлено
    expect(done.sent).toBe(1);
    expect(done.items.map((item) => item.entry.entry_id)).toEqual(["b", "c"]);
    expect(addExpense).toHaveBeenCalledTimes(2);
  });

  it("marks what the server refused and does not send it again", async () => {
    enqueue({ entry: entry("a"), title: "a" });
    enqueue({ entry: entry("b"), title: "b" });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValueOnce(
        new ApiError(422, "validation", "Дата вне окна", { day: "старая" }),
      )
      .mockResolvedValueOnce(writtenFixture);
    const done = await flushOutbox(fakeApi({ addExpense }));
    expect(done.sent).toBe(1);
    expect(done.items).toEqual([
      { entry: entry("a"), title: "a", error: "Дата вне окна" },
    ]);
    await flushOutbox(fakeApi({ addExpense }));
    expect(addExpense).toHaveBeenCalledTimes(2);
  });
});
