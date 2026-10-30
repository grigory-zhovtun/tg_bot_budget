import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type ExpenseInput } from "../api";
import { writtenFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import {
  dropPending,
  enqueue,
  enqueueSending,
  finishSending,
  flushOutbox,
  readOutbox,
  unsent,
  writeOutbox,
} from "./outbox";

const OWNER = 1;
const WIFE = 2;

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
  vi.restoreAllMocks();
  writeOutbox(OWNER, []);
  writeOutbox(WIFE, []);
  localStorage.clear();
});

describe("outbox", () => {
  it("keeps the expense on the phone between openings", () => {
    enqueue(OWNER, { entry: entry("a"), title: "кофе 48 000 UZS" });
    expect(
      JSON.parse(localStorage.getItem("budget.outbox.v1.1") ?? "[]"),
    ).toEqual([{ entry: entry("a"), title: "кофе 48 000 UZS" }]);
    expect(readOutbox(OWNER).map((item) => item.entry.entry_id)).toEqual(["a"]);
    // то же нажатие второй раз — одна строка
    enqueue(OWNER, { entry: entry("a"), title: "кофе 48 000 UZS" });
    expect(readOutbox(OWNER)).toHaveLength(1);
    expect(dropPending(OWNER, "a")).toEqual([]);
    expect(localStorage.getItem("budget.outbox.v1.1")).toBeNull();
  });

  it("keeps each person's expenses apart", () => {
    enqueue(OWNER, { entry: entry("a"), title: "a" });
    enqueue(WIFE, { entry: entry("b"), title: "b" });
    expect(readOutbox(OWNER).map((item) => item.entry.entry_id)).toEqual(["a"]);
    expect(readOutbox(WIFE).map((item) => item.entry.entry_id)).toEqual(["b"]);
  });

  it("lives until the app is closed when the phone storage is off", () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("private mode");
    });
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("private mode");
    });
    enqueue(OWNER, { entry: entry("a"), title: "кофе" });
    expect(readOutbox(OWNER).map((item) => item.entry.entry_id)).toEqual(["a"]);
  });

  it("does not bring back an old list when the phone storage is full", () => {
    enqueue(OWNER, { entry: entry("a"), title: "a" });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("quota");
    });
    enqueue(OWNER, { entry: entry("b"), title: "b" });
    dropPending(OWNER, "a");
    // в памяти телефона остался бы список с «a» — он не должен победить свежий
    expect(readOutbox(OWNER).map((item) => item.entry.entry_id)).toEqual(["b"]);
  });

  it("skips a broken list in the phone storage", () => {
    localStorage.setItem("budget.outbox.v1.1", '{"entry": 1}');
    expect(readOutbox(OWNER)).toEqual([]);
    localStorage.setItem(
      "budget.outbox.v1.1",
      JSON.stringify([
        1,
        { title: "без траты" },
        { entry: entry("a"), title: "a" },
      ]),
    );
    expect(readOutbox(OWNER).map((item) => item.entry.entry_id)).toEqual(["a"]);
  });

  it("sends in order, stops without a connection and keeps the rest", async () => {
    enqueue(OWNER, { entry: entry("a"), title: "a" });
    enqueue(OWNER, { entry: entry("b"), title: "b" });
    enqueue(OWNER, { entry: entry("c"), title: "c" });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockResolvedValueOnce({ ...writtenFixture, status: "duplicate" })
      .mockRejectedValueOnce(new ApiError(0, "network", "Нет связи"));
    const done = await flushOutbox(fakeApi({ addExpense }), OWNER);
    // «уже записано» — тоже отправлено
    expect(done.sent).toBe(1);
    expect(done.items.map((item) => item.entry.entry_id)).toEqual(["b", "c"]);
    expect(done.stopped).toBeNull();
    expect(addExpense).toHaveBeenCalledTimes(2);
  });

  it("marks what the server refused with the field's reason", async () => {
    enqueue(OWNER, { entry: entry("a"), title: "a" });
    enqueue(OWNER, { entry: entry("b"), title: "b" });
    const day = "Дата — не раньше чем 31 день назад и не позже сегодня";
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockRejectedValueOnce(
        new ApiError(422, "validation", "Проверьте введённые данные", { day }),
      )
      .mockResolvedValueOnce(writtenFixture);
    const done = await flushOutbox(fakeApi({ addExpense }), OWNER);
    expect(done.sent).toBe(1);
    expect(done.items).toEqual([{ entry: entry("a"), title: "a", error: day }]);
    await flushOutbox(fakeApi({ addExpense }), OWNER);
    expect(addExpense).toHaveBeenCalledTimes(2);
  });

  it("stops on an expired login and does not mark the expense", async () => {
    enqueue(OWNER, { entry: entry("a"), title: "a" });
    const expired = new ApiError(
      401,
      "unauthorized",
      "Откройте приложение заново из бота",
    );
    const addExpense = vi.fn<Api["addExpense"]>().mockRejectedValue(expired);
    const done = await flushOutbox(fakeApi({ addExpense }), OWNER);
    expect(done.stopped).toBe(expired);
    expect(done.items).toEqual([{ entry: entry("a"), title: "a" }]);
  });

  it("does not send what the entry screen is sending right now", async () => {
    enqueue(OWNER, { entry: entry("a"), title: "a" });
    enqueueSending(OWNER, { entry: entry("b"), title: "b" });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockResolvedValue(writtenFixture);
    const done = await flushOutbox(fakeApi({ addExpense }), OWNER);
    expect(addExpense.mock.calls.map(([sent]) => sent.entry_id)).toEqual(["a"]);
    // идёт отправка — строкой «Не отправлено» не показываем, но с телефона не теряем
    expect(done.items).toEqual([]);
    expect(readOutbox(OWNER).map((item) => item.entry.entry_id)).toEqual(["b"]);
    finishSending(OWNER, "b", true);
    expect(unsent(OWNER).map((item) => item.entry.entry_id)).toEqual(["b"]);
    finishSending(OWNER, "b", false);
    expect(readOutbox(OWNER)).toEqual([]);
  });

  it("sends each expense once when two sends start together", async () => {
    enqueue(OWNER, { entry: entry("a"), title: "a" });
    const addExpense = vi
      .fn<Api["addExpense"]>()
      .mockResolvedValue(writtenFixture);
    const api = fakeApi({ addExpense });
    const [first, second] = await Promise.all([
      flushOutbox(api, OWNER),
      flushOutbox(api, OWNER),
    ]);
    expect(addExpense).toHaveBeenCalledTimes(1);
    expect(first.sent + second.sent).toBeGreaterThan(0);
  });
});
