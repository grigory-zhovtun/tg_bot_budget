import { afterEach, describe, expect, it } from "vitest";
import type { PlanAddable, PlanArticle, PlanEvent, PlanGoal } from "../api";
import { planFixture } from "../mocks/fixtures";
import {
  addRow,
  amountText,
  changeCount,
  editEvent,
  editGoal,
  editRow,
  emptyDraft,
  loadDraft,
  planTotals,
  rebase,
  removeRow,
  storeDraft,
  toSave,
} from "./draft";

function row(key: string): PlanArticle {
  const found = planFixture.rows.find((item) => item.key === key);
  if (!found) throw new Error(key);
  return found;
}

function slot(number: number): PlanEvent {
  const found = (planFixture.events ?? []).find((item) => item.slot === number);
  if (!found) throw new Error(String(number));
  return found;
}

function goal(): PlanGoal {
  if (!planFixture.goal) throw new Error("goal");
  return planFixture.goal;
}

function withCoffee(plan: number) {
  return {
    ...planFixture,
    rows: planFixture.rows.map((item) =>
      item.key === "кофе|UZS" ? { ...item, plan } : item,
    ),
  };
}

describe("plan draft", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("keeps only real changes", () => {
    let draft = editRow(emptyDraft("2026-10"), row("кофе|UZS"), "1800000");
    expect(draft.rows).toEqual({
      "кофе|UZS": { base: 1_500_000, text: "1800000" },
    });
    draft = editRow(draft, row("кофе|UZS"), "1500000");
    expect(changeCount(draft)).toBe(0);
  });

  it("counts the month like the server", () => {
    expect(planTotals(planFixture, emptyDraft("2026-10"))).toEqual({
      incomes: 22_000_000,
      spend: 10_500_000,
      goal: 2_000_000,
      incomeRows: 20_000_000,
      free: 9_500_000,
    });
    const draft = editRow(emptyDraft("2026-10"), row("кофе|UZS"), "1800000");
    expect(planTotals(planFixture, draft).free).toBe(9_200_000);
  });

  it("adds, changes and clears rows of the list", () => {
    let draft = editEvent(emptyDraft("2026-10"), slot(3), {
      name: "Премия",
      day: 25,
      income: true,
      text: "1000000",
      currency: "UZS",
    });
    draft = editEvent(draft, slot(2), null);
    expect(planTotals(planFixture, draft).incomes).toBe(23_000_000);
    expect(toSave(draft).events).toEqual([
      {
        slot: 2,
        base: { name: "Квартплата", day: 18, amount: -500, currency: "USD" },
        value: null,
      },
      {
        slot: 3,
        base: { name: null, day: null, amount: null, currency: null },
        value: { name: "Премия", day: 25, amount: "1000000", currency: "UZS" },
      },
    ]);
  });

  it("sends payments with a minus and cents with a dot", () => {
    let draft = editRow(emptyDraft("2026-10"), row("квартплата|USD"), "650,5");
    draft = editEvent(draft, slot(2), {
      name: "Квартплата",
      day: 18,
      income: false,
      text: "650,5",
      currency: "USD",
    });
    const body = toSave(draft);
    expect(body.rows).toEqual([
      { key: "квартплата|USD", base: 500, plan: "650.5" },
    ]);
    expect(body.events[0]?.value?.amount).toBe("-650.5");
    expect(body.goal).toBeNull();
  });

  it("changes the goal and drops it when it is back", () => {
    let draft = editGoal(emptyDraft("2026-10"), goal(), "3000000", "UZS");
    expect(toSave(draft).goal).toEqual({
      base: { amount: 2_000_000, currency: "UZS" },
      value: { amount: "3000000", currency: "UZS" },
    });
    draft = editGoal(draft, goal(), "2000000", "UZS");
    expect(draft.goal).toBeNull();
  });

  it("rebases the draft on a fresh plan and names the conflicts", () => {
    const draft = editGoal(
      editRow(emptyDraft("2026-10"), row("кофе|UZS"), "1800000"),
      goal(),
      "3000000",
      "UZS",
    );
    const next = rebase(draft, withCoffee(1_600_000));
    expect(next.conflicts).toEqual(["кофе|UZS"]);
    expect(next.draft.rows["кофе|UZS"]).toEqual({
      base: 1_600_000,
      text: "1800000",
    });
    expect(next.draft.goal?.text).toBe("3000000");
  });

  it("drops an edit that the fresh plan already has", () => {
    const draft = editRow(emptyDraft("2026-10"), row("кофе|UZS"), "1800000");
    expect(changeCount(rebase(draft, withCoffee(1_800_000)).draft)).toBe(0);
  });

  it("keeps the draft in the phone between launches", () => {
    const draft = editRow(emptyDraft("2026-10"), row("кофе|UZS"), "1800000");
    storeDraft(1, draft);
    expect(loadDraft(1, "2026-10")).toEqual(draft);
    expect(loadDraft(2, "2026-10")).toBeNull();
    expect(loadDraft(1, "2026-11")).toBeNull();
    storeDraft(1, emptyDraft("2026-10"));
    expect(loadDraft(1, "2026-10")).toBeNull();
  });

  it("ignores a broken stored draft", () => {
    localStorage.setItem("plan-draft:1:2026-10", "{not json");
    expect(loadDraft(1, "2026-10")).toBeNull();
  });

  it("turns numbers into keypad text", () => {
    expect(amountText(1_234_567, false)).toBe("1234567");
    expect(amountText(12.5, true)).toBe("12,5");
    expect(amountText(500, true)).toBe("500");
    expect(amountText(0, false)).toBe("");
  });
});

function offer(subcategory: string, currency: string): PlanAddable {
  const found = planFixture.addable.find(
    (item) => item.subcategory === subcategory && item.currency === currency,
  );
  if (!found) throw new Error(subcategory);
  return found;
}

/** План, где пару уже добавили во вкладку с суммой plan. */
function withRestaurant(plan: number) {
  return {
    ...planFixture,
    rows: [
      ...planFixture.rows,
      {
        key: "ресторан|UZS",
        kind: "spend" as const,
        group: "🍔 ЕДА",
        subcategory: "ресторан",
        icon: "🍷",
        currency: "UZS",
        plan,
        average: 600_000,
        last: 900_000,
        current: 0,
        editable: true,
      },
    ],
    addable: planFixture.addable.slice(1),
  };
}

describe("new articles", () => {
  const start = emptyDraft("2026-10");

  it("count as changes, join the totals and go into the request", () => {
    const draft = addRow(start, offer("ресторан", "UZS"), "500000");
    expect(changeCount(draft)).toBe(1);
    expect(planTotals(planFixture, draft).spend).toBe(
      planTotals(planFixture, start).spend + 500_000,
    );
    expect(toSave(draft).new_rows).toEqual([
      {
        group: "🍔 ЕДА",
        subcategory: "ресторан",
        currency: "UZS",
        plan: "500000",
      },
    ]);
    expect(changeCount(removeRow(draft, "ресторан|UZS"))).toBe(0);
  });

  it("in a currency count at its rate, incomes apart from spending", () => {
    const draft = addRow(
      addRow(start, offer("ресторан", "USD"), "50"),
      offer("премия", "UZS"),
      "1000000",
    );
    const totals = planTotals(planFixture, draft);
    expect(totals.spend).toBe(planTotals(planFixture, start).spend + 600_000);
    expect(totals.incomeRows).toBe(
      planTotals(planFixture, start).incomeRows + 1_000_000,
    );
  });

  it("become an edit with a conflict when the pair was added meanwhile", () => {
    const draft = addRow(start, offer("ресторан", "UZS"), "500000");
    const next = rebase(draft, withRestaurant(300_000));
    expect(next.conflicts).toEqual(["ресторан|UZS"]);
    expect(next.draft.added).toEqual({});
    expect(next.draft.rows["ресторан|UZS"]).toEqual({
      base: 300_000,
      text: "500000",
    });
  });

  it("leave the draft once the tab has them with the same sum", () => {
    const draft = addRow(start, offer("ресторан", "UZS"), "500000");
    const next = rebase(draft, withRestaurant(500_000));
    expect(next.conflicts).toEqual([]);
    expect(changeCount(next.draft)).toBe(0);
  });

  it("stay in the draft while the pair is still missing", () => {
    const draft = addRow(start, offer("такси", "UZS"), "400000");
    expect(rebase(draft, planFixture).draft.added).toEqual(draft.added);
  });
});

describe("drafts from before new articles", () => {
  afterEach(() => localStorage.clear());

  it("still load", () => {
    localStorage.setItem(
      "plan-draft:7:2026-10",
      JSON.stringify({
        month: "2026-10",
        rows: { "кофе|UZS": { base: 1_500_000, text: "1800000" } },
        events: {},
        goal: null,
      }),
    );
    const draft = loadDraft(7, "2026-10");
    expect(draft?.added).toEqual({});
    expect(draft ? changeCount(draft) : 0).toBe(1);
  });
});
