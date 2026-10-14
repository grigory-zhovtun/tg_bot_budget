import { describe, expect, it } from "vitest";
import type { PlanEvent } from "../api";
import { receiptHint } from "./receipt";

function event(over: Partial<PlanEvent>): PlanEvent {
  return {
    slot: 0,
    name: "Аванс",
    day: 15,
    amount: 2_720_000,
    currency: "UZS",
    ...over,
  };
}

describe("receiptHint", () => {
  it("says what came in and when", () => {
    expect(
      receiptHint(
        event({
          status: "done",
          received: 2_720_000,
          received_on: "2026-10-15",
        }),
        "2026-10",
      ),
    ).toBe("✓ пришло 2,72 млн · 15.10");
  });

  it("says a payment is paid", () => {
    const rent = event({
      name: "Квартплата",
      amount: -650,
      currency: "USD",
      status: "done",
      received: -650,
      received_on: "2026-10-17",
    });
    expect(receiptHint(rent, "2026-10")).toBe("✓ оплачено 650,00 USD · 17.10");
  });

  it("shows a part that came in", () => {
    const part = event({
      amount: 108_000,
      currency: "RUB",
      status: "partial",
      received: 54_000,
    });
    expect(receiptHint(part, "2026-10")).toBe(
      "пришло 54 000,00 RUB из 108 000,00 RUB",
    );
  });

  it("waits for a late row and says nothing ahead of time", () => {
    expect(receiptHint(event({ day: 30, status: "waiting" }), "2026-10")).toBe(
      "⏳ ждём с 30.10",
    );
    expect(receiptHint(event({ status: "future" }), "2026-10")).toBeNull();
    expect(receiptHint(event({}), "2026-10")).toBeNull();
  });
});
