import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { PlanArticle, PlanEvent } from "../api";
import { planFixture } from "../mocks/fixtures";
import ArticleSheet from "./ArticleSheet";
import EventSheet from "./EventSheet";
import GoalSheet from "./GoalSheet";

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

describe("ArticleSheet", () => {
  it("shows the hints and fills the amount from them", async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    render(
      <ArticleSheet
        article={row("кофе|UZS")}
        text=""
        history={planFixture.history}
        onDone={onDone}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByText("Среднее за июль–сентябрь")).toBeTruthy();
    expect(screen.getByText("Октябрь пока (10 дней)")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "= среднему" }));
    expect(
      screen
        .getByRole("status", { name: "Сумма" })
        .textContent?.replace(/\s+/g, " "),
    ).toBe("1 800 000 UZS");
    await user.click(screen.getByRole("button", { name: "= сентябрю" }));
    await user.click(screen.getByRole("button", { name: "Стереть" }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    expect(onDone).toHaveBeenCalledWith("200000");
  });
});

describe("EventSheet", () => {
  it("adds a payment only when it is filled in", async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    render(
      <EventSheet
        event={slot(3)}
        currencies={["UZS", "USD"]}
        onDone={onDone}
        onClose={vi.fn()}
      />,
    );
    const done = screen.getByRole("button", { name: "Готово" });
    expect(done).toHaveProperty("disabled", true);
    await user.type(screen.getByLabelText("Название"), "Садик");
    await user.type(screen.getByLabelText("День"), "5");
    await user.click(screen.getByRole("radio", { name: "Платёж" }));
    for (const key of ["4", "000", "000"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    await user.click(done);
    expect(onDone).toHaveBeenCalledWith({
      name: "Садик",
      day: 5,
      income: false,
      text: "4000000",
      currency: "UZS",
    });
  });

  it("clears a filled row", async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    render(
      <EventSheet
        event={slot(0)}
        currencies={["UZS"]}
        onDone={onDone}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByLabelText("Название")).toHaveProperty("value", "Аванс");
    await user.click(screen.getByRole("button", { name: "Удалить строку" }));
    expect(onDone).toHaveBeenCalledWith(null);
  });
});

describe("GoalSheet", () => {
  it("returns the amount and the currency", async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    render(
      <GoalSheet
        goal={{ amount: 1_000_000, currency: "UZS" }}
        currencies={["UZS", "USD"]}
        onDone={onDone}
        onClose={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("button", { name: "000" }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    expect(onDone).toHaveBeenCalledWith("1000000000", "UZS");
  });
});
