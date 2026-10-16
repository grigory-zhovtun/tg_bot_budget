import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type Api } from "../api";
import { dashboardFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import DashboardScreen from "./DashboardScreen";

describe("DashboardScreen", () => {
  it("shows what is left today, the plan by groups and the chart", async () => {
    const { container } = render(<DashboardScreen api={fakeApi()} />);
    expect(await screen.findByText("Можно сегодня")).toBeTruthy();
    expect(screen.getByText("380 000")).toBeTruthy();
    expect(screen.getByText(/лимит 500 000 · потрачено 120 000/)).toBeTruthy();
    expect(container.querySelector('[data-status="over"]')).not.toBeNull();
    expect(
      screen.getByRole("img", {
        name: "Траты по дням: 9 дней, сверх лимита — 2",
      }),
    ).toBeTruthy();
    expect(screen.getByText(/за 9 дней при плане 4,05 млн/)).toBeTruthy();
    expect(screen.getByText("запас 1,10 млн")).toBeTruthy();
    expect(screen.getByText("можно 500 000 в день")).toBeTruthy();
    expect(screen.getByText("план 450 000 в день")).toBeTruthy();
    expect(screen.getByText("−650,00 USD")).toBeTruthy();
    expect(screen.getByText(/ждём/)).toBeTruthy();
  });

  it("shows when the sums run out and how many dollars to exchange", async () => {
    render(<DashboardScreen api={fakeApi()} />);
    expect(await screen.findByText("Хватит до 18 октября")).toBeTruthy();
    expect(screen.getByText("До 30 октября обменять ≈ $460")).toBeTruthy();
    expect(screen.getByText("не хватит 5,50 млн")).toBeTruthy();
  });

  it("says when the sums last until the end of the month", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      runway: {
        balance: 9_000_000,
        pace: 100_000,
        last_day: null,
        short: 0,
        until: null,
        dollars: null,
      },
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("До конца месяца хватит")).toBeTruthy();
  });

  it("says how far the finished days went over the plan", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      today: "2026-10-03",
      spending: [
        { day: "2026-10-01", spent: 900_000, limit: 450_000 },
        { day: "2026-10-02", spent: 800_000, limit: 435_000 },
        { day: "2026-10-03", spent: 0, limit: 420_000 },
      ],
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("сверх на 800 000")).toBeTruthy();
    expect(
      screen.getByRole("img", {
        name: "Траты по дням: 2 дня, сверх лимита — 2",
      }),
    ).toBeTruthy();
  });

  it("says when the finished days match the plan exactly", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      today: "2026-10-02",
      spending: [
        { day: "2026-10-01", spent: 450_000, limit: 450_000 },
        { day: "2026-10-02", spent: 0, limit: 450_000 },
      ],
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("ровно по плану")).toBeTruthy();
  });

  it("opens a group to show its subcategories", async () => {
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi()} />);
    const plan = await screen.findByRole("region", {
      name: "План-факт по категориям",
    });
    await user.click(within(plan).getByRole("button", { name: /ЕДА/ }));
    expect(within(plan).getByText("🍽️ кафе")).toBeTruthy();
  });

  it("shows the cards now and where the money went this month", async () => {
    render(<DashboardScreen api={fakeApi()} />);
    // плашка — остаток на вечер вчера для сравнения с планом, список — сейчас
    expect(await screen.findByText("На картах утром")).toBeTruthy();
    const cards = screen.getByRole("region", { name: "Остатки на картах" });
    expect(within(cards).getByText("5 250 000 сум")).toBeTruthy();
    expect(
      within(cards).getByText("в банке 2 450,00 RUB · сверено 01.10"),
    ).toBeTruthy();
    const spent = screen.getByRole("region", {
      name: "План-факт по категориям",
    });
    expect(
      within(spent).getByText("за 1–10 октября · план на месяц 5,00 млн"),
    ).toBeTruthy();
    expect(
      within(spent).getByRole("button", {
        name: /ЕДА.*3,34 млн из 3,00 млн · 87 %/,
      }),
    ).toBeTruthy();
    expect(
      within(spent).getByText(/не учтено: 🍔 ЕДА \/ кафе \(USD\)/),
    ).toBeTruthy();
  });

  it("keeps the cards and the chart without the forecast block", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      status: "no_forecast",
      limit: null,
      left_today: null,
      spent_today: null,
      balance: null,
      planned_balance: null,
      runway: null,
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(
      await screen.findByRole("region", { name: "Остатки на картах" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("region", { name: "План-факт по категориям" }),
    ).toBeTruthy();
  });

  it("puts the daily spending right after the plan vs fact chart", async () => {
    render(<DashboardScreen api={fakeApi()} />);
    await screen.findByText("Можно сегодня");
    // две похожие полосатые диаграммы не стоят рядом: план-факт один, следом — дни
    expect(
      screen
        .getAllByRole("heading", { level: 2 })
        .map((heading) => heading.textContent),
    ).toEqual([
      "Остатки на картах",
      "План-факт по категориям",
      "Траты по дням",
      "Впереди",
      "Подписки",
    ]);
  });

  it("offers a retry when the sheet does not answer", async () => {
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockRejectedValueOnce(
        new ApiError(
          503,
          "sheets_unavailable",
          "Таблица не ответила — попробуйте ещё раз",
        ),
      )
      .mockResolvedValueOnce(dashboardFixture);
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(
      await screen.findByText("Таблица не ответила — попробуйте ещё раз"),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("Можно сегодня")).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("explains a missing month tab", async () => {
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockResolvedValue({ ...dashboardFixture, status: "no_month_tab" });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(
      await screen.findByText(
        "Вкладки месяца нет — бот создаёт её 1-го числа.",
      ),
    ).toBeTruthy();
  });

  it("says there is no limit today when the month is behind the plan", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      limit: -50_000,
      left_today: -120_000,
      days_left: 22,
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("Лимита на сегодня нет")).toBeTruthy();
    expect(
      screen.getByText(/до плана на конец месяца не хватает 1,10 млн/),
    ).toBeTruthy();
    expect(screen.getByText("лимита нет")).toBeTruthy(); // в графике вместо «можно»
  });

  it("marks a row of the list that is late", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      upcoming: [
        {
          name: "ЗП",
          day: 5,
          amount: 10_000_000,
          currency: "UZS",
          uzs: 10_000_000,
        },
      ],
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("⏳ 05 — ЗП")).toBeTruthy();
  });
});
