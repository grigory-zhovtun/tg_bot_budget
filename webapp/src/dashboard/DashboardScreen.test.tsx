import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api } from "../api";
import { dashboardFixture } from "../mocks/fixtures";
import { Store, StoreProvider } from "../store";
import { fakeApi } from "../test/fakeApi";
import DashboardScreen from "./DashboardScreen";

async function show(user: ReturnType<typeof userEvent.setup>, name: string) {
  await user.click(await screen.findByRole("radio", { name }));
}

afterEach(() => {
  localStorage.clear();
});

describe("DashboardScreen", () => {
  it("opens on «Сегодня» with what is left today", async () => {
    render(<DashboardScreen api={fakeApi()} />);
    const today = await screen.findByRole("region", { name: "Можно сегодня" });
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
      "Сводка · октябрь",
    );
    expect(within(today).getByText("380 000 сум").className).toContain(
      "text-big",
    );
    expect(within(today).getByText("можно потратить сегодня")).toBeTruthy();
    expect(
      within(today).getByText("лимит 500 000, потрачено 120 000"),
    ).toBeTruthy();
    expect(
      screen
        .getByRole("radio", { name: "Сегодня" })
        .getAttribute("aria-checked"),
    ).toBe("true");
    expect(
      screen.queryByRole("region", { name: "План-факт по категориям" }),
    ).toBeNull();
  });

  it("«Траты» shows the plan by groups and the daily chart", async () => {
    const user = userEvent.setup();
    const { container } = render(<DashboardScreen api={fakeApi()} />);
    await show(user, "Траты");
    expect(container.querySelector('[data-status="over"]')).not.toBeNull();
    expect(
      screen.getByRole("img", {
        name: "Траты по дням: 9 дней, сверх лимита — 2",
      }),
    ).toBeTruthy();
    expect(screen.getByText("✓ Запас 1 100 000 за 9 дней")).toBeTruthy();
    // «потрачено» на вкладке одно — в итоге месяца сверху
    expect(screen.getAllByText(/^Потрачено/i)).toHaveLength(1);
    expect(screen.getByText("можно 500 000 в день")).toBeTruthy();
    expect(screen.getByText("план 450 000 в день")).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Можно сегодня" })).toBeNull();
  });

  it("«Впереди» shows the list and the subscriptions", async () => {
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi()} />);
    await show(user, "Впереди");
    // платёж списка — оранжевым, как точки платежей в календаре
    expect(screen.getByText("−380 $").className).not.toContain("text-payment");
    expect(screen.getByText("⏳ ждём")).toBeTruthy();
  });

  it("shows when the sums run out and how many dollars to exchange", async () => {
    render(<DashboardScreen api={fakeApi()} />);
    const sums = await screen.findByRole("region", { name: "Сумы" });
    expect(within(sums).getByText("💳 Сумы 2 000 000").className).toContain(
      "text-fact",
    );
    expect(within(sums).getByText("⚠️ хватит до 18 октября")).toBeTruthy();
    expect(
      within(sums).getByText("до 30 октября обменять ≈ 460 $"),
    ).toBeTruthy();
    expect(within(sums).getByText("не хватит 5 500 000")).toBeTruthy();
    expect(within(sums).getByText("в день ≈ 500 000")).toBeTruthy();
    expect(within(sums).getByText("в среднем за 14 дней")).toBeTruthy();
  });

  it("shows how far today went over the limit", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      left_today: -20_000,
      spent_today: 520_000,
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    const today = await screen.findByRole("region", { name: "Можно сегодня" });
    expect(within(today).getByText("−20 000 сум")).toBeTruthy();
    expect(within(today).getByText("⚠️ сверх лимита дня")).toBeTruthy();
    expect(
      within(today).getByText("лимит 500 000, потрачено 520 000"),
    ).toBeTruthy();
  });

  it("shows the frozen dollars in a card of their own", async () => {
    render(<DashboardScreen api={fakeApi()} />);
    const frozen = await screen.findByRole("region", { name: "Заморожено" });
    expect(within(frozen).getByText("🧊 Заморожено 1 000 $")).toBeTruthy();
    expect(within(frozen).getByText("с 1-го числа −50 $")).toBeTruthy();
  });

  it("says when the cards are above the plan and the frozen sum stood still", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      balance: 43_000_000,
      frozen: { amount: 1000, currency: "USD", uzs: 12_000_000, change: 0 },
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("✓ больше плана на 1 500 000")).toBeTruthy();
    expect(screen.getByText("с 1-го числа без изменений")).toBeTruthy();
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
    expect(await screen.findByText("✓ до конца месяца хватит")).toBeTruthy();
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
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    await show(user, "Траты");
    expect(screen.getByText("⚠️ Сверх плана на 800 000 за 2 дня")).toBeTruthy();
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
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    await show(user, "Траты");
    expect(screen.getByText("✓ Ровно по плану за 1 день")).toBeTruthy();
  });

  it("opens a group to show its subcategories", async () => {
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi()} />);
    await show(user, "Траты");
    const plan = screen.getByRole("region", {
      name: "План-факт по категориям",
    });
    await user.click(within(plan).getByRole("button", { name: /ЕДА/ }));
    expect(within(plan).getByText("🍽️ кафе")).toBeTruthy();
  });

  it("shows the cards now and where the money went this month", async () => {
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi()} />);
    // карточка — остаток на вечер вчера для сравнения с планом, таблица — сейчас
    expect(await screen.findByText("На картах 39 000 000")).toBeTruthy();
    expect(screen.getByText("утром, по плану 41 500 000")).toBeTruthy();
    expect(
      screen.getByText("⚠️ меньше плана на 2 500 000").className,
    ).toContain("text-danger");
    const cards = screen.getByRole("region", { name: "Остатки на картах" });
    expect(within(cards).getByText("5 250 000")).toBeTruthy();
    expect(
      within(cards).getByText("⚠️ VISA 6666 в банке 2 450 ₽"),
    ).toBeTruthy();
    expect(within(cards).getByText("сверено 01.10")).toBeTruthy();
    expect(within(cards).getByText("Всего ≈ 23 613 075 сум")).toBeTruthy();
    await show(user, "Траты");
    const spent = screen.getByRole("region", {
      name: "План-факт по категориям",
    });
    expect(within(spent).getByText("за 1–10 октября")).toBeTruthy();
    expect(
      within(spent).getByText("Потрачено 3 842 000 из 5 000 000"),
    ).toBeTruthy();
    expect(
      within(spent).getByRole("button", { name: /ЕДА.*⚠️ 111\s%/ }),
    ).toBeTruthy();
    expect(within(spent).getByText("3 342 000 из 3 000 000")).toBeTruthy();
    expect(within(spent).getByText("кафе (USD)")).toBeTruthy();
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
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    // лимита нет именно в «Сегодня» — там и объясняем
    expect(
      await screen.findByRole("region", { name: "Остатки на картах" }),
    ).toBeTruthy();
    expect(screen.getByText("⚠️ Нет блока прогноза")).toBeTruthy();
    expect(screen.getByText("лимит на день не посчитать")).toBeTruthy();
    await show(user, "Траты");
    expect(
      screen.getByRole("region", { name: "План-факт по категориям" }),
    ).toBeTruthy();
    // список без блока прогноза не прочитать — не «платежей нет»
    await show(user, "Впереди");
    expect(screen.getByText("⚠️ Нет блока прогноза")).toBeTruthy();
    expect(
      screen.queryByText("До конца месяца платежей и подписок нет"),
    ).toBeNull();
  });

  it("each section has its own cards", async () => {
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi()} />);
    await screen.findByRole("region", { name: "Можно сегодня" });
    const headings = () =>
      screen
        .getAllByRole("heading", { level: 2 })
        .map((heading) => heading.textContent);
    expect(headings()).toEqual(["Остатки на картах"]);
    await show(user, "Траты");
    // две похожие полосатые диаграммы не стоят рядом: план-факт один, следом — дни
    expect(headings()).toEqual(["План-факт по категориям", "Траты по дням"]);
    await show(user, "Впереди");
    expect(headings()).toEqual(["Впереди", "Подписки"]);
  });

  it("switches sections and keeps the choice for the next launch", async () => {
    const user = userEvent.setup();
    const first = render(<DashboardScreen api={fakeApi()} />);
    await show(user, "Впереди");
    expect(
      await screen.findByRole("heading", { name: "Подписки" }),
    ).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Можно сегодня" })).toBeNull();
    expect(localStorage.getItem("dashboard-section")).toBe("ahead");
    first.unmount();
    render(<DashboardScreen api={fakeApi()} />);
    const ahead = await screen.findByRole("radio", { name: "Впереди" });
    expect(ahead.getAttribute("aria-checked")).toBe("true");
  });

  it("has three sections: «Накопления» moved to «План»", async () => {
    render(<DashboardScreen api={fakeApi()} />);
    const group = await screen.findByRole("radiogroup", {
      name: "Раздел сводки",
    });
    expect(
      within(group)
        .getAllByRole("radio")
        .map((radio) => radio.textContent),
    ).toEqual(["Сегодня", "Траты", "Впереди"]);
  });

  it("opens «Сегодня» when the phone remembers «Накопления»", async () => {
    localStorage.setItem("dashboard-section", "savings");
    render(<DashboardScreen api={fakeApi()} />);
    expect(
      await screen.findByRole("region", { name: "Можно сегодня" }),
    ).toBeTruthy();
    expect(
      screen
        .getByRole("radio", { name: "Сегодня" })
        .getAttribute("aria-checked"),
    ).toBe("true");
  });

  it("reads only the summary anew on «Обновить»", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<DashboardScreen api={api} />);
    await screen.findByRole("region", { name: "Можно сегодня" });
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(api.dashboard).toHaveBeenLastCalledWith(true);
    expect(api.savings).not.toHaveBeenCalled();
  });

  it("keeps the chosen section when the numbers are read anew", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<DashboardScreen api={api} />);
    await show(user, "Траты");
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(
      await screen.findByRole("button", { name: "Обновить" }),
    ).toBeTruthy();
    expect(api.dashboard).toHaveBeenLastCalledWith(true);
    expect(
      screen.getByRole("region", { name: "План-факт по категориям" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("radio", { name: "Траты" }).getAttribute("aria-checked"),
    ).toBe("true");
  });

  it("«Впереди» says when nothing is ahead", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      upcoming: [],
      subscriptions: [],
    });
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    await show(user, "Впереди");
    expect(
      screen.getByText("До конца месяца платежей и подписок нет"),
    ).toBeTruthy();
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
    expect(
      await screen.findByRole("region", { name: "Можно сегодня" }),
    ).toBeTruthy();
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
    // в таком ответе нет данных ни для одной вкладки
    expect(
      screen.queryByRole("radiogroup", { name: "Раздел сводки" }),
    ).toBeNull();
  });

  it("says there is no limit today when the month is behind the plan", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      limit: -50_000,
      left_today: -120_000,
      days_left: 22,
    });
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("⚠️ Лимита нет")).toBeTruthy();
    expect(screen.getByText("не хватает 1 100 000")).toBeTruthy();
    expect(screen.getByText("до плана на конец месяца")).toBeTruthy();
    expect(screen.getByText("потрачено сегодня 120 000")).toBeTruthy();
    await show(user, "Траты");
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
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    await show(user, "Впереди");
    expect(screen.getByText("⏳ ЗП")).toBeTruthy();
  });

  it("shows this launch's summary at once without asking again", () => {
    const store = new Store();
    store.put("dashboard", dashboardFixture);
    const api = fakeApi();
    render(
      <StoreProvider value={store}>
        <DashboardScreen api={api} />
      </StoreProvider>,
    );
    expect(screen.getByRole("region", { name: "Можно сегодня" })).toBeTruthy();
    expect(api.dashboard).not.toHaveBeenCalled();
  });

  it("reads the table anew on «Обновить» and keeps the numbers meanwhile", async () => {
    const store = new Store();
    store.put("dashboard", dashboardFixture);
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockReturnValue(new Promise(() => undefined));
    const user = userEvent.setup();
    render(
      <StoreProvider value={store}>
        <DashboardScreen api={fakeApi({ dashboard })} />
      </StoreProvider>,
    );
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(dashboard).toHaveBeenCalledWith(true);
    expect(screen.getByRole("button", { name: "Обновляю…" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "Можно сегодня" })).toBeTruthy();
  });
});
