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

/** Строки карточки по порядку, пробелы внутри чисел — обычные. */
function lines(card: HTMLElement): (string | undefined)[] {
  return Array.from(card.querySelectorAll("p")).map((line) =>
    line.textContent?.replace(/\s/g, " "),
  );
}

/** Строки таблицы «Откуда лимит». */
function sources(card: HTMLElement): (string | undefined)[] {
  return within(within(card).getByRole("table", { name: "Откуда лимит" }))
    .getAllByRole("row")
    .map((row) => row.textContent?.replace(/\s/g, " "));
}

const SALARY_DAY = {
  ...dashboardFixture,
  left_today: -20_000, // утренний лимит устарел: «сверх лимита дня» не пишем
  salary_came: 10_000_000,
  tomorrow: {
    limit: 612_903,
    days: 31,
    payday: "2026-11-10",
    until: "до зарплаты 10 ноября",
    sums: 16_700_000,
    bills: 300_000,
    advances: 2_000_000,
    advance_on: "2026-10-25",
  },
};

afterEach(() => {
  localStorage.clear();
});

describe("DashboardScreen", () => {
  it("adds the advance to the money until the salary", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      until: "до зарплаты 30-го",
      payday_on: "30-го",
      payday: "2026-10-30",
      advances: 3_000_000,
      advance_on: "2026-10-15",
      days_left: 21,
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    const source = await screen.findByRole("table", { name: "Откуда лимит" });
    expect(
      within(source)
        .getAllByRole("row")
        .map((row) => row.textContent?.replace(/\s/g, " ")),
    ).toEqual([
      "сумы утром2 800 000",
      "аванс 15.10+3 000 000",
      "платежи до 30.10−300 000",
      "на 21 день5 500 000",
    ]);
  });

  it("counts to the advance when the money before it is short", async () => {
    // страховка: срок — до аванса, строки аванса в таблице нет
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      sums: 5_000_000,
      bills: 4_600_000,
      days_left: 6,
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    const source = await screen.findByRole("table", { name: "Откуда лимит" });
    expect(
      within(source)
        .getAllByRole("row")
        .map((row) => row.textContent?.replace(/\s/g, " ")),
    ).toEqual([
      "сумы утром5 000 000",
      "платежи до 15.10−4 600 000",
      "на 6 дней400 000",
    ]);
  });

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
      within(today).getByText("лимит 500 000 в день до аванса 15\u2011го"),
    ).toBeTruthy();
    expect(within(today).getByText("потрачено сегодня 120 000")).toBeTruthy();
    // откуда лимит — таблицей: сумы утром, платежи до срока и деньги на его дни
    const source = within(today).getByRole("table", { name: "Откуда лимит" });
    expect(
      within(source)
        .getAllByRole("row")
        .map((row) => row.textContent?.replace(/\s/g, " ")),
    ).toEqual([
      "сумы утром2 800 000",
      "платежи до 15.10−300 000",
      "на 5 дней2 500 000",
    ]);
    // план — одной строкой, той же, что у графика «Траты»
    const plan = within(today).getByText("✓ Запас 1 100 000 за 9 дней");
    expect(plan.className).toContain("text-ok");
    // «На картах … утром, по плану» больше нет
    expect(screen.queryByRole("region", { name: "На картах" })).toBeNull();
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
    expect(
      within(sums).getByText("💳 Сумы сейчас 2 000 000").className,
    ).toContain("text-fact");
    // цель — до зарплаты, как у лимита
    expect(lines(sums)).toEqual([
      "💳 Сумы сейчас 2 000 000",
      "⚠️ хватит до 18 октября",
      "до зарплаты 30‑го обменять ≈ 460 $",
      "не хватит 5 500 000",
      "в день ≈ 500 000",
      "в среднем за 14 дней",
    ]);
  });

  it("says the sums last until the salary and what is left", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      runway: {
        balance: 9_000_000,
        pace: 100_000,
        last_day: null,
        short: 0,
        until: null,
        dollars: null,
        left: 6_000_000,
        payday: "2026-10-30",
        target: "до зарплаты 30-го",
      },
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    const sums = await screen.findByRole("region", { name: "Сумы" });
    expect(lines(sums)).toEqual([
      "💳 Сумы сейчас 9 000 000",
      "✓ до зарплаты 30‑го хватит",
      "останется ≈ 6 000 000",
      "если тратить ≈ 100 000 в день,",
      "как в среднем за 14 дней",
    ]);
    expect(
      within(sums).getByText("✓ до зарплаты 30‑го хватит").className,
    ).toContain("text-ok");
  });

  it("on the salary day counts the limit from tomorrow", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue(SALARY_DAY);
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    const today = await screen.findByRole("region", { name: "Можно сегодня" });
    expect(lines(today)).toEqual([
      "✓ Пришла ЗП 10 000 000",
      "612 903 сум",
      "можно в день с завтра",
      "до зарплаты 10 ноября, 31 день",
      "потрачено сегодня 120 000",
      "✓ Запас 1 100 000 за 9 дней",
    ]);
    expect(within(today).getByText("612 903 сум").className).toContain(
      "text-big",
    );
    // откуда лимит с завтра — сумы сейчас, аванс и платежи срока с завтра
    expect(sources(today)).toEqual([
      "сумы сейчас16 700 000",
      "аванс 25.10+2 000 000",
      "платежи до 10.11−300 000",
      "на 31 день18 400 000",
    ]);
  });

  it("on the salary day the daily chart says tomorrow's limit", async () => {
    // «можно X в день» у графика — то же «с завтра», что в «Сегодня»
    const user = userEvent.setup();
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue(SALARY_DAY);
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    await show(user, "Траты");
    expect(screen.getByText("можно 612 903 в день")).toBeTruthy();
  });

  it("says the advance came when it stands in for the salary", async () => {
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockResolvedValue({ ...SALARY_DAY, came: "Пришёл аванс" });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    const today = await screen.findByRole("region", { name: "Можно сегодня" });
    expect(lines(today)[0]).toBe("✓ Пришёл аванс 10 000 000");
  });

  it("on the salary day says when there is no limit from tomorrow either", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...SALARY_DAY,
      tomorrow: {
        ...SALARY_DAY.tomorrow,
        limit: -50_000,
        days: 16,
        payday: "2026-10-25",
        until: "до аванса 25-го",
        sums: -500_000,
        bills: 300_000,
        advances: 0,
        advance_on: null,
      },
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    const today = await screen.findByRole("region", { name: "Можно сегодня" });
    expect(lines(today)).toEqual([
      "✓ Пришла ЗП 10 000 000",
      "⚠️ Лимита нет",
      "до аванса 25‑го не хватает 800 000",
      "потрачено сегодня 120 000",
      "✓ Запас 1 100 000 за 9 дней",
    ]);
    expect(sources(today)).toEqual([
      "сумы сейчас−500 000",
      "платежи до 25.10−300 000",
      "на 16 дней−800 000",
    ]);
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
      within(today).getByText("лимит 500 000 в день до аванса 15\u2011го"),
    ).toBeTruthy();
    expect(within(today).getByText("потрачено сегодня 520 000")).toBeTruthy();
  });

  it("shows the frozen dollars in a card of their own", async () => {
    render(<DashboardScreen api={fakeApi()} />);
    const frozen = await screen.findByRole("region", { name: "Заморожено" });
    expect(within(frozen).getByText("🧊 Заморожено 1 000 $")).toBeTruthy();
    expect(within(frozen).getByText("с 1-го числа −50 $")).toBeTruthy();
  });

  it("says when the frozen sum stood still", async () => {
    const dashboard = vi.fn<Api["dashboard"]>().mockResolvedValue({
      ...dashboardFixture,
      frozen: { amount: 1000, currency: "USD", uzs: 12_000_000, change: 0 },
    });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("с 1-го числа без изменений")).toBeTruthy();
    expect(screen.queryByText(/плана на \d/)).toBeNull();
  });

  it("says when the sums last until the end of the month", async () => {
    // ответ сервера до срока «до зарплаты» — как раньше
    const old = {
      balance: 9_000_000,
      pace: 100_000,
      last_day: null,
      short: 0,
      until: null,
      dollars: null,
    };
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockResolvedValue({ ...dashboardFixture, runway: old });
    const first = render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("✓ до конца месяца хватит")).toBeTruthy();
    expect(screen.queryByText(/останется/)).toBeNull();
    first.unmount();
    // строк зарплаты в списке нет — срок до конца месяца
    const monthEnd = {
      ...old,
      left: 7_000_000,
      payday: null,
      target: "до конца месяца",
    };
    dashboard.mockResolvedValue({ ...dashboardFixture, runway: monthEnd });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(await screen.findByText("✓ до конца месяца хватит")).toBeTruthy();
    expect(screen.getByText("останется ≈ 7 000 000")).toBeTruthy();
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
    const cards = await screen.findByRole("region", {
      name: "Остатки на картах",
    });
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
    expect(
      screen.getByText("до аванса 15\u2011го не хватает 1 100 000"),
    ).toBeTruthy();
    expect(screen.getByText("потрачено сегодня 120 000")).toBeTruthy();
    // откуда недостача — той же таблицей
    const today = screen.getByRole("region", { name: "Можно сегодня" });
    expect(
      within(today).getByRole("table", { name: "Откуда лимит" }),
    ).toBeTruthy();
    expect(screen.queryByText("до плана на конец месяца")).toBeNull();
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
