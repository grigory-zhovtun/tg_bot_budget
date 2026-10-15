import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type CalendarRecord, type Forecast } from "../api";
import { forecastFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import ForecastScreen from "./ForecastScreen";
import { VIEW_KEY } from "./view";

function pill(day: number): HTMLElement {
  return screen.getByRole("button", { name: new RegExp(`^${day} октября`) });
}

function show(api: Api = fakeApi()) {
  return render(<ForecastScreen api={api} />);
}

describe("ForecastScreen", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("shows the month from this week with the evening and the records", async () => {
    show();
    expect(await screen.findByText("Октябрь")).toBeTruthy();
    const today = pill(15);
    expect(today.getAttribute("aria-pressed")).toBe("true");
    expect(today.getAttribute("aria-current")).toBe("date");
    expect(within(today).getByText("33,4")).toBeTruthy();
    expect(within(today).getByText("−30к")).toBeTruthy();
    // 16-е — кофе и кафе; 18-е во «Всего» — ещё и $20 доставки по 12 000
    expect(within(pill(16)).getByText("−180к")).toBeTruthy();
    expect(within(pill(18)).getByText("−270к")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /^14 октября/ })).toBeNull();
    expect(screen.getByText("14")).toBeTruthy(); // прошедший день — серым
    expect(screen.getAllByRole("button", { name: /октября:/ })).toHaveLength(
      17,
    );
    expect(
      screen.getByText(
        "В клетке — сколько будет вечером, млн сум (по плану), и записи дня",
      ),
    ).toBeTruthy();
  });

  it("shows the chosen day with its events and ordinary spending", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(17));
    expect(screen.getByText("Суббота, 17 октября")).toBeTruthy();
    expect(screen.getByText("Детский центр")).toBeTruthy();
    expect(screen.getByText("−320 000 сум")).toBeTruthy();
    expect(
      screen.getByText("обычные траты ≈ 470 000 сум + $3 + 300 ₽ (по плану)"),
    ).toBeTruthy();
  });

  it("lists the records of the chosen day", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(16));
    const card = screen.getByText("Пятница, 16 октября").closest("section");
    if (!card) throw new Error("no day card");
    expect(within(card).getByText("Записи на день")).toBeTruthy();
    expect(within(card).getByText("ЕДА / кафе · пт")).toBeTruthy();
    expect(within(card).getByText("−150 000 сум")).toBeTruthy();
    expect(within(card).getByText("ЕДА / кофе · каждый день")).toBeTruthy();
    expect(within(card).getByText("сверх плана")).toBeTruthy();
  });

  it("shows a paid record as spent", async () => {
    const paid: Forecast = {
      ...forecastFixture,
      currencies: forecastFixture.currencies.map((line) => ({
        ...line,
        records: line.records.map((record) =>
          record.day === 15 ? { ...record, open: 0, done: true } : record,
        ),
      })),
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(paid);
    show(fakeApi({ forecast }));
    expect(await screen.findByText("✓ потрачено 30 000 сум")).toBeTruthy();
    expect(within(pill(15)).queryByText("−30к")).toBeNull();
  });

  it("warns when the pace takes the sums below zero", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    expect(screen.queryByText(/уйдут в минус/)).toBeNull();
    await user.click(screen.getByRole("radio", { name: "UZS" }));
    expect(
      screen.getByText("⚠️ По темпу сумы уйдут в минус 25.10, вернутся 30.10"),
    ).toBeTruthy();
    expect(within(pill(17)).getByText("7,6")).toBeTruthy();
  });

  it("shows the total in dollars", async () => {
    const user = userEvent.setup();
    show();
    await user.click(await screen.findByRole("radio", { name: "$" }));
    expect(
      screen.getByText(
        "В клетке — сколько будет вечером, $ (по плану), и записи дня",
      ),
    ).toBeTruthy();
    expect(within(pill(15)).getByText("2 780")).toBeTruthy();
  });

  it("selects any day of the month", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(29));
    expect(pill(29).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByText("Четверг, 29 октября")).toBeTruthy();
  });

  it("selects the day under a tap on the chart", async () => {
    const { container } = show();
    await screen.findByText("Октябрь");
    const zone = container.querySelector('[data-day="25"]');
    if (!zone) throw new Error("no tap zone");
    fireEvent.click(zone);
    expect(screen.getByText("Воскресенье, 25 октября")).toBeTruthy();
    expect(pill(25).getAttribute("aria-pressed")).toBe("true");
  });

  it("remembers the chosen view on the phone", async () => {
    const user = userEvent.setup();
    const { unmount } = show();
    await user.click(await screen.findByRole("radio", { name: "USD" }));
    expect(JSON.parse(localStorage.getItem(VIEW_KEY) ?? "null")).toEqual({
      scope: "USD",
      unit: "uzs",
    });
    unmount();
    show();
    const usd = await screen.findByRole("radio", { name: "USD" });
    expect(usd.getAttribute("aria-checked")).toBe("true");
  });

  it("lists every currency now and at the month end", async () => {
    show();
    expect(await screen.findByText("По валютам: сейчас → 31.10")).toBeTruthy();
    expect(screen.getByText("минус 25–29.10")).toBeTruthy();
    expect(screen.getByText("по темпу $1 215")).toBeTruthy();
  });

  it("shows a single day on the last day of the month", async () => {
    const lastDay: Forecast = {
      ...forecastFixture,
      today: "2026-10-31",
      currencies: forecastFixture.currencies.map((line) => ({
        ...line,
        events: [],
        days: line.days.filter((point) => point.day === 31),
        below_zero: { plan: null, pace: null },
        records: line.records.filter((record) => record.day === 31),
      })),
      total: forecastFixture.total && {
        ...forecastFixture.total,
        uzs: forecastFixture.total.uzs.filter((point) => point.day === 31),
        usd: forecastFixture.total.usd.filter((point) => point.day === 31),
      },
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(lastDay);
    show(fakeApi({ forecast }));
    expect(await screen.findByText("Суббота, 31 октября")).toBeTruthy();
    expect(screen.getAllByRole("button", { name: /октября:/ })).toHaveLength(1);
    expect(screen.getByText("26")).toBeTruthy(); // неделя — с понедельника
  });

  it("marks a row that is late with ⏳", async () => {
    const late: Forecast = {
      ...forecastFixture,
      currencies: forecastFixture.currencies.map((line) =>
        line.code === "UZS"
          ? {
              ...line,
              events: [
                { day: 15, name: "Кружок", amount: -100_000, late: true },
                ...line.events,
              ],
            }
          : line,
      ),
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(late);
    show(fakeApi({ forecast }));
    expect(await screen.findByText("Кружок")).toBeTruthy();
    expect(screen.getByText("⏳")).toBeTruthy();
  });

  it("says when the month tab is missing", async () => {
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue({
      status: "no_month_tab",
      month: "Nov 26",
      today: "2026-11-01",
      end: null,
      currencies: [],
      total: null,
      rates: {},
      calendar: [],
      articles: [],
      hints: {},
    });
    show(fakeApi({ forecast }));
    expect(
      await screen.findByText(
        "Нет вкладки месяца — прогноз появится, когда она будет",
      ),
    ).toBeTruthy();
  });

  it("offers a retry when the sheet does not answer", async () => {
    const forecast = vi
      .fn<Api["forecast"]>()
      .mockRejectedValueOnce(
        new ApiError(
          503,
          "sheets_unavailable",
          "Таблица не ответила — попробуйте ещё раз",
        ),
      )
      .mockResolvedValueOnce(forecastFixture);
    const user = userEvent.setup();
    show(fakeApi({ forecast }));
    expect(
      await screen.findByText("Таблица не ответила — попробуйте ещё раз"),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("Октябрь")).toBeTruthy();
  });

  it("lists the repeats and the one-offs ahead", async () => {
    show();
    await screen.findByText("Октябрь");
    expect(screen.getByText("Повторы")).toBeTruthy();
    expect(
      screen.getByText("ЕДА / кофе · каждый день · до 31.10 — 510 000 сум"),
    ).toBeTruthy();
    expect(
      screen.getByText(
        "ДОМ / продукты · 18-го каждого месяца · до 31.10 — $20",
      ),
    ).toBeTruthy();
    expect(screen.getByText("Разовые")).toBeTruthy();
    expect(screen.getByText("24.10")).toBeTruthy();
    expect(screen.getByText("Продукты на неделю")).toBeTruthy();
  });

  it("lists records without days left in the month", async () => {
    // рынок 5-го уже прошёл, лагерь — в ноябре: обе записи видны и открываются
    const market: CalendarRecord = {
      id: "r3n7aa",
      date: "2026-09-05",
      repeat: "monthly",
      weekdays: [],
      group: "🏚️ ДОМ",
      subcategory: "продукты",
      currency: "UZS",
      amount: 200_000,
      name: "Рынок",
      author: "Тест",
      month_total: 0,
      booked: 0,
      over: false,
    };
    const camp: CalendarRecord = {
      ...market,
      id: "c4mp00",
      date: "2026-11-03",
      repeat: "once",
      amount: 700_000,
      name: "Лагерь",
    };
    const forecast = vi
      .fn<Api["forecast"]>()
      .mockResolvedValue({ ...forecastFixture, calendar: [market, camp] });
    show(fakeApi({ forecast }));
    expect(
      await screen.findByText("ДОМ / продукты · 5-го каждого месяца"),
    ).toBeTruthy();
    expect(screen.getByText("03.11")).toBeTruthy();
    expect(screen.getByText("700 000 сум")).toBeTruthy(); // не «✓»: лагерь впереди
  });
});
