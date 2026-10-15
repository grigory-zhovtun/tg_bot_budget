import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type Forecast } from "../api";
import { forecastFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import ForecastScreen from "./ForecastScreen";
import { VIEW_KEY } from "./view";

function pill(day: number): HTMLElement {
  return screen.getByRole("button", { name: new RegExp(`^${day} октября`) });
}

describe("ForecastScreen", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("shows the week from today with the evening by plan", async () => {
    render(<ForecastScreen api={fakeApi()} />);
    expect(await screen.findByText("Октябрь, неделя")).toBeTruthy();
    const today = pill(15);
    expect(today.getAttribute("aria-pressed")).toBe("true");
    expect(within(today).getByText("сег")).toBeTruthy();
    expect(within(today).getByText("33,4")).toBeTruthy();
    expect(pill(21)).toBeTruthy();
    expect(screen.queryByRole("button", { name: /^22 октября/ })).toBeNull();
    expect(
      screen.getByText("Под датой — сколько будет вечером, млн сум (по плану)"),
    ).toBeTruthy();
    expect(
      screen
        .getByRole("button", { name: "Предыдущая неделя" })
        .hasAttribute("disabled"),
    ).toBe(true);
  });

  it("shows the chosen day with its events and ordinary spending", async () => {
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi()} />);
    await screen.findByText("Октябрь, неделя");
    await user.click(pill(17));
    expect(screen.getByText("Суббота, 17 октября")).toBeTruthy();
    expect(screen.getByText("Детский центр")).toBeTruthy();
    expect(screen.getByText("−320 000 сум")).toBeTruthy();
    expect(
      screen.getByText("обычные траты ≈ 470 000 сум + $3 + 300 ₽ (по плану)"),
    ).toBeTruthy();
  });

  it("warns when the pace takes the sums below zero", async () => {
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi()} />);
    await screen.findByText("Октябрь, неделя");
    expect(screen.queryByText(/уйдут в минус/)).toBeNull();
    await user.click(screen.getByRole("radio", { name: "UZS" }));
    expect(
      screen.getByText("⚠️ По темпу сумы уйдут в минус 25.10, вернутся 30.10"),
    ).toBeTruthy();
    expect(within(pill(17)).getByText("7,6")).toBeTruthy();
  });

  it("shows the total in dollars", async () => {
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi()} />);
    await user.click(await screen.findByRole("radio", { name: "$" }));
    expect(
      screen.getByText("Под датой — сколько будет вечером, $ (по плану)"),
    ).toBeTruthy();
    expect(within(pill(15)).getByText("2 780")).toBeTruthy();
  });

  it("pages the weeks inside the month", async () => {
    const user = userEvent.setup();
    render(<ForecastScreen api={fakeApi()} />);
    await screen.findByText("Октябрь, неделя");
    const next = screen.getByRole("button", { name: "Следующая неделя" });
    await user.click(next);
    expect(pill(22).getAttribute("aria-pressed")).toBe("true");
    await user.click(next);
    expect(pill(31)).toBeTruthy();
    expect(screen.getAllByRole("button", { name: /октября:/ })).toHaveLength(3);
    expect(next.hasAttribute("disabled")).toBe(true);
    expect(screen.getByText("Четверг, 29 октября")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Предыдущая неделя" }));
    expect(screen.getByText("Четверг, 22 октября")).toBeTruthy();
  });

  it("selects the day under a tap on the chart", async () => {
    const { container } = render(<ForecastScreen api={fakeApi()} />);
    await screen.findByText("Октябрь, неделя");
    const zone = container.querySelector('[data-day="25"]');
    if (!zone) throw new Error("no tap zone");
    fireEvent.click(zone);
    expect(screen.getByText("Воскресенье, 25 октября")).toBeTruthy();
    expect(pill(25).getAttribute("aria-pressed")).toBe("true");
  });

  it("remembers the chosen view on the phone", async () => {
    const user = userEvent.setup();
    const { unmount } = render(<ForecastScreen api={fakeApi()} />);
    await user.click(await screen.findByRole("radio", { name: "USD" }));
    expect(JSON.parse(localStorage.getItem(VIEW_KEY) ?? "null")).toEqual({
      scope: "USD",
      unit: "uzs",
    });
    unmount();
    render(<ForecastScreen api={fakeApi()} />);
    const usd = await screen.findByRole("radio", { name: "USD" });
    expect(usd.getAttribute("aria-checked")).toBe("true");
  });

  it("lists every currency now and at the month end", async () => {
    render(<ForecastScreen api={fakeApi()} />);
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
      })),
      total: forecastFixture.total && {
        ...forecastFixture.total,
        uzs: forecastFixture.total.uzs.filter((point) => point.day === 31),
        usd: forecastFixture.total.usd.filter((point) => point.day === 31),
      },
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(lastDay);
    render(<ForecastScreen api={fakeApi({ forecast })} />);
    expect(await screen.findByText("Суббота, 31 октября")).toBeTruthy();
    expect(screen.getAllByRole("button", { name: /октября:/ })).toHaveLength(1);
    for (const name of ["Предыдущая неделя", "Следующая неделя"]) {
      expect(
        screen.getByRole("button", { name }).hasAttribute("disabled"),
      ).toBe(true);
    }
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
    render(<ForecastScreen api={fakeApi({ forecast })} />);
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
    render(<ForecastScreen api={fakeApi({ forecast })} />);
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
    render(<ForecastScreen api={fakeApi({ forecast })} />);
    expect(
      await screen.findByText("Таблица не ответила — попробуйте ещё раз"),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("Октябрь, неделя")).toBeTruthy();
  });
});
