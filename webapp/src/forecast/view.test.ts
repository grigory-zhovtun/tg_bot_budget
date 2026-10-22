import { afterEach, describe, expect, it } from "vitest";
import type { Forecast } from "../api";
import { forecastFixture } from "../mocks/fixtures";
import {
  viewPast,
  DEFAULT_VIEW,
  VIEW_KEY,
  amount,
  chartNote,
  currencyRows,
  dayEvents,
  dayTitle,
  loadView,
  marks,
  minusRange,
  articleText,
  dayArticles,
  pillUnit,
  pillValue,
  storeView,
  viewDays,
  warnings,
  type View,
} from "./view";

/** Пробелы между разрядами — неразрывные, как в money(). */
const nb = (text: string) => text.replace(/(\d) (?=\d)/g, "$1 ");
const UZS: View = { scope: "UZS", unit: "uzs" };
const DOLLARS: View = { scope: "total", unit: "usd" };

function withUzs(change: Partial<Forecast["currencies"][number]>): Forecast {
  return {
    ...forecastFixture,
    currencies: forecastFixture.currencies.map((line) =>
      line.code === "UZS" ? { ...line, ...change } : line,
    ),
  };
}

describe("forecast view", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("names the days", () => {
    expect(dayTitle("2026-10-17")).toBe("Суббота, 17 октября");
  });

  it("writes sums in millions on the pills and other money whole", () => {
    expect(pillValue(8_840_000, "UZS")).toBe("8,8");
    expect(pillValue(-360_000, "UZS")).toBe("−0,4");
    expect(pillValue(-20_000, "UZS")).toBe("0,0");
    expect(pillValue(1_797.4, "USD")).toBe(nb("1 797"));
    expect(pillUnit("UZS")).toBe("млн сум");
    expect(pillUnit("RUB")).toBe("₽");
    expect(amount(-500, "USD")).toBe("−$500");
    expect(amount(20_000, "RUB")).toBe(nb("20 000 ₽"));
    expect(amount(300, "EUR")).toBe("300 EUR");
  });

  it("takes the lines of the chosen view", () => {
    expect(viewDays(forecastFixture, UZS)[2]).toEqual({
      day: 17,
      plan: 7_580_000,
      pace: 6_440_000,
    });
    expect(viewDays(forecastFixture, DEFAULT_VIEW)[0]?.plan).toBe(33_359_000);
    expect(viewDays(forecastFixture, DOLLARS)[0]?.plan).toBeCloseTo(
      33_359_000 / 12_000,
    );
    expect(chartNote(DEFAULT_VIEW)).toBe("в сумах, по курсу таблицы");
    expect(chartNote(UZS)).toBe("в сумах");
  });

  it("collects the events of a day and the marks of the chart", () => {
    expect(dayEvents(forecastFixture, DEFAULT_VIEW, 18)).toEqual([
      {
        day: 18,
        name: "Квартплата",
        amount: -500,
        late: false,
        currency: "USD",
      },
    ]);
    expect(dayEvents(forecastFixture, UZS, 18)).toEqual([]);
    // ▼ 24-го — разовая запись «Продукты на неделю»; кофе каждый день не отмечаем
    expect(marks(forecastFixture, UZS)).toEqual([
      { day: 15, up: true },
      { day: 17, up: false },
      { day: 24, up: false },
      { day: 30, up: true },
    ]);
  });

  it("warns when a line of the view goes below zero", () => {
    expect(warnings(forecastFixture, DEFAULT_VIEW)).toEqual([]);
    expect(warnings(forecastFixture, UZS)).toEqual([
      "⚠️ По темпу сумы уйдут в минус 25.10, вернутся 30.10",
    ]);
    const gone = withUzs({
      below_zero: { plan: { start: 15, until: null }, pace: null },
    });
    expect(warnings(gone, UZS)).toEqual([
      "⚠️ По плану сумы в минусе уже сегодня",
    ]);
  });

  it("lists the articles of a day, each in its currency, the biggest first", () => {
    const total = dayArticles(forecastFixture, DEFAULT_VIEW, 14);
    expect(
      total.spent.map((item) => [item.subcategory, item.currency]),
    ).toEqual([
      ["продукты", "UZS"],
      ["кафе", "UZS"],
      ["доставка", "USD"], // $5 — 60 000 сум
      ["подписка", "RUB"], // 300 ₽ — 45 000 сум
    ]);
    expect(total.earned).toEqual([]);
    expect(
      dayArticles(forecastFixture, UZS, 14).spent.map(
        (item) => item.subcategory,
      ),
    ).toEqual(["продукты", "кафе"]);
    const [advance] = dayArticles(forecastFixture, UZS, 5).earned;
    expect(advance?.subcategory).toBe("аванс");
    // сегодня — то, что уже было
    expect(
      dayArticles(forecastFixture, UZS, 15).spent.map((item) => item.amount),
    ).toEqual([120_000]);
  });

  it("keeps to the currencies of the total in «Всего»", () => {
    const noRubles: Forecast = {
      ...forecastFixture,
      currencies: forecastFixture.currencies.map((line) =>
        line.code === "RUB" ? { ...line, in_total: false } : line,
      ),
    };
    // рубли без курса не входят ни в итог дня, ни в его статьи
    expect(
      dayArticles(noRubles, DEFAULT_VIEW, 14).spent.map(
        (item) => item.currency,
      ),
    ).toEqual(["UZS", "UZS", "USD"]);
  });

  it("writes a sum of an article in its currency, a refund as a refund", () => {
    const [groceries, refund] = dayArticles(forecastFixture, UZS, 9).spent;
    if (!groceries || !refund) throw new Error("fixture");
    expect(articleText(groceries)).toBe(nb("500 000 сум"));
    expect(articleText(refund)).toBe(nb("+50 000 сум возврат"));
    const [delivery] = dayArticles(
      forecastFixture,
      {
        scope: "USD",
        unit: "uzs",
      },
      14,
    ).spent;
    if (!delivery) throw new Error("fixture");
    expect(articleText(delivery)).toBe("$5");
  });

  it("shows every currency now and at the month end", () => {
    expect(currencyRows(forecastFixture)).toEqual([
      {
        code: "UZS",
        now: nb("6 810 000 сум"),
        end: nb("31 000 000 сум"),
        pace: "минус 25–29.10",
        minus: true,
        counted: true,
      },
      {
        code: "USD",
        now: nb("$1 800"),
        end: nb("$1 249"),
        pace: nb("по темпу $1 215"),
        minus: false,
        counted: true,
      },
      {
        code: "RUB",
        now: nb("20 000 ₽"),
        end: nb("14 900 ₽"),
        pace: nb("по темпу 20 000 ₽"),
        minus: false,
        counted: true,
      },
    ]);
    expect(minusRange({ start: 25, until: 26 }, "2026-10-15")).toBe(
      "минус 25.10",
    );
    expect(minusRange({ start: 25, until: null }, "2026-10-15")).toBe(
      "минус с 25.10",
    );
  });

  it("remembers the view and falls back when it no longer fits", () => {
    expect(loadView(forecastFixture)).toEqual(DEFAULT_VIEW);
    storeView(UZS);
    expect(JSON.parse(localStorage.getItem(VIEW_KEY) ?? "null")).toEqual(UZS);
    expect(loadView(forecastFixture)).toEqual(UZS);
    storeView(DOLLARS);
    expect(loadView(forecastFixture)).toEqual(DOLLARS);
    const noDollar: Forecast = {
      ...forecastFixture,
      total: forecastFixture.total && { ...forecastFixture.total, usd: [] },
    };
    expect(loadView(noDollar)).toEqual(DEFAULT_VIEW);
    localStorage.setItem(
      VIEW_KEY,
      JSON.stringify({ scope: "EUR", unit: "uzs" }),
    );
    expect(loadView(forecastFixture)).toEqual(DEFAULT_VIEW);
    localStorage.setItem(VIEW_KEY, "{");
    expect(loadView(forecastFixture)).toEqual(DEFAULT_VIEW);
  });

  it("gives the days by fact in the numbers of the view", () => {
    // сегодня — то, что уже было: вечер — деньги сейчас
    expect(viewPast(forecastFixture, UZS)[14]).toMatchObject({
      day: 15,
      evening: 6_810_000,
      change: -120_000,
      spent: 120_000,
      earned: 0,
    });
    expect(viewPast(forecastFixture, UZS)[13]).toMatchObject({
      day: 14,
      evening: 6_930_000,
      change: -450_000,
      spent: 450_000,
      earned: 0,
    });
    // «Всего»: сумы + $1 800 по 12 000 + 20 000 ₽ по 150
    expect(viewPast(forecastFixture, DEFAULT_VIEW)[13]).toEqual({
      day: 14,
      evening: 31_530_000,
      change: -555_000,
      spent: 555_000,
      earned: 0,
      articles: [],
    });
    // 5-го и траты, и аванс — врозь
    expect(viewPast(forecastFixture, DOLLARS)[4]?.spent).toBeCloseTo(
      555_000 / 12_000,
    );
    expect(viewPast(forecastFixture, DOLLARS)[4]?.earned).toBeCloseTo(
      4_000_000 / 12_000,
    );
    expect(viewPast({ ...forecastFixture, total: null }, DEFAULT_VIEW)).toEqual(
      [],
    );
  });
});
