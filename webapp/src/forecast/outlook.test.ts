import { describe, expect, it } from "vitest";
import type { Forecast } from "../api";
import { forecastFixture } from "../mocks/fixtures";
import {
  outlook,
  paceVerdict,
  perDayText,
  shortageText,
  spendPace,
  sumText,
} from "./outlook";
import { DEFAULT_VIEW, type View } from "./view";

/** Пробелы между разрядами — неразрывные, как в money(). */
const nb = (text: string) => text.replace(/(\d) (?=\d)/g, "$1 ");
const UZS: View = { scope: "UZS", unit: "uzs" };
const USD: View = { scope: "USD", unit: "uzs" };
const RUB: View = { scope: "RUB", unit: "uzs" };
const DOLLARS: View = { scope: "total", unit: "usd" };

function withUzs(change: Partial<Forecast["currencies"][number]>): Forecast {
  return {
    ...forecastFixture,
    currencies: forecastFixture.currencies.map((line) =>
      line.code === "UZS" ? { ...line, ...change } : line,
    ),
  };
}

describe("month outlook", () => {
  it("names the money now, the lowest evening and the month end", () => {
    expect(outlook(forecastFixture, UZS)).toEqual({
      today: 15,
      now: 6_810_000,
      low: { day: 29, plan: 1_940_000, pace: -3_760_000 },
      end: { day: 31, plan: 31_000_000, pace: 24_540_000 },
      short: { line: "pace", start: 25 },
    });
  });

  it("adds the currencies of the total up by the table's rates", () => {
    const total = outlook(forecastFixture, DEFAULT_VIEW);
    // 6 810 000 + 1 800 × 12 000 + 20 000 × 150
    expect(total?.now).toBe(31_410_000);
    expect(total?.short).toBeNull();
    expect(outlook(forecastFixture, DOLLARS)?.now).toBeCloseTo(2_617.5);
  });

  it("leaves a currency without a rate out of the total", () => {
    const unrated: Forecast = {
      ...forecastFixture,
      currencies: forecastFixture.currencies.map((line) =>
        line.code === "RUB" ? { ...line, in_total: false } : line,
      ),
    };
    expect(outlook(unrated, DEFAULT_VIEW)?.now).toBe(28_410_000);
  });

  it("has no lowest point when the money only grows or only falls", () => {
    // рубли только убывают: меньше всего — в последний день, он и так назван
    expect(outlook(forecastFixture, RUB)?.low).toBeNull();
    const growing = withUzs({
      days: [
        { day: 15, plan: 100, pace: 100 },
        { day: 16, plan: 200, pace: 150 },
        { day: 17, plan: 300, pace: 250 },
      ],
    });
    expect(outlook(growing, UZS)?.low).toBeNull();
  });

  it("says the plan's shortage before the pace's", () => {
    const both = withUzs({
      below_zero: {
        plan: { start: 27, until: 30 },
        pace: { start: 25, until: 30 },
      },
    });
    expect(outlook(both, UZS)?.short).toEqual({ line: "plan", start: 27 });
  });

  it("has nothing to show without days ahead", () => {
    expect(outlook(withUzs({ days: [] }), UZS)).toBeNull();
  });

  it("writes whether the money lasts", () => {
    const today = "2026-10-15";
    expect(shortageText(null, today)).toBe("✓ Денег хватает на весь месяц");
    expect(shortageText({ line: "plan", start: 27 }, today)).toBe(
      "Денег не хватит с 27 октября",
    );
    expect(shortageText({ line: "pace", start: 25 }, today)).toBe(
      "При таком темпе денег не хватит с 25 октября",
    );
    expect(shortageText({ line: "plan", start: 15 }, today)).toBe(
      "Денег не хватает уже сегодня",
    );
    expect(shortageText({ line: "pace", start: 15 }, today)).toBe(
      "При таком темпе денег не хватит уже сегодня",
    );
  });

  it("writes big sums in millions and the rest whole", () => {
    expect(sumText(31_000_000, "UZS")).toBe("31,0 млн");
    expect(sumText(-3_760_000, "UZS")).toBe("−3,8 млн");
    expect(sumText(360_000, "UZS")).toBe(nb("360 000 сум"));
    expect(sumText(1_215.4, "USD")).toBe(nb("$1 215"));
    expect(sumText(20_000, "RUB")).toBe(nb("20 000 ₽"));
  });
});

describe("spending pace", () => {
  it("takes the day's spending of a currency: by plan and as it goes", () => {
    expect(spendPace(forecastFixture, UZS)).toEqual({
      plan: 470_000,
      pace: 850_000,
    });
    expect(spendPace(forecastFixture, USD)).toEqual({ plan: 3, pace: 5 });
  });

  it("adds the total up by the table's rates", () => {
    // 470 000 + 3 × 12 000 + 300 × 150 и 850 000 + 5 × 12 000 + 0
    expect(spendPace(forecastFixture, DEFAULT_VIEW)).toEqual({
      plan: 551_000,
      pace: 910_000,
    });
    const dollars = spendPace(forecastFixture, DOLLARS);
    expect(dollars?.plan).toBeCloseTo(551_000 / 12_000);
    expect(dollars?.pace).toBeCloseTo(910_000 / 12_000);
  });

  it("has nothing to compare when nothing is spent either way", () => {
    expect(spendPace(withUzs({ per_day_plan: 0, per_day_pace: 0 }), UZS)).toBe(
      null,
    );
    expect(spendPace(forecastFixture, { scope: "EUR", unit: "uzs" })).toBe(
      null,
    );
  });

  it("writes a day's spending short", () => {
    expect(perDayText(1_244_000, "UZS")).toBe("1,24 млн");
    expect(perDayText(470_000, "UZS")).toBe("470 тыс.");
    expect(perDayText(999_700, "UZS")).toBe("1,00 млн");
    expect(perDayText(400, "UZS")).toBe("400 сум");
    expect(perDayText(5, "USD")).toBe("$5");
    expect(perDayText(300, "RUB")).toBe("300 ₽");
  });

  it("says how the pace goes against the plan", () => {
    expect(paceVerdict({ plan: 470_000, pace: 850_000 }, "UZS")).toEqual({
      tone: "fast",
      text: "Быстрее плана на 380 тыс. в день",
    });
    expect(paceVerdict({ plan: 300, pace: 0 }, "RUB")).toEqual({
      tone: "slow",
      text: "Медленнее плана на 300 ₽ в день",
    });
    // разница меньше тысячи сумов (или единицы валюты) — по плану
    expect(paceVerdict({ plan: 470_000, pace: 470_400 }, "UZS")).toEqual({
      tone: "even",
      text: "Траты идут по плану",
    });
    expect(paceVerdict({ plan: 3, pace: 3.3 }, "USD")).toEqual({
      tone: "even",
      text: "Траты идут по плану",
    });
  });
});
