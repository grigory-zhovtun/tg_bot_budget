import { describe, expect, it } from "vitest";
import type { CalendarRecord, ForecastArticle } from "../api";
import { forecastFixture } from "../mocks/fixtures";
import {
  cellAmount,
  dayRecordSum,
  dayRecords,
  gridWeeks,
  groupTitle,
  hintChips,
  isoWeekday,
  laterRecords,
  overBy,
  ruleDays,
  ruleText,
} from "./records";
import { DEFAULT_VIEW, type View } from "./view";

/** Пробелы между разрядами — неразрывные, как в money(). */
const nb = (text: string) => text.replace(/(\d) (?=\d)/g, "$1\u00a0");
const UZS: View = { scope: "UZS", unit: "uzs" };
const DOLLARS: View = { scope: "total", unit: "usd" };
const TODAY = "2026-10-15"; // четверг
const END = "2026-10-31";

describe("calendar records", () => {
  it("names the rule of a record", () => {
    expect(ruleText("once", [], "2026-10-17")).toBe("разово");
    expect(ruleText("daily", [], "2026-10-01")).toBe("каждый день");
    expect(ruleText("weekdays", [3, 1], "2026-10-06")).toBe("пн ср");
    expect(ruleText("monthly", [], "2026-09-05")).toBe("5-го каждого месяца");
    expect(groupTitle("🏚️ ДОМ")).toBe("ДОМ");
  });

  it("finds the days of a rule like the server", () => {
    expect(isoWeekday("2026-10-15")).toBe(4);
    expect(isoWeekday("2026-10-18")).toBe(7);
    expect(ruleDays("once", [], "2026-10-17", TODAY, END)).toEqual([17]);
    expect(ruleDays("once", [], "2026-10-10", TODAY, END)).toEqual([]);
    expect(ruleDays("once", [], "2026-11-03", TODAY, END)).toEqual([]);
    expect(ruleDays("daily", [], "2026-10-29", TODAY, END)).toEqual([
      29, 30, 31,
    ]);
    expect(ruleDays("daily", [], "2026-10-01", "2026-10-30", END)).toEqual([
      30, 31,
    ]);
    expect(ruleDays("weekdays", [1, 3], "2026-10-06", TODAY, END)).toEqual([
      19, 21, 26, 28,
    ]);
    expect(ruleDays("monthly", [], "2026-09-20", TODAY, END)).toEqual([20]);
    expect(ruleDays("monthly", [], "2026-10-05", TODAY, END)).toEqual([]);
    expect(
      ruleDays("monthly", [], "2026-08-31", "2026-11-01", "2026-11-30"),
    ).toEqual([30]); // 31-е в ноябре — его последний день
  });

  it("finds the records no day card of the month can open", () => {
    // у всех записей фикстуры есть дни до конца месяца
    expect(laterRecords(forecastFixture)).toEqual([]);
    const [first] = forecastFixture.calendar;
    if (!first) throw new Error("fixture");
    const market: CalendarRecord = {
      ...first,
      id: "r3n7aa",
      date: "2026-09-05",
      repeat: "monthly",
      currency: "UZS",
      name: "Рынок",
    };
    const camp: CalendarRecord = {
      ...market,
      id: "c4mp00",
      date: "2026-11-03",
      repeat: "once",
      name: "Лагерь",
    };
    const euro: CalendarRecord = {
      ...market,
      id: "e1u2r0",
      date: "2026-09-10",
      currency: "EUR",
      name: "Кружок",
    };
    const data = {
      ...forecastFixture,
      calendar: [camp, ...forecastFixture.calendar, euro, market],
    };
    // 5-е прошло, лагерь в ноябре, евро без карты; по дате записи
    expect(laterRecords(data).map((record) => record.id)).toEqual([
      "r3n7aa",
      "e1u2r0",
      "c4mp00",
    ]);
  });

  it("lays the month out in weeks from the current one", () => {
    expect(gridWeeks(TODAY, END)).toEqual([
      [12, 13, 14, 15, 16, 17, 18],
      [19, 20, 21, 22, 23, 24, 25],
      [26, 27, 28, 29, 30, 31, null],
    ]);
    expect(gridWeeks("2026-11-01", "2026-11-30")[0]).toEqual([
      null,
      null,
      null,
      null,
      null,
      null,
      1,
    ]);
    expect(gridWeeks("2026-10-31", END)).toEqual([
      [26, 27, 28, 29, 30, 31, null],
    ]);
    // весь месяц — с недели 1-го числа
    expect(gridWeeks("2026-10-01", END)).toEqual([
      [null, null, null, 1, 2, 3, 4],
      [5, 6, 7, 8, 9, 10, 11],
      [12, 13, 14, 15, 16, 17, 18],
      [19, 20, 21, 22, 23, 24, 25],
      [26, 27, 28, 29, 30, 31, null],
    ]);
  });

  it("sums the open records of a day in the units of the view", () => {
    expect(dayRecords(forecastFixture, UZS, 16).map((r) => r.name)).toEqual([
      "Кофе",
      "Кафе с друзьями",
    ]);
    expect(dayRecordSum(forecastFixture, UZS, 16)).toBe(180_000);
    // 18-го: кофе 30 000 сум и доставка $20 по 12 000
    expect(dayRecordSum(forecastFixture, DEFAULT_VIEW, 18)).toBe(270_000);
    expect(dayRecordSum(forecastFixture, DOLLARS, 18)).toBeCloseTo(22.5);
    expect(cellAmount(180_000, "UZS")).toBe("180к");
    expect(cellAmount(1_230_000, "UZS")).toBe("1,2м");
    expect(cellAmount(999_700, "UZS")).toBe("1,0м");
    expect(cellAmount(1_250, "USD")).toBe(nb("1 250"));
  });

  it("tells by how much the records of an article pass its rest", () => {
    const coffee: ForecastArticle = {
      group: "🍔 ЕДА",
      subcategory: "кофе",
      currency: "UZS",
      plan: 900_000,
      rest: 480_000,
      booked: 510_000,
    };
    expect(overBy(coffee, 0, 0)).toBe(30_000);
    expect(overBy(coffee, 510_000, 450_000)).toBe(0);
    expect(overBy({ ...coffee, booked: 480_000 }, 0, 0)).toBe(0);
    expect(overBy(undefined, 0, 100_000)).toBe(100_000); // статьи нет в плане
  });

  it("offers amounts from the history of the article", () => {
    const cafe = forecastFixture.hints["кафе|UZS"];
    expect(hintChips(cafe, "once")).toEqual([
      { label: nb("обычно 160 000"), value: 160_000 },
      { label: nb("в прошлый раз 145 000"), value: 145_000 },
    ]);
    expect(hintChips(cafe, "daily")).toEqual([
      { label: nb("в среднем в день 21 000"), value: 21_000 },
    ]);
    expect(
      hintChips({ typical: 13, last: null, per_day: 0 }, "weekdays"),
    ).toEqual([{ label: "обычно 13", value: 13 }]);
    expect(hintChips({ typical: 13, last: null, per_day: 0 }, "daily")).toEqual(
      [],
    ); // ноль подставлять нечего
    expect(hintChips(undefined, "once")).toEqual([]);
  });
});
