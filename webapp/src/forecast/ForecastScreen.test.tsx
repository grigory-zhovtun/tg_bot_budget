import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  type Api,
  type CalendarRecord,
  type DayArticle,
  type Forecast,
  type ForecastPast,
} from "../api";
import {
  bootstrapFixture,
  dashboardFixture,
  forecastFixture,
} from "../mocks/fixtures";
import { Store, StoreProvider } from "../store";
import { fakeApi } from "../test/fakeApi";
import ForecastScreen from "./ForecastScreen";
import { VIEW_KEY } from "./view";

/** Карточка выбранного дня по её заголовку. */
function dayCard(title: string): HTMLElement {
  const card = screen.getByText(title).closest("section");
  if (!card) throw new Error("no day card");
  return card;
}

function pill(day: number): HTMLElement {
  return screen.getByRole("button", { name: new RegExp(`^${day} октября`) });
}

/** Статья дня для подменённого прогноза: траты в сумах, rate — к суму. */
function article(
  subcategory: string,
  amount: number,
  group = "🍔 ЕДА",
): DayArticle {
  return {
    group,
    subcategory,
    icon: "",
    amount,
    uzs: amount,
    income: false,
  };
}

/** Прогноз фикстуры с другим днём валюты (и тем же днём «Всего» в сумах). */
function withPast(
  code: string,
  day: number,
  change: Partial<ForecastPast>,
): Forecast {
  const patch = (point: ForecastPast) =>
    point.day === day
      ? { ...point, ...change, articles: change.articles ?? point.articles }
      : point;
  return {
    ...forecastFixture,
    currencies: forecastFixture.currencies.map((line) =>
      line.code === code ? { ...line, past: line.past.map(patch) } : line,
    ),
    total: forecastFixture.total && {
      ...forecastFixture.total,
      past_uzs: forecastFixture.total.past_uzs.map((point) =>
        point.day === day ? { ...patch(point), articles: [] } : point,
      ),
    },
  };
}

/** Пробелы между разрядами — неразрывные, как в money(). */
const nb = (text: string) => text.replace(/(\d) (?=\d)/g, "$1\u00a0");

function show(api: Api = fakeApi()) {
  return render(<ForecastScreen api={api} boot={bootstrapFixture} />);
}

describe("ForecastScreen", () => {
  it("titles the screen as the month's calendar, in Russian", async () => {
    show();
    expect(
      await screen.findByRole("heading", {
        level: 1,
        name: "Календарь · октябрь",
      }),
    ).toBeTruthy();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it("shows the whole month: past days by fact, the rest by plan", async () => {
    show();
    expect(await screen.findByText("Октябрь")).toBeTruthy();
    const today = pill(15);
    expect(today.getAttribute("aria-pressed")).toBe("true");
    expect(today.getAttribute("aria-current")).toBe("date");
    expect(within(today).getByText("33,4")).toBeTruthy();
    // сегодня — что уже потрачено, а записи дня — в его карточке
    expect(within(today).getByText("−120к")).toBeTruthy();
    expect(within(today).queryByText("−30к")).toBeNull();
    expect(today.getAttribute("aria-label")).toBe(
      nb("15 октября: вечером 33,4 млн сум, потрачено 120 000 сум"),
    );
    // 16-е — кофе и кафе; 18-е во «Всего» — ещё и $20 доставки по 12 000
    expect(within(pill(16)).getByText("−180к")).toBeTruthy();
    expect(within(pill(18)).getByText("−270к")).toBeTruthy();
    // записи — голубым, не красным
    expect(within(pill(16)).getByText("−180к").className).toContain(
      "text-planned",
    );
    // прошедшие дни — по факту: сколько было вечером, траты и доходы дня врозь
    expect(within(pill(14)).getByText("31,5")).toBeTruthy();
    expect(within(pill(14)).getByText("−555к").className).toContain(
      "text-spent",
    );
    expect(within(pill(5)).getByText("36,5")).toBeTruthy();
    expect(within(pill(5)).getByText("−555к").className).toContain(
      "text-spent",
    );
    expect(within(pill(5)).getByText("+4,0м").className).toContain(
      "text-earned",
    );
    // скринридеру — полные суммы, а не сокращения клетки
    expect(pill(5).getAttribute("aria-label")).toBe(
      nb(
        "5 октября: вечером было 36,5 млн сум, потрачено 555 000 сум, пришло 4 000 000 сум",
      ),
    );
    expect(screen.getAllByRole("button", { name: /октября:/ })).toHaveLength(
      31,
    );
    // без легенды и пояснений под графиком: владелец — «создают шум»
    expect(screen.queryByText(/Остаток на вечер/)).toBeNull();
    expect(screen.queryByText("потрачено за день")).toBeNull();
    expect(screen.queryByText("минус на вечер")).toBeNull();
    expect(screen.queryByText(/План — остаток плана статей/)).toBeNull();
    expect(screen.queryByText(/В день: по плану/)).toBeNull();
  });

  it("shows a past day by fact", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(5));
    expect(pill(5).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByText("Понедельник, 5 октября")).toBeTruthy();
    expect(screen.getByText("по факту")).toBeTruthy();
    expect(screen.getByText("36 465 000 сум")).toBeTruthy();
    expect(screen.getByText("−555 000 сум")).toBeTruthy();
    expect(screen.getByText("+4 000 000 сум")).toBeTruthy();
    // траты — суммами по статьям, каждая в своей валюте; доходы — отдельно
    const spent = screen.getByRole("list", { name: "Траты по статьям" });
    expect(
      within(spent)
        .getAllByRole("listitem")
        .map((item) => item.textContent),
    ).toEqual([nb("🛒продукты450 000 сум"), "🛵доставка$5", "📺подписка300 ₽"]);
    const earned = screen.getByRole("list", { name: "Доходы по статьям" });
    expect(within(earned).getByText("аванс")).toBeTruthy();
    // в прошлое не планируют
    expect(screen.queryByRole("button", { name: /Запланировать/ })).toBeNull();
  });

  it("lists what was spent today in today's card", async () => {
    show();
    await screen.findByText("Октябрь");
    expect(screen.getByText("потрачено сегодня")).toBeTruthy();
    expect(screen.getByText("−120 000 сум")).toBeTruthy();
    const spent = screen.getByRole("list", { name: "Траты по статьям" });
    expect(within(spent).getByText("продукты")).toBeTruthy();
    expect(within(spent).getByText("120 000 сум")).toBeTruthy();
  });

  it("shows a refund of a day as a refund", async () => {
    const user = userEvent.setup();
    show();
    await user.click(await screen.findByRole("radio", { name: "UZS" }));
    await user.click(pill(9));
    expect(screen.getByText("+50 000 сум возврат")).toBeTruthy();
  });

  it("colors today's spending when another day is chosen", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(16));
    expect(within(pill(15)).getByText("−120к").className).toContain(
      "text-spent",
    );
  });

  it("marks an evening below zero by the cell, not by red text", async () => {
    const user = userEvent.setup();
    const minus: Forecast = {
      ...forecastFixture,
      currencies: forecastFixture.currencies.map((line) =>
        line.code === "UZS"
          ? {
              ...line,
              days: line.days.map((point) =>
                point.day === 20 ? { ...point, plan: -400_000 } : point,
              ),
            }
          : line,
      ),
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(minus);
    show(fakeApi({ forecast }));
    await user.click(await screen.findByRole("radio", { name: "UZS" }));
    const cell = pill(20);
    expect(cell.getAttribute("data-minus")).toBe("true");
    expect(within(cell).getByText("−0,4").className).not.toContain(
      "text-danger",
    );
  });

  it("shows the chosen day with its events, without «обычные траты»", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(17));
    expect(screen.getByText("Суббота, 17 октября")).toBeTruthy();
    expect(screen.getByText("Детский центр")).toBeTruthy();
    expect(screen.getByText("−320 000 сум")).toBeTruthy();
    expect(screen.queryByText(/обычные траты/)).toBeNull();
  });

  it("shows today without «обычные траты» either", async () => {
    show();
    await screen.findByText("Октябрь");
    expect(screen.queryByText(/обычные траты/)).toBeNull();
  });

  it("lists the records of the chosen day", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(16));
    const card = screen.getByText("Пятница, 16 октября").closest("section");
    if (!card) throw new Error("no day card");
    expect(within(card).getByText("Запланировано на день")).toBeTruthy();
    expect(within(card).getByText("ЕДА / кафе · пт")).toBeTruthy();
    expect(within(card).getByText("−150 000 сум")).toBeTruthy();
    expect(within(card).getByText("ЕДА / кофе · каждый день")).toBeTruthy();
    expect(within(card).getByText("сверх плана")).toBeTruthy();
    expect(
      within(card).getByRole("button", {
        name: "＋ Запланировать на 16 октября",
      }),
    ).toBeTruthy();
  });

  it("shows a paid record as spent", async () => {
    const paid: Forecast = {
      ...forecastFixture,
      currencies: forecastFixture.currencies.map((line) => ({
        ...line,
        records: line.records.map((record) =>
          record.day === 15
            ? { ...record, open: 0, done: true, spent: 30_000 }
            : record,
        ),
      })),
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(paid);
    show(fakeApi({ forecast }));
    expect(await screen.findByText("✓ потрачено 30 000 сум")).toBeTruthy();
    // потрачено ровно по плану — «по плану» не повторяем
    expect(screen.getByText("ЕДА / кофе · каждый день")).toBeTruthy();
    expect(within(pill(15)).queryByText("−30к")).toBeNull();
  });

  it("shows what was really spent on a record and its plan", async () => {
    // запланировали 30 000 на кофе, потратили 90 000: «потрачено» — по факту
    const more: Forecast = {
      ...forecastFixture,
      currencies: forecastFixture.currencies.map((line) => ({
        ...line,
        records: line.records.map((record) =>
          record.day === 15
            ? { ...record, open: 0, done: true, spent: 90_000 }
            : record,
        ),
      })),
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue(more);
    show(fakeApi({ forecast }));
    expect(await screen.findByText("✓ потрачено 90 000 сум")).toBeTruthy();
    expect(
      screen.getByText("ЕДА / кофе · каждый день · по плану 30 000 сум"),
    ).toBeTruthy();
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
    expect(within(pill(15)).getByText("2 780")).toBeTruthy();
    // единица — в подписи клетки для скринридера
    expect(pill(15).getAttribute("aria-label")).toContain(nb("2 780 $"));
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
        past: [],
      })),
      total: forecastFixture.total && {
        ...forecastFixture.total,
        uzs: forecastFixture.total.uzs.filter((point) => point.day === 31),
        usd: forecastFixture.total.usd.filter((point) => point.day === 31),
        past_uzs: [],
        past_usd: [],
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

  it("shows record sums in the color of records", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(16));
    // запись — голубым, как в клетках календаря
    expect(
      within(dayCard("Пятница, 16 октября")).getByText("−150 000 сум")
        .className,
    ).toContain("text-planned");
  });

  it("has no list of records under the calendar", async () => {
    show();
    await screen.findByText("Октябрь");
    expect(screen.queryByRole("heading", { name: "Записи" })).toBeNull();
    expect(screen.queryByText("Повторы")).toBeNull();
    expect(screen.queryByRole("button", { name: "＋ Новая" })).toBeNull();
  });

  it("lists the records no day of the month shows, to open them", async () => {
    // рынок 5-го уже прошёл, лагерь — в ноябре: карточки дней их не покажут
    const [first] = forecastFixture.calendar;
    if (!first) throw new Error("fixture");
    const market: CalendarRecord = {
      ...first,
      id: "r3n7aa",
      date: "2026-09-05",
      repeat: "monthly",
      weekdays: [],
      group: "🏚️ ДОМ",
      subcategory: "продукты",
      currency: "UZS",
      amount: 200_000,
      name: "Рынок",
    };
    const camp: CalendarRecord = {
      ...market,
      id: "c4mp00",
      date: "2026-11-03",
      repeat: "once",
      amount: 700_000,
      name: "Лагерь",
    };
    const forecast = vi.fn<Api["forecast"]>().mockResolvedValue({
      ...forecastFixture,
      calendar: [...forecastFixture.calendar, camp, market],
    });
    const user = userEvent.setup();
    show(fakeApi({ forecast }));
    const later = await screen.findByRole("region", {
      name: "Запланировано дальше",
    });
    const rows = within(later).getAllByRole("button");
    expect(rows.map((row) => row.textContent?.split("ДОМ")[0])).toEqual([
      "Рынок",
      "Лагерь",
    ]);
    expect(
      within(later).getByText("ДОМ / продукты · 5-го каждого месяца"),
    ).toBeTruthy();
    expect(within(later).getByText("ДОМ / продукты · 3 ноября")).toBeTruthy();
    expect(within(later).getByText("700 000 сум").className).toContain(
      "text-planned",
    );
    await user.click(within(later).getByRole("button", { name: /Лагерь/ }));
    const sheet = screen.getByRole("dialog", { name: "Запись" });
    expect(within(sheet).getByLabelText("Название")).toHaveProperty(
      "value",
      "Лагерь",
    );
  });

  it("shows no «Запланировано дальше» while every record has its day", async () => {
    show();
    await screen.findByText("Октябрь");
    expect(
      screen.queryByRole("region", { name: "Запланировано дальше" }),
    ).toBeNull();
  });

  it("offers no planning on a past day", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(3));
    // в прошлое не планируют: новую запись ставят из сегодняшнего или будущего дня
    expect(
      screen.queryByRole("button", { name: /Запланировать|＋ Новая/ }),
    ).toBeNull();
  });

  it("plans a record on the chosen day and shows the fresh forecast", async () => {
    const fresh: Forecast = {
      ...forecastFixture,
      calendar: [],
      currencies: forecastFixture.currencies.map((line) => ({
        ...line,
        records: [],
      })),
    };
    const addRecord = vi.fn<Api["addRecord"]>().mockResolvedValue(fresh);
    const user = userEvent.setup();
    show(fakeApi({ addRecord }));
    await screen.findByText("Октябрь");
    await user.click(pill(16));
    await user.click(
      screen.getByRole("button", { name: "＋ Запланировать на 16 октября" }),
    );
    const sheet = screen.getByRole("dialog", { name: "Запись на 16 октября" });
    await user.click(within(sheet).getByRole("button", { name: "🍔 ЕДА" }));
    await user.click(within(sheet).getByRole("button", { name: "кафе" }));
    expect(
      within(sheet).getByText(
        "В статье осталось 1 200 000 сум из 1 700 000 сум",
      ),
    ).toBeTruthy();
    await user.click(
      within(sheet).getByRole("button", { name: nb("обычно 160 000") }),
    );
    await user.type(within(sheet).getByLabelText("Название"), "Кафе");
    await user.click(
      within(sheet).getByRole("button", { name: "Запланировать" }),
    );
    expect(addRecord).toHaveBeenCalledWith({
      entry_id: expect.any(String),
      date: "2026-10-16",
      repeat: "once",
      weekdays: [],
      group: "🍔 ЕДА",
      subcategory: "кафе",
      currency: "UZS",
      amount: "160000",
      name: "Кафе",
    });
    // свежий прогноз: в карточке дня больше нет записей
    await vi.waitFor(() => {
      expect(screen.queryByText("Запланировано на день")).toBeNull();
    });
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("opens a record from the day card", async () => {
    const user = userEvent.setup();
    show();
    await screen.findByText("Октябрь");
    await user.click(pill(24));
    await user.click(
      within(dayCard("Суббота, 24 октября")).getByRole("button", {
        name: /Продукты на неделю/,
      }),
    );
    const sheet = screen.getByRole("dialog", { name: "Запись" });
    expect(within(sheet).getByLabelText("Название")).toHaveProperty(
      "value",
      "Продукты на неделю",
    );
    await user.click(within(sheet).getByRole("button", { name: "Закрыть" }));
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("only shows the records without the catalog", async () => {
    render(<ForecastScreen api={fakeApi()} boot={null} />);
    await screen.findByText("Октябрь");
    expect(screen.queryByRole("button", { name: /Запланировать/ })).toBeNull();
    expect(screen.queryByRole("button", { name: "＋ Новая" })).toBeNull();
  });

  it("shows this launch's forecast at once without asking again", () => {
    const store = new Store();
    store.put("forecast", forecastFixture);
    const api = fakeApi();
    render(
      <StoreProvider value={store}>
        <ForecastScreen api={api} boot={bootstrapFixture} />
      </StoreProvider>,
    );
    expect(screen.getByText("Октябрь")).toBeTruthy();
    expect(api.forecast).not.toHaveBeenCalled();
  });

  it("after a record is deleted the summary is asked again", async () => {
    const store = new Store();
    store.put("dashboard", dashboardFixture);
    const user = userEvent.setup();
    const api = fakeApi();
    render(
      <StoreProvider value={store}>
        <ForecastScreen api={api} boot={bootstrapFixture} />
      </StoreProvider>,
    );
    await screen.findByText("Октябрь");
    await user.click(pill(24));
    await user.click(
      within(dayCard("Суббота, 24 октября")).getByRole("button", {
        name: /Продукты на неделю/,
      }),
    );
    const sheet = screen.getByRole("dialog", { name: "Запись" });
    await user.click(
      within(sheet).getByRole("button", { name: "Удалить запись" }),
    );
    await user.click(
      within(sheet).getByRole("button", { name: "Удалить насовсем" }),
    );
    expect(api.deleteRecord).toHaveBeenCalled();
    await vi.waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
    // лимит дня в «Сводке» учитывает записи календаря
    expect(store.get("dashboard").at).toBe(0);
  });

  it("names the group when a subcategory is in two groups that day", async () => {
    const user = userEvent.setup();
    const twice: Forecast = withPast("UZS", 14, {
      articles: [
        article("продукты", 300_000, "🍔 ЕДА"),
        article("продукты", 150_000, "🏚️ ДОМ"),
      ],
    });
    show(
      fakeApi({ forecast: vi.fn<Api["forecast"]>().mockResolvedValue(twice) }),
    );
    await user.click(await screen.findByRole("radio", { name: "UZS" }));
    await user.click(pill(14));
    expect(screen.getByText("продукты · ЕДА")).toBeTruthy();
    expect(screen.getByText("продукты · ДОМ")).toBeTruthy();
  });

  it("says when a day had only transfers between own cards", async () => {
    const user = userEvent.setup();
    const moved = withPast("UZS", 14, {
      spent: 0,
      earned: 0,
      change: -1_200_000,
      articles: [],
    });
    show(
      fakeApi({ forecast: vi.fn<Api["forecast"]>().mockResolvedValue(moved) }),
    );
    await user.click(await screen.findByRole("radio", { name: "UZS" }));
    await user.click(pill(14));
    expect(screen.getByText("переводы и выравнивание")).toBeTruthy();
    expect(screen.getByText("−1 200 000 сум")).toBeTruthy();
    expect(screen.queryByText("движений не было")).toBeNull();
  });

  it("shows a day with only a refund as money that came back", async () => {
    const user = userEvent.setup();
    const back = withPast("UZS", 14, {
      spent: -50_000,
      change: 50_000,
      articles: [article("кафе", -50_000)],
    });
    show(
      fakeApi({ forecast: vi.fn<Api["forecast"]>().mockResolvedValue(back) }),
    );
    await user.click(await screen.findByRole("radio", { name: "UZS" }));
    await user.click(pill(14));
    expect(screen.getByText("вернули")).toBeTruthy();
    // плюс — мятным, как «пришло», а не жёлтым трат
    expect(screen.getByText("+50 000 сум").className).toContain("text-earned");
  });

  it("shows nothing spent today without a day's movement", async () => {
    const quiet = withPast("UZS", 15, { spent: 0, change: 0, articles: [] });
    show(
      fakeApi({ forecast: vi.fn<Api["forecast"]>().mockResolvedValue(quiet) }),
    );
    await screen.findByText("Октябрь");
    expect(screen.queryByText("потрачено сегодня")).toBeNull();
    // сегодня ещё не кончился — «движений не было» рано
    expect(screen.queryByText("движений не было")).toBeNull();
  });

  it("shows today's income in today's cell too", async () => {
    const paid = withPast("UZS", 15, {
      earned: 2_500_000,
      change: 2_380_000,
      articles: [
        article("продукты", 120_000),
        { ...article("аванс", 2_500_000, "💰 ДОХОДЫ"), income: true },
      ],
    });
    show(
      fakeApi({ forecast: vi.fn<Api["forecast"]>().mockResolvedValue(paid) }),
    );
    await screen.findByText("Октябрь");
    expect(within(pill(15)).getByText("+2,5м")).toBeTruthy();
    expect(pill(15).getAttribute("aria-label")).toBe(
      nb(
        "15 октября: вечером 33,4 млн сум, потрачено 120 000 сум, пришло 2 500 000 сум",
      ),
    );
  });

  it("keeps the numbers of the chosen today cell white", async () => {
    show();
    await screen.findByText("Октябрь");
    expect(pill(15).getAttribute("aria-pressed")).toBe("true");
    expect(within(pill(15)).getByText("−120к").className).not.toContain(
      "text-spent",
    );
  });
});
