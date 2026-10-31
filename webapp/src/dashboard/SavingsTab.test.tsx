import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { Api, SavingsScreen } from "../api";
import {
  bootstrapFixture,
  dashboardFixture,
  savingsFixture,
} from "../mocks/fixtures";
import { Store, StoreProvider } from "../store";
import { fakeApi } from "../test/fakeApi";
import { nb } from "../test/text";
import SavingsTab from "./SavingsTab";

function month() {
  if (!savingsFixture.month) throw new Error("fixture");
  return savingsFixture.month;
}

function cushion() {
  if (!savingsFixture.cushion) throw new Error("fixture");
  return savingsFixture.cushion;
}

/** Первая карточка: «Откладываем N %». */
function setAside(): HTMLElement {
  return screen.getByRole("region", { name: "Откладываем с поступлений" });
}

function bar(): HTMLElement {
  return screen.getByRole("progressbar", { name: "Отложено за месяц" });
}

/** Строки таблицы «Куда» без шапки: клетки текстом. */
function where(card: HTMLElement): string[][] {
  const table = within(card).getByRole("table", { name: "Куда" });
  return [...table.querySelectorAll("tbody tr")].map((row) =>
    [...row.querySelectorAll("td")].map((cell) => cell.textContent ?? ""),
  );
}

/** Строки карточки по порядку. */
function lines(card: HTMLElement): string[] {
  return [...card.querySelectorAll("p")].map((line) => line.textContent ?? "");
}

/** Строки идут в этом порядке (между ними могут быть другие). */
function inOrder(card: HTMLElement, wanted: string[]): void {
  const found = lines(card);
  const at = wanted.map((text) => found.indexOf(nb(text)));
  expect(at.every((index) => index >= 0)).toBe(true);
  expect([...at].sort((a, b) => a - b)).toEqual(at);
}

function withSavings(changes: Partial<SavingsScreen>): Api {
  return fakeApi({
    savings: vi
      .fn<Api["savings"]>()
      .mockResolvedValue({ ...savingsFixture, ...changes }),
  });
}

describe("SavingsTab", () => {
  it("titles the section with the month and reads the table anew", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<SavingsTab api={api} boot={null} />);
    expect(
      await screen.findByRole("heading", {
        level: 1,
        name: "Накопления · октябрь",
      }),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(api.savings).toHaveBeenLastCalledWith(true);
  });

  it("says the percent, the month's sum and how much is set aside", async () => {
    const api = withSavings({
      month: { ...month(), saved: 1_000_000, left: 3_000_000 },
      piggies: savingsFixture.piggies.map((piggy) =>
        piggy.id === "p3k9za" ? { ...piggy, saved_month: 1_000_000 } : piggy,
      ),
    });
    render(<SavingsTab api={api} boot={bootstrapFixture} />);
    const heading = await screen.findByRole("heading", {
      level: 2,
      name: nb("Откладываем 25 %"),
    });
    expect(heading.className).toContain("text-fact");
    const card = setAside();
    expect(
      within(card).getByRole("button", { name: "Изменить процент" })
        .textContent,
    ).toBe("Изменить");
    expect(
      within(card).getByText("за октябрь отложить 4 000 000"),
    ).toBeTruthy();
    expect(bar().getAttribute("aria-valuenow")).toBe("25");
    expect(
      within(card).getByText("отложено 1 000 000, ещё 3 000 000"),
    ).toBeTruthy();
    expect(within(card).getByText("25 % от пришедших 16 000 000")).toBeTruthy();
    const table = within(card).getByRole("table", { name: "Куда" });
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((cell) => cell.textContent),
    ).toEqual(["куда", "отложено"]);
    expect(where(card)).toEqual([
      ["Новый год", nb("1 000 000")],
      ["подушка", "0"],
    ]);
    expect(within(table).getByText("Новый год").className).toContain(
      "wrap-anywhere",
    );
    expect(within(card).getByRole("button", { name: "Отложил…" })).toBeTruthy();
    // копилкам ещё есть что отложить — про долю подушки скажет окно
    expect(within(card).queryByText(/доля подушки/)).toBeNull();
  });

  it("says in green when the whole month is set aside", async () => {
    const api = withSavings({
      month: {
        ...month(),
        saved: 3_600_000,
        left: 0,
        cushion: 400_000,
        cushion_saved: 400_000,
      },
    });
    render(<SavingsTab api={api} boot={bootstrapFixture} />);
    const done = await screen.findByText("✓ Октябрь: отложено 4 000 000");
    expect(done.className).toContain("text-ok");
    expect(
      screen.queryByRole("progressbar", { name: "Отложено за месяц" }),
    ).toBeNull();
    expect(screen.queryByRole("button", { name: "Отложил…" })).toBeNull();
    expect(where(setAside())).toEqual([["подушка", nb("400 000")]]);
  });

  it("keeps the cushion's part until the dollars are bought", async () => {
    const api = withSavings({
      month: {
        ...month(),
        saved: 3_600_000,
        left: 0,
        cushion: 400_000,
        cushion_saved: 0,
      },
    });
    render(<SavingsTab api={api} boot={bootstrapFixture} />);
    expect(
      await screen.findByText("отложено 3 600 000, ещё 400 000"),
    ).toBeTruthy();
    expect(bar().getAttribute("aria-valuenow")).toBe("90");
    // копилкам откладывать нечего — «Отложил…» нет, а «ещё» — доля подушки
    expect(screen.queryByRole("button", { name: "Отложил…" })).toBeNull();
    inOrder(setAside(), [
      "отложено 3 600 000, ещё 400 000",
      "ещё — доля подушки: купите на неё долларов",
    ]);
  });

  it("counts the dollars bought before the piggies are written", async () => {
    const api = withSavings({
      month: {
        ...month(),
        due: 4_000_000,
        saved: 0,
        left: 2_000_000,
        cushion: 2_000_000,
        cushion_saved: 4_000_000,
        cushion_room: 0,
      },
    });
    render(<SavingsTab api={api} boot={bootstrapFixture} />);
    const done = await screen.findByText("✓ Октябрь: отложено 4 000 000");
    expect(done.className).toContain("text-ok");
    const card = setAside();
    expect(
      screen.queryByRole("progressbar", { name: "Отложено за месяц" }),
    ).toBeNull();
    expect(within(card).queryByText(/ещё/)).toBeNull();
    // копилкам есть что записать
    expect(within(card).getByRole("button", { name: "Отложил…" })).toBeTruthy();
    expect(where(card)).toEqual([["подушка", nb("4 000 000")]]);
  });

  it("puts the two warnings together with the numbers", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} />);
    await screen.findByText("Откладываем 25 %");
    const card = setAside();
    const warnings = within(card).getAllByText(/^⚠️/);
    expect(warnings.map((line) => line.textContent)).toEqual([
      "⚠️ Цели не сходятся с тратами",
    ]);
    expect(warnings[0]?.className).toContain("text-danger");
    inOrder(card, [
      "⚠️ Цели не сходятся с тратами",
      "копилкам к срокам нужно 30 %,",
      "а траты и при 25 % больше",
      "дохода на 2 200 000 в месяц",
      "сдвиньте сроки копилок или сократите траты",
    ]);
    expect(within(card).queryByText(/В плане «Отложить»/)).toBeNull();
  });

  it("asks for the percent only when the spending is fine", async () => {
    const api = withSavings({
      enough: {
        income: 42_000_000,
        save: 4_000_000,
        life: 38_000_000,
        ordinary: 34_000_000,
        gap: 4_000_000,
      },
    });
    render(<SavingsTab api={api} boot={null} />);
    await screen.findByText("Откладываем 25 %");
    const card = setAside();
    expect(
      within(card)
        .getAllByText(/^⚠️/)
        .map((line) => line.textContent),
    ).toEqual([nb("⚠️ Копилкам к срокам нужно 30 %")]);
    inOrder(card, [
      "⚠️ Копилкам к срокам нужно 30 %",
      "поднимите процент или сдвиньте сроки копилок",
    ]);
  });

  it("says what life lacks when the percent is fine", async () => {
    const api = withSavings({ month: { ...month(), percent_needed: 20 } });
    render(<SavingsTab api={api} boot={null} />);
    await screen.findByText("Откладываем 25 %");
    const card = setAside();
    expect(
      within(card)
        .getAllByText(/^⚠️/)
        .map((line) => line.textContent),
    ).toEqual([nb("⚠️ На жизнь не хватит 2 200 000 в месяц")]);
    inOrder(card, [
      "⚠️ На жизнь не хватит 2 200 000 в месяц",
      "сократите траты или уменьшите процент",
    ]);
  });

  it("leaves out the good news to keep the card short", async () => {
    const api = withSavings({
      month: { ...month(), percent_needed: 20 },
      enough: {
        income: 42_000_000,
        save: 4_000_000,
        life: 38_000_000,
        ordinary: 34_000_000,
        gap: 4_000_000,
      },
    });
    render(<SavingsTab api={api} boot={null} />);
    await screen.findByText("Откладываем 25 %");
    expect(within(setAside()).queryByText(/^(⚠️|✓)/)).toBeNull();
  });

  it("says when the percent cell cannot be read", async () => {
    const api = withSavings({
      month: { ...month(), percent: 10, percent_source: "bad" },
    });
    render(<SavingsTab api={api} boot={null} />);
    expect(
      await screen.findByText("⚠️ Процент не читается, считаю 10 %"),
    ).toBeTruthy();
  });

  it("says nothing came in yet", async () => {
    const api = withSavings({
      month: { ...month(), received: 0, due: 0, left: 0 },
    });
    render(<SavingsTab api={api} boot={null} />);
    expect(
      await screen.findByText("в октябре поступлений ещё не было"),
    ).toBeTruthy();
    expect(
      screen.queryByRole("progressbar", { name: "Отложено за месяц" }),
    ).toBeNull();
    expect(screen.queryByRole("button", { name: "Отложил…" })).toBeNull();
    const card = setAside();
    expect(within(card).queryByText(/от пришедших/)).toBeNull();
    expect(within(card).queryByRole("table", { name: "Куда" })).toBeNull();
  });

  it("opens the percent and the order windows", async () => {
    const user = userEvent.setup();
    render(<SavingsTab api={fakeApi()} boot={bootstrapFixture} />);
    await user.click(
      await screen.findByRole("button", { name: "Изменить процент" }),
    );
    expect(
      screen.getByRole("dialog", { name: "Откладываем с поступлений" }),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Закрыть" }));
    await user.click(screen.getByRole("button", { name: "Порядок" }));
    expect(
      screen.getByRole("dialog", { name: "Порядок важности" }),
    ).toBeTruthy();
  });

  it("opens the month of life window", async () => {
    const user = userEvent.setup();
    render(<SavingsTab api={fakeApi()} boot={bootstrapFixture} />);
    await user.click(
      await screen.findByRole("button", { name: "Изменить месяц жизни" }),
    );
    expect(screen.getByRole("dialog", { name: "Месяц жизни" })).toBeTruthy();
  });

  it("shows the cushion without the money of the piggies", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} />);
    const table = await screen.findByRole("table", { name: "Расчёт подушки" });
    expect(
      within(table)
        .getAllByRole("row")
        .map((row) => row.textContent?.replace(/\s/g, " ")),
    ).toEqual([
      "доллары на карте1 900 $",
      "Квартплата 18.10−400 $",
      "копилки−400 $",
      "подушка1 100 $",
    ]);
    expect(screen.queryByText("на вечер вчера")).toBeNull();
  });

  it("warns when the piggies hold more than the frozen money", async () => {
    const api = withSavings({
      cushion: { ...cushion(), amount: 0, over: 300 },
    });
    render(<SavingsTab api={api} boot={null} />);
    expect(await screen.findByText("⚠️ Копилки больше карты")).toBeTruthy();
    expect(screen.getByText("разница 300 $")).toBeTruthy();
    expect(screen.getByText("отложите разницу")).toBeTruthy();
    expect(screen.getByText("или поправьте копилки")).toBeTruthy();
  });

  it("lists the piggies by deadline with their state", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} />);
    const list = await screen.findByRole("list", { name: "Копилки" });
    const cards = within(list).getAllByRole("listitem");
    expect(
      cards.map((card) => within(card).getByRole("heading").textContent),
    ).toEqual(["Новый год", "Отпуск", "Ремонт", "Машина", "Без валюты"]);
    const [newYear, trip, repair, car, broken] = cards;
    if (!newYear || !trip || !repair || !car || !broken)
      throw new Error("cards");
    const saved = within(newYear).getByText("0");
    expect(saved.tagName).toBe("STRONG");
    expect(saved.closest("p")?.textContent).toBe(
      nb("накоплено 0 из 12 000 000"),
    );
    expect(within(newYear).getByText("к 20.12, каждый год")).toBeTruthy();
    expect(within(newYear).getByText("в октябре отложено 0")).toBeTruthy();
    expect(
      within(newYear).getByText("в этом месяце ещё 6 000 000"),
    ).toBeTruthy();
    expect(within(newYear).getByText("✓ успеваем").className).toContain(
      "bg-chip-good",
    );
    expect(within(trip).getByText("400 $").closest("p")?.textContent).toBe(
      nb("накоплено 400 $ из 2 500 $"),
    );
    expect(within(trip).getByText("⚠️ Не хватит 500 $")).toBeTruthy();
    expect(within(trip).getByText("к 01.06 будет 2 000 $")).toBeTruthy();
    expect(within(trip).queryByText("нужно 2 500 $")).toBeNull();
    expect(within(repair).getByText("⚠️ Не указана сумма").className).toContain(
      "text-danger",
    );
    expect(within(repair).getByText("до 01.08.2027")).toBeTruthy();
    expect(within(car).getByText("без срока")).toBeTruthy();
    expect(within(broken).getByText("⚠️ Проверьте строку 6")).toBeTruthy();
    expect(within(broken).getByText("на листе «Копилки»")).toBeTruthy();
  });

  it("invites to start the first piggy", async () => {
    render(<SavingsTab api={withSavings({ piggies: [] })} boot={null} />);
    expect(
      await screen.findByText(
        "Заведите копилку на Новый год, отпуск или другую большую трату — бот разложит взносы по месяцам",
      ),
    ).toBeTruthy();
  });

  it("shows three months of the plan and the whole year on request", async () => {
    const user = userEvent.setup();
    render(<SavingsTab api={fakeApi()} boot={null} />);
    const plan = await screen.findByRole("region", { name: "План на год" });
    expect(within(plan).getAllByRole("listitem")).toHaveLength(3);
    const november = within(plan).getByText("Ноябрь — 10 000 000");
    expect(november.className).toContain("text-fact");
    const item = november.closest("li");
    if (!item) throw new Error("no november");
    expect(within(item).getByText("Новый год 6 000 000")).toBeTruthy();
    expect(within(item).getByText("подушка 4 000 000")).toBeTruthy();
    expect(within(item).getByText("✓ Новый год — собрано")).toBeTruthy();
    await user.click(within(plan).getByRole("button", { name: "Весь год" }));
    expect(within(plan).getAllByRole("listitem")).toHaveLength(12);
    expect(within(plan).getByRole("button", { name: "Свернуть" })).toBeTruthy();
  });

  it("keeps the savings rate at the bottom", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} />);
    await screen.findByText("Откладываем 25 %");
    expect(
      screen
        .getAllByRole("heading", { level: 2 })
        .map((heading) => heading.textContent),
    ).toEqual([
      nb("Откладываем 25 %"),
      "🛟 Подушка",
      "🐷 Копилки",
      "План на год",
      "Норма сбережений",
    ]);
  });

  it("explains a missing month tab", async () => {
    const api = withSavings({ status: "no_month_tab", month: null });
    render(<SavingsTab api={api} boot={null} />);
    expect(
      await screen.findByText(
        "Вкладки месяца нет — бот создаёт её 1-го числа.",
      ),
    ).toBeTruthy();
  });

  it("opens a piggy from its card and closes the window after a save", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<SavingsTab api={api} boot={bootstrapFixture} />);
    await user.click(
      await screen.findByRole("button", {
        name: "Открыть копилку «Новый год»",
      }),
    );
    expect(screen.getByRole("dialog", { name: "Новый год" })).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.updatePiggy).toHaveBeenCalled();
    await vi.waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  });

  it("asks «Сводка» again after a piggy save: its frozen card shows the piggies", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    const store = new Store();
    store.put("dashboard", dashboardFixture);
    render(
      <StoreProvider value={store}>
        <SavingsTab api={api} boot={bootstrapFixture} />
      </StoreProvider>,
    );
    await user.click(
      await screen.findByRole("button", {
        name: "Открыть копилку «Новый год»",
      }),
    );
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await vi.waitFor(() => expect(store.get("dashboard").at).toBe(0));
  });

  it("starts a new piggy and sets a sum aside from the top", async () => {
    const user = userEvent.setup();
    render(<SavingsTab api={fakeApi()} boot={bootstrapFixture} />);
    await user.click(await screen.findByRole("button", { name: "＋ Копилка" }));
    expect(screen.getByRole("dialog", { name: "Новая копилка" })).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Закрыть" }));
    await user.click(screen.getByRole("button", { name: "Отложил…" }));
    expect(screen.getByRole("dialog", { name: "Отложил" })).toBeTruthy();
  });

  it("only shows the piggies until the catalog is loaded", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} />);
    await screen.findByText("Откладываем 25 %");
    expect(screen.queryByRole("button", { name: "＋ Копилка" })).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Изменить процент" }),
    ).toBeNull();
    expect(screen.queryByRole("button", { name: "Порядок" })).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Открыть копилку «Новый год»" }),
    ).toBeNull();
  });
});
