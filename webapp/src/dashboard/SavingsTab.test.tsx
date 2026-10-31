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
  return screen.getByRole("progressbar", { name: "Отложено копилкам" });
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
    expect(within(card).getByRole("button", { name: "Отложил…" })).toBeTruthy();
    // меньше текста: ни таблицы копилок, ни «пришло», ни «с каждого поступления»
    expect(within(card).queryByRole("table")).toBeNull();
    expect(within(card).queryByText(/пришло|с каждого поступления/)).toBeNull();
  });

  it("says in green when the month's part is set aside", async () => {
    const api = withSavings({
      month: { ...month(), saved: 5_000_000, left: 0 },
    });
    render(<SavingsTab api={api} boot={bootstrapFixture} />);
    const done = await screen.findByText("✓ Отложено 5 000 000 из 4 000 000");
    expect(done.className).toContain("text-ok");
    expect(bar().getAttribute("aria-valuenow")).toBe("100");
    expect(screen.queryByRole("button", { name: "Отложил…" })).toBeNull();
  });

  it("measures the piggies' part and names the cushion's", async () => {
    const api = withSavings({
      month: { ...month(), cushion: 300_000, saved: 3_700_000, left: 0 },
    });
    render(<SavingsTab api={api} boot={null} />);
    expect(
      await screen.findByText("✓ Отложено 3 700 000 из 3 700 000"),
    ).toBeTruthy();
    expect(screen.getByText("подушке — 300 000")).toBeTruthy();
    expect(bar().getAttribute("aria-valuenow")).toBe("100");
  });

  it("puts each warning in one line with its number", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} />);
    await screen.findByText("Откладываем 25 %");
    const card = setAside();
    const warnings = within(card).getAllByText(/^⚠️/);
    expect(warnings.map((line) => line.textContent)).toEqual(
      [
        "⚠️ К срокам нужно 30 %",
        "⚠️ На жизнь не хватит 2 200 000",
        "⚠️ В плане «Отложить» 2 000 000, нужно 7 200 000",
      ].map(nb),
    );
    expect(warnings[0]?.className).toContain("text-danger");
    // подробности — в окне процента, не здесь
    expect(
      within(card).queryByText(
        /успеть все копилки|останется|обычные траты|разница|поправьте|не сходится/,
      ),
    ).toBeNull();
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
      goal: {
        suggested: 7_200_000,
        in_plan: 7_200_000,
        piggies: 6_000_000,
        share: 4_000_000,
        spent: 2_800_000,
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
      screen.queryByRole("progressbar", { name: "Отложено копилкам" }),
    ).toBeNull();
    expect(screen.queryByRole("button", { name: "Отложил…" })).toBeNull();
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
    expect(within(newYear).getByText("в этом месяце 6 000 000")).toBeTruthy();
    expect(within(newYear).getByText("✓ успеваем").className).toContain(
      "bg-chip-good",
    );
    expect(within(trip).getByText("400 $").closest("p")?.textContent).toBe(
      nb("накоплено 400 $ из 2 500 $"),
    );
    expect(within(trip).getByText("⚠️ Не хватит 500 $")).toBeTruthy();
    expect(within(trip).getByText("к 01.06 будет 2 000 $")).toBeTruthy();
    expect(within(trip).getByText("нужно 2 500 $")).toBeTruthy();
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
