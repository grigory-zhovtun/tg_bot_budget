import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { Api, SavingsScreen } from "../api";
import { bootstrapFixture, savingsFixture } from "../mocks/fixtures";
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

function withSavings(changes: Partial<SavingsScreen>): Api {
  return fakeApi({
    savings: vi
      .fn<Api["savings"]>()
      .mockResolvedValue({ ...savingsFixture, ...changes }),
  });
}

describe("SavingsTab", () => {
  it("tells when the month plan's «Отложить» differs from the piggies", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
    expect(await screen.findByText("⚠️ План месяца не сходится")).toBeTruthy();
    expect(screen.getByText("в плане «Отложить» 2 000 000")).toBeTruthy();
    expect(screen.getByText("по копилкам нужно 7 200 000")).toBeTruthy();
    expect(screen.getByText("поправьте во вкладке «План»")).toBeTruthy();
  });

  it("says nothing more when the plan already matches", async () => {
    const goal = {
      suggested: 7_200_000,
      in_plan: 7_200_000,
      piggies: 6_000_000,
      share: 4_000_000,
      spent: 2_800_000,
    };
    render(<SavingsTab api={withSavings({ goal })} boot={null} refresh={0} />);
    await screen.findByText("Откладываем 25 %");
    expect(screen.queryByText(/План месяца не сходится/)).toBeNull();
  });

  it("says the percent and how much to set aside of what came in", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
    const heading = await screen.findByText("Откладываем 25 %");
    expect(heading.className).toContain("text-fact");
    expect(screen.getByText("с каждого поступления")).toBeTruthy();
    expect(screen.getByText("Отложить 4 000 000").className).toContain(
      "text-fact",
    );
    expect(screen.getByText("в октябре пришло 16 000 000")).toBeTruthy();
    const table = screen.getByRole("table");
    expect(
      [...table.querySelectorAll("tbody tr")].map((row) => row.textContent),
    ).toEqual([nb("Новый год4 000 000")]);
    expect(screen.getByText("⚠️ К срокам нужно 30 %").className).toContain(
      "text-danger",
    );
    expect(screen.getByText("чтобы успеть все копилки")).toBeTruthy();
  });

  it("names the cushion part and a percent that is enough", async () => {
    const api = withSavings({
      month: { ...month(), cushion: 1_000_000, percent_needed: 20 },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    const table = await screen.findByRole("table");
    expect(
      [...table.querySelectorAll("tbody tr")].map((row) => row.textContent),
    ).toEqual([nb("Новый год4 000 000"), nb("подушка1 000 000")]);
    expect(screen.getByText("✓ Успеваем всё к срокам")).toBeTruthy();
  });

  it("says when the percent cell cannot be read", async () => {
    const api = withSavings({
      month: { ...month(), percent: 10, percent_source: "bad" },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(await screen.findByText("⚠️ Процент не читается")).toBeTruthy();
    expect(screen.getByText("считаю по 10 %")).toBeTruthy();
  });

  it("says nothing came in yet", async () => {
    const api = withSavings({
      month: { ...month(), received: 0, due: 0, left: 0 },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(
      await screen.findByText("в октябре поступлений ещё не было"),
    ).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Отложил…" })).toBeNull();
  });

  it("opens the percent and the order windows", async () => {
    const user = userEvent.setup();
    render(<SavingsTab api={fakeApi()} boot={bootstrapFixture} refresh={0} />);
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

  it("tells whether the plan leaves enough for life", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
    const line = await screen.findByText("⚠️ На жизнь не хватит");
    // не хватает — оранжевым, как остальные предупреждения
    expect(line.className).toContain("text-danger");
    expect(screen.getByText("останется 31 800 000")).toBeTruthy();
    expect(screen.getByText("обычные траты 34 000 000")).toBeTruthy();
    expect(screen.getByText("разница 2 200 000")).toBeTruthy();
  });

  it("says when the plan leaves enough for life", async () => {
    const api = withSavings({
      enough: {
        income: 42_000_000,
        save: 4_000_000,
        life: 38_000_000,
        ordinary: 34_000_000,
        gap: 4_000_000,
      },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(await screen.findByText("✓ На жизнь хватает")).toBeTruthy();
    expect(screen.getByText("останется 38 000 000")).toBeTruthy();
    expect(screen.getByText("запас 4 000 000")).toBeTruthy();
  });

  it("shows the cushion without the money of the piggies", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
    expect(await screen.findByText("заморожено 1 500 $")).toBeTruthy();
    expect(screen.getByText("минус копилки 400 $")).toBeTruthy();
    expect(screen.getByText("на вечер вчера")).toBeTruthy();
  });

  it("warns when the piggies hold more than the frozen money", async () => {
    const api = withSavings({
      cushion: { ...cushion(), amount: 0, over: 300 },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(await screen.findByText("⚠️ Копилки больше карты")).toBeTruthy();
    expect(screen.getByText("разница 300 $")).toBeTruthy();
    expect(screen.getByText("отложите разницу")).toBeTruthy();
    expect(screen.getByText("или поправьте копилки")).toBeTruthy();
  });

  it("lists the piggies by deadline with their state", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
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
    expect(
      within(trip).getByText("к 01.06 будет 2 000 $ из 2 500 $"),
    ).toBeTruthy();
    expect(within(repair).getByText("⚠️ Не указана сумма").className).toContain(
      "text-danger",
    );
    expect(within(repair).getByText("до 01.08.2027")).toBeTruthy();
    expect(within(car).getByText("без срока")).toBeTruthy();
    expect(within(broken).getByText("⚠️ Проверьте строку 6")).toBeTruthy();
    expect(within(broken).getByText("на листе «Копилки»")).toBeTruthy();
  });

  it("invites to start the first piggy", async () => {
    render(
      <SavingsTab api={withSavings({ piggies: [] })} boot={null} refresh={0} />,
    );
    expect(
      await screen.findByText(
        "Заведите копилку на Новый год, отпуск или другую большую трату — бот разложит взносы по месяцам",
      ),
    ).toBeTruthy();
  });

  it("shows three months of the plan and the whole year on request", async () => {
    const user = userEvent.setup();
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
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
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
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
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(
      await screen.findByText(
        "Вкладки месяца нет — бот создаёт её 1-го числа.",
      ),
    ).toBeTruthy();
  });

  it("reads the table anew when the summary is refreshed", async () => {
    const api = fakeApi();
    const { rerender } = render(
      <SavingsTab api={api} boot={null} refresh={0} />,
    );
    await screen.findByText("Откладываем 25 %");
    rerender(<SavingsTab api={api} boot={null} refresh={1} />);
    expect(api.savings).toHaveBeenLastCalledWith(true);
  });

  it("opens a piggy from its card and closes the window after a save", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<SavingsTab api={api} boot={bootstrapFixture} refresh={0} />);
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

  it("starts a new piggy and sets a sum aside from the top", async () => {
    const user = userEvent.setup();
    render(<SavingsTab api={fakeApi()} boot={bootstrapFixture} refresh={0} />);
    await user.click(await screen.findByRole("button", { name: "＋ Копилка" }));
    expect(screen.getByRole("dialog", { name: "Новая копилка" })).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Закрыть" }));
    await user.click(screen.getByRole("button", { name: "Отложил…" }));
    expect(screen.getByRole("dialog", { name: "Отложил" })).toBeTruthy();
  });

  it("only shows the piggies until the catalog is loaded", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
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
