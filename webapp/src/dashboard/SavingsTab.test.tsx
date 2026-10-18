import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { Api, SavingsScreen } from "../api";
import { bootstrapFixture, savingsFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
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
    expect(
      await screen.findByText(
        "В плане месяца «Отложить» — 2,00 млн, по копилкам нужно 7,20 млн: поправьте в «Плане»",
      ),
    ).toBeTruthy();
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
    await screen.findByText("Откладываем 25 % с поступлений");
    expect(screen.queryByText(/В плане месяца/)).toBeNull();
  });

  it("says the percent and how much to set aside of what came in", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
    expect(
      await screen.findByText("Откладываем 25 % с поступлений"),
    ).toBeTruthy();
    expect(
      screen.getByText("В октябре пришло 16,00 млн — отложить 4,00 млн"),
    ).toBeTruthy();
    expect(screen.getByText("«Новый год» 4,00 млн")).toBeTruthy();
    expect(
      screen.getByText("Чтобы успеть всё к срокам, нужно 30 %").className,
    ).toContain("text-warn");
  });

  it("names the cushion part and a percent that is enough", async () => {
    const api = withSavings({
      month: { ...month(), cushion: 1_000_000, percent_needed: 20 },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(
      await screen.findByText("«Новый год» 4,00 млн · в подушку 1,00 млн"),
    ).toBeTruthy();
    expect(screen.getByText("Успеваем всё к срокам ✓")).toBeTruthy();
  });

  it("says when the percent cell cannot be read", async () => {
    const api = withSavings({
      month: { ...month(), percent: 10, percent_source: "bad" },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(
      await screen.findByText("Процент в таблице не читается — считаю по 10 %"),
    ).toBeTruthy();
  });

  it("says nothing came in yet", async () => {
    const api = withSavings({
      month: { ...month(), received: 0, due: 0, left: 0 },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(
      await screen.findByText("В октябре поступлений ещё не было"),
    ).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Отложил…" })).toBeNull();
  });

  it("opens the percent and the order windows", async () => {
    const user = userEvent.setup();
    render(<SavingsTab api={fakeApi()} boot={bootstrapFixture} refresh={0} />);
    await user.click(await screen.findByRole("button", { name: "Изменить" }));
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
    const line = await screen.findByText(
      "на жизнь остаётся 31,80 млн, обычные траты — 34,00 млн: не хватает 2,20 млн",
    );
    // не хватает — оранжевым, как остальные предупреждения
    expect(line.className).toContain("text-danger");
  });

  it("shows the cushion without the money of the piggies", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
    expect(
      await screen.findByText(
        "$1 100 = заморожено $1 500 − копилки $400 (на вечер вчера)",
      ),
    ).toBeTruthy();
  });

  it("warns when the piggies hold more than the frozen money", async () => {
    const api = withSavings({
      cushion: { ...cushion(), amount: 0, over: 300 },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(
      await screen.findByText(
        "В копилках на $300 больше, чем на долларовой карте, — отложите разницу или поправьте копилки",
      ),
    ).toBeTruthy();
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
    expect(within(newYear).getByText("накоплено 0 из 12,00 млн")).toBeTruthy();
    expect(within(newYear).getByText("к 20.12 · каждый год")).toBeTruthy();
    expect(within(newYear).getByText("в этом месяце 6,00 млн")).toBeTruthy();
    expect(within(newYear).getByText("успеваем").className).toContain(
      "text-ok",
    );
    expect(within(trip).getByText("накоплено 400 $ из 2 500 $")).toBeTruthy();
    expect(within(repair).getByText("сумма?").className).toContain("text-warn");
    expect(within(repair).getByText("до 01.08.2027")).toBeTruthy();
    expect(within(car).getByText("без срока")).toBeTruthy();
    expect(
      within(broken).getByText("Проверьте строку 6 на листе «Копилки»"),
    ).toBeTruthy();
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
    expect(
      within(plan).getByText(
        "Ноябрь — 10,00 млн: Новый год 6,00 млн · подушка 4,00 млн",
      ),
    ).toBeTruthy();
    expect(within(plan).getByText("✓ Новый год")).toBeTruthy();
    await user.click(within(plan).getByRole("button", { name: "Весь год" }));
    expect(within(plan).getAllByRole("listitem")).toHaveLength(12);
    expect(within(plan).getByRole("button", { name: "Свернуть" })).toBeTruthy();
  });

  it("keeps the savings rate at the bottom", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
    await screen.findByText("Откладываем 25 % с поступлений");
    expect(
      screen
        .getAllByRole("heading", { level: 2 })
        .map((heading) => heading.textContent),
    ).toEqual([
      "Откладываем 25 % с поступлений",
      "🛟 Подушка безопасности",
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
    await screen.findByText("Откладываем 25 % с поступлений");
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
    await screen.findByText("Откладываем 25 % с поступлений");
    expect(screen.queryByRole("button", { name: "＋ Копилка" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Изменить" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Порядок" })).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Открыть копилку «Новый год»" }),
    ).toBeNull();
  });
});
