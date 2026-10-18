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
  it("says how much to set aside this month", async () => {
    render(<SavingsTab api={fakeApi()} boot={null} refresh={0} />);
    expect(await screen.findByText("В октябре отложить")).toBeTruthy();
    expect(
      screen.getByText("в копилки по плану 6,00 млн, осталось 6,00 млн"),
    ).toBeTruthy();
    expect(
      screen.getByText("10 % от поступлений ≈ 4,00 млн — в подушку"),
    ).toBeTruthy();
  });

  it("names the goals that share the 10 %", async () => {
    const api = withSavings({
      month: {
        ...month(),
        share_to: "split",
        goals: [{ id: "car001", amount: 2_000_000 }],
      },
    });
    render(<SavingsTab api={api} boot={null} refresh={0} />);
    expect(
      await screen.findByText(
        "10 % от поступлений ≈ 4,00 млн — пополам: подушка и «Машина»",
      ),
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
    await screen.findByText("В октябре отложить");
    expect(
      screen
        .getAllByRole("heading", { level: 2 })
        .map((heading) => heading.textContent),
    ).toEqual([
      "В октябре отложить",
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
    await screen.findByText("В октябре отложить");
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
    await screen.findByText("В октябре отложить");
    expect(screen.queryByRole("button", { name: "＋ Копилка" })).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Открыть копилку «Новый год»" }),
    ).toBeNull();
  });
});
