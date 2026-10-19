import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  type AdvicePoint,
  type Api,
  type MonthPlan,
  type PlanArticle,
} from "../api";
import { adviceFixture, nextPlanFixture, planFixture } from "../mocks/fixtures";
import { Store, StoreProvider } from "../store";
import { fakeApi } from "../test/fakeApi";
import { nb } from "../test/text";
import { editRow, emptyDraft, type Draft } from "./draft";
import PlanScreen from "./PlanScreen";

function row(key: string): PlanArticle {
  const found = planFixture.rows.find((item) => item.key === key);
  if (!found) throw new Error(key);
  return found;
}

const EDITED = editRow(emptyDraft("2026-10"), row("кофе|UZS"), "1800000");

function withCoffee(plan: number): MonthPlan {
  return {
    ...planFixture,
    rows: planFixture.rows.map((item) =>
      item.key === "кофе|UZS" ? { ...item, plan } : item,
    ),
  };
}

function Harness({
  api,
  month = "2026-10",
  initial,
  restored = false,
}: {
  api: Api;
  month?: string;
  initial?: Draft;
  restored?: boolean;
}) {
  const [draft, setDraft] = useState(initial ?? emptyDraft(month));
  const [shown, setShown] = useState(restored);
  return (
    <PlanScreen
      api={api}
      month={month}
      draft={draft}
      restored={shown}
      onDraft={setDraft}
      onRestoredSeen={() => setShown(false)}
    />
  );
}

function fakeButton() {
  const button = {
    setText: vi.fn(() => button),
    show: vi.fn(() => button),
    hide: vi.fn(() => button),
    enable: vi.fn(() => button),
    disable: vi.fn(() => button),
    showProgress: vi.fn(() => button),
    hideProgress: vi.fn(() => button),
    onClick: vi.fn(() => button),
    offClick: vi.fn(() => button),
  };
  return button;
}

describe("PlanScreen", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("shows the month: free money, the list and articles with hints", async () => {
    render(<Harness api={fakeApi()} />);
    const total = await screen.findByRole("region", { name: "Итог месяца" });
    expect(within(total).getByText("9 500 000 сум").className).toContain(
      "text-big",
    );
    expect(within(total).getByText("свободно в октябре")).toBeTruthy();
    expect(
      [...total.querySelectorAll("tr")].map((row) => row.textContent),
    ).toEqual(
      [
        "поступления22 000 000",
        "расходы−10 500 000",
        "🎯 отложить−2 000 000",
      ].map(nb),
    );
    const list = screen.getByRole("region", { name: "Поступления и платежи" });
    expect(within(list).getAllByRole("button")[0]?.textContent).toBe(
      nb("Аванс15-го+5 000 000"),
    );
    expect(screen.getByText("Аванс")).toBeTruthy();
    expect(screen.getByRole("button", { name: "кофе" })).toBeTruthy();
    const food = screen.getByRole("region", { name: "🍔 ЕДА" });
    expect(within(food).getByText("🍔 ЕДА").className).toContain("text-fact");
    expect(within(food).getByText("4 500 000").className).toContain(
      "text-fact",
    );
    expect(within(food).getByText("1 500 000").className).toContain(
      "text-fact",
    );
    expect(within(food).getByText("ср. 1 800 000, сен 2 000 000")).toBeTruthy();
    // статья в валюте — со знаком валюты, без копеек у целой суммы
    const home = screen.getByRole("region", { name: "🏚️ ДОМ" });
    expect(within(home).getByText("500 $")).toBeTruthy();
    expect(within(home).getByText("ср. 500 $, сен 500 $")).toBeTruthy();
    expect(screen.getByText("⚠️ Доходы не сходятся")).toBeTruthy();
    expect(screen.getByText("по статьям 20 000 000")).toBeTruthy();
    expect(screen.getByText("в поступлениях 22 000 000")).toBeTruthy();
    const goal = screen.getByRole("button", { name: /Отложить за месяц/ });
    expect(within(goal).getByText("2 000 000")).toBeTruthy();
  });

  it("edits an article, recounts the month and saves", async () => {
    // сервер отвечает свежим планом — уже с записанной суммой
    const savePlan = vi
      .fn<Api["savePlan"]>()
      .mockResolvedValue({ ...withCoffee(1_800_000), saved: 1 });
    const api = fakeApi({ savePlan });
    const user = userEvent.setup();
    render(<Harness api={api} />);
    await user.click(await screen.findByRole("button", { name: "кофе" }));
    await user.click(screen.getByRole("button", { name: /^= в среднем/ }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    expect(screen.getByText("9 200 000 сум")).toBeTruthy();
    // исправленный план — цветом ссылки, прежний — зачёркнут
    const food = screen.getByRole("region", { name: "🍔 ЕДА" });
    expect(within(food).getByText("1 800 000").className).toContain(
      "text-link",
    );
    expect(within(food).getByText("1 500 000").className).toContain(
      "line-through",
    );
    const save = screen.getByRole("button", { name: "Сохранить 1 изменение" });
    expect(save.className).toContain("text-lg");
    await user.click(save);
    expect(api.savePlan).toHaveBeenCalledWith({
      month: "2026-10",
      rows: [{ key: "кофе|UZS", base: 1_500_000, plan: "1800000" }],
      events: [],
      goal: null,
      new_rows: [],
    });
    // сообщение — крупной строкой и строкой под ней, как заметки бота
    expect((await screen.findByText("✅ План сохранён")).className).toContain(
      "text-fact",
    );
    expect(screen.getByText("1 изменение")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /Сохранить/ })).toBeNull();
  });

  it("shows a conflict and keeps the edit on top of the fresh plan", async () => {
    const user = userEvent.setup();
    const message =
      "Пока вы правили, план поменяли — проверьте подсвеченные строки";
    const savePlan = vi.fn<Api["savePlan"]>().mockRejectedValue(
      new ApiError(
        409,
        "plan_changed",
        message,
        {},
        {
          error: { code: "plan_changed", message },
          plan: withCoffee(1_600_000),
          conflicts: ["кофе|UZS"],
        },
      ),
    );
    render(<Harness api={fakeApi({ savePlan })} initial={EDITED} />);
    await user.click(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    );
    expect(await screen.findByText(message)).toBeTruthy();
    const coffee = screen.getByRole("button", { name: "кофе" });
    expect(coffee.getAttribute("data-conflict")).toBe("true");
    expect(
      screen.getByRole("button", { name: "Сохранить 1 изменение" }),
    ).toBeTruthy();
  });

  it("keeps an edit made while the save is on its way", async () => {
    let finish: (plan: MonthPlan) => void = () => undefined;
    const savePlan = vi.fn<Api["savePlan"]>().mockImplementation(
      () =>
        new Promise<MonthPlan>((resolve) => {
          finish = resolve;
        }),
    );
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ savePlan })} initial={EDITED} />);
    await user.click(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    );
    await user.click(screen.getByRole("button", { name: "кафе" }));
    await user.click(screen.getByRole("button", { name: /^= в среднем/ }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    await act(async () => {
      finish({ ...withCoffee(1_800_000), saved: 1 });
    });
    expect(await screen.findByText(/План сохранён/)).toBeTruthy();
    // кофе уже в таблице, а правка кафе, сделанная во время сохранения, осталась
    expect(
      screen.getByRole("button", { name: "Сохранить 1 изменение" }),
    ).toBeTruthy();
  });

  it("keeps the edits when the sheet does not answer", async () => {
    const user = userEvent.setup();
    const savePlan = vi
      .fn<Api["savePlan"]>()
      .mockRejectedValue(
        new ApiError(
          503,
          "sheets_unavailable",
          "Таблица не ответила — попробуйте ещё раз",
        ),
      );
    render(<Harness api={fakeApi({ savePlan })} initial={EDITED} />);
    await user.click(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    );
    expect(await screen.findByText(/Правки не потеряны/)).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Сохранить 1 изменение" }),
    ).toBeTruthy();
  });

  it("offers to continue or reset the edits from last time", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} initial={EDITED} restored />);
    expect(await screen.findByText("Несохранённые правки")).toBeTruthy();
    expect(screen.getByText("остались с прошлого раза")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Сбросить" }));
    expect(screen.queryByRole("button", { name: /Сохранить/ })).toBeNull();
    expect(screen.queryByText("Несохранённые правки")).toBeNull();
  });

  it("marks the edits that the sheet changed since last time", async () => {
    const plan = vi.fn<Api["plan"]>().mockResolvedValue(withCoffee(1_600_000));
    render(<Harness api={fakeApi({ plan })} initial={EDITED} restored />);
    const coffee = await screen.findByRole("button", { name: "кофе" });
    expect(coffee.getAttribute("data-conflict")).toBe("true");
  });

  it("adds a row to the list", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} />);
    await user.click(
      await screen.findByRole("button", {
        name: "＋ Добавить поступление или платёж",
      }),
    );
    await user.type(screen.getByLabelText("Название"), "Премия");
    await user.type(screen.getByLabelText("День"), "25");
    for (const key of ["1", "000", "000"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    await user.click(screen.getByRole("button", { name: "Готово" }));
    expect(screen.getByText("23 000 000")).toBeTruthy();
    expect(screen.getByText("Премия")).toBeTruthy();
  });

  it("edits the savings goal", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} />);
    await user.click(
      await screen.findByRole("button", { name: /Отложить за месяц/ }),
    );
    await user.click(screen.getByRole("button", { name: "Стереть" }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    expect(screen.getByText("11 300 000 сум")).toBeTruthy();
  });

  it("shows a goal below zero as money taken from the savings", async () => {
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      goal: { amount: -9_800_000, currency: "UZS" },
    });
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ plan })} />);
    const line = await screen.findByRole("button", { name: /Из накоплений/ });
    expect(within(line).getByText(/^9\s800\s000$/)).toBeTruthy();
    await user.click(line);
    await user.click(screen.getByRole("button", { name: "Готово" }));
    expect(screen.getByRole("button", { name: /Из накоплений/ })).toBeTruthy();
    expect(screen.queryByRole("button", { name: /Сохранить/ })).toBeNull();
  });

  it("applies the piggies' «Отложить» to the goal", async () => {
    const savePlan = vi
      .fn<Api["savePlan"]>()
      .mockResolvedValue({ ...planFixture, saved: 1 });
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      goal_hint: {
        suggested: -9_800_000,
        piggies: 1_000_000,
        share: 1_200_000,
        spent: 12_000_000,
        percent: 25,
      },
    });
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ plan, savePlan })} />);
    const hint = await screen.findByRole("region", { name: "По копилкам" });
    expect(
      within(hint).getByText("По копилкам −9 800 000").className,
    ).toContain("text-fact");
    expect(
      within(hint).getByText("откладываем 25 % с поступлений"),
    ).toBeTruthy();
    expect(
      [...hint.querySelectorAll("tr")].map((row) => row.textContent),
    ).toEqual(
      ["копилки1 000 000", "подушка1 200 000", "из копилок−12 000 000"].map(nb),
    );
    await user.click(
      within(hint).getByRole("button", { name: /^Применить −9\s800\s000$/ }),
    );
    expect(screen.getByRole("button", { name: /Из накоплений/ })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "По копилкам" })).toBeNull();
    await user.click(
      screen.getByRole("button", { name: "Сохранить 1 изменение" }),
    );
    expect(savePlan).toHaveBeenCalledWith(
      expect.objectContaining({
        goal: {
          base: { amount: 2_000_000, currency: "UZS" },
          value: { amount: "-9800000", currency: "UZS" },
        },
      }),
    );
  });

  it("puts a piggy's spending into its article and the goal", async () => {
    const savePlan = vi
      .fn<Api["savePlan"]>()
      .mockResolvedValue({ ...planFixture, saved: 2 });
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      addable: [
        ...planFixture.addable,
        {
          kind: "spend",
          group: "🎉 ПРАЗДНИК",
          subcategory: "сезонный",
          icon: "🎄",
          currency: "UZS",
          average: 0,
          last: 0,
          current: 0,
        },
      ],
      goal_hint: {
        suggested: -9_800_000,
        piggies: 1_000_000,
        share: 1_200_000,
        spent: 12_000_000,
        percent: 25,
      },
      piggy_hints: [
        {
          piggy: "Новый год",
          month: "2026-10",
          group: "🎉 ПРАЗДНИК",
          subcategory: "сезонный",
          currency: "UZS",
          amount: 12_000_000,
          need: 12_000_000,
          key: null,
        },
      ],
    });
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ plan, savePlan })} />);
    const hints = await screen.findByRole("region", {
      name: "Копилки в плане",
    });
    expect(
      within(hints).getByText("🐷 Из копилки 12 000 000").className,
    ).toContain("text-fact");
    expect(within(hints).getByText("«Новый год» в октябре")).toBeTruthy();
    expect(within(hints).getByText("заложите в статью")).toBeTruthy();
    expect(within(hints).getByText("«ПРАЗДНИК / сезонный»")).toBeTruthy();
    await user.click(within(hints).getByRole("button", { name: "Применить" }));
    expect(
      screen.getByRole("button", { name: "сезонный, новая" }),
    ).toBeTruthy();
    expect(
      screen.queryByRole("region", { name: "Копилки в плане" }),
    ).toBeNull();
    expect(screen.getByRole("button", { name: /Из накоплений/ })).toBeTruthy();
    await user.click(
      screen.getByRole("button", { name: "Сохранить 2 изменения" }),
    );
    expect(savePlan).toHaveBeenCalledWith(
      expect.objectContaining({
        new_rows: [
          {
            group: "🎉 ПРАЗДНИК",
            subcategory: "сезонный",
            currency: "UZS",
            plan: "12000000",
          },
        ],
      }),
    );
  });

  it("says money taken from the savings in the totals", async () => {
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      goal: { amount: -9_800_000, currency: "UZS" },
    });
    render(<Harness api={fakeApi({ plan })} />);
    const total = await screen.findByRole("region", { name: "Итог месяца" });
    expect(
      [...total.querySelectorAll("tr")].map((row) => row.textContent),
    ).toContain(nb("💰 из накоплений+9 800 000"));
  });

  it("says how much is short when the plan does not fit", async () => {
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      goal: { amount: 30_000_000, currency: "UZS" },
    });
    render(<Harness api={fakeApi({ plan })} />);
    expect(await screen.findByText("⚠️ не хватает в октябре")).toBeTruthy();
    expect(screen.getByText("−18 500 000 сум")).toBeTruthy();
  });

  it("explains an article that the sheet has twice", async () => {
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      rows: planFixture.rows.map((item) =>
        item.key === "кафе|UZS" ? { ...item, editable: false } : item,
      ),
    });
    render(<Harness api={fakeApi({ plan })} />);
    const cafe = await screen.findByRole("button", { name: "кафе" });
    expect(cafe.hasAttribute("disabled")).toBe(true);
    expect(within(cafe).getByText("⚠️ статья встречается дважды")).toBeTruthy();
    expect(within(cafe).getByText("поправьте вкладку")).toBeTruthy();
  });

  it("says the month tab is not there yet", async () => {
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      status: "no_month_tab",
    });
    render(<Harness api={fakeApi({ plan })} />);
    expect(await screen.findByText("⚠️ Вкладки месяца ещё нет")).toBeTruthy();
    expect(screen.getByText("бот создаёт её сам 1-го числа")).toBeTruthy();
  });

  it("offers a retry when the plan does not load", async () => {
    const user = userEvent.setup();
    const plan = vi
      .fn<Api["plan"]>()
      .mockRejectedValueOnce(
        new ApiError(503, "sheets_unavailable", "Таблица не ответила"),
      )
      .mockResolvedValueOnce(planFixture);
    render(<Harness api={fakeApi({ plan })} />);
    await user.click(await screen.findByRole("button", { name: "Повторить" }));
    expect(
      await screen.findByRole("region", { name: "Итог месяца" }),
    ).toBeTruthy();
  });

  it("offers to create the next month's plan", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<Harness api={api} month="2026-11" />);
    expect(
      (await screen.findByText("Плана на ноябрь ещё нет")).className,
    ).toContain("text-fact");
    for (const line of [
      "создам вкладку «Nov 26»",
      "копией октября: статьи, суммы,",
      "поступления и цель",
      "потом поправите, что изменится",
      "до 1 ноября вкладка спрятана",
      "бот и лимит считают по октябрю",
    ]) {
      expect(screen.getByText(line)).toBeTruthy();
    }
    await user.click(
      screen.getByRole("button", { name: "Создать план на ноябрь" }),
    );
    expect(api.createNextPlan).toHaveBeenCalledTimes(1);
    expect(
      await screen.findByRole("region", { name: "Итог месяца" }),
    ).toBeTruthy();
  });

  it("keeps the offer when the plan cannot be created", async () => {
    const createNextPlan = vi
      .fn<Api["createNextPlan"]>()
      .mockRejectedValue(
        new ApiError(
          503,
          "sheets_unavailable",
          "Таблица не ответила — попробуйте ещё раз",
        ),
      );
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ createNextPlan })} month="2026-11" />);
    await user.click(
      await screen.findByRole("button", { name: "Создать план на ноябрь" }),
    );
    expect(
      await screen.findByText("Таблица не ответила — попробуйте ещё раз"),
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Создать план на ноябрь" }),
    ).toBeTruthy();
  });

  it("uses the Telegram button and hides it on leaving", async () => {
    const main = fakeButton();
    vi.stubGlobal("Telegram", {
      WebApp: {
        platform: "ios",
        MainButton: main,
        BackButton: fakeButton(),
        HapticFeedback: {
          notificationOccurred: vi.fn(),
          selectionChanged: vi.fn(),
        },
      },
    });
    const user = userEvent.setup();
    const { unmount } = render(<Harness api={fakeApi()} initial={EDITED} />);
    await screen.findByRole("region", { name: "Итог месяца" });
    expect(main.setText).toHaveBeenCalledWith("Сохранить 1 изменение");
    expect(main.show).toHaveBeenCalled();
    main.hide.mockClear();
    await user.click(screen.getByRole("button", { name: "кофе" }));
    expect(main.hide).toHaveBeenCalled(); // окно правки открыто — кнопки нет
    unmount();
    expect(main.hide).toHaveBeenCalledTimes(1);
  });

  it("adds an article to a group and saves it with the rest", async () => {
    const savePlan = vi
      .fn<Api["savePlan"]>()
      .mockResolvedValue({ ...planFixture, saved: 1 });
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ savePlan })} />);
    const food = await screen.findByRole("region", { name: "🍔 ЕДА" });
    await user.click(within(food).getByRole("button", { name: "＋ статья" }));
    for (const key of ["5", "0", "0", "0", "0", "0"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    await user.click(screen.getByRole("button", { name: "Добавить" }));
    expect(
      within(food).getByRole("button", { name: "ресторан, новая" }),
    ).toBeTruthy();
    expect(within(food).getByText("новая статья")).toBeTruthy();
    expect(screen.getByText("расходы").closest("tr")?.textContent).toBe(
      nb("расходы−11 000 000"),
    );
    await user.click(
      screen.getByRole("button", { name: "Сохранить 1 изменение" }),
    );
    expect(savePlan).toHaveBeenCalledWith(
      expect.objectContaining({
        new_rows: [
          {
            group: "🍔 ЕДА",
            subcategory: "ресторан",
            currency: "UZS",
            plan: "500000",
          },
        ],
      }),
    );
  });

  it("shows a group the tab has no rows of yet", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} />);
    const taxi = await screen.findByRole("region", { name: "🚌 ТРАНСПОРТ" });
    await user.click(within(taxi).getByRole("button", { name: "＋ статья" }));
    await user.click(screen.getByRole("button", { name: /^= в среднем/ }));
    await user.click(screen.getByRole("button", { name: "Добавить" }));
    expect(
      within(taxi).getByRole("button", { name: "такси, новая" }),
    ).toBeTruthy();
  });

  it("adds an income article and removes a new article before saving", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} />);
    const incomes = await screen.findByRole("region", {
      name: "Доходы по статьям",
    });
    await user.click(
      within(incomes).getByRole("button", { name: "＋ статья" }),
    );
    await user.click(screen.getByRole("button", { name: "1" }));
    await user.click(screen.getByRole("button", { name: "Добавить" }));
    await user.click(
      within(incomes).getByRole("button", { name: "премия, новая" }),
    );
    await user.click(screen.getByRole("button", { name: "Убрать статью" }));
    expect(
      within(incomes).queryByRole("button", { name: "премия, новая" }),
    ).toBeNull();
    expect(screen.queryByRole("button", { name: /Сохранить/ })).toBeNull();
  });

  it("names the field a save was refused for", async () => {
    const savePlan = vi.fn<Api["savePlan"]>().mockRejectedValue(
      new ApiError(422, "validation", "Проверьте введённые данные", {
        new_rows: "Во вкладке нет места для новых статей",
      }),
    );
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ savePlan })} initial={EDITED} />);
    await user.click(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    );
    expect(
      await screen.findByText("Во вкладке нет места для новых статей"),
    ).toBeTruthy();
  });

  it("creates the next month once on a double tap and lays the old draft over it", async () => {
    const createNextPlan = vi
      .fn<Api["createNextPlan"]>()
      .mockResolvedValue(nextPlanFixture);
    const stale = editRow(
      emptyDraft("2026-11"),
      { ...row("кофе|UZS"), plan: 1 },
      "1800000",
    );
    render(
      <Harness
        api={fakeApi({ createNextPlan })}
        month="2026-11"
        initial={stale}
      />,
    );
    const button = await screen.findByRole("button", {
      name: "Создать план на ноябрь",
    });
    act(() => {
      button.click();
      button.click(); // до перерисовки
    });
    expect(createNextPlan).toHaveBeenCalledTimes(1);
    const coffee = await screen.findByRole("button", { name: "кофе" });
    expect(coffee.getAttribute("data-conflict")).toBe("true");
  });

  it("speaks to the user formally on the next month card", async () => {
    render(<Harness api={fakeApi()} month="2026-11" />);
    expect(
      await screen.findByText("потом поправите, что изменится"),
    ).toBeTruthy();
  });

  it("asks the advisor about the draft and applies a point", async () => {
    const planAdvice = vi
      .fn<Api["planAdvice"]>()
      .mockResolvedValue(adviceFixture);
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ planAdvice })} initial={EDITED} />);
    await user.click(
      await screen.findByRole("button", { name: "🧠 Совет по плану" }),
    );
    expect(planAdvice).toHaveBeenCalledWith(
      expect.objectContaining({
        month: "2026-10",
        rows: [{ key: "кофе|UZS", plan: "1800000" }],
      }),
    );
    // пункт советника — абзацем крупно
    expect(
      (await screen.findByText("После цели остаётся 9,5 млн — месяц сходится."))
        .className,
    ).toContain("text-line");
    await user.click(
      screen.getByRole("button", {
        name: "Применить: кафе → 2\u00a0800\u00a0000",
      }),
    );
    expect(screen.getByRole("button", { name: "✓ Применено" })).toBeTruthy();
    expect(
      await screen.findByRole("button", { name: "Сохранить 2 изменения" }),
    ).toBeTruthy();
  });

  it("says when the advisor is down and keeps the plan usable", async () => {
    const planAdvice = vi
      .fn<Api["planAdvice"]>()
      .mockRejectedValue(
        new ApiError(503, "advisor_unavailable", "Советник сейчас недоступен"),
      );
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ planAdvice })} />);
    await user.click(
      await screen.findByRole("button", { name: "🧠 Совет по плану" }),
    );
    expect(await screen.findByText("Советник сейчас недоступен")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "кофе" }));
    expect(screen.getByRole("dialog")).toBeTruthy();
  });

  it("asks once while the advisor thinks", async () => {
    let answer: (value: { points: AdvicePoint[] }) => void = () => {};
    const planAdvice = vi.fn<Api["planAdvice"]>().mockImplementation(
      () =>
        new Promise((resolve) => {
          answer = resolve;
        }),
    );
    render(<Harness api={fakeApi({ planAdvice })} />);
    const button = await screen.findByRole("button", {
      name: "🧠 Совет по плану",
    });
    act(() => {
      button.click();
      button.click(); // до перерисовки
    });
    expect(planAdvice).toHaveBeenCalledTimes(1);
    expect(screen.getByText("Советник думает…")).toBeTruthy();
    expect(button.hasAttribute("disabled")).toBe(true);
    await act(async () => {
      answer(adviceFixture);
    });
    expect(
      screen.getByText("После цели остаётся 9,5 млн — месяц сходится."),
    ).toBeTruthy();
  });

  it("offers only the pairs that are not in the draft yet", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} />);
    const food = await screen.findByRole("region", { name: "🍔 ЕДА" });
    await user.click(within(food).getByRole("button", { name: "＋ статья" }));
    await user.click(screen.getByRole("button", { name: "5" }));
    await user.click(screen.getByRole("button", { name: "Добавить" }));
    await user.click(within(food).getByRole("button", { name: "＋ статья" }));
    expect(screen.queryByRole("radiogroup", { name: "Валюта" })).toBeNull();
    expect(screen.getByRole("status", { name: "Сумма" }).textContent).toContain(
      "USD",
    ); // ресторан в сумах уже в черновике — остался USD
  });

  it("turns a new article the other user added into a highlighted edit", async () => {
    const fresh = {
      ...planFixture,
      rows: [
        ...planFixture.rows,
        {
          key: "ресторан|UZS",
          kind: "spend" as const,
          group: "🍔 ЕДА",
          subcategory: "ресторан",
          icon: "🍷",
          currency: "UZS",
          plan: 300_000,
          average: 600_000,
          last: 900_000,
          current: 0,
          editable: true,
        },
      ],
      addable: planFixture.addable.slice(1),
    };
    const savePlan = vi.fn<Api["savePlan"]>().mockRejectedValue(
      new ApiError(
        409,
        "plan_changed",
        "Пока вы правили, план поменяли",
        {},
        {
          plan: fresh,
          conflicts: ["ресторан|UZS"],
        },
      ),
    );
    const user = userEvent.setup();
    render(<Harness api={fakeApi({ savePlan })} />);
    const food = await screen.findByRole("region", { name: "🍔 ЕДА" });
    await user.click(within(food).getByRole("button", { name: "＋ статья" }));
    for (const key of ["5", "0", "0", "0", "0", "0"]) {
      await user.click(screen.getByRole("button", { name: key }));
    }
    await user.click(screen.getByRole("button", { name: "Добавить" }));
    await user.click(
      screen.getByRole("button", { name: "Сохранить 1 изменение" }),
    );
    const restaurant = await within(food).findByRole("button", {
      name: "ресторан",
    });
    expect(restaurant.getAttribute("data-conflict")).toBe("true");
    expect(
      within(food).queryByRole("button", { name: "ресторан, новая" }),
    ).toBeNull();
  });

  it("shows what came in for the rows of the list", async () => {
    const plan = vi.fn<Api["plan"]>().mockResolvedValue({
      ...planFixture,
      events: (planFixture.events ?? []).map((item) => {
        if (item.slot === 0) {
          return {
            ...item,
            status: "done" as const,
            received: 5_000_000,
            received_on: "2026-10-15",
          };
        }
        return item.slot === 2 ? { ...item, status: "waiting" as const } : item;
      }),
    });
    render(<Harness api={fakeApi({ plan })} />);
    expect(await screen.findByText("✓ пришло 5 000 000, 15.10")).toBeTruthy();
    expect(screen.getByText("⏳ ждём с 18.10")).toBeTruthy();
  });

  it("shows the copy of the plan at once while it asks the sheet", () => {
    const store = new Store();
    store.put("plan:2026-10", planFixture);
    const plan = vi
      .fn<Api["plan"]>()
      .mockReturnValue(new Promise(() => undefined));
    render(
      <StoreProvider value={store}>
        <Harness api={fakeApi({ plan })} />
      </StoreProvider>,
    );
    expect(screen.getByRole("region", { name: "Итог месяца" })).toBeTruthy();
    // черновик ложится на свежий план, поэтому сервер спрашивается всегда
    expect(plan).toHaveBeenCalledTimes(1);
  });

  it("after a save the summary and the forecast are asked again", async () => {
    const store = new Store();
    store.put("dashboard", { today: "2026-10-10" });
    store.put("forecast", { today: "2026-10-10" });
    store.put("savings", { today: "2026-10-10" });
    const user = userEvent.setup();
    render(
      <StoreProvider value={store}>
        <Harness api={fakeApi()} initial={EDITED} />
      </StoreProvider>,
    );
    await user.click(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    );
    expect(await screen.findByText(/План сохранён/)).toBeTruthy();
    expect(store.get("dashboard").at).toBe(0);
    expect(store.get("forecast").at).toBe(0);
    // список и день денег меняют взносы копилок и долю 10 %
    expect(store.get("savings").at).toBe(0);
  });

  it("says the plan could not be refreshed when it shows a copy", async () => {
    const store = new Store();
    store.put("plan:2026-10", planFixture);
    const plan = vi
      .fn<Api["plan"]>()
      .mockRejectedValue(
        new ApiError(503, "sheets_unavailable", "Таблица не ответила"),
      );
    render(
      <StoreProvider value={store}>
        <Harness api={fakeApi({ plan })} />
      </StoreProvider>,
    );
    expect(screen.getByRole("region", { name: "Итог месяца" })).toBeTruthy();
    expect((await screen.findByRole("alert")).textContent).toContain(
      "Таблица не ответила",
    );
  });

  it("reads the sheet anew when there are unsaved edits", async () => {
    const store = new Store();
    // первое чтение запуска уже было — дальше копия на сервере
    await store.fetch("dashboard", () => Promise.resolve({}));
    const api = fakeApi();
    render(
      <StoreProvider value={store}>
        <Harness api={api} initial={EDITED} />
      </StoreProvider>,
    );
    // правки ложатся на план без копии: что поменяли руками — подсветится сразу
    expect(api.plan).toHaveBeenCalledWith("2026-10", true);
  });
});
