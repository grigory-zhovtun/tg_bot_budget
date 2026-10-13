import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type MonthPlan, type PlanArticle } from "../api";
import { planFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
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
    expect(await screen.findByText("Свободно")).toBeTruthy();
    expect(screen.getByText("9,50 млн")).toBeTruthy();
    expect(screen.getByText("Аванс")).toBeTruthy();
    expect(screen.getByRole("button", { name: "кофе" })).toBeTruthy();
    expect(screen.getByText("ср. 1,80 млн · сен 2,00 млн")).toBeTruthy();
    expect(
      screen.getByText("По статьям 20,00 млн, в поступлениях 22,00 млн"),
    ).toBeTruthy();
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
    await user.click(screen.getByRole("button", { name: "= среднему" }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    expect(screen.getByText("9,20 млн")).toBeTruthy();
    await user.click(
      screen.getByRole("button", { name: "Сохранить 1 изменение" }),
    );
    expect(api.savePlan).toHaveBeenCalledWith({
      month: "2026-10",
      rows: [{ key: "кофе|UZS", base: 1_500_000, plan: "1800000" }],
      events: [],
      goal: null,
    });
    expect(await screen.findByText(/План сохранён/)).toBeTruthy();
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
    await user.click(screen.getByRole("button", { name: "= среднему" }));
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
    expect(
      await screen.findByText("Есть несохранённые правки с прошлого раза"),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Сбросить" }));
    expect(screen.queryByRole("button", { name: /Сохранить/ })).toBeNull();
    expect(
      screen.queryByText("Есть несохранённые правки с прошлого раза"),
    ).toBeNull();
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
    expect(screen.getByText("23,00 млн")).toBeTruthy();
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
    expect(screen.getByText("11,30 млн")).toBeTruthy();
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
    expect(await screen.findByText("Свободно")).toBeTruthy();
  });

  it("offers to create the next month's plan", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<Harness api={api} month="2026-11" />);
    expect(await screen.findByText("Плана на ноябрь ещё нет")).toBeTruthy();
    await user.click(
      screen.getByRole("button", { name: "Создать план на ноябрь" }),
    );
    expect(api.createNextPlan).toHaveBeenCalledTimes(1);
    expect(await screen.findByText("Свободно")).toBeTruthy();
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
    await screen.findByText("Свободно");
    expect(main.setText).toHaveBeenCalledWith("Сохранить 1 изменение");
    expect(main.show).toHaveBeenCalled();
    main.hide.mockClear();
    await user.click(screen.getByRole("button", { name: "кофе" }));
    expect(main.hide).toHaveBeenCalled(); // окно правки открыто — кнопки нет
    unmount();
    expect(main.hide).toHaveBeenCalledTimes(1);
  });
});
