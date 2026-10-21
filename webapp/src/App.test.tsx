import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type Dashboard } from "./api";
import App from "./App";
import { dashboardFixture, planFixture } from "./mocks/fixtures";
import { CACHE_KEY } from "./store";
import { editRow, emptyDraft } from "./plan/draft";
import { fakeApi } from "./test/fakeApi";

// плитка «ЕДА» из dashboardFixture: перерасход — двумя строками
const FOOD_TILE = { name: "ЕДА", description: /^⚠️ сверх на 300\s000$/ };

describe("App", () => {
  it("opens the entry screen on the user's card", async () => {
    render(<App api={fakeApi()} />);
    expect(
      await screen.findByRole("radio", {
        name: "VISA 9090 UZS",
        checked: true,
      }),
    ).toBeTruthy();
  });

  it("asks to reopen when the bot does not recognise the user", async () => {
    const bootstrap = vi
      .fn<Api["bootstrap"]>()
      .mockRejectedValue(
        new ApiError(401, "unauthorized", "Откройте приложение заново из бота"),
      );
    render(<App api={fakeApi({ bootstrap })} />);
    expect(
      await screen.findByText("Откройте приложение заново из бота"),
    ).toBeTruthy();
    expect(screen.getByRole("button", { name: "Повторить" })).toBeTruthy();
  });

  it("opens «Отложил» in «Накопления» from the bot's button", async () => {
    window.history.replaceState(null, "", "/app/?launch=t&open=save");
    try {
      render(<App api={fakeApi()} />);
      expect(
        await screen.findByRole("dialog", { name: "Отложил" }),
      ).toBeTruthy();
      // «Накопления» теперь — раздел «Плана»
      expect(
        screen.getByRole("tab", { name: "План" }).getAttribute("aria-selected"),
      ).toBe("true");
      expect(
        screen.getByRole("radio", { name: "Накопления", checked: true }),
      ).toBeTruthy();
    } finally {
      window.history.replaceState(null, "", "/");
    }
  });

  it("does not open «Отложил» again after the user moves on", async () => {
    window.history.replaceState(null, "", "/app/?launch=t&open=save");
    try {
      const user = userEvent.setup();
      render(<App api={fakeApi()} />);
      await screen.findByRole("dialog", { name: "Отложил" });
      await user.click(screen.getByRole("button", { name: "Закрыть" }));
      await user.click(screen.getByRole("tab", { name: "Сводка" }));
      await user.click(screen.getByRole("tab", { name: "План" }));
      await screen.findByText("Откладываем 25 %");
      expect(screen.queryByRole("dialog", { name: "Отложил" })).toBeNull();
    } finally {
      window.history.replaceState(null, "", "/");
    }
  });

  it("switches to the dashboard", async () => {
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    const tab = screen.getByRole("tab", { name: "Сводка" });
    await user.click(tab);
    expect(
      await screen.findByRole("region", { name: "Можно сегодня" }),
    ).toBeTruthy();
    // вкладки приложения — крупно, как «Сводка», но все четыре в ряд
    expect(tab.className).toContain("text-tab");
  });

  it("names the tabs by what they hold, each as wide as its name", () => {
    render(<App api={fakeApi()} />);
    const tabs = screen.getAllByRole("tab");
    expect(tabs.map((tab) => tab.textContent)).toEqual([
      "Ввод",
      "Сводка",
      "План",
      "Календарь",
    ]);
    // «Календарь» не влезает в равную четверть ряда — ширина по подписи; отступы
    // узкие: при крупном шрифте телефона (×1,25) ряд влезает и в 320 px
    const list = screen.getByRole("tablist");
    expect(list.className).toContain("flex");
    expect(list.className).toContain("gap-0.5");
    expect(list.className).not.toContain("grid-cols-4");
    for (const tab of tabs) {
      expect(tab.className).toContain("flex-auto");
      expect(tab.className).toContain("px-0.5");
    }
  });

  it("switches to the calendar", async () => {
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    await user.click(screen.getByRole("tab", { name: "Календарь" }));
    expect(await screen.findByText("Октябрь")).toBeTruthy();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it("opens the plan tab", async () => {
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    await user.click(await screen.findByRole("tab", { name: /План/ }));
    expect(
      await screen.findByRole("region", { name: "Итог месяца" }),
    ).toBeTruthy();
  });

  it("marks unsaved plan edits and offers them again", async () => {
    const coffee = planFixture.rows[0];
    if (!coffee) throw new Error("fixture");
    const draft = editRow(emptyDraft("2026-10"), coffee, "1800000");
    localStorage.setItem("plan-draft:1:2026-10", JSON.stringify(draft));
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    expect(
      await screen.findByLabelText("есть несохранённые правки"),
    ).toBeTruthy();
    await user.click(screen.getByRole("tab", { name: /План/ }));
    expect(await screen.findByText("Несохранённые правки")).toBeTruthy();
  });

  it("keeps plan edits between tabs", async () => {
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    await user.click(await screen.findByRole("tab", { name: /План/ }));
    await user.click(await screen.findByRole("button", { name: "кофе" }));
    await user.click(screen.getByRole("button", { name: /^= в среднем/ }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    await user.click(screen.getByRole("tab", { name: "Сводка" }));
    expect(screen.getByLabelText("есть несохранённые правки")).toBeTruthy();
    await user.click(screen.getByRole("tab", { name: /План/ }));
    expect(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    ).toBeTruthy();
  });

  it("marks unsaved edits of the next month too", async () => {
    const coffee = planFixture.rows[0];
    if (!coffee) throw new Error("fixture");
    const draft = editRow(emptyDraft("2026-11"), coffee, "1800000");
    localStorage.setItem("plan-draft:1:2026-11", JSON.stringify(draft));
    render(<App api={fakeApi()} />);
    expect(
      await screen.findByLabelText("есть несохранённые правки"),
    ).toBeTruthy();
  });

  it("asks the summary once for the entry tiles and the summary tab", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<App api={api} />);
    expect(await screen.findByRole("button", FOOD_TILE)).toBeTruthy();
    await user.click(screen.getByRole("tab", { name: "Сводка" }));
    expect(screen.getByRole("region", { name: "Можно сегодня" })).toBeTruthy();
    expect(api.dashboard).toHaveBeenCalledTimes(1);
    // первое чтение запуска — по таблице заново: правки руками видны сразу
    expect(api.dashboard).toHaveBeenCalledWith(true);
  });

  it("loads «Накопления» in the background after the summary", async () => {
    const api = fakeApi();
    render(<App api={api} />);
    await waitFor(() => {
      expect(api.savings).toHaveBeenCalledWith(false);
    });
  });

  it("loads the forecast and the plan in the background", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<App api={api} />);
    await waitFor(() => {
      expect(api.plan).toHaveBeenCalledWith("2026-10", false);
    });
    expect(api.forecast).toHaveBeenCalledWith(false);
    await user.click(screen.getByRole("tab", { name: "Календарь" }));
    expect(screen.getByText("Октябрь")).toBeTruthy();
    expect(api.forecast).toHaveBeenCalledTimes(1);
  });

  it("opens with today's numbers from the phone while it asks for fresh ones", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-10-10T09:00:00"));
    const first = render(<App api={fakeApi()} />);
    expect(await screen.findByRole("button", FOOD_TILE)).toBeTruthy();
    first.unmount();
    const waiting = () => new Promise<never>(() => undefined);
    const user = userEvent.setup();
    render(
      <App
        api={fakeApi({
          bootstrap: vi.fn<Api["bootstrap"]>(waiting),
          dashboard: vi.fn<Api["dashboard"]>(waiting),
        })}
      />,
    );
    // справочник и остатки на плитках — из памяти телефона, без ожидания
    expect(screen.getByRole("button", FOOD_TILE)).toBeTruthy();
    await user.click(screen.getByRole("tab", { name: "Сводка" }));
    expect(screen.getByRole("region", { name: "Можно сегодня" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Обновляю…" })).toBeTruthy();
    vi.useRealTimers();
  });

  it("asks the forecast and the plan only after the summary", async () => {
    let answer: (value: Dashboard) => void = () => undefined;
    const dashboard = vi.fn<Api["dashboard"]>().mockReturnValue(
      new Promise<Dashboard>((resolve) => {
        answer = resolve;
      }),
    );
    const api = fakeApi({ dashboard });
    render(<App api={api} />);
    await screen.findByRole("tab", { name: "Сводка" });
    await act(async () => {
      await Promise.resolve();
    });
    expect(api.forecast).not.toHaveBeenCalled();
    await act(async () => {
      answer(dashboardFixture);
      await Promise.resolve();
    });
    await waitFor(() => {
      expect(api.forecast).toHaveBeenCalled();
    });
  });

  it("forgets the numbers on the phone when the bot does not know the user", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-10-10T09:00:00"));
    const first = render(<App api={fakeApi()} />);
    expect(await screen.findByRole("button", FOOD_TILE)).toBeTruthy();
    first.unmount();
    expect(localStorage.getItem(CACHE_KEY)).not.toBeNull();
    // подпись не та: сервер отказывает каждому запросу
    const refused = () =>
      Promise.reject(
        new ApiError(401, "unauthorized", "Откройте приложение заново из бота"),
      );
    render(
      <App
        api={fakeApi({
          bootstrap: vi.fn<Api["bootstrap"]>(refused),
          dashboard: vi.fn<Api["dashboard"]>(refused),
          forecast: vi.fn<Api["forecast"]>(refused),
          plan: vi.fn<Api["plan"]>(refused),
          savings: vi.fn<Api["savings"]>(refused),
        })}
      />,
    );
    expect(
      await screen.findByText("Откройте приложение заново из бота"),
    ).toBeTruthy();
    expect(localStorage.getItem(CACHE_KEY)).toBeNull();
    // и на экране — не цифры с прошлого раза
    expect(screen.queryByText("300 000")).toBeNull();
    vi.useRealTimers();
  });
});
