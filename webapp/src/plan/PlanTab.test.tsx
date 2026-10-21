import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Api } from "../api";
import { bootstrapFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import { emptyDraft, type Draft } from "./draft";
import PlanTab from "./PlanTab";

function Harness({
  api,
  ready = true,
  openSave = false,
}: {
  api: Api;
  ready?: boolean; // справочник и черновики уже есть
  openSave?: boolean;
}) {
  const [drafts, setDrafts] = useState<Record<string, Draft>>({
    "2026-10": emptyDraft("2026-10"),
    "2026-11": emptyDraft("2026-11"),
  });
  // как App: просьба бота открыть «Отложил» гаснет, когда её увидели
  const [save, setSave] = useState(openSave);
  return (
    <PlanTab
      api={api}
      boot={ready ? bootstrapFixture : null}
      months={ready ? ["2026-10", "2026-11"] : []}
      drafts={ready ? drafts : null}
      restored={[]}
      onDraft={(draft) =>
        setDrafts((current) => ({ ...current, [draft.month]: draft }))
      }
      onRestoredSeen={() => undefined}
      openSave={save}
      onSaveSeen={() => setSave(false)}
    />
  );
}

describe("PlanTab", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("opens on the month's plan with the months to choose", async () => {
    render(<Harness api={fakeApi()} />);
    const group = screen.getByRole("radiogroup", { name: "Раздел плана" });
    expect(
      within(group)
        .getAllByRole("radio")
        .map((radio) => radio.textContent),
    ).toEqual(["План месяца", "Накопления"]);
    expect(
      screen.getByRole("radio", { name: "План месяца", checked: true }),
    ).toBeTruthy();
    expect(screen.getByRole("radiogroup", { name: "Месяц" })).toBeTruthy();
    expect(
      await screen.findByRole("region", { name: "Итог месяца" }),
    ).toBeTruthy();
  });

  it("shows «Накопления» as the second section and remembers it", async () => {
    const user = userEvent.setup();
    const first = render(<Harness api={fakeApi()} />);
    await user.click(screen.getByRole("radio", { name: "Накопления" }));
    await screen.findByText("Откладываем 25 %");
    expect(
      screen
        .getAllByRole("heading", { level: 2 })
        .map((heading) => heading.textContent),
    ).toEqual([
      "Откладываем 25\u00a0%",
      "🛟 Подушка",
      "🐷 Копилки",
      "План на год",
      "Норма сбережений",
    ]);
    // месяцы — только у плана месяца
    expect(screen.queryByRole("radiogroup", { name: "Месяц" })).toBeNull();
    expect(localStorage.getItem("plan-section")).toBe("savings");
    first.unmount();
    render(<Harness api={fakeApi()} />);
    expect(
      screen.getByRole("radio", { name: "Накопления", checked: true }),
    ).toBeTruthy();
  });

  it("keeps unsaved edits of the plan while «Накопления» are open", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} />);
    await user.click(await screen.findByRole("button", { name: "кофе" }));
    await user.click(screen.getByRole("button", { name: /^= в среднем/ }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    await user.click(screen.getByRole("radio", { name: "Накопления" }));
    await screen.findByText("Откладываем 25 %");
    await user.click(screen.getByRole("radio", { name: "План месяца" }));
    expect(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    ).toBeTruthy();
  });

  it("shows «Накопления» before the catalog and the drafts are ready", async () => {
    localStorage.setItem("plan-section", "savings");
    render(<Harness api={fakeApi()} ready={false} />);
    expect(await screen.findByText("Откладываем 25 %")).toBeTruthy();
  });

  it("opens «Отложил» from the bot's button only once", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} openSave />);
    expect(await screen.findByRole("dialog", { name: "Отложил" })).toBeTruthy();
    expect(
      screen.getByRole("radio", { name: "Накопления", checked: true }),
    ).toBeTruthy();
    expect(localStorage.getItem("plan-section")).toBe("savings");
    await user.click(screen.getByRole("button", { name: "Закрыть" }));
    await user.click(screen.getByRole("radio", { name: "План месяца" }));
    await user.click(screen.getByRole("radio", { name: "Накопления" }));
    await screen.findByText("Откладываем 25 %");
    expect(screen.queryByRole("dialog", { name: "Отложил" })).toBeNull();
  });

  it("switches between this month and the next", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(<Harness api={api} />);
    expect(
      await screen.findByRole("region", { name: "Итог месяца" }),
    ).toBeTruthy();
    await user.click(screen.getByRole("radio", { name: "Ноябрь" }));
    expect(await screen.findByText("Плана на ноябрь ещё нет")).toBeTruthy();
    expect(vi.mocked(api.plan).mock.lastCall?.[0]).toBe("2026-11");
  });

  it("keeps the drafts of the two months apart", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} />);
    await user.click(await screen.findByRole("button", { name: "кофе" }));
    await user.click(screen.getByRole("button", { name: /^= в среднем/ }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    await user.click(screen.getByRole("radio", { name: "Ноябрь" }));
    await user.click(
      await screen.findByRole("button", { name: "Создать план на ноябрь" }),
    );
    expect(
      await screen.findByRole("region", { name: "Итог месяца" }),
    ).toBeTruthy();
    expect(screen.queryByRole("button", { name: /Сохранить/ })).toBeNull();
    await user.click(screen.getByRole("radio", { name: /^Октябрь/ }));
    expect(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    ).toBeTruthy();
  });

  it("tells which month has unsaved edits", async () => {
    const user = userEvent.setup();
    render(<Harness api={fakeApi()} />);
    await user.click(await screen.findByRole("button", { name: "кофе" }));
    await user.click(screen.getByRole("button", { name: /^= в среднем/ }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    expect(
      screen.getByRole("radio", { name: "Октябрь, есть несохранённые правки" }),
    ).toBeTruthy();
    expect(screen.getByRole("radio", { name: "Ноябрь" })).toBeTruthy();
  });

  it("draws the month buttons in big bold text", async () => {
    render(<Harness api={fakeApi()} />);
    const october = await screen.findByRole("radio", { name: "Октябрь" });
    expect(october.className).toContain("text-line");
    expect(october.className).toContain("font-extrabold");
  });
});
