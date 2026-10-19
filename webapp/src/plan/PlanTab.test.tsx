import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import type { Api } from "../api";
import { fakeApi } from "../test/fakeApi";
import { emptyDraft, type Draft } from "./draft";
import PlanTab from "./PlanTab";

function Harness({ api }: { api: Api }) {
  const [drafts, setDrafts] = useState<Record<string, Draft>>({
    "2026-10": emptyDraft("2026-10"),
    "2026-11": emptyDraft("2026-11"),
  });
  return (
    <PlanTab
      api={api}
      months={["2026-10", "2026-11"]}
      drafts={drafts}
      restored={[]}
      onDraft={(draft) =>
        setDrafts((current) => ({ ...current, [draft.month]: draft }))
      }
      onRestoredSeen={() => undefined}
    />
  );
}

describe("PlanTab", () => {
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
