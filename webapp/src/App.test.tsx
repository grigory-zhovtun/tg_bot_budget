import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type Api } from "./api";
import App from "./App";
import { planFixture } from "./mocks/fixtures";
import { editRow, emptyDraft } from "./plan/draft";
import { fakeApi } from "./test/fakeApi";

describe("App", () => {
  it("opens the entry screen on the user's card", async () => {
    render(<App api={fakeApi()} />);
    expect(
      await screen.findByRole("radio", {
        name: "VISA 9120 UZS",
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

  it("switches to the dashboard", async () => {
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    await user.click(screen.getByRole("tab", { name: "Сводка" }));
    expect(await screen.findByText("Можно сегодня")).toBeTruthy();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it("opens the plan tab", async () => {
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    await user.click(await screen.findByRole("tab", { name: /План/ }));
    expect(await screen.findByText("Свободно")).toBeTruthy();
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
    expect(
      await screen.findByText("Есть несохранённые правки с прошлого раза"),
    ).toBeTruthy();
  });

  it("keeps plan edits between tabs", async () => {
    const user = userEvent.setup();
    render(<App api={fakeApi()} />);
    await user.click(await screen.findByRole("tab", { name: /План/ }));
    await user.click(await screen.findByRole("button", { name: "кофе" }));
    await user.click(screen.getByRole("button", { name: "= среднему" }));
    await user.click(screen.getByRole("button", { name: "Готово" }));
    await user.click(screen.getByRole("tab", { name: "Сводка" }));
    expect(screen.getByLabelText("есть несохранённые правки")).toBeTruthy();
    await user.click(screen.getByRole("tab", { name: /План/ }));
    expect(
      await screen.findByRole("button", { name: "Сохранить 1 изменение" }),
    ).toBeTruthy();
  });
});
