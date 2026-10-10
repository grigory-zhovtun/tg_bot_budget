import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type Api } from "./api";
import App from "./App";
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
});
