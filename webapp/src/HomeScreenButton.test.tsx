import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import HomeScreenButton from "./HomeScreenButton";
import type { HomeScreenStatus, WebApp } from "./telegram";

function stubTelegram(initData: string, status: HomeScreenStatus = "missed") {
  const addToHomeScreen = vi.fn();
  const openTelegramLink = vi.fn();
  const fake = {
    platform: "ios",
    initData,
    initDataUnsafe: {},
    isVersionAtLeast: () => true,
    checkHomeScreenStatus: (callback: (value: HomeScreenStatus) => void) =>
      callback(status),
    addToHomeScreen,
    openTelegramLink,
  } as unknown as WebApp;
  vi.stubGlobal("Telegram", { WebApp: fake });
  return { addToHomeScreen, openTelegramLink };
}

describe("HomeScreenButton", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("adds the shortcut right away when Telegram signed the launch", async () => {
    const telegram = stubTelegram("query_id=1&hash=x");
    render(<HomeScreenButton botUsername="budget_test_bot" />);
    await userEvent.click(
      await screen.findByRole("button", { name: "📌 На экран телефона" }),
    );
    expect(telegram.addToHomeScreen).toHaveBeenCalledOnce();
  });

  it("goes through the bot's Mini App link when opened from the keyboard", async () => {
    const telegram = stubTelegram("");
    render(<HomeScreenButton botUsername="budget_test_bot" />);
    await userEvent.click(
      await screen.findByRole("button", { name: "📌 На экран телефона" }),
    );
    expect(telegram.openTelegramLink).toHaveBeenCalledWith(
      "https://t.me/budget_test_bot?startapp=home",
    );
  });

  it("hides when the shortcut is already there", () => {
    stubTelegram("query_id=1&hash=x", "added");
    render(<HomeScreenButton botUsername="budget_test_bot" />);
    expect(screen.queryByRole("button")).toBeNull();
  });
});
