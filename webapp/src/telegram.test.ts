import { describe, expect, it, vi } from "vitest";
import {
  followScheme,
  offerHomeScreen,
  secondaryButton,
  type BottomButton,
  type WebApp,
} from "./telegram";

function app(version: boolean, startParam?: string) {
  const addToHomeScreen = vi.fn();
  const fake = {
    isVersionAtLeast: () => version,
    initDataUnsafe: startParam ? { start_param: startParam } : {},
    addToHomeScreen,
  } as unknown as WebApp;
  return { fake, addToHomeScreen };
}

describe("offerHomeScreen", () => {
  it("offers the shortcut when opened by the startapp=home link", () => {
    const { fake, addToHomeScreen } = app(true, "home");
    expect(offerHomeScreen(fake)).toBe(true);
    expect(addToHomeScreen).toHaveBeenCalledOnce();
  });

  it("does nothing on old clients and ordinary launches", () => {
    const old = app(false, "home");
    const plain = app(true);
    expect(offerHomeScreen(old.fake)).toBe(false);
    expect(offerHomeScreen(plain.fake)).toBe(false);
    expect(offerHomeScreen(null)).toBe(false);
    expect(old.addToHomeScreen).not.toHaveBeenCalled();
    expect(plain.addToHomeScreen).not.toHaveBeenCalled();
  });
});

describe("secondaryButton", () => {
  const button = {} as BottomButton;

  it("gives Telegram's second bottom button from Bot API 7.10", () => {
    const fake = {
      isVersionAtLeast: (version: string) => version === "7.10",
      SecondaryButton: button,
    } as unknown as WebApp;
    expect(secondaryButton(fake)).toBe(button);
  });

  it("is null on an older Telegram, without the button and outside Telegram", () => {
    const old = {
      isVersionAtLeast: () => false,
      SecondaryButton: button,
    } as unknown as WebApp;
    const missing = { isVersionAtLeast: () => true } as unknown as WebApp;
    expect(secondaryButton(old)).toBeNull();
    expect(secondaryButton(missing)).toBeNull();
    expect(secondaryButton(null)).toBeNull();
  });
});

describe("followScheme", () => {
  it("marks the page with Telegram's color scheme and follows its changes", () => {
    const handlers: (() => void)[] = [];
    const fake = {
      colorScheme: "dark",
      onEvent: (_event: string, handler: () => void) => handlers.push(handler),
    };
    const root = document.createElement("div");
    followScheme(fake as unknown as WebApp, root);
    expect(root.dataset.scheme).toBe("dark");
    fake.colorScheme = "light";
    handlers.forEach((handler) => handler());
    expect(root.dataset.scheme).toBe("light");
    followScheme(null, root); // вне Telegram — светлые цвета по умолчанию
    expect(root.dataset.scheme).toBe("light");
  });
});
