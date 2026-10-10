import { describe, expect, it, vi } from "vitest";
import { offerHomeScreen, type WebApp } from "./telegram";

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
