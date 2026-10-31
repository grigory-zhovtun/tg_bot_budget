import { act } from "@testing-library/react";
import { vi, type Mock } from "vitest";
import type { BackButton, BottomButton, WebApp } from "../telegram";

/** Кнопка панели Telegram в тестах: каждый метод — шпион. */
export type FakeButton = { [K in keyof BottomButton]: Mock<BottomButton[K]> };

function fakeButton(): FakeButton {
  return {
    setText: vi.fn<BottomButton["setText"]>(),
    setParams: vi.fn<BottomButton["setParams"]>(),
    show: vi.fn<BottomButton["show"]>(),
    hide: vi.fn<BottomButton["hide"]>(),
    enable: vi.fn<BottomButton["enable"]>(),
    disable: vi.fn<BottomButton["disable"]>(),
    showProgress: vi.fn<BottomButton["showProgress"]>(),
    hideProgress: vi.fn<BottomButton["hideProgress"]>(),
    onClick: vi.fn<BottomButton["onClick"]>(),
    offClick: vi.fn<BottomButton["offClick"]>(),
  };
}

/** «Назад» Telegram в тестах: каждый метод — шпион. */
export type FakeBack = { [K in keyof BackButton]: Mock<BackButton[K]> };

/** Приложение открыто в Telegram; version — есть ли Bot API 7.10 (вторая кнопка
 * панели). Снимать — vi.unstubAllGlobals(). */
export function inTelegram(version = true): {
  main: FakeButton;
  second: FakeButton;
  back: FakeBack;
} {
  const main = fakeButton();
  const second = fakeButton();
  const back: FakeBack = {
    show: vi.fn<BackButton["show"]>(),
    hide: vi.fn<BackButton["hide"]>(),
    onClick: vi.fn<BackButton["onClick"]>(),
    offClick: vi.fn<BackButton["offClick"]>(),
  };
  const app = {
    platform: "ios",
    isVersionAtLeast: () => version,
    MainButton: main,
    SecondaryButton: second,
    BackButton: back,
    HapticFeedback: {
      notificationOccurred: vi.fn(),
      selectionChanged: vi.fn(),
    },
  };
  vi.stubGlobal("Telegram", { WebApp: app as unknown as WebApp });
  return { main, second, back };
}

/** Нажать кнопку панели: обработчик, который на неё повесили последним. */
export async function tapPanel(button: FakeButton): Promise<void> {
  const handler = button.onClick.mock.lastCall?.[0];
  if (!handler) throw new Error("the button has no handler");
  await act(async () => {
    handler();
  });
}

/** Порядковый номер последнего вызова метода среди всех шпионов; 0 — не вызывали. */
export function lastCall(method: Mock): number {
  return Math.max(0, ...method.mock.invocationCallOrder);
}
