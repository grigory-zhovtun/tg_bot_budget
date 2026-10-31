/** Типизированная обёртка над Telegram.WebApp (скрипт telegram-web-app.js). */

export type HomeScreenStatus = "unsupported" | "unknown" | "added" | "missed";

/** Параметры кнопки панели (Bot API 7.10): цвета — строкой «#rrggbb», position —
 * только у второй кнопки. */
export interface BottomButtonParams {
  text?: string;
  color?: string;
  text_color?: string;
  position?: "left" | "right" | "top" | "bottom";
  is_active?: boolean;
  is_visible?: boolean;
}

export interface BottomButton {
  setText(text: string): BottomButton;
  setParams(params: BottomButtonParams): BottomButton;
  show(): BottomButton;
  hide(): BottomButton;
  enable(): BottomButton;
  disable(): BottomButton;
  showProgress(leaveActive?: boolean): BottomButton;
  hideProgress(): BottomButton;
  onClick(callback: () => void): BottomButton;
  offClick(callback: () => void): BottomButton;
}

export interface BackButton {
  show(): BackButton;
  hide(): BackButton;
  onClick(callback: () => void): BackButton;
  offClick(callback: () => void): BackButton;
}

export interface WebApp {
  initData: string;
  initDataUnsafe: { start_param?: string; user?: { id: number } };
  platform: string;
  ready(): void;
  expand(): void;
  isVersionAtLeast(version: string): boolean;
  MainButton: BottomButton;
  SecondaryButton?: BottomButton; // с Bot API 7.10
  BackButton: BackButton;
  HapticFeedback: {
    notificationOccurred(type: "error" | "success" | "warning"): void;
    selectionChanged(): void;
  };
  addToHomeScreen(): void;
  checkHomeScreenStatus(callback: (status: HomeScreenStatus) => void): void;
  openTelegramLink(url: string): void;
  colorScheme: "light" | "dark";
  onEvent(eventType: "themeChanged", callback: () => void): void;
}

declare global {
  interface Window {
    Telegram?: { WebApp?: WebApp };
  }
}

/** Mini App внутри Telegram; в обычном браузере (npm run dev, тесты) — null. */
export function telegram(): WebApp | null {
  const app = window.Telegram?.WebApp;
  return app && app.platform !== "unknown" ? app : null;
}

/**
 * Тема Telegram — атрибутом data-scheme на <html>: у сумм календаря свои цвета
 * для тёмной и светлой темы. Вне Telegram — светлая, как запасные цвета index.css.
 */
export function followScheme(
  app: WebApp | null,
  root: HTMLElement = document.documentElement,
): void {
  if (!app) {
    root.dataset.scheme = "light";
    return;
  }
  const apply = () => {
    root.dataset.scheme = app.colorScheme === "dark" ? "dark" : "light";
  };
  apply();
  app.onEvent("themeChanged", apply);
}

/** Вторая кнопка панели — с Bot API 7.10; в старом Telegram и вне его — null. */
export function secondaryButton(app: WebApp | null): BottomButton | null {
  if (!app || !app.isVersionAtLeast("7.10")) return null;
  return app.SecondaryButton ?? null;
}

export function haptic(type: "error" | "success"): void {
  telegram()?.HapticFeedback.notificationOccurred(type);
}

/** Лёгкий «тик» при выборе плитки. */
export function tick(): void {
  telegram()?.HapticFeedback.selectionChanged();
}

/** Ярлык на экран — с Bot API 8.0. */
export function canUseHomeScreen(app: WebApp | null): app is WebApp {
  return app !== null && app.isVersionAtLeast("8.0");
}

/** Открыли по ссылке «…?startapp=home» — сразу предложить ярлык. */
export function offerHomeScreen(app: WebApp | null): boolean {
  if (!canUseHomeScreen(app) || app.initDataUnsafe.start_param !== "home") {
    return false;
  }
  app.addToHomeScreen();
  return true;
}
