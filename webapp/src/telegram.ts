/** Типизированная обёртка над Telegram.WebApp (скрипт telegram-web-app.js). */

export type HomeScreenStatus = "unsupported" | "unknown" | "added" | "missed";

export interface BottomButton {
  setText(text: string): BottomButton;
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
  initDataUnsafe: { start_param?: string };
  platform: string;
  ready(): void;
  expand(): void;
  isVersionAtLeast(version: string): boolean;
  MainButton: BottomButton;
  BackButton: BackButton;
  HapticFeedback: {
    notificationOccurred(type: "error" | "success" | "warning"): void;
  };
  addToHomeScreen(): void;
  checkHomeScreenStatus(callback: (status: HomeScreenStatus) => void): void;
  openTelegramLink(url: string): void;
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

export function haptic(type: "error" | "success"): void {
  telegram()?.HapticFeedback.notificationOccurred(type);
}
