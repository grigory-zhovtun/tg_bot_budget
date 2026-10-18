/** Общие куски окон: валюты карт, кнопки-чипы и сегменты, ошибка поля. */
import type { Bootstrap } from "./api";

const ORDER = ["UZS", "USD", "RUB"];

export const FIELD = "rounded-xl bg-section px-3 py-2 text-base text-text";

/** Валюты карт: UZS, USD, RUB, остальные по алфавиту; плюс валюта правки. */
export function cardCurrencies(
  boot: Bootstrap,
  extra: string | undefined,
): string[] {
  const codes = new Set(boot.sources.map((source) => source.currency));
  if (extra) codes.add(extra);
  if (codes.size === 0) codes.add("UZS");
  const rest = [...codes].filter((code) => !ORDER.includes(code)).sort();
  return [...ORDER.filter((code) => codes.has(code)), ...rest];
}

export function chip(on: boolean): string {
  return `rounded-full px-3 py-1.5 text-sm ${
    on ? "bg-button text-button-text" : "bg-section"
  }`;
}

export function segment(on: boolean): string {
  return `rounded-lg px-1 py-1.5 text-sm font-medium ${
    on ? "bg-bg shadow-sm" : "text-hint"
  }`;
}

export function FieldError({ text }: { text: string | undefined }) {
  return text ? <p className="text-sm text-danger">{text}</p> : null;
}
