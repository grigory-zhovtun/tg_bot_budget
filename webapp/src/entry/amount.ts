/** Своя клавиатура суммы: какие нажатия допустимы и как сумма уходит в API. */

export type Key =
  "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "0" | "000" | "," | "⌫";

export const MAX_WHOLE_DIGITS = 12; // API принимает до 10¹²

export function press(text: string, key: Key, decimals: boolean): string {
  if (key === "⌫") return text.slice(0, -1);
  if (key === ",") {
    if (!decimals || text.includes(",")) return text;
    return text === "" ? "0," : `${text},`;
  }
  const [whole = "", fraction] = text.split(",");
  if (fraction !== undefined) {
    return key === "000" || fraction.length >= 2 ? text : text + key;
  }
  if (key === "000" && (whole === "" || whole === "0")) return text;
  const next = whole === "0" ? key : whole + key;
  return next.length > MAX_WHOLE_DIGITS ? text : next;
}

/** «12,5» → 12.5; пусто → 0. */
export function amountValue(text: string): number {
  return text ? Number(text.replace(",", ".")) : 0;
}

/** Для API: «12,5» → «12.5», «12,» → «12». */
export function apiAmount(text: string): string {
  return text.replace(",", ".").replace(/\.$/, "");
}

/** На экране: «48000» → «48 000», «12,5» → «12,5». */
export function displayAmount(text: string): string {
  if (!text) return "0";
  const [whole = "0", fraction] = text.split(",");
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, " ");
  return fraction === undefined ? grouped : `${grouped},${fraction}`;
}
