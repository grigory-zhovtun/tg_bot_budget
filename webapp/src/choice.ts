/** Выбор на экране, который помнит телефон: раздел «Сводки», раздел «Плана». */

/** Значение из памяти телефона; нет, чужое или память закрыта — fallback. */
export function readChoice<T extends string>(
  key: string,
  allowed: readonly T[],
  fallback: T,
): T {
  try {
    const stored = localStorage.getItem(key);
    return allowed.find((item) => item === stored) ?? fallback;
  } catch {
    return fallback;
  }
}

export function saveChoice(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // память недоступна (приватный режим): выбор живёт до закрытия приложения
  }
}
