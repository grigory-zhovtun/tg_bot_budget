/** Поля и кнопки окон копилок крупно. Общие классы fields.tsx берёт и окно записи
 * «Прогноза» — его вид не меняем, поэтому здесь свои. */

/** Поле ввода — 18 px. */
export const BIG_FIELD =
  "rounded-xl bg-section px-3 py-2 text-lg font-normal text-text";

/** Подпись поля — 17 px жирная, цветом текста. */
export const BIG_LABEL = "text-line font-bold text-text";

/** Кнопка-чип: выбранная — цветом кнопки. */
export function bigChip(on: boolean): string {
  return `rounded-full px-4 py-2 text-line font-bold ${
    on ? "bg-button text-button-text" : "bg-section"
  }`;
}

/** Сегмент переключателя: выбранный — жирнее и на фоне, остальные — цветом
 * текста, не бледные. */
export function bigSegment(on: boolean): string {
  return `rounded-lg px-1 py-2 text-line ${
    on ? "bg-bg font-extrabold shadow-sm" : "font-semibold text-text"
  }`;
}
