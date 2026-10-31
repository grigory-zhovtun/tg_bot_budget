/** Поля, кнопки и сообщения крупно — «Ввод» и окна копилок. Общие классы fields.tsx
 * берёт и окно записи «Календаря» — его вид не меняем, поэтому здесь свои. */

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

/** Отказ сервера под полем — 17 px жирно, как остальные строки окна. */
export function BigFieldError({ text }: { text: string | undefined }) {
  return text ? (
    <p className="text-line font-bold text-danger">{text}</p>
  ) : null;
}

/** Главная кнопка окна — 18 px сверхжирная. */
export const PRIMARY =
  "h-12 rounded-xl bg-button text-lg font-extrabold text-button-text disabled:opacity-50";

/** Зелёная кнопка «Зачислить» — фон плашки «хорошо», белый сверхжирный текст. */
export const GOOD_BUTTON =
  "h-12 rounded-xl bg-chip-good text-lg font-extrabold text-white disabled:opacity-50";

/** Ошибка над кнопкой — 17 px жирно. */
export function ErrorLine({ text }: { text: string | null }) {
  return text ? (
    <p className="text-line font-bold text-danger" role="alert">
      {text}
    </p>
  ) : null;
}
