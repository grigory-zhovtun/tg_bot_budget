/** Поля записи «Ввода» — общие для шага суммы и экрана перевода: сумма крупно,
 * «Комментарий» и «Дата». */

import { BIG_FIELD } from "../dashboard/controls";
import { dayLabel, shiftDay } from "../format";

const MAX_DAYS_BACK = 31;
// «12 345 678» — ещё 40 px; длиннее — на ступень мельче, чтобы сумма с валютой
// осталась в строке (как у главной цифры «Сводки»)
const AMOUNT_FITS = 10;

/** Сумма крупно: число не рвётся между группами цифр, валюта может уйти ниже.
 * Блочный span, а не p: на экране перевода сумма стоит внутри кнопки поля. */
export function AmountText({
  text,
  currency,
  center = false,
}: {
  text: string; // как на экране: «48 000», «12,5»
  currency: string;
  center?: boolean;
}) {
  return (
    <span
      className={`block font-black tabular-nums ${center ? "text-center" : ""} ${
        text.length > AMOUNT_FITS ? "text-big-long" : "text-big"
      }`}
      aria-live="polite"
    >
      <span className="whitespace-nowrap">{text}</span>{" "}
      <span className="text-fact font-extrabold">{currency}</span>
    </span>
  );
}

/** «Комментарий» — поле во всю ширину, до 200 символов. */
export function CommentField({
  value,
  onChange,
}: {
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="flex flex-col gap-1 text-line font-bold">
      Комментарий
      <input
        className={`${BIG_FIELD} min-h-12`}
        value={value}
        maxLength={200}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

/** «Дата: Сегодня» и выбор дня до месяца назад; выбрали сегодня — null: дату
 * ставит бот. */
export function DayField({
  day,
  today,
  onChange,
}: {
  day: string | null;
  today: string;
  onChange: (day: string | null) => void;
}) {
  return (
    <label className="flex items-center justify-between gap-3 text-line font-bold">
      <span>Дата: {dayLabel(day ?? today, today)}</span>
      <input
        type="date"
        className={BIG_FIELD}
        value={day ?? today}
        min={shiftDay(today, -MAX_DAYS_BACK)}
        max={today}
        onChange={(event) => {
          const picked = event.target.value;
          onChange(picked && picked !== today ? picked : null);
        }}
      />
    </label>
  );
}
