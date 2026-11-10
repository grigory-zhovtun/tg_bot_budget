import { capital, monthName, nextMonth, prevMonth } from "../plan/months";

interface Props {
  month: string; // «YYYY-MM» — месяц на экране
  current: string; // текущий месяц: дальше него листать некуда
  earliest: string | null; // первый месяц с операциями; null — ещё не знаем
  onMonth: (month: string) => void;
}

/** Стрелка листания: крупная, в рамке; недоступная — бледная. */
const ARROW =
  "grid size-11 shrink-0 place-items-center rounded-xl border-2 border-outline disabled:opacity-30";

function Chevron({ back }: { back: boolean }) {
  return (
    <svg
      viewBox="0 0 24 24"
      className="size-6"
      fill="none"
      stroke="currentColor"
      strokeWidth="3.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d={back ? "M15 5l-7 7 7 7" : "M9 5l7 7-7 7"} />
    </svg>
  );
}

/** «Октябрь», в другом году — «Декабрь 2025». */
export function monthTitle(month: string, current: string): string {
  const name = capital(monthName(month));
  return month.slice(0, 4) === current.slice(0, 4)
    ? name
    : `${name} ${month.slice(0, 4)}`;
}

/**
 * Листание месяцев календаря: назад — до первого месяца с операциями, вперёд —
 * до текущего. В прошедшем месяце — возврат к текущему одной кнопкой.
 */
export default function MonthNav({ month, current, earliest, onMonth }: Props) {
  const back = earliest === null || month > earliest;
  const forward = month < current;
  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between gap-2">
        <button
          type="button"
          className={ARROW}
          aria-label="Предыдущий месяц"
          disabled={!back}
          onClick={() => onMonth(prevMonth(month))}
        >
          <Chevron back />
        </button>
        <h2 className="text-center text-xl font-extrabold" aria-live="polite">
          {monthTitle(month, current)}
        </h2>
        <button
          type="button"
          className={ARROW}
          aria-label="Следующий месяц"
          disabled={!forward}
          onClick={() => onMonth(nextMonth(month))}
        >
          <Chevron back={false} />
        </button>
      </div>
      {forward && (
        <button
          type="button"
          className="min-h-11 self-center px-3 font-bold text-link"
          onClick={() => onMonth(current)}
        >
          {`Вернуться в ${monthName(current)}`}
        </button>
      )}
    </div>
  );
}
