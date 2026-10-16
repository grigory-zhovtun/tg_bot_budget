import type { Forecast, ForecastPast } from "../api";
import { longDay } from "../format";
import { capital, monthName } from "../plan/months";
import { WEEKDAY_NAMES, cellAmount, dayRecordSum, gridWeeks } from "./records";
import {
  amount,
  dayEvents,
  dayOf,
  isoDay,
  pillUnit,
  pillValue,
  viewCode,
  viewDays,
  viewPast,
  type View,
} from "./view";

interface Props {
  data: Forecast;
  view: View;
  selected: number;
  onSelect: (day: number) => void;
}

/** Клетка дня: жирные цифры, рамка 2 px — крупно и контрастно. */
const CELL =
  "flex flex-col items-center gap-0.5 rounded-xl border-2 px-0.5 py-1.5";

/** Сумма в клетке: «412к», «1,8м»; ноль после округления — null. */
function cellSum(value: number, code: string): string | null {
  return Math.round(value) > 0 ? cellAmount(value, code) : null;
}

/** Фон клетки: выбранная — кнопкой, минус на вечер — красной, остальное — как задано. */
function cellColors(on: boolean, minus: boolean, usual: string): string {
  if (on) return "border-button bg-button text-button-text";
  return minus ? "border-minus bg-minus text-white" : usual;
}

interface PastProps {
  iso: string;
  point: ForecastPast;
  code: string;
  on: boolean;
  onSelect: () => void;
}

/** Прошедший день по факту, в рамке: сколько было вечером, траты и доходы дня врозь. */
function PastCell({ iso, point, code, on, onSelect }: PastProps) {
  const spent = cellSum(point.spent, code);
  const earned = cellSum(point.earned, code);
  const minus = point.evening < 0;
  const plain = on || minus; // на кнопке и красной клетке цифры белые
  const label = [
    `${longDay(iso)}: вечером было ${pillValue(point.evening, code)} ${pillUnit(code)}`,
    spent && `потрачено ${amount(point.spent, code)}`,
    earned && `пришло ${amount(point.earned, code)}`,
  ]
    .filter(Boolean)
    .join(", ");
  return (
    <button
      type="button"
      aria-pressed={on}
      aria-label={label}
      data-minus={minus || undefined}
      className={`${CELL} ${cellColors(on, minus, "border-outline")}`}
      onClick={onSelect}
    >
      <span className="text-base leading-tight font-bold">
        {Number(iso.slice(8, 10))}
      </span>
      <span className="text-[13px] font-bold">
        {pillValue(point.evening, code)}
      </span>
      {spent && (
        <span className={`text-sm font-extrabold ${plain ? "" : "text-spent"}`}>
          {`−${spent}`}
        </span>
      )}
      {earned && (
        <span
          className={`text-sm font-extrabold ${plain ? "" : "text-earned"}`}
        >
          {`+${earned}`}
        </span>
      )}
    </button>
  );
}

/** Месяц с 1-го числа: прошедшие дни по факту, дальше остаток на вечер по плану, записи дня и точки списка. */
export default function MonthGrid({ data, view, selected, onSelect }: Props) {
  const today = dayOf(data.today);
  const code = viewCode(view);
  const evenings = new Map(
    viewDays(data, view).map((point) => [point.day, point.plan]),
  );
  const past = new Map(viewPast(data, view).map((point) => [point.day, point]));
  return (
    <section className="flex flex-col gap-1">
      <h2 className="font-semibold">
        {capital(monthName(data.today.slice(0, 7)))}
      </h2>
      <div
        className="grid grid-cols-7 gap-1 text-center text-[13px] font-bold"
        aria-hidden="true"
      >
        {WEEKDAY_NAMES.map((name) => (
          <span key={name}>{name}</span>
        ))}
      </div>
      {gridWeeks(isoDay(data.today, 1), data.end ?? data.today).map((week) => (
        <div key={week.join("-")} className="grid grid-cols-7 gap-1">
          {week.map((day, index) => {
            const fact =
              day === null || day >= today ? undefined : past.get(day);
            if (day !== null && fact) {
              return (
                <PastCell
                  key={day}
                  iso={isoDay(data.today, day)}
                  point={fact}
                  code={code}
                  on={day === selected}
                  onSelect={() => onSelect(day)}
                />
              );
            }
            const evening = day === null ? undefined : evenings.get(day);
            if (day === null || day < today || evening === undefined) {
              return (
                <span
                  key={day ?? `empty-${index}`}
                  className="py-1.5 text-center text-base text-hint opacity-50"
                >
                  {day ?? ""}
                </span>
              );
            }
            const on = day === selected;
            const minus = evening < 0;
            const plain = on || minus;
            const records = cellSum(dayRecordSum(data, view, day), code);
            const label = `${longDay(isoDay(data.today, day))}: вечером ${pillValue(evening, code)} ${pillUnit(code)}${
              records ? `, записи −${records}` : ""
            }`;
            return (
              <button
                key={day}
                type="button"
                aria-pressed={on}
                aria-current={day === today ? "date" : undefined}
                aria-label={label}
                data-minus={minus || undefined}
                className={`${CELL} ${cellColors(on, minus, "border-section bg-section")} ${
                  day === today && !on ? "ring-2 ring-link ring-inset" : ""
                }`}
                onClick={() => onSelect(day)}
              >
                <span className="text-base leading-tight font-bold">{day}</span>
                <span className="text-[13px] font-bold">
                  {pillValue(evening, code)}
                </span>
                {records && (
                  <span
                    className={`text-sm font-extrabold ${plain ? "" : "text-planned"}`}
                  >
                    {`−${records}`}
                  </span>
                )}
                <span className="mt-auto flex h-2 gap-1 pt-0.5">
                  {dayEvents(data, view, day).map((event, index) => (
                    <i
                      key={`${event.currency}-${event.name}-${index}`}
                      className={`inline-block size-2 rounded-full ${
                        event.amount > 0 ? "bg-earned" : "bg-payment"
                      }`}
                    />
                  ))}
                </span>
              </button>
            );
          })}
        </div>
      ))}
      <div className="mt-1 flex flex-col gap-1 text-[13px] font-semibold">
        <p>
          {`Остаток на вечер, ${pillUnit(code)}: прошедшие дни — по факту, в рамке; дальше — по плану`}
        </p>
        <ul className="flex flex-wrap gap-x-4 gap-y-1">
          <li>
            <b className="font-extrabold text-spent">−</b> потрачено за день
          </li>
          <li>
            <b className="font-extrabold text-earned">+</b> пришло за день
          </li>
          <li>
            <b className="font-extrabold text-planned">−</b> запись на день
          </li>
          <li>
            <i
              className="inline-block size-3 rounded-sm bg-minus align-[-1px]"
              aria-hidden="true"
            />{" "}
            минус на вечер
          </li>
        </ul>
      </div>
    </section>
  );
}
