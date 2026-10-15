import type { Forecast } from "../api";
import { longDay } from "../format";
import { capital, monthName } from "../plan/months";
import { WEEKDAY_NAMES, cellAmount, dayRecordSum, gridWeeks } from "./records";
import {
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

/** Расход или доход дня в клетке: «−410к», «+3,9м»; ноль — null. */
function changeText(change: number, code: string): string | null {
  if (Math.round(Math.abs(change)) === 0) return null;
  return `${change > 0 ? "+" : "−"}${cellAmount(Math.abs(change), code)}`;
}

interface PastProps {
  iso: string;
  evening: number;
  change: number;
  code: string;
  on: boolean;
  onSelect: () => void;
}

/** Прошедший день по факту: сколько было вечером и расход или доход за день. */
function PastCell({ iso, evening, change, code, on, onSelect }: PastProps) {
  const moved = changeText(change, code);
  return (
    <button
      type="button"
      aria-pressed={on}
      aria-label={`${longDay(iso)}: вечером было ${pillValue(evening, code)} ${pillUnit(code)}${
        moved ? `, за день ${moved}` : ""
      }`}
      className={`flex flex-col items-center rounded-xl border px-0.5 py-1.5 ${
        on ? "border-button bg-button text-button-text" : "border-section"
      }`}
      onClick={onSelect}
    >
      <span className="text-base leading-tight font-semibold">
        {Number(iso.slice(8, 10))}
      </span>
      <span
        className={`text-[11px] font-semibold ${evening < 0 && !on ? "text-danger" : ""}`}
      >
        {pillValue(evening, code)}
      </span>
      {moved && (
        <span
          className={`text-[11px] font-semibold ${
            on ? "" : change > 0 ? "text-ok" : "text-hint"
          }`}
        >
          {moved}
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
        className="grid grid-cols-7 gap-1 text-center text-[11px] text-hint"
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
                  evening={fact.evening}
                  change={fact.change}
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
            const booked = dayRecordSum(data, view, day);
            const records = Math.round(booked) > 0;
            const label = `${longDay(isoDay(data.today, day))}: вечером ${pillValue(evening, code)} ${pillUnit(code)}${
              records ? `, записи −${cellAmount(booked, code)}` : ""
            }`;
            return (
              <button
                key={day}
                type="button"
                aria-pressed={on}
                aria-current={day === today ? "date" : undefined}
                aria-label={label}
                className={`flex flex-col items-center rounded-xl px-0.5 py-1.5 ${
                  on ? "bg-button text-button-text" : "bg-section"
                } ${day === today && !on ? "ring-2 ring-link ring-inset" : ""}`}
                onClick={() => onSelect(day)}
              >
                <span className="text-base leading-tight font-semibold">
                  {day}
                </span>
                <span
                  className={`text-[11px] font-semibold ${
                    evening < 0 && !on ? "text-danger" : ""
                  }`}
                >
                  {pillValue(evening, code)}
                </span>
                {records && (
                  <span
                    className={`text-[11px] font-semibold ${on ? "" : "text-danger"}`}
                  >
                    {`−${cellAmount(booked, code)}`}
                  </span>
                )}
                <span className="mt-auto flex h-1.5 gap-0.5 pt-0.5">
                  {dayEvents(data, view, day).map((event, index) => (
                    <i
                      key={`${event.currency}-${event.name}-${index}`}
                      className={`inline-block size-1.5 rounded-full ${
                        event.amount > 0 ? "bg-ok" : "bg-over"
                      }`}
                    />
                  ))}
                </span>
              </button>
            );
          })}
        </div>
      ))}
      <p className="text-xs text-hint">
        {`В клетке — остаток на вечер, ${pillUnit(code)}: прошедшие дни — по факту, с расходом или доходом за день; дальше — по плану, с записями дня`}
      </p>
    </section>
  );
}
