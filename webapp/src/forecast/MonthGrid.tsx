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
  type View,
} from "./view";

interface Props {
  data: Forecast;
  view: View;
  selected: number;
  onSelect: (day: number) => void;
}

/** Месяц с недели сегодня: в клетке остаток на вечер по плану, записи дня и точки списка. */
export default function MonthGrid({ data, view, selected, onSelect }: Props) {
  const today = dayOf(data.today);
  const code = viewCode(view);
  const evenings = new Map(
    viewDays(data, view).map((point) => [point.day, point.plan]),
  );
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
      {gridWeeks(data.today, data.end ?? data.today).map((week) => (
        <div key={week.join("-")} className="grid grid-cols-7 gap-1">
          {week.map((day, index) => {
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
        {`В клетке — сколько будет вечером, ${pillUnit(code)} (по плану), и записи дня`}
      </p>
    </section>
  );
}
