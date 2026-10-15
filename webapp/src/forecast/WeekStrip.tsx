import type { Forecast } from "../api";
import { longDay } from "../format";
import { capital, monthName } from "../plan/months";
import {
  dayEvents,
  dayOf,
  isoDay,
  pillUnit,
  pillValue,
  viewCode,
  viewDays,
  weekdayShort,
  type View,
} from "./view";

interface Props {
  data: Forecast;
  view: View;
  start: number; // первый день недели ленты
  selected: number;
  onSelect: (day: number) => void;
}

/** «Октябрь, неделя ‹ ›» и до семи плашек: остаток на вечер по плану и точки событий. */
export default function WeekStrip({
  data,
  view,
  start,
  selected,
  onSelect,
}: Props) {
  const today = dayOf(data.today);
  const end = data.end ? dayOf(data.end) : today;
  const code = viewCode(view);
  const days = viewDays(data, view).filter(
    (point) => point.day >= start && point.day < start + 7,
  );
  return (
    <section className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <h2 className="font-semibold">
          {`${capital(monthName(data.today.slice(0, 7)))}, неделя`}
        </h2>
        <div className="flex gap-1">
          <button
            type="button"
            aria-label="Предыдущая неделя"
            className="px-3 text-xl leading-none text-link disabled:opacity-30"
            disabled={start <= today}
            onClick={() => onSelect(start - 7)}
          >
            ‹
          </button>
          <button
            type="button"
            aria-label="Следующая неделя"
            className="px-3 text-xl leading-none text-link disabled:opacity-30"
            disabled={start + 7 > end}
            onClick={() => onSelect(start + 7)}
          >
            ›
          </button>
        </div>
      </div>
      <div className="grid grid-cols-7 gap-1">
        {days.map((point) => {
          const iso = isoDay(data.today, point.day);
          const on = point.day === selected;
          return (
            <button
              key={point.day}
              type="button"
              aria-pressed={on}
              aria-label={`${longDay(iso)}: вечером ${pillValue(point.plan, code)} ${pillUnit(code)}`}
              className={`flex flex-col items-center rounded-xl px-0.5 py-2 ${
                on ? "bg-button text-button-text" : "bg-section"
              }`}
              onClick={() => onSelect(point.day)}
            >
              <span
                className={`text-[11px] ${on ? "opacity-80" : "text-hint"}`}
              >
                {point.day === today ? "сег" : weekdayShort(iso)}
              </span>
              <span className="text-lg leading-tight font-semibold">
                {point.day}
              </span>
              <span
                className={`text-[11px] font-semibold ${
                  point.plan < 0 && !on ? "text-danger" : ""
                }`}
              >
                {pillValue(point.plan, code)}
              </span>
              <span className="mt-0.5 flex h-1.5 gap-0.5">
                {dayEvents(data, view, point.day).map((event, index) => (
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
      <p className="text-xs text-hint">
        {`Под датой — сколько будет вечером, ${pillUnit(code)} (по плану)`}
      </p>
    </section>
  );
}
