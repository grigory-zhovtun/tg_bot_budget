import type { Forecast } from "../api";
import {
  amount,
  dayEvents,
  dayTitle,
  isoDay,
  ordinary,
  signed,
  viewCode,
  viewDays,
  type View,
} from "./view";

interface Props {
  data: Forecast;
  view: View;
  day: number;
}

/** Выбранный день: остаток на вечер по плану и по темпу, события дня, обычные траты. */
export default function DayCard({ data, view, day }: Props) {
  const point = viewDays(data, view).find((item) => item.day === day);
  if (!point) return null;
  const code = viewCode(view);
  const usual = ordinary(data, view, day);
  return (
    <section className="flex flex-col gap-1 rounded-2xl bg-section p-4">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">{dayTitle(isoDay(data.today, day))}</h2>
        <span className="text-xs text-hint">вечером</span>
      </div>
      <p className="flex justify-between gap-2">
        <span>
          <span className="text-link">━</span> по плану
        </span>
        <span
          className={`font-semibold ${point.plan < 0 ? "text-danger" : ""}`}
        >
          {amount(point.plan, code)}
        </span>
      </p>
      <p className="flex justify-between gap-2">
        <span>
          <span className="text-warn">╍</span> по темпу
        </span>
        <span
          className={`font-semibold ${point.pace < 0 ? "text-danger" : ""}`}
        >
          {amount(point.pace, code)}
        </span>
      </p>
      <ul className="mt-1 flex flex-col gap-0.5 text-sm">
        {dayEvents(data, view, day).map((event, index) => (
          <li
            key={`${event.currency}-${event.name}-${index}`}
            className="flex justify-between gap-2"
          >
            <span className="flex gap-1">
              {event.late && <span>⏳</span>}
              <span className={event.amount > 0 ? "text-ok" : "text-over"}>
                {event.amount > 0 ? "▲" : "▼"}
              </span>
              <span>{event.name}</span>
            </span>
            <span className={event.amount > 0 ? "text-ok" : ""}>
              {signed(event.amount, event.currency)}
            </span>
          </li>
        ))}
      </ul>
      {usual && (
        <p className="text-xs text-hint">{`обычные траты ${usual} (по плану)`}</p>
      )}
    </section>
  );
}
