import type { Forecast } from "../api";
import { longDay } from "../format";
import { REPEAT_NAMES, dayRecords, groupTitle, ruleText } from "./records";
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
  onAdd?: (day: number) => void; // нет — справочник не загрузился, записи только смотреть
  onEdit?: (id: string) => void;
}

/** Выбранный день: остаток на вечер по плану и по темпу, события, записи, обычные траты. */
export default function DayCard({ data, view, day, onAdd, onEdit }: Props) {
  const point = viewDays(data, view).find((item) => item.day === day);
  if (!point) return null;
  const code = viewCode(view);
  const usual = ordinary(data, view, day);
  const records = dayRecords(data, view, day);
  const iso = isoDay(data.today, day);
  return (
    <section className="flex flex-col gap-1 rounded-2xl bg-section p-4">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">{dayTitle(iso)}</h2>
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
      {records.length > 0 && (
        <div className="mt-2 flex flex-col">
          <h3 className="text-xs text-hint">Записи на день</h3>
          <ul className="flex flex-col">
            {records.map((record) => {
              const rule = data.calendar.find((item) => item.id === record.id);
              return (
                <li key={`${record.currency}-${record.id}`}>
                  <button
                    type="button"
                    className="flex w-full items-start gap-2 py-1.5 text-left text-sm disabled:opacity-100"
                    disabled={!onEdit}
                    onClick={() => onEdit?.(record.id)}
                  >
                    <span
                      className={
                        record.repeat === "once" ? "text-over" : "text-link"
                      }
                      aria-hidden="true"
                    >
                      {record.repeat === "once" ? "▼" : "↻"}
                    </span>
                    <span className="flex flex-1 flex-col">
                      <span className="flex flex-wrap items-center gap-1">
                        {record.name}
                        {record.over && (
                          <span className="rounded-md bg-warn/20 px-1.5 text-xs text-text">
                            сверх плана
                          </span>
                        )}
                      </span>
                      <span className="text-xs text-hint">
                        {`${groupTitle(record.group)} / ${record.subcategory} · ${
                          rule
                            ? ruleText(rule.repeat, rule.weekdays, rule.date)
                            : REPEAT_NAMES[record.repeat].toLowerCase()
                        }`}
                      </span>
                    </span>
                    <span
                      className={
                        record.done
                          ? "text-xs text-hint"
                          : "font-semibold whitespace-nowrap"
                      }
                    >
                      {record.done
                        ? `✓ потрачено ${amount(record.amount, record.currency)}`
                        : signed(-record.open, record.currency)}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
      )}
      {onAdd && (
        <button
          type="button"
          className="mt-2 rounded-xl border border-dashed border-link/50 py-2 font-medium text-link"
          onClick={() => onAdd(day)}
        >
          {`＋ Запланировать на ${longDay(iso)}`}
        </button>
      )}
    </section>
  );
}
