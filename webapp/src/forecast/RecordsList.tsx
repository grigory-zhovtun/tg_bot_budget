import type { CalendarRecord, Forecast } from "../api";
import { dayLabel } from "../format";
import { groupTitle, ruleText } from "./records";
import { amount, dayOf, shortDate } from "./view";

interface Props {
  data: Forecast;
  onAdd?: () => void; // нет — справочник не загрузился, записи только смотреть
  onEdit?: (id: string) => void;
}

/** «24.10» — день и месяц записи, даже если она в следующем месяце. */
function dayMonth(iso: string): string {
  return `${iso.slice(8, 10)}.${iso.slice(5, 7)}`;
}

function OverMark({ record }: { record: CalendarRecord }) {
  return record.over ? (
    <span className="rounded-md bg-warn/20 px-1.5 text-xs text-text">
      сверх плана
    </span>
  ) : null;
}

/** «Записи»: повторы с суммой до конца месяца и разовые с сегодня. */
export default function RecordsList({ data, onAdd, onEdit }: Props) {
  const end = data.end ?? data.today;
  const repeats = data.calendar.filter((record) => record.repeat !== "once");
  const once = data.calendar.filter((record) => record.repeat === "once");
  const row =
    "flex w-full items-start gap-2 py-2 text-left disabled:opacity-100";
  return (
    <section className="flex flex-col gap-1">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">Записи</h2>
        {onAdd && (
          <button type="button" className="text-link" onClick={onAdd}>
            ＋ Новая
          </button>
        )}
      </div>
      {data.calendar.length === 0 && (
        <p className="text-sm text-hint">
          Запланируйте траты на дни — прогноз вычтет их в свой день.
        </p>
      )}
      {repeats.length > 0 && (
        <>
          <h3 className="text-xs text-hint">Повторы</h3>
          <ul className="flex flex-col">
            {repeats.map((record) => {
              const later =
                record.date > data.today ? ` с ${dayMonth(record.date)}` : "";
              const month =
                record.month_total > 0
                  ? ` · до ${shortDate(dayOf(end), data.today)} — ${amount(record.month_total, record.currency)}`
                  : "";
              return (
                <li key={record.id}>
                  <button
                    type="button"
                    className={row}
                    disabled={!onEdit}
                    onClick={() => onEdit?.(record.id)}
                  >
                    <span className="flex flex-1 flex-col">
                      <span className="flex flex-wrap items-center gap-1">
                        {record.name || record.subcategory}
                        <OverMark record={record} />
                      </span>
                      <span className="text-xs text-hint">
                        {`${groupTitle(record.group)} / ${record.subcategory} · ${ruleText(record.repeat, record.weekdays, record.date)}${later}${month}`}
                      </span>
                    </span>
                    <span className="font-semibold whitespace-nowrap">
                      {amount(record.amount, record.currency)}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </>
      )}
      {once.length > 0 && (
        <>
          <h3 className="text-xs text-hint">Разовые</h3>
          <ul className="flex flex-col">
            {once.map((record) => {
              const paid = record.date <= end && record.month_total === 0;
              return (
                <li key={record.id}>
                  <button
                    type="button"
                    className={row}
                    disabled={!onEdit}
                    onClick={() => onEdit?.(record.id)}
                  >
                    <span className="w-16 shrink-0 text-sm text-hint">
                      {dayLabel(record.date, data.today)}
                    </span>
                    <span className="flex flex-1 flex-col">
                      <span className="flex flex-wrap items-center gap-1">
                        {record.name || record.subcategory}
                        <OverMark record={record} />
                      </span>
                      <span className="text-xs text-hint">
                        {`${groupTitle(record.group)} / ${record.subcategory}`}
                      </span>
                    </span>
                    <span
                      className={`whitespace-nowrap ${paid ? "text-hint" : "font-semibold"}`}
                    >
                      {`${paid ? "✓ " : ""}${amount(record.amount, record.currency)}`}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </>
      )}
    </section>
  );
}
