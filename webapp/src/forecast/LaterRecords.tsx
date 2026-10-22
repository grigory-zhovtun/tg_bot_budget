import type { CalendarRecord, Forecast } from "../api";
import { longDay } from "../format";
import { groupTitle, laterRecords, ruleText } from "./records";
import { amount } from "./view";

interface Props {
  data: Forecast;
  onEdit?: (id: string) => void; // нет — справочник не загрузился, только смотреть
}

/** «3 ноября» у разовой; у повтора — правило, «с 5 ноября», если он ещё не начался. */
function whenText(record: CalendarRecord, today: string): string {
  if (record.repeat === "once") return longDay(record.date);
  const rule = ruleText(record.repeat, record.weekdays, record.date);
  return record.date > today ? `${rule} с ${longDay(record.date)}` : rule;
}

/**
 * «Запланировано дальше»: записи, которых нет в карточках дней этого месяца, —
 * разовые позже месяца, повторы без дней до его конца, валюта без карты; нажатие
 * открывает запись. Таких нет — блока нет.
 */
export default function LaterRecords({ data, onEdit }: Props) {
  const later = laterRecords(data);
  if (later.length === 0) return null;
  return (
    <section
      aria-label="Запланировано дальше"
      className="flex flex-col gap-1 rounded-2xl bg-section p-4"
    >
      <h2 className="font-semibold">Запланировано дальше</h2>
      <ul className="flex flex-col">
        {later.map((record) => (
          <li key={record.id}>
            <button
              type="button"
              className="flex w-full items-start gap-2 py-1.5 text-left text-sm disabled:opacity-100"
              disabled={!onEdit}
              onClick={() => onEdit?.(record.id)}
            >
              <span className="flex flex-1 flex-col">
                <span>{record.name || record.subcategory}</span>
                <span className="text-xs text-hint">
                  {`${groupTitle(record.group)} / ${record.subcategory} · ${whenText(record, data.today)}`}
                </span>
              </span>
              <span className="font-bold whitespace-nowrap text-planned">
                {amount(record.amount, record.currency)}
              </span>
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
