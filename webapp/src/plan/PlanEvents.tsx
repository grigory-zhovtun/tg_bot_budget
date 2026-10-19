import { signedMoney } from "../format";

export interface EventRow {
  slot: number;
  name: string;
  day: number | null;
  amount: number;
  currency: string;
  edited: boolean; // правка в черновике — название цветом ссылки
  conflict: boolean; // строку поменяли в таблице — рамкой
  note: string | null; // что по строке пришло
}

// внутри кнопки: рамка списка (overflow-hidden) не срезает кольцо
const RING = "rounded-lg ring-2 ring-inset ring-warn";

/**
 * Строки списка поступлений и платежей — пункты в две строки, как «Впереди» в
 * «Сводке», и кнопки правки. Без полос: цвет ссылки у исправленной строки на
 * полосе не держит 7:1.
 */
export default function PlanEvents({
  rows,
  onPick,
}: {
  rows: EventRow[];
  onPick: (slot: number) => void;
}) {
  if (rows.length === 0) return null;
  return (
    <ul className="flex flex-col overflow-hidden rounded-xl border border-outline text-line">
      {rows.map((row) => (
        <li key={row.slot} className="border-b border-outline last:border-b-0">
          <button
            type="button"
            data-conflict={row.conflict || undefined}
            className={`flex w-full flex-col gap-0.5 px-3 py-2 text-left ${row.conflict ? RING : ""}`}
            onClick={() => onPick(row.slot)}
          >
            <span className={`font-extrabold ${row.edited ? "text-link" : ""}`}>
              {row.name}
            </span>
            <span className="flex w-full items-baseline justify-between gap-3">
              <span>{row.day !== null ? `${row.day}-го` : ""}</span>
              <span className="font-extrabold whitespace-nowrap tabular-nums">
                {signedMoney(row.amount, row.currency)}
              </span>
            </span>
            {row.note && <span>{row.note}</span>}
          </button>
        </li>
      ))}
    </ul>
  );
}
