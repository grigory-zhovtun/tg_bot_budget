import { useState } from "react";
import type { SpentGroup, Untracked } from "../api";
import { longDay, money, short } from "../format";

/** Доля от целого: «31 %»; ненулевая доля меньше процента — «<1 %». */
function percent(part: number, whole: number): string {
  const value = whole > 0 ? Math.round((part / whole) * 100) : 0;
  return value === 0 && part > 0 ? "<1 %" : `${value} %`;
}

/** Ширина полосы от самой длинной, до десятой процента: «43.8%». */
function width(part: number, longest: number): string {
  const share = longest > 0 ? Math.min(Math.max(part, 0) / longest, 1) : 0;
  return `${Math.round(share * 1000) / 10}%`;
}

/** «за 1–15 октября»; 1-го числа — «за 1 октября». */
function period(today: string): string {
  return today.endsWith("-01")
    ? `за ${longDay(today)}`
    : `за 1–${longDay(today)}`;
}

/** Статьи без строки во вкладке месяца: их траты в план-факт не попали. */
function untrackedText(items: Untracked[]): string {
  const list = items
    .map(
      (item) =>
        `${item.group} / ${item.subcategory} (${item.currency}) — ${money(item.amount)} сум`,
    )
    .join("; ");
  return `⚠️ Во вкладке месяца не учтено: ${list}. Добавьте статью во вкладке «План».`;
}

interface Props {
  groups: SpentGroup[]; // от большей к меньшей
  untracked: Untracked[];
  today: string;
}

/** Расходы месяца по категориям: полосы от самой большой группы, касание — подкатегории. */
export default function SpentBars({ groups, untracked, today }: Props) {
  const [open, setOpen] = useState<string | null>(null);
  const total = groups.reduce((sum, group) => sum + group.amount, 0);
  const longest = Math.max(0, ...groups.map((group) => group.amount));
  return (
    <section className="flex flex-col gap-2" aria-label="Расходы по категориям">
      <h2 className="font-semibold">Расходы по категориям</h2>
      <p className="text-sm">
        <span className="font-semibold">{short(total)}</span>
        <span className="text-hint"> {period(today)}</span>
      </p>
      <ul className="flex flex-col gap-3">
        {groups.map((group) => (
          <li key={group.name} className="flex flex-col gap-1">
            <button
              type="button"
              aria-expanded={open === group.name}
              className="flex items-baseline justify-between gap-2 text-left"
              onClick={() => setOpen(open === group.name ? null : group.name)}
            >
              <span className="font-medium">{group.name}</span>
              <span className="text-sm whitespace-nowrap text-hint">
                {`${short(group.amount)} · ${percent(group.amount, total)}`}
              </span>
            </button>
            <div
              className="h-2 overflow-hidden rounded-full bg-section"
              aria-hidden="true"
            >
              <div
                data-bar
                className="h-full min-w-1 rounded-full bg-button"
                style={{ width: width(group.amount, longest) }}
              />
            </div>
            {open === group.name && (
              <ul className="mt-1 flex flex-col gap-2 pl-3">
                {group.items.map((item) => (
                  <li key={item.name} className="flex flex-col gap-1 text-sm">
                    <span className="flex justify-between gap-2">
                      <span>
                        {item.icon ? `${item.icon} ${item.name}` : item.name}
                      </span>
                      <span className="whitespace-nowrap text-hint">
                        {short(item.amount)}
                      </span>
                    </span>
                    <div
                      className="h-1.5 overflow-hidden rounded-full bg-section"
                      aria-hidden="true"
                    >
                      <div
                        className="h-full rounded-full bg-button opacity-60"
                        style={{ width: width(item.amount, group.amount) }}
                      />
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </li>
        ))}
      </ul>
      {untracked.length > 0 && (
        <p className="text-sm text-danger">{untrackedText(untracked)}</p>
      )}
    </section>
  );
}
