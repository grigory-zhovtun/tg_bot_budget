import { useState } from "react";
import type { Category, Untracked } from "../api";
import { longDay, money, short } from "../format";
import { STATUS_COLOR, statusOf } from "./status";

/** Доля от целого: «31 %»; ненулевая доля меньше процента — «<1 %». */
function percent(part: number, whole: number): string {
  const value = whole > 0 ? Math.round((part / whole) * 100) : 0;
  return value === 0 && part > 0 ? "<1 %" : `${value} %`;
}

/** Место на шкале, до десятой процента: «58.3%». */
function position(part: number, scale: number): string {
  const share = scale > 0 ? Math.min(Math.max(part, 0) / scale, 1) : 0;
  return `${Math.round(share * 1000) / 10}%`;
}

/** «за 1–15 октября»; 1-го числа — «за 1 октября». */
function period(today: string): string {
  return today.endsWith("-01")
    ? `за ${longDay(today)}`
    : `за 1–${longDay(today)}`;
}

/** «3,30 млн из 3,00 млн» или «600 000 вне плана». */
function amounts(plan: number, fact: number): string {
  return plan > 0
    ? `${short(fact)} из ${short(plan)}`
    : `${short(fact)} вне плана`;
}

/** Статьи без строки во вкладке месяца: их траты в план-факт таблицы не попали. */
function untrackedText(items: Untracked[]): string {
  const list = items
    .map(
      (item) =>
        `${item.group} / ${item.subcategory} (${item.currency}) — ${money(item.amount)} сум`,
    )
    .join("; ");
  return `⚠️ Во вкладке месяца не учтено: ${list}. Добавьте статью во вкладке «План».`;
}

interface BarProps {
  plan: number;
  fact: number;
  scale: number;
  thin?: boolean;
}

/** Полоса — сколько потрачено, цвет — по плану статьи; черта — сам план. */
function Bar({ plan, fact, scale, thin = false }: BarProps) {
  const status = statusOf(plan, fact);
  return (
    <div
      className={`relative rounded-full bg-section ${thin ? "h-1.5" : "h-2"}`}
      data-status={status}
      aria-hidden="true"
    >
      {fact > 0 && (
        <div
          data-fact
          className={`h-full min-w-1 rounded-full ${STATUS_COLOR[status]}`}
          style={{ width: position(fact, scale) }}
        />
      )}
      {plan > 0 && (
        <div
          data-plan
          className="absolute -inset-y-0.5 w-0.5 -translate-x-1/2 rounded-full bg-text"
          style={{ left: position(plan, scale) }}
        />
      )}
    </div>
  );
}

/** Самое большое из трат и плана — шкала полос. */
function longest(lines: { plan: number; fact: number }[]): number {
  return Math.max(0, ...lines.map((line) => Math.max(line.plan, line.fact)));
}

interface Props {
  categories: Category[]; // от больших трат к меньшим
  untracked: Untracked[];
  today: string;
}

/** План-факт месяца по категориям: траты и план групп на одной шкале, касание — подкатегории. */
export default function CategoryChart({ categories, untracked, today }: Props) {
  const [open, setOpen] = useState<string | null>(null);
  const fact = categories.reduce((sum, group) => sum + group.fact, 0);
  const plan = categories.reduce((sum, group) => sum + group.plan, 0);
  const scale = longest(categories);
  return (
    <section
      className="flex flex-col gap-2"
      aria-label="План-факт по категориям"
    >
      <h2 className="font-semibold">План-факт по категориям</h2>
      <p className="text-sm">
        <span className="font-semibold">{short(fact)}</span>
        <span className="text-hint">{` из ${short(plan)} ${period(today)}`}</span>
      </p>
      <ul className="flex flex-col gap-3">
        {categories.map((group) => (
          <li key={group.name} className="flex flex-col gap-1">
            <button
              type="button"
              aria-expanded={open === group.name}
              className="flex items-baseline justify-between gap-2 text-left"
              onClick={() => setOpen(open === group.name ? null : group.name)}
            >
              <span className="font-medium">{group.name}</span>
              <span className="text-sm whitespace-nowrap text-hint">
                {amounts(group.plan, group.fact) +
                  (group.fact > 0 ? ` · ${percent(group.fact, fact)}` : "")}
              </span>
            </button>
            <Bar plan={group.plan} fact={group.fact} scale={scale} />
            {open === group.name && (
              <ul className="mt-1 flex flex-col gap-2 pl-3">
                {group.items.map((item) => (
                  <li key={item.name} className="flex flex-col gap-1 text-sm">
                    <span className="flex justify-between gap-2">
                      <span>
                        {item.icon ? `${item.icon} ${item.name}` : item.name}
                      </span>
                      <span className="whitespace-nowrap text-hint">
                        {amounts(item.plan, item.fact)}
                      </span>
                    </span>
                    <Bar
                      plan={item.plan}
                      fact={item.fact}
                      scale={longest(group.items)}
                      thin
                    />
                  </li>
                ))}
              </ul>
            )}
          </li>
        ))}
      </ul>
      <p className="text-xs text-hint">
        Полоса — сколько потрачено, черта — план
      </p>
      {untracked.length > 0 && (
        <p className="text-sm text-danger">{untrackedText(untracked)}</p>
      )}
    </section>
  );
}
