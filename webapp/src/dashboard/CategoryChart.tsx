import { useState } from "react";
import type { Category, Untracked } from "../api";
import { longDay, money } from "../format";
import { Card, Chip, Fact, Line, Table, Warn } from "./blocks";
import { STATUS_COLOR, statusOf } from "./status";

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

/** «3 342 000 из 3 000 000» или «600 000 вне плана»; без плана и трат — ничего. */
function amounts(plan: number, fact: number): string | null {
  if (plan > 0) return `${money(fact)} из ${money(plan)}`;
  return fact > 0 ? `${money(fact)} вне плана` : null;
}

const TONE = { ok: "good", warn: "mid", over: "bad" } as const;
const SIGN = { ok: "✓ ", warn: "", over: "⚠️ " } as const;

/** Плашка «доля плана»: зелёная до 80 %, жёлтая до 100 %, красная сверх; без плана — нет. */
function Share({
  plan,
  fact,
  small = false,
}: {
  plan: number;
  fact: number;
  small?: boolean;
}) {
  const status = statusOf(plan, fact);
  if (status === "off") return null;
  const share = Math.round((fact / plan) * 100);
  return (
    <Chip tone={TONE[status]} small={small}>
      {`${SIGN[status]}${share}\u00a0%`}
    </Chip>
  );
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
      className={`relative rounded-full bg-stripe ${thin ? "h-2" : "h-3"}`}
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
          className="absolute -inset-y-1 w-1 -translate-x-1/2 rounded-full bg-text"
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

/** Статьи без строки во вкладке месяца: их траты в план-факт таблицы не попали. */
function NotInPlan({ items }: { items: Untracked[] }) {
  return (
    <Card label="Нет в плане месяца">
      <Warn>Нет в плане месяца</Warn>
      <Table
        head={["статья", "не учтено"]}
        align="lr"
        rows={items.map((item) => [
          `${item.subcategory} (${item.currency})`,
          money(item.amount),
        ])}
      />
      <Line>добавьте во вкладке «План»</Line>
    </Card>
  );
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
      className="flex flex-col gap-3"
      aria-label="План-факт по категориям"
    >
      <h2 className="text-fact font-extrabold">План-факт по категориям</h2>
      <Card>
        <Fact>{`Потрачено ${money(fact)}`}</Fact>
        <Line>{period(today)}</Line>
        <Line>{`план на месяц ${money(plan)}`}</Line>
      </Card>
      <ul className="flex flex-col gap-4">
        {categories.map((group) => {
          const line = amounts(group.plan, group.fact);
          return (
            <li key={group.name} className="flex flex-col gap-1.5">
              <button
                type="button"
                aria-expanded={open === group.name}
                className="flex flex-wrap items-center justify-between gap-x-2 gap-y-1 text-left"
                onClick={() => setOpen(open === group.name ? null : group.name)}
              >
                <span className="text-fact font-extrabold">{group.name}</span>
                <Share plan={group.plan} fact={group.fact} />
              </button>
              <Bar plan={group.plan} fact={group.fact} scale={scale} />
              {line && <Line>{line}</Line>}
              {open === group.name && (
                <ul className="mt-1 flex flex-col gap-3 border-l-4 border-outline pl-3">
                  {group.items.map((item) => {
                    const itemLine = amounts(item.plan, item.fact);
                    return (
                      <li key={item.name} className="flex flex-col gap-1">
                        <span className="flex flex-wrap items-center justify-between gap-x-2 gap-y-1">
                          <span className="text-line font-extrabold">
                            {item.icon
                              ? `${item.icon} ${item.name}`
                              : item.name}
                          </span>
                          <Share plan={item.plan} fact={item.fact} small />
                        </span>
                        <Bar
                          plan={item.plan}
                          fact={item.fact}
                          scale={longest(group.items)}
                          thin
                        />
                        {itemLine && <Line>{itemLine}</Line>}
                      </li>
                    );
                  })}
                </ul>
              )}
            </li>
          );
        })}
      </ul>
      {untracked.length > 0 && <NotInPlan items={untracked} />}
    </section>
  );
}
