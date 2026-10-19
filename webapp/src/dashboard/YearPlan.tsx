import { useState } from "react";
import type { PiggyPlanMonth } from "../api";
import { money } from "../format";
import { Fact, Good, Line } from "./blocks";
import { MONTH_NAMES, monthIndex } from "./piggyText";

const SHOWN = 3; // видны три месяца, «Весь год» раскрывает остальные

interface Props {
  plan: PiggyPlanMonth[];
  names: Record<string, string>;
}

/** Части взноса месяца: копилки, подушка и цели — по строке на каждую. */
function parts(
  month: PiggyPlanMonth,
  names: Record<string, string>,
): [string, number][] {
  return [
    ...month.items.map((item): [string, number] => [
      names[item.id] ?? item.id,
      item.amount,
    ]),
    ...(month.cushion > 0
      ? [["подушка", month.cushion] as [string, number]]
      : []),
    ...month.goals.map((goal): [string, number] => [
      names[goal.id] ?? goal.id,
      goal.amount,
    ]),
  ];
}

/** «План на год»: сколько и куда откладывать по месяцам, что к какому собрано. */
export default function YearPlan({ plan, names }: Props) {
  const [whole, setWhole] = useState(false);
  const shown = whole ? plan : plan.slice(0, SHOWN);
  return (
    <section aria-label="План на год" className="flex flex-col gap-2">
      <h2 className="text-fact font-extrabold">План на год</h2>
      <ul className="flex flex-col gap-3 rounded-2xl bg-section p-4">
        {shown.map((month) => {
          const goals = month.goals.reduce((sum, goal) => sum + goal.amount, 0);
          const total = month.total + month.cushion + goals;
          const name = MONTH_NAMES[monthIndex(month.month)] ?? month.month;
          return (
            <li key={month.month} className="flex flex-col gap-0.5">
              <Fact>{`${name} — ${money(total)}`}</Fact>
              {parts(month, names).map(([label, amount], index) => (
                <Line key={index}>{`${label} ${money(amount)}`}</Line>
              ))}
              {month.ready.map((id) => (
                <Good key={id}>{`${names[id] ?? id} — собрано`}</Good>
              ))}
            </li>
          );
        })}
      </ul>
      {plan.length > SHOWN && (
        <button
          type="button"
          className="self-start text-line font-extrabold text-link"
          aria-expanded={whole}
          onClick={() => setWhole(!whole)}
        >
          {whole ? "Свернуть" : "Весь год"}
        </button>
      )}
    </section>
  );
}
