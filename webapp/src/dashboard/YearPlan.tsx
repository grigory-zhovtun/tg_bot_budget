import { useState } from "react";
import type { PiggyPlanMonth } from "../api";
import { short } from "../format";
import { MONTH_NAMES, monthIndex } from "./piggyText";

const SHOWN = 3; // видны три месяца, «Весь год» раскрывает остальные

interface Props {
  plan: PiggyPlanMonth[];
  names: Record<string, string>;
}

function line(month: PiggyPlanMonth, names: Record<string, string>): string {
  const goals = month.goals.reduce((sum, goal) => sum + goal.amount, 0);
  const parts = [
    ...month.items.map(
      (item) => `${names[item.id] ?? item.id} ${short(item.amount)}`,
    ),
    ...(month.cushion > 0 ? [`подушка ${short(month.cushion)}`] : []),
    ...month.goals.map(
      (goal) => `${names[goal.id] ?? goal.id} ${short(goal.amount)}`,
    ),
  ];
  const total = month.total + month.cushion + goals;
  const name = MONTH_NAMES[monthIndex(month.month)] ?? month.month;
  return `${name} — ${short(total)}${parts.length ? `: ${parts.join(" · ")}` : ""}`;
}

/** «План на год»: сколько и куда откладывать по месяцам, что к какому собрано. */
export default function YearPlan({ plan, names }: Props) {
  const [whole, setWhole] = useState(false);
  const shown = whole ? plan : plan.slice(0, SHOWN);
  return (
    <section aria-label="План на год" className="flex flex-col gap-2">
      <h2 className="font-semibold">План на год</h2>
      <ul className="flex flex-col gap-2 text-sm">
        {shown.map((month) => (
          <li key={month.month}>
            <p>{line(month, names)}</p>
            {month.ready.length > 0 && (
              <p className="font-semibold text-ok">
                {`✓ ${month.ready.map((id) => names[id] ?? id).join(", ")}`}
              </p>
            )}
          </li>
        ))}
      </ul>
      {plan.length > SHOWN && (
        <button
          type="button"
          className="self-start text-link"
          aria-expanded={whole}
          onClick={() => setWhole(!whole)}
        >
          {whole ? "Свернуть" : "Весь год"}
        </button>
      )}
    </section>
  );
}
