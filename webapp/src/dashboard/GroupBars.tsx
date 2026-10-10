import { useState } from "react";
import type { GroupPlan } from "../api";
import { short } from "../format";
import { filled, STATUS_COLOR, statusOf } from "./status";

function amounts(plan: number, fact: number): string {
  return plan > 0
    ? `${short(fact)} из ${short(plan)}`
    : `${short(fact)} вне плана`;
}

function Bar({ plan, fact }: { plan: number; fact: number }) {
  const status = statusOf(plan, fact);
  return (
    <div
      className="h-2 overflow-hidden rounded-full bg-section"
      data-status={status}
    >
      <div
        className={`h-full rounded-full ${STATUS_COLOR[status]}`}
        style={{ width: `${filled(plan, fact) * 100}%` }}
      />
    </div>
  );
}

export default function GroupBars({ groups }: { groups: GroupPlan[] }) {
  const [open, setOpen] = useState<string | null>(null);
  return (
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
            <span className="text-sm text-hint">
              {amounts(group.plan, group.fact)}
            </span>
          </button>
          <Bar plan={group.plan} fact={group.fact} />
          {open === group.name && (
            <ul className="mt-1 flex flex-col gap-2 pl-3">
              {group.items.map((item) => (
                <li key={item.name} className="flex flex-col gap-1 text-sm">
                  <span className="flex justify-between gap-2">
                    <span>
                      {item.icon} {item.name}
                    </span>
                    <span className="text-hint">
                      {amounts(item.plan, item.fact)}
                    </span>
                  </span>
                  <Bar plan={item.plan} fact={item.fact} />
                </li>
              ))}
            </ul>
          )}
        </li>
      ))}
    </ul>
  );
}
