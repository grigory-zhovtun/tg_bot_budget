import { useId } from "react";
import type { Group, Subcategory } from "../api";
import { STATUS_COLOR } from "../dashboard/status";
import { tick } from "../telegram";
import { groupColor, tint } from "./palette";
import { planLine, type PlanFact, type PlanIndex } from "./plan";

interface TileProps {
  emoji: string;
  title: string;
  color: string;
  plan: PlanFact | undefined;
  minHeight: number;
  onPick: () => void;
}

/** Крупная плитка в цвете группы: эмодзи в «пузыре», название, остаток по плану. */
function Tile({ emoji, title, color, plan, minHeight, onPick }: TileProps) {
  const id = useId();
  const line = planLine(plan);
  return (
    <button
      type="button"
      aria-label={title}
      aria-describedby={line ? id : undefined}
      className="flex flex-col items-start rounded-3xl border p-4 text-left transition-transform duration-100 active:scale-[0.97]"
      style={{
        minHeight,
        background: tint(color, 0.14),
        borderColor: tint(color, 0.3),
      }}
      onClick={() => {
        tick();
        onPick();
      }}
    >
      <span
        className="flex size-14 items-center justify-center rounded-2xl text-3xl"
        style={{ background: tint(color, 0.26) }}
        aria-hidden="true"
      >
        {emoji || "•"}
      </span>
      <span className="mt-3 text-lg leading-tight font-semibold">{title}</span>
      {line && (
        // строка плана прижата к низу: названия в ряду стоят на одной высоте
        <span className="mt-auto flex w-full flex-col gap-1 pt-1">
          <span id={id} className="text-[13px] text-hint">
            {line.text}
          </span>
          <span
            className="h-1.5 w-full overflow-hidden rounded-full bg-[rgba(127,127,127,0.2)]"
            aria-hidden="true"
          >
            <span
              className={`block h-full rounded-full ${STATUS_COLOR[line.status]}`}
              style={{ width: `${Math.max(line.filled * 100, 3)}%` }}
            />
          </span>
        </span>
      )}
    </button>
  );
}

export function GroupTiles({
  groups,
  plan,
  onPick,
}: {
  groups: Group[];
  plan: PlanIndex;
  onPick: (group: Group) => void;
}) {
  return (
    <div className="grid grid-cols-2 gap-3">
      {groups.map((group, index) => (
        <Tile
          key={group.name}
          emoji={group.emoji}
          title={group.title}
          color={groupColor(group.name, index)}
          plan={plan.group(group.name)}
          minHeight={124}
          onPick={() => onPick(group)}
        />
      ))}
    </div>
  );
}

export function SubTiles({
  group,
  color,
  plan,
  onPick,
}: {
  group: Group;
  color: string;
  plan: PlanIndex;
  onPick: (sub: Subcategory) => void;
}) {
  // мало подкатегорий — плитки выше и занимают пустой экран
  const minHeight = group.subcategories.length <= 4 ? 168 : 140;
  return (
    <div className="grid grid-cols-2 gap-3">
      {group.subcategories.map((sub) => (
        <Tile
          key={sub.name}
          emoji={sub.icon}
          title={sub.name}
          color={color}
          plan={plan.sub(group.name, sub.name)}
          minHeight={minHeight}
          onPick={() => onPick(sub)}
        />
      ))}
    </div>
  );
}
