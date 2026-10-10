import type { Group, Subcategory } from "../api";

export function GroupTiles({
  groups,
  onPick,
}: {
  groups: Group[];
  onPick: (group: Group) => void;
}) {
  return (
    <div className="grid grid-cols-2 gap-2">
      {groups.map((group) => (
        <button
          key={group.name}
          type="button"
          className="flex min-h-20 flex-col items-start justify-between rounded-2xl bg-section p-3 text-left active:opacity-60"
          onClick={() => onPick(group)}
        >
          <span className="text-3xl" aria-hidden="true">
            {group.emoji}
          </span>
          <span className="text-sm font-semibold">{group.title}</span>
        </button>
      ))}
    </div>
  );
}

export function SubTiles({
  group,
  onPick,
}: {
  group: Group;
  onPick: (sub: Subcategory) => void;
}) {
  return (
    <div className="grid grid-cols-3 gap-2">
      {group.subcategories.map((sub) => (
        <button
          key={sub.name}
          type="button"
          className="flex min-h-20 flex-col items-center justify-center gap-1 rounded-2xl bg-section p-2 text-center active:opacity-60"
          onClick={() => onPick(sub)}
        >
          <span className="text-2xl" aria-hidden="true">
            {sub.icon || "•"}
          </span>
          <span className="text-xs">{sub.name}</span>
        </button>
      ))}
    </div>
  );
}
