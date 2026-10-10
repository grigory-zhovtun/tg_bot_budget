import type { Source } from "../api";

interface Props {
  sources: Source[];
  current: string;
  onPick: (name: string) => void;
}

export default function SourceChips({ sources, current, onPick }: Props) {
  return (
    <div
      className="-mx-4 flex gap-2 overflow-x-auto px-4 pb-1"
      role="radiogroup"
      aria-label="Карта"
    >
      {sources.map((source) => {
        const active = source.name === current;
        return (
          <button
            key={source.name}
            type="button"
            role="radio"
            aria-checked={active}
            className={`shrink-0 rounded-full px-3 py-1.5 text-sm ${
              active ? "bg-button text-button-text" : "bg-section"
            }`}
            onClick={() => onPick(source.name)}
          >
            {source.name}
          </button>
        );
      })}
    </div>
  );
}
