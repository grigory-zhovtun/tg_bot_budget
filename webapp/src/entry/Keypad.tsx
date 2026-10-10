import type { Key } from "./amount";

const DIGITS: Key[] = ["1", "2", "3", "4", "5", "6", "7", "8", "9"];

interface Props {
  decimals: boolean;
  onKey: (key: Key) => void;
}

export default function Keypad({ decimals, onKey }: Props) {
  const keys: Key[] = [...DIGITS, decimals ? "," : "000", "0", "⌫"];
  return (
    <div className="grid grid-cols-3 gap-2">
      {keys.map((key) => (
        <button
          key={key}
          type="button"
          aria-label={key === "⌫" ? "Стереть" : key}
          className="h-14 rounded-xl bg-section text-2xl font-medium active:opacity-60"
          onClick={() => onKey(key)}
        >
          {key}
        </button>
      ))}
    </div>
  );
}
