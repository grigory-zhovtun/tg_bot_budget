import type { Key } from "./amount";

const DIGITS: Key[] = ["1", "2", "3", "4", "5", "6", "7", "8", "9"];

interface Props {
  decimals: boolean;
  onKey: (key: Key) => void;
  big?: boolean; // крупные жирные цифры — во «Вводе»; в окнах — как были
}

const SIZE = {
  normal: "h-14 text-2xl font-medium",
  big: "h-16 text-[28px] font-extrabold",
};

export default function Keypad({ decimals, onKey, big = false }: Props) {
  const keys: Key[] = [...DIGITS, decimals ? "," : "000", "0", "⌫"];
  return (
    <div className="grid grid-cols-3 gap-2">
      {keys.map((key) => (
        <button
          key={key}
          type="button"
          aria-label={key === "⌫" ? "Стереть" : key}
          className={`${big ? SIZE.big : SIZE.normal} rounded-xl bg-section active:opacity-60`}
          onClick={() => onKey(key)}
        >
          {key}
        </button>
      ))}
    </div>
  );
}
