import { displayAmount, press, type Key } from "../entry/amount";
import Keypad from "../entry/Keypad";

interface Props {
  text: string;
  currency: string;
  onChange: (text: string) => void;
}

/** Сумма крупно и клавиатура, как при вводе траты; копейки — только у валюты. */
export default function AmountPad({ text, currency, onChange }: Props) {
  const decimals = currency !== "UZS";
  return (
    <div className="flex flex-col gap-3">
      <output
        aria-label="Сумма"
        className="text-center text-4xl font-semibold"
        aria-live="polite"
      >
        {displayAmount(text)}{" "}
        <span className="text-xl text-hint">{currency}</span>
      </output>
      <Keypad
        decimals={decimals}
        onKey={(key: Key) => onChange(press(text, key, decimals))}
      />
    </div>
  );
}
