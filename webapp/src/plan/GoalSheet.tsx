import { useState } from "react";
import type { PlanGoal } from "../api";
import AmountPad from "./AmountPad";
import { amountText } from "./draft";
import Sheet from "./Sheet";

interface Props {
  goal: PlanGoal; // как в черновике сейчас
  currencies: string[];
  onDone: (text: string, currency: string) => void;
  onClose: () => void;
}

export default function GoalSheet({
  goal,
  currencies,
  onDone,
  onClose,
}: Props) {
  const [currency, setCurrency] = useState(goal.currency);
  const [text, setText] = useState(
    amountText(goal.amount, goal.currency !== "UZS"),
  );
  return (
    <Sheet title="🎯 Отложить за месяц" onClose={onClose}>
      <label className="flex items-center justify-between gap-3 text-sm text-hint">
        Валюта
        <select
          className="rounded-xl bg-section px-3 py-2 text-base text-text"
          value={currency}
          onChange={(change) => {
            setCurrency(change.target.value);
            setText("");
          }}
        >
          {currencies.map((code) => (
            <option key={code} value={code}>
              {code}
            </option>
          ))}
        </select>
      </label>
      <AmountPad text={text} currency={currency} onChange={setText} />
      <button
        type="button"
        className="h-12 rounded-xl bg-button font-semibold text-button-text"
        onClick={() => onDone(text, currency)}
      >
        Готово
      </button>
    </Sheet>
  );
}
