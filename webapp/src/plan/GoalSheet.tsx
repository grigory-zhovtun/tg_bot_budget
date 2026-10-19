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
  // цель меньше нуля на клавиатуре не набрать: без ввода «Готово» её не трогает
  const [touched, setTouched] = useState(false);
  return (
    <Sheet title="🎯 Отложить за месяц" onClose={onClose}>
      <label className="flex items-center justify-between gap-3 text-line font-bold text-text">
        Валюта
        <select
          className="rounded-xl bg-section px-3 py-2 text-lg text-text"
          value={currency}
          onChange={(change) => {
            setCurrency(change.target.value);
            setText("");
            setTouched(true);
          }}
        >
          {currencies.map((code) => (
            <option key={code} value={code}>
              {code}
            </option>
          ))}
        </select>
      </label>
      <AmountPad
        text={text}
        currency={currency}
        onChange={(next) => {
          setText(next);
          setTouched(true);
        }}
      />
      <button
        type="button"
        className="h-12 rounded-xl bg-button text-lg font-extrabold text-button-text"
        onClick={() =>
          !touched && goal.amount < 0 ? onClose() : onDone(text, currency)
        }
      >
        Готово
      </button>
    </Sheet>
  );
}
