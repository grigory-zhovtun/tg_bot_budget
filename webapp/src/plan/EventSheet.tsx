import { useState } from "react";
import type { PlanEvent } from "../api";
import { amountValue } from "../entry/amount";
import AmountPad from "./AmountPad";
import { amountText, type EventDraft } from "./draft";
import Sheet from "./Sheet";

interface Props {
  event: PlanEvent; // как в черновике сейчас
  currencies: string[];
  onDone: (value: EventDraft | null) => void; // null — очистить строку
  onClose: () => void;
}

const FIELD = "rounded-xl bg-section px-3 py-2 text-base text-text";

export default function EventSheet({
  event,
  currencies,
  onDone,
  onClose,
}: Props) {
  const filled = event.name !== null;
  const [name, setName] = useState(event.name ?? "");
  const [day, setDay] = useState(event.day ? String(event.day) : "");
  const [income, setIncome] = useState((event.amount ?? 1) > 0);
  const [currency, setCurrency] = useState(event.currency ?? "UZS");
  const [text, setText] = useState(
    event.amount
      ? amountText(Math.abs(event.amount), (event.currency ?? "UZS") !== "UZS")
      : "",
  );
  const dayNumber = Number(day);
  const valid =
    name.trim().length > 0 &&
    Number.isInteger(dayNumber) &&
    dayNumber >= 1 &&
    dayNumber <= 31 &&
    amountValue(text) > 0;
  const kind = (active: boolean) =>
    `rounded-lg py-1.5 text-sm font-medium ${active ? "bg-bg shadow-sm" : "text-hint"}`;
  return (
    <Sheet
      title={filled ? "Поступление или платёж" : "Новая строка списка"}
      onClose={onClose}
    >
      <label className="flex flex-col gap-1 text-sm text-hint">
        Название
        <input
          className={FIELD}
          value={name}
          maxLength={40}
          onChange={(change) => setName(change.target.value)}
        />
      </label>
      <div className="flex gap-2">
        <label className="flex flex-1 flex-col gap-1 text-sm text-hint">
          День
          <input
            className={FIELD}
            type="number"
            inputMode="numeric"
            min={1}
            max={31}
            value={day}
            onChange={(change) => setDay(change.target.value)}
          />
        </label>
        <label className="flex flex-1 flex-col gap-1 text-sm text-hint">
          Валюта
          <select
            className={FIELD}
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
      </div>
      <div
        className="grid grid-cols-2 gap-1 rounded-xl bg-section p-1"
        role="radiogroup"
        aria-label="Вид"
      >
        <button
          type="button"
          role="radio"
          aria-checked={income}
          className={kind(income)}
          onClick={() => setIncome(true)}
        >
          Поступление
        </button>
        <button
          type="button"
          role="radio"
          aria-checked={!income}
          className={kind(!income)}
          onClick={() => setIncome(false)}
        >
          Платёж
        </button>
      </div>
      <AmountPad text={text} currency={currency} onChange={setText} />
      <button
        type="button"
        disabled={!valid}
        className="h-12 rounded-xl bg-button font-semibold text-button-text disabled:opacity-50"
        onClick={() =>
          onDone({ name: name.trim(), day: dayNumber, income, text, currency })
        }
      >
        Готово
      </button>
      {filled && (
        <button
          type="button"
          className="text-danger"
          onClick={() => onDone(null)}
        >
          Удалить строку
        </button>
      )}
    </Sheet>
  );
}
