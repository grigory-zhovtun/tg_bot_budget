import { useState } from "react";
import type { PlanAddable, PlanHistory } from "../api";
import { money } from "../format";
import AmountPad from "./AmountPad";
import { amountText, pairKey } from "./draft";
import { capital, days, monthDative, monthName, monthRange } from "./months";
import Sheet from "./Sheet";

interface Props {
  group: string;
  items: PlanAddable[]; // пары группы, которых нет во вкладке
  history: PlanHistory | null;
  initial: { key: string; text: string } | null; // правка уже добавленной статьи
  onDone: (item: PlanAddable, text: string) => void;
  onRemove: (() => void) | null;
  onClose: () => void;
}

const QUICK = "rounded-xl bg-section py-2 text-link";

function chip(chosen: boolean): string {
  return `rounded-full px-3 py-1.5 text-sm ${
    chosen ? "bg-button text-button-text" : "bg-section"
  }`;
}

/** Окно новой статьи: подкатегория группы, валюта, сумма с подсказками. */
export default function AddArticleSheet({
  group,
  items,
  history,
  initial,
  onDone,
  onRemove,
  onClose,
}: Props) {
  const start =
    items.find(
      (item) => pairKey(item.subcategory, item.currency) === initial?.key,
    ) ?? items[0];
  const [subcategory, setSubcategory] = useState(start?.subcategory ?? "");
  const [currency, setCurrency] = useState(start?.currency ?? "UZS");
  const [text, setText] = useState(initial?.text ?? "");
  const names = [...new Set(items.map((item) => item.subcategory))];
  const codes = items
    .filter((item) => item.subcategory === subcategory)
    .map((item) => item.currency);
  const item = items.find(
    (one) => one.subcategory === subcategory && one.currency === currency,
  );
  const decimals = currency !== "UZS";
  const fraction = decimals ? 2 : 0;
  // подсказку в сумах округляем до тысяч, как в окне статьи
  const nice = (value: number) =>
    amountText(decimals ? value : Math.round(value / 1000) * 1000, decimals);
  const pick = (name: string) => {
    const options = items
      .filter((one) => one.subcategory === name)
      .map((one) => one.currency);
    setSubcategory(name);
    setCurrency(options.includes("UZS") ? "UZS" : (options[0] ?? "UZS"));
  };
  return (
    <Sheet title={`Новая статья · ${group}`} onClose={onClose}>
      <div
        className="flex flex-wrap gap-2"
        role="radiogroup"
        aria-label="Подкатегория"
      >
        {names.map((name) => {
          const icon = items.find((one) => one.subcategory === name)?.icon;
          return (
            <button
              key={name}
              type="button"
              role="radio"
              aria-checked={name === subcategory}
              className={chip(name === subcategory)}
              onClick={() => pick(name)}
            >
              {icon ? `${icon} ${name}` : name}
            </button>
          );
        })}
      </div>
      {codes.length > 1 && (
        <div className="flex gap-2" role="radiogroup" aria-label="Валюта">
          {codes.map((code) => (
            <button
              key={code}
              type="button"
              role="radio"
              aria-checked={code === currency}
              className={chip(code === currency)}
              onClick={() => setCurrency(code)}
            >
              {code}
            </button>
          ))}
        </div>
      )}
      <AmountPad text={text} currency={currency} onChange={setText} />
      {history && item && (
        <dl className="flex flex-col gap-1 rounded-2xl bg-section p-3 text-sm">
          <div className="flex justify-between gap-3">
            <dt>Среднее за {monthRange(history.average)}</dt>
            <dd>{money(item.average, fraction)}</dd>
          </div>
          <div className="flex justify-between gap-3">
            <dt>{capital(monthName(history.last))}</dt>
            <dd>{money(item.last, fraction)}</dd>
          </div>
          <div className="flex justify-between gap-3">
            <dt>
              {capital(monthName(history.current))} пока (
              {days(history.current_days)})
            </dt>
            <dd>{money(item.current, fraction)}</dd>
          </div>
        </dl>
      )}
      {item && (
        <div className="grid grid-cols-2 gap-2 text-sm">
          <button
            type="button"
            className={QUICK}
            onClick={() => setText(nice(item.average))}
          >
            = среднему
          </button>
          {history && (
            <button
              type="button"
              className={QUICK}
              onClick={() => setText(nice(item.last))}
            >
              = {monthDative(history.last)}
            </button>
          )}
        </div>
      )}
      <button
        type="button"
        disabled={!item}
        className="h-12 rounded-xl bg-button font-semibold text-button-text disabled:opacity-50"
        onClick={() => {
          if (item) onDone(item, text);
        }}
      >
        {initial ? "Готово" : "Добавить"}
      </button>
      {onRemove && (
        <button type="button" className="py-2 text-danger" onClick={onRemove}>
          Убрать статью
        </button>
      )}
    </Sheet>
  );
}
