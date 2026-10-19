import { useState } from "react";
import type { PlanAddable, PlanHistory } from "../api";
import { amountValue } from "../entry/amount";
import { moneyText } from "../format";
import AmountPad from "./AmountPad";
import { amountText, pairKey } from "./draft";
import History, { QUICK, rounded } from "./History";
import { monthIn } from "./months";
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

function chip(chosen: boolean): string {
  return `rounded-full px-4 py-2 text-line font-bold ${
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
  // подсказку в сумах округляем до тысяч, как в окне статьи
  const nice = (value: number) =>
    amountText(rounded(value, currency), decimals);
  const sum = (value: number) => moneyText(rounded(value, currency), currency);
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
      {item && (
        <History
          currency={currency}
          history={history}
          average={item.average}
          last={item.last}
          current={item.current}
        />
      )}
      {item && (
        <div className="flex flex-col gap-2">
          <button
            type="button"
            className={QUICK}
            onClick={() => setText(nice(item.average))}
          >
            {`= в среднем ${sum(item.average)}`}
          </button>
          {history && (
            <button
              type="button"
              className={QUICK}
              onClick={() => setText(nice(item.last))}
            >
              {`= как ${monthIn(history.last)} ${sum(item.last)}`}
            </button>
          )}
        </div>
      )}
      <button
        type="button"
        disabled={!item || amountValue(text) <= 0}
        className="h-12 rounded-xl bg-button text-lg font-extrabold text-button-text disabled:opacity-50"
        onClick={() => {
          if (item) onDone(item, text);
        }}
      >
        {initial ? "Готово" : "Добавить"}
      </button>
      {onRemove && (
        <button
          type="button"
          className="py-2 text-line font-extrabold text-danger"
          onClick={onRemove}
        >
          Убрать статью
        </button>
      )}
    </Sheet>
  );
}
