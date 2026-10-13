import { useState } from "react";
import type { PlanArticle, PlanHistory } from "../api";
import { money } from "../format";
import AmountPad from "./AmountPad";
import { amountText } from "./draft";
import { capital, days, monthDative, monthName, monthRange } from "./months";
import Sheet from "./Sheet";

interface Props {
  article: PlanArticle;
  text: string;
  history: PlanHistory | null;
  onDone: (text: string) => void;
  onClose: () => void;
}

const QUICK = "rounded-xl bg-section py-2 text-link";

export default function ArticleSheet({
  article,
  text: initial,
  history,
  onDone,
  onClose,
}: Props) {
  const [text, setText] = useState(initial);
  const decimals = article.currency !== "UZS";
  const fraction = decimals ? 2 : 0;
  // подсказку в сумах округляем до тысяч: план не нужен с точностью до сума
  const nice = (value: number) =>
    amountText(decimals ? value : Math.round(value / 1000) * 1000, decimals);
  const icon = article.icon ? `${article.icon} ` : "";
  return (
    <Sheet
      title={`${icon}${article.subcategory} · ${article.currency}`}
      onClose={onClose}
    >
      <AmountPad text={text} currency={article.currency} onChange={setText} />
      <p className="text-center text-sm text-hint">
        было в плане: {money(article.plan, fraction)}
      </p>
      {history && (
        <dl className="flex flex-col gap-1 rounded-2xl bg-section p-3 text-sm">
          <div className="flex justify-between gap-3">
            <dt>Среднее за {monthRange(history.average)}</dt>
            <dd>{money(article.average, fraction)}</dd>
          </div>
          <div className="flex justify-between gap-3">
            <dt>{capital(monthName(history.last))}</dt>
            <dd>{money(article.last, fraction)}</dd>
          </div>
          <div className="flex justify-between gap-3">
            <dt>
              {capital(monthName(history.current))} пока (
              {days(history.current_days)})
            </dt>
            <dd>{money(article.current, fraction)}</dd>
          </div>
        </dl>
      )}
      <div className="grid grid-cols-3 gap-2 text-sm">
        <button
          type="button"
          className={QUICK}
          onClick={() => setText(nice(article.average))}
        >
          = среднему
        </button>
        {history && (
          <button
            type="button"
            className={QUICK}
            onClick={() => setText(nice(article.last))}
          >
            = {monthDative(history.last)}
          </button>
        )}
        <button
          type="button"
          className={QUICK}
          onClick={() => setText(amountText(article.plan, decimals))}
        >
          ↺ как было
        </button>
      </div>
      <button
        type="button"
        className="h-12 rounded-xl bg-button font-semibold text-button-text"
        onClick={() => onDone(text)}
      >
        Готово
      </button>
    </Sheet>
  );
}
