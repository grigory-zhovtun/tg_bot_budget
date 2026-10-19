import { useState } from "react";
import type { PlanArticle, PlanHistory } from "../api";
import { moneyText } from "../format";
import AmountPad from "./AmountPad";
import { amountText } from "./draft";
import History, { QUICK, rounded } from "./History";
import { monthIn } from "./months";
import Sheet from "./Sheet";

interface Props {
  article: PlanArticle;
  text: string;
  history: PlanHistory | null;
  onDone: (text: string) => void;
  onClose: () => void;
}

export default function ArticleSheet({
  article,
  text: initial,
  history,
  onDone,
  onClose,
}: Props) {
  const [text, setText] = useState(initial);
  const { currency } = article;
  const decimals = currency !== "UZS";
  const nice = (value: number) =>
    amountText(rounded(value, currency), decimals);
  const sum = (value: number) => moneyText(rounded(value, currency), currency);
  const icon = article.icon ? `${article.icon} ` : "";
  return (
    <Sheet
      title={`${icon}${article.subcategory}${decimals ? ` · ${currency}` : ""}`}
      onClose={onClose}
    >
      <AmountPad text={text} currency={currency} onChange={setText} />
      <History
        currency={currency}
        history={history}
        plan={article.plan}
        average={article.average}
        last={article.last}
        current={article.current}
      />
      <div className="flex flex-col gap-2">
        <button
          type="button"
          className={QUICK}
          onClick={() => setText(nice(article.average))}
        >
          {`= в среднем ${sum(article.average)}`}
        </button>
        {history && (
          <button
            type="button"
            className={QUICK}
            onClick={() => setText(nice(article.last))}
          >
            {`= как ${monthIn(history.last)} ${sum(article.last)}`}
          </button>
        )}
        <button
          type="button"
          className={QUICK}
          onClick={() => setText(amountText(article.plan, decimals))}
        >
          {`↺ как было ${moneyText(article.plan, currency)}`}
        </button>
      </div>
      <button
        type="button"
        className="h-12 rounded-xl bg-button text-lg font-extrabold text-button-text"
        onClick={() => onDone(text)}
      >
        Готово
      </button>
    </Sheet>
  );
}
