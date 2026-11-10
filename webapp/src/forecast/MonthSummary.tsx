import type { Forecast } from "../api";
import { Articles } from "./DayCard";
import {
  amount,
  monthArticles,
  monthTotals,
  viewCode,
  type View,
} from "./view";

interface Props {
  data: Forecast;
  view: View;
}

/**
 * Итог прошедшего месяца по факту: потрачено, пришло, сколько было на конец
 * месяца и траты по статьям — каждая в своей валюте, от больших.
 */
export default function MonthSummary({ data, view }: Props) {
  const totals = monthTotals(data, view);
  if (!totals) return null;
  const code = viewCode(view);
  const articles = monthArticles(data, view);
  // одни возвраты: деньги вернулись — плюсом, как в карточке дня
  const back = totals.spent < 0;
  return (
    <section
      aria-label="Итог месяца"
      className="flex flex-col gap-1 rounded-2xl bg-section p-4"
    >
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">Итог месяца</h2>
        <span className="text-sm font-semibold">по факту</span>
      </div>
      <p className="flex justify-between gap-2">
        <span>{back ? "вернули" : "потрачено"}</span>
        <span className={`font-bold ${back ? "text-earned" : "text-spent"}`}>
          {back
            ? `+${amount(-totals.spent, code)}`
            : `−${amount(totals.spent, code)}`}
        </span>
      </p>
      <p className="flex justify-between gap-2">
        <span>пришло</span>
        <span className="font-bold text-earned">
          {`+${amount(totals.earned, code)}`}
        </span>
      </p>
      <p className="flex justify-between gap-2">
        <span>на конец месяца</span>
        <span
          className={`font-bold ${
            totals.evening < 0 ? "rounded-md bg-minus px-1.5 text-white" : ""
          }`}
        >
          {amount(totals.evening, code)}
        </span>
      </p>
      {articles.length > 0 && (
        <div className="mt-2 flex flex-col gap-1">
          <h3 className="font-semibold">Траты по статьям</h3>
          <Articles label="Траты месяца по статьям" articles={articles} />
        </div>
      )}
    </section>
  );
}
