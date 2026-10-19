import type { PlanHistory } from "../api";
import { Line, Table } from "../dashboard/blocks";
import { moneyText } from "../format";
import { days, monthIn, monthRange } from "./months";

/** Подсказка в сумах — до тысяч: план не нужен с точностью до сума. */
export function rounded(value: number, currency: string): number {
  return currency === "UZS" ? Math.round(value / 1000) * 1000 : value;
}

/** Кнопка быстрого выбора в окнах статьи — во всю ширину, с суммой. */
export const QUICK =
  "rounded-xl bg-section px-3 py-2.5 text-line font-extrabold text-link";

interface Props {
  currency: string;
  history: PlanHistory | null;
  plan?: number; // «было в плане» — только у статьи из вкладки
  average: number;
  last: number;
  current: number;
}

/** История статьи таблицей с короткими подписями; периоды — строками под ней. */
export default function History({
  currency,
  history,
  plan,
  average,
  last,
  current,
}: Props) {
  const rows: string[][] = [];
  if (plan !== undefined) {
    rows.push(["было в плане", moneyText(plan, currency)]);
  }
  if (history) {
    rows.push(["в среднем", moneyText(average, currency)]);
    rows.push([monthIn(history.last), moneyText(last, currency)]);
    rows.push([
      `${monthIn(history.current)} пока`,
      moneyText(current, currency),
    ]);
  }
  if (rows.length === 0) return null;
  return (
    <div className="flex flex-col gap-1">
      <Table align="lr" rows={rows} />
      {history && (
        <>
          <Line>{`в среднем — за ${monthRange(history.average)}`}</Line>
          <Line>
            {`${monthIn(history.current)} — за ${days(history.current_days)}`}
          </Line>
        </>
      )}
    </div>
  );
}
