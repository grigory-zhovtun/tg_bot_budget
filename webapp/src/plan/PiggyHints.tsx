import type { MonthPlan, PlanPiggyHint } from "../api";
import { Fact, Line } from "../dashboard/blocks";
import { MONTHS_IN, monthIndex } from "../dashboard/piggyText";
import { moneyText } from "../format";
import { amountValue } from "../entry/amount";
import { groupTitle } from "../forecast/records";
import {
  addRow,
  amountText,
  applyGoal,
  editRow,
  pairKey,
  rowPlan,
  type Draft,
} from "./draft";

interface Props {
  plan: MonthPlan;
  draft: Draft;
  onDraft: (draft: Draft) => void;
}

/** План статьи в черновике: строка вкладки или новая, ещё не сохранённая. */
function planned(plan: MonthPlan, draft: Draft, hint: PlanPiggyHint): number {
  const key = hint.key ?? pairKey(hint.subcategory, hint.currency);
  const row = plan.rows.find((item) => item.key === key);
  if (row) return rowPlan(draft, row);
  const added = draft.added[pairKey(hint.subcategory, hint.currency)];
  return added ? amountValue(added.text) : 0;
}

/** Статья получает трату копилки (нет строки — добавляется), цель — по копилкам. */
function applied(plan: MonthPlan, draft: Draft, hint: PlanPiggyHint): Draft {
  const text = amountText(hint.amount, hint.currency !== "UZS");
  const key = hint.key ?? pairKey(hint.subcategory, hint.currency);
  const row = plan.rows.find((item) => item.key === key);
  let next = draft;
  if (row) {
    next = editRow(next, row, text);
  } else {
    const item = plan.addable.find(
      (entry) =>
        entry.kind === "spend" &&
        entry.subcategory === hint.subcategory &&
        entry.currency === hint.currency,
    );
    if (item) next = addRow(next, item, text);
  }
  if (plan.goal && plan.goal_hint) {
    next = applyGoal(next, plan.goal, plan.goal_hint.suggested);
  }
  return next;
}

/** «🐷 Из копилки 12 000 000», «Новый год» в декабре, «заложите в статью …» — пока
 * плана статьи не хватает на трату копилки (спека 4.7). */
export default function PiggyHints({ plan, draft, onDraft }: Props) {
  const shown = (plan.piggy_hints ?? []).filter(
    (hint) => planned(plan, draft, hint) < hint.amount,
  );
  if (shown.length === 0) return null;
  return (
    <section
      aria-label="Копилки в плане"
      className="flex flex-col gap-4 rounded-2xl bg-section p-4"
    >
      {shown.map((hint) => (
        <div
          key={`${hint.piggy}|${hint.subcategory}|${hint.currency}`}
          className="flex flex-col gap-1"
        >
          <Fact>{`🐷 Из копилки ${moneyText(hint.amount, hint.currency)}`}</Fact>
          <Line>
            {`«${hint.piggy}» в ${MONTHS_IN[monthIndex(hint.month)] ?? hint.month}`}
          </Line>
          <Line>заложите в статью</Line>
          <Line>{`«${groupTitle(hint.group)} / ${hint.subcategory}»`}</Line>
          <button
            type="button"
            className="mt-1 h-12 rounded-xl bg-button text-lg font-extrabold text-button-text"
            onClick={() => onDraft(applied(plan, draft, hint))}
          >
            Применить
          </button>
        </div>
      ))}
    </section>
  );
}
