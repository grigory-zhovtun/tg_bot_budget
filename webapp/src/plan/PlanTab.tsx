import { useState } from "react";
import type { Api } from "../api";
import { changeCount, emptyDraft, type Draft } from "./draft";
import { capital, monthName } from "./months";
import PlanScreen from "./PlanScreen";

interface Props {
  api: Api;
  months: string[]; // текущий и следующий, «YYYY-MM»
  drafts: Record<string, Draft>;
  restored: string[]; // месяцы с черновиком с прошлого раза
  onDraft: (draft: Draft) => void;
  onRestoredSeen: (month: string) => void;
}

/** Вкладка «План»: переключатель месяцев и экран выбранного месяца. */
export default function PlanTab({
  api,
  months,
  drafts,
  restored,
  onDraft,
  onRestoredSeen,
}: Props) {
  const [month, setMonth] = useState(months[0] ?? "");
  const draft = drafts[month] ?? emptyDraft(month);
  return (
    <div className="flex flex-col gap-4">
      <div className="flex gap-2" role="radiogroup" aria-label="Месяц">
        {months.map((item) => {
          const chosen = item === month;
          const unsaved = changeCount(drafts[item] ?? emptyDraft(item)) > 0;
          return (
            <button
              key={item}
              type="button"
              role="radio"
              aria-checked={chosen}
              className={`rounded-full px-4 py-1.5 text-sm font-medium ${
                chosen ? "bg-button text-button-text" : "bg-section"
              }`}
              onClick={() => setMonth(item)}
            >
              {capital(monthName(item))}
              {unsaved && (
                <span
                  aria-hidden="true"
                  className="ml-1 inline-block size-1.5 rounded-full bg-current align-middle"
                />
              )}
            </button>
          );
        })}
      </div>
      <PlanScreen
        key={month}
        api={api}
        month={month}
        draft={draft}
        restored={restored.includes(month)}
        onDraft={onDraft}
        onRestoredSeen={() => onRestoredSeen(month)}
      />
    </div>
  );
}
