import { useCallback, useEffect, useState } from "react";
import type { Api, Bootstrap } from "../api";
import SavingsTab from "../dashboard/SavingsTab";
import SectionSwitch from "../dashboard/SectionSwitch";
import { changeCount, emptyDraft, type Draft } from "./draft";
import { capital, monthName } from "./months";
import PlanScreen from "./PlanScreen";
import {
  PLAN_SECTIONS,
  storePlanSection,
  storedPlanSection,
  type PlanSection,
} from "./section";

interface Props {
  api: Api;
  boot: Bootstrap | null; // каталог — для окон копилок
  months: string[]; // текущий и следующий, «YYYY-MM»; пусто — справочника ещё нет
  drafts: Record<string, Draft> | null; // null — черновики ещё не открыты
  restored: string[]; // месяцы с черновиком с прошлого раза
  onDraft: (draft: Draft) => void;
  onRestoredSeen: (month: string) => void;
  openSave: boolean; // кнопка бота «Отложил часть…»: «Накопления» и окно «Отложил»
  onSaveSeen: () => void; // просьбу бота увидели — окно больше не открывать
}

/** Вкладка «План»: разделы «План месяца» (месяцы и экран месяца) и «Накопления». */
export default function PlanTab({
  api,
  boot,
  months,
  drafts,
  restored,
  onDraft,
  onRestoredSeen,
  openSave,
  onSaveSeen,
}: Props) {
  const [section, setSection] = useState<PlanSection>(() =>
    openSave ? "savings" : storedPlanSection(),
  );
  // из чата — как нажатие: «Накопления» запоминаются выбранным разделом
  useEffect(() => {
    if (openSave) storePlanSection("savings");
  }, [openSave]);
  const choose = useCallback(
    (next: PlanSection) => {
      setSection(next);
      storePlanSection(next);
      onSaveSeen();
    },
    [onSaveSeen],
  );
  // месяц выбранный, пока он среди месяцев справочника; иначе — текущий
  const [picked, setPicked] = useState<string | null>(null);
  const month =
    picked !== null && months.includes(picked) ? picked : (months[0] ?? "");
  return (
    <div className="flex flex-col gap-4">
      <SectionSwitch
        label="Раздел плана"
        sections={PLAN_SECTIONS}
        section={section}
        onSection={choose}
      />
      {section === "savings" && (
        <SavingsTab api={api} boot={boot} openSave={openSave} />
      )}
      {section === "month" && drafts && month && (
        <>
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
                  className={`rounded-full px-4 py-2 text-line font-extrabold ${
                    chosen ? "bg-button text-button-text" : "bg-section"
                  }`}
                  onClick={() => setPicked(item)}
                >
                  {capital(monthName(item))}
                  {unsaved && (
                    <>
                      <span className="sr-only">
                        , есть несохранённые правки
                      </span>
                      <span
                        aria-hidden="true"
                        className="ml-1.5 inline-block size-2.5 rounded-full bg-current align-middle"
                      />
                    </>
                  )}
                </button>
              );
            })}
          </div>
          <PlanScreen
            key={month}
            api={api}
            month={month}
            draft={drafts[month] ?? emptyDraft(month)}
            restored={restored.includes(month)}
            onDraft={onDraft}
            onRestoredSeen={() => onRestoredSeen(month)}
          />
        </>
      )}
    </div>
  );
}
