/** Разделы «Плана» и выбранный — в памяти телефона, как разделы «Сводки». */
import { readChoice, saveChoice } from "../choice";

export type PlanSection = "month" | "savings";

export const PLAN_SECTIONS: readonly { name: PlanSection; title: string }[] = [
  { name: "month", title: "План месяца" },
  { name: "savings", title: "Накопления" },
];

export const PLAN_SECTION_KEY = "plan-section";

/** Раздел из памяти телефона; нет, чужое или память закрыта — «План месяца». */
export function storedPlanSection(): PlanSection {
  return readChoice(
    PLAN_SECTION_KEY,
    PLAN_SECTIONS.map((section) => section.name),
    "month",
  );
}

export function storePlanSection(section: PlanSection): void {
  saveChoice(PLAN_SECTION_KEY, section);
}
