/** Разделы «Сводки» и выбранный — в памяти телефона, как вид «Календаря». */
import { readChoice, saveChoice } from "../choice";

export type Section = "today" | "spending";

export const SECTIONS: readonly { name: Section; title: string }[] = [
  { name: "today", title: "Сегодня" },
  { name: "spending", title: "Траты" },
];

export const SECTION_KEY = "dashboard-section";

/** Раздел из памяти телефона; нет, чужое значение («Накопления» — они теперь во
 * «Плане», «Впереди» — раздела больше нет) или память закрыта — «Сегодня». */
export function storedSection(): Section {
  return readChoice(
    SECTION_KEY,
    SECTIONS.map((section) => section.name),
    "today",
  );
}

export function storeSection(section: Section): void {
  saveChoice(SECTION_KEY, section);
}
