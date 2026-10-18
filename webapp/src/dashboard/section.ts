/** Вкладки «Сводки» и выбранная — в памяти телефона, как вид «Прогноза». */

export type Section = "today" | "spending" | "savings" | "ahead";

export const SECTIONS: readonly { name: Section; title: string }[] = [
  { name: "today", title: "Сегодня" },
  { name: "spending", title: "Траты" },
  { name: "savings", title: "Накопления" },
  { name: "ahead", title: "Впереди" },
];

export const SECTION_KEY = "dashboard-section";

const DEFAULT_SECTION: Section = "today";

/** Вкладка из памяти телефона; нет, чужое значение или память закрыта — «Сегодня». */
export function storedSection(): Section {
  try {
    const stored = localStorage.getItem(SECTION_KEY);
    return (
      SECTIONS.find((section) => section.name === stored)?.name ??
      DEFAULT_SECTION
    );
  } catch {
    return DEFAULT_SECTION;
  }
}

export function storeSection(section: Section): void {
  try {
    localStorage.setItem(SECTION_KEY, section);
  } catch {
    // память недоступна (приватный режим): выбор живёт до закрытия приложения
  }
}
