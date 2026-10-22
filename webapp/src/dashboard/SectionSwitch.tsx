// классы колонок — целиком: Tailwind собирает только те, что видит в коде
const COLUMNS: Record<number, string> = { 2: "grid-cols-2", 3: "grid-cols-3" };

interface Props<T extends string> {
  label: string; // подпись группы для чтения с экрана: «Раздел сводки»
  sections: readonly { name: T; title: string }[];
  section: T;
  onSection: (section: T) => void;
}

/** Разделы экрана в одну строку: крупные подписи, выбранный — фоном и жирностью. */
export default function SectionSwitch<T extends string>({
  label,
  sections,
  section,
  onSection,
}: Props<T>) {
  return (
    <div
      role="radiogroup"
      aria-label={label}
      className={`grid ${COLUMNS[sections.length] ?? "grid-cols-2"} gap-1 rounded-xl bg-section p-1`}
    >
      {sections.map(({ name, title }) => (
        <button
          key={name}
          type="button"
          role="radio"
          aria-checked={section === name}
          // крупный шрифт телефона: одно длинное слово переносится с дефисом,
          // несколько — по пробелам
          className={`rounded-lg px-1 py-2 text-base break-words ${
            title.includes(" ") ? "" : "hyphens-auto"
          } ${
            section === name
              ? "bg-bg font-extrabold shadow-sm"
              : "font-semibold text-text"
          }`}
          onClick={() => onSection(name)}
        >
          {title}
        </button>
      ))}
    </div>
  );
}
