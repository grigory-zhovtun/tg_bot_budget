import { SECTIONS, type Section } from "./section";

interface Props {
  section: Section;
  onSection: (section: Section) => void;
}

/**
 * «Сегодня · Траты · Накопления · Впереди» — ширина по подписи; крупный шрифт
 * телефона (Android масштабирует текст) переносит вкладки на вторую строку,
 * а не выталкивает последнюю за плашку.
 */
export default function SectionSwitch({ section, onSection }: Props) {
  return (
    <div
      role="radiogroup"
      aria-label="Раздел сводки"
      className="flex flex-wrap gap-1 rounded-xl bg-section p-1"
    >
      {SECTIONS.map(({ name, title }) => (
        <button
          key={name}
          type="button"
          role="radio"
          aria-checked={section === name}
          className={`flex-auto whitespace-nowrap rounded-lg px-1.5 py-1.5 text-sm ${
            section === name
              ? "bg-bg font-semibold shadow-sm"
              : "font-medium text-hint"
          }`}
          onClick={() => onSection(name)}
        >
          {title}
        </button>
      ))}
    </div>
  );
}
