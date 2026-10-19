import { SECTIONS, type Section } from "./section";

interface Props {
  section: Section;
  onSection: (section: Section) => void;
}

/**
 * «Сегодня · Траты» / «Накопления · Впереди» — два ряда по два: крупные подписи
 * не слипаются и при крупном шрифте телефона остаются в своей клетке.
 */
export default function SectionSwitch({ section, onSection }: Props) {
  return (
    <div
      role="radiogroup"
      aria-label="Раздел сводки"
      className="grid grid-cols-2 gap-1 rounded-xl bg-section p-1"
    >
      {SECTIONS.map(({ name, title }) => (
        <button
          key={name}
          type="button"
          role="radio"
          aria-checked={section === name}
          className={`rounded-lg px-2 py-2 text-base ${
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
