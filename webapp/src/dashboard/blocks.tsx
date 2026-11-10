/** Кирпичи крупной «Сводки»: главная цифра, крупная строка, пояснения, плашки, таблицы. */
import type { ReactNode } from "react";

interface Children {
  children: ReactNode;
}

/** Карточка раздела. */
export function Card({ label, children }: Children & { label?: string }) {
  return (
    <section
      aria-label={label}
      className="flex flex-col gap-1 rounded-2xl bg-section p-4"
    >
      {children}
    </section>
  );
}

const BIG_FITS = 13; // «9 500 000 сум» — ещё 40 px; длиннее — 34 px
const HERO_FITS = 11; // «412 000 сум» — 52 px по центру экрана

/** Главная цифра — 40 px; длинная (восьмизначная сумма с «сум») — 34 px, чтобы
 * осталась в одной строке; hero — по центру экрана: короткая — 52 px. */
export function Big({ children, hero = false }: Children & { hero?: boolean }) {
  const length = typeof children === "string" ? children.length : 0;
  let size = "text-big";
  if (length > BIG_FITS) size = "text-big-long";
  else if (hero && length <= HERO_FITS) size = "text-big-hero";
  return <p className={`${size} font-black tabular-nums`}>{children}</p>;
}

/** Заголовок блока «Сводки» — цветом ссылки: блоки видны при прокрутке. */
export function Title({ children }: Children) {
  return <h2 className="text-fact font-extrabold text-link">{children}</h2>;
}

/** Круглый значок строки: белый знак на цветном круге. */
export function Round({ tone, children }: Children & { tone: string }) {
  return (
    <span
      aria-hidden="true"
      className={`grid size-11 shrink-0 place-items-center rounded-full text-sm font-black text-white ${tone}`}
    >
      {children}
    </span>
  );
}

/** Крупная строка — 21 px: с неё начинается карточка или пункт. */
export function Fact({ children }: Children) {
  return <p className="text-fact font-extrabold">{children}</p>;
}

/** Строка пояснения — 17 px, цветом текста, не бледная. */
export function Line({ children }: Children) {
  return <p className="text-line">{children}</p>;
}

/** Предупреждение: «⚠️ …» жирно, оранжевым. */
export function Warn({ children }: Children) {
  return <p className="text-line font-extrabold text-danger">⚠️ {children}</p>;
}

/** Хорошая новость: «✓ …» жирно, зелёным. */
export function Good({ children }: Children) {
  return <p className="text-line font-extrabold text-ok">✓ {children}</p>;
}

const TONE = {
  good: "bg-chip-good",
  mid: "bg-chip-mid",
  bad: "bg-chip-bad",
} as const;

/** Плашка статуса: белый жирный текст на цветном фоне; знак — в тексте;
 * small — для подкатегорий, на ступень мельче. */
export function Chip({
  tone,
  small = false,
  children,
}: Children & { tone: keyof typeof TONE; small?: boolean }) {
  return (
    <span
      className={`inline-block shrink-0 rounded-lg px-2.5 py-0.5 font-black whitespace-nowrap text-white ${small ? "text-line" : "text-fact"} ${TONE[tone]}`}
    >
      {children}
    </span>
  );
}

interface TableProps {
  rows: ReactNode[][];
  align: string; // по колонкам: «l» — влево, «r» — вправо (числа)
  head?: ReactNode[];
  label?: string;
}

/** Таблица с рамкой и полосами: текст жирный, числа вправо и без переноса. */
export function Table({ rows, align, head, label }: TableProps) {
  const side = (index: number) =>
    align[index] === "r"
      ? "text-right whitespace-nowrap tabular-nums"
      : "text-left";
  return (
    <table aria-label={label} className="w-full border-collapse text-line">
      {head && (
        <thead>
          <tr>
            {head.map((cell, index) => (
              <th
                key={index}
                className={`border border-outline bg-stripe px-1.5 py-1.5 text-base font-extrabold ${side(index)}`}
              >
                {cell}
              </th>
            ))}
          </tr>
        </thead>
      )}
      <tbody>
        {rows.map((row, at) => (
          <tr key={at} className="even:bg-stripe">
            {row.map((cell, index) => (
              <td
                key={index}
                className={`border border-outline px-1.5 py-1.5 font-bold ${side(index)}`}
              >
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
