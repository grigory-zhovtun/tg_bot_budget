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

/** Главная цифра — 40 px. */
export function Big({ children }: Children) {
  return <p className="text-big font-black tabular-nums">{children}</p>;
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

export interface Item {
  key: string;
  title: ReactNode;
  left: ReactNode;
  right: ReactNode;
}

/** Пункты в две строки: название во всю ширину, под ним слева день, справа сумма. */
export function Items({ items, label }: { items: Item[]; label: string }) {
  return (
    <ul
      aria-label={label}
      className="flex flex-col overflow-hidden rounded-xl border border-outline text-line"
    >
      {items.map((item) => (
        <li
          key={item.key}
          className="flex flex-col gap-0.5 border-b border-outline px-3 py-2 last:border-b-0 even:bg-stripe"
        >
          <p className="font-extrabold">{item.title}</p>
          <div className="flex items-baseline justify-between gap-3">
            <span>{item.left}</span>
            <span className="font-extrabold whitespace-nowrap tabular-nums">
              {item.right}
            </span>
          </div>
        </li>
      ))}
    </ul>
  );
}
