import { useEffect, useState, type ReactNode } from "react";

export type Tab = "entry" | "dashboard" | "forecast" | "plan";

const STROKE = {
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 2.4,
  strokeLinecap: "round",
  strokeLinejoin: "round",
} as const;

const ICONS: Record<Tab, ReactNode> = {
  entry: <path d="M12 5v14M5 12h14" />,
  dashboard: <path d="M5 19v-8M12 19V5M19 19v-5" />,
  forecast: (
    <>
      <rect x="4" y="5" width="16" height="15" rx="3" />
      <path d="M4 10h16M8 3v4M16 3v4" />
    </>
  ),
  plan: <path d="M9 7h10M9 12h10M9 17h10M5 7h.01M5 12h.01M5 17h.01" />,
};

export const TABS: { name: Tab; title: string }[] = [
  { name: "entry", title: "Ввод" },
  { name: "dashboard", title: "Сводка" },
  { name: "forecast", title: "Календарь" },
  { name: "plan", title: "План" },
];

/** Набирают ли сейчас текст: клавиатура телефона поднимает панель на поле ввода. */
function useTyping(): boolean {
  const [typing, setTyping] = useState(false);
  useEffect(() => {
    const text = (target: EventTarget | null) =>
      target instanceof HTMLInputElement ||
      target instanceof HTMLTextAreaElement ||
      target instanceof HTMLSelectElement;
    const on = (event: FocusEvent) => {
      if (text(event.target)) setTyping(true);
    };
    const off = (event: FocusEvent) => {
      if (text(event.target)) setTyping(false);
    };
    document.addEventListener("focusin", on);
    document.addEventListener("focusout", off);
    return () => {
      document.removeEventListener("focusin", on);
      document.removeEventListener("focusout", off);
    };
  }, []);
  return typing;
}

interface Props {
  tab: Tab;
  unsaved: boolean; // у «Плана» есть несохранённые правки — точка
  onTab: (tab: Tab) => void;
}

/**
 * Вкладки приложения — плавающая панель внизу, под большим пальцем: значок и
 * подпись, выбранная — фоном, цветом и жирностью. Ширина вкладки — по подписи:
 * ряд влезает в 320 px и при крупном шрифте телефона. Пока набирают текст,
 * панели нет — клавиатура подняла бы её на поле.
 */
export default function TabBar({ tab, unsaved, onTab }: Props) {
  const typing = useTyping();
  return (
    <nav
      className={`fixed inset-x-0 bottom-0 z-10 px-3 pt-2 pb-[max(0.75rem,env(safe-area-inset-bottom))] ${
        typing ? "hidden" : ""
      }`}
    >
      <div
        className="mx-auto flex max-w-md gap-0.5 rounded-full border-2 border-outline bg-section p-1.5 shadow-lg"
        role="tablist"
        aria-label="Разделы"
      >
        {TABS.map(({ name, title }) => (
          <button
            key={name}
            type="button"
            role="tab"
            aria-selected={tab === name}
            className={`flex flex-auto flex-col items-center gap-0.5 rounded-full px-0.5 py-1.5 text-tab ${
              tab === name
                ? "bg-bg font-extrabold text-link shadow-sm"
                : "font-semibold text-text"
            }`}
            onClick={() => onTab(name)}
          >
            <svg viewBox="0 0 24 24" className="size-6" aria-hidden="true">
              <g {...STROKE}>{ICONS[name]}</g>
            </svg>
            <span>
              {title}
              {name === "plan" && unsaved && (
                <span
                  role="img"
                  aria-label="есть несохранённые правки"
                  className="ml-1 inline-block size-1.5 rounded-full bg-link align-middle"
                />
              )}
            </span>
          </button>
        ))}
      </div>
    </nav>
  );
}
