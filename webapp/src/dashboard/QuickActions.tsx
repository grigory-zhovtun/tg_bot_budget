import type { ReactNode } from "react";

/** Быстрое действие «Сводки»: что записать, не выбирая вкладку и группу. */
export type Action = "spend" | "income" | "transfer" | "save";

const STROKE = {
  fill: "none",
  strokeWidth: 2.4,
  strokeLinecap: "round",
  strokeLinejoin: "round",
} as const;

// цвет значка — как у сумм в календаре: траты, доходы, записи, платежи
const ACTIONS: Record<
  Action,
  { title: string; tone: string; icon: ReactNode }
> = {
  spend: {
    title: "Трата",
    tone: "text-spent",
    icon: (
      <>
        <circle cx="12" cy="12" r="9" />
        <path d="M8 12h8" />
      </>
    ),
  },
  income: {
    title: "Доход",
    tone: "text-earned",
    icon: (
      <>
        <circle cx="12" cy="12" r="9" />
        <path d="M8 12h8M12 8v8" />
      </>
    ),
  },
  transfer: {
    title: "Перевод",
    tone: "text-planned",
    icon: <path d="M6 8h12l-3-3M18 16H6l3 3" />,
  },
  save: {
    title: "Отложить",
    tone: "text-payment",
    icon: <path d="M12 4v11M8 11l4 4 4-4M5 19h14" />,
  },
};

interface Props {
  actions: Action[]; // какие доступны по справочнику, по порядку
  onAction: (action: Action) => void;
}

/**
 * Ряд быстрых действий под главной цифрой: значок и подпись, кнопки не меньше
 * пальца. Подпись сжимается с экраном и при крупном шрифте телефона переносится.
 */
export default function QuickActions({ actions, onAction }: Props) {
  if (actions.length === 0) return null;
  return (
    <div
      role="group"
      aria-label="Быстрые действия"
      className="grid auto-cols-fr grid-flow-col gap-1.5"
    >
      {actions.map((name) => {
        const { title, tone, icon } = ACTIONS[name];
        return (
          <button
            key={name}
            type="button"
            className="flex min-h-16 flex-col items-center justify-center gap-1 rounded-2xl bg-section px-0.5 py-2.5 text-action font-bold hyphens-auto break-words"
            onClick={() => onAction(name)}
          >
            <svg
              viewBox="0 0 24 24"
              className={`size-7 ${tone}`}
              aria-hidden="true"
            >
              <g stroke="currentColor" {...STROKE}>
                {icon}
              </g>
            </svg>
            {title}
          </button>
        );
      })}
    </div>
  );
}
