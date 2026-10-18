import type { ReactNode } from "react";

interface Props {
  title: string;
  onClose: () => void;
  children: ReactNode;
}

/** Окно снизу поверх экрана: заголовок, «✕», содержимое. */
export default function Sheet({ title, onClose, children }: Props) {
  return (
    <div
      className="fixed inset-0 z-20 flex flex-col justify-end bg-black/50"
      role="dialog"
      aria-modal="true"
      aria-label={title}
    >
      <div className="flex-1" onClick={onClose} />
      {/* длинное содержимое прокручивается, а не сжимает кнопки (shrink-0 у детей) */}
      <div className="mx-auto flex max-h-[92vh] w-full max-w-md flex-col gap-3 overflow-y-auto rounded-t-3xl bg-bg p-4 pb-6 *:shrink-0">
        <div className="flex items-center justify-between gap-3">
          <h3 className="text-lg font-semibold">{title}</h3>
          <button
            type="button"
            className="text-xl text-hint"
            aria-label="Закрыть"
            onClick={onClose}
          >
            ✕
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}
