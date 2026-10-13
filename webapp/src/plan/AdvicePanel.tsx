import type { AdvicePoint } from "../api";

export type AdviceState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; points: AdvicePoint[] };

interface Props {
  state: AdviceState;
  applyLabel: (point: AdvicePoint) => string | null; // null — применять нечего
  applied: string[]; // тексты пунктов, которые уже применили
  onApply: (point: AdvicePoint) => void;
  onClose: () => void;
}

/** Панель советника: пункты по черновику; «Применить» меняет только черновик. */
export default function AdvicePanel({
  state,
  applyLabel,
  applied,
  onApply,
  onClose,
}: Props) {
  return (
    <section
      className="flex flex-col gap-2 rounded-2xl bg-section p-4 text-sm"
      aria-label="Совет по плану"
    >
      <div className="flex items-center justify-between">
        <h3 className="font-semibold">🧠 Совет по плану</h3>
        <button
          type="button"
          className="text-hint"
          aria-label="Скрыть совет"
          onClick={onClose}
        >
          ✕
        </button>
      </div>
      {state.kind === "loading" && (
        <p className="text-hint">Советник думает…</p>
      )}
      {state.kind === "error" && (
        <p className="text-danger" role="alert">
          {state.message}
        </p>
      )}
      {state.kind === "ready" && state.points.length === 0 && (
        <p className="text-hint">Советнику нечего добавить</p>
      )}
      {state.kind === "ready" && state.points.length > 0 && (
        <ol className="flex flex-col gap-3">
          {state.points.map((point, index) => {
            const label = applyLabel(point);
            const done = applied.includes(point.text);
            return (
              <li
                key={`${index}-${point.text}`}
                className="flex flex-col gap-1"
              >
                <p>{point.text}</p>
                {label && (
                  <button
                    type="button"
                    disabled={done}
                    className="self-start rounded-xl bg-bg px-3 py-1.5 text-link disabled:text-hint"
                    onClick={() => onApply(point)}
                  >
                    {done ? "✓ Применено" : label}
                  </button>
                )}
              </li>
            );
          })}
        </ol>
      )}
    </section>
  );
}
