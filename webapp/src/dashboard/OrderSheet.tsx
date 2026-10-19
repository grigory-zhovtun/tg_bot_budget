import { useEffect, useRef, useState } from "react";
import { ApiError, type Api, type SavingsScreen } from "../api";
import Sheet from "../plan/Sheet";
import { Line } from "./blocks";
import { dayMonth, forecastLines } from "./piggyText";

interface Props {
  api: Api;
  data: SavingsScreen;
  onSaved: (fresh: SavingsScreen) => void;
  onRefresh?: (fresh: SavingsScreen) => void; // экран перечитан, окно открыто
  onClose: () => void;
}

const PAUSE_MS = 300; // предпросмотр — после паузы в нажатиях
const CHANGED = "Копилки поменялись — проверьте порядок и сохраните ещё раз";

/** ID копилок порядка — самая важная первой. */
function orderOf(screen: SavingsScreen): string[] {
  return screen.piggies
    .filter((piggy) => piggy.order !== null && piggy.status !== "broken")
    .sort((a, b) => (a.order ?? 0) - (b.order ?? 0))
    .map((piggy) => piggy.id);
}

/** Окно «Порядок важности»: стрелки ↑ ↓ и что каждая копилка успеет при этом
 * порядке. */
export default function OrderSheet({
  api,
  data,
  onSaved,
  onRefresh,
  onClose,
}: Props) {
  const startIds = orderOf(data);
  const startKey = startIds.join(",");
  const [ids, setIds] = useState<string[]>(startIds);
  const [view, setView] = useState<SavingsScreen>(data);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const asked = useRef(0);
  useEffect(() => {
    if (ids.join(",") === startKey) return;
    const ticket = ++asked.current;
    const timer = setTimeout(() => {
      api.savings(false, { order: ids }).then(
        (fresh) => {
          if (asked.current === ticket) setView(fresh);
        },
        () => undefined, // предпросмотр не удался — остаётся прошлый
      );
    }, PAUSE_MS);
    return () => clearTimeout(timer);
  }, [api, ids, startKey]);
  const shown = ids.join(",") === startKey ? data : view;
  const byId = new Map(shown.piggies.map((piggy) => [piggy.id, piggy]));

  function move(index: number, shift: number) {
    setIds((current) => {
      const next = [...current];
      const [moved] = next.splice(index, 1);
      if (moved === undefined) return current;
      next.splice(index + shift, 0, moved);
      return next;
    });
  }

  function save() {
    if (busy) return;
    setBusy(true);
    setError(null);
    api.setOrder(ids).then(onSaved, (reason: unknown) => {
      if (reason instanceof ApiError && reason.code === "piggies_changed") {
        // набор копилок поменяли с другого телефона — перечитать и показать свежий
        api.savings(true).then(
          (fresh) => {
            onRefresh?.(fresh);
            setView(fresh);
            setIds(orderOf(fresh));
            setBusy(false);
            setError(CHANGED);
          },
          () => {
            setBusy(false);
            setError(reason.message);
          },
        );
        return;
      }
      setBusy(false);
      setError(
        reason instanceof ApiError
          ? reason.message
          : "Не получилось сохранить — попробуйте ещё раз",
      );
    });
  }

  const arrow =
    "flex size-11 shrink-0 items-center justify-center rounded-xl bg-bg text-[26px] font-black disabled:opacity-30";
  return (
    <Sheet title="Порядок важности" onClose={onClose}>
      <div>
        <Line>если на все сроки не хватает,</Line>
        <Line>первой собирается копилка сверху</Line>
      </div>
      <ul aria-label="Порядок важности" className="flex flex-col gap-2">
        {ids.map((id, index) => {
          const piggy = byId.get(id);
          if (!piggy) return null;
          const { will, of } = forecastLines(
            piggy,
            `к ${dayMonth(piggy.deadline ?? "")}`,
          );
          return (
            <li
              key={id}
              className="flex items-center gap-3 rounded-2xl bg-section p-3 text-line"
            >
              {/* стрелки — столбиком справа: строкам прогноза остаётся почти вся ширина */}
              <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                <div className="flex items-center gap-2">
                  <span className="inline-flex size-7 shrink-0 items-center justify-center rounded-full bg-text text-line font-black text-bg">
                    {index + 1}
                  </span>
                  <h4 className="min-w-0 text-fact font-extrabold break-words">
                    {piggy.name}
                  </h4>
                </div>
                <p>{will}</p>
                <p>{of}</p>
              </div>
              <div className="flex flex-col gap-2">
                <button
                  type="button"
                  aria-label={`Выше «${piggy.name}»`}
                  disabled={index === 0}
                  className={arrow}
                  onClick={() => move(index, -1)}
                >
                  ↑
                </button>
                <button
                  type="button"
                  aria-label={`Ниже «${piggy.name}»`}
                  disabled={index === ids.length - 1}
                  className={arrow}
                  onClick={() => move(index, 1)}
                >
                  ↓
                </button>
              </div>
            </li>
          );
        })}
      </ul>
      {error && (
        <p className="text-line font-bold text-danger" role="alert">
          {error}
        </p>
      )}
      <button
        type="button"
        disabled={busy}
        className="h-12 rounded-xl bg-button text-lg font-extrabold text-button-text disabled:opacity-50"
        onClick={save}
      >
        Сохранить порядок
      </button>
    </Sheet>
  );
}
