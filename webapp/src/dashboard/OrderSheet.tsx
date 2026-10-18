import { useEffect, useRef, useState } from "react";
import { ApiError, type Api, type SavingsScreen } from "../api";
import Sheet from "../plan/Sheet";
import { dayMonth, forecastText } from "./piggyText";

interface Props {
  api: Api;
  data: SavingsScreen;
  onSaved: (fresh: SavingsScreen) => void;
  onClose: () => void;
}

const PAUSE_MS = 300; // предпросмотр — после паузы в нажатиях

/** Окно «Порядок важности»: стрелки ↑ ↓ и что каждая копилка успеет при этом
 * порядке. */
export default function OrderSheet({ api, data, onSaved, onClose }: Props) {
  const startIds = data.piggies
    .filter((piggy) => piggy.order !== null && piggy.status !== "broken")
    .sort((a, b) => (a.order ?? 0) - (b.order ?? 0))
    .map((piggy) => piggy.id);
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
      setBusy(false);
      setError(
        reason instanceof ApiError
          ? reason.message
          : "Не получилось сохранить — попробуйте ещё раз",
      );
    });
  }

  const arrow =
    "flex h-11 w-11 items-center justify-center rounded-xl bg-bg text-2xl font-bold disabled:opacity-30";
  return (
    <Sheet title="Порядок важности" onClose={onClose}>
      <p>Если на все сроки не хватает, первой собирается копилка сверху.</p>
      <ul aria-label="Порядок важности" className="flex flex-col gap-2">
        {ids.map((id, index) => {
          const piggy = byId.get(id);
          if (!piggy) return null;
          return (
            <li
              key={id}
              className="flex items-center gap-3 rounded-2xl bg-section p-3"
            >
              <span className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-text text-sm font-bold text-bg">
                {index + 1}
              </span>
              <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                <h4 className="font-semibold">{piggy.name}</h4>
                <p className="text-sm">
                  {`к ${dayMonth(piggy.deadline ?? "")} · `}
                  <span
                    className={`font-semibold ${
                      piggy.status === "short" ? "text-danger" : "text-ok"
                    }`}
                  >
                    {`будет ${forecastText(piggy)}`}
                  </span>
                </p>
              </div>
              <div className="flex flex-col gap-1">
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
        <p className="text-sm text-danger" role="alert">
          {error}
        </p>
      )}
      <button
        type="button"
        disabled={busy}
        className="h-12 rounded-xl bg-button font-semibold text-button-text disabled:opacity-50"
        onClick={save}
      >
        Сохранить порядок
      </button>
    </Sheet>
  );
}
