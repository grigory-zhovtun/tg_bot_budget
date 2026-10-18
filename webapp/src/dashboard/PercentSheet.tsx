import { useEffect, useRef, useState } from "react";
import { ApiError, type Api, type SavingsScreen } from "../api";
import { short } from "../format";
import Sheet from "../plan/Sheet";
import { dayMonth, enoughText, forecastText, percentText } from "./piggyText";

interface Props {
  api: Api;
  data: SavingsScreen;
  onSaved: (fresh: SavingsScreen) => void;
  onClose: () => void;
}

const QUICK = [3, 5, 10, 15, 20];
const PAUSE_MS = 300; // предпросмотр — после паузы в нажатиях

function clamp(value: number): number {
  return Math.min(100, Math.max(0, Math.round(value * 10) / 10));
}

/** Окно «Откладываем с поступлений»: процент и что он даст копилкам к срокам. */
export default function PercentSheet({ api, data, onSaved, onClose }: Props) {
  const start = data.month?.percent ?? 0;
  const [value, setValue] = useState(start);
  const [view, setView] = useState<SavingsScreen>(data);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const asked = useRef(0);
  useEffect(() => {
    if (value === start) return;
    const ticket = ++asked.current;
    const timer = setTimeout(() => {
      api.savings(false, { percent: value }).then(
        (fresh) => {
          if (asked.current === ticket) setView(fresh);
        },
        () => undefined, // предпросмотр не удался — остаётся прошлый
      );
    }, PAUSE_MS);
    return () => clearTimeout(timer);
  }, [api, value, start]);
  const shown = value === start ? data : view;
  const month = shown.month;
  const ordered = shown.piggies
    .filter((piggy) => piggy.order !== null && piggy.status !== "broken")
    .sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
  const needed = month?.percent_needed ?? null;

  function save() {
    if (busy) return;
    setBusy(true);
    setError(null);
    api.setPercent(value).then(onSaved, (reason: unknown) => {
      setBusy(false);
      setError(
        reason instanceof ApiError
          ? (reason.fields.percent ?? reason.message)
          : "Не получилось сохранить — попробуйте ещё раз",
      );
    });
  }

  const step = "h-16 w-16 rounded-2xl bg-section text-3xl font-bold";
  return (
    <Sheet title="Откладываем с поступлений" onClose={onClose}>
      <div className="flex items-center justify-center gap-5">
        <button
          type="button"
          aria-label="Меньше на 1 %"
          className={step}
          onClick={() => setValue((current) => clamp(current - 1))}
        >
          −
        </button>
        <p
          aria-live="polite"
          className="min-w-28 text-center text-4xl font-bold"
        >
          {`${percentText(value)} %`}
        </p>
        <button
          type="button"
          aria-label="Больше на 1 %"
          className={step}
          onClick={() => setValue((current) => clamp(current + 1))}
        >
          +
        </button>
      </div>
      <div
        role="radiogroup"
        aria-label="Быстрый выбор"
        className="flex flex-wrap justify-center gap-2"
      >
        {QUICK.map((quick) => (
          <button
            key={quick}
            type="button"
            role="radio"
            aria-checked={value === quick}
            className={`rounded-full px-4 py-2 font-bold ${
              value === quick
                ? "bg-button text-button-text ring-2 ring-text"
                : "bg-section"
            }`}
            onClick={() => setValue(quick)}
          >
            {`${quick} %`}
          </button>
        ))}
      </div>
      {month && <p>{`в месяц ≈ ${short(month.capacity)}`}</p>}
      {ordered.length > 0 && (
        <ul
          aria-label="К срокам"
          className="flex flex-col gap-2 rounded-2xl bg-section p-3"
        >
          {ordered.map((piggy) => (
            <li key={piggy.id} className="flex justify-between gap-3">
              <span className="font-semibold">
                {`${piggy.name} · ${dayMonth(piggy.deadline ?? "")}`}
              </span>
              <span
                className={`text-right font-semibold ${
                  piggy.status === "short" ? "text-danger" : "text-ok"
                }`}
              >
                {forecastText(piggy)}
              </span>
            </li>
          ))}
        </ul>
      )}
      {needed !== null && month && month.percent < needed && (
        <p className="font-semibold text-warn">
          {`Чтобы успеть всё к срокам — ${needed} %`}
        </p>
      )}
      {needed !== null && month && month.percent >= needed && (
        <p className="font-semibold text-ok">Успеваем всё к срокам ✓</p>
      )}
      {shown.enough && (
        <p
          className={`text-sm font-semibold ${
            shown.enough.gap < 0 ? "text-danger" : "text-ok"
          }`}
        >
          {enoughText(shown.enough).replace("остаётся", "останется")}
        </p>
      )}
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
        {`Сохранить ${percentText(value)} %`}
      </button>
    </Sheet>
  );
}
