import { useEffect, useRef, useState } from "react";
import { ApiError, type Api, type SavingsScreen } from "../api";
import { money } from "../format";
import Sheet from "../plan/Sheet";
import { Good, Warn } from "./blocks";
import { ErrorLine, PRIMARY } from "./controls";
import EnoughLines from "./EnoughLines";
import { dayMonth, forecastLines, percentText } from "./piggyText";

interface Props {
  api: Api;
  data: SavingsScreen;
  onSaved: (fresh: SavingsScreen) => void;
  onClose: () => void;
}

const QUICK = [3, 5, 10, 15, 20];
const NBSP = "\u00a0";
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
            className={`rounded-full px-4 py-2 text-line font-extrabold ${
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
      {month && (
        <p className="text-center text-line">{`в месяц ≈ ${money(month.capacity)}`}</p>
      )}
      {ordered.length > 0 && (
        <ul
          aria-label="К срокам"
          className="flex flex-col overflow-hidden rounded-xl border border-outline text-line"
        >
          {ordered.map((piggy) => {
            const { will, of } = forecastLines(piggy);
            return (
              <li
                key={piggy.id}
                className="flex flex-col gap-0.5 border-b border-outline px-3 py-2 last:border-b-0"
              >
                <p className="font-extrabold break-words">
                  {`${piggy.name}, к ${dayMonth(piggy.deadline ?? "")}`}
                </p>
                <p>{will}</p>
                <p>{of}</p>
              </li>
            );
          })}
        </ul>
      )}
      {needed !== null && month && month.percent < needed && (
        <Warn>{`К срокам нужно ${needed}${NBSP}%`}</Warn>
      )}
      {needed !== null && month && month.percent >= needed && (
        <Good>Успеваем всё к срокам</Good>
      )}
      {shown.enough && <EnoughLines enough={shown.enough} />}
      <ErrorLine text={error} />
      <button type="button" disabled={busy} className={PRIMARY} onClick={save}>
        {`Сохранить ${percentText(value)} %`}
      </button>
    </Sheet>
  );
}
