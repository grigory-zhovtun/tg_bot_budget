import type { Forecast } from "../api";
import { longDay } from "../format";
import {
  outlook,
  paceVerdict,
  perDayText,
  shortageText,
  spendPace,
  sumText,
  type SpendPace,
  type Verdict,
} from "./outlook";
import { chartNote, isoDay, viewCode, type View } from "./view";

interface PointProps {
  dot: string; // цвет точки
  tone: string; // цвет подписи
  title: string;
  value: string;
  pace?: string; // сколько будет, если тратить как сейчас
  last?: boolean;
}

/** Точка пути: подпись с днём, крупная сумма и, если есть, сумма по темпу. */
function Point({ dot, tone, title, value, pace, last = false }: PointProps) {
  return (
    <li className="flex gap-3">
      <div className="flex w-4 flex-col items-center" aria-hidden="true">
        <span className={`mt-1 h-3.5 w-3.5 shrink-0 rounded-full ${dot}`} />
        {!last && <span className="w-[3px] grow bg-outline" />}
      </div>
      <div className={`flex min-w-0 flex-col ${last ? "" : "pb-4"}`}>
        <span className={`text-sm font-bold ${tone}`}>{title}</span>
        <span className="text-3xl leading-tight font-extrabold">{value}</span>
        {pace !== undefined && (
          <span className="text-sm font-bold text-warn">
            {`если тратить как сейчас: ${pace}`}
          </span>
        )}
      </div>
    </li>
  );
}

const VERDICT_TONES: Record<Verdict["tone"], string> = {
  fast: "bg-chip-mid",
  slow: "bg-chip-good",
  even: "bg-chip-good",
};

/** Доля от большей из двух сумм, в процентах для ширины полосы. */
function share(value: number, top: number): string {
  if (top <= 0) return "0%";
  return `${Math.round((Math.max(value, 0) / top) * 10_000) / 100}%`;
}

interface PaceProps {
  pace: SpendPace;
  code: string;
}

/** Траты в день: полоса плана, полоса «сейчас уходит» и вывод одной строкой. */
function Pace({ pace, code }: PaceProps) {
  const top = Math.max(pace.plan, pace.pace);
  const verdict = paceVerdict(pace, code);
  const rows = [
    { name: "по плану", value: pace.plan, text: "text-link", bar: "bg-link" },
    {
      name: "сейчас уходит",
      value: pace.pace,
      text: "text-warn",
      bar: "bg-warn",
    },
  ];
  return (
    <>
      <div className="h-px bg-outline" aria-hidden="true" />
      <div className="flex items-baseline justify-between gap-2">
        <h3 className="font-semibold">Темп трат</h3>
        <span className="text-xs text-hint">в день</span>
      </div>
      {rows.map((row) => (
        <div key={row.name} className="flex flex-col gap-1.5">
          <div className="flex items-baseline justify-between gap-3">
            <span className={`font-bold ${row.text}`}>{row.name}</span>
            <span className="text-xl font-extrabold whitespace-nowrap">
              {perDayText(row.value, code)}
            </span>
          </div>
          <div
            data-testid="pace-bar"
            className={`h-4 rounded-full ${row.bar}`}
            style={{ width: share(row.value, top) }}
          />
        </div>
      ))}
      <p
        className={`rounded-xl px-3 py-2 font-bold text-white ${VERDICT_TONES[verdict.tone]}`}
      >
        {verdict.text}
      </p>
    </>
  );
}

interface Props {
  data: Forecast;
  view: View;
}

/**
 * «Деньги до конца месяца»: сколько сейчас, когда будет меньше всего и сколько
 * останется к концу месяца — по плану и если тратить как сейчас; ниже — траты в
 * день против плана. Нехватка денег — красной плашкой с белым текстом.
 */
export default function MonthOutlook({ data, view }: Props) {
  const found = outlook(data, view);
  if (!found) return null;
  const code = viewCode(view);
  const date = (day: number) => longDay(isoDay(data.today, day));
  const pace = spendPace(data, view);
  return (
    <section className="flex flex-col gap-3 rounded-2xl bg-section p-4">
      <div className="flex flex-col">
        <h2 className="font-semibold">Деньги до конца месяца</h2>
        <span className="text-xs text-hint">{chartNote(view)}</span>
      </div>
      <ol className="flex flex-col">
        <Point
          dot="bg-text"
          tone="text-hint"
          title={`Сейчас · ${date(found.today)}`}
          value={sumText(found.now, code)}
        />
        {found.low && (
          <Point
            dot="bg-payment"
            tone="text-payment"
            title={`Меньше всего · ${date(found.low.day)}`}
            value={sumText(found.low.plan, code)}
            pace={sumText(found.low.pace, code)}
          />
        )}
        <Point
          dot="bg-earned"
          tone="text-earned"
          title={`Конец месяца · ${date(found.end.day)}`}
          value={sumText(found.end.plan, code)}
          pace={sumText(found.end.pace, code)}
          last
        />
      </ol>
      <p
        className={`rounded-xl px-3 py-2 font-bold text-white ${
          found.short ? "bg-minus" : "bg-chip-good"
        }`}
      >
        {shortageText(found.short, data.today)}
      </p>
      {pace && <Pace pace={pace} code={code} />}
    </section>
  );
}
