import type { DaySpend } from "../api";
import { money } from "../format";
import { days } from "../plan/months";
import { Card, Line } from "./blocks";
import {
  daysInMonth,
  spendingBars,
  spendingSummary,
  type Bar,
} from "./spending";

const WIDTH = 320;
const HEIGHT = 150;
const SIZE = { width: WIDTH, top: 18, bottom: HEIGHT - 16 };
const LABEL_DAYS = [1, 5, 10, 15, 20, 25];
const FONT = 15; // подписи графика — крупно, как строки «Сводки»
const CHAR = 8.4; // средняя ширина знака подписи при FONT, жирным

/** Примерная ширина подписи: хватает, чтобы не уйти за край графика. */
function labelWidth(text: string): number {
  return text.length * CHAR;
}

/** Середина подписи не ближе margin к краям: подпись не вылезает за график. */
function inside(x: number, margin: number): number {
  return Math.min(Math.max(x, margin), WIDTH - margin);
}

const FILL: Record<Bar["state"], string> = {
  over: "var(--color-over)",
  ok: "var(--color-ok)",
  today: "var(--color-button)",
  ahead: "none",
};

/** Подпись поверх столбиков: обводка цветом фона, чтобы читалась. */
const HALO = {
  stroke: "var(--color-bg)",
  strokeWidth: 3,
  paintOrder: "stroke",
} as const;

interface Props {
  spending: DaySpend[];
  today: string;
  limit: number | null;
  planPerDay: number | null;
}

/** Итог прошедших дней: запас или перерасход крупно, ниже — траты и план. */
function Summary({ spending, today, planPerDay }: Omit<Props, "limit">) {
  const summary = spendingSummary(spending, today, planPerDay);
  if (!summary) return null;
  const over = Math.round(summary.spent - summary.plan);
  let verdict = "✓ Ровно по плану";
  if (over > 0) verdict = `⚠️ Сверх на ${money(over)}`;
  if (over < 0) verdict = `✓ Запас ${money(-over)}`;
  return (
    <Card>
      <p
        className={`text-fact font-extrabold ${over > 0 ? "text-danger" : "text-ok"}`}
      >
        {verdict}
      </p>
      <Line>{`потрачено ${money(summary.spent)} за ${days(summary.days)}`}</Line>
      <Line>{`по плану ${money(summary.plan)}`}</Line>
    </Card>
  );
}

/** Траты каждого дня против лимита этого дня; впереди — сколько можно в день. */
export default function SpendingChart({
  spending,
  today,
  limit,
  planPerDay,
}: Props) {
  if (spending.length === 0) return null;
  const { bars, planY } = spendingBars(
    spending,
    today,
    limit,
    planPerDay,
    SIZE,
  );
  const now = Number(today.slice(8, 10));
  const month = daysInMonth(today);
  const slot = WIDTH / month;
  const done = bars.filter((bar) => bar.state === "over" || bar.state === "ok");
  const over = done.filter((bar) => bar.state === "over").length;
  const spent = bars.filter((bar) => bar.state !== "ahead");
  const peak = spent.reduce<Bar | null>(
    (top, bar) => (bar.value > (top?.value ?? 0) ? bar : top),
    null,
  );
  const ahead = bars.find((bar) => bar.state === "ahead");
  const aheadText =
    ahead && limit !== null ? `можно ${money(limit)} в день` : "лимита нет";
  // подпись про дни впереди — от сегодняшнего дня вправо, если влезает, иначе у края
  const aheadX = now * slot;
  const aheadLeft = aheadX + 2 + labelWidth(aheadText) <= WIDTH;
  const peakText = peak ? money(peak.value) : "";
  const labels = [...LABEL_DAYS, month].filter(
    (day) => Math.abs(day - now) >= 3,
  );
  const name =
    done.length > 0
      ? `Траты по дням: ${days(done.length)}, сверх лимита — ${over}`
      : "Траты по дням";
  return (
    <figure className="m-0 flex flex-col gap-2">
      <Summary spending={spending} today={today} planPerDay={planPerDay} />
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        className="w-full overflow-visible"
        role="img"
        aria-label={name}
      >
        {bars.map((bar) => (
          <rect
            key={bar.day}
            data-state={bar.state}
            x={bar.x}
            y={bar.y}
            width={bar.width}
            height={bar.height}
            rx={2}
            fill={FILL[bar.state]}
            stroke={bar.state === "ahead" ? "var(--color-hint)" : undefined}
            strokeOpacity={bar.state === "ahead" ? 0.7 : undefined}
            strokeDasharray={bar.state === "ahead" ? "2 3" : undefined}
          />
        ))}
        {bars
          .filter((bar) => bar.cut)
          .map((bar) => (
            <path
              key={`cut-${bar.day}`}
              d={`M${bar.x - 1},${bar.y + 7} l${bar.width + 2},-3 M${bar.x - 1},${bar.y + 11} l${bar.width + 2},-3`}
              stroke="var(--color-bg)"
              strokeWidth={2}
            />
          ))}
        {bars.map(
          (bar) =>
            bar.tick !== null && (
              <line
                key={`tick-${bar.day}`}
                x1={bar.x - 1.5}
                x2={bar.x + bar.width + 1.5}
                y1={bar.tick}
                y2={bar.tick}
                stroke="var(--color-text)"
                strokeWidth={2}
              />
            ),
        )}
        {planY !== null && (
          <line
            x1={0}
            x2={WIDTH}
            y1={planY}
            y2={planY}
            stroke="var(--color-hint)"
            strokeDasharray="4 3"
          />
        )}
        {peak && (
          <text
            x={inside(peak.x + peak.width / 2, labelWidth(peakText) / 2 + 2)}
            y={peak.y - 4}
            fontSize={FONT}
            fontWeight={700}
            textAnchor="middle"
            fill="var(--color-text)"
            {...HALO}
          >
            {peakText}
          </text>
        )}
        {now < month && limit !== null && (
          <text
            x={aheadLeft ? aheadX + 2 : WIDTH - 2}
            y={ahead ? ahead.y - 5 : SIZE.top + 8}
            fontSize={FONT}
            fontWeight={700}
            textAnchor={aheadLeft ? "start" : "end"}
            fill={ahead ? "var(--color-text)" : "var(--color-danger)"}
            {...HALO}
          >
            {aheadText}
          </text>
        )}
        {[...labels, now].map((day) => (
          <text
            key={`day-${day}`}
            x={inside((day - 0.5) * slot, 10)}
            y={HEIGHT - 2}
            fontSize={FONT}
            textAnchor="middle"
            fill={day === now ? "var(--color-text)" : "var(--color-hint)"}
            fontWeight={day === now ? 800 : 600}
          >
            {day}
          </text>
        ))}
      </svg>
      <figcaption className="flex flex-wrap gap-x-4 gap-y-1 text-line font-semibold">
        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <i className="inline-block size-3 rounded-sm bg-over" />
          сверх лимита дня
        </span>
        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <i className="inline-block size-3 rounded-sm bg-ok" />в лимите
        </span>
        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <i className="inline-block h-1 w-4 bg-text" />
          лимит дня
        </span>
        {planPerDay !== null && planPerDay > 0 && (
          <span className="flex items-center gap-1.5 whitespace-nowrap">
            <i className="inline-block w-4 border-t-2 border-dashed border-hint" />
            {`план ${money(planPerDay)} в день`}
          </span>
        )}
      </figcaption>
    </figure>
  );
}
