import type { DayPoint } from "../api";
import { short } from "../format";
import { chartPaths } from "./chart";

const WIDTH = 320;
const HEIGHT = 140;

export default function BalanceChart({ points }: { points: DayPoint[] }) {
  if (points.length === 0) return null;
  const { plan, fact, min, max } = chartPaths(points, WIDTH, HEIGHT);
  return (
    <figure className="m-0 flex flex-col gap-2">
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        className="h-36 w-full overflow-visible"
        role="img"
        aria-label="Остаток по дням: план и факт"
      >
        <path
          d={plan}
          fill="none"
          stroke="var(--color-hint)"
          strokeWidth={2}
          strokeDasharray="4 4"
        />
        <path
          d={fact}
          fill="none"
          stroke="var(--color-button)"
          strokeWidth={3}
        />
      </svg>
      <figcaption className="flex justify-between text-xs text-hint">
        <span>пунктир — план, линия — факт</span>
        <span>
          {short(min)} … {short(max)}
        </span>
      </figcaption>
    </figure>
  );
}
