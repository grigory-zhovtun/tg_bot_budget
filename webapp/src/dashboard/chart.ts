import type { DayPoint } from "../api";

export interface Paths {
  plan: string;
  fact: string;
  min: number;
  max: number;
}

/** SVG-линии остатка: план на весь месяц, факт — пока он есть. */
export function chartPaths(
  points: DayPoint[],
  width: number,
  height: number,
): Paths {
  if (points.length === 0) return { plan: "", fact: "", min: 0, max: 0 };
  const values = points.flatMap((p) =>
    p.fact === null ? [p.plan] : [p.plan, p.fact],
  );
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const x = (index: number) =>
    points.length > 1 ? (index / (points.length - 1)) * width : width / 2;
  const y = (value: number) => height - ((value - min) / span) * height;
  const line = (pairs: [number, number][]) =>
    pairs
      .map(
        ([px, py], index) =>
          `${index ? "L" : "M"}${px.toFixed(1)},${py.toFixed(1)}`,
      )
      .join(" ");
  const plan = line(
    points.map((p, index): [number, number] => [x(index), y(p.plan)]),
  );
  const fact = line(
    points.flatMap((p, index): [number, number][] =>
      p.fact === null ? [] : [[x(index), y(p.fact)]],
    ),
  );
  return { plan, fact, min, max };
}
