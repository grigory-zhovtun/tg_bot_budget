/** Геометрия графика «До конца месяца»: две линии по дням и нулевая линия. */

import type { ForecastDay } from "../api";

export interface Box {
  width: number;
  top: number; // верх шкалы
  bottom: number; // низ шкалы
  left: number;
  right: number;
}

export interface Geometry {
  x: (day: number) => number;
  y: (value: number) => number;
  zero: number; // y нулевой линии
  plan: string; // путь линии «по плану»
  pace: string; // путь линии «по темпу»
  below: boolean; // есть точки ниже нуля
  slot: number; // ширина дня — для зон нажатия
}

const round = (value: number) => Math.round(value * 10) / 10; // SVG хватит десятых

/** Шкала от минимума (не выше нуля) до максимума (не ниже нуля); один день — посередине. */
export function geometry(days: ForecastDay[], box: Box): Geometry {
  const values = days.flatMap((point) => [point.plan, point.pace]);
  const max = Math.max(0, ...values);
  const min = Math.min(0, ...values);
  const span = max - min || 1;
  const first = days[0]?.day ?? 1;
  const last = days[days.length - 1]?.day ?? first;
  const inner = box.width - box.left - box.right;
  const slot = last > first ? inner / (last - first) : inner;
  const x = (day: number) =>
    round(box.left + (last > first ? (day - first) * slot : inner / 2));
  const y = (value: number) =>
    round(box.top + ((max - value) / span) * (box.bottom - box.top));
  const path = (pick: (point: ForecastDay) => number) =>
    days
      .map(
        (point, index) =>
          `${index === 0 ? "M" : "L"}${x(point.day)},${y(pick(point))}`,
      )
      .join(" ");
  return {
    x,
    y,
    zero: y(0),
    plan: path((point) => point.plan),
    pace: path((point) => point.pace),
    below: min < 0,
    slot: round(slot),
  };
}

/** Подписи под графиком: сегодня, каждое пятое число и последний день, не теснее трёх дней. */
export function labelDays(first: number, last: number): number[] {
  const days = [first];
  for (let day = 5; day < last; day += 5) {
    if (day - (days[days.length - 1] ?? first) >= 3) days.push(day);
  }
  const tail = days[days.length - 1] ?? first;
  if (last !== tail) {
    if (last - tail < 3 && days.length > 1) days.pop();
    days.push(last);
  }
  return days;
}
