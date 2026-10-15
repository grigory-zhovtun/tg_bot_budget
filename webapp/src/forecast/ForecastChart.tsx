import { useId } from "react";
import type { ForecastDay } from "../api";
import { geometry, labelDays } from "./chart";

const WIDTH = 320;
const HEIGHT = 150;
const BOX = { width: WIDTH, top: 14, bottom: HEIGHT - 18, left: 8, right: 8 };

interface Props {
  days: ForecastDay[];
  marks: { day: number; up: boolean }[]; // ▲ поступление, ▼ платёж
  selected: number;
  label: string;
  onSelect: (day: number) => void;
}

/** Сплошная — по плану, пунктир — по темпу; ниже нуля линии красные, нажатие выбирает день. */
export default function ForecastChart({
  days,
  marks,
  selected,
  label,
  onSelect,
}: Props) {
  const clip = `below-${useId().replace(/[^a-zA-Z0-9_-]/g, "")}`;
  const first = days[0];
  if (!first) return null;
  const last = days[days.length - 1] ?? first;
  const chart = geometry(days, BOX);
  const point = days.find((item) => item.day === selected) ?? first;
  const anchor = (day: number) => {
    if (first.day === last.day) return "middle";
    if (day === first.day) return "start";
    return day === last.day ? "end" : "middle";
  };
  return (
    <svg
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      className="w-full overflow-visible"
      role="img"
      aria-label={label}
    >
      <defs>
        <clipPath id={clip}>
          <rect
            x={0}
            y={chart.zero}
            width={WIDTH}
            height={Math.max(BOX.bottom - chart.zero, 0)}
          />
        </clipPath>
      </defs>
      {chart.below && (
        <rect
          x={0}
          y={chart.zero}
          width={WIDTH}
          height={BOX.bottom - chart.zero}
          fill="var(--color-over)"
          opacity={0.08}
        />
      )}
      <line
        x1={0}
        x2={WIDTH}
        y1={chart.zero}
        y2={chart.zero}
        stroke="var(--color-hint)"
        opacity={0.5}
      />
      <line
        x1={chart.x(point.day)}
        x2={chart.x(point.day)}
        y1={BOX.top}
        y2={BOX.bottom}
        stroke="var(--color-hint)"
        strokeDasharray="2 3"
      />
      <path
        d={chart.plan}
        fill="none"
        stroke="var(--color-link)"
        strokeWidth={2}
      />
      <path
        d={chart.pace}
        fill="none"
        stroke="var(--color-warn)"
        strokeWidth={2}
        strokeDasharray="5 4"
      />
      {chart.below && (
        <g clipPath={`url(#${clip})`}>
          <path
            d={chart.plan}
            fill="none"
            stroke="var(--color-over)"
            strokeWidth={2}
          />
          <path
            d={chart.pace}
            fill="none"
            stroke="var(--color-over)"
            strokeWidth={2}
            strokeDasharray="5 4"
          />
        </g>
      )}
      {marks.map((mark) => {
        const at = days.find((item) => item.day === mark.day);
        if (!at) return null;
        const y = chart.y(at.plan);
        return (
          <text
            key={`${mark.day}-${mark.up ? "up" : "down"}`}
            x={chart.x(mark.day)}
            y={mark.up ? y - 6 : y + 12}
            fontSize={9}
            textAnchor="middle"
            fill={mark.up ? "var(--color-ok)" : "var(--color-over)"}
          >
            {mark.up ? "▲" : "▼"}
          </text>
        );
      })}
      <circle
        cx={chart.x(first.day)}
        cy={chart.y(first.plan)}
        r={3}
        fill="var(--color-button)"
      />
      <circle
        cx={chart.x(point.day)}
        cy={chart.y(point.plan)}
        r={3.5}
        fill="var(--color-link)"
      />
      <circle
        cx={chart.x(point.day)}
        cy={chart.y(point.pace)}
        r={3.5}
        fill="var(--color-warn)"
      />
      {labelDays(first.day, last.day).map((day) => (
        <text
          key={`label-${day}`}
          x={chart.x(day)}
          y={HEIGHT - 2}
          fontSize={10}
          textAnchor={anchor(day)}
          fill={day === first.day ? "var(--color-text)" : "var(--color-hint)"}
          fontWeight={day === first.day ? 700 : 400}
        >
          {day}
        </text>
      ))}
      {days.map((item) => (
        <rect
          key={`zone-${item.day}`}
          data-day={item.day}
          x={chart.x(item.day) - chart.slot / 2}
          y={0}
          width={chart.slot}
          height={HEIGHT}
          fill="transparent"
          onClick={() => onSelect(item.day)}
        />
      ))}
    </svg>
  );
}
