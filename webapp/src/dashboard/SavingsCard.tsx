import type { SavingMonth, Savings } from "../api";
import { money } from "../format";
import { monthName, monthRange, monthShort } from "../plan/months";
import { Card, Chip, Line } from "./blocks";

const GOOD_PERCENT = 10; // от 10 % — мятным: «сначала заплати себе»
const MAX_PERCENT = 999; // дальше — «< −999 %»: подпись не ломает колонку

/** Доля в целых процентах, как на экране: 0,096 → 10. */
function percentOf(rate: number): number {
  return Math.round(rate * 100);
}

/** 0.25 → «+25 %», −1.41 → «−141 %», −79 → «< −999 %», null → «—»;
 * unit = false — без «%»: так подпись влезает над столбиком крупно. */
function rateText(rate: number | null, unit = true): string {
  if (rate === null) return "—";
  const percent = percentOf(rate);
  const tail = unit ? "\u00a0%" : "";
  if (percent > MAX_PERCENT) return `> +${MAX_PERCENT}${tail}`;
  if (percent < -MAX_PERCENT) return `< −${MAX_PERCENT}${tail}`;
  const sign = percent > 0 ? "+" : percent < 0 ? "−" : "";
  return `${sign}${Math.abs(percent)}${tail}`;
}

/** По числу на экране: от 10 % — мятный, от нуля — жёлтый, минус — оранжевый. */
function rateTone(rate: number | null): { text: string; bar: string } {
  if (rate === null) return { text: "", bar: "" };
  const percent = percentOf(rate);
  if (percent >= GOOD_PERCENT) return { text: "text-ok", bar: "bg-ok" };
  if (percent >= 0) return { text: "text-warn", bar: "bg-warn" };
  return { text: "text-danger", bar: "bg-danger" };
}

function amountText(value: number): string {
  return value > 0 ? money(value) : "0";
}

/** Плашка нормы за три месяца: «✓ 25 %» от 10 %, «5 %» от нуля, «⚠️ −10 %» в минусе. */
function RateChip({ rate }: { rate: number }) {
  const percent = Math.max(
    -MAX_PERCENT,
    Math.min(percentOf(rate), MAX_PERCENT),
  );
  if (percent >= GOOD_PERCENT) {
    return <Chip tone="good">{`✓ ${percent}\u00a0%`}</Chip>;
  }
  if (percent >= 0) return <Chip tone="mid">{`${percent}\u00a0%`}</Chip>;
  return <Chip tone="bad">{`⚠️ −${Math.abs(percent)}\u00a0%`}</Chip>;
}

/**
 * Столбик месяца: вверх — отложили, вниз — проели запас; дальше ±100 % не
 * растёт, маленькая доля — не тоньше 3 px. Траты без дохода — столбик вниз во
 * всю высоту: запас проели, это не «нет данных».
 */
function MonthBar({ item }: { item: SavingMonth }) {
  const eaten = item.rate === null && item.spent > 0;
  const rate = eaten ? -1 : item.rate;
  const tone = rateTone(rate);
  const height = rate === null ? 0 : Math.min(Math.abs(rate), 1) * 50;
  const result = item.rate === null ? "дохода не было" : rateText(item.rate);
  return (
    <div
      role="img"
      aria-label={`${monthName(item.month)}: пришло ${amountText(item.income)}, потрачено ${amountText(item.spent)}, ${result}`}
      className="flex flex-col items-center gap-1"
    >
      <span
        className={`text-[15px] font-extrabold whitespace-nowrap ${rateTone(item.rate).text}`}
      >
        {rateText(item.rate, false)}
      </span>
      <div className="relative h-16 w-full">
        <div className="absolute inset-x-0 top-1/2 h-0.5 bg-outline" />
        {rate !== null && height > 0 && (
          <div
            data-bar={rate >= 0 ? "up" : "down"}
            className={`absolute inset-x-2 rounded-sm ${tone.bar}`}
            style={{
              [rate >= 0 ? "bottom" : "top"]: "50%",
              height: `max(${height}%, 3px)`,
            }}
          />
        )}
      </div>
      <span className="text-[15px] font-bold">{monthShort(item.month)}</span>
    </div>
  );
}

/** «Норма сбережений»: доля дохода, которая осталась, — за 3 месяца и по месяцам. */
export default function SavingsCard({ savings }: { savings: Savings }) {
  const recent = savings.months.slice(-3);
  const range = monthRange(recent.map((item) => item.month));
  const income = recent.reduce((sum, item) => sum + item.income, 0);
  const spent = recent.reduce((sum, item) => sum + item.spent, 0);
  return (
    <Card label="Норма сбережений">
      <h2 className="text-fact font-extrabold">Норма сбережений</h2>
      {savings.rate !== null && (
        <div>
          <RateChip rate={savings.rate} />
        </div>
      )}
      {savings.rate === null ? (
        <Line>{`за ${range} дохода не было`}</Line>
      ) : (
        <>
          <Line>{`за ${range}`}</Line>
          <Line>{`пришло ${amountText(income)}`}</Line>
          <Line>{`потрачено ${amountText(spent)}`}</Line>
        </>
      )}
      <div className="mt-2 grid grid-cols-6 gap-1">
        {savings.months.map((item) => (
          <MonthBar key={item.month} item={item} />
        ))}
      </div>
      <Line>числа над столбиками — в %</Line>
    </Card>
  );
}
