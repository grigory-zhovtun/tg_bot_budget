import type { SavingMonth, Savings } from "../api";
import { money } from "../format";
import { monthName, monthRange, monthShort } from "../plan/months";
import { Card, Chip, Line } from "./blocks";

const GOOD_PERCENT = 10; // от 10 % — мятным: «сначала заплати себе»
const MAX_PERCENT = 999; // дальше — «< −999 %»: подпись не ломает строку

/** Доля в целых процентах, как на экране: 0,096 → 10. */
function percentOf(rate: number): number {
  return Math.round(rate * 100);
}

/** 0.25 → «+25 %», −1.41 → «−141 %», −79 → «< −999 %», null → «—». */
function rateText(rate: number | null): string {
  if (rate === null) return "—";
  const percent = percentOf(rate);
  const tail = "\u00a0%";
  if (percent > MAX_PERCENT) return `> +${MAX_PERCENT}${tail}`;
  if (percent < -MAX_PERCENT) return `< −${MAX_PERCENT}${tail}`;
  const sign = percent > 0 ? "+" : percent < 0 ? "−" : "";
  return `${sign}${Math.abs(percent)}${tail}`;
}

/** По числу на экране: от 10 % — мятный, от нуля — жёлтый, минус — оранжевый. */
function rateTone(rate: number | null): { bar: string; border: string } {
  if (rate === null) return { bar: "", border: "" };
  const percent = percentOf(rate);
  if (percent >= GOOD_PERCENT) return { bar: "bg-ok", border: "border-ok" };
  if (percent >= 0) return { bar: "bg-warn", border: "border-warn" };
  return { bar: "bg-danger", border: "border-danger" };
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
 * всю высоту: запас проели, это не «нет данных». Числа — в aria-label: над
 * столбиками их нет. Текущий месяц — пунктиром, без заливки, с подписью «пока».
 */
function MonthBar({ item, current }: { item: SavingMonth; current: boolean }) {
  const eaten = item.rate === null && item.spent > 0;
  const rate = eaten ? -1 : item.rate;
  const tone = rateTone(rate);
  const height = rate === null ? 0 : Math.min(Math.abs(rate), 1) * 50;
  const result = item.rate === null ? "дохода не было" : rateText(item.rate);
  const name = `${monthName(item.month)}${current ? " пока" : ""}`;
  const look = current ? `border-2 border-dashed ${tone.border}` : tone.bar;
  return (
    <div
      role="img"
      aria-label={`${name}: пришло ${amountText(item.income)}, потрачено ${amountText(item.spent)}, ${result}`}
      data-current={current ? "" : undefined}
      className="flex flex-col items-center gap-1"
    >
      <div className="relative h-16 w-full">
        <div className="absolute inset-x-0 top-1/2 h-0.5 bg-outline" />
        {rate !== null && height > 0 && (
          <div
            data-bar={rate >= 0 ? "up" : "down"}
            className={`absolute inset-x-2 rounded-sm ${look}`}
            style={{
              [rate >= 0 ? "bottom" : "top"]: "50%",
              height: `max(${height}%, 3px)`,
            }}
          />
        )}
      </div>
      <span className="text-[15px] font-bold">
        {current ? "пока" : monthShort(item.month)}
      </span>
    </div>
  );
}

/** Итог трёх месяцев: «траты больше дохода на X» или «отложено X — N % дохода». */
function total(range: string, income: number, spent: number): string {
  if (spent > income) {
    return `за ${range} траты больше дохода на ${money(spent - income)}`;
  }
  const percent = Math.min(percentOf((income - spent) / income), MAX_PERCENT);
  return `за ${range} отложено ${money(income - spent)} — ${percent}\u00a0% дохода`;
}

/** «Норма сбережений»: доля дохода, которая осталась, — за 3 месяца с причиной
 * минуса, за полгода, по месяцам и в текущем месяце «пока». */
export default function SavingsCard({ savings }: { savings: Savings }) {
  const recent = savings.months.slice(-3);
  const range = monthRange(recent.map((item) => item.month));
  const income = recent.reduce((sum, item) => sum + item.income, 0);
  const spent = recent.reduce((sum, item) => sum + item.spent, 0);
  const worst = savings.months.find((item) => item.month === savings.worst);
  const current = savings.current;
  return (
    <Card label="Норма сбережений">
      <div className="flex flex-wrap items-center justify-between gap-x-2 gap-y-1">
        <h2 className="text-fact font-extrabold">Норма сбережений</h2>
        {savings.rate !== null && <RateChip rate={savings.rate} />}
      </div>
      <Line>
        {savings.rate === null
          ? `за ${range} дохода не было`
          : total(range, income, spent)}
      </Line>
      {worst && (
        <Line>
          {`больше всего — ${monthName(worst.month)}: потрачено ${amountText(worst.spent)}, пришло ${amountText(worst.income)}`}
        </Line>
      )}
      <Line>
        {savings.half_rate === null
          ? "за полгода дохода не было"
          : `за полгода ${rateText(savings.half_rate)}`}
      </Line>
      <div
        className={`mt-2 grid ${current ? "grid-cols-7" : "grid-cols-6"} gap-1`}
      >
        {savings.months.map((item) => (
          <MonthBar key={item.month} item={item} current={false} />
        ))}
        {current && <MonthBar item={current} current />}
      </div>
      <Line>вверх — отложили, вниз — проели</Line>
      {current && (
        <Line>
          {current.rate === null
            ? `${monthName(current.month)} пока: дохода ещё не было`
            : `${monthName(current.month)} пока ${rateText(current.rate)}`}
        </Line>
      )}
    </Card>
  );
}
