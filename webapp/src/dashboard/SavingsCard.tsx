import type { SavingMonth, Savings } from "../api";
import { short } from "../format";
import { monthName, monthRange, monthShort } from "../plan/months";

const GOOD_PERCENT = 10; // от 10 % — мятным: «сначала заплати себе»
const MAX_PERCENT = 999; // дальше — «< −999 %»: подпись не ломает колонку

/** Доля в целых процентах, как на экране: 0,096 → 10. */
function percentOf(rate: number): number {
  return Math.round(rate * 100);
}

/** 0.25 → «+25 %», −1.41 → «−141 %», −79 → «< −999 %», null → «—». */
function rateText(rate: number | null): string {
  if (rate === null) return "—";
  const percent = percentOf(rate);
  if (percent > MAX_PERCENT) return `> +${MAX_PERCENT}\u00a0%`;
  if (percent < -MAX_PERCENT) return `< −${MAX_PERCENT}\u00a0%`;
  const sign = percent > 0 ? "+" : percent < 0 ? "−" : "";
  return `${sign}${Math.abs(percent)}\u00a0%`;
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
  return value > 0 ? short(value) : "0";
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
        className={`text-[13px] font-bold whitespace-nowrap ${rateTone(item.rate).text}`}
      >
        {rateText(item.rate)}
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
      <span className="text-[13px] font-semibold">
        {monthShort(item.month)}
      </span>
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
    <section
      className="flex flex-col gap-2 rounded-2xl bg-section p-4"
      aria-label="Норма сбережений"
    >
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">Норма сбережений</h2>
        <span
          className={`text-2xl font-extrabold ${rateTone(savings.rate).text}`}
        >
          {rateText(savings.rate)}
        </span>
      </div>
      <p className="text-sm">
        {savings.rate === null
          ? `за ${range} дохода не было`
          : `за ${range}: пришло ${amountText(income)}, потрачено ${amountText(spent)}`}
      </p>
      <div className="grid grid-cols-6 gap-1">
        {savings.months.map((item) => (
          <MonthBar key={item.month} item={item} />
        ))}
      </div>
    </section>
  );
}
