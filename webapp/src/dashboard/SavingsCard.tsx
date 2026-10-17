import type { SavingMonth, Savings } from "../api";
import { short } from "../format";
import { monthName, monthRange, monthShort } from "../plan/months";

const GOOD_RATE = 0.1; // от 10 % — мятным: «сначала заплати себе»

/** 0.25 → «+25 %», −1.41 → «−141 %», null → «—». */
function rateText(rate: number | null): string {
  if (rate === null) return "—";
  const percent = Math.round(Math.abs(rate) * 100);
  const sign = percent === 0 ? "" : rate > 0 ? "+" : "−";
  return `${sign}${percent} %`;
}

/** От 10 % — мятный, от нуля — жёлтый, минус — оранжевый (не красный). */
function rateTone(rate: number | null): { text: string; bar: string } {
  if (rate === null) return { text: "", bar: "" };
  if (rate >= GOOD_RATE) return { text: "text-ok", bar: "bg-ok" };
  if (rate >= 0) return { text: "text-warn", bar: "bg-warn" };
  return { text: "text-danger", bar: "bg-danger" };
}

function amountText(value: number): string {
  return value > 0 ? short(value) : "0";
}

/** Столбик месяца: вверх — отложили, вниз — проели запас; дальше ±100 % не растёт. */
function MonthBar({ item }: { item: SavingMonth }) {
  const tone = rateTone(item.rate);
  const height = item.rate === null ? 0 : Math.min(Math.abs(item.rate), 1) * 50;
  const result = item.rate === null ? "дохода не было" : rateText(item.rate);
  return (
    <div
      role="img"
      aria-label={`${monthName(item.month)}: пришло ${amountText(item.income)}, потрачено ${amountText(item.spent)}, ${result}`}
      className="flex flex-col items-center gap-1"
    >
      <span className={`text-[13px] font-bold ${tone.text}`}>
        {rateText(item.rate)}
      </span>
      <div className="relative h-16 w-full">
        <div className="absolute inset-x-0 top-1/2 h-0.5 bg-outline" />
        {item.rate !== null && (
          <div
            className={`absolute inset-x-2 rounded-sm ${tone.bar}`}
            style={
              item.rate >= 0
                ? { bottom: "50%", height: `${height}%` }
                : { top: "50%", height: `${height}%` }
            }
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
