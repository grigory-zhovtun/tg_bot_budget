import type { Cushion } from "../api";
import { money, short, withCurrency } from "../format";

const SAFE_MONTHS = 3; // меньше — предупреждение оранжевым

/** Цвет числа и полоски: до трёх месяцев — оранжевый, до цели — жёлтый, цель — мятный. */
function tone(months: number, target: number): { text: string; bar: string } {
  if (months >= target) return { text: "text-ok", bar: "bg-ok" };
  if (months >= SAFE_MONTHS) return { text: "text-warn", bar: "bg-warn" };
  return { text: "text-danger", bar: "bg-danger" };
}

/** 0.36 → «0,4». */
function monthsText(value: number): string {
  return value.toFixed(1).replace(".", ",");
}

/** «$19 000» или «150 000 RUB» — целыми. */
function inCurrency(value: number, currency: string): string {
  const whole = Math.round(value);
  return currency === "USD"
    ? `$${money(whole)}`
    : withCurrency(whole, currency);
}

/** «🛟 Подушка безопасности»: «Заморожено» в месяцах средних трат и путь до цели. */
export default function CushionCard({ cushion }: { cushion: Cushion }) {
  const { months, target_months: target } = cushion;
  const colors = months === null ? null : tone(months, target);
  const spending =
    cushion.monthly > 0
      ? ` — при тратах ${short(cushion.monthly)} в месяц`
      : "";
  return (
    <section
      className="flex flex-col gap-2 rounded-2xl bg-section p-4"
      aria-label="Подушка безопасности"
    >
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">🛟 Подушка безопасности</h2>
        <span className={`text-2xl font-extrabold ${colors?.text ?? ""}`}>
          {months === null ? "—" : `${monthsText(months)} мес.`}
        </span>
      </div>
      {months !== null && colors && (
        <div
          role="progressbar"
          aria-label={`Подушка: ${monthsText(months)} из ${target} месяцев`}
          aria-valuemin={0}
          aria-valuemax={target}
          aria-valuenow={months}
          className="h-3 overflow-hidden rounded-full bg-bg"
        >
          <div
            className={`h-full rounded-full ${colors.bar}`}
            style={{ width: `${Math.min(months / target, 1) * 100}%` }}
          />
        </div>
      )}
      <p className="text-sm">
        {`${withCurrency(cushion.amount, cushion.currency)} ≈ ${short(cushion.uzs)}${spending}`}
      </p>
      {months !== null &&
        (cushion.short_uzs > 0 ? (
          <p className="text-sm font-semibold">
            {`до цели в ${target} мес. не хватает ≈ ${short(cushion.short_uzs)} (${inCurrency(cushion.short, cushion.currency)})`}
          </p>
        ) : (
          <p className="text-sm font-semibold text-ok">
            {`цель в ${target} мес. достигнута`}
          </p>
        ))}
    </section>
  );
}
