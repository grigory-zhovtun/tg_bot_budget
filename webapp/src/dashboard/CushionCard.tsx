import type { Cushion } from "../api";
import { inCurrency, money, moneyText } from "../format";
import { plural } from "../plan/months";
import { Card, Chip, Fact, Good, Line, Warn } from "./blocks";

const SAFE_MONTHS = 3; // меньше — красная плашка

/**
 * Плашка месяцев: цель достигнута — зелёная, от трёх месяцев — жёлтая, меньше —
 * красная; месяцы — как на экране, а не неокруглённые.
 */
function tone(cushion: Cushion, months: number): "good" | "mid" | "bad" {
  if (cushion.short_uzs <= 0) return "good";
  if (Math.round(months * 10) / 10 >= SAFE_MONTHS) return "mid";
  return "bad";
}

const SIGN = { good: "✓ ", mid: "", bad: "⚠️ " } as const;
const BAR = { good: "bg-ok", mid: "bg-warn", bad: "bg-danger" } as const;

/** 0.36 → «0,4». */
function monthsText(value: number): string {
  return value.toFixed(1).replace(".", ",");
}

/** «19 000 $» — целыми, валюта после числа. */
function whole(value: number, currency: string): string {
  return inCurrency(Math.round(value), currency);
}

/** «🛟 Подушка»: «Заморожено» в месяцах средних трат и путь до цели. */
export default function CushionCard({ cushion }: { cushion: Cushion }) {
  const { months, target_months: target, currency } = cushion;
  const level = months === null ? null : tone(cushion, months);
  return (
    <Card label="Подушка безопасности">
      <div className="flex items-center justify-between gap-2">
        <h2 className="text-fact font-extrabold">🛟 Подушка</h2>
        {months !== null && level && (
          <Chip tone={level}>{`${SIGN[level]}${monthsText(months)} мес.`}</Chip>
        )}
      </div>
      {months !== null && level && (
        <div
          role="progressbar"
          aria-label={`Подушка: ${monthsText(months)} из ${target} месяцев`}
          aria-valuemin={0}
          aria-valuemax={target}
          aria-valuenow={Math.min(months, target)}
          aria-valuetext={`${monthsText(months)} из ${target} месяцев`}
          className="my-1 h-3.5 overflow-hidden rounded-full border border-outline"
        >
          <div
            className={`h-full rounded-full ${BAR[level]}`}
            style={{ width: `${Math.min(months / target, 1) * 100}%` }}
          />
        </div>
      )}
      <Line>
        {`${moneyText(cushion.amount, currency)} ≈ ${money(cushion.uzs)} сум`}
      </Line>
      {cushion.monthly > 0 && (
        <Line>{`траты в месяц ${money(cushion.monthly)}`}</Line>
      )}
      {cushion.frozen != null && !!cushion.piggies && (
        <>
          <Line>{`заморожено ${whole(cushion.frozen, currency)}`}</Line>
          <Line>{`минус копилки ${whole(cushion.piggies, currency)}`}</Line>
          <Line>на вечер вчера</Line>
        </>
      )}
      {!!cushion.over && (
        <>
          <Warn>Копилки больше карты</Warn>
          <Line>{`разница ${whole(cushion.over, currency)}`}</Line>
          <Line>отложите разницу</Line>
          <Line>или поправьте копилки</Line>
        </>
      )}
      {months === null && <Line>трат за 3 полных месяца ещё нет</Line>}
      {months !== null &&
        (cushion.short_uzs > 0 ? (
          <>
            <Fact>{`Не хватает ≈ ${money(cushion.short_uzs)}`}</Fact>
            <Line>
              {`до цели ${target} ${plural(target, "месяц", "месяца", "месяцев")}, это ${whole(cushion.short, currency)}`}
            </Line>
          </>
        ) : (
          <Good>{`Цель ${target} мес. достигнута`}</Good>
        ))}
    </Card>
  );
}
