import type { Cushion, CushionStep } from "../api";
import { money, moneyText } from "../format";
import { monthName, monthRange, plural } from "../plan/months";
import { Big, Card, Chip, Fact, Good, Line, Table } from "./blocks";
import { FrozenWarnings } from "./FrozenCard";
import { dayMonth } from "./piggyText";

const STEPS = [1, 3]; // ступени до цели — отметки на полосе

type Level = "good" | "mid" | "bad";

/** Плашка месяцев по ступеням: меньше месяца — красная, до трёх — коричневая,
 * от трёх — зелёная, от цели — зелёная с «✓» (и при цели меньше трёх месяцев);
 * месяцы — как на экране. */
function level(months: number, target: number): { tone: Level; sign: string } {
  if (months < 1) return { tone: "bad", sign: "⚠️ " };
  if (months >= target) return { tone: "good", sign: "✓ " };
  if (months < 3) return { tone: "mid", sign: "" };
  return { tone: "good", sign: "" };
}

const BAR = { good: "bg-ok", mid: "bg-warn", bad: "bg-danger" } as const;

/** 0.8 → «0,8». */
function monthsText(value: number): string {
  return value.toFixed(1).replace(".", ",");
}

/** «месяц» после числа: «1 месяц», «3 месяца», «6 месяцев». */
function monthsWord(count: number): string {
  return plural(count, "месяц", "месяца", "месяцев");
}

/** «месяц» после «До»: «До 1 месяца», «До 3 месяцев», «До цели 21 месяца». */
function monthsAfterTo(count: number): string {
  return count % 10 === 1 && count % 100 !== 11 ? "месяца" : "месяцев";
}

/** Полоса до цели с чертами на 1 и 3 месяцах и подписями под ними. */
function Bar({
  months,
  target,
  tone,
}: {
  months: number;
  target: number;
  tone: Level;
}) {
  const marks = STEPS.filter((step) => step < target);
  const at = (step: number) => `${(step / target) * 100}%`;
  return (
    <div className="my-1 flex flex-col gap-0.5">
      <div
        role="progressbar"
        aria-label={`Подушка: ${monthsText(months)} из ${target} месяцев`}
        aria-valuemin={0}
        aria-valuemax={target}
        aria-valuenow={Math.min(months, target)}
        aria-valuetext={`${monthsText(months)} из ${target} месяцев`}
        className="h-3.5 overflow-hidden rounded-full border border-outline"
      >
        <div
          className={`h-full rounded-full ${BAR[tone]}`}
          style={{ width: `${Math.min(months / target, 1) * 100}%` }}
        />
      </div>
      <div className="relative h-2" aria-hidden="true">
        {marks.map((step) => (
          <div
            key={step}
            className="absolute top-0 h-2 w-0.5 -translate-x-1/2 bg-text"
            style={{ left: at(step) }}
          />
        ))}
      </div>
      {/* высота — в строку подписи: при крупном шрифте подписи не наезжают на сумму */}
      <div
        className="relative h-[1.4em] text-line font-bold"
        data-testid="cushion-scale"
        aria-hidden="true"
      >
        {marks.map((step) => (
          <span
            key={step}
            className="absolute top-0 -translate-x-1/2"
            style={{ left: at(step) }}
          >
            {String(step)}
          </span>
        ))}
        <span className="absolute top-0 right-0 whitespace-nowrap">
          {`${target} мес.`}
        </span>
      </div>
    </div>
  );
}

/** Ступени: следующая — крупно, с суммой в сумах; остальные — строками. */
function Steps({ cushion }: { cushion: Cushion }) {
  const { steps, target_months: target, currency } = cushion;
  const [next, ...rest] = steps;
  if (!next) return <Good>{`Цель ${target} мес. достигнута`}</Good>;
  const sum = (step: CushionStep) => moneyText(step.short, currency);
  const goal = (step: CushionStep) => step.months === target;
  return (
    <>
      <Fact>
        {`До ${goal(next) ? "цели " : ""}${next.months} ${monthsAfterTo(next.months)} — ещё ${sum(next)}`}
      </Fact>
      <Line>{`≈ ${money(next.short_uzs)} сум`}</Line>
      {rest.map((step) => (
        <Line key={step.months}>
          {`${goal(step) ? "цель " : ""}${step.months} ${monthsWord(step.months)} — ещё ${sum(step)}`}
        </Line>
      ))}
    </>
  );
}

/** Месяц жизни: средние траты обязательных групп и из каких месяцев. */
function LifeMonth({ cushion }: { cushion: Cushion }) {
  if (cushion.life_groups.length === 0) {
    return <Line>обязательных групп нет в справочнике</Line>;
  }
  if (cushion.months === null) {
    return <Line>трат за 3 полных месяца ещё нет</Line>;
  }
  const [only] = cushion.life_months;
  const months =
    cushion.life_months.length === 1 && only
      ? monthName(only)
      : monthRange(cushion.life_months);
  return (
    <>
      <Fact>{`Месяц жизни ${money(cushion.monthly)}`}</Fact>
      <Line>{`в среднем за ${months}`}</Line>
      <Line>{`обязательные: ${cushion.life_groups.join(", ")}`}</Line>
    </>
  );
}

/** «🛟 Подушка»: подушка на сейчас — доллары на карте без платежей срока и копилок,
 * в месяцах жизни; полоса до цели и сколько ещё до ступеней 1, 3 и цели. */
export default function CushionCard({
  cushion,
  today,
}: {
  cushion: Cushion;
  today: string;
}) {
  const { months, target_months: target, currency } = cushion;
  const sum = (value: number) => moneyText(value, currency);
  const shown = months === null ? null : level(months, target);
  const rows = [
    ["доллары на карте", sum(cushion.card)],
    ...cushion.payments.map((payment) => [
      `${payment.name} ${dayMonth(payment.day)}`,
      `−${sum(payment.amount)}`,
    ]),
    ["копилки", cushion.piggies > 0 ? `−${sum(cushion.piggies)}` : sum(0)],
    ["подушка", sum(cushion.amount)],
  ];
  return (
    <Card label="Подушка безопасности">
      <div className="flex flex-wrap items-center justify-between gap-x-2 gap-y-1">
        <h2 className="text-fact font-extrabold">🛟 Подушка</h2>
        {months !== null && shown && (
          <Chip
            tone={shown.tone}
          >{`${shown.sign}${monthsText(months)} мес.`}</Chip>
        )}
      </div>
      {months !== null && shown && (
        <Bar months={months} target={target} tone={shown.tone} />
      )}
      <Big>{sum(cushion.amount)}</Big>
      <Line>в подушке сейчас</Line>
      <Line>{`≈ ${money(cushion.uzs)} сум`}</Line>
      <div className="my-1">
        <Table label="Расчёт подушки" align="lr" rows={rows} />
      </div>
      <LifeMonth cushion={cushion} />
      {months !== null && <Steps cushion={cushion} />}
      <FrozenWarnings
        currency={currency}
        over={cushion.over}
        lacking={cushion.lacking}
        lackingOn={cushion.lacking_on}
        today={today}
      />
    </Card>
  );
}
