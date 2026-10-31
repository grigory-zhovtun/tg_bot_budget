import type { Piggy } from "../api";
import { moneyText } from "../format";
import { monthIn, nextMonth } from "../plan/months";
import { Chip, Good, Line, Warn } from "./blocks";
import { dayMonth, whenText } from "./piggyText";

interface Props {
  piggy: Piggy;
  month: string; // «2026-10» — месяц «Накоплений»
  onOpen?: (piggy: Piggy) => void;
}

/** Статус внизу карточки: что будет к сроку, хорошее — зелёной плашкой,
 * остальное — предупреждением. */
function Status({ piggy }: { piggy: Piggy }) {
  const money = (value: number) => moneyText(value, piggy.currency);
  const forecast =
    piggy.forecast !== null && piggy.deadline ? (
      <Line>
        {`к ${dayMonth(piggy.deadline)} будет ${money(piggy.forecast)}`}
      </Line>
    ) : null;
  switch (piggy.status) {
    case "on_track":
      return (
        <>
          {forecast}
          <div>
            <Chip tone="good">✓ успеваем</Chip>
          </div>
        </>
      );
    case "done":
      return (
        <div>
          <Chip tone="good">✓ собрано</Chip>
        </div>
      );
    case "late":
      return (
        <>
          <Warn>К сроку не успеть</Warn>
          <Line>{`не хватает ${money(piggy.short)}`}</Line>
        </>
      );
    case "short":
      return (
        <>
          {forecast}
          <Warn>{`Не хватит ${money(piggy.short)}`}</Warn>
        </>
      );
    case "no_target":
      return <Warn>Не указана сумма</Warn>;
    case "leftover":
      return (
        <>
          <Warn>Срок прошёл</Warn>
          <Line>{`осталось ${money(piggy.leftover)}`}</Line>
        </>
      );
    case "broken":
      return (
        <>
          <Warn>{`Проверьте строку ${piggy.row}`}</Warn>
          <Line>на листе «Копилки»</Line>
        </>
      );
    case "goal":
      return null;
  }
}

/** Карточка копилки: срок, сколько накоплено, что отложено в этом месяце и что
 * дальше по плану, что будет к сроку. */
export default function PiggyCard({ piggy, month, onOpen }: Props) {
  const money = (value: number) => moneyText(value, piggy.currency);
  const target = piggy.target;
  const share =
    target !== null && target > 0
      ? Math.min(piggy.collected / target, 1)
      : null;
  const part = piggy.this_month > 0 || piggy.saved_month > 0; // взнос месяца был
  const done = piggy.left_this_month <= 0; // взнос месяца собран
  const got = `${monthIn(month)} отложено ${money(piggy.saved_month)}`;
  const next =
    piggy.next_month > 0
      ? `${monthIn(nextMonth(month))} по плану ${money(piggy.next_month)}`
      : null;
  return (
    <li className="relative flex flex-col gap-1 rounded-2xl bg-section p-4">
      <div className="flex items-center gap-2">
        {piggy.order !== null && (
          <span
            aria-label={`Важность ${piggy.order}`}
            className="inline-flex size-8 shrink-0 items-center justify-center rounded-full bg-text text-base font-black text-bg"
          >
            {piggy.order}
          </span>
        )}
        <h3 className="text-fact font-extrabold">{piggy.name}</h3>
      </div>
      {piggy.status !== "broken" && (
        <>
          <Line>{whenText(piggy)}</Line>
          <p className="text-line">
            накоплено{" "}
            <strong className="font-extrabold">{money(piggy.collected)}</strong>
            {target !== null ? ` из ${money(target)}` : ""}
          </p>
          {share !== null && (
            <div
              role="progressbar"
              aria-label={`${piggy.name}: собрано`}
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={Math.round(share * 100)}
              className="my-1 h-3 overflow-hidden rounded-full border border-outline"
            >
              <div
                className={`h-full rounded-full ${share >= 1 ? "bg-ok" : "bg-warn"}`}
                style={{ width: `${share * 100}%` }}
              />
            </div>
          )}
          {piggy.balance !== piggy.collected && (
            <Line>{`в копилке ${money(piggy.balance)}`}</Line>
          )}
          {part && done && (
            <>
              <Good>{got}</Good>
              {next && <Line>{next}</Line>}
            </>
          )}
          {part && !done && (
            <>
              <Line>{got}</Line>
              <Line>{`в этом месяце ещё ${money(piggy.left_this_month)}`}</Line>
            </>
          )}
          {!part && next && <Line>{next}</Line>}
          {piggy.leftover > 0 && piggy.status !== "leftover" && (
            <Line>{`осталось с прошлого раза ${money(piggy.leftover)}`}</Line>
          )}
        </>
      )}
      <Status piggy={piggy} />
      {onOpen && (
        <button
          type="button"
          aria-label={`Открыть копилку «${piggy.name}»`}
          className="absolute inset-0 rounded-2xl"
          onClick={() => onOpen(piggy)}
        />
      )}
    </li>
  );
}
