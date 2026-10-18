import type { Piggy } from "../api";
import { piggyMoney, whenText } from "./piggyText";

interface Props {
  piggy: Piggy;
  onOpen?: (piggy: Piggy) => void;
}

function Status({ piggy }: { piggy: Piggy }) {
  const money = (value: number) => piggyMoney(value, piggy.currency);
  switch (piggy.status) {
    case "on_track":
      return <p className="text-sm font-semibold text-ok">успеваем</p>;
    case "late":
      return (
        <p className="text-sm font-semibold text-danger">
          {`к сроку не успеть — не хватает ${money(piggy.short)}`}
        </p>
      );
    case "no_target":
      return <p className="text-sm font-semibold text-warn">сумма?</p>;
    case "done":
      return <p className="text-sm font-semibold text-ok">собрано ✓</p>;
    case "leftover":
      return (
        <p className="text-sm font-semibold text-warn">
          {`срок прошёл, осталось ${money(piggy.leftover)}`}
        </p>
      );
    case "broken":
      return (
        <p className="text-sm font-semibold text-danger">
          {`Проверьте строку ${piggy.row} на листе «Копилки»`}
        </p>
      );
    case "goal":
      return null;
  }
}

/** Карточка копилки: сколько накоплено, срок, взнос месяца и статус. */
export default function PiggyCard({ piggy, onOpen }: Props) {
  const money = (value: number) => piggyMoney(value, piggy.currency);
  const target = piggy.target;
  const share =
    target !== null && target > 0
      ? Math.min(piggy.collected / target, 1)
      : null;
  return (
    <li className="relative flex flex-col gap-1 rounded-2xl bg-section p-4">
      <h3 className="font-semibold">{piggy.name}</h3>
      {piggy.status !== "broken" && (
        <>
          <p className="text-sm">
            {target !== null
              ? `накоплено ${money(piggy.collected)} из ${money(target)}`
              : `накоплено ${money(piggy.collected)}`}
          </p>
          {share !== null && (
            <div
              role="progressbar"
              aria-label={`${piggy.name}: собрано`}
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={Math.round(share * 100)}
              className="h-2.5 overflow-hidden rounded-full border border-outline"
            >
              <div
                className={`h-full rounded-full ${share >= 1 ? "bg-ok" : "bg-warn"}`}
                style={{ width: `${share * 100}%` }}
              />
            </div>
          )}
          {piggy.balance !== piggy.collected && (
            <p className="text-sm">{`в копилке ${money(piggy.balance)}`}</p>
          )}
          <p className="text-sm">{whenText(piggy)}</p>
          {piggy.this_month > 0 && (
            <p className="text-sm">
              {`в этом месяце ${money(piggy.this_month)}${piggy.left_this_month > 0 ? "" : " — отложено ✓"}`}
            </p>
          )}
          {piggy.leftover > 0 && piggy.status !== "leftover" && (
            <p className="text-sm">
              {`осталось с прошлого раза ${money(piggy.leftover)}`}
            </p>
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
