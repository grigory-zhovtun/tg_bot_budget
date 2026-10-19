import type { Enough } from "../api";
import { money } from "../format";
import { Good, Line, Warn } from "./blocks";

/** Хватает ли на жизнь после взносов: вывод строкой, числа — строками ниже. */
export default function EnoughLines({ enough }: { enough: Enough }) {
  return (
    <>
      {enough.gap < 0 ? (
        <Warn>На жизнь не хватит</Warn>
      ) : (
        <Good>На жизнь хватает</Good>
      )}
      <Line>{`останется ${money(enough.life)}`}</Line>
      <Line>{`обычные траты ${money(enough.ordinary)}`}</Line>
      <Line>
        {enough.gap < 0
          ? `разница ${money(-enough.gap)}`
          : `запас ${money(enough.gap)}`}
      </Line>
    </>
  );
}
