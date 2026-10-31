import type { CardBalance, Source } from "../api";
import { Line } from "../dashboard/blocks";
import { moneyText } from "../format";
import Sheet from "../plan/Sheet";
import { balanceOf } from "./transfer";

interface Props {
  side: "from" | "to"; // окно «Откуда» или «Куда»
  sources: Source[];
  chosen: string | null; // карта стороны; у «Откуда» null — «не моя карта»
  exclude: string | null; // карта другой стороны: её здесь нет
  cards: CardBalance[] | null; // «Остатки на картах»; null — не загружены
  onPick: (name: string | null) => void; // null — «не моя карта»
  onClose: () => void;
}

/** Строка окна: название крупно, под ним пояснение; выбранная — цветом кнопки. */
function Row({
  on,
  title,
  note,
  onClick,
}: {
  on: boolean;
  title: string;
  note: string | null;
  onClick: () => void;
}) {
  return (
    <li>
      <button
        type="button"
        className={`w-full rounded-xl px-4 py-3 text-left ${
          on ? "bg-button text-button-text" : "bg-section text-text"
        }`}
        onClick={onClick}
      >
        <span className="block text-fact font-extrabold break-words">
          {on ? `✓ ${title}` : title}
        </span>
        {note && <span className="block text-line">{note}</span>}
      </button>
    </li>
  );
}

/** Окно выбора карты перевода: карты справочника с остатком, без карты другой
 * стороны; у «Откуда» последняя строка — «не моя карта». Закрывает вызывающий. */
export default function CardSheet({
  side,
  sources,
  chosen,
  exclude,
  cards,
  onPick,
  onClose,
}: Props) {
  const list = sources.filter((source) => source.name !== exclude);
  return (
    <Sheet title={side === "from" ? "Откуда" : "Куда"} onClose={onClose}>
      {list.length === 0 && <Line>других карт нет</Line>}
      {(list.length > 0 || side === "from") && (
        <ul aria-label="Карты" className="flex flex-col gap-2">
          {list.map((source) => {
            const card = balanceOf(cards, source.name);
            return (
              <Row
                key={source.name}
                on={source.name === chosen}
                title={source.name}
                note={
                  card && card.balance !== null
                    ? `на карте ${moneyText(card.balance, card.currency)}`
                    : null
                }
                onClick={() => onPick(source.name)}
              />
            );
          })}
          {side === "from" && (
            <Row
              on={chosen === null}
              title="не моя карта"
              note="деньги от других людей"
              onClick={() => onPick(null)}
            />
          )}
        </ul>
      )}
    </Sheet>
  );
}
