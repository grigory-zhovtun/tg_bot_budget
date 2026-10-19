import type { CardBalance } from "../api";
import { dayLabel, daysBetween, money, moneyText } from "../format";
import { Fact, Line, Table, Warn } from "./blocks";

/** ✓ — остаток таблицы сходится с банком, рядом — когда сверяли; ⚠️ — не сходится. */
function Mark({ card, today }: { card: CardBalance; today: string }) {
  if (card.matches === false) return <span>⚠️</span>;
  if (card.matches !== true) return null;
  let label = "✓";
  if (card.checked === null) {
    label = "✓ давно"; // без времени сверки: остаток банка вписан руками раньше
  } else {
    const days = daysBetween(card.checked, today);
    if (days > 0) label = `✓ ${days} дн.`;
  }
  return <span className={card.stale ? "text-warn" : "text-ok"}>{label}</span>;
}

interface Props {
  cards: CardBalance[];
  today: string;
}

/** Остатки на картах сейчас — блок остатков листа fact: таблица, расхождения и итог. */
export default function CardBalances({ cards, today }: Props) {
  const known = cards.filter((card) => card.uzs !== null);
  const total = known.reduce((sum, card) => sum + (card.uzs ?? 0), 0);
  // карты с остатком, но без курса в итог не входят — называем их валюты
  const unrated = [
    ...new Set(
      cards
        .filter((card) => card.balance !== null && card.uzs === null)
        .map((card) => card.currency),
    ),
  ];
  const differs = cards.filter(
    (card) => card.matches === false && card.bank !== null,
  );
  return (
    <section className="flex flex-col gap-2" aria-label="Остатки на картах">
      <h2 className="text-fact font-extrabold">Остатки на картах</h2>
      <Table
        head={["карта", "остаток", "сверка"]}
        align="lrl"
        rows={cards.map((card) => [
          card.name,
          card.balance === null ? "—" : moneyText(card.balance, card.currency),
          <Mark key="mark" card={card} today={today} />,
        ])}
      />
      {differs.map((card) => (
        <div key={`${card.name}-${card.currency}`} className="flex flex-col">
          <Warn>
            {`${card.name} в банке ${moneyText(card.bank ?? 0, card.currency)}`}
          </Warn>
          {card.checked && (
            <Line>{`сверено ${dayLabel(card.checked, today).toLowerCase()}`}</Line>
          )}
        </div>
      ))}
      {known.length > 0 && <Fact>{`Всего ≈ ${money(total)}\u00a0сум`}</Fact>}
      {unrated.length > 0 && (
        <Line>{`без ${unrated.join(", ")} — нет курса`}</Line>
      )}
    </section>
  );
}
