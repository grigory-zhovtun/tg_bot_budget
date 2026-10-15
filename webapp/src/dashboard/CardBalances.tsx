import type { CardBalance } from "../api";
import { dayLabel, daysBetween, money, short, withCurrency } from "../format";

/** «5 250 000 сум», «1 500,25 USD»: сумы целыми, остальное — с копейками. */
function amount(value: number, currency: string): string {
  return currency === "UZS"
    ? `${money(value)} сум`
    : withCurrency(value, currency);
}

/** ✓ — остаток таблицы сходится с банком; рядом — когда сверяли. */
function Mark({ card, today }: { card: CardBalance; today: string }) {
  if (card.matches !== true) return null;
  let label = "✓";
  if (card.checked === null) {
    label = "✓ давно"; // без времени сверки: остаток банка вписан руками раньше
  } else {
    const days = daysBetween(card.checked, today);
    if (days > 0) label = `✓ ${days} дн.`;
  }
  return (
    <span className={card.stale ? "font-medium text-warn" : "text-ok"}>
      {label}
    </span>
  );
}

interface Props {
  cards: CardBalance[];
  today: string;
}

/** Остатки на картах сейчас — блок остатков листа fact, как в таблице. */
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
  const without = unrated.length > 0 ? ` (без ${unrated.join(", ")})` : "";
  return (
    <section className="flex flex-col gap-2" aria-label="Остатки на картах">
      <h2 className="font-semibold">Остатки на картах</h2>
      <ul className="flex flex-col gap-1 text-sm">
        {cards.map((card) => (
          <li key={`${card.name}-${card.currency}`} className="flex flex-col">
            <div className="grid grid-cols-[1fr_auto_3.5rem] items-baseline gap-2">
              <span>{card.name}</span>
              <span className="text-right font-medium whitespace-nowrap tabular-nums">
                {card.balance === null
                  ? "—"
                  : amount(card.balance, card.currency)}
              </span>
              <span className="text-xs whitespace-nowrap">
                <Mark card={card} today={today} />
              </span>
            </div>
            {card.matches === false && card.bank !== null && (
              <p className="text-xs text-danger">
                {`в банке ${amount(card.bank, card.currency)}`}
                {card.checked &&
                  ` · сверено ${dayLabel(card.checked, today).toLowerCase()}`}
              </p>
            )}
          </li>
        ))}
      </ul>
      {known.length > 0 && (
        <p className="text-sm text-hint">
          {`Всего ≈ ${short(total)} сум${without}`}
        </p>
      )}
    </section>
  );
}
