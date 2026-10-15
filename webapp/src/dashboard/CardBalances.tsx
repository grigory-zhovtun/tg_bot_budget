import type { CardBalance } from "../api";
import { dayLabel, daysBetween, money, short, withCurrency } from "../format";

/** «5 250 000 сум», «1 500,25 USD»: сумы целыми, остальное — с копейками. */
function amount(value: number, currency: string): string {
  return currency === "UZS"
    ? `${money(value)} сум`
    : withCurrency(value, currency);
}

/** ✓ — остаток таблицы сходится с банком; рядом — сколько дней назад сверяли. */
function Mark({ card, today }: { card: CardBalance; today: string }) {
  if (card.matches !== true) return null;
  const days = card.checked === null ? 0 : daysBetween(card.checked, today);
  return (
    <span className={card.stale ? "font-medium text-warn" : "text-ok"}>
      {days > 0 ? `✓ ${days} дн.` : "✓"}
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
        <p className="text-sm text-hint">{`Всего ≈ ${short(total)} сум`}</p>
      )}
    </section>
  );
}
