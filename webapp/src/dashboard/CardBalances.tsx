import type { CardBalance } from "../api";
import { dayLabel, daysBetween, money, moneyText } from "../format";
import { Fact, Line, Round, Title } from "./blocks";

/** Знак и цвет круга карты — по её валюте: сумы, доллары, рубли; прочее — код. */
const SIGNS: Record<string, { sign: string; tone: string }> = {
  UZS: { sign: "сум", tone: "bg-round-sums" },
  USD: { sign: "$", tone: "bg-round-dollars" },
  RUB: { sign: "₽", tone: "bg-round-rubles" },
};

/**
 * Сверка под названием карты: «✓ сверено сегодня», давняя — жёлтым с числом дней,
 * расхождение — «⚠️ в банке …» и когда сверяли; не сверяли — ничего.
 */
function Check({ card, today }: { card: CardBalance; today: string }) {
  if (card.matches === false) {
    return (
      <>
        <span className="font-extrabold text-danger">
          {card.bank === null
            ? "⚠️ не сходится с банком"
            : `⚠️ в банке ${moneyText(card.bank, card.currency)}`}
        </span>
        {card.checked && (
          <span>{`сверено ${dayLabel(card.checked, today).toLowerCase()}`}</span>
        )}
      </>
    );
  }
  if (card.matches !== true) return null;
  if (card.checked === null) {
    // без времени сверки: остаток банка вписан руками раньше
    return <span className="font-bold text-warn">✓ сверено давно</span>;
  }
  const days = daysBetween(card.checked, today);
  const when =
    days > 0
      ? `${days} дн. назад`
      : dayLabel(card.checked, today).toLowerCase();
  return (
    <span className={`font-bold ${card.stale ? "text-warn" : "text-ok"}`}>
      {`✓ сверено ${when}`}
    </span>
  );
}

interface Props {
  cards: CardBalance[];
  today: string;
}

/**
 * Остатки на картах сейчас — блок остатков листа fact: строка на карту с круглым
 * знаком валюты, сверкой под названием и остатком справа; внизу — итог в сумах.
 */
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
  return (
    <section
      className="flex flex-col gap-3 rounded-2xl bg-section p-4"
      aria-label="Остатки на картах"
    >
      <Title>Остатки на картах</Title>
      <ul className="flex flex-col gap-3">
        {cards.map((card) => {
          const { sign, tone } = SIGNS[card.currency] ?? {
            sign: card.currency,
            tone: "bg-chip-off",
          };
          return (
            <li
              key={`${card.name}-${card.currency}`}
              className="flex items-center gap-3"
            >
              <Round tone={tone}>{sign}</Round>
              <span className="flex min-w-0 flex-1 flex-col text-base">
                {/* длинный остаток при крупном шрифте уходит строкой ниже, вправо */}
                <span className="flex flex-wrap items-baseline justify-between gap-x-3 text-line font-extrabold">
                  <span>{card.name}</span>
                  <span className="ml-auto whitespace-nowrap tabular-nums">
                    {card.balance === null
                      ? "—"
                      : moneyText(card.balance, card.currency)}
                  </span>
                </span>
                <Check card={card} today={today} />
              </span>
            </li>
          );
        })}
      </ul>
      {known.length > 0 && (
        <div className="border-t-2 border-outline pt-3">
          <Fact>{`Всего ≈ ${money(total)}\u00a0сум`}</Fact>
          {unrated.length > 0 && (
            <Line>{`без ${unrated.join(", ")} — нет курса`}</Line>
          )}
        </div>
      )}
    </section>
  );
}
