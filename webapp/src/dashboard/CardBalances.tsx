import type { CardBalance, HeldMoney } from "../api";
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

/** «сбор на праздник» → «Сбор на праздник». */
function capital(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** Остаток сбора: у круга-значка нет знака валюты, поэтому сумы — со словом «сум». */
function heldFigure(amount: number, currency: string): string {
  const text = moneyText(amount, currency);
  return currency === "UZS" ? `${text}\u00a0сум` : text;
}

/**
 * Чужие деньги на карте — подстрока под строкой карты, с левой чертой: название
 * статьи и сколько осталось; под ним — сколько пришло и потрачено, а при
 * перерасходе — сколько доложено из своих.
 */
function Held({ item, currency }: { item: HeldMoney; currency: string }) {
  return (
    <div className="ml-5 flex items-center gap-3 border-l-[3px] border-outline pl-3">
      <Round tone="bg-chip-mid">{item.icon}</Round>
      <span className="flex min-w-0 flex-1 flex-col text-base">
        <span className="flex flex-wrap items-baseline justify-between gap-x-3 text-line font-extrabold text-warn">
          <span>{capital(item.name)}</span>
          <span className="ml-auto whitespace-nowrap tabular-nums">
            {heldFigure(Math.max(item.left, 0), currency)}
          </span>
        </span>
        {item.left < 0 ? (
          <span className="font-extrabold text-danger">
            {`доложено из своих ${money(-item.left)}`}
          </span>
        ) : (
          <span className="font-bold text-hint">
            {`чужие · пришло ${money(item.came)} · потрачено ${money(item.spent)}`}
          </span>
        )}
      </span>
    </div>
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
  // в итог идут свои деньги; у карты без сбора own_uzs может быть пуст — берём uzs
  const total = known.reduce(
    (sum, card) => sum + (card.own_uzs ?? card.uzs ?? 0),
    0,
  );
  // чужие деньги в сумах — только там, где есть что держать
  const heldUzs = cards.reduce(
    (sum, card) =>
      card.held.some((item) => item.left > 0) &&
      card.uzs !== null &&
      card.own_uzs !== null
        ? sum + (card.uzs - card.own_uzs)
        : sum,
    0,
  );
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
          const hasHeld = card.held.length > 0 && card.own !== null;
          const shown = hasHeld ? card.own : card.balance;
          return (
            <li
              key={`${card.name}-${card.currency}`}
              className="flex flex-col gap-3"
            >
              <div className="flex items-center gap-3">
                <Round tone={tone}>{sign}</Round>
                <span className="flex min-w-0 flex-1 flex-col text-base">
                  {/* длинный остаток при крупном шрифте уходит строкой ниже, вправо */}
                  <span className="flex flex-wrap items-baseline justify-between gap-x-3 text-line font-extrabold">
                    <span>{card.name}</span>
                    <span className="ml-auto whitespace-nowrap tabular-nums">
                      {shown === null ? "—" : moneyText(shown, card.currency)}
                    </span>
                  </span>
                  {hasHeld && card.balance !== null ? (
                    <span className="flex flex-wrap gap-x-2">
                      <span className="font-bold text-hint">
                        {`на карте всего ${money(card.balance)}`}
                      </span>
                      <Check card={card} today={today} />
                    </span>
                  ) : (
                    <Check card={card} today={today} />
                  )}
                </span>
              </div>
              {card.held.map((item) => (
                <Held key={item.name} item={item} currency={card.currency} />
              ))}
            </li>
          );
        })}
      </ul>
      {known.length > 0 && (
        <div className="border-t-2 border-outline pt-3">
          <Fact>{`Всего ≈ ${money(total)}\u00a0сум`}</Fact>
          {heldUzs > 0 && <Line>{`чужие ${money(heldUzs)} — не в счёт`}</Line>}
          {unrated.length > 0 && (
            <Line>{`без ${unrated.join(", ")} — нет курса`}</Line>
          )}
        </div>
      )}
    </section>
  );
}
