import type { Dashboard, SubscriptionState } from "../api";
import { money, moneyText, signedMoney } from "../format";
import { Items, Line, Warn } from "./blocks";

/** Значок подписки перед названием и его расшифровка под списком. */
const STATE: Record<SubscriptionState, { sign: string; text: string }> = {
  charged: { sign: "✓", text: "списано" },
  expected: { sign: "⏳", text: "ждём" },
  twice: { sign: "⚠️", text: "дважды" },
  missed: { sign: "❓", text: "не было" },
};
const STATES: SubscriptionState[] = ["charged", "expected", "twice", "missed"];

/** Без блока прогноза во вкладке месяца нет ни лимита, ни списка впереди. */
export function NoForecastNote() {
  return (
    <div className="flex flex-col">
      <Warn>Нет блока прогноза</Warn>
      <Line>во вкладке месяца</Line>
      <Line>лимит на день не посчитать</Line>
    </div>
  );
}

/** «28-го» — весь список в текущем месяце, месяц не нужен. */
function dayText(day: number): string {
  return `${day}-го`;
}

/**
 * «Впереди»: строки жёлтого списка до конца месяца и подписки. Пункты — в две
 * строки: названия у банка длинные, а суммы бывают в восемь цифр.
 */
export default function Ahead({ data }: { data: Dashboard }) {
  // список без блока прогноза не прочитать — это не «платежей нет»
  if (data.status === "no_forecast") return <NoForecastNote />;
  if (data.upcoming.length === 0 && data.subscriptions.length === 0) {
    return <p className="text-line">До конца месяца платежей и подписок нет</p>;
  }
  const today = Number(data.today.slice(8, 10));
  const late = data.upcoming.some((item) => item.day < today);
  const seen = STATES.filter((state) =>
    data.subscriptions.some((sub) => sub.state === state),
  );
  return (
    <>
      {data.upcoming.length > 0 && (
        <section className="flex flex-col gap-2" aria-label="Впереди">
          <h2 className="text-fact font-extrabold">Впереди</h2>
          <Items
            label="Платежи и поступления"
            items={data.upcoming.map((item, index) => ({
              // названия могут совпасть: два остатка прошлого месяца с одним именем
              key: `${index}-${item.day}-${item.name}`,
              title: `${item.day < today ? "⏳ " : ""}${item.name}`,
              left: dayText(item.day),
              // цветом текста: на полосе зелёный и оранжевый не держат 7:1, смысл — в знаке
              right: signedMoney(
                item.currency === "UZS" ? item.uzs : item.amount,
                item.currency,
              ),
            }))}
          />
          {late && <Line>⏳ — день прошёл, записи ещё нет</Line>}
        </section>
      )}
      {data.subscriptions.length > 0 && (
        <section className="flex flex-col gap-2" aria-label="Подписки">
          <h2 className="text-fact font-extrabold">Подписки</h2>
          <Items
            label="Подписки"
            items={data.subscriptions.map((sub) => ({
              key: `${sub.day}-${sub.name}-${sub.uzs}`,
              title: `${STATE[sub.state].sign} ${sub.name}`,
              left: dayText(sub.day),
              right:
                sub.currency === "UZS"
                  ? money(sub.uzs)
                  : moneyText(sub.amount, sub.currency),
            }))}
          />
          <p className="flex flex-wrap gap-x-4 gap-y-1 text-line">
            {seen.map((state) => (
              <span key={state} className="whitespace-nowrap">
                {`${STATE[state].sign} ${STATE[state].text}`}
              </span>
            ))}
          </p>
        </section>
      )}
    </>
  );
}
