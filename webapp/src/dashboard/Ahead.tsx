import type { Dashboard, SubscriptionState } from "../api";
import { itemAmount, money, withCurrency } from "../format";

const STATE_TEXT: Record<SubscriptionState, string> = {
  charged: "списано",
  twice: "⚠️ дважды",
  expected: "ждём",
  missed: "не было",
};

/** Без блока прогноза во вкладке месяца нет ни лимита, ни списка впереди. */
export function NoForecastNote() {
  return (
    <p className="text-sm text-hint">
      Во вкладке месяца нет блока прогноза — лимит на день не посчитать.
    </p>
  );
}

/** «Впереди»: строки жёлтого списка до конца месяца и подписки. */
export default function Ahead({ data }: { data: Dashboard }) {
  // список без блока прогноза не прочитать — это не «платежей нет»
  if (data.status === "no_forecast") return <NoForecastNote />;
  if (data.upcoming.length === 0 && data.subscriptions.length === 0) {
    return <p className="text-hint">До конца месяца платежей и подписок нет</p>;
  }
  return (
    <>
      {data.upcoming.length > 0 && (
        <section className="flex flex-col gap-2">
          <h2 className="font-semibold">Впереди</h2>
          <ul className="flex flex-col gap-1 text-sm">
            {data.upcoming.map((item) => (
              <li
                key={`${item.day}-${item.name}`}
                className="flex justify-between gap-2"
              >
                <span>
                  {item.day < Number(data.today.slice(8, 10)) ? "⏳ " : ""}
                  {String(item.day).padStart(2, "0")} — {item.name}
                </span>
                <span
                  className={`font-semibold ${item.amount > 0 ? "text-earned" : "text-payment"}`}
                >
                  {itemAmount(item.amount, item.currency, item.uzs)}
                </span>
              </li>
            ))}
          </ul>
        </section>
      )}
      {data.subscriptions.length > 0 && (
        <section className="flex flex-col gap-2">
          <h2 className="font-semibold">Подписки</h2>
          <ul className="flex flex-col gap-1 text-sm">
            {data.subscriptions.map((sub) => (
              <li
                key={`${sub.day}-${sub.name}-${sub.uzs}`}
                className="flex justify-between gap-2"
              >
                <span>
                  {String(sub.day).padStart(2, "0")} — {sub.name}
                </span>
                <span className="text-hint">
                  {sub.currency === "UZS"
                    ? money(sub.uzs)
                    : withCurrency(sub.amount, sub.currency)}{" "}
                  · {STATE_TEXT[sub.state]}
                </span>
              </li>
            ))}
          </ul>
        </section>
      )}
    </>
  );
}
