import { useCallback, useEffect, useState } from "react";
import {
  ApiError,
  type Api,
  type Dashboard,
  type SubscriptionState,
} from "../api";
import { itemAmount, money, short, withCurrency } from "../format";
import BalanceChart from "./BalanceChart";
import GroupBars from "./GroupBars";

const STATE_TEXT: Record<SubscriptionState, string> = {
  charged: "списано",
  twice: "⚠️ дважды",
  expected: "ждём",
  missed: "не было",
};

function Skeleton() {
  return (
    <div className="flex flex-col gap-3" aria-label="Загрузка">
      {[0, 1, 2].map((key) => (
        <div key={key} className="h-20 animate-pulse rounded-2xl bg-section" />
      ))}
    </div>
  );
}

function Today({ data }: { data: Dashboard }) {
  if (
    data.limit === null ||
    data.left_today === null ||
    data.spent_today === null
  ) {
    return null;
  }
  return (
    <section className="rounded-2xl bg-section p-4">
      <p className="text-sm text-hint">Можно сегодня</p>
      <p
        className={`text-4xl font-semibold ${data.left_today < 0 ? "text-danger" : ""}`}
      >
        {money(data.left_today)}
      </p>
      <p className="text-sm text-hint">
        лимит {money(data.limit)} · потрачено {money(data.spent_today)}
      </p>
    </section>
  );
}

function Cards({ data }: { data: Dashboard }) {
  if (data.balance === null || data.planned_balance === null) return null;
  const gap = data.balance - data.planned_balance;
  return (
    <section className="grid grid-cols-2 gap-2">
      <div className="rounded-2xl bg-section p-3">
        <p className="text-xs text-hint">На картах</p>
        <p className="font-semibold">{short(data.balance)}</p>
        <p className="text-xs text-hint">
          по плану {short(data.planned_balance)} ({gap >= 0 ? "+" : ""}
          {short(gap)})
        </p>
      </div>
      {data.frozen && (
        <div className="rounded-2xl bg-section p-3">
          <p className="text-xs text-hint">🧊 Заморожено</p>
          <p className="font-semibold">
            {withCurrency(data.frozen.amount, data.frozen.currency)}
          </p>
          <p className="text-xs text-hint">
            с 1-го {data.frozen.change >= 0 ? "+" : ""}
            {money(data.frozen.change, 2)}
          </p>
        </div>
      )}
    </section>
  );
}

function Content({ data }: { data: Dashboard }) {
  if (data.status === "no_month_tab") {
    return <p>Вкладки месяца нет — бот создаёт её 1-го числа.</p>;
  }
  return (
    <>
      <Today data={data} />
      <Cards data={data} />
      {data.status === "no_forecast" && (
        <p className="text-sm text-hint">
          Во вкладке месяца нет блока прогноза — лимит на день не посчитать.
        </p>
      )}
      {data.groups.length > 0 && (
        <section className="flex flex-col gap-2">
          <h2 className="font-semibold">План-факт</h2>
          <GroupBars groups={data.groups} />
        </section>
      )}
      {data.daily.length > 0 && (
        <section className="flex flex-col gap-2">
          <h2 className="font-semibold">Остаток по дням</h2>
          <BalanceChart points={data.daily} />
        </section>
      )}
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
                  {String(item.day).padStart(2, "0")} — {item.name}
                </span>
                <span className={item.amount > 0 ? "text-ok" : ""}>
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

export default function DashboardScreen({ api }: { api: Api }) {
  const [data, setData] = useState<Dashboard | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [version, setVersion] = useState(0);

  useEffect(() => {
    let alive = true;
    api.dashboard().then(
      (result) => {
        if (!alive) return;
        setData(result);
        setError(null);
        setLoading(false);
      },
      (reason: unknown) => {
        if (!alive) return;
        setError(
          reason instanceof ApiError
            ? reason.message
            : "Не получилось загрузить сводку",
        );
        setLoading(false);
      },
    );
    return () => {
      alive = false;
    };
  }, [api, version]);

  const refresh = useCallback(() => {
    setLoading(true);
    setVersion((current) => current + 1);
  }, []);

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">
          {data ? `Сводка · ${data.month}` : "Сводка"}
        </h1>
        <button
          type="button"
          className="text-link disabled:opacity-50"
          disabled={loading}
          onClick={refresh}
        >
          Обновить
        </button>
      </div>
      {loading && !data && <Skeleton />}
      {error && (
        <div className="flex flex-col items-start gap-2" role="alert">
          <p>{error}</p>
          <button type="button" className="text-link" onClick={refresh}>
            Повторить
          </button>
        </div>
      )}
      {data && <Content data={data} />}
    </div>
  );
}
