import { useCallback, useState } from "react";
import type { Api, Bootstrap, Dashboard } from "../api";
import { longDay, money, short, withCurrency } from "../format";
import { useResource, useStore } from "../store";
import Ahead, { NoForecastNote } from "./Ahead";
import CardBalances from "./CardBalances";
import CategoryChart from "./CategoryChart";
import SavingsTab from "./SavingsTab";
import SectionSwitch from "./SectionSwitch";
import SpendingChart from "./SpendingChart";
import { storeSection, storedSection, type Section } from "./section";

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
  if (data.limit <= 0) {
    // как /today: до плана на конец месяца уже не хватает — лимита на день нет
    const missing = -data.limit * (data.days_left ?? 1);
    return (
      <section className="rounded-2xl bg-section p-4">
        <p className="text-sm text-hint">Можно сегодня</p>
        <p className="text-2xl font-semibold text-danger">
          Лимита на сегодня нет
        </p>
        <p className="text-sm text-hint">
          до плана на конец месяца не хватает {short(missing)} · потрачено{" "}
          {money(data.spent_today)}
        </p>
      </section>
    );
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
        <p className="text-xs text-hint">На картах утром</p>
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

/** Когда при нынешнем темпе закончатся сумы и сколько долларов обменять. */
function Sums({ data }: { data: Dashboard }) {
  const runway = data.runway;
  if (!runway) return null;
  let when = "";
  if (runway.short > 0) {
    if (runway.last_day === null || runway.last_day < data.today) {
      when = "Сумы уже закончились";
    } else if (runway.last_day === data.today) {
      when = "Хватит только на сегодня";
    } else {
      when = `Хватит до ${longDay(runway.last_day)}`;
    }
  }
  const target = runway.until
    ? `До ${longDay(runway.until)}`
    : "До конца месяца";
  return (
    <section
      className="flex flex-col gap-1 rounded-2xl bg-section p-4"
      aria-label="Сумы"
    >
      <div className="flex items-baseline justify-between">
        <p className="text-sm text-hint">💳 Сумы на картах и наличными</p>
        <p className="font-semibold">{short(runway.balance)}</p>
      </div>
      <p className="text-xs text-hint">
        {`тратите в среднем ${short(runway.pace)} в день (за 14 дней)`}
      </p>
      {runway.short <= 0 ? (
        <p className="font-medium text-ok">До конца месяца хватит</p>
      ) : (
        <>
          <p className="font-medium text-danger">{when}</p>
          <p className="text-sm">
            {runway.dollars !== null
              ? `${target} обменять ≈ $${money(runway.dollars)}`
              : `${target} обменять`}
          </p>
          <p className="text-xs text-hint">{`не хватит ${short(runway.short)}`}</p>
        </>
      )}
    </section>
  );
}

interface SectionProps {
  api: Api;
  boot: Bootstrap | null;
  data: Dashboard;
  section: Section;
  refresh: number;
}

/** Карточки выбранной вкладки: «Накопления» — свой ответ, остальные — «Сводки». */
function SectionCards({ api, boot, data, section, refresh }: SectionProps) {
  switch (section) {
    case "today":
      return (
        <>
          {/* лимита нет именно здесь — здесь и объясняем */}
          {data.status === "no_forecast" && <NoForecastNote />}
          <Today data={data} />
          <Cards data={data} />
          <Sums data={data} />
          {data.cards.length > 0 && (
            <CardBalances cards={data.cards} today={data.today} />
          )}
        </>
      );
    case "spending":
      return (
        <>
          {data.categories.length > 0 && (
            <CategoryChart
              categories={data.categories}
              untracked={data.untracked}
              today={data.today}
            />
          )}
          {data.spending.length > 0 && (
            <section className="flex flex-col gap-2">
              <h2 className="font-semibold">Траты по дням</h2>
              <SpendingChart
                spending={data.spending}
                today={data.today}
                limit={data.limit}
                planPerDay={data.plan_per_day}
              />
            </section>
          )}
        </>
      );
    case "savings":
      return <SavingsTab api={api} boot={boot} refresh={refresh} />;
    case "ahead":
      return <Ahead data={data} />;
  }
}

interface ContentProps {
  api: Api;
  boot: Bootstrap | null;
  data: Dashboard;
  section: Section;
  refresh: number;
  onSection: (section: Section) => void;
}

function Content({
  api,
  boot,
  data,
  section,
  refresh,
  onSection,
}: ContentProps) {
  if (data.status === "no_month_tab") {
    // в таком ответе нет данных ни для одной вкладки
    return <p>Вкладки месяца нет — бот создаёт её 1-го числа.</p>;
  }
  return (
    <>
      <SectionSwitch section={section} onSection={onSection} />
      <SectionCards
        api={api}
        boot={boot}
        data={data}
        section={section}
        refresh={refresh}
      />
    </>
  );
}

export default function DashboardScreen({
  api,
  boot = null,
}: {
  api: Api;
  boot?: Bootstrap | null; // каталог — для окон копилок
}) {
  const store = useStore();
  const load = useCallback((fresh: boolean) => api.dashboard(fresh), [api]);
  const { data, loading, error, reload } = useResource<Dashboard>(
    store,
    "dashboard",
    load,
    { failure: "Не получилось загрузить сводку" },
  );
  // «Обновить» читает таблицу заново, а не копию листов на сервере — и «Сводку»,
  // и «Накопления»: у них свой ответ, его вкладка ловит счётчик
  const [refreshed, setRefreshed] = useState(0);
  const refresh = useCallback(() => {
    reload(true);
    setRefreshed((count) => count + 1);
  }, [reload]);
  const [section, setSection] = useState<Section>(storedSection);
  const choose = useCallback((next: Section) => {
    setSection(next);
    storeSection(next);
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
          {loading ? "Обновляю…" : "Обновить"}
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
      {data && (
        <Content
          api={api}
          boot={boot}
          data={data}
          section={section}
          refresh={refreshed}
          onSection={choose}
        />
      )}
    </div>
  );
}
