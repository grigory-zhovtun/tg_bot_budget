import { useCallback, useState } from "react";
import type { Api, Bootstrap, Dashboard } from "../api";
import { inCurrency, longDay, money, moneyText, signedMoney } from "../format";
import { monthName } from "../plan/months";
import { useResource, useStore } from "../store";
import Ahead, { NoForecastNote } from "./Ahead";
import { Big, Card, Fact, Good, Line, Warn } from "./blocks";
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

/** Сколько можно сегодня: главная цифра и под ней лимит и траты дня. */
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
      <Card label="Можно сегодня">
        <p className="text-fact font-extrabold text-danger">⚠️ Лимита нет</p>
        <Line>{`не хватает ${money(missing)}`}</Line>
        <Line>до плана на конец месяца</Line>
        <Line>{`потрачено сегодня ${money(data.spent_today)}`}</Line>
      </Card>
    );
  }
  return (
    <Card label="Можно сегодня">
      {/* перед «сум» — обычный пробел: при крупном шрифте единица уходит вниз, а не за экран */}
      <Big>{`${money(data.left_today)} сум`}</Big>
      {data.left_today < 0 ? (
        <Warn>сверх лимита дня</Warn>
      ) : (
        <Line>можно потратить сегодня</Line>
      )}
      <Line>
        {`лимит ${money(data.limit)}, потрачено ${money(data.spent_today)}`}
      </Line>
    </Card>
  );
}

/** На картах утром против плана и «Заморожено» — каждая своей карточкой. */
function Cards({ data }: { data: Dashboard }) {
  if (data.balance === null || data.planned_balance === null) return null;
  const gap = Math.round(data.balance - data.planned_balance);
  const frozen = data.frozen;
  return (
    <>
      <Card label="На картах">
        <Fact>{`На картах ${money(data.balance)}`}</Fact>
        <Line>{`утром, по плану ${money(data.planned_balance)}`}</Line>
        {gap < 0 && <Warn>{`меньше плана на ${money(-gap)}`}</Warn>}
        {gap > 0 && <Good>{`больше плана на ${money(gap)}`}</Good>}
        {gap === 0 && <Good>ровно по плану</Good>}
      </Card>
      {frozen && (
        <Card label="Заморожено">
          <Fact>
            {`🧊 Заморожено ${moneyText(frozen.amount, frozen.currency)}`}
          </Fact>
          <Line>
            {Math.round(frozen.change * 100) === 0
              ? "с 1-го числа без изменений"
              : `с 1-го числа ${signedMoney(frozen.change, frozen.currency)}`}
          </Line>
        </Card>
      )}
    </>
  );
}

/** Когда при нынешнем темпе закончатся сумы и сколько долларов обменять. */
function Sums({ data }: { data: Dashboard }) {
  const runway = data.runway;
  if (!runway) return null;
  let when = "";
  if (runway.short > 0) {
    if (runway.last_day === null || runway.last_day < data.today) {
      when = "сумы уже закончились";
    } else if (runway.last_day === data.today) {
      when = "хватит только на сегодня";
    } else {
      when = `хватит до ${longDay(runway.last_day)}`;
    }
  }
  const target = runway.until
    ? `до ${longDay(runway.until)}`
    : "до конца месяца";
  return (
    <Card label="Сумы">
      <Fact>{`💳 Сумы ${money(runway.balance)}`}</Fact>
      {runway.short <= 0 ? (
        <Good>до конца месяца хватит</Good>
      ) : (
        <>
          <Warn>{when}</Warn>
          <Line>
            {runway.dollars !== null
              ? `${target} обменять ≈ ${inCurrency(runway.dollars, "USD")}`
              : `${target} обменять`}
          </Line>
          <Line>{`не хватит ${money(runway.short)}`}</Line>
        </>
      )}
      <Line>{`в день ≈ ${money(runway.pace)}`}</Line>
      <Line>в среднем за 14 дней</Line>
    </Card>
  );
}

interface SectionProps {
  api: Api;
  boot: Bootstrap | null;
  data: Dashboard;
  section: Section;
  refresh: number;
  openSave: boolean;
}

/** Карточки выбранной вкладки: «Накопления» — свой ответ, остальные — «Сводки». */
function SectionCards({
  api,
  boot,
  data,
  section,
  refresh,
  openSave,
}: SectionProps) {
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
      return (
        <SavingsTab
          api={api}
          boot={boot}
          refresh={refresh}
          openSave={openSave}
        />
      );
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
  openSave: boolean;
  onSection: (section: Section) => void;
}

function Content({
  api,
  boot,
  data,
  section,
  refresh,
  openSave,
  onSection,
}: ContentProps) {
  if (data.status === "no_month_tab") {
    // в таком ответе нет данных ни для одной вкладки
    return (
      <p className="text-line">
        Вкладки месяца нет — бот создаёт её 1-го числа.
      </p>
    );
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
        openSave={openSave}
      />
    </>
  );
}

export default function DashboardScreen({
  api,
  boot = null,
  openSave = false,
}: {
  api: Api;
  boot?: Bootstrap | null; // каталог — для окон копилок
  openSave?: boolean; // из чата «Отложил часть…» — «Накопления» и окно «Отложил»
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
  const [section, setSection] = useState<Section>(() =>
    openSave ? "savings" : storedSection(),
  );
  const choose = useCallback((next: Section) => {
    setSection(next);
    storeSection(next);
  }, []);

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-extrabold">
          {data ? `Сводка · ${monthName(data.today.slice(0, 7))}` : "Сводка"}
        </h1>
        <button
          type="button"
          className="text-lg font-bold text-link disabled:opacity-50"
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
          openSave={openSave}
          onSection={choose}
        />
      )}
    </div>
  );
}
