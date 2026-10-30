import { useCallback, useState } from "react";
import type { Api, Dashboard } from "../api";
import {
  dayLabel,
  inCurrency,
  longDay,
  money,
  moneyText,
  signedMoney,
} from "../format";
import { days, monthName } from "../plan/months";
import { useResource, useStore } from "../store";
import Ahead, { NoForecastNote } from "./Ahead";
import { Big, Card, Fact, Good, Line, Table, Warn } from "./blocks";
import CardBalances from "./CardBalances";
import CategoryChart from "./CategoryChart";
import SectionSwitch from "./SectionSwitch";
import SpendingChart from "./SpendingChart";
import { paceVerdict } from "./spending";
import { SECTIONS, storeSection, storedSection, type Section } from "./section";

function Skeleton() {
  return (
    <div className="flex flex-col gap-3" aria-label="Загрузка">
      {[0, 1, 2].map((key) => (
        <div key={key} className="h-20 animate-pulse rounded-2xl bg-section" />
      ))}
    </div>
  );
}

/** «15-го» не рвётся по дефису: неразрывный дефис. */
function glued(text: string): string {
  return text.replace(/(\d)-/g, "$1\u2011");
}

/** Откуда лимит — таблицей: сумы утром, аванс срока, платежи до его конца и деньги
 * на его дни; числа — те же, что делит лимит. */
function LimitSource({ data }: { data: Dashboard }) {
  if (data.sums === null) return null;
  const advances = data.advances ?? 0;
  const bills = data.bills ?? 0;
  const rows = [["сумы утром", money(data.sums)]];
  if (Math.round(advances) > 0 && data.advance_on) {
    rows.push([
      `аванс ${dayLabel(data.advance_on, data.today)}`,
      `+${money(advances)}`,
    ]);
  }
  if (Math.round(bills) > 0) {
    const until = data.payday
      ? dayLabel(data.payday, data.today)
      : "конца месяца";
    rows.push([`платежи до ${until}`, `−${money(bills)}`]);
  }
  if (data.days_left) {
    rows.push([
      `на ${days(data.days_left)}`,
      money(data.sums + advances - bills),
    ]);
  }
  return (
    <div className="my-1">
      <Table rows={rows} align="lr" label="Откуда лимит" />
    </div>
  );
}

/** Сколько можно сегодня — по деньгам до зарплаты, как утром в боте: главная цифра,
 * лимит и траты дня, из чего лимит и план месяца одной строкой, как у графика. */
function Today({ data }: { data: Dashboard }) {
  if (
    data.limit === null ||
    data.left_today === null ||
    data.spent_today === null
  ) {
    return null;
  }
  const until = glued(data.until ?? "до конца месяца");
  const pace = paceVerdict(data.spending, data.today, data.plan_per_day);
  const plan = pace && (
    <p
      className={`text-line font-extrabold ${pace.over ? "text-danger" : "text-ok"}`}
    >
      {pace.text}
    </p>
  );
  if (data.limit <= 0) {
    // как /today: сумов до зарплаты не хватает даже на платежи — лимита на день нет
    const missing = -data.limit * (data.days_left ?? 1);
    return (
      <Card label="Можно сегодня">
        <p className="text-fact font-extrabold text-danger">⚠️ Лимита нет</p>
        <Line>{`${until} не хватает ${money(missing)}`}</Line>
        <LimitSource data={data} />
        <Line>{`потрачено сегодня ${money(data.spent_today)}`}</Line>
        {plan}
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
      <Line>{`лимит ${money(data.limit)} в день ${until}`}</Line>
      <LimitSource data={data} />
      <Line>{`потрачено сегодня ${money(data.spent_today)}`}</Line>
      {plan}
    </Card>
  );
}

/** «Заморожено» — своей карточкой. */
function Frozen({ data }: { data: Dashboard }) {
  const frozen = data.frozen;
  if (!frozen) return null;
  return (
    <Card label="Заморожено">
      <Fact>{`🧊 Заморожено ${moneyText(frozen.amount, frozen.currency)}`}</Fact>
      <Line>
        {Math.round(frozen.change * 100) === 0
          ? "с 1-го числа без изменений"
          : `с 1-го числа ${signedMoney(frozen.change, frozen.currency)}`}
      </Line>
    </Card>
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
  data: Dashboard;
  section: Section;
}

/** Карточки выбранного раздела «Сводки». */
function SectionCards({ data, section }: SectionProps) {
  switch (section) {
    case "today":
      return (
        <>
          {/* лимита нет именно здесь — здесь и объясняем */}
          {data.status === "no_forecast" && <NoForecastNote />}
          <Today data={data} />
          <Frozen data={data} />
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
            <section className="flex flex-col gap-3">
              <h2 className="text-fact font-extrabold">Траты по дням</h2>
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
    case "ahead":
      return <Ahead data={data} />;
  }
}

interface ContentProps {
  data: Dashboard;
  section: Section;
  onSection: (section: Section) => void;
}

function Content({ data, section, onSection }: ContentProps) {
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
      <SectionSwitch
        label="Раздел сводки"
        sections={SECTIONS}
        section={section}
        onSection={onSection}
      />
      <SectionCards data={data} section={section} />
    </>
  );
}

export default function DashboardScreen({ api }: { api: Api }) {
  const store = useStore();
  const load = useCallback((fresh: boolean) => api.dashboard(fresh), [api]);
  const { data, loading, error, reload } = useResource<Dashboard>(
    store,
    "dashboard",
    load,
    { failure: "Не получилось загрузить сводку" },
  );
  // «Обновить» читает таблицу заново, а не копию листов на сервере
  const refresh = useCallback(() => reload(true), [reload]);
  const [section, setSection] = useState<Section>(storedSection);
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
      {data && <Content data={data} section={section} onSection={choose} />}
    </div>
  );
}
