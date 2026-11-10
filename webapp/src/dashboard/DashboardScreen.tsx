import { useCallback, useRef, useState, type ReactNode } from "react";
import type { Api, Dashboard, Tomorrow } from "../api";
import { dayLabel, glued, inCurrency, longDay, money } from "../format";
import { days, monthName } from "../plan/months";
import { useResource, useStore } from "../store";
import { Big, Card, Fact, Good, Line, Table, Title, Warn } from "./blocks";
import CardBalances from "./CardBalances";
import CategoryChart from "./CategoryChart";
import FrozenCard from "./FrozenCard";
import QuickActions, { type Action } from "./QuickActions";
import SectionSwitch from "./SectionSwitch";
import SpendingChart from "./SpendingChart";
import SpendingMap from "./SpendingMap";
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

/** Без блока прогноза во вкладке месяца лимит на день не посчитать. */
function NoForecastNote() {
  return (
    <div className="flex flex-col">
      <Warn>Нет блока прогноза</Warn>
      <Line>во вкладке месяца</Line>
      <Line>лимит на день не посчитать</Line>
    </div>
  );
}

/** Деньги лимита: сумы, аванс срока, платежи до его конца и дни срока. */
interface LimitMoney {
  sums: number | null;
  advances: number | null;
  advance_on: string | null;
  bills: number | null;
  payday: string | null;
  days: number | null;
}

interface LimitSourceProps {
  source: LimitMoney;
  today: string;
  first: string; // «сумы утром» — лимит дня, «сумы сейчас» — лимит с завтра
}

/** Откуда лимит — таблицей: сумы, аванс срока, платежи до его конца и деньги на его
 * дни; числа — те же, что делит лимит. */
function LimitSource({ source, today, first }: LimitSourceProps) {
  if (source.sums === null) return null;
  const advances = source.advances ?? 0;
  const bills = source.bills ?? 0;
  const rows = [[first, money(source.sums)]];
  if (Math.round(advances) > 0 && source.advance_on) {
    rows.push([
      `аванс ${dayLabel(source.advance_on, today)}`,
      `+${money(advances)}`,
    ]);
  }
  if (Math.round(bills) > 0) {
    const until = source.payday
      ? dayLabel(source.payday, today)
      : "конца месяца";
    rows.push([`платежи до ${until}`, `−${money(bills)}`]);
  }
  if (source.days) {
    rows.push([
      `на ${days(source.days)}`,
      money(source.sums + advances - bills),
    ]);
  }
  return (
    <div className="my-1">
      <Table rows={rows} align="lr" label="Откуда лимит" />
    </div>
  );
}

/** День прихода зарплаты: утренний лимит устарел — лимит с завтра по деньгам сейчас;
 * «сверх лимита дня» не показываем. */
function SalaryDay({
  data,
  tomorrow,
  plan,
}: {
  data: Dashboard;
  tomorrow: Tomorrow;
  plan: ReactNode;
}) {
  const ahead = glued(tomorrow.until);
  const came = (
    <Good>{`${data.came ?? "Пришла ЗП"} ${money(data.salary_came ?? 0)}`}</Good>
  );
  const source = (
    <LimitSource source={tomorrow} today={data.today} first="сумы сейчас" />
  );
  const spent = (
    <Line>{`потрачено сегодня ${money(data.spent_today ?? 0)}`}</Line>
  );
  if (tomorrow.limit <= 0) {
    // и с зарплатой денег до срока не хватает — как обычный день без лимита
    const missing = -tomorrow.limit * tomorrow.days;
    return (
      <Card label="Можно сегодня">
        {came}
        <p className="text-fact font-extrabold text-danger">⚠️ Лимита нет</p>
        <Line>{`${ahead} не хватает ${money(missing)}`}</Line>
        {source}
        {spent}
        {plan}
      </Card>
    );
  }
  return (
    <Card label="Можно сегодня">
      {came}
      <Big>{`${money(tomorrow.limit)} сум`}</Big>
      <Line>можно в день с завтра</Line>
      <Line>{`${ahead}, ${days(tomorrow.days)}`}</Line>
      {source}
      {spent}
      {plan}
    </Card>
  );
}

/** Сколько можно сегодня — по деньгам до зарплаты, как утром в боте: главная цифра,
 * лимит и траты дня, из чего лимит и план месяца одной строкой, как у графика. */
function Today({ data, actions }: { data: Dashboard; actions: ReactNode }) {
  if (
    data.limit === null ||
    data.left_today === null ||
    data.spent_today === null
  ) {
    // лимита нет — действия всё равно под рукой
    return <>{actions}</>;
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
  if (data.tomorrow) {
    return (
      <>
        <SalaryDay data={data} tomorrow={data.tomorrow} plan={plan} />
        {actions}
      </>
    );
  }
  const morning: LimitMoney = {
    sums: data.sums,
    advances: data.advances,
    advance_on: data.advance_on,
    bills: data.bills,
    payday: data.payday,
    days: data.days_left,
  };
  const source = (
    <LimitSource source={morning} today={data.today} first="сумы утром" />
  );
  if (data.limit <= 0) {
    // как /today: сумов до зарплаты не хватает даже на платежи — лимита на день нет
    const missing = -data.limit * (data.days_left ?? 1);
    return (
      <>
        <Card label="Можно сегодня">
          <p className="text-fact font-extrabold text-danger">⚠️ Лимита нет</p>
          <Line>{`${until} не хватает ${money(missing)}`}</Line>
          {source}
          <Line>{`потрачено сегодня ${money(data.spent_today)}`}</Line>
          {plan}
        </Card>
        {actions}
      </>
    );
  }
  const over = data.left_today < 0;
  const share = Math.min(Math.max(data.spent_today / data.limit, 0), 1);
  return (
    <section aria-label="Можно сегодня" className="flex flex-col gap-3">
      {/* главная цифра — по центру, без карточки: с неё начинается экран */}
      <div className="flex flex-col items-center gap-1 pt-1 text-center">
        {/* перед «сум» — обычный пробел: при крупном шрифте единица уходит вниз, а не за экран */}
        <Big hero>{`${money(data.left_today)} сум`}</Big>
        {over ? (
          <Warn>сверх лимита дня</Warn>
        ) : (
          <Line>можно потратить сегодня</Line>
        )}
        {/* сколько лимита дня уже ушло; сверх лимита — вся полоса красная */}
        <div
          aria-hidden="true"
          data-day-bar={over ? "over" : "ok"}
          className="my-1 h-3 w-full overflow-hidden rounded-full bg-stripe"
        >
          <div
            className={`h-full rounded-full ${over ? "bg-chip-bad" : "bg-warn"}`}
            style={{ width: `${Math.round((over ? 1 : share) * 100)}%` }}
          />
        </div>
        <Line>{`лимит ${money(data.limit)} в день ${until}`}</Line>
      </div>
      {actions}
      <div className="flex flex-col gap-1 rounded-2xl bg-section p-4">
        {source}
        <Line>{`потрачено сегодня ${money(data.spent_today)}`}</Line>
        {plan}
      </div>
    </section>
  );
}

/** Хватит ли сумов до зарплаты при нынешнем темпе и сколько долларов обменять;
 * «сейчас» — сумы с утра уже могли измениться. */
function Sums({ data }: { data: Dashboard }) {
  const runway = data.runway;
  if (!runway) return null;
  const head = <Fact>{`💳 Сумы сейчас ${money(runway.balance)}`}</Fact>;
  const pace = money(runway.pace);
  // срок до зарплаты называет сервер; в старом ответе его нет — как раньше
  const target =
    runway.target != null
      ? glued(runway.target)
      : runway.until
        ? `до ${longDay(runway.until)}`
        : "до конца месяца";
  if (runway.short <= 0) {
    const left = runway.left ?? null;
    if (left === null || runway.target == null) {
      return (
        <Card label="Сумы">
          {head}
          <Good>до конца месяца хватит</Good>
          <Line>{`в день ≈ ${pace}`}</Line>
          <Line>в среднем за 14 дней</Line>
        </Card>
      );
    }
    return (
      <Card label="Сумы">
        {head}
        <Good>{`${target} хватит`}</Good>
        <Line>{`останется ≈ ${money(left)}`}</Line>
        <Line>{`если тратить ≈ ${pace} в день,`}</Line>
        <Line>как в среднем за 14 дней</Line>
      </Card>
    );
  }
  let when: string;
  if (runway.last_day === null || runway.last_day < data.today) {
    when = "сумы уже закончились";
  } else if (runway.last_day === data.today) {
    when = "хватит только на сегодня";
  } else {
    when = `хватит до ${longDay(runway.last_day)}`;
  }
  return (
    <Card label="Сумы">
      {head}
      <Warn>{when}</Warn>
      <Line>
        {runway.dollars !== null
          ? `${target} обменять ≈ ${inCurrency(runway.dollars, "USD")}`
          : `${target} обменять`}
      </Line>
      <Line>{`не хватит ${money(runway.short)}`}</Line>
      <Line>{`в день ≈ ${pace}`}</Line>
      <Line>в среднем за 14 дней</Line>
    </Card>
  );
}

/**
 * «Траты»: карта трат, план-факт по категориям и траты по дням. Плитка карты
 * раскрывает свою группу в план-факте и подводит к ней экран.
 */
function Spending({ data }: { data: Dashboard }) {
  const [open, setOpen] = useState<string | null>(null);
  const chart = useRef<HTMLDivElement>(null);
  const pick = useCallback((group: string) => {
    setOpen(group);
    // после перерисовки: подкатегории уже раскрыты
    requestAnimationFrame(() => {
      const rows = chart.current?.querySelectorAll<HTMLElement>("[data-group]");
      const row = [...(rows ?? [])].find(
        (item) => item.dataset.group === group,
      );
      row?.scrollIntoView?.({ behavior: "smooth", block: "start" });
    });
  }, []);
  return (
    <>
      {data.categories.length > 0 && (
        <>
          <SpendingMap categories={data.categories} onPick={pick} />
          <div ref={chart}>
            <CategoryChart
              categories={data.categories}
              untracked={data.untracked}
              today={data.today}
              open={open}
              onOpen={setOpen}
            />
          </div>
        </>
      )}
      {data.spending.length > 0 && (
        <section className="flex flex-col gap-3">
          <Title>Траты по дням</Title>
          <SpendingChart
            spending={data.spending}
            today={data.today}
            // в день зарплаты «можно X в день» — с завтра, как в «Сегодня»
            limit={data.tomorrow?.limit ?? data.limit}
            planPerDay={data.plan_per_day}
          />
        </section>
      )}
    </>
  );
}

interface SectionProps {
  data: Dashboard;
  section: Section;
  actions: Action[];
  onAction?: (action: Action) => void;
}

/** Карточки выбранного раздела «Сводки». */
function SectionCards({ data, section, actions, onAction }: SectionProps) {
  switch (section) {
    case "today":
      return (
        <>
          {/* лимита нет именно здесь — здесь и объясняем */}
          {data.status === "no_forecast" && <NoForecastNote />}
          <Today
            data={data}
            actions={
              onAction && <QuickActions actions={actions} onAction={onAction} />
            }
          />
          {data.frozen && (
            <FrozenCard frozen={data.frozen} today={data.today} />
          )}
          <Sums data={data} />
          {data.cards.length > 0 && (
            <CardBalances cards={data.cards} today={data.today} />
          )}
        </>
      );
    case "spending":
      return <Spending data={data} />;
  }
}

interface ContentProps extends SectionProps {
  onSection: (section: Section) => void;
}

function Content({
  data,
  section,
  onSection,
  actions,
  onAction,
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
      <SectionSwitch
        label="Раздел сводки"
        sections={SECTIONS}
        section={section}
        onSection={onSection}
      />
      <SectionCards
        data={data}
        section={section}
        actions={actions}
        onAction={onAction}
      />
    </>
  );
}

interface Props {
  api: Api;
  actions?: Action[]; // быстрые действия под главной цифрой — по справочнику
  onAction?: (action: Action) => void; // нет — экран без них
}

const NO_ACTIONS: Action[] = [];

export default function DashboardScreen({
  api,
  actions = NO_ACTIONS,
  onAction,
}: Props) {
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
      {data && (
        <Content
          data={data}
          section={section}
          onSection={choose}
          actions={actions}
          onAction={onAction}
        />
      )}
    </div>
  );
}
