import { useCallback, useState, type ReactNode } from "react";
import type {
  Api,
  Bootstrap,
  Enough,
  Piggy,
  SavingsMonth,
  SavingsScreen,
} from "../api";
import { money, moneyText } from "../format";
import { capital, monthName } from "../plan/months";
import { useResource, useStore } from "../store";
import { Card, Good, Line, Table, Warn } from "./blocks";
import CushionCard from "./CushionCard";
import LifeSheet from "./LifeSheet";
import OrderSheet from "./OrderSheet";
import PercentSheet from "./PercentSheet";
import PiggyCard from "./PiggyCard";
import PiggySheet from "./PiggySheet";
import SaveSheet from "./SaveSheet";
import SavingsCard from "./SavingsCard";
import YearPlan from "./YearPlan";
import { MONTHS_IN, monthIndex, percentText } from "./piggyText";

interface Props {
  api: Api;
  boot: Bootstrap | null; // нет каталога — окна копилки не открыть
  openSave?: boolean; // кнопка бота «Отложил часть…» — сразу окно «Отложил»
}

interface SetAsideProps {
  month: SavingsMonth;
  enough: Enough | null;
  piggies: Piggy[];
  onSave: (() => void) | null; // null — откладывать нечего
  onPercent: (() => void) | null; // null — каталога ещё нет
}

const NBSP = "\u00a0";

/** Полоса: какая часть суммы месяца отложена — копилкам и подушке. */
function SavedBar({ share }: { share: number }) {
  const percent = Math.round(share * 100);
  return (
    <div
      role="progressbar"
      aria-label="Отложено за месяц"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={percent}
      className="my-1 h-3 overflow-hidden rounded-full border border-outline"
    >
      <div
        className="h-full rounded-full bg-ok"
        style={{ width: `${percent}%` }}
      />
    </div>
  );
}

/** «Откладываем N %»: сколько отложить за месяц, сколько уже отложено — копилкам
 * и подушке (её настоящий рост с 1-го, доллары куплены), куда ушло и
 * предупреждения — каждое с тем, что делать. */
function SetAside({
  month,
  enough,
  piggies,
  onSave,
  onPercent,
}: SetAsideProps) {
  const named = MONTHS_IN[monthIndex(month.month)] ?? month.month;
  const needed = month.percent_needed;
  const percent = `${percentText(month.percent)}${NBSP}%`;
  const due = month.due; // отложить за месяц
  const saved = month.saved + month.cushion_saved; // отложено копилкам и подушке
  const more = Math.max(due - saved, 0); // ещё
  // длинное имя копилки одним словом переносится внутри клетки
  const rows: ReactNode[][] = piggies
    .filter((piggy) => piggy.status !== "broken" && piggy.saved_month > 0)
    .map((piggy) => [
      <span className="wrap-anywhere">{piggy.name}</span>,
      moneyText(piggy.saved_month, piggy.currency),
    ]);
  rows.push(["подушка", money(month.cushion_saved)]);
  const needs = needed !== null && month.percent < needed;
  const short = enough !== null && enough.gap < 0;
  const lacks = enough ? money(-enough.gap) : "";
  return (
    <Card label="Откладываем с поступлений">
      {/* «Изменить» справа; на узком экране с крупным шрифтом — строкой ниже */}
      <div className="flex flex-wrap items-start justify-between gap-x-3">
        <h2 className="min-w-min flex-1 text-fact font-extrabold">{`Откладываем ${percent}`}</h2>
        {onPercent && (
          <button
            type="button"
            aria-label="Изменить процент"
            className="shrink-0 text-line font-extrabold text-link"
            onClick={onPercent}
          >
            Изменить
          </button>
        )}
      </div>
      {month.percent_source === "bad" && (
        <Warn>{`Процент не читается, считаю ${percent}`}</Warn>
      )}
      {month.received > 0 ? (
        <>
          {due > 0 && more <= 0 && (
            <Good>{`${capital(monthName(month.month))}: отложено ${money(saved)}`}</Good>
          )}
          {due > 0 && more > 0 && (
            <>
              <Line>{`за ${monthName(month.month)} отложить ${money(due)}`}</Line>
              <SavedBar share={Math.min(saved / due, 1)} />
              <Line>{`отложено ${money(saved)}, ещё ${money(more)}`}</Line>
              {month.left <= 0 && (
                // копилки записаны: остаток — доля подушки, окно её не пишет
                <Line>ещё — доля подушки: купите на неё долларов</Line>
              )}
            </>
          )}
          <Line>{`${percent} от пришедших ${money(month.received)}`}</Line>
          <Table
            label="Куда"
            head={["куда", "отложено"]}
            align="lr"
            rows={rows}
          />
        </>
      ) : (
        <Line>{`в ${named} поступлений ещё не было`}</Line>
      )}
      {onSave && (
        <button
          type="button"
          className="mt-2 h-12 rounded-xl bg-button text-lg font-extrabold text-button-text"
          onClick={onSave}
        >
          Отложил…
        </button>
      )}
      {needs && short && (
        <>
          <Warn>Цели не сходятся с тратами</Warn>
          <Line>{`копилкам к срокам нужно ${needed}${NBSP}%,`}</Line>
          <Line>{`а траты и при ${percent} больше`}</Line>
          <Line>{`дохода на ${lacks} в месяц`}</Line>
          <Line>сдвиньте сроки копилок или сократите траты</Line>
        </>
      )}
      {needs && !short && (
        <>
          <Warn>{`Копилкам к срокам нужно ${needed}${NBSP}%`}</Warn>
          <Line>поднимите процент или сдвиньте сроки копилок</Line>
        </>
      )}
      {short && !needs && (
        <>
          <Warn>{`На жизнь не хватит ${lacks} в месяц`}</Warn>
          <Line>сократите траты или уменьшите процент</Line>
        </>
      )}
    </Card>
  );
}

function Skeleton() {
  return (
    <div className="flex flex-col gap-3" aria-label="Загрузка">
      {[0, 1, 2].map((key) => (
        <div key={key} className="h-20 animate-pulse rounded-2xl bg-section" />
      ))}
    </div>
  );
}

/** «Отложил…» — пока с пришедшего есть что отложить копилкам. */
function canSetAside(data: SavingsScreen): boolean {
  return (data.month?.left ?? 0) > 0;
}

/** «Накопления» — второй раздел «Плана»: строка месяца с «Обновить», что отложить,
 * подушка, копилки, план на год и норма сбережений. */
export default function SavingsTab({ api, boot, openSave }: Props) {
  const store = useStore();
  const load = useCallback((fresh: boolean) => api.savings(fresh), [api]);
  const { data, loading, error, reload } = useResource<SavingsScreen>(
    store,
    "savings",
    load,
    { failure: "Не получилось загрузить накопления" },
  );
  const [opened, setOpened] = useState<Piggy | "new" | null>(null);
  const [saving, setSaving] = useState(false);
  const [setting, setSetting] = useState<"percent" | "order" | "life" | null>(
    null,
  );
  const saved = useCallback(
    (fresh: SavingsScreen) => {
      store.put("savings", fresh);
      // «Заморожено» «Сводки» делится на подушку и копилки — пусть перечитает
      store.expire(["dashboard"]);
      setOpened(null);
      setSaving(false);
      setSetting(null);
    },
    [store],
  );
  // из чата «Отложил часть…»: окно — один раз, по свежему ответу, а не по копии
  // (правка состояния во время рендера: так React советует вместо эффекта)
  const [asked, setAsked] = useState(false);
  if (openSave && !asked && data && !loading) {
    setAsked(true);
    if (data.status === "ok" && canSetAside(data)) setSaving(true);
  }
  // как у «Сводки»: «Обновить» читает таблицу заново, а не копию на сервере
  const header = (
    <div className="flex items-center justify-between">
      <h1 className="text-xl font-extrabold">
        {data
          ? `Накопления · ${monthName(data.today.slice(0, 7))}`
          : "Накопления"}
      </h1>
      <button
        type="button"
        className="text-lg font-bold text-link disabled:opacity-50"
        disabled={loading}
        onClick={() => reload(true)}
      >
        {loading ? "Обновляю…" : "Обновить"}
      </button>
    </div>
  );

  if (!data) {
    return (
      <>
        {header}
        {error ? (
          <div className="flex flex-col items-start gap-2" role="alert">
            <p>{error}</p>
            <button
              type="button"
              className="text-link"
              onClick={() => reload()}
            >
              Повторить
            </button>
          </div>
        ) : (
          loading && <Skeleton />
        )}
      </>
    );
  }
  if (data.status === "no_month_tab") {
    return (
      <>
        {header}
        <p className="text-line">
          Вкладки месяца нет — бот создаёт её 1-го числа.
        </p>
      </>
    );
  }
  const names = Object.fromEntries(
    data.piggies.map((piggy) => [piggy.id, piggy.name]),
  );
  const canSave = canSetAside(data);
  const ordered = data.piggies.filter(
    (piggy) => piggy.order !== null && piggy.status !== "broken",
  );
  return (
    <>
      {header}
      {data.month && (
        <SetAside
          month={data.month}
          enough={data.enough}
          piggies={data.piggies}
          onSave={canSave ? () => setSaving(true) : null}
          onPercent={boot ? () => setSetting("percent") : null}
        />
      )}
      {data.cushion && (
        <CushionCard
          cushion={data.cushion}
          today={data.today}
          onLife={boot ? () => setSetting("life") : undefined}
        />
      )}
      <section className="flex flex-col gap-2" aria-label="Копилки">
        {/* крупный шрифт на 320 px: кнопки переносятся строкой ниже, а не за экран */}
        <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1">
          <h2 className="text-fact font-extrabold">🐷 Копилки</h2>
          {boot && (
            <div className="flex flex-wrap gap-x-4 gap-y-1">
              {ordered.length > 1 && (
                <button
                  type="button"
                  className="text-line font-extrabold text-link"
                  onClick={() => setSetting("order")}
                >
                  Порядок
                </button>
              )}
              <button
                type="button"
                className="text-line font-extrabold text-link"
                onClick={() => setOpened("new")}
              >
                ＋ Копилка
              </button>
            </div>
          )}
        </div>
        {data.piggies.length > 0 ? (
          <ul className="flex flex-col gap-2" aria-label="Копилки">
            {data.piggies.map((piggy) => (
              <PiggyCard
                key={`${piggy.row}-${piggy.id}`}
                piggy={piggy}
                month={data.month?.month ?? data.today.slice(0, 7)}
                onOpen={boot ? setOpened : undefined}
              />
            ))}
          </ul>
        ) : (
          <p className="text-line">
            Заведите копилку на Новый год, отпуск или другую большую трату — бот
            разложит взносы по месяцам
          </p>
        )}
      </section>
      {data.plan.length > 0 && <YearPlan plan={data.plan} names={names} />}
      {data.savings && <SavingsCard savings={data.savings} />}
      {boot && opened && (
        <PiggySheet
          key={opened === "new" ? "new" : opened.id}
          api={api}
          boot={boot}
          data={data}
          piggy={opened === "new" ? null : opened}
          onSaved={saved}
          onClose={() => setOpened(null)}
        />
      )}
      {setting === "percent" && (
        <PercentSheet
          api={api}
          data={data}
          onSaved={saved}
          onClose={() => setSetting(null)}
        />
      )}
      {setting === "order" && (
        <OrderSheet
          api={api}
          data={data}
          onSaved={saved}
          onRefresh={(fresh) => store.put("savings", fresh)}
          onClose={() => setSetting(null)}
        />
      )}
      {boot && setting === "life" && (
        <LifeSheet
          api={api}
          boot={boot}
          data={data}
          onSaved={saved}
          onClose={() => setSetting(null)}
        />
      )}
      {saving && (
        <SaveSheet
          api={api}
          boot={boot}
          data={data}
          onSaved={saved}
          onClose={() => setSaving(false)}
        />
      )}
    </>
  );
}
