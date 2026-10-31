import { useCallback, useState } from "react";
import type {
  Api,
  Bootstrap,
  Enough,
  Piggy,
  SavingsGoal,
  SavingsMonth,
  SavingsScreen,
} from "../api";
import { money } from "../format";
import { monthName } from "../plan/months";
import { useResource, useStore } from "../store";
import { Card, Good, Line, Warn } from "./blocks";
import CushionCard from "./CushionCard";
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
  goal: SavingsGoal | null;
  onSave: (() => void) | null; // null — откладывать нечего
  onPercent: (() => void) | null; // null — каталога ещё нет
}

const GOAL_STEP = 1000; // в таблицу цель пишется до тысячи
const NBSP = "\u00a0";

/** Q20 вкладки разошлась с копилками: сколько в плане и сколько нужно; сходится — null. */
function goalDiffers(
  goal: SavingsGoal | null,
): { inPlan: number; needed: number } | null {
  if (!goal) return null;
  const inPlan = goal.in_plan ?? 0;
  if (goal.in_plan !== null && Math.abs(inPlan - goal.suggested) < GOAL_STEP) {
    return null;
  }
  return { inPlan, needed: goal.suggested };
}

/** Полоса: какая часть доли копилок (сумма месяца без подушки) отложена; доходит
 * до конца, когда откладывать больше нечего. */
function SavedBar({ share }: { share: number }) {
  const percent = Math.round(share * 100);
  return (
    <div
      role="progressbar"
      aria-label="Отложено копилкам"
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

/** «Откладываем N %» коротко: сколько отложить за месяц, полоса отложенного и
 * предупреждения по одной строке; подробности — в окне процента. */
function SetAside({ month, enough, goal, onSave, onPercent }: SetAsideProps) {
  const differs = goalDiffers(goal);
  const named = MONTHS_IN[monthIndex(month.month)] ?? month.month;
  const needed = month.percent_needed;
  const percent = `${percentText(month.percent)}${NBSP}%`;
  const piggies = Math.max(month.due - month.cushion, 0); // доля копилок
  const share =
    piggies > 0 ? Math.min(Math.max(1 - month.left / piggies, 0), 1) : 0;
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
          <Line>{`за ${monthName(month.month)} отложить ${money(month.due)}`}</Line>
          {piggies > 0 && (
            <>
              <SavedBar share={share} />
              {month.left > 0 ? (
                <Line>
                  {`отложено ${money(month.saved)}, ещё ${money(month.left)}`}
                </Line>
              ) : (
                <Good>{`Отложено ${money(month.saved)} из ${money(piggies)}`}</Good>
              )}
            </>
          )}
          {month.cushion > 0 && (
            <Line>{`подушке — ${money(month.cushion)}`}</Line>
          )}
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
      {needed !== null && month.percent < needed && (
        <Warn>{`К срокам нужно ${needed}${NBSP}%`}</Warn>
      )}
      {enough && enough.gap < 0 && (
        <Warn>{`На жизнь не хватит ${money(-enough.gap)}`}</Warn>
      )}
      {differs && (
        <Warn>
          {`В плане «Отложить» ${money(differs.inPlan)}, нужно ${money(differs.needed)}`}
        </Warn>
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
  const [setting, setSetting] = useState<"percent" | "order" | null>(null);
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
          goal={data.goal ?? null}
          onSave={canSave ? () => setSaving(true) : null}
          onPercent={boot ? () => setSetting("percent") : null}
        />
      )}
      {data.cushion && (
        <CushionCard cushion={data.cushion} today={data.today} />
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
