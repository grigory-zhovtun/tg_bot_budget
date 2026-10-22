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
import { money, moneyText } from "../format";
import { monthName } from "../plan/months";
import { useResource, useStore } from "../store";
import { Card, Fact, Good, Line, Table, Warn } from "./blocks";
import CushionCard from "./CushionCard";
import EnoughLines from "./EnoughLines";
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
  piggies: Piggy[];
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

/** «Откладываем N %»: сколько пришло, сколько отложить и куда, успеваем ли к
 * срокам и хватает ли на жизнь. */
function SetAside({
  month,
  piggies,
  enough,
  goal,
  onSave,
  onPercent,
}: SetAsideProps) {
  const differs = goalDiffers(goal);
  const named = MONTHS_IN[monthIndex(month.month)] ?? month.month;
  const rows = piggies
    .filter((piggy) => piggy.status !== "broken" && piggy.now > 0)
    .map((piggy) => [piggy.name, moneyText(piggy.now, piggy.currency)]);
  if (month.cushion > 0) rows.push(["подушка", money(month.cushion)]);
  const needed = month.percent_needed;
  const percent = `${percentText(month.percent)}${NBSP}%`;
  return (
    <Card label="Откладываем с поступлений">
      <h2 className="text-fact font-extrabold">{`Откладываем ${percent}`}</h2>
      <Line>с каждого поступления</Line>
      {onPercent && (
        <button
          type="button"
          className="self-start text-line font-extrabold text-link"
          onClick={onPercent}
        >
          Изменить процент
        </button>
      )}
      {month.percent_source === "bad" && (
        <>
          <Warn>Процент не читается</Warn>
          <Line>{`считаю по ${percent}`}</Line>
        </>
      )}
      {month.received > 0 ? (
        <>
          <Fact>{`Отложить ${money(month.due)}`}</Fact>
          <Line>{`в ${named} пришло ${money(month.received)}`}</Line>
        </>
      ) : (
        <Line>{`в ${named} поступлений ещё не было`}</Line>
      )}
      {rows.length > 0 && (
        <Table head={["копилка", "сумма"]} align="lr" rows={rows} />
      )}
      {needed !== null && month.percent < needed && (
        <>
          <Warn>{`К срокам нужно ${needed}${NBSP}%`}</Warn>
          <Line>чтобы успеть все копилки</Line>
        </>
      )}
      {needed !== null && month.percent >= needed && (
        <Good>Успеваем всё к срокам</Good>
      )}
      {enough && <EnoughLines enough={enough} />}
      {differs && (
        <>
          <Warn>План месяца не сходится</Warn>
          <Line>{`в плане «Отложить» ${money(differs.inPlan)}`}</Line>
          <Line>{`по копилкам нужно ${money(differs.needed)}`}</Line>
          <Line>поправьте в «Плане месяца»</Line>
        </>
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
          piggies={data.piggies}
          enough={data.enough}
          goal={data.goal ?? null}
          onSave={canSave ? () => setSaving(true) : null}
          onPercent={boot ? () => setSetting("percent") : null}
        />
      )}
      {data.cushion && <CushionCard cushion={data.cushion} />}
      <section className="flex flex-col gap-2" aria-label="Копилки">
        <div className="flex items-center justify-between gap-2">
          <h2 className="text-fact font-extrabold">🐷 Копилки</h2>
          {boot && (
            <div className="flex gap-4">
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
