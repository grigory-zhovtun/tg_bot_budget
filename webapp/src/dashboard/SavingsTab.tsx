import { useCallback, useEffect, useRef, useState } from "react";
import type {
  Api,
  Bootstrap,
  Enough,
  Piggy,
  SavingsGoal,
  SavingsMonth,
  SavingsScreen,
} from "../api";
import { short } from "../format";
import { useResource, useStore } from "../store";
import CushionCard from "./CushionCard";
import OrderSheet from "./OrderSheet";
import PercentSheet from "./PercentSheet";
import PiggyCard from "./PiggyCard";
import PiggySheet from "./PiggySheet";
import SaveSheet from "./SaveSheet";
import SavingsCard from "./SavingsCard";
import YearPlan from "./YearPlan";
import {
  enoughText,
  MONTHS_IN,
  monthIndex,
  percentText,
  piggyMoney,
} from "./piggyText";

interface Props {
  api: Api;
  boot: Bootstrap | null; // нет каталога — окна копилки не открыть
  refresh: number; // растёт с каждым «Обновить» «Сводки» — читать таблицу заново
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

/** Q20 вкладки разошлась с копилками: «В плане месяца «Отложить» — W, …». */
function goalText(goal: SavingsGoal | null): string | null {
  if (!goal) return null;
  const inPlan = goal.in_plan ?? 0;
  if (goal.in_plan !== null && Math.abs(inPlan - goal.suggested) < GOAL_STEP) {
    return null;
  }
  return `В плане месяца «Отложить» — ${short(inPlan)}, по копилкам нужно ${short(goal.suggested)}: поправьте в «Плане»`;
}

/** «Откладываем N % с поступлений»: сколько пришло, сколько отложить и кому,
 * успеваем ли к срокам и хватает ли на жизнь. */
function SetAside({
  month,
  piggies,
  enough,
  goal,
  onSave,
  onPercent,
}: SetAsideProps) {
  const differs = goalText(goal);
  const named = MONTHS_IN[monthIndex(month.month)] ?? month.month;
  const parts = piggies
    .filter((piggy) => piggy.status !== "broken" && piggy.now > 0)
    .map((piggy) => `«${piggy.name}» ${piggyMoney(piggy.now, piggy.currency)}`)
    .join(", ");
  const line = [
    parts,
    month.cushion > 0 ? `в подушку ${short(month.cushion)}` : "",
  ]
    .filter(Boolean)
    .join(" · ");
  const needed = month.percent_needed;
  return (
    <section
      aria-label="Откладываем с поступлений"
      className="flex flex-col gap-1 rounded-2xl bg-section p-4"
    >
      <div className="flex items-start justify-between gap-3">
        <h2 className="font-semibold">
          {`Откладываем ${percentText(month.percent)} % с поступлений`}
        </h2>
        {onPercent && (
          <button
            type="button"
            className="shrink-0 font-semibold text-link"
            onClick={onPercent}
          >
            Изменить
          </button>
        )}
      </div>
      {month.percent_source === "bad" && (
        <p className="text-sm font-semibold text-warn">
          {`Процент в таблице не читается — считаю по ${percentText(month.percent)} %`}
        </p>
      )}
      <p>
        {month.received > 0
          ? `В ${named} пришло ${short(month.received)} — отложить ${short(month.due)}`
          : `В ${named} поступлений ещё не было`}
      </p>
      {line && <p>{line}</p>}
      {needed !== null && month.percent < needed && (
        <p className="text-sm font-semibold text-warn">
          {`Чтобы успеть всё к срокам, нужно ${needed} %`}
        </p>
      )}
      {needed !== null && month.percent >= needed && (
        <p className="text-sm font-semibold text-ok">Успеваем всё к срокам ✓</p>
      )}
      {enough && (
        <p
          className={`text-sm font-semibold ${enough.gap < 0 ? "text-danger" : "text-ok"}`}
        >
          {enoughText(enough)}
        </p>
      )}
      {differs && <p className="text-sm font-semibold text-warn">{differs}</p>}
      {onSave && (
        <button
          type="button"
          className="mt-2 h-11 rounded-xl bg-button font-semibold text-button-text"
          onClick={onSave}
        >
          Отложил…
        </button>
      )}
    </section>
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

/** Вкладка «Накопления»: что отложить, подушка, копилки, план на год и норма. */
/** «Отложил…» — пока с пришедшего есть что отложить копилкам. */
function canSetAside(data: SavingsScreen): boolean {
  return (data.month?.left ?? 0) > 0;
}

export default function SavingsTab({ api, boot, refresh, openSave }: Props) {
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
  // «Обновить» «Сводки» после открытия вкладки — читать таблицу заново
  const seen = useRef(refresh);
  useEffect(() => {
    if (refresh !== seen.current) {
      seen.current = refresh;
      reload(true);
    }
  }, [refresh, reload]);

  if (!data) {
    if (error) {
      return (
        <div className="flex flex-col items-start gap-2" role="alert">
          <p>{error}</p>
          <button type="button" className="text-link" onClick={() => reload()}>
            Повторить
          </button>
        </div>
      );
    }
    return loading ? <Skeleton /> : null;
  }
  if (data.status === "no_month_tab") {
    return <p>Вкладки месяца нет — бот создаёт её 1-го числа.</p>;
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
          <h2 className="font-semibold">🐷 Копилки</h2>
          {boot && (
            <div className="flex gap-4">
              {ordered.length > 1 && (
                <button
                  type="button"
                  className="font-semibold text-link"
                  onClick={() => setSetting("order")}
                >
                  Порядок
                </button>
              )}
              <button
                type="button"
                className="font-semibold text-link"
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
          <p className="text-sm">
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
