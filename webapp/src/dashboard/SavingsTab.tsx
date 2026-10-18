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
import PiggyCard from "./PiggyCard";
import PiggySheet from "./PiggySheet";
import SaveSheet from "./SaveSheet";
import SavingsCard from "./SavingsCard";
import YearPlan from "./YearPlan";
import { enoughText, MONTHS_IN, monthIndex, shareText } from "./piggyText";

interface Props {
  api: Api;
  boot: Bootstrap | null; // нет каталога — окна копилки не открыть
  refresh: number; // растёт с каждым «Обновить» «Сводки» — читать таблицу заново
  openSave?: boolean; // кнопка бота «Отложил часть…» — сразу окно «Отложил»
}

interface SetAsideProps {
  month: SavingsMonth;
  enough: Enough | null;
  names: Record<string, string>;
  goal: SavingsGoal | null;
  onSave: (() => void) | null; // null — откладывать нечего
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

/** «В октябре отложить»: взносы копилок, доля 10 % и хватает ли на жизнь. */
function SetAside({ month, enough, names, goal, onSave }: SetAsideProps) {
  const differs = goalText(goal);
  const title = `В ${MONTHS_IN[monthIndex(month.month)] ?? month.month} отложить`;
  return (
    <section
      aria-label={title}
      className="flex flex-col gap-1 rounded-2xl bg-section p-4"
    >
      <h2 className="font-semibold">{title}</h2>
      <p>
        {month.piggy > 0
          ? `в копилки по плану ${short(month.piggy)}, осталось ${short(month.piggy_left)}`
          : "в копилки в этом месяце — ничего"}
      </p>
      {month.share > 0 && (
        <p>{`10 % от поступлений ≈ ${short(month.share)} — ${shareText(month, names)}`}</p>
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
/** «Отложил…» раскладывает сумму по взносам месяца — без них класть некуда. */
function canSetAside(data: SavingsScreen): boolean {
  return data.piggies.some(
    (piggy) => piggy.status !== "broken" && piggy.left_this_month > 0,
  );
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
  const saved = useCallback(
    (fresh: SavingsScreen) => {
      store.put("savings", fresh);
      setOpened(null);
      setSaving(false);
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
  return (
    <>
      {data.month && (
        <SetAside
          month={data.month}
          enough={data.enough}
          names={names}
          goal={data.goal ?? null}
          onSave={canSave ? () => setSaving(true) : null}
        />
      )}
      {data.cushion && <CushionCard cushion={data.cushion} />}
      <section className="flex flex-col gap-2" aria-label="Копилки">
        <div className="flex items-center justify-between gap-2">
          <h2 className="font-semibold">🐷 Копилки</h2>
          {boot && (
            <button
              type="button"
              className="font-semibold text-link"
              onClick={() => setOpened("new")}
            >
              ＋ Копилка
            </button>
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
