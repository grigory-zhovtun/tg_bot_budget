import { useCallback, useState } from "react";
import type { Api, Bootstrap, CalendarRecord, Forecast } from "../api";
import { useResource, useStore } from "../store";
import DayCard from "./DayCard";
import ForecastChart from "./ForecastChart";
import MonthGrid from "./MonthGrid";
import RecordSheet from "./RecordSheet";
import RecordsList from "./RecordsList";
import {
  TOTAL,
  amount,
  chartNote,
  currencyRows,
  dayOf,
  hasDollars,
  fitView,
  marks,
  perDay,
  scopes,
  shortDate,
  storeView,
  storedView,
  viewCode,
  viewDays,
  warnings,
  type View,
} from "./view";

const EXPLAIN =
  "План — остаток плана статей, разложенный по дням. Темп — средние траты за 14 дней без переводов и платежей из списка.";

function Skeleton() {
  return (
    <div className="flex flex-col gap-3" aria-label="Загрузка">
      {[0, 1, 2].map((key) => (
        <div key={key} className="h-20 animate-pulse rounded-2xl bg-section" />
      ))}
    </div>
  );
}

interface SwitchesProps {
  data: Forecast;
  view: View;
  onView: (view: View) => void;
}

/** «Всего · UZS · USD · RUB» и для «Всего» — «сум | $». */
function Switches({ data, view, onView }: SwitchesProps) {
  const button = (on: boolean) =>
    `rounded-lg px-2 py-1 text-sm font-medium ${on ? "bg-bg shadow-sm" : "text-hint"}`;
  return (
    <div className="flex gap-2">
      <div
        role="radiogroup"
        aria-label="Что показать"
        className="grid flex-1 auto-cols-fr grid-flow-col gap-1 rounded-xl bg-section p-1"
      >
        {scopes(data).map((scope) => (
          <button
            key={scope}
            type="button"
            role="radio"
            aria-checked={view.scope === scope}
            className={button(view.scope === scope)}
            onClick={() => onView({ ...view, scope })}
          >
            {scope === TOTAL ? "Всего" : scope}
          </button>
        ))}
      </div>
      {view.scope === TOTAL && hasDollars(data) && (
        <div
          role="radiogroup"
          aria-label="Единица"
          className="grid grid-cols-2 gap-1 rounded-xl bg-section p-1"
        >
          {(["uzs", "usd"] as const).map((unit) => (
            <button
              key={unit}
              type="button"
              role="radio"
              aria-checked={view.unit === unit}
              className={button(view.unit === unit)}
              onClick={() => onView({ ...view, unit })}
            >
              {unit === "uzs" ? "сум" : "$"}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

/** Выбранный день, если он ещё в месяце ответа; иначе сегодня. */
function shownDay(data: Forecast, selected: number | null): number {
  const today = dayOf(data.today);
  const end = data.end ? dayOf(data.end) : today;
  return selected !== null && selected >= 1 && selected <= end
    ? selected
    : today;
}

interface MonthProps {
  data: Forecast;
  view: View;
  day: number;
  onView: (view: View) => void;
  onSelect: (day: number) => void;
  onAdd?: (day: number) => void; // нет — без справочника записи только смотреть
  onEdit?: (id: string) => void;
}

function Month({
  data,
  view,
  day,
  onView,
  onSelect,
  onAdd,
  onEdit,
}: MonthProps) {
  const end = data.end ? dayOf(data.end) : dayOf(data.today);
  const code = viewCode(view);
  const days = viewDays(data, view);
  const last = days[days.length - 1];
  const ends = last
    ? `по плану ${amount(last.plan, code)}, по темпу ${amount(last.pace, code)}`
    : "";
  return (
    <>
      <Switches data={data} view={view} onView={onView} />
      {warnings(data, view).map((text) => (
        <p key={text} className="text-sm font-medium text-danger">
          {text}
        </p>
      ))}
      <MonthGrid data={data} view={view} selected={day} onSelect={onSelect} />
      <DayCard
        data={data}
        view={view}
        day={day}
        onAdd={onAdd}
        onEdit={onEdit}
      />
      <section className="flex flex-col gap-2 rounded-2xl bg-section p-4">
        <div className="flex items-baseline justify-between gap-2">
          <h2 className="font-semibold">До конца месяца</h2>
          <span className="text-xs text-hint">{chartNote(view)}</span>
        </div>
        <ForecastChart
          days={days}
          marks={marks(data, view)}
          selected={day}
          label={`До конца месяца: ${ends}`}
          onSelect={onSelect}
        />
        {last && (
          <p className="flex flex-wrap gap-x-3 text-xs text-hint">
            <span>
              <span className="text-link">━</span>
              {` по плану ≈ ${amount(last.plan, code)}`}
            </span>
            <span>
              <span className="text-warn">╍</span>
              {` по темпу ≈ ${amount(last.pace, code)}`}
            </span>
          </p>
        )}
      </section>
      <RecordsList
        data={data}
        // в прошлое не планируют: при выбранном прошедшем дне — с сегодня
        onAdd={onAdd && (() => onAdd(Math.max(day, dayOf(data.today))))}
        onEdit={onEdit}
      />
      <section className="flex flex-col gap-1">
        <h2 className="font-semibold">
          {`По валютам: сейчас → ${shortDate(end, data.today)}`}
        </h2>
        <ul className="flex flex-col gap-1 text-sm">
          {currencyRows(data).map((row) => (
            <li
              key={row.code}
              className="grid grid-cols-[2.5rem_1fr_auto] items-baseline gap-2"
            >
              <span className="text-hint">{row.code}</span>
              <span>
                {row.now}{" "}
                <span className="whitespace-nowrap">
                  → <b>{row.end}</b>
                </span>
                {!row.counted && (
                  <span className="text-xs text-hint">
                    {" "}
                    · нет курса, не во «Всего»
                  </span>
                )}
              </span>
              <span
                className={`text-xs ${row.minus ? "text-danger" : "text-hint"}`}
              >
                {row.pace}
              </span>
            </li>
          ))}
        </ul>
      </section>
      <p className="text-xs text-hint">{`${EXPLAIN} ${perDay(data, view)}`}</p>
    </>
  );
}

interface Editing {
  record: CalendarRecord | null; // null — новая запись
  day: number;
}

interface Props {
  api: Api;
  boot: Bootstrap | null; // справочник для окна записи; нет — записи только смотреть
}

export default function ForecastScreen({ api, boot }: Props) {
  const store = useStore();
  const load = useCallback((fresh: boolean) => api.forecast(fresh), [api]);
  const { data, loading, error, reload } = useResource<Forecast>(
    store,
    "forecast",
    load,
    { failure: "Не получилось загрузить прогноз" },
  );
  const [chosen, setChosen] = useState<View>(storedView);
  const [selected, setSelected] = useState<number | null>(null);
  const [editing, setEditing] = useState<Editing | null>(null);
  const view = data ? fitView(chosen, data) : chosen;

  // «Обновить» читает таблицу заново, а не копию листов на сервере
  const refresh = useCallback(() => {
    reload(true);
  }, [reload]);
  const changeView = useCallback((next: View) => {
    setChosen(next);
    storeView(next);
  }, []);
  const day = data ? shownDay(data, selected) : 0;
  const saved = useCallback(
    (fresh: Forecast) => {
      store.put("forecast", fresh);
      // лимит дня в «Сводке» учитывает записи календаря
      store.expire(["dashboard"]);
      setEditing(null);
    },
    [store],
  );
  const add = useCallback((on: number) => {
    setEditing({ record: null, day: on });
  }, []);
  const edit = useCallback(
    (id: string) => {
      const record = data?.calendar.find((item) => item.id === id);
      if (record) setEditing({ record, day });
    },
    [data, day],
  );

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">
          {data ? `Прогноз · ${data.month}` : "Прогноз"}
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
      {data?.status === "no_month_tab" && (
        <p>Нет вкладки месяца — прогноз появится, когда она будет</p>
      )}
      {data?.status === "ok" && (
        <Month
          data={data}
          view={view}
          day={day}
          onView={changeView}
          onSelect={setSelected}
          onAdd={boot ? add : undefined}
          onEdit={boot ? edit : undefined}
        />
      )}
      {editing && boot && data?.status === "ok" && (
        <RecordSheet
          api={api}
          boot={boot}
          data={data}
          day={editing.day}
          record={editing.record}
          onSaved={saved}
          onClose={() => setEditing(null)}
        />
      )}
    </div>
  );
}
