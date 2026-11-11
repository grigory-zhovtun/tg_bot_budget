import { useCallback, useEffect, useState, type ReactNode } from "react";
import type { Api, Bootstrap, CalendarRecord, Forecast } from "../api";
import { deviceDay } from "../format";
import { prevMonth } from "../plan/months";
import { useCached, useResource, useStore, type Store } from "../store";
import DayCard from "./DayCard";
import LaterRecords from "./LaterRecords";
import MonthGrid from "./MonthGrid";
import MonthNav from "./MonthNav";
import MonthOutlook from "./MonthOutlook";
import MonthSummary from "./MonthSummary";
import RecordSheet from "./RecordSheet";
import {
  TOTAL,
  currencyRows,
  dayOf,
  hasDollars,
  fitView,
  scopes,
  shortDate,
  storeView,
  storedView,
  warnings,
  type View,
} from "./view";

/** Ключ кэша: текущий месяц — «forecast», прошедший — «forecast:2026-09». */
function cacheKey(month: string | null): string {
  return month === null ? "forecast" : `forecast:${month}`;
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

/** Выбранный день, если он ещё в месяце ответа; иначе сегодня (у прошедшего
 * месяца «сегодня» ответа — его последний день). */
function shownDay(data: Forecast, selected: number | null): number {
  const today = dayOf(data.today);
  const end = data.end ? dayOf(data.end) : today;
  return selected !== null && selected >= 1 && selected <= end
    ? selected
    : today;
}

interface ClosedProps {
  data: Forecast;
  view: View;
  day: number;
  onView: (view: View) => void;
  onSelect: (day: number) => void;
}

/** Прошедший месяц: все дни по факту, карточка дня и итог месяца. */
function ClosedMonth({ data, view, day, onView, onSelect }: ClosedProps) {
  return (
    <>
      <Switches data={data} view={view} onView={onView} />
      <MonthGrid
        data={data}
        view={view}
        selected={day}
        onSelect={onSelect}
        untitled
      />
      <DayCard data={data} view={view} day={day} />
      <MonthSummary data={data} view={view} />
    </>
  );
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
  return (
    <>
      <Switches data={data} view={view} onView={onView} />
      {warnings(data, view).map((text) => (
        <p key={text} className="text-sm font-medium text-danger">
          {text}
        </p>
      ))}
      <MonthGrid
        data={data}
        view={view}
        selected={day}
        onSelect={onSelect}
        untitled
      />
      <DayCard
        data={data}
        view={view}
        day={day}
        onAdd={onAdd}
        onEdit={onEdit}
      />
      <LaterRecords data={data} onEdit={onEdit} />
      <MonthOutlook data={data} view={view} />
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
    </>
  );
}

interface Editing {
  record: CalendarRecord | null; // null — новая запись
  day: number;
}

interface ScreenProps {
  store: Store; // кэш экрана — один на все его месяцы
  api: Api;
  boot: Bootstrap | null; // справочник для окна записи; нет — записи только смотреть
  month: string | null; // «YYYY-MM» прошедшего месяца; null — текущий
  nav: ReactNode; // стрелки листания с названием месяца
  view: View;
  onView: (view: View) => void;
  onEarliest: (month: string) => void;
}

/** Один месяц календаря: текущий — прогноз и записи, прошедший — по факту. */
function MonthScreen({
  store,
  api,
  boot,
  month,
  nav,
  view: chosen,
  onView,
  onEarliest,
}: ScreenProps) {
  const load = useCallback(
    (fresh: boolean) =>
      month === null ? api.forecast(fresh) : api.forecast(fresh, month),
    [api, month],
  );
  const key = cacheKey(month);
  const { data, loading, error, reload } = useResource<Forecast>(
    store,
    key,
    load,
    { failure: "Не получилось загрузить прогноз" },
  );
  const [selected, setSelected] = useState<number | null>(null);
  const [editing, setEditing] = useState<Editing | null>(null);
  const view = data ? fitView(chosen, data) : chosen;
  const earliest = data?.earliest ?? null;

  useEffect(() => {
    if (earliest) onEarliest(earliest);
  }, [earliest, onEarliest]);

  // месяц перед открытым — заранее: листание назад без ожидания
  const shown = data?.status === "ok" ? data.today.slice(0, 7) : null;
  useEffect(() => {
    if (!shown || !earliest || shown <= earliest) return;
    const before = prevMonth(shown);
    void store
      .ensure(cacheKey(before), (fresh) => api.forecast(fresh, before))
      .catch(() => undefined);
  }, [api, store, shown, earliest]);

  // «Обновить» читает таблицу заново, а не копию листов на сервере
  const refresh = useCallback(() => {
    reload(true);
  }, [reload]);
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
  // просили прошедший месяц, а пришёл другой (сервер ещё без листания): чужие дни
  // под его названием не показываем
  const foreign =
    month !== null && data?.status === "ok" && data.today.slice(0, 7) !== month;
  const closed = data?.status === "ok" && !foreign && data.closed === true;
  const open = data?.status === "ok" && !foreign && !closed;

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">Календарь</h1>
        <button
          type="button"
          className="text-link disabled:opacity-50"
          disabled={loading}
          onClick={refresh}
        >
          {loading ? "Обновляю…" : "Обновить"}
        </button>
      </div>
      {/* стрелки — всегда на одном месте, и пока месяц грузится: листать можно дальше */}
      {nav}
      {loading && !data && <Skeleton />}
      {error && (
        <div className="flex flex-col items-start gap-2" role="alert">
          <p>{error}</p>
          <button type="button" className="text-link" onClick={refresh}>
            Повторить
          </button>
        </div>
      )}
      {foreign && (
        <p role="alert">Этот месяц не открылся — нажмите «Обновить»</p>
      )}
      {data?.status === "no_month_tab" && (
        <p>Нет вкладки месяца — прогноз появится, когда она будет</p>
      )}
      {data && closed && (
        <ClosedMonth
          data={data}
          view={view}
          day={day}
          onView={onView}
          onSelect={setSelected}
        />
      )}
      {data && open && (
        <Month
          data={data}
          view={view}
          day={day}
          onView={onView}
          onSelect={setSelected}
          onAdd={boot ? add : undefined}
          onEdit={boot ? edit : undefined}
        />
      )}
      {editing && boot && data && open && (
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

interface Props {
  api: Api;
  boot: Bootstrap | null; // справочник для окна записи; нет — записи только смотреть
}

/**
 * «Календарь»: текущий месяц и листание назад по прошедшим. Вид («Всего», валюта)
 * — общий для всех месяцев; выбранный день у каждого месяца свой.
 */
export default function ForecastScreen({ api, boot }: Props) {
  const store = useStore();
  const now = useCached(store, "forecast").data as Forecast | null;
  // текущий месяц — по боту (он живёт по Ташкенту); до его ответа — по телефону
  const current = (now?.today ?? boot?.today ?? deviceDay()).slice(0, 7);
  const [asked, setAsked] = useState<string | null>(null);
  const [earliest, setEarliest] = useState<string | null>(null);
  const [chosen, setChosen] = useState<View>(storedView);
  const past = asked !== null && asked < current ? asked : null;
  const changeView = useCallback((next: View) => {
    setChosen(next);
    storeView(next);
  }, []);

  return (
    <MonthScreen
      // у каждого месяца свои выбранный день и запрос
      key={past ?? "current"}
      store={store}
      api={api}
      boot={boot}
      month={past}
      nav={
        <MonthNav
          month={past ?? current}
          current={current}
          earliest={earliest}
          onMonth={setAsked}
        />
      }
      view={chosen}
      onView={changeView}
      onEarliest={setEarliest}
    />
  );
}
