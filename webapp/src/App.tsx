import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, launchUser, type Api, type Bootstrap } from "./api";
import DashboardScreen from "./dashboard/DashboardScreen";
import EntryScreen from "./entry/EntryScreen";
import { deviceDay } from "./format";
import ForecastScreen from "./forecast/ForecastScreen";
import HomeScreenButton from "./HomeScreenButton";
import {
  changeCount,
  emptyDraft,
  loadDraft,
  storeDraft,
  type Draft,
} from "./plan/draft";
import { nextMonth } from "./plan/months";
import PlanTab from "./plan/PlanTab";
import { phoneStorage, Store, StoreProvider } from "./store";
import { telegram } from "./telegram";

type Tab = "entry" | "dashboard" | "plan" | "forecast";

const TABS: { name: Tab; title: string }[] = [
  { name: "entry", title: "Ввод" },
  { name: "dashboard", title: "Сводка" },
  { name: "plan", title: "План" },
  { name: "forecast", title: "Прогноз" },
];

function errorText(reason: unknown): string {
  return reason instanceof ApiError
    ? reason.message
    : "Не получилось загрузить — попробуйте ещё раз";
}

interface Drafts {
  drafts: Record<string, Draft>;
  restored: string[]; // месяцы с черновиком с прошлого раза
}

/** Черновики плана текущего и следующего месяца из памяти телефона. */
function openDrafts(boot: Bootstrap): Drafts {
  const current = boot.today.slice(0, 7);
  const drafts: Record<string, Draft> = {};
  const restored: string[] = [];
  for (const month of [current, nextMonth(current)]) {
    const stored = loadDraft(boot.user.id, month);
    drafts[month] = stored ?? emptyDraft(month);
    if (stored) restored.push(month);
  }
  return { drafts, restored };
}

export default function App({ api }: { api: Api }) {
  const [store] = useState(
    () =>
      new Store({
        storage: phoneStorage(),
        build: import.meta.url, // имя файла сборки меняется с каждым выпуском
        today: deviceDay(),
        user:
          telegram()?.initDataUnsafe.user?.id ??
          launchUser(window.location.search),
      }),
  );
  // справочник с прошлого раза (сегодняшний): «Ввод» сразу, свежий — фоном
  const [initial] = useState(() => {
    const saved = store.get("bootstrap").data as Bootstrap | null;
    return { boot: saved, drafts: saved ? openDrafts(saved) : null };
  });
  const [boot, setBoot] = useState<Bootstrap | null>(initial.boot);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
  const [tab, setTab] = useState<Tab>("entry");
  const [drafts, setDrafts] = useState<Record<string, Draft> | null>(
    initial.drafts?.drafts ?? null,
  );
  const [restored, setRestored] = useState<string[]>(
    initial.drafts?.restored ?? [],
  );
  const draftsLoaded = useRef(initial.drafts !== null);
  const reload = useCallback(() => setVersion((current) => current + 1), []);

  useEffect(() => {
    let alive = true;
    api.bootstrap().then(
      (data) => {
        if (!alive) return;
        store.put("bootstrap", data);
        setBoot(data);
        setError(null);
        if (!draftsLoaded.current) {
          // черновики плана — на сессию: перечитанный справочник их не сбрасывает
          draftsLoaded.current = true;
          const opened = openDrafts(data);
          setDrafts(opened.drafts);
          setRestored(opened.restored);
        }
      },
      (reason: unknown) => {
        if (!alive) return;
        setError(errorText(reason));
        // бот не узнал пользователя: копия с прошлого раза не его
        if (
          reason instanceof ApiError &&
          (reason.status === 401 || reason.status === 403)
        ) {
          store.forget();
          setBoot(null);
        }
      },
    );
    return () => {
      alive = false;
    };
  }, [api, store, version]);

  const userId = boot?.user.id;
  const updateDraft = useCallback(
    (next: Draft) => {
      setDrafts((current) => ({ ...(current ?? {}), [next.month]: next }));
      if (userId !== undefined) storeDraft(userId, next);
    },
    [userId],
  );
  const restoredSeen = useCallback(
    (month: string) =>
      setRestored((current) => current.filter((item) => item !== month)),
    [],
  );
  const unsaved =
    drafts !== null &&
    Object.values(drafts).some((draft) => changeCount(draft) > 0);
  const currentMonth = boot ? boot.today.slice(0, 7) : "";
  const months = currentMonth ? [currentMonth, nextMonth(currentMonth)] : [];
  const needsBoot = tab === "entry" || tab === "plan";

  // Фоном: сначала сводка — её ждут плитки «Ввода», потом прогноз и план. Сводка
  // первой за запуск читает таблицу заново, остальные — из её копии на сервере.
  useEffect(() => {
    let alive = true;
    const quiet = () => undefined;
    void store
      .ensure("dashboard", api.dashboard)
      .catch(quiet)
      .then(() => {
        if (!alive) return;
        void store.ensure("forecast", api.forecast).catch(quiet);
        if (currentMonth) {
          void store
            .ensure(`plan:${currentMonth}`, (fresh) =>
              api.plan(currentMonth, fresh),
            )
            .catch(quiet);
        }
      });
    return () => {
      alive = false;
    };
  }, [api, store, currentMonth]);

  return (
    <StoreProvider value={store}>
      <main className="mx-auto flex max-w-md flex-col gap-4 p-4">
        <nav
          className="grid grid-cols-4 gap-1 rounded-xl bg-section p-1"
          role="tablist"
          aria-label="Разделы"
        >
          {TABS.map(({ name, title }) => (
            <button
              key={name}
              type="button"
              role="tab"
              aria-selected={tab === name}
              className={`rounded-lg py-1.5 text-sm font-medium ${
                tab === name ? "bg-bg shadow-sm" : "text-hint"
              }`}
              onClick={() => setTab(name)}
            >
              {title}
              {name === "plan" && unsaved && (
                <span
                  role="img"
                  aria-label="есть несохранённые правки"
                  className="ml-1 inline-block size-1.5 rounded-full bg-link align-middle"
                />
              )}
            </button>
          ))}
        </nav>
        {needsBoot && !boot && !error && <p className="text-hint">Загрузка…</p>}
        {needsBoot && !boot && error && (
          <div className="flex flex-col items-start gap-2" role="alert">
            <p>{error}</p>
            <button type="button" className="text-link" onClick={reload}>
              Повторить
            </button>
          </div>
        )}
        {tab === "entry" && boot && (
          <EntryScreen api={api} boot={boot} onCatalogChanged={reload} />
        )}
        {tab === "dashboard" && <DashboardScreen api={api} />}
        {tab === "dashboard" && boot && (
          <HomeScreenButton botUsername={boot.bot_username} />
        )}
        {tab === "plan" && boot && drafts && (
          <PlanTab
            api={api}
            months={months}
            drafts={drafts}
            restored={restored}
            onDraft={updateDraft}
            onRestoredSeen={restoredSeen}
          />
        )}
        {tab === "forecast" && <ForecastScreen api={api} boot={boot} />}
      </main>
    </StoreProvider>
  );
}
