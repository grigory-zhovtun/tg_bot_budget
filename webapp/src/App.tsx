import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, type Api, type Bootstrap } from "./api";
import DashboardScreen from "./dashboard/DashboardScreen";
import EntryScreen from "./entry/EntryScreen";
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

type Tab = "entry" | "dashboard" | "plan";

const TABS: { name: Tab; title: string }[] = [
  { name: "entry", title: "Ввод" },
  { name: "dashboard", title: "Сводка" },
  { name: "plan", title: "План" },
];

function errorText(reason: unknown): string {
  return reason instanceof ApiError
    ? reason.message
    : "Не получилось загрузить — попробуйте ещё раз";
}

export default function App({ api }: { api: Api }) {
  const [boot, setBoot] = useState<Bootstrap | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
  const [tab, setTab] = useState<Tab>("entry");
  const [drafts, setDrafts] = useState<Record<string, Draft> | null>(null);
  const [restored, setRestored] = useState<string[]>([]);
  const draftsLoaded = useRef(false);
  const reload = useCallback(() => setVersion((current) => current + 1), []);

  useEffect(() => {
    let alive = true;
    api.bootstrap().then(
      (data) => {
        if (!alive) return;
        setBoot(data);
        setError(null);
        if (!draftsLoaded.current) {
          // черновики плана — на сессию: перечитанный справочник их не сбрасывает
          draftsLoaded.current = true;
          const current = data.today.slice(0, 7);
          const loaded: Record<string, Draft> = {};
          const found: string[] = [];
          for (const month of [current, nextMonth(current)]) {
            const stored = loadDraft(data.user.id, month);
            loaded[month] = stored ?? emptyDraft(month);
            if (stored) found.push(month);
          }
          setDrafts(loaded);
          setRestored(found);
        }
      },
      (reason: unknown) => {
        if (alive) setError(errorText(reason));
      },
    );
    return () => {
      alive = false;
    };
  }, [api, version]);

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

  return (
    <main className="mx-auto flex max-w-md flex-col gap-4 p-4">
      <nav
        className="grid grid-cols-3 gap-1 rounded-xl bg-section p-1"
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
    </main>
  );
}
