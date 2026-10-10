import { useCallback, useEffect, useState } from "react";
import { ApiError, type Api, type Bootstrap } from "./api";
import DashboardScreen from "./dashboard/DashboardScreen";
import EntryScreen from "./entry/EntryScreen";
import HomeScreenButton from "./HomeScreenButton";

type Tab = "entry" | "dashboard";

const TABS: { name: Tab; title: string }[] = [
  { name: "entry", title: "Ввод" },
  { name: "dashboard", title: "Сводка" },
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
  const reload = useCallback(() => setVersion((current) => current + 1), []);

  useEffect(() => {
    let alive = true;
    api.bootstrap().then(
      (data) => {
        if (!alive) return;
        setBoot(data);
        setError(null);
      },
      (reason: unknown) => {
        if (alive) setError(errorText(reason));
      },
    );
    return () => {
      alive = false;
    };
  }, [api, version]);

  return (
    <main className="mx-auto flex max-w-md flex-col gap-4 p-4">
      <nav
        className="grid grid-cols-2 gap-1 rounded-xl bg-section p-1"
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
          </button>
        ))}
      </nav>
      {tab === "entry" && !boot && !error && (
        <p className="text-hint">Загрузка…</p>
      )}
      {tab === "entry" && !boot && error && (
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
    </main>
  );
}
