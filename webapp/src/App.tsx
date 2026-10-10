import { useCallback, useEffect, useState } from "react";
import { ApiError, type Api, type Bootstrap } from "./api";
import EntryScreen from "./entry/EntryScreen";

function errorText(reason: unknown): string {
  return reason instanceof ApiError
    ? reason.message
    : "Не получилось загрузить — попробуйте ещё раз";
}

export default function App({ api }: { api: Api }) {
  const [boot, setBoot] = useState<Bootstrap | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
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
      {!boot && !error && <p className="text-hint">Загрузка…</p>}
      {!boot && error && (
        <div className="flex flex-col items-start gap-2" role="alert">
          <p>{error}</p>
          <button type="button" className="text-link" onClick={reload}>
            Повторить
          </button>
        </div>
      )}
      {boot && <EntryScreen api={api} boot={boot} onCatalogChanged={reload} />}
    </main>
  );
}
