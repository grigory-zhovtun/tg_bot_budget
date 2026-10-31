import { useState } from "react";
import { ApiError, type Api, type Bootstrap, type SavingsScreen } from "../api";
import { CLOSED_GROUPS } from "../forecast/records";
import Sheet from "../plan/Sheet";
import { Line } from "./blocks";
import { ErrorLine, PRIMARY } from "./controls";

interface Props {
  api: Api;
  boot: Bootstrap;
  data: SavingsScreen;
  onSaved: (fresh: SavingsScreen) => void;
  onClose: () => void;
}

/** Окно «Месяц жизни»: отметки групп трат справочника — какие траты подушка
 * должна закрыть; сохраняются в порядке справочника. */
export default function LifeSheet({
  api,
  boot,
  data,
  onSaved,
  onClose,
}: Props) {
  const groups = boot.groups
    .map((item) => item.name)
    .filter((name) => !CLOSED_GROUPS.includes(name));
  const [checked, setChecked] = useState<Set<string>>(
    () => new Set(data.cushion?.life_groups ?? []),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const chosen = groups.filter((name) => checked.has(name));

  function flip(name: string) {
    setChecked((current) => {
      const next = new Set(current);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }

  function save() {
    if (busy || chosen.length === 0) return;
    setBusy(true);
    setError(null);
    api.setLife(chosen).then(onSaved, (reason: unknown) => {
      setBusy(false);
      setError(
        reason instanceof ApiError
          ? (reason.fields.groups ?? reason.message)
          : "Не получилось сохранить — попробуйте ещё раз",
      );
    });
  }

  return (
    <Sheet title="Месяц жизни" onClose={onClose}>
      <ul aria-label="Обязательные группы" className="flex flex-col gap-2">
        {groups.map((name) => {
          const on = checked.has(name);
          return (
            <li key={name}>
              <button
                type="button"
                role="checkbox"
                aria-checked={on}
                aria-label={name}
                className={`w-full rounded-xl px-4 py-3 text-left text-line font-bold break-words ${
                  on ? "bg-button text-button-text" : "bg-section text-text"
                }`}
                onClick={() => flip(name)}
              >
                {on ? `✓ ${name}` : name}
              </button>
            </li>
          );
        })}
      </ul>
      {chosen.length === 0 && <Line>отметьте хотя бы одну группу</Line>}
      <ErrorLine text={error} />
      <button
        type="button"
        disabled={busy || chosen.length === 0}
        className={PRIMARY}
        onClick={save}
      >
        Сохранить
      </button>
    </Sheet>
  );
}
