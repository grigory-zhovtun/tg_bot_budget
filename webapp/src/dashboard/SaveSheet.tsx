import { useRef, useState } from "react";
import { ApiError, type Api, type Bootstrap, type SavingsScreen } from "../api";
import { amountValue, apiAmount } from "../entry/amount";
import { FieldError, cardCurrencies, segment } from "../fields";
import { short } from "../format";
import AmountPad from "../plan/AmountPad";
import { amountText } from "../plan/draft";
import Sheet from "../plan/Sheet";
import { piggyMoney } from "./piggyText";

interface Props {
  api: Api;
  boot: Bootstrap | null;
  data: SavingsScreen;
  onSaved: (fresh: SavingsScreen) => void;
  onClose: () => void;
}

interface Part {
  name: string;
  text: string;
}

/** Как сервер разложит сумму: копилкам со сроком по порядку экрана их «осталось»,
 * потом долгим целям; остальное — подушке (в журнал не пишется). */
function layout(
  data: SavingsScreen,
  uzs: number,
): { parts: Part[]; rest: number } {
  const ordered = [
    ...data.piggies.filter((piggy) => piggy.deadline !== null),
    ...data.piggies.filter((piggy) => piggy.deadline === null),
  ];
  let rest = uzs;
  const parts: Part[] = [];
  for (const piggy of ordered) {
    const rate = data.rates[piggy.currency];
    if (piggy.status === "broken" || piggy.left_this_month <= 0 || !rate)
      continue;
    if (rest <= 0) break;
    const take = Math.min(piggy.left_this_month * rate, rest);
    rest -= take;
    parts.push({
      name: piggy.name,
      text: piggyMoney(
        piggy.currency === "UZS"
          ? Math.round(take)
          : Math.round((take / rate) * 100) / 100,
        piggy.currency,
      ),
    });
  }
  return { parts, rest };
}

/** Окно «Отложил…»: одна сумма на все копилки, раскладка — до записи. */
export default function SaveSheet({
  api,
  boot,
  data,
  onSaved,
  onClose,
}: Props) {
  const currencies = boot ? cardCurrencies(boot, undefined) : ["UZS"];
  const [currency, setCurrency] = useState("UZS");
  const left = data.piggies
    .filter((piggy) => piggy.status !== "broken")
    .reduce(
      (sum, piggy) =>
        sum + piggy.left_this_month * (data.rates[piggy.currency] ?? 0),
      0,
    );
  const [text, setText] = useState(amountText(left, false));
  const [entryId] = useState(() => crypto.randomUUID());
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fields, setFields] = useState<Record<string, string>>({});
  const inFlight = useRef(false);
  const uzs = amountValue(text) * (data.rates[currency] ?? 0);
  const { parts, rest } = layout(data, uzs);

  function save() {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setError(null);
    setFields({});
    api
      .savePiggies({ entry_id: entryId, amount: apiAmount(text), currency })
      .then(onSaved, (reason: unknown) => {
        inFlight.current = false;
        setBusy(false);
        if (reason instanceof ApiError) {
          setFields(reason.fields);
          setError(
            reason.fields.amount || reason.fields.currency
              ? null
              : reason.message,
          );
        } else {
          setError("Не получилось сохранить — попробуйте ещё раз");
        }
      });
  }

  return (
    <Sheet title="Отложил" onClose={onClose}>
      {currencies.length > 1 && (
        <div
          role="radiogroup"
          aria-label="Валюта"
          className="grid auto-cols-fr grid-flow-col gap-1 self-end rounded-xl bg-section p-1"
        >
          {currencies.map((code) => (
            <button
              key={code}
              type="button"
              role="radio"
              aria-checked={currency === code}
              className={segment(currency === code)}
              onClick={() => {
                setCurrency(code);
                setText("");
              }}
            >
              {code}
            </button>
          ))}
        </div>
      )}
      <AmountPad text={text} currency={currency} onChange={setText} />
      <FieldError text={fields.amount ?? fields.currency} />
      <ul className="flex flex-col gap-1 text-sm" aria-label="Куда ляжет">
        {parts.map((part) => (
          <li key={part.name}>{`«${part.name}» — ${part.text}`}</li>
        ))}
        {rest > 0.5 && (
          <li>{`Подушке — ${short(rest)}, в журнал не пишется`}</li>
        )}
      </ul>
      {error && (
        <p className="text-sm text-danger" role="alert">
          {error}
        </p>
      )}
      <button
        type="button"
        disabled={amountValue(text) <= 0 || busy}
        className="h-12 rounded-xl bg-button font-semibold text-button-text disabled:opacity-50"
        onClick={save}
      >
        Записать
      </button>
    </Sheet>
  );
}
