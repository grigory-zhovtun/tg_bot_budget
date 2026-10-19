import { useRef, useState } from "react";
import {
  ApiError,
  type Api,
  type Bootstrap,
  type Piggy,
  type SavingsScreen,
} from "../api";
import { amountValue, apiAmount } from "../entry/amount";
import { FieldError, cardCurrencies, segment } from "../fields";
import { money, moneyText } from "../format";
import AmountPad from "../plan/AmountPad";
import { amountText } from "../plan/draft";
import Sheet from "../plan/Sheet";
import { Line, Table } from "./blocks";

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

/** Как сервер разложит сумму: копилкам порядка — их «осталось в этом месяце»,
 * копилкам «не хватит» — до «не хватит», долгим целям — их «осталось»; остальное
 * — подушке (в журнал не пишется). */
function layout(
  data: SavingsScreen,
  uzs: number,
): { parts: Part[]; rest: number } {
  const ordered = data.piggies
    .filter((piggy) => piggy.order !== null && piggy.status !== "broken")
    .sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
  let rest = uzs;
  const taken = new Map<Piggy, number>();
  function give(piggy: Piggy, cap: number) {
    const rate = data.rates[piggy.currency];
    if (!rate || cap <= 0 || rest <= 0) return;
    const take = Math.min(cap * rate, rest);
    taken.set(piggy, (taken.get(piggy) ?? 0) + take);
    rest -= take;
  }
  for (const piggy of ordered) give(piggy, piggy.left_this_month);
  for (const piggy of ordered) {
    if (piggy.status === "short") give(piggy, piggy.short);
  }
  for (const piggy of data.piggies) {
    if (piggy.deadline === null && piggy.status !== "broken")
      give(piggy, piggy.left_this_month);
  }
  const parts = [...taken].map(([piggy, take]) => {
    const rate = data.rates[piggy.currency] ?? 1;
    return {
      name: piggy.name,
      text: moneyText(
        piggy.currency === "UZS"
          ? Math.round(take)
          : Math.round((take / rate) * 100) / 100,
        piggy.currency,
      ),
    };
  });
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
  const left = data.month?.left ?? 0; // «отложить сейчас», сумы
  const [text, setText] = useState(amountText(left, false));
  const [entryId] = useState(() => crypto.randomUUID());
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fields, setFields] = useState<Record<string, string>>({});
  const inFlight = useRef(false);
  const uzs = amountValue(text) * (data.rates[currency] ?? 0);
  const { parts, rest } = layout(data, uzs);
  const cushion = rest > 0.5;
  const rows = parts.map((part) => [part.name, part.text]);
  if (cushion) rows.push(["подушка", money(rest)]);

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
      {rows.length > 0 && (
        <Table
          label="Куда ляжет"
          head={["копилка", "сумма"]}
          align="lr"
          rows={rows}
        />
      )}
      {cushion && <Line>подушка — в журнал не пишется</Line>}
      {error && (
        <p className="text-line font-bold text-danger" role="alert">
          {error}
        </p>
      )}
      <button
        type="button"
        disabled={amountValue(text) <= 0 || busy}
        className="h-12 rounded-xl bg-button text-lg font-extrabold text-button-text disabled:opacity-50"
        onClick={save}
      >
        Записать
      </button>
    </Sheet>
  );
}
