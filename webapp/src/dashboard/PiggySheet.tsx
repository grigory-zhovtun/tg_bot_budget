import { useRef, useState } from "react";
import {
  ApiError,
  type Api,
  type Bootstrap,
  type Piggy,
  type PiggyInput,
  type PiggyPlanMonth,
  type SavingsScreen,
} from "../api";
import { amountValue, apiAmount } from "../entry/amount";
import { FIELD, FieldError, cardCurrencies, chip, segment } from "../fields";
import { shiftDay } from "../format";
import { CLOSED_GROUPS, groupTitle } from "../forecast/records";
import AmountPad from "../plan/AmountPad";
import { amountText } from "../plan/draft";
import Sheet from "../plan/Sheet";
import {
  MONTH_NAMES,
  MONTHS_FROM,
  MONTHS_IN,
  monthIndex,
  piggyMoney,
} from "./piggyText";

const NEW_DEADLINE_DAYS = 60; // срок новой копилки по умолчанию — через два месяца
const LATEST_DAYS = 5 * 366; // как на сервере: срок не дальше 5 лет
// поля с ошибкой под собой; остальные ошибки — над кнопкой
const FIELDS = ["name", "target", "currency", "deadline", "repeat", "group",
  "subcategory", "amount"]; // prettier-ignore

interface Props {
  api: Api;
  boot: Bootstrap;
  data: SavingsScreen;
  piggy: Piggy | null; // null — новая копилка
  onSaved: (fresh: SavingsScreen) => void;
  onClose: () => void;
}

type Move = "saved" | "spent";

/** «05.10» из «2026-10-05». */
function dayMonth(isoDay: string): string {
  return `${isoDay.slice(8, 10)}.${isoDay.slice(5, 7)}`;
}

/** Подсказка плана: взнос месяца и следующие («в ноябре — …», «с декабря по май — по …
 * в месяц»), или «к сроку не успеть». */
function planHint(piggy: Piggy, data: SavingsScreen): string | null {
  const money = (value: number) => piggyMoney(value, piggy.currency);
  if (piggy.status === "late" && piggy.deadline) {
    return `К ${dayMonth(piggy.deadline)} не успеть: не хватает ${money(piggy.short)}`;
  }
  // взносы плана — в сумах; в валюте копилки — вверх до целых, как у сервера
  const part = (month: PiggyPlanMonth) =>
    month.items.find((item) => item.id === piggy.id)?.amount ?? 0;
  const later = data.plan.slice(1);
  const first = later.findIndex((month) => part(month) > 0);
  const head =
    piggy.this_month > 0
      ? `В этом месяце ${money(piggy.this_month)}`
      : "В этом месяце ничего";
  const start = later[first];
  if (!start) return piggy.this_month > 0 ? head : null;
  const each = part(start);
  const run = later.slice(first);
  const stop = run.findIndex((month) => part(month) !== each);
  const end = run[(stop < 0 ? run.length : stop) - 1] ?? start;
  const rate = data.rates[piggy.currency] ?? 1;
  const amount = money(
    piggy.currency === "UZS" ? each : Math.ceil(each / rate - 1e-6),
  );
  const from = monthIndex(start.month);
  if (end === start) return `${head}, в ${MONTHS_IN[from]} — ${amount}`;
  const to = (MONTH_NAMES[monthIndex(end.month)] ?? end.month).toLowerCase();
  return `${head}, с ${MONTHS_FROM[from]} по ${to} — по ${amount} в месяц`;
}

/** Окно копилки: название, цель, срок, повтор, статья; у существующей — «Отложил…»,
 * «Потратил…», «Списать», «Закрыть» и история. */
export default function PiggySheet({
  api,
  boot,
  data,
  piggy,
  onSaved,
  onClose,
}: Props) {
  const today = data.today;
  const currencies = cardCurrencies(boot, piggy?.currency);
  const groups = boot.groups.filter(
    (item) => !CLOSED_GROUPS.includes(item.name),
  );
  const [name, setName] = useState(piggy?.name ?? "");
  const [currency, setCurrency] = useState(
    piggy?.currency || currencies[0] || "UZS",
  );
  const [text, setText] = useState(
    piggy?.target ? amountText(piggy.target, piggy.currency !== "UZS") : "",
  );
  const [noTarget, setNoTarget] = useState(
    piggy !== null && piggy.target === null,
  );
  const fallback = piggy?.deadline ?? shiftDay(today, NEW_DEADLINE_DAYS);
  const [deadline, setDeadline] = useState<string | null>(
    piggy ? piggy.deadline : fallback,
  );
  const [repeat, setRepeat] = useState<"once" | "yearly">(
    piggy?.repeat ?? "once",
  );
  const [group, setGroup] = useState(piggy?.group ?? "");
  const [subcategory, setSubcategory] = useState(piggy?.subcategory ?? "");
  const [move, setMove] = useState<Move | null>(null);
  const [moveText, setMoveText] = useState("");
  const [entryId] = useState(() => crypto.randomUUID());
  const [busy, setBusy] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fields, setFields] = useState<Record<string, string>>({});
  const inFlight = useRef(false); // двойной тап — один запрос

  const chosen = groups.find((item) => item.name === group);
  const latest = shiftDay(today, LATEST_DAYS);
  const deadlineOk =
    deadline === null ||
    deadline === piggy?.deadline ||
    (deadline >= today && deadline <= latest);
  const valid =
    name.trim() !== "" &&
    (noTarget || amountValue(text) > 0) &&
    deadlineOk &&
    (group === "" || subcategory !== "");

  function fail(reason: unknown) {
    inFlight.current = false;
    setBusy(false);
    setConfirming(false);
    if (reason instanceof ApiError) {
      setFields(reason.fields);
      const underFields = FIELDS.some((field) => reason.fields[field]);
      setError(underFields ? null : reason.message);
    } else {
      setError("Не получилось сохранить — попробуйте ещё раз");
    }
  }

  function send(call: () => Promise<SavingsScreen>) {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setError(null);
    setFields({});
    call().then(onSaved, fail);
  }

  function save() {
    const input: PiggyInput = {
      name: name.trim(),
      target: noTarget ? null : apiAmount(text),
      currency,
      deadline,
      repeat: deadline ? repeat : "once",
      group: group || null,
      subcategory: subcategory || null,
    };
    send(() =>
      piggy
        ? api.updatePiggy(piggy.id, input)
        : api.addPiggy({ ...input, entry_id: entryId }),
    );
  }

  function moveMoney(kind: Move | "write_off", amount: string) {
    if (!piggy) return;
    send(() =>
      api.movePiggy(piggy.id, { entry_id: crypto.randomUUID(), kind, amount }),
    );
  }

  function close() {
    if (!piggy) return;
    if (!confirming) {
      setConfirming(true);
      return;
    }
    send(() => api.closePiggy(piggy.id));
  }

  if (piggy && move) {
    return (
      <Sheet
        title={move === "saved" ? "Отложил" : "Потратил"}
        onClose={onClose}
      >
        <p className="text-sm">{piggy.name}</p>
        <AmountPad
          text={moveText}
          currency={piggy.currency}
          onChange={setMoveText}
        />
        <FieldError text={fields.amount} />
        {error && (
          <p className="text-sm text-danger" role="alert">
            {error}
          </p>
        )}
        <button
          type="button"
          disabled={amountValue(moveText) <= 0 || busy}
          className="h-12 rounded-xl bg-button font-semibold text-button-text disabled:opacity-50"
          onClick={() => moveMoney(move, apiAmount(moveText))}
        >
          Записать
        </button>
        <button
          type="button"
          className="text-link"
          onClick={() => setMove(null)}
        >
          Назад
        </button>
      </Sheet>
    );
  }

  const hint = piggy ? planHint(piggy, data) : null;
  const leftover =
    piggy && (piggy.status === "leftover" || piggy.leftover > 0)
      ? piggy.leftover
      : 0;
  return (
    <Sheet title={piggy ? piggy.name : "Новая копилка"} onClose={onClose}>
      <label className="flex flex-col gap-1 text-sm text-hint">
        Название
        <input
          className={FIELD}
          value={name}
          maxLength={40}
          placeholder="Новый год"
          onChange={(change) => setName(change.target.value)}
        />
      </label>
      <FieldError text={fields.name} />
      <div className="flex items-center justify-between gap-2">
        <span className="text-sm text-hint">Цель</span>
        {currencies.length > 1 && (
          <div
            role="radiogroup"
            aria-label="Валюта"
            className="grid auto-cols-fr grid-flow-col gap-1 rounded-xl bg-section p-1"
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
      </div>
      <FieldError text={fields.currency} />
      {deadline === null && (
        <button
          type="button"
          aria-pressed={noTarget}
          className={`self-start ${chip(noTarget)}`}
          onClick={() => setNoTarget(!noTarget)}
        >
          Без цели
        </button>
      )}
      {!noTarget && (
        <AmountPad text={text} currency={currency} onChange={setText} />
      )}
      <FieldError text={fields.target} />
      <span className="text-sm text-hint">Срок</span>
      {deadline !== null && (
        <label className="flex flex-col gap-1 text-sm text-hint">
          <span className="sr-only">Срок</span>
          <input
            className={FIELD}
            type="date"
            aria-label="Срок"
            min={today}
            max={latest}
            value={deadline}
            onChange={(change) => {
              if (change.target.value) setDeadline(change.target.value);
            }}
          />
        </label>
      )}
      <button
        type="button"
        aria-pressed={deadline === null}
        className={`self-start ${chip(deadline === null)}`}
        onClick={() => {
          if (deadline === null) {
            setDeadline(fallback);
            setNoTarget(false);
          } else {
            setDeadline(null);
          }
        }}
      >
        Без срока
      </button>
      <FieldError text={fields.deadline} />
      {deadline !== null && (
        <div
          role="radiogroup"
          aria-label="Повтор"
          className="grid grid-cols-2 gap-1 rounded-xl bg-section p-1"
        >
          {(["once", "yearly"] as const).map((item) => (
            <button
              key={item}
              type="button"
              role="radio"
              aria-checked={repeat === item}
              className={segment(repeat === item)}
              onClick={() => setRepeat(item)}
            >
              {item === "once" ? "Один раз" : "Каждый год"}
            </button>
          ))}
        </div>
      )}
      <FieldError text={fields.repeat} />
      <span className="text-sm text-hint">Статья трат</span>
      <div
        className="flex flex-wrap gap-1.5"
        role="group"
        aria-label="Статья трат"
      >
        <button
          type="button"
          aria-pressed={group === ""}
          className={chip(group === "")}
          onClick={() => {
            setGroup("");
            setSubcategory("");
          }}
        >
          Без статьи
        </button>
        {groups.map((item) => (
          <button
            key={item.name}
            type="button"
            aria-pressed={group === item.name}
            className={chip(group === item.name)}
            onClick={() => {
              setGroup(item.name);
              setSubcategory("");
            }}
          >
            {`${item.emoji} ${item.title}`}
          </button>
        ))}
      </div>
      <FieldError text={fields.group} />
      {chosen && (
        <div
          className="flex flex-wrap gap-1.5"
          role="group"
          aria-label="Подкатегория"
        >
          {chosen.subcategories.map((item) => (
            <button
              key={item.name}
              type="button"
              aria-pressed={subcategory === item.name}
              className={chip(subcategory === item.name)}
              onClick={() => setSubcategory(item.name)}
            >
              {item.name}
            </button>
          ))}
        </div>
      )}
      <FieldError text={fields.subcategory} />
      {hint && <p className="text-sm font-semibold">{hint}</p>}
      {error && (
        <p className="text-sm text-danger" role="alert">
          {error}
        </p>
      )}
      <button
        type="button"
        disabled={!valid || busy}
        className="h-12 rounded-xl bg-button font-semibold text-button-text disabled:opacity-50"
        onClick={save}
      >
        {piggy ? "Сохранить" : "Создать"}
      </button>
      {piggy && (
        <>
          <div className="grid grid-cols-2 gap-2">
            <button
              type="button"
              className="h-11 rounded-xl bg-section font-semibold"
              onClick={() => {
                setMove("saved");
                setMoveText(
                  amountText(piggy.left_this_month, piggy.currency !== "UZS"),
                );
              }}
            >
              Отложил…
            </button>
            <button
              type="button"
              className="h-11 rounded-xl bg-section font-semibold"
              onClick={() => {
                setMove("spent");
                setMoveText("");
              }}
            >
              Потратил…
            </button>
          </div>
          {leftover > 0 && (
            <button
              type="button"
              disabled={busy}
              className="h-11 rounded-xl bg-section font-semibold disabled:opacity-50"
              onClick={() =>
                moveMoney(
                  "write_off",
                  apiAmount(amountText(leftover, piggy.currency !== "UZS")),
                )
              }
            >
              Списать как потраченное
            </button>
          )}
          <button
            type="button"
            disabled={busy}
            className={
              confirming
                ? "h-11 rounded-xl bg-minus font-semibold text-white disabled:opacity-50"
                : "h-11 rounded-xl font-semibold text-danger disabled:opacity-50"
            }
            onClick={close}
          >
            {confirming
              ? "Закрыть — остаток вернётся в подушку"
              : "Закрыть копилку"}
          </button>
          {(piggy.history.length > 0 || piggy.spends.length > 0) && (
            <section aria-label="История" className="flex flex-col gap-1">
              <h4 className="font-semibold">История</h4>
              <ul className="flex flex-col gap-1 text-sm">
                {piggy.history.map((item, index) => (
                  <li key={`move-${index}`}>
                    {`${dayMonth(item.day)} ${item.kind} ${piggyMoney(Math.abs(item.amount), piggy.currency)}`}
                  </li>
                ))}
                {piggy.spends.map((item) => (
                  <li
                    key={item.key}
                    className="flex items-center justify-between gap-2"
                  >
                    <span>
                      {`${dayMonth(item.day)} ${groupTitle(item.group)} / ${item.subcategory} ${piggyMoney(item.amount, piggy.currency)}`}
                    </span>
                    <button
                      type="button"
                      disabled={busy}
                      className="shrink-0 text-link disabled:opacity-50"
                      onClick={() =>
                        send(() => api.excludeSpend(piggy.id, item.key))
                      }
                    >
                      Не из копилки
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </>
      )}
    </Sheet>
  );
}
