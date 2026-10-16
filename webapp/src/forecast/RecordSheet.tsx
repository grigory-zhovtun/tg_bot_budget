import { useRef, useState } from "react";
import {
  ApiError,
  type Api,
  type Bootstrap,
  type CalendarRecord,
  type Forecast,
  type RecordInput,
  type Repeat,
} from "../api";
import { amountValue, apiAmount } from "../entry/amount";
import { longDay, shiftDay } from "../format";
import AmountPad from "../plan/AmountPad";
import { amountText } from "../plan/draft";
import Sheet from "../plan/Sheet";
import {
  CLOSED_GROUPS,
  REPEAT_NAMES,
  WEEKDAY_NAMES,
  groupTitle,
  hintChips,
  isoWeekday,
  overBy,
  ruleDays,
} from "./records";
import { amount, isoDay } from "./view";

const MAX_AHEAD_DAYS = 366; // как на сервере: не дальше чем через год
const REPEATS: Repeat[] = ["once", "daily", "weekdays", "monthly"];
const ORDER = ["UZS", "USD", "RUB"];
// поля с ошибкой под собой; остальные ошибки — над кнопкой
const FIELDS = [
  "currency",
  "group",
  "subcategory",
  "amount",
  "name",
  "repeat",
  "weekdays",
  "date",
];
const FIELD = "rounded-xl bg-section px-3 py-2 text-base text-text";

interface Props {
  api: Api;
  boot: Bootstrap;
  data: Forecast;
  day: number; // выбранный день — дата новой записи
  record: CalendarRecord | null; // null — новая запись
  onSaved: (fresh: Forecast) => void;
  onClose: () => void;
}

/** Валюты карт: UZS, USD, RUB, остальные по алфавиту; плюс валюта правки. */
function cardCurrencies(boot: Bootstrap, extra: string | undefined): string[] {
  const codes = new Set(boot.sources.map((source) => source.currency));
  if (extra) codes.add(extra);
  if (codes.size === 0) codes.add("UZS");
  const rest = [...codes].filter((code) => !ORDER.includes(code)).sort();
  return [...ORDER.filter((code) => codes.has(code)), ...rest];
}

function chip(on: boolean): string {
  return `rounded-full px-3 py-1.5 text-sm ${
    on ? "bg-button text-button-text" : "bg-section"
  }`;
}

function segment(on: boolean): string {
  return `rounded-lg px-1 py-1.5 text-sm font-medium ${
    on ? "bg-bg shadow-sm" : "text-hint"
  }`;
}

function FieldError({ text }: { text: string | undefined }) {
  return text ? <p className="text-sm text-danger">{text}</p> : null;
}

/** Окно записи календаря: статья, сумма с подсказками, название, повтор, дата. */
export default function RecordSheet({
  api,
  boot,
  data,
  day,
  record,
  onSaved,
  onClose,
}: Props) {
  const today = data.today;
  const end = data.end ?? today;
  const currencies = cardCurrencies(boot, record?.currency);
  const groups = boot.groups.filter(
    (item) => !CLOSED_GROUPS.includes(item.name),
  );
  const firstDate = record?.date ?? isoDay(today, day);
  const [currency, setCurrency] = useState(
    record?.currency ?? currencies[0] ?? "UZS",
  );
  const [group, setGroup] = useState(record?.group ?? "");
  const [subcategory, setSubcategory] = useState(record?.subcategory ?? "");
  const [text, setText] = useState(
    record ? amountText(record.amount, record.currency !== "UZS") : "",
  );
  const [name, setName] = useState(record?.name ?? "");
  const [repeat, setRepeat] = useState<Repeat>(record?.repeat ?? "once");
  const [weekdays, setWeekdays] = useState<number[]>(
    record?.weekdays.length ? record.weekdays : [isoWeekday(firstDate)],
  );
  const [date, setDate] = useState(firstDate);
  const [entryId] = useState(() => crypto.randomUUID());
  const [busy, setBusy] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fields, setFields] = useState<Record<string, string>>({});
  const inFlight = useRef(false); // двойной тап — один запрос

  const chosen = groups.find((item) => item.name === group);
  const article = data.articles.find(
    (item) => item.subcategory === subcategory && item.currency === currency,
  );
  const value = amountValue(text);
  const days = ruleDays(repeat, weekdays, date, today, end);
  const before =
    record?.subcategory === subcategory && record.currency === currency
      ? record.booked
      : 0;
  const excess =
    subcategory && value > 0 ? overBy(article, before, days.length * value) : 0;
  const kept = record !== null && repeat !== "once" && date === record.date;
  const latest = shiftDay(today, MAX_AHEAD_DAYS);
  const dateOk = kept || (date >= today && date <= latest);
  const valid =
    group !== "" &&
    subcategory !== "" &&
    value > 0 &&
    dateOk &&
    (repeat !== "weekdays" || weekdays.length > 0);

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

  function send(call: () => Promise<Forecast>) {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setError(null);
    setFields({});
    call().then(onSaved, fail);
  }

  function save() {
    const input: RecordInput = {
      date,
      repeat,
      weekdays:
        repeat === "weekdays" ? [...weekdays].sort((a, b) => a - b) : [],
      group,
      subcategory,
      currency,
      amount: apiAmount(text),
      name: name.trim(),
    };
    send(() =>
      record
        ? api.updateRecord(record.id, input)
        : api.addRecord({ ...input, entry_id: entryId }),
    );
  }

  function remove() {
    if (!record) return;
    if (!confirming) {
      setConfirming(true);
      return;
    }
    send(() => api.deleteRecord(record.id));
  }

  function toggleDay(number: number) {
    setWeekdays((current) =>
      current.includes(number)
        ? current.filter((item) => item !== number)
        : [...current, number],
    );
  }

  return (
    <Sheet
      title={record ? "Запись" : `Запись на ${longDay(date)}`}
      onClose={onClose}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="text-sm text-hint">Статья</span>
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
      <div className="flex flex-wrap gap-1.5" role="group" aria-label="Группа">
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
      {subcategory && (
        <p className="text-sm text-hint">
          {article && article.plan > 0
            ? `В статье осталось ${amount(article.rest, currency)} из ${amount(article.plan, currency)}`
            : "Статьи нет в плане месяца"}
        </p>
      )}
      {subcategory && (
        <div className="flex flex-wrap gap-1.5">
          {hintChips(data.hints[`${subcategory}|${currency}`], repeat).map(
            (hint) => (
              <button
                key={hint.label}
                type="button"
                className={chip(false)}
                onClick={() =>
                  setText(amountText(hint.value, currency !== "UZS"))
                }
              >
                {hint.label}
              </button>
            ),
          )}
        </div>
      )}
      <AmountPad text={text} currency={currency} onChange={setText} />
      <FieldError text={fields.amount} />
      <label className="flex flex-col gap-1 text-sm text-hint">
        Название
        <input
          className={FIELD}
          value={name}
          maxLength={60}
          placeholder={subcategory || "Необязательно"}
          onChange={(change) => setName(change.target.value)}
        />
      </label>
      <FieldError text={fields.name} />
      <span className="text-sm text-hint">Повтор</span>
      <div
        role="radiogroup"
        aria-label="Повтор"
        className="grid grid-cols-4 gap-1 rounded-xl bg-section p-1"
      >
        {REPEATS.map((item) => (
          <button
            key={item}
            type="button"
            role="radio"
            aria-checked={repeat === item}
            className={segment(repeat === item)}
            onClick={() => setRepeat(item)}
          >
            {REPEAT_NAMES[item]}
          </button>
        ))}
      </div>
      <FieldError text={fields.repeat} />
      {repeat === "weekdays" && (
        <div
          className="grid grid-cols-7 gap-1"
          role="group"
          aria-label="Дни недели"
        >
          {WEEKDAY_NAMES.map((label, index) => (
            <button
              key={label}
              type="button"
              aria-pressed={weekdays.includes(index + 1)}
              className={chip(weekdays.includes(index + 1))}
              onClick={() => toggleDay(index + 1)}
            >
              {label}
            </button>
          ))}
        </div>
      )}
      <FieldError text={fields.weekdays} />
      <label className="flex flex-col gap-1 text-sm text-hint">
        {repeat === "once" ? "Дата" : "Начало"}
        <input
          className={FIELD}
          type="date"
          min={kept ? record?.date : today}
          max={latest}
          value={date}
          onChange={(change) => {
            if (change.target.value) setDate(change.target.value);
          }}
        />
      </label>
      <FieldError text={fields.date} />
      {excess > 0 && (
        <p className="rounded-xl bg-minus px-3 py-2 text-sm font-semibold text-white">
          {`⚠️ План статьи «${groupTitle(group)} / ${subcategory}» будет превышен на ${amount(excess, currency)}`}
        </p>
      )}
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
        {record ? "Сохранить" : "Запланировать"}
      </button>
      {record && (
        <button
          type="button"
          disabled={busy}
          className={
            confirming
              ? "h-12 rounded-xl bg-danger font-semibold text-white disabled:opacity-50"
              : "text-danger disabled:opacity-50"
          }
          onClick={remove}
        >
          {confirming ? "Удалить насовсем" : "Удалить запись"}
        </button>
      )}
    </Sheet>
  );
}
