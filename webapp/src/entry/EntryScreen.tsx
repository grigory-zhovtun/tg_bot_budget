import {
  Fragment,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  ApiError,
  type Api,
  type Bootstrap,
  type Dashboard,
  type ExpenseInput,
  type ExpenseResult,
  type Group,
  type Subcategory,
} from "../api";
import { botDay, dayLabel, shiftDay, withCurrency } from "../format";
import { useResource, useStore } from "../store";
import { haptic, telegram } from "../telegram";
import {
  amountValue,
  apiAmount,
  displayAmount,
  press,
  type Key,
} from "./amount";
import {
  BIG_FIELD,
  ErrorLine,
  GOOD_BUTTON,
  PRIMARY,
} from "../dashboard/controls";
import Keypad from "./Keypad";
import {
  dropPending,
  enqueueSending,
  finishSending,
  flushOutbox,
  stopsQueue,
  unsent,
  type Pending,
} from "./outbox";
import {
  creditHead,
  INCOME_GROUP,
  spentHead,
  type Outgoing,
  type Sign,
} from "./outgoing";
import { groupColor } from "./palette";
import { planIndex } from "./plan";
import { retryable, sendExpense, sleep, type Wait } from "./send";
import SourceChips from "./SourceChips";
import { GroupTiles, SubTiles } from "./Tiles";

const MAX_DAYS_BACK = 31;
// «12 345 678» — ещё 40 px; длиннее — на ступень мельче, чтобы сумма с валютой
// осталась в строке (как у главной цифры «Сводки»)
const AMOUNT_FITS = 10;
const CATALOG_FIELDS = ["source", "category", "subcategory"];
// трата и её отмена меняют сводку, прогноз, факт плана и траты из копилок
const CHANGED_BY_WRITES = ["dashboard", "forecast", "plan:", "savings"];
const OUTBOX_EVERY_MS = 30_000; // очередь на телефоне — снова, пока не пуста

type Step = "group" | "sub" | "amount";

/** Ответ на запись: строки сервера и своя первая строка («✓ Списано …»). */
interface Done {
  result: ExpenseResult;
  head: string | null;
}

interface Props {
  api: Api;
  boot: Bootstrap;
  onCatalogChanged: () => void;
  wait?: Wait; // пауза перед повтором; тесты не ждут
}

/** Очередь на телефоне: что не ушло, с отказами сервера и «Удалить» в два нажатия. */
function Outbox({
  items,
  today,
  busy,
  onSend,
  onDrop,
}: {
  items: Pending[];
  today: string;
  busy: boolean; // очередь отправляется — кнопка ждёт ответа
  onSend: () => void;
  onDrop: (entryId: string) => void;
}) {
  const [asked, setAsked] = useState<string | null>(null); // «Удалить» нажали раз
  const waiting = items.some((item) => !item.error);
  return (
    <section
      aria-label="Не отправлено"
      className="flex flex-col gap-2 rounded-2xl bg-section p-3 text-line"
    >
      <p className="text-fact font-extrabold">{`📤 Не отправлено: ${items.length}`}</p>
      {waiting && <p>отправлю, когда будет связь</p>}
      <ul className="flex flex-col gap-2">
        {items.map((item) => {
          const id = item.entry.entry_id;
          const sure = asked === id;
          return (
            <li key={id} className="flex flex-col gap-0.5">
              <span className="font-bold">
                {`${item.entry.incoming ? "зачисление: " : ""}${item.title}, ${dayLabel(item.entry.day ?? today, today).toLowerCase()}`}
              </span>
              {item.error && (
                <>
                  <span className="font-extrabold text-danger">{`⚠️ ${item.error}`}</span>
                  <button
                    type="button"
                    className={`min-h-11 self-start py-1 font-bold ${sure ? "text-danger" : "text-link"}`}
                    onClick={() => (sure ? onDrop(id) : setAsked(id))}
                  >
                    {sure ? "Точно удалить" : "Удалить"}
                  </button>
                </>
              )}
            </li>
          );
        })}
      </ul>
      {waiting && (
        <button
          type="button"
          className={PRIMARY}
          disabled={busy}
          aria-busy={busy}
          onClick={onSend}
        >
          Отправить сейчас
        </button>
      )}
    </section>
  );
}

/** Заголовок шага: «🍔 ЕДА» или «🍔 ЕДА · ☕ кофе»; длинная пара переносится
 * только между частями. */
function Header({
  parts,
  onBack,
}: {
  parts: string[];
  onBack: (() => void) | null;
}) {
  return (
    <div className="flex items-center gap-3">
      {onBack && (
        <button
          type="button"
          className="shrink-0 whitespace-nowrap text-line font-bold text-link"
          onClick={onBack}
        >
          ← Назад
        </button>
      )}
      <h2 className="text-fact font-extrabold">
        {parts.map((part, index) => (
          <Fragment key={part}>
            {index > 0 && " · "}
            <span className="whitespace-nowrap">{part}</span>
          </Fragment>
        ))}
      </h2>
    </div>
  );
}

export default function EntryScreen({
  api,
  boot,
  onCatalogChanged,
  wait = sleep,
}: Props) {
  const owner = boot.user.id;
  const fallbackSource = boot.default_source ?? boot.sources[0]?.name ?? "";
  // карта, выбранная на экране; null — по умолчанию из справочника: он мог прийти
  // из памяти телефона, а свежий — назвать другую карту
  const [picked, setPicked] = useState<string | null>(null);
  const [step, setStep] = useState<Step>("group");
  const [group, setGroup] = useState<Group | null>(null);
  const [sub, setSub] = useState<Subcategory | null>(null);
  const [amount, setAmount] = useState("");
  const [comment, setComment] = useState("");
  const [day, setDay] = useState<string | null>(null); // null — сегодня по боту
  const [entryId, setEntryId] = useState("");
  const [sending, setSending] = useState<Sign | null>(null); // чей знак ждёт ответа
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState<Done | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [undoing, setUndoing] = useState(false);
  const [outbox, setOutbox] = useState<Pending[]>(() => unsent(owner));
  const [flushing, setFlushing] = useState(false); // очередь отправляется
  const expired = useRef(false); // вход устарел: очередь — при новом открытии
  const inFlight = useRef(false); // второй тап до перерисовки не шлёт второй запрос
  const undoInFlight = useRef(false);
  const store = useStore();
  const loadDashboard = useCallback(
    (fresh: boolean) => api.dashboard(fresh),
    [api],
  );
  // Остаток по плану на плитках — подсказка: таблица не ответила, плитки без неё
  const { data: dashboard, reload: reloadDashboard } = useResource<Dashboard>(
    store,
    "dashboard",
    loadDashboard,
    {},
  );

  // справочник могли перечитать: карту, которой больше нет, не держим
  const source =
    picked !== null && boot.sources.some((s) => s.name === picked)
      ? picked
      : fallbackSource;
  const currency =
    boot.sources.find((s) => s.name === source)?.currency ?? "UZS";
  const decimals = currency !== "UZS";
  const value = amountValue(amount);
  const shown = displayAmount(amount);
  const label = `Записать ${withCurrency(value, currency)}`; // у доходов
  const income = group?.name === INCOME_GROUP;
  const tg = telegram();

  const plan = useMemo(() => planIndex(dashboard), [dashboard]);

  // после записи: остатки — заново (не вышло — плитки без них), прогноз и план —
  // при следующем открытии их вкладки
  const loadPlan = useCallback(() => {
    store.expire(CHANGED_BY_WRITES);
    reloadDashboard();
  }, [reloadDashboard, store]);

  // очередь на телефоне: по одной попытке на трату; ушло — остатки перечитать.
  // Пока идёт отмена — не шлём: сервер отменяет последнюю запись, а ею стала бы
  // трата из очереди
  const flush = useCallback(
    (byHand = false) => {
      if (expired.current || undoInFlight.current) return;
      if (unsent(owner).every((item) => item.error)) return;
      setFlushing(true);
      flushOutbox(api, owner)
        .then(({ sent, items, stopped }) => {
          setOutbox(items);
          if (stopped) {
            expired.current = true;
            setError(stopped.message);
          }
          if (sent > 0) {
            setDone(null); // «Отменить» под сводкой отменило бы трату из очереди
            setNotice(`✅ Отправлено из очереди: ${sent}`);
            loadPlan();
          } else if (byHand && !stopped && items.some((item) => !item.error)) {
            setNotice("📵 Связи нет — трата ждёт на телефоне");
          }
        })
        .finally(() => setFlushing(false));
    },
    [api, loadPlan, owner],
  );
  const flushRef = useRef(flush);
  useEffect(() => {
    flushRef.current = flush;
  }, [flush]);

  // открыли экран, вернулась связь, приложение снова на экране — отправить очередь
  useEffect(() => {
    const send = () => flushRef.current();
    const visible = () => {
      if (document.visibilityState === "visible") send();
    };
    send();
    window.addEventListener("online", send);
    document.addEventListener("visibilitychange", visible);
    return () => {
      window.removeEventListener("online", send);
      document.removeEventListener("visibilitychange", visible);
    };
  }, []);

  const waiting = outbox.some((item) => !item.error);
  useEffect(() => {
    if (!waiting) return;
    const timer = setInterval(() => flushRef.current(), OUTBOX_EVERY_MS);
    return () => clearInterval(timer);
  }, [waiting]);

  const back = useCallback(() => {
    setError(null);
    setStep((current) => (current === "amount" ? "sub" : "group"));
  }, []);

  // запись уходит на сервер — одна дорога для любой кнопки: очередь на телефоне,
  // повторы, отказы; знак — из самой записи
  const send = useCallback(
    (out: Outgoing) => {
      if (inFlight.current) return;
      inFlight.current = true;
      setSending(out.entry.incoming ? "credit" : "spend");
      setError(null);
      const input = out.entry;
      // трата на телефоне до первой попытки: закрыли приложение во время повторов —
      // уйдёт при следующем открытии с датой нажатия (сегодня по боту)
      enqueueSending(owner, {
        entry: {
          ...input,
          day: input.day ?? botDay(boot.today, boot.utc_offset),
        },
        title: out.title,
      });
      sendExpense(api, input, wait)
        .then((result) => {
          finishSending(owner, input.entry_id, false);
          haptic("success");
          loadPlan();
          setDone({ result, head: out.head });
          setNotice(null);
          setGroup(null);
          setSub(null);
          setStep("group");
        })
        .catch((reason: unknown) => {
          // связи нет и после повторов или вход устарел: трата ждёт на телефоне
          const keep = retryable(reason) || stopsQueue(reason);
          finishSending(owner, input.entry_id, keep);
          if (keep) {
            setOutbox(unsent(owner));
            if (stopsQueue(reason)) {
              expired.current = true;
              setError(reason.message);
              setNotice(null);
            } else {
              setNotice("📤 Трата сохранена на телефоне");
            }
            setDone(null);
            setGroup(null);
            setSub(null);
            setStep("group");
            return;
          }
          haptic("error");
          const fields =
            reason instanceof ApiError ? Object.keys(reason.fields) : [];
          if (fields.some((field) => CATALOG_FIELDS.includes(field))) {
            setError("Справочник обновился — выберите заново");
            setGroup(null);
            setSub(null);
            setStep("group");
            onCatalogChanged();
            return;
          }
          setError(
            reason instanceof ApiError
              ? reason.message
              : "Не получилось — попробуйте ещё раз",
          );
        })
        .finally(() => {
          inFlight.current = false;
          setSending(null);
        });
    },
    [api, boot.today, boot.utc_offset, loadPlan, onCatalogChanged, owner, wait],
  );

  // запись из полей экрана суммы: «Списать» (и «Записать» доходов) — без знака,
  // «Зачислить» — incoming: в таблицу минус
  const submit = useCallback(
    (sign: Sign) => {
      if (value <= 0 || !group || !sub) return;
      const input: ExpenseInput = {
        entry_id: entryId,
        source,
        category: group.name,
        subcategory: sub.name,
        amount: apiAmount(amount),
        comment: comment.trim(),
        ...(day ? { day } : {}),
        ...(sign === "credit" ? { incoming: true } : {}),
      };
      send({
        entry: input,
        title: `${sub.name} ${withCurrency(value, currency)}`,
        head:
          group.name === INCOME_GROUP
            ? null
            : sign === "credit"
              ? creditHead(value, currency)
              : spentHead(value, currency),
      });
    },
    [amount, comment, currency, day, entryId, group, send, source, sub, value],
  );
  const spend = useCallback(() => submit("spend"), [submit]);
  const credit = useCallback(() => submit("credit"), [submit]);

  // Системные кнопки Telegram: «Назад» после первого шага, «Записать» на сумме
  // у доходов
  useEffect(() => {
    if (!tg || step === "group") return;
    tg.BackButton.show();
    tg.BackButton.onClick(back);
    return () => {
      tg.BackButton.offClick(back);
      tg.BackButton.hide();
    };
  }, [tg, step, back]);

  useEffect(() => {
    if (!tg || step !== "amount" || !income) return;
    tg.MainButton.show();
    return () => {
      tg.MainButton.hide();
    };
  }, [tg, step, income]);

  useEffect(() => {
    if (!tg || step !== "amount" || !income) return;
    tg.MainButton.setText(label);
    if (value > 0) tg.MainButton.enable();
    else tg.MainButton.disable();
    if (sending) tg.MainButton.showProgress(false);
    else tg.MainButton.hideProgress();
  }, [tg, step, income, label, value, sending]);

  useEffect(() => {
    if (!tg || step !== "amount" || !income) return;
    tg.MainButton.onClick(spend);
    return () => {
      tg.MainButton.offClick(spend);
    };
  }, [tg, step, income, spend]);

  function chooseSource(name: string) {
    setPicked(name);
    setAmount("");
  }

  function chooseGroup(next: Group) {
    setGroup(next);
    setDone(null);
    setNotice(null);
    setError(null);
    setStep("sub");
  }

  function chooseSub(next: Subcategory) {
    setSub(next);
    setAmount("");
    setComment("");
    setDay(null);
    setEntryId(crypto.randomUUID());
    setStep("amount");
  }

  function undo() {
    if (undoInFlight.current) return; // двойной тап — одна отмена
    undoInFlight.current = true;
    setUndoing(true);
    api
      .undo()
      .then(
        (result) => {
          loadPlan();
          setNotice(result.message);
          setDone(null);
        },
        (reason: unknown) => {
          setNotice(
            reason instanceof ApiError
              ? reason.message
              : "Не получилось отменить",
          );
        },
      )
      .finally(() => {
        undoInFlight.current = false;
        setUndoing(false);
      });
  }

  const lines = done
    ? done.result.lines.length > 0
      ? done.result.lines
      : [`✅ Уже записано: строка ${done.result.rows.first}`]
    : [];
  const off = value <= 0 || sending !== null; // кнопки записи недоступны

  return (
    <div className="flex flex-col gap-4">
      {outbox.length > 0 && (
        <Outbox
          items={outbox}
          today={botDay(boot.today, boot.utc_offset)}
          busy={flushing}
          onSend={() => flush(true)}
          onDrop={(entryId) => {
            dropPending(owner, entryId);
            setOutbox(unsent(owner));
          }}
        />
      )}
      <SourceChips
        sources={boot.sources}
        current={source}
        onPick={chooseSource}
      />
      {done && (
        <div
          className="flex flex-col gap-1 rounded-2xl bg-section p-3 text-line"
          role="status"
        >
          {done.head && <p className="text-fact font-extrabold">{done.head}</p>}
          {lines.map((line, index) => (
            <p key={`${index}-${line}`}>{line}</p>
          ))}
          <button
            type="button"
            className="min-h-11 self-start py-1 text-line font-bold text-danger disabled:opacity-50"
            disabled={undoing || flushing}
            aria-busy={undoing}
            onClick={undo}
          >
            ↩️ Отменить
          </button>
        </div>
      )}
      {notice && (
        <p className="rounded-2xl bg-section p-3 text-line" role="status">
          {notice}
        </p>
      )}
      <ErrorLine text={error} />
      {step === "group" && (
        <GroupTiles groups={boot.groups} plan={plan} onPick={chooseGroup} />
      )}
      {step === "sub" && group && (
        <>
          <Header parts={[group.name]} onBack={tg ? null : back} />
          <SubTiles
            group={group}
            color={groupColor(
              group.name,
              boot.groups.findIndex((g) => g.name === group.name),
            )}
            plan={plan}
            onPick={chooseSub}
          />
        </>
      )}
      {step === "amount" && group && sub && (
        <div className="flex flex-col gap-3">
          <Header
            parts={[group.name, `${sub.icon} ${sub.name}`]}
            onBack={tg ? null : back}
          />
          <p
            className={`text-center font-black tabular-nums ${
              shown.length > AMOUNT_FITS ? "text-big-long" : "text-big"
            }`}
            aria-live="polite"
          >
            {/* число не рвётся между группами цифр; валюта может уйти ниже */}
            <span className="whitespace-nowrap">{shown}</span>{" "}
            <span className="text-fact font-extrabold">{currency}</span>
          </p>
          <label className="flex flex-col gap-1 text-line font-bold">
            Комментарий
            <input
              className={`${BIG_FIELD} min-h-12`}
              value={comment}
              maxLength={200}
              onChange={(event) => setComment(event.target.value)}
            />
          </label>
          <label className="flex items-center justify-between gap-3 text-line font-bold">
            <span>Дата: {dayLabel(day ?? boot.today, boot.today)}</span>
            <input
              type="date"
              className={BIG_FIELD}
              value={day ?? boot.today}
              min={shiftDay(boot.today, -MAX_DAYS_BACK)}
              max={boot.today}
              onChange={(event) => {
                const picked = event.target.value;
                setDay(picked && picked !== boot.today ? picked : null);
              }}
            />
          </label>
          <Keypad
            big
            decimals={decimals}
            onKey={(key: Key) =>
              setAmount((text) => press(text, key, decimals))
            }
          />
          {!income ? (
            // «Списать» справа — ближе к большому пальцу, её жмут чаще
            <div className="grid grid-cols-2 gap-2">
              <button
                type="button"
                className={GOOD_BUTTON}
                disabled={off}
                onClick={credit}
              >
                Зачислить
              </button>
              <button
                type="button"
                className={PRIMARY}
                disabled={off}
                onClick={spend}
              >
                Списать
              </button>
            </div>
          ) : (
            !tg && (
              <button
                type="button"
                className={PRIMARY}
                disabled={off}
                onClick={spend}
              >
                {label}
              </button>
            )
          )}
        </div>
      )}
    </div>
  );
}
