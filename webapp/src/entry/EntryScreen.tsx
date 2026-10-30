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
import { dayLabel, shiftDay, withCurrency } from "../format";
import { useResource, useStore } from "../store";
import { haptic, telegram } from "../telegram";
import {
  amountValue,
  apiAmount,
  displayAmount,
  press,
  type Key,
} from "./amount";
import { BIG_FIELD, ErrorLine, PRIMARY } from "../dashboard/controls";
import Keypad from "./Keypad";
import {
  dropPending,
  enqueue,
  flushOutbox,
  readOutbox,
  type Pending,
} from "./outbox";
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

interface Props {
  api: Api;
  boot: Bootstrap;
  onCatalogChanged: () => void;
  wait?: Wait; // пауза перед повтором; тесты не ждут
}

/** Очередь на телефоне: что не ушло, с отказами сервера и «Удалить». */
function Outbox({
  items,
  today,
  onSend,
  onDrop,
}: {
  items: Pending[];
  today: string;
  onSend: () => void;
  onDrop: (entryId: string) => void;
}) {
  const waiting = items.some((item) => !item.error);
  return (
    <section
      aria-label="Не отправлено"
      className="flex flex-col gap-2 rounded-2xl bg-section p-3 text-line"
    >
      <p className="text-fact font-extrabold">{`📤 Не отправлено: ${items.length}`}</p>
      {waiting && <p>отправлю, когда будет связь</p>}
      <ul className="flex flex-col gap-2">
        {items.map((item) => (
          <li key={item.entry.entry_id} className="flex flex-col gap-0.5">
            <span className="font-bold">
              {`${item.title}, ${dayLabel(item.entry.day ?? today, today).toLowerCase()}`}
            </span>
            {item.error && (
              <>
                <span className="font-extrabold text-danger">{`⚠️ ${item.error}`}</span>
                <button
                  type="button"
                  className="min-h-11 self-start py-1 font-bold text-link"
                  onClick={() => onDrop(item.entry.entry_id)}
                >
                  Удалить
                </button>
              </>
            )}
          </li>
        ))}
      </ul>
      {waiting && (
        <button type="button" className={PRIMARY} onClick={onSend}>
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
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState<ExpenseResult | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [undoing, setUndoing] = useState(false);
  const [outbox, setOutbox] = useState<Pending[]>(() => readOutbox());
  const flushing = useRef(false); // одна отправка очереди за раз
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
  const label = `Записать ${withCurrency(value, currency)}`;
  const tg = telegram();

  const plan = useMemo(() => planIndex(dashboard), [dashboard]);

  // после записи: остатки — заново (не вышло — плитки без них), прогноз и план —
  // при следующем открытии их вкладки
  const loadPlan = useCallback(() => {
    store.expire(CHANGED_BY_WRITES);
    reloadDashboard();
  }, [reloadDashboard, store]);

  // очередь на телефоне: по одной попытке на трату; ушло — остатки перечитать
  const flush = useCallback(() => {
    if (flushing.current || readOutbox().every((item) => item.error)) return;
    flushing.current = true;
    flushOutbox(api)
      .then(({ sent, items }) => {
        setOutbox(items);
        if (sent > 0) {
          setNotice(`✅ Отправлено из очереди: ${sent}`);
          loadPlan();
        }
      })
      .finally(() => {
        flushing.current = false;
      });
  }, [api, loadPlan]);
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

  const submit = useCallback(() => {
    if (inFlight.current || value <= 0 || !group || !sub) return;
    inFlight.current = true;
    setSending(true);
    setError(null);
    const input: ExpenseInput = {
      entry_id: entryId,
      source,
      category: group.name,
      subcategory: sub.name,
      amount: apiAmount(amount),
      comment: comment.trim(),
      ...(day ? { day } : {}),
    };
    sendExpense(api, input, wait)
      .then((result) => {
        haptic("success");
        loadPlan();
        setDone(result);
        setNotice(null);
        setGroup(null);
        setSub(null);
        setStep("group");
      })
      .catch((reason: unknown) => {
        if (retryable(reason)) {
          // связи нет и после повторов: трата ждёт на телефоне со своей датой
          setOutbox(
            enqueue({
              entry: { ...input, day: input.day ?? boot.today },
              title: `${sub.name} ${withCurrency(value, currency)}`,
            }),
          );
          setNotice("📤 Сохранено на телефоне — отправлю, когда будет связь");
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
        setSending(false);
      });
  }, [
    amount,
    api,
    boot.today,
    comment,
    currency,
    day,
    entryId,
    group,
    loadPlan,
    onCatalogChanged,
    source,
    sub,
    value,
    wait,
  ]);

  // Системные кнопки Telegram: «Назад» после первого шага, «Записать» на сумме
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
    if (!tg || step !== "amount") return;
    tg.MainButton.show();
    return () => {
      tg.MainButton.hide();
    };
  }, [tg, step]);

  useEffect(() => {
    if (!tg || step !== "amount") return;
    tg.MainButton.setText(label);
    if (value > 0) tg.MainButton.enable();
    else tg.MainButton.disable();
    if (sending) tg.MainButton.showProgress(false);
    else tg.MainButton.hideProgress();
  }, [tg, step, label, value, sending]);

  useEffect(() => {
    if (!tg || step !== "amount") return;
    tg.MainButton.onClick(submit);
    return () => {
      tg.MainButton.offClick(submit);
    };
  }, [tg, step, submit]);

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
    ? done.lines.length > 0
      ? done.lines
      : [`✅ Уже записано: строка ${done.rows.first}`]
    : [];

  return (
    <div className="flex flex-col gap-4">
      {outbox.length > 0 && (
        <Outbox
          items={outbox}
          today={boot.today}
          onSend={flush}
          onDrop={(entryId) => setOutbox(dropPending(entryId))}
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
          {lines.map((line, index) => (
            <p key={`${index}-${line}`}>{line}</p>
          ))}
          <button
            type="button"
            className="min-h-11 self-start py-1 text-line font-bold text-danger disabled:opacity-50"
            disabled={undoing}
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
          {!tg && (
            <button
              type="button"
              className={PRIMARY}
              disabled={value <= 0 || sending}
              onClick={submit}
            >
              {label}
            </button>
          )}
        </div>
      )}
    </div>
  );
}
