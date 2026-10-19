import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ApiError,
  type Api,
  type Bootstrap,
  type Dashboard,
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
import Keypad from "./Keypad";
import { groupColor } from "./palette";
import { planIndex } from "./plan";
import SourceChips from "./SourceChips";
import { GroupTiles, SubTiles } from "./Tiles";

const MAX_DAYS_BACK = 31;
// «12 345 678» — ещё 40 px; длиннее — на ступень мельче, чтобы сумма с валютой
// осталась в строке (как у главной цифры «Сводки»)
const AMOUNT_FITS = 10;
const CATALOG_FIELDS = ["source", "category", "subcategory"];
// трата и её отмена меняют сводку, прогноз, факт плана и траты из копилок
const CHANGED_BY_WRITES = ["dashboard", "forecast", "plan:", "savings"];

type Step = "group" | "sub" | "amount";

interface Props {
  api: Api;
  boot: Bootstrap;
  onCatalogChanged: () => void;
}

function Header({
  title,
  onBack,
}: {
  title: string;
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
      <h2 className="text-fact font-extrabold">{title}</h2>
    </div>
  );
}

export default function EntryScreen({ api, boot, onCatalogChanged }: Props) {
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

  const back = useCallback(() => {
    setError(null);
    setStep((current) => (current === "amount" ? "sub" : "group"));
  }, []);

  const submit = useCallback(() => {
    if (inFlight.current || value <= 0 || !group || !sub) return;
    inFlight.current = true;
    setSending(true);
    setError(null);
    api
      .addExpense({
        entry_id: entryId,
        source,
        category: group.name,
        subcategory: sub.name,
        amount: apiAmount(amount),
        comment: comment.trim(),
        ...(day ? { day } : {}),
      })
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
    comment,
    day,
    entryId,
    group,
    loadPlan,
    onCatalogChanged,
    source,
    sub,
    value,
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
            className="self-start py-1 text-line font-bold text-danger disabled:opacity-50"
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
      {error && (
        <p className="text-line font-bold text-danger" role="alert">
          {error}
        </p>
      )}
      {step === "group" && (
        <GroupTiles groups={boot.groups} plan={plan} onPick={chooseGroup} />
      )}
      {step === "sub" && group && (
        <>
          <Header title={group.name} onBack={tg ? null : back} />
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
            title={`${group.name} · ${sub.icon} ${sub.name}`}
            onBack={tg ? null : back}
          />
          <p
            className={`text-center font-black tabular-nums ${
              shown.length > AMOUNT_FITS ? "text-big-long" : "text-big"
            }`}
            aria-live="polite"
          >
            {shown} <span className="text-fact font-extrabold">{currency}</span>
          </p>
          <label className="flex flex-col gap-1 text-line font-bold">
            Комментарий
            <input
              className="min-h-12 rounded-xl bg-section px-3 py-2 text-lg font-normal text-text"
              value={comment}
              maxLength={200}
              onChange={(event) => setComment(event.target.value)}
            />
          </label>
          <label className="flex items-center justify-between gap-3 text-line font-bold">
            <span>Дата: {dayLabel(day ?? boot.today, boot.today)}</span>
            <input
              type="date"
              className="rounded-xl bg-section px-3 py-2 text-lg font-normal text-text"
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
              className="h-14 rounded-xl bg-button text-lg font-extrabold text-button-text disabled:opacity-50"
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
