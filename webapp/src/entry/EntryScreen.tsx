import { useCallback, useEffect, useRef, useState } from "react";
import {
  ApiError,
  type Api,
  type Bootstrap,
  type ExpenseResult,
  type Group,
  type Subcategory,
} from "../api";
import { dayLabel, shiftDay, withCurrency } from "../format";
import { haptic, telegram } from "../telegram";
import {
  amountValue,
  apiAmount,
  displayAmount,
  press,
  type Key,
} from "./amount";
import Keypad from "./Keypad";
import SourceChips from "./SourceChips";
import { GroupTiles, SubTiles } from "./Tiles";

const MAX_DAYS_BACK = 31;
const CATALOG_FIELDS = ["source", "category", "subcategory"];

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
        <button type="button" className="text-link" onClick={onBack}>
          ← Назад
        </button>
      )}
      <h2 className="font-semibold">{title}</h2>
    </div>
  );
}

export default function EntryScreen({ api, boot, onCatalogChanged }: Props) {
  const fallbackSource = boot.default_source ?? boot.sources[0]?.name ?? "";
  const [chosenSource, setChosenSource] = useState(fallbackSource);
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

  // справочник могли перечитать: карту, которой больше нет, не держим
  const source = boot.sources.some((s) => s.name === chosenSource)
    ? chosenSource
    : fallbackSource;
  const currency =
    boot.sources.find((s) => s.name === source)?.currency ?? "UZS";
  const decimals = currency !== "UZS";
  const value = amountValue(amount);
  const label = `Записать ${withCurrency(value, currency)}`;
  const tg = telegram();

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
    setChosenSource(name);
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
          className="flex flex-col gap-1 rounded-2xl bg-section p-3 text-sm"
          role="status"
        >
          {lines.map((line, index) => (
            <p key={`${index}-${line}`}>{line}</p>
          ))}
          <button
            type="button"
            className="self-start text-danger disabled:opacity-50"
            disabled={undoing}
            aria-busy={undoing}
            onClick={undo}
          >
            ↩️ Отменить
          </button>
        </div>
      )}
      {notice && (
        <p className="rounded-2xl bg-section p-3 text-sm" role="status">
          {notice}
        </p>
      )}
      {error && (
        <p className="text-sm text-danger" role="alert">
          {error}
        </p>
      )}
      {step === "group" && (
        <GroupTiles groups={boot.groups} onPick={chooseGroup} />
      )}
      {step === "sub" && group && (
        <>
          <Header title={group.name} onBack={tg ? null : back} />
          <SubTiles group={group} onPick={chooseSub} />
        </>
      )}
      {step === "amount" && group && sub && (
        <div className="flex flex-col gap-3">
          <Header
            title={`${group.title} · ${sub.icon} ${sub.name}`}
            onBack={tg ? null : back}
          />
          <p className="text-center text-4xl font-semibold" aria-live="polite">
            {displayAmount(amount)}{" "}
            <span className="text-xl text-hint">{currency}</span>
          </p>
          <label className="flex flex-col gap-1 text-sm text-hint">
            Комментарий
            <input
              className="rounded-xl bg-section px-3 py-2 text-base text-text"
              value={comment}
              maxLength={200}
              onChange={(event) => setComment(event.target.value)}
            />
          </label>
          <label className="flex items-center justify-between gap-3 text-sm text-hint">
            <span>Дата: {dayLabel(day ?? boot.today, boot.today)}</span>
            <input
              type="date"
              className="rounded-xl bg-section px-3 py-2 text-text"
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
            decimals={decimals}
            onKey={(key: Key) =>
              setAmount((text) => press(text, key, decimals))
            }
          />
          {!tg && (
            <button
              type="button"
              className="h-12 rounded-xl bg-button font-semibold text-button-text disabled:opacity-50"
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
