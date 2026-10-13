import { useCallback, useEffect, useRef, useState } from "react";
import {
  ApiError,
  planConflict,
  type Api,
  type MonthPlan,
  type PlanArticle,
} from "../api";
import { groupColor, tint } from "../entry/palette";
import { itemAmount, money, short, withCurrency } from "../format";
import { haptic, telegram } from "../telegram";
import ArticleSheet from "./ArticleSheet";
import {
  amountText,
  changeCount,
  editEvent,
  editGoal,
  editRow,
  emptyDraft,
  planTotals,
  rebase,
  rowPlan,
  shownEvents,
  shownGoal,
  toSave,
  type Draft,
} from "./draft";
import EventSheet from "./EventSheet";
import GoalSheet from "./GoalSheet";
import { monthShort, plural } from "./months";

type Editing =
  | { kind: "article"; key: string }
  | { kind: "event"; slot: number }
  | { kind: "goal" }
  | null;

interface Props {
  api: Api;
  month: string;
  draft: Draft;
  restored: boolean; // черновик с прошлого открытия приложения
  onDraft: (draft: Draft) => void;
  onRestoredSeen: () => void;
}

const KEPT = "Правки не потеряны — сохраните ещё раз";
const RING = "rounded-lg ring-2 ring-warn";

function changesText(count: number): string {
  return `${count} ${plural(count, "изменение", "изменения", "изменений")}`;
}

function currencies(plan: MonthPlan): string[] {
  const others = Object.keys(plan.rates).filter((code) => code !== "UZS");
  return ["UZS", ...others.sort()];
}

function groupsOf(rows: PlanArticle[]): [string, PlanArticle[]][] {
  const groups = new Map<string, PlanArticle[]>();
  for (const row of rows) {
    groups.set(row.group, [...(groups.get(row.group) ?? []), row]);
  }
  return [...groups.entries()];
}

function Line({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between">
      <span className="text-hint">{label}</span>
      <span className="font-medium">{value}</span>
    </div>
  );
}

interface ArticleLineProps {
  article: PlanArticle;
  value: number;
  edited: boolean;
  conflict: boolean;
  hint: string;
  onPick: () => void;
}

function ArticleLine({
  article,
  value,
  edited,
  conflict,
  hint,
  onPick,
}: ArticleLineProps) {
  const foreign = article.currency !== "UZS";
  const fraction = foreign ? 2 : 0;
  return (
    <button
      type="button"
      aria-label={
        foreign
          ? `${article.subcategory} ${article.currency}`
          : article.subcategory
      }
      data-conflict={conflict || undefined}
      disabled={!article.editable}
      className={`flex w-full items-center justify-between gap-3 border-t border-black/5 py-2 text-left disabled:opacity-60 ${conflict ? RING : ""}`}
      onClick={onPick}
    >
      <span className="flex flex-col">
        <span className="font-medium">
          {article.icon} {article.subcategory}
          {foreign ? ` · ${article.currency}` : ""}
        </span>
        <span className="text-xs text-hint">
          {article.editable
            ? hint
            : "Статья встречается дважды — поправьте вкладку"}
        </span>
      </span>
      <span className="text-right">
        <span className={`block font-semibold ${edited ? "text-link" : ""}`}>
          {money(value, fraction)}
        </span>
        {edited && (
          <span className="block text-xs text-hint line-through">
            {money(article.plan, fraction)}
          </span>
        )}
      </span>
    </button>
  );
}

export default function PlanScreen({
  api,
  month,
  draft,
  restored,
  onDraft,
  onRestoredSeen,
}: Props) {
  const [plan, setPlan] = useState<MonthPlan | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
  const [editing, setEditing] = useState<Editing>(null);
  const [saving, setSaving] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [conflicts, setConflicts] = useState<string[]>([]);
  const draftRef = useRef(draft);
  const onDraftRef = useRef(onDraft);
  const inFlight = useRef(false); // второй тап до перерисовки не шлёт второй запрос
  const tg = telegram();

  useEffect(() => {
    draftRef.current = draft;
    onDraftRef.current = onDraft;
  });

  useEffect(() => {
    let alive = true;
    api.plan(month).then(
      (data) => {
        if (!alive) return;
        setPlan(data);
        setError(null);
        if (data.status !== "ok") return;
        // черновик с прошлого раза ложится на свежий план; что поменялось — подсветить
        const next = rebase(draftRef.current, data);
        setConflicts(next.conflicts);
        onDraftRef.current(next.draft);
      },
      (reason: unknown) => {
        if (!alive) return;
        setError(
          reason instanceof ApiError
            ? reason.message
            : "Не получилось загрузить план — попробуйте ещё раз",
        );
      },
    );
    return () => {
      alive = false;
    };
  }, [api, month, version]);

  const count = changeCount(draft);
  const label = `Сохранить ${changesText(count)}`;

  const save = useCallback(() => {
    if (inFlight.current || changeCount(draft) === 0) return;
    inFlight.current = true;
    setSaving(true);
    setNotice(null);
    api
      .savePlan(toSave(draft))
      .then(
        (fresh) => {
          haptic("success");
          setPlan(fresh);
          setConflicts([]);
          // правки, сделанные пока шло сохранение, остаются; записанные совпали
          // со свежим планом и уходят сами
          onDraft(rebase(draftRef.current, fresh).draft);
          setNotice(
            `✅ План сохранён: ${changesText(fresh.saved ?? changeCount(draft))}`,
          );
        },
        (reason: unknown) => {
          haptic("error");
          const conflict = planConflict(reason);
          const message =
            reason instanceof ApiError
              ? reason.message
              : "Не получилось сохранить";
          if (conflict) {
            const next = rebase(draftRef.current, conflict.plan);
            setPlan(conflict.plan);
            setConflicts(conflict.conflicts);
            onDraft(next.draft);
            setNotice(message);
            return;
          }
          setNotice(`${message}. ${KEPT}`);
        },
      )
      .finally(() => {
        inFlight.current = false;
        setSaving(false);
      });
  }, [api, draft, onDraft]);

  // Системная кнопка Telegram: «Сохранить N изменений», пока не открыто окно правки
  useEffect(() => {
    if (!tg || count === 0 || editing) return;
    tg.MainButton.setText(label);
    tg.MainButton.show();
    tg.MainButton.onClick(save);
    if (saving) tg.MainButton.showProgress(false);
    else tg.MainButton.hideProgress();
    return () => {
      tg.MainButton.offClick(save);
      tg.MainButton.hide();
    };
  }, [tg, count, editing, label, save, saving]);

  // «Назад» Telegram закрывает окно правки
  useEffect(() => {
    if (!tg || !editing) return;
    const close = () => setEditing(null);
    tg.BackButton.show();
    tg.BackButton.onClick(close);
    return () => {
      tg.BackButton.offClick(close);
      tg.BackButton.hide();
    };
  }, [tg, editing]);

  if (!plan) {
    if (!error) return <p className="text-hint">Загрузка плана…</p>;
    return (
      <div className="flex flex-col items-start gap-2" role="alert">
        <p>{error}</p>
        <button
          type="button"
          className="text-link"
          onClick={() => {
            setError(null);
            setVersion((current) => current + 1);
          }}
        >
          Повторить
        </button>
      </div>
    );
  }
  if (plan.status !== "ok") {
    return (
      <p className="rounded-2xl bg-section p-4 text-sm">
        Вкладки месяца ещё нет — бот создаёт её сам 1-го числа.
      </p>
    );
  }

  const totals = planTotals(plan, draft);
  const events = shownEvents(plan, draft);
  const goal = shownGoal(plan, draft);
  const history = plan.history;
  const hint = (row: PlanArticle) =>
    history
      ? `ср. ${short(row.average)} · ${monthShort(history.last)} ${short(row.last)}`
      : "";
  const spend = groupsOf(plan.rows.filter((row) => row.kind === "spend"));
  const income = plan.rows.filter((row) => row.kind === "income");
  const filled = events
    .filter((item) => item.name !== null)
    .sort((left, right) => (left.day ?? 0) - (right.day ?? 0));
  const empty = events.find((item) => item.name === null);
  const differs =
    Math.abs(totals.incomeRows - totals.incomes) >
    0.01 * Math.max(totals.incomes, 1);
  const article =
    editing?.kind === "article"
      ? plan.rows.find((row) => row.key === editing.key)
      : undefined;
  const event =
    editing?.kind === "event"
      ? events.find((item) => item.slot === editing.slot)
      : undefined;
  const close = () => setEditing(null);

  const line = (row: PlanArticle) => (
    <ArticleLine
      key={row.key}
      article={row}
      value={rowPlan(draft, row)}
      edited={draft.rows[row.key] !== undefined}
      conflict={conflicts.includes(row.key)}
      hint={hint(row)}
      onPick={() => setEditing({ kind: "article", key: row.key })}
    />
  );

  return (
    <div className="flex flex-col gap-4">
      {restored && count > 0 && (
        <div
          className="flex flex-col gap-2 rounded-2xl bg-section p-3 text-sm"
          role="status"
        >
          <p>Есть несохранённые правки с прошлого раза</p>
          <div className="flex gap-4">
            <button
              type="button"
              className="text-link"
              onClick={onRestoredSeen}
            >
              Продолжить
            </button>
            <button
              type="button"
              className="text-danger"
              onClick={() => {
                onDraft(emptyDraft(month));
                setConflicts([]);
                onRestoredSeen();
              }}
            >
              Сбросить
            </button>
          </div>
        </div>
      )}
      {notice && (
        <p className="rounded-2xl bg-section p-3 text-sm" role="status">
          {notice}
        </p>
      )}
      <section
        className="flex flex-col gap-1 rounded-2xl bg-section p-4 text-sm"
        aria-label="Итог месяца"
      >
        <Line label="Поступления" value={short(totals.incomes)} />
        <Line label="Расходы по плану" value={short(totals.spend)} />
        <Line label="🎯 Отложить" value={short(totals.goal)} />
        <div className="mt-2 flex items-baseline justify-between border-t border-black/10 pt-2">
          <span className="font-semibold">
            {totals.free >= 0 ? "Свободно" : "Не хватает"}
          </span>
          <span
            className={`text-2xl font-semibold ${totals.free >= 0 ? "text-ok" : "text-danger"}`}
          >
            {short(Math.abs(totals.free))}
          </span>
        </div>
      </section>
      {plan.events === null ? (
        <p className="text-sm text-hint">
          Списка поступлений во вкладке нет — его правят в таблице.
        </p>
      ) : (
        <section className="flex flex-col" aria-label="Поступления и платежи">
          <h3 className="mb-1 text-xs font-semibold text-hint uppercase">
            Поступления и платежи
          </h3>
          {filled.map((item) => {
            const conflict = conflicts.includes(`event:${item.slot}`);
            const edited = draft.events[String(item.slot)] !== undefined;
            const amount = item.amount ?? 0;
            return (
              <button
                key={item.slot}
                type="button"
                data-conflict={conflict || undefined}
                className={`flex items-center gap-3 border-t border-black/5 py-2 text-left ${conflict ? RING : ""}`}
                onClick={() => setEditing({ kind: "event", slot: item.slot })}
              >
                <span className="w-8 rounded-lg bg-section py-0.5 text-center text-xs font-bold">
                  {item.day}
                </span>
                <span className={`flex-1 ${edited ? "text-link" : ""}`}>
                  {item.name}
                </span>
                <span className={amount > 0 ? "text-ok" : "text-danger"}>
                  {itemAmount(amount, item.currency ?? "UZS", amount)}
                </span>
              </button>
            );
          })}
          {empty && (
            <button
              type="button"
              className="py-2 text-left text-link"
              onClick={() => setEditing({ kind: "event", slot: empty.slot })}
            >
              ＋ Добавить поступление или платёж
            </button>
          )}
        </section>
      )}
      {spend.map(([group, rows], index) => {
        const color = groupColor(group, index);
        const sum = rows.reduce(
          (total, row) =>
            total + rowPlan(draft, row) * (plan.rates[row.currency] ?? 0),
          0,
        );
        return (
          <section
            key={group}
            aria-label={group}
            className="rounded-3xl border px-3 pt-2 pb-1"
            style={{
              background: tint(color, 0.12),
              borderColor: tint(color, 0.28),
            }}
          >
            <div className="flex items-center justify-between pb-1">
              <h3 className="font-semibold">{group}</h3>
              <span className="text-sm text-hint">{short(sum)}</span>
            </div>
            {rows.map(line)}
          </section>
        );
      })}
      {income.length > 0 && (
        <section
          className="flex flex-col rounded-3xl bg-section px-3 pt-2 pb-1"
          aria-label="Доходы по статьям"
        >
          <h3 className="pb-1 font-semibold">Доходы по статьям</h3>
          {income.map(line)}
          {differs && (
            <p className="py-2 text-xs text-hint">
              {`По статьям ${short(totals.incomeRows)}, в поступлениях ${short(totals.incomes)}`}
            </p>
          )}
        </section>
      )}
      {goal && (
        <button
          type="button"
          data-conflict={conflicts.includes("goal") || undefined}
          className={`flex items-center justify-between rounded-2xl bg-section p-3 text-left ${conflicts.includes("goal") ? "ring-2 ring-warn" : ""}`}
          onClick={() => setEditing({ kind: "goal" })}
        >
          <span>🎯 Отложить за месяц</span>
          <span className={`font-semibold ${draft.goal ? "text-link" : ""}`}>
            {withCurrency(goal.amount, goal.currency)}
          </span>
        </button>
      )}
      {!tg && count > 0 && (
        <button
          type="button"
          className="h-12 rounded-xl bg-button font-semibold text-button-text disabled:opacity-50"
          disabled={saving}
          onClick={save}
        >
          {label}
        </button>
      )}
      {article && (
        <ArticleSheet
          article={article}
          text={
            draft.rows[article.key]?.text ??
            amountText(article.plan, article.currency !== "UZS")
          }
          history={plan.history}
          onDone={(text) => {
            onDraft(editRow(draft, article, text));
            close();
          }}
          onClose={close}
        />
      )}
      {event && (
        <EventSheet
          event={event}
          currencies={currencies(plan)}
          onDone={(value) => {
            const original =
              plan.events?.find((item) => item.slot === event.slot) ?? event;
            onDraft(editEvent(draft, original, value));
            close();
          }}
          onClose={close}
        />
      )}
      {editing?.kind === "goal" && goal && plan.goal && (
        <GoalSheet
          goal={goal}
          currencies={currencies(plan)}
          onDone={(text, currency) => {
            if (plan.goal) onDraft(editGoal(draft, plan.goal, text, currency));
            close();
          }}
          onClose={close}
        />
      )}
    </div>
  );
}
