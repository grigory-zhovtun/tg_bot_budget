import { useCallback, useEffect, useRef, useState } from "react";
import {
  ApiError,
  planConflict,
  type AdvicePoint,
  type Api,
  type MonthPlan,
  type PlanArticle,
  type PlanGoal,
  type PlanGoalHint,
} from "../api";
import { groupColor, tint } from "../entry/palette";
import { longDay, money, moneyText } from "../format";
import { useResource, useStore } from "../store";
import { haptic, telegram } from "../telegram";
import { amountValue } from "../entry/amount";
import { percentText } from "../dashboard/piggyText";
import { Big, Card, Fact, Line, Table, Warn } from "../dashboard/blocks";
import AddArticleSheet from "./AddArticleSheet";
import AdvicePanel, { type AdviceState } from "./AdvicePanel";
import ArticleSheet from "./ArticleSheet";
import {
  addRow,
  amountText,
  changeCount,
  editEvent,
  applyGoal,
  editGoal,
  editRow,
  emptyDraft,
  pairKey,
  planTotals,
  rebase,
  removeRow,
  rowPlan,
  shownEvents,
  shownGoal,
  toAdvice,
  toSave,
  type AddedRow,
  type Draft,
} from "./draft";
import EventSheet from "./EventSheet";
import { noticeParts } from "./notice";
import PlanEvents from "./PlanEvents";
import GoalSheet from "./GoalSheet";
import PiggyHints from "./PiggyHints";
import { monthDative, monthIn, monthName, monthShort, plural } from "./months";
import { receiptHint } from "./receipt";

type Editing =
  | { kind: "article"; key: string }
  | { kind: "event"; slot: number }
  | { kind: "goal" }
  | { kind: "add"; group: string; key: string | null } // key — правка добавленной
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

const GOAL_STEP = 1000; // «Отложить за месяц» в таблицу — до тысячи

function goalInSums(goal: PlanGoal, rates: Record<string, number>): number {
  return goal.currency === "UZS"
    ? goal.amount
    : goal.amount * (rates[goal.currency] ?? 0);
}

/** «Откладываем N % — W: копилки X + подушка Y − из копилок Z» и «Применить», пока
 * цель черновика с этим расходится (спека 4.7, процент — спека процента 4.6). */
function GoalHint({
  hint,
  shown,
  onApply,
}: {
  hint: PlanGoalHint;
  shown: number;
  onApply: (suggested: number) => void;
}) {
  if (Math.abs(shown - hint.suggested) < GOAL_STEP) return null;
  return (
    <Card label="По копилкам">
      <Fact>{`По копилкам ${money(hint.suggested)}`}</Fact>
      <Line>
        {`откладываем ${percentText(hint.percent)}\u00a0% с поступлений`}
      </Line>
      <Table
        align="lr"
        rows={[
          ["копилки", money(hint.piggies)],
          ["подушка", money(hint.share)],
          ["из копилок", minus(hint.spent)],
        ]}
      />
      <button
        type="button"
        className="mt-2 h-12 rounded-xl bg-button text-lg font-extrabold text-button-text"
        onClick={() => onApply(hint.suggested)}
      >
        {`Применить ${money(hint.suggested)}`}
      </button>
    </Card>
  );
}

function changesText(count: number): string {
  return `${count} ${plural(count, "изменение", "изменения", "изменений")}`;
}

function currencies(plan: MonthPlan): string[] {
  const others = Object.keys(plan.rates).filter((code) => code !== "UZS");
  return ["UZS", ...others.sort()];
}

function groupsOf(
  rows: PlanArticle[],
  extra: string[],
): [string, PlanArticle[]][] {
  const groups = new Map<string, PlanArticle[]>();
  for (const row of rows) {
    groups.set(row.group, [...(groups.get(row.group) ?? []), row]);
  }
  for (const group of extra) {
    if (!groups.has(group)) groups.set(group, []);
  }
  return [...groups.entries()];
}

/** «−10 500 000» — расход в итоге; ноль — без знака. */
function minus(value: number): string {
  return Math.round(value) > 0 ? `−${money(value)}` : "0";
}

/** Сообщение «заголовок: подробности» — крупной строкой и строкой под ней. */
function Notice({ text }: { text: string }) {
  const { head, rest } = noticeParts(text);
  return (
    <div
      className="flex flex-col gap-1 rounded-2xl bg-section p-4"
      role="status"
    >
      {head && <Fact>{head}</Fact>}
      <p className={`text-line ${head ? "" : "font-bold"}`}>{rest}</p>
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

/** Название статьи и её план в одной строке; длинное название — план строкой ниже,
 * справа (без переноса текста). */
function NameAndSum({
  name,
  sum,
  tone = "",
}: {
  name: string;
  sum: string;
  tone?: string;
}) {
  return (
    <span className="flex w-full flex-wrap items-baseline justify-between gap-x-3 gap-y-0.5">
      <span className="text-line font-extrabold">{name}</span>
      <span
        className={`ml-auto text-fact font-black whitespace-nowrap tabular-nums ${tone}`}
      >
        {sum}
      </span>
    </span>
  );
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
      className={`flex w-full flex-col gap-0.5 border-t border-black/10 py-2 text-left ${conflict ? RING : ""}`}
      onClick={onPick}
    >
      <NameAndSum
        name={`${article.icon} ${article.subcategory}${foreign ? ` · ${article.currency}` : ""}`}
        sum={moneyText(value, article.currency)}
        tone={edited ? "text-link" : ""}
      />
      {edited && (
        <span className="self-end text-line line-through">
          {moneyText(article.plan, article.currency)}
        </span>
      )}
      {article.editable ? (
        hint && <span className="text-line">{hint}</span>
      ) : (
        <>
          <span className="text-line font-extrabold text-danger">
            ⚠️ статья встречается дважды
          </span>
          <span className="text-line">поправьте вкладку</span>
        </>
      )}
    </button>
  );
}

function AddedLine({ row, onPick }: { row: AddedRow; onPick: () => void }) {
  const foreign = row.currency !== "UZS";
  return (
    <button
      type="button"
      aria-label={`${row.subcategory}${foreign ? ` ${row.currency}` : ""}, новая`}
      className="flex w-full flex-col gap-0.5 border-t border-black/10 py-2 text-left"
      onClick={onPick}
    >
      <NameAndSum
        name={`${row.icon} ${row.subcategory}${foreign ? ` · ${row.currency}` : ""}`}
        sum={moneyText(amountValue(row.text), row.currency)}
        tone="text-link"
      />
      <span className="text-line font-extrabold text-link">новая статья</span>
    </button>
  );
}

/** «2026-11» → «2026-10». */
function monthBefore(month: string): string {
  const [year = 0, number = 1] = month.split("-").map(Number);
  return number === 1
    ? `${year - 1}-12`
    : `${year}-${String(number - 1).padStart(2, "0")}`;
}

export default function PlanScreen({
  api,
  month,
  draft,
  restored,
  onDraft,
  onRestoredSeen,
}: Props) {
  const [editing, setEditing] = useState<Editing>(null);
  const [saving, setSaving] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [conflicts, setConflicts] = useState<string[]>([]);
  const [creating, setCreating] = useState(false);
  const [advice, setAdvice] = useState<AdviceState | null>(null);
  const [applied, setApplied] = useState<string[]>([]);
  const asking = useRef(false); // второй тап до перерисовки не шлёт второй запрос
  const draftRef = useRef(draft);
  const onDraftRef = useRef(onDraft);
  const inFlight = useRef(false); // второй тап до перерисовки не шлёт второй запрос
  const tg = telegram();

  useEffect(() => {
    draftRef.current = draft;
    onDraftRef.current = onDraft;
  });

  const store = useStore();
  const key = `plan:${month}`;
  // с несохранёнными правками — таблица заново, не копия на сервере: что
  // поменяли руками, подсветится сразу, а не ошибкой при сохранении
  const load = useCallback(
    (fresh: boolean) =>
      api.plan(month, fresh || changeCount(draftRef.current) > 0),
    [api, month],
  );
  // черновик с прошлого раза ложится на свежий план; что поменялось — подсветить
  const answered = useCallback((data: MonthPlan) => {
    if (data.status !== "ok") return;
    const next = rebase(draftRef.current, data);
    setConflicts(next.conflicts);
    onDraftRef.current(next.draft);
  }, []);
  // копия плана видна сразу, но сервер спрашивается всегда: черновик ложится
  // только на свежий план
  const {
    data: plan,
    error,
    reload,
  } = useResource<MonthPlan>(store, key, load, {
    failure: "Не получилось загрузить план — попробуйте ещё раз",
    always: true,
    onAnswer: answered,
  });
  const setPlan = useCallback(
    (fresh: MonthPlan) => {
      store.put(key, fresh);
    },
    [key, store],
  );

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
          // план меняет сводку, прогноз и взносы копилок (день денег, 10 %) —
          // их вкладки спросят сервер заново
          store.expire(["dashboard", "forecast", "savings"]);
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
          const fields =
            reason instanceof ApiError ? Object.values(reason.fields) : [];
          const message =
            fields[0] ??
            (reason instanceof ApiError
              ? reason.message
              : "Не получилось сохранить");
          if (conflict) {
            const next = rebase(draftRef.current, conflict.plan);
            setPlan(conflict.plan);
            setConflicts(conflict.conflicts);
            onDraft(next.draft);
            setNotice(message);
            return;
          }
          if (reason instanceof ApiError && reason.status === 422) {
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
  }, [api, draft, onDraft, setPlan, store]);

  const create = useCallback(() => {
    if (inFlight.current) return; // второй тап до перерисовки — без второго запроса
    inFlight.current = true;
    setCreating(true);
    setNotice(null);
    api
      .createNextPlan()
      .then(
        (fresh) => {
          haptic("success");
          setPlan(fresh);
          // черновик с прошлого раза ложится на созданный план, как при открытии
          const next = rebase(draftRef.current, fresh);
          setConflicts(next.conflicts);
          onDraftRef.current(next.draft);
        },
        (reason: unknown) => {
          haptic("error");
          setNotice(
            reason instanceof ApiError
              ? reason.message
              : "Не получилось создать план — попробуйте ещё раз",
          );
        },
      )
      .finally(() => {
        inFlight.current = false;
        setCreating(false);
      });
  }, [api, setPlan]);

  const ask = useCallback(() => {
    if (asking.current) return;
    asking.current = true;
    setAdvice({ kind: "loading" });
    setApplied([]);
    api
      .planAdvice(toAdvice(draftRef.current))
      .then(
        (answer) => setAdvice({ kind: "ready", points: answer.points }),
        (reason: unknown) =>
          setAdvice({
            kind: "error",
            message:
              reason instanceof ApiError
                ? reason.message
                : "Советник сейчас недоступен",
          }),
      )
      .finally(() => {
        asking.current = false;
      });
  }, [api]);

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
    if (!error) return <p className="text-line">Загрузка плана…</p>;
    return (
      <div className="flex flex-col items-start gap-2" role="alert">
        <p className="text-line font-bold">{error}</p>
        <button
          type="button"
          className="text-line font-extrabold text-link"
          onClick={() => {
            reload();
          }}
        >
          Повторить
        </button>
      </div>
    );
  }
  if (plan.status === "not_created") {
    const now = monthBefore(month);
    const [, nowGenitive = ""] = longDay(`${now}-01`).split(" ");
    return (
      <section
        className="flex flex-col items-center gap-1 rounded-2xl bg-section p-6 text-center"
        aria-label="Следующий месяц"
      >
        <p className="text-4xl" aria-hidden="true">
          📅
        </p>
        <h3 className="text-fact font-extrabold">
          {`Плана на ${monthName(month)} ещё нет`}
        </h3>
        <Line>{`создам вкладку «${plan.title}»`}</Line>
        <Line>{`копией ${nowGenitive}: статьи, суммы,`}</Line>
        <Line>поступления и цель</Line>
        <Line>потом поправите, что изменится</Line>
        <Line>{`до ${longDay(`${month}-01`)} вкладка спрятана`}</Line>
        <Line>{`бот и лимит считают по ${monthDative(now)}`}</Line>
        {notice && (
          <p className="text-line font-extrabold text-danger" role="alert">
            {notice}
          </p>
        )}
        <button
          type="button"
          disabled={creating}
          className="mt-2 h-12 w-full rounded-xl bg-button text-lg font-extrabold text-button-text disabled:opacity-50"
          onClick={create}
        >
          {`Создать план на ${monthName(month)}`}
        </button>
      </section>
    );
  }
  if (plan.status !== "ok") {
    return (
      <div className="flex flex-col rounded-2xl bg-section p-4">
        <Warn>Вкладки месяца ещё нет</Warn>
        <Line>бот создаёт её сам 1-го числа</Line>
      </div>
    );
  }

  const totals = planTotals(plan, draft);
  const events = shownEvents(plan, draft);
  const goal = shownGoal(plan, draft);
  const history = plan.history;
  const hint = (row: PlanArticle) =>
    history
      ? `ср. ${moneyText(row.average, row.currency)}, ${monthShort(history.last)} ${moneyText(row.last, row.currency)}`
      : "";
  const extra = [
    ...new Set(
      plan.addable
        .filter((item) => item.kind === "spend")
        .map((item) => item.group),
    ),
  ];
  const spend = groupsOf(
    plan.rows.filter((row) => row.kind === "spend"),
    extra,
  );
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
  const target = (point: AdvicePoint) =>
    point.apply
      ? plan.rows.find((row) => row.key === point.apply?.key && row.editable)
      : undefined;
  const applyLabel = (point: AdvicePoint) => {
    const article = target(point);
    if (!article || !point.apply) return null;
    const foreign = article.currency !== "UZS";
    const name = foreign
      ? `${article.subcategory} ${article.currency}`
      : article.subcategory;
    return `Применить: ${name} → ${money(point.apply.plan, foreign ? 2 : 0)}`;
  };
  const applyPoint = (point: AdvicePoint) => {
    const article = target(point);
    if (!article || !point.apply) return;
    const text = amountText(point.apply.plan, article.currency !== "UZS");
    onDraft(editRow(draft, article, text));
    setApplied((done) => [...done, point.text]);
  };
  const added = Object.entries(draft.added);
  const addedIn = (group: string) =>
    added.filter(([, row]) => row.group === group);
  const offers = (group: string, keep: string | null = null) =>
    plan.addable.filter((item) => {
      const key = pairKey(item.subcategory, item.currency);
      return item.group === group && (key === keep || !(key in draft.added));
    });
  const incomeOffers = plan.addable.filter(
    (item) =>
      item.kind === "income" &&
      !(pairKey(item.subcategory, item.currency) in draft.added),
  );
  const incomeAdded = added.filter(([, row]) => row.kind === "income");
  const adding = editing?.kind === "add" ? editing : null;
  const uzsOf = (row: AddedRow) =>
    amountValue(row.text) * (plan.rates[row.currency] ?? 0);
  const addedLine = ([key, row]: [string, AddedRow]) => (
    <AddedLine
      key={key}
      row={row}
      onPick={() => setEditing({ kind: "add", group: row.group, key })}
    />
  );
  const addButton = (group: string) => (
    <button
      type="button"
      className="py-2 text-left text-line font-extrabold text-link"
      onClick={() => setEditing({ kind: "add", group, key: null })}
    >
      ＋ статья
    </button>
  );

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
      {error && (
        <div
          className="flex flex-col gap-1 rounded-2xl bg-section p-4"
          role="alert"
        >
          <Warn>План не обновился</Warn>
          <Line>{error}</Line>
          <button
            type="button"
            className="self-start text-line font-extrabold text-link"
            onClick={() => {
              reload();
            }}
          >
            Повторить
          </button>
        </div>
      )}
      {restored && count > 0 && (
        <div
          className="flex flex-col gap-1 rounded-2xl bg-section p-4"
          role="status"
        >
          <Fact>Несохранённые правки</Fact>
          <Line>остались с прошлого раза</Line>
          <div className="mt-1 flex justify-between gap-4">
            <button
              type="button"
              className="text-line font-extrabold text-link"
              onClick={onRestoredSeen}
            >
              Продолжить
            </button>
            <button
              type="button"
              className="text-line font-extrabold text-danger"
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
      {notice && <Notice text={notice} />}
      <Card label="Итог месяца">
        <Big>{`${money(totals.free)} сум`}</Big>
        {totals.free >= 0 ? (
          <Line>{`свободно ${monthIn(month)}`}</Line>
        ) : (
          <Warn>{`не хватает ${monthIn(month)}`}</Warn>
        )}
        <Table
          align="lr"
          rows={[
            ["поступления", money(totals.incomes)],
            ["расходы", minus(totals.spend)],
            totals.goal >= 0
              ? ["🎯 отложить", minus(totals.goal)]
              : ["💰 из накоплений", `+${money(-totals.goal)}`],
          ]}
        />
      </Card>
      {plan.events === null ? (
        <div className="flex flex-col">
          <Line>списка поступлений во вкладке нет</Line>
          <Line>его правят в таблице</Line>
        </div>
      ) : (
        <section
          className="flex flex-col gap-2"
          aria-label="Поступления и платежи"
        >
          <h3 className="text-fact font-extrabold">Поступления и платежи</h3>
          <PlanEvents
            rows={filled.map((item) => {
              const edited = draft.events[String(item.slot)] !== undefined;
              const original = plan.events?.find((e) => e.slot === item.slot);
              return {
                slot: item.slot,
                name: item.name ?? "",
                day: item.day,
                amount: item.amount ?? 0,
                currency: item.currency ?? "UZS",
                edited,
                conflict: conflicts.includes(`event:${item.slot}`),
                note:
                  !edited && original
                    ? receiptHint(original, plan.month)
                    : null,
              };
            })}
            onPick={(slot) => setEditing({ kind: "event", slot })}
          />
          {empty && (
            <button
              type="button"
              className="self-start py-1 text-left text-line font-extrabold text-link"
              onClick={() => setEditing({ kind: "event", slot: empty.slot })}
            >
              ＋ Добавить поступление или платёж
            </button>
          )}
        </section>
      )}
      <button
        type="button"
        disabled={advice?.kind === "loading"}
        className="h-12 rounded-xl bg-section text-lg font-extrabold text-link disabled:opacity-50"
        onClick={ask}
      >
        🧠 Совет по плану
      </button>
      {advice && (
        <AdvicePanel
          state={advice}
          applyLabel={applyLabel}
          applied={applied}
          onApply={applyPoint}
          onClose={() => setAdvice(null)}
        />
      )}
      {spend.map(([group, rows], index) => {
        const color = groupColor(group, index);
        const sum =
          rows.reduce(
            (total, row) =>
              total + rowPlan(draft, row) * (plan.rates[row.currency] ?? 0),
            0,
          ) + addedIn(group).reduce((total, [, row]) => total + uzsOf(row), 0);
        return (
          <section
            key={group}
            aria-label={group}
            className="rounded-3xl border px-3 pt-3 pb-1"
            style={{
              background: tint(color, 0.12),
              borderColor: tint(color, 0.28),
            }}
          >
            <div className="flex flex-wrap items-baseline justify-between gap-x-3 pb-1">
              <h3 className="text-fact font-extrabold">{group}</h3>
              <span className="ml-auto text-fact font-extrabold whitespace-nowrap tabular-nums">
                {money(sum)}
              </span>
            </div>
            {rows.map(line)}
            {addedIn(group).map(addedLine)}
            {offers(group).length > 0 && addButton(group)}
          </section>
        );
      })}
      {(income.length > 0 ||
        incomeAdded.length > 0 ||
        incomeOffers.length > 0) && (
        <section
          className="flex flex-col rounded-3xl bg-section px-3 pt-3 pb-1"
          aria-label="Доходы по статьям"
        >
          <h3 className="pb-1 text-fact font-extrabold">Доходы по статьям</h3>
          {income.map(line)}
          {incomeAdded.map(addedLine)}
          {incomeOffers.length > 0 && addButton(incomeOffers[0]?.group ?? "")}
          {differs && (
            <div className="flex flex-col py-2">
              <Warn>Доходы не сходятся</Warn>
              <Line>{`по статьям ${money(totals.incomeRows)}`}</Line>
              <Line>{`в поступлениях ${money(totals.incomes)}`}</Line>
            </div>
          )}
        </section>
      )}
      {goal && (
        <button
          type="button"
          data-conflict={conflicts.includes("goal") || undefined}
          className={`flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1 rounded-2xl bg-section p-4 text-left ${conflicts.includes("goal") ? "ring-2 ring-warn" : ""}`}
          onClick={() => setEditing({ kind: "goal" })}
        >
          <span className="text-fact font-extrabold">
            {goal.amount < 0 ? "💰 Из накоплений" : "🎯 Отложить за месяц"}
          </span>
          <span
            className={`ml-auto text-fact font-black whitespace-nowrap tabular-nums ${draft.goal ? "text-link" : ""}`}
          >
            {moneyText(Math.abs(goal.amount), goal.currency)}
          </span>
        </button>
      )}
      {goal && plan.goal && plan.goal_hint && (
        <GoalHint
          hint={plan.goal_hint}
          shown={goalInSums(goal, plan.rates)}
          onApply={(suggested) => {
            if (plan.goal) onDraft(applyGoal(draft, plan.goal, suggested));
          }}
        />
      )}
      <PiggyHints plan={plan} draft={draft} onDraft={onDraft} />
      {!tg && count > 0 && (
        <button
          type="button"
          className="h-12 rounded-xl bg-button text-lg font-extrabold text-button-text disabled:opacity-50"
          disabled={saving}
          onClick={save}
        >
          {label}
        </button>
      )}
      {adding && (
        <AddArticleSheet
          group={adding.group}
          items={offers(adding.group, adding.key)}
          history={plan.history}
          initial={
            adding.key
              ? { key: adding.key, text: draft.added[adding.key]?.text ?? "" }
              : null
          }
          onDone={(item, text) => {
            const base = adding.key ? removeRow(draft, adding.key) : draft;
            onDraft(addRow(base, item, text));
            close();
          }}
          onRemove={
            adding.key
              ? () => {
                  onDraft(removeRow(draft, adding.key ?? ""));
                  close();
                }
              : null
          }
          onClose={close}
        />
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
