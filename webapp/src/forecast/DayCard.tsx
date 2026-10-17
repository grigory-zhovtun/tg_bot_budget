import type { Forecast, ForecastPast } from "../api";
import { longDay } from "../format";
import { REPEAT_NAMES, dayRecords, groupTitle, ruleText } from "./records";
import {
  amount,
  articleText,
  dayArticles,
  dayEvents,
  dayOf,
  dayTitle,
  isoDay,
  ordinary,
  signed,
  viewCode,
  viewDays,
  viewPast,
  type ShownArticle,
  type View,
} from "./view";

interface Props {
  data: Forecast;
  view: View;
  day: number;
  onAdd?: (day: number) => void; // нет — справочник не загрузился, записи только смотреть
  onEdit?: (id: string) => void;
}

interface ArticlesProps {
  label: string; // для скринридера: «Траты по статьям»
  articles: ShownArticle[];
}

/**
 * Статьи дня столбиком: значок и подкатегория, справа сумма в своей валюте.
 * Подкатегория в двух группах за день — с группой, иначе строк не различить.
 */
function Articles({ label, articles }: ArticlesProps) {
  if (articles.length === 0) return null;
  const seen = articles.map((article) => article.subcategory);
  const twice = new Set(
    seen.filter((name, index) => seen.indexOf(name) !== index),
  );
  return (
    <ul className="flex flex-col pl-3" aria-label={label}>
      {articles.map((article) => (
        <li
          key={`${article.currency}-${article.group}-${article.subcategory}`}
          className="flex justify-between gap-2 py-0.5"
        >
          <span className="flex items-center gap-2">
            {article.icon && <span aria-hidden="true">{article.icon}</span>}
            <span>
              {twice.has(article.subcategory)
                ? `${article.subcategory} · ${groupTitle(article.group)}`
                : article.subcategory}
            </span>
          </span>
          <span className="font-semibold whitespace-nowrap">
            {articleText(article)}
          </span>
        </li>
      ))}
    </ul>
  );
}

interface FactsProps {
  data: Forecast;
  view: View;
  point: ForecastPast;
  today: boolean;
}

/**
 * Траты и доходы дня: итог — в числах вида, по статьям — каждая в своей валюте.
 * Без трат и доходов — переводы между своими картами и выравнивание, если были.
 */
function DayFacts({ data, view, point, today }: FactsProps) {
  const code = viewCode(view);
  const { spent, earned } = dayArticles(data, view, point.day);
  const spending = spent.length > 0 || Math.round(point.spent) !== 0;
  const income = earned.length > 0 || Math.round(point.earned) !== 0;
  if (!spending && !income) {
    if (Math.round(point.change) !== 0) {
      return (
        <p className="flex justify-between gap-2">
          <span>переводы и выравнивание</span>
          <span className="font-bold">{signed(point.change, code)}</span>
        </p>
      );
    }
    return today ? null : <p>движений не было</p>;
  }
  // одни возвраты: деньги вернулись — плюсом, мятным, как «пришло»
  const back = point.spent < 0;
  const spentLabel = back
    ? "вернули"
    : today
      ? "потрачено сегодня"
      : "потрачено";
  return (
    <>
      {spending && (
        <>
          <p className="flex justify-between gap-2">
            <span>{spentLabel}</span>
            <span
              className={`font-bold ${back ? "text-earned" : "text-spent"}`}
            >
              {back
                ? `+${amount(-point.spent, code)}`
                : `−${amount(point.spent, code)}`}
            </span>
          </p>
          <Articles label="Траты по статьям" articles={spent} />
        </>
      )}
      {income && (
        <>
          <p className="flex justify-between gap-2">
            <span>{today ? "пришло сегодня" : "пришло"}</span>
            <span className="font-bold text-earned">
              {`+${amount(point.earned, code)}`}
            </span>
          </p>
          <Articles label="Доходы по статьям" articles={earned} />
        </>
      )}
    </>
  );
}

/** Прошедший день по факту: сколько было вечером, траты и доходы дня по статьям. */
function PastCard({ data, view, day }: Pick<Props, "data" | "view" | "day">) {
  const point = viewPast(data, view).find((item) => item.day === day);
  if (!point) return null;
  const code = viewCode(view);
  return (
    <section className="flex flex-col gap-1 rounded-2xl bg-section p-4">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">{dayTitle(isoDay(data.today, day))}</h2>
        <span className="text-sm font-semibold">по факту</span>
      </div>
      <p className="flex justify-between gap-2">
        <span>вечером было</span>
        <span
          className={`font-bold ${
            point.evening < 0 ? "rounded-md bg-minus px-1.5 text-white" : ""
          }`}
        >
          {amount(point.evening, code)}
        </span>
      </p>
      <DayFacts data={data} view={view} point={point} today={false} />
    </section>
  );
}

/**
 * Выбранный день: остаток на вечер по плану и по темпу; сегодня — что уже
 * потрачено и пришло по статьям; события, обычные траты, записи.
 */
export default function DayCard({ data, view, day, onAdd, onEdit }: Props) {
  const today = dayOf(data.today);
  if (day < today) {
    return <PastCard data={data} view={view} day={day} />;
  }
  const point = viewDays(data, view).find((item) => item.day === day);
  if (!point) return null;
  const code = viewCode(view);
  const fact =
    day === today
      ? viewPast(data, view).find((item) => item.day === day)
      : undefined;
  const usual = ordinary(data, view, day);
  const records = dayRecords(data, view, day);
  const iso = isoDay(data.today, day);
  return (
    <section className="flex flex-col gap-1 rounded-2xl bg-section p-4">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">{dayTitle(iso)}</h2>
        <span className="text-xs text-hint">вечером</span>
      </div>
      <p className="flex justify-between gap-2">
        <span>
          <span className="text-link">━</span> по плану
        </span>
        <span
          className={`font-semibold ${point.plan < 0 ? "text-danger" : ""}`}
        >
          {amount(point.plan, code)}
        </span>
      </p>
      <p className="flex justify-between gap-2">
        <span>
          <span className="text-warn">╍</span> по темпу
        </span>
        <span
          className={`font-semibold ${point.pace < 0 ? "text-danger" : ""}`}
        >
          {amount(point.pace, code)}
        </span>
      </p>
      {fact && (
        <div className="mt-2 flex flex-col gap-1">
          <DayFacts data={data} view={view} point={fact} today />
        </div>
      )}
      <ul className="mt-1 flex flex-col gap-0.5 text-sm">
        {dayEvents(data, view, day).map((event, index) => (
          <li
            key={`${event.currency}-${event.name}-${index}`}
            className="flex justify-between gap-2"
          >
            <span className="flex gap-1">
              {event.late && <span>⏳</span>}
              <span
                className={`font-bold ${event.amount > 0 ? "text-earned" : "text-payment"}`}
              >
                {event.amount > 0 ? "▲" : "▼"}
              </span>
              <span>{event.name}</span>
            </span>
            <span
              className={`font-semibold ${event.amount > 0 ? "text-earned" : ""}`}
            >
              {signed(event.amount, event.currency)}
            </span>
          </li>
        ))}
      </ul>
      {usual && <p className="text-sm text-hint">{usual}</p>}
      {records.length > 0 && (
        <div className="mt-2 flex flex-col">
          <h3 className="text-sm font-semibold text-hint">
            Запланировано на день
          </h3>
          <ul className="flex flex-col">
            {records.map((record) => {
              const rule = data.calendar.find((item) => item.id === record.id);
              // закрыта фактом: «потрачено» — сколько на самом деле, а не по плану
              const spent = record.spent > 0 ? record.spent : record.amount;
              const differs =
                record.done &&
                record.spent > 0 &&
                Math.round(record.spent) !== Math.round(record.amount);
              return (
                <li key={`${record.currency}-${record.id}`}>
                  <button
                    type="button"
                    className="flex w-full items-start gap-2 py-1.5 text-left text-sm disabled:opacity-100"
                    disabled={!onEdit}
                    onClick={() => onEdit?.(record.id)}
                  >
                    <span
                      className={
                        record.repeat === "once"
                          ? "font-bold text-planned"
                          : "font-bold text-link"
                      }
                      aria-hidden="true"
                    >
                      {record.repeat === "once" ? "▼" : "↻"}
                    </span>
                    <span className="flex flex-1 flex-col">
                      <span className="flex flex-wrap items-center gap-1">
                        {record.name}
                        {record.over && (
                          <span className="rounded-md bg-minus px-1.5 text-xs font-semibold text-white">
                            сверх плана
                          </span>
                        )}
                      </span>
                      <span className="text-xs text-hint">
                        {`${groupTitle(record.group)} / ${record.subcategory} · ${
                          rule
                            ? ruleText(rule.repeat, rule.weekdays, rule.date)
                            : REPEAT_NAMES[record.repeat].toLowerCase()
                        }${
                          differs
                            ? ` · по плану ${amount(record.amount, record.currency)}`
                            : ""
                        }`}
                      </span>
                    </span>
                    <span
                      className={
                        record.done
                          ? "text-sm font-semibold text-hint"
                          : "font-bold whitespace-nowrap text-planned"
                      }
                    >
                      {record.done
                        ? `✓ потрачено ${amount(spent, record.currency)}`
                        : signed(-record.open, record.currency)}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
      )}
      {onAdd && (
        <button
          type="button"
          className="mt-2 rounded-xl border border-dashed border-link/50 py-2 font-medium text-link"
          onClick={() => onAdd(day)}
        >
          {`＋ Запланировать на ${longDay(iso)}`}
        </button>
      )}
    </section>
  );
}
