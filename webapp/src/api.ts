/** Клиент API бота: заголовок авторизации, типы ответов, ошибки. */

export interface Source {
  name: string;
  currency: string;
}

export interface Subcategory {
  name: string;
  icon: string;
}

export interface Group {
  name: string;
  emoji: string;
  title: string;
  subcategories: Subcategory[];
}

export interface Bootstrap {
  user: { id: number; first_name: string };
  bot_username: string;
  today: string;
  utc_offset?: number; // минут к востоку от UTC у бота; нет — справочник из старой копии
  default_source: string | null;
  sources: Source[];
  groups: Group[];
}

export interface ExpenseInput {
  entry_id: string;
  source: string;
  category: string;
  subcategory: string;
  amount: string;
  comment: string;
  day?: string; // нет — сегодня по часам бота
}

export interface ExpenseResult {
  status: "written" | "duplicate";
  rows: { first: number; last: number };
  lines: string[];
}

export interface UndoResult {
  message: string;
}

export interface PlanItem {
  name: string;
  icon: string;
  plan: number;
  fact: number;
}

export interface GroupPlan {
  name: string;
  plan: number;
  fact: number;
  items: PlanItem[];
}

/** План-факт группы: план — вкладка месяца, факт — строки fact с 1-го по сегодня. */
export interface Category {
  name: string;
  plan: number; // в сумах
  fact: number; // в сумах
  items: { name: string; icon: string; plan: number; fact: number }[];
}

/** Траты статьи без строки во вкладке месяца — в план-факт они не попали. */
export interface Untracked {
  group: string;
  subcategory: string;
  currency: string;
  amount: number; // в сумах
}

/** Остаток карты по таблице и сверка с банком. */
export interface CardBalance {
  name: string; // без валюты — она у суммы: «VISA 1111»
  currency: string;
  balance: number | null; // «Текущий баланс»; null — ошибка в ячейке
  uzs: number | null; // null — нет курса или остатка
  bank: number | null; // остаток банка при последней сверке
  checked: string | null; // когда сверяли, ISO-дата
  matches: boolean | null; // null — сверять не с чем
  stale: boolean; // сверке больше недели
}

/** Ступень подушки: 1, 3 месяца или цель — сколько ещё до неё. */
export interface CushionStep {
  months: number;
  short: number; // в валюте подушки, вверх до целых
  short_uzs: number; // сумы целыми
}

/** Подушка безопасности: «Заморожено» без копилок на сейчас в месяцах жизни. */
export interface Cushion {
  amount: number; // в валюте подушки
  currency: string;
  uzs: number;
  monthly: number; // месяц жизни: средние траты обязательных групп, сумы
  months: number | null; // месяцев подушки, один знак вниз; null — трат ещё нет
  target_months: number;
  short_uzs: number; // сколько не хватает до цели; 0 — цель достигнута
  short: number; // то же в валюте подушки
  frozen: number | null; // «Заморожено» на сейчас
  piggies: number; // остатки копилок на сейчас, в валюте подушки
  over: number; // на сколько копилки больше «Заморожено»
  card: number; // всего на картах в валюте подушки
  payments: FrozenPayment[]; // платежи срока в этой валюте
  lacking: number; // копилок нет, а долларов на платежи не хватает
  lacking_on: string | null; // день платежа, с которого не хватает
  life_months: string[]; // месяцы месяца жизни: «2026-07»
  life_groups: string[]; // обязательные группы справочника
  steps: CushionStep[]; // ступени, до которых ещё не дошли
}

/** Полный месяц: доход без «нач остаток» и траты без переводов, в сумах. */
export interface SavingMonth {
  month: string; // «2026-09»
  income: number;
  spent: number;
  rate: number | null; // доля дохода, которая осталась; null — дохода не было
}

export interface Savings {
  rate: number | null; // за 3 последних полных месяца
  months: SavingMonth[]; // 6 полных месяцев, от старых
}

/** День месяца: траты без платежей из списка и лимит, с которым день начался. */
export interface DaySpend {
  day: string;
  spent: number;
  limit: number;
}

export interface ForecastItem {
  name: string;
  day: number;
  amount: number;
  currency: string;
  uzs: number;
}

export type SubscriptionState = "charged" | "twice" | "expected" | "missed";

export interface Subscription {
  name: string;
  day: number;
  amount: number;
  currency: string;
  uzs: number;
  state: SubscriptionState;
}

/** «Сумы» до зарплаты: когда кончатся и сколько долларов обменять до неё. */
export interface Runway {
  balance: number; // сумы сейчас
  pace: number;
  last_day: string | null; // последний день, когда сумы ещё не в минусе
  short: number; // сколько сумов не хватит в самой низкой точке; 0 — хватает
  until: string | null; // день срока, если не хватает; null — конец месяца
  dollars: number | null; // сколько долларов обменять; null — нет курса
  left?: number | null; // останется вечером в день срока; нет — старый ответ
  payday?: string | null; // день срока; null — конец месяца
  target?: string | null; // «до зарплаты 30-го», «до зарплаты сегодня»
}

/** Платёж срока в валюте заморозки: день — ISO-дата (в следующем месяце — его
 * день там), сумма плюсом. */
export interface FrozenPayment {
  day: string;
  name: string;
  amount: number;
}

/** «🧊 Заморожено»: доллары на сейчас без платежей срока — подушка и копилки. */
export interface Frozen {
  amount: number;
  currency: string;
  uzs: number;
  change: number; // всегда 0: «с 1-го числа» убрана
  total: number; // всего на картах в этой валюте
  payments: FrozenPayment[];
  piggies: number | null; // копилки на сейчас; null — их не читали
  cushion: number | null; // заморожено без копилок; null — копилки не читали
  over: number; // копилки больше карты на столько
  lacking: number; // копилок нет, а долларов на платежи не хватает
  lacking_on: string | null; // день платежа, с которого не хватает
  target: string; // срок платежей, как у «Сумы»: «до зарплаты 30 ноября»
}

/** Лимит с завтра в день прихода зарплаты: сумы сейчас, срок с завтра. */
export interface Tomorrow {
  limit: number;
  days: number;
  payday: string | null;
  until: string; // «до зарплаты 30 ноября»
  sums: number;
  bills: number;
  advances: number;
  advance_on: string | null;
}

export interface Dashboard {
  status: "ok" | "no_month_tab" | "no_forecast";
  month: string;
  today: string;
  limit: number | null;
  spent_today: number | null;
  left_today: number | null;
  plan_per_day: number | null;
  days_left: number | null;
  balance: number | null;
  sums: number | null; // сумы на вечер вчера: из них лимит до зарплаты
  bills: number | null; // платежи в сумах до дня перед зарплатой
  until: string | null; // «до зарплаты 30-го», «до аванса 15 ноября», «до конца месяца»
  payday_on: string | null; // «30-го», «15 ноября», «конца месяца»
  payday: string | null; // день, по который лимит; null — конец месяца
  advances: number | null; // авансы срока: прибавлены к деньгам лимита
  advance_on: string | null; // первый из них; null — авансов в сроке нет
  tomorrow?: Tomorrow | null; // день прихода зарплаты: лимит с завтра; иначе null
  salary_came?: number | null; // сколько пришло по строке зарплаты в этот день
  came?: string | null; // «Пришла ЗП» или «Пришёл аванс»; нет — «Пришла ЗП»
  frozen: Frozen | null;
  spending: DaySpend[]; // с 1-го числа по сегодня
  upcoming: ForecastItem[];
  subscriptions: Subscription[];
  runway: Runway | null;
  groups: GroupPlan[]; // план-факт вкладки: остаток по плану на плитках «Ввода»
  untracked: Untracked[];
  cards: CardBalance[];
  categories: Category[]; // план-факт по категориям, от больших трат
}

/** Копилка «Накоплений»; суммы — в её валюте. */
export type PiggyStatus =
  | "on_track"
  | "late"
  | "short"
  | "no_target"
  | "done"
  | "leftover"
  | "goal"
  | "broken";

export interface AmountItem {
  id: string; // копилка
  amount: number; // сумы
}

export interface PiggyMove {
  day: string;
  amount: number; // плюс — отложили, минус — забрали
  kind: string;
  who: string;
}

export interface PiggySpend {
  key: string; // строка fact — для «Не из копилки»
  day: string;
  group: string;
  subcategory: string;
  amount: number;
}

export interface Piggy {
  id: string;
  row: number; // строка листа «Копилки»
  name: string;
  target: number | null;
  currency: string;
  deadline: string | null;
  repeat: "once" | "yearly";
  group: string | null;
  subcategory: string | null;
  author: string;
  saved: number;
  spent: number;
  balance: number;
  collected: number; // к ближайшему сроку: остаток и уже потраченное из него
  this_month: number;
  left_this_month: number;
  status: PiggyStatus;
  short: number;
  leftover: number;
  error: string | null;
  order: number | null; // номер важности; null — не в порядке
  forecast: number | null; // будет к ближайшему сроку по плану
  now: number; // отложить сейчас
  saved_month: number; // отложено в этом месяце
  history: PiggyMove[];
  spends: PiggySpend[];
}

/** Месяц плана взносов, сумы. */
export interface PiggyPlanMonth {
  month: string; // «2026-10»
  total: number; // копилки со сроком
  items: AmountItem[];
  cushion: number; // доля процента подушке
  goals: AmountItem[]; // доля 10 % долгим целям
  ready: string[]; // копилки, собранные к сроку в этом месяце
}

/** Текущий месяц: процент с поступлений и сколько отложить, сумы. */
export interface SavingsMonth {
  month: string;
  percent: number; // проценты: 5
  percent_source: "sheet" | "default" | "bad" | "preview";
  percent_needed: number | null; // целые проценты «успеть всё»
  capacity: number; // процент от поступлений плана месяца
  received: number; // пришло с 1-го по сегодня
  due: number; // процент от пришедшего
  left: number; // отложить сейчас — копилкам и долгим целям
  cushion: number; // часть подушке
  saved: number; // отложено копилкам с 1-го числа
  cushion_saved: number; // «Отложено подушке»: её настоящий рост с 1-го числа
  cushion_room: number; // подушке ещё не отложено из её части; 0 — сначала ничего
}

/** Предпросмотр окон «Процент» и «Порядок»: ничего не пишется. */
export interface SavingsPreview {
  percent?: number;
  order?: string[];
}

export interface Enough {
  income: number;
  save: number;
  life: number;
  ordinary: number;
  gap: number; // меньше нуля — не хватает
}

/** «🎯 Отложить за месяц» по копилкам и что сейчас во вкладке, сумы. */
export interface SavingsGoal {
  suggested: number;
  in_plan: number | null; // null — во вкладке нет строки цели
  piggies: number;
  share: number;
  spent: number; // траты из копилок в месяце
}

export interface SavingsScreen {
  status: "ok" | "no_month_tab";
  today: string;
  month: SavingsMonth | null;
  goal?: SavingsGoal | null; // нет — открытых копилок нет
  cushion: Cushion | null;
  piggies: Piggy[];
  plan: PiggyPlanMonth[];
  enough: Enough | null;
  savings: Savings | null;
  rates: Record<string, number>;
}

export interface PiggyInput {
  entry_id?: string;
  name: string;
  target: string | null; // строкой, как сумма траты
  currency: string;
  deadline: string | null;
  repeat: "once" | "yearly";
  group: string | null;
  subcategory: string | null;
}

export interface MoveInput {
  entry_id: string;
  kind: "saved" | "spent" | "write_off";
  amount: string;
}

export interface SaveInput {
  entry_id: string;
  amount: string;
  currency: string;
}

/** Строка списка, которая ещё впереди: сумма — остаток, в валюте строки. */
export interface ForecastEvent {
  day: number;
  name: string;
  amount: number;
  late: boolean; // день строки прошёл — ждём сегодня
}

/** Сколько будет вечером дня: по плану и по темпу. */
export interface ForecastDay {
  day: number;
  plan: number;
  pace: number;
}

/** Статья дня по факту: траты (возврат — минусом) или доход, в валюте строк. */
export interface DayArticle {
  group: string;
  subcategory: string;
  icon: string;
  amount: number;
  uzs: number; // в сумах по курсу таблицы; 0 — курса нет
  income: boolean;
}

/** День по факту: сколько было вечером и изменение за день (доход плюсом). */
export interface ForecastPast {
  day: number;
  evening: number; // сегодня — деньги сейчас
  change: number;
  spent: number; // траты дня по статьям расходов
  earned: number; // доходы дня
  articles: DayArticle[]; // по статьям: траты от больших, доходы; у «Всего» — пусто
}

/** Уход в минус: первый день ниже нуля и когда остаток вернётся. */
export interface ForecastDip {
  start: number;
  until: number | null; // null — до конца месяца не вернётся
}

export interface ForecastDips {
  plan: ForecastDip | null;
  pace: ForecastDip | null;
}

export type Repeat = "once" | "daily" | "weekdays" | "monthly";

/** Вхождение записи календаря в день: open — что ещё впереди, в валюте записи. */
export interface ForecastRecord {
  id: string;
  day: number;
  name: string; // название записи или подкатегория
  group: string;
  subcategory: string;
  repeat: Repeat;
  amount: number;
  open: number;
  done: boolean; // закрыта фактом: open 0
  over: boolean; // записей статьи больше её остатка
  spent: number; // потрачено на самом деле: закрывший платёж или доля трат статьи
}

export interface ForecastCurrency {
  code: string;
  now: number;
  per_day_plan: number;
  per_day_pace: number;
  spent_today: number;
  in_total: boolean; // false — нет курса, во «Всего» не входит
  events: ForecastEvent[];
  days: ForecastDay[]; // с сегодня по последний день месяца
  below_zero: ForecastDips;
  records: ForecastRecord[];
  past: ForecastPast[]; // с 1-го по сегодня по факту: сегодня — что уже было
}

/** Запись для списка и окна: повтор или разовая с сегодня. */
export interface CalendarRecord {
  id: string;
  date: string; // день разовой записи или первый день повтора
  repeat: Repeat;
  weekdays: number[]; // ISO: пн = 1 … вс = 7
  group: string;
  subcategory: string;
  currency: string;
  amount: number;
  name: string; // пусто — показывается подкатегория
  author: string;
  month_total: number; // открыто с сегодня до конца месяца
  booked: number; // её доля в booked статьи
  over: boolean;
}

/** Статья трат: rest — план минус факт по вчера, booked — записи на утро. */
export interface ForecastArticle {
  group: string;
  subcategory: string;
  currency: string;
  plan: number;
  rest: number;
  booked: number;
}

/** Подсказка суммы из истории статьи; null — операций нет. */
export interface AmountHint {
  typical: number | null;
  last: number | null;
  per_day: number | null;
}

export interface Forecast {
  status: "ok" | "no_month_tab";
  month: string;
  today: string;
  end: string | null;
  currencies: ForecastCurrency[];
  total: {
    uzs: ForecastDay[];
    usd: ForecastDay[]; // пусто — нет курса доллара
    below_zero: ForecastDips;
    past_uzs: ForecastPast[];
    past_usd: ForecastPast[];
  } | null;
  rates: Record<string, number>; // к суму, без UZS
  calendar: CalendarRecord[];
  articles: ForecastArticle[];
  hints: Record<string, AmountHint>; // «подкатегория|валюта»
}

/** Запись из окна: сумма строкой, как у траты; entry_id — только у новой. */
export interface RecordInput {
  entry_id?: string;
  date: string;
  repeat: Repeat;
  weekdays: number[];
  group: string;
  subcategory: string;
  currency: string;
  amount: string;
  name: string;
}

export interface PlanArticle {
  key: string;
  kind: "spend" | "income";
  group: string;
  subcategory: string;
  icon: string;
  currency: string;
  plan: number;
  average: number;
  last: number;
  current: number;
  editable: boolean;
}

export interface PlanEvent {
  slot: number;
  name: string | null;
  day: number | null;
  amount: number | null; // со знаком: «+» поступление, «−» платёж
  currency: string | null;
  received?: number | null; // пришло по строке: в её валюте и со знаком
  received_on?: string | null; // «YYYY-MM-DD» последней операции
  status?: "done" | "partial" | "waiting" | "future";
}

export interface PlanGoal {
  amount: number;
  currency: string;
}

export interface PlanHistory {
  average: string[];
  last: string;
  current: string;
  current_days: number;
}

/** Пара справочника, которой нет во вкладке, — для «＋ статья». */
export interface PlanAddable {
  kind: "spend" | "income";
  group: string;
  subcategory: string;
  icon: string;
  currency: string;
  average: number;
  last: number;
  current: number;
}

/** «Отложить за месяц» по копилкам для месяца «Плана», сумы. */
export interface PlanGoalHint {
  suggested: number;
  piggies: number;
  share: number;
  spent: number; // траты из копилок в месяце
  percent: number; // процент с поступлений
}

/** Трата из копилки в месяце «Плана»: заложить её в план статьи. */
export interface PlanPiggyHint {
  piggy: string;
  month: string;
  group: string;
  subcategory: string;
  currency: string; // валюта копилки — и статьи
  amount: number;
  need: number; // сколько не хватает плану статьи во вкладке
  key: string | null; // статья вкладки; null — строки нет
}

export interface MonthPlan {
  month: string;
  title: string;
  status: "ok" | "not_created" | "no_month_tab";
  today: string;
  rates: Record<string, number>;
  history: PlanHistory | null;
  rows: PlanArticle[];
  events: PlanEvent[] | null;
  goal: PlanGoal | null;
  addable: PlanAddable[];
  saved: number | null;
  goal_hint?: PlanGoalHint | null; // нет — открытых копилок нет
  piggy_hints?: PlanPiggyHint[];
}

export interface EventValue {
  name: string;
  day: number;
  amount: string; // «1000000», «-380.5»
  currency: string;
}

export interface PlanSave {
  month: string;
  rows: { key: string; base: number; plan: string }[];
  events: {
    slot: number;
    base: Omit<PlanEvent, "slot">;
    value: EventValue | null;
  }[];
  goal: {
    base: PlanGoal;
    value: { amount: string; currency: string };
  } | null;
  new_rows: {
    group: string;
    subcategory: string;
    currency: string;
    plan: string;
  }[];
}

export interface PlanConflict {
  plan: MonthPlan;
  conflicts: string[];
}

/** Пункт совета; apply — новая сумма статьи для «Применить». */
export interface AdvicePoint {
  text: string;
  apply: { key: string; plan: number } | null;
}

/** Черновик для советника: значения, как на экране, без того, что было. */
export interface PlanAdviceRequest {
  month: string;
  rows: { key: string; plan: string }[];
  new_rows: PlanSave["new_rows"];
  events: { slot: number; value: EventValue | null }[];
  goal: { amount: string; currency: string } | null;
}

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly fields: Record<string, string>;
  readonly body: unknown; // всё тело ответа — у 409 там свежий план

  constructor(
    status: number,
    code: string,
    message: string,
    fields: Record<string, string> = {},
    body: unknown = null,
  ) {
    super(message);
    this.status = status;
    this.code = code;
    this.fields = fields;
    this.body = body;
  }
}

/** «tma <initData>», если Telegram передал подпись, иначе токен из ссылки кнопки. */
export function authHeader(initData: string, search: string): string | null {
  if (initData) return `tma ${initData}`;
  const launch = new URLSearchParams(search).get("launch");
  return launch ? `Launch ${launch}` : null;
}

/** id пользователя из ссылки кнопки «📱 Приложение» («<id>.<время>.<подпись>»).
 * Без проверки подписи — только чтобы не показать чужую копию данных. */
export function launchUser(search: string): number | null {
  const id = new URLSearchParams(search).get("launch")?.split(".")[0] ?? "";
  return /^\d+$/.test(id) ? Number(id) : null;
}

interface ErrorBody {
  error: { code: string; message: string; fields?: Record<string, string> };
}

function isErrorBody(body: unknown): body is ErrorBody {
  if (typeof body !== "object" || body === null || !("error" in body)) {
    return false;
  }
  const error: unknown = body.error;
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    "message" in error
  );
}

// Таблица Google иногда думает долго, но не бесконечно ждём ответа
export const REQUEST_TIMEOUT_MS = 25_000;
// Советник: сервер ждёт модель до 60 с, ещё время — на чтение таблицы
export const ADVICE_TIMEOUT_MS = 100_000;

export async function request<T>(
  path: string,
  init: RequestInit = {},
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<T> {
  const auth = authHeader(
    window.Telegram?.WebApp?.initData ?? "",
    window.location.search,
  );
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (auth) headers.Authorization = auth;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let response: Response;
  try {
    response = await fetch(`/api${path}`, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch {
    throw controller.signal.aborted
      ? new ApiError(
          0,
          "timeout",
          "Сервер долго не отвечает — попробуйте ещё раз",
        )
      : new ApiError(0, "network", "Нет связи с сервером — проверьте интернет");
  } finally {
    clearTimeout(timer);
  }
  const body: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    if (isErrorBody(body)) {
      const { code, message, fields } = body.error;
      throw new ApiError(response.status, code, message, fields ?? {}, body);
    }
    throw new ApiError(
      response.status,
      "server",
      "Сервер не ответил — попробуйте ещё раз",
    );
  }
  return body as T;
}

/** 409 сохранения плана: свежий план и что поменялось после открытия. */
export function planConflict(error: unknown): PlanConflict | null {
  if (!(error instanceof ApiError) || error.code !== "plan_changed")
    return null;
  const body = error.body;
  if (
    typeof body !== "object" ||
    body === null ||
    !("plan" in body) ||
    !("conflicts" in body)
  ) {
    return null;
  }
  return {
    plan: body.plan as MonthPlan,
    conflicts: body.conflicts as string[],
  };
}

/** «?fresh=1» — сервер читает таблицу заново, а не берёт копию листов. */
function fresh(read: boolean, joiner = "?"): string {
  return read ? `${joiner}fresh=1` : "";
}

export const api = {
  bootstrap: () => request<Bootstrap>("/bootstrap"),
  addExpense: (entry: ExpenseInput) =>
    request<ExpenseResult>("/expenses", {
      method: "POST",
      body: JSON.stringify(entry),
    }),
  undo: () => request<UndoResult>("/expenses/undo", { method: "POST" }),
  dashboard: (read = false) => request<Dashboard>(`/dashboard${fresh(read)}`),
  forecast: (read = false) => request<Forecast>(`/forecast${fresh(read)}`),
  addRecord: (input: RecordInput) =>
    request<Forecast>("/calendar", {
      method: "POST",
      body: JSON.stringify(input),
    }),
  updateRecord: (id: string, input: RecordInput) =>
    request<Forecast>(`/calendar/${encodeURIComponent(id)}`, {
      method: "PUT",
      body: JSON.stringify(input),
    }),
  deleteRecord: (id: string) =>
    request<Forecast>(`/calendar/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),
  plan: (month: string, read = false) =>
    request<MonthPlan>(
      `/plan?month=${encodeURIComponent(month)}${fresh(read, "&")}`,
    ),
  savePlan: (input: PlanSave) =>
    request<MonthPlan>("/plan", {
      method: "POST",
      body: JSON.stringify(input),
    }),
  createNextPlan: () => request<MonthPlan>("/plan/next", { method: "POST" }),
  savings: (read = false, preview?: SavingsPreview) => {
    const params = new URLSearchParams();
    if (read) params.set("fresh", "1");
    if (preview?.percent !== undefined)
      params.set("percent", String(preview.percent));
    if (preview?.order) params.set("order", preview.order.join(","));
    const query = params.toString();
    return request<SavingsScreen>(`/savings${query ? `?${query}` : ""}`);
  },
  setPercent: (percent: number) =>
    request<SavingsScreen>("/piggy/percent", {
      method: "PUT",
      body: JSON.stringify({ percent }),
    }),
  setOrder: (ids: string[]) =>
    request<SavingsScreen>("/piggy/order", {
      method: "PUT",
      body: JSON.stringify({ ids }),
    }),
  addPiggy: (input: PiggyInput) =>
    request<SavingsScreen>("/piggy", {
      method: "POST",
      body: JSON.stringify(input),
    }),
  updatePiggy: (id: string, input: PiggyInput) =>
    request<SavingsScreen>(`/piggy/${encodeURIComponent(id)}`, {
      method: "PUT",
      body: JSON.stringify(input),
    }),
  closePiggy: (id: string) =>
    request<SavingsScreen>(`/piggy/${encodeURIComponent(id)}/close`, {
      method: "POST",
    }),
  movePiggy: (id: string, input: MoveInput) =>
    request<SavingsScreen>(`/piggy/${encodeURIComponent(id)}/moves`, {
      method: "POST",
      body: JSON.stringify(input),
    }),
  savePiggies: (input: SaveInput) =>
    request<SavingsScreen>("/piggy/save", {
      method: "POST",
      body: JSON.stringify(input),
    }),
  excludeSpend: (id: string, key: string) =>
    request<SavingsScreen>(`/piggy/${encodeURIComponent(id)}/exclude`, {
      method: "POST",
      body: JSON.stringify({ key }),
    }),
  planAdvice: (input: PlanAdviceRequest) =>
    request<{ points: AdvicePoint[] }>(
      "/plan/advice",
      { method: "POST", body: JSON.stringify(input) },
      ADVICE_TIMEOUT_MS,
    ),
};

export type Api = typeof api;
