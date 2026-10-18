import type {
  AdvicePoint,
  Bootstrap,
  CalendarRecord,
  Dashboard,
  DayArticle,
  ExpenseResult,
  Forecast,
  ForecastDay,
  ForecastPast,
  ForecastRecord,
  MonthPlan,
  Piggy,
  SavingsScreen,
} from "../api";

export const bootstrapFixture: Bootstrap = {
  user: { id: 1, first_name: "Тест" },
  bot_username: "budget_test_bot",
  today: "2026-10-10",
  default_source: "VISA 9120 UZS",
  sources: [
    { name: "VISA 9120 UZS", currency: "UZS" },
    { name: "VISA 4058 USD", currency: "USD" },
  ],
  groups: [
    {
      name: "🍔 ЕДА",
      emoji: "🍔",
      title: "ЕДА",
      subcategories: [
        { name: "кофе", icon: "☕" },
        { name: "кафе", icon: "🍽️" },
      ],
    },
    {
      name: "🏚️ ДОМ",
      emoji: "🏚️",
      title: "ДОМ",
      subcategories: [{ name: "продукты", icon: "🛒" }],
    },
  ],
};

export const writtenFixture: ExpenseResult = {
  status: "written",
  rows: { first: 4169, last: 4169 },
  lines: [
    "✅ 48 000 UZS • 🍔 ЕДА (☕ кофе) • VISA 9120 UZS",
    "💸 На сегодня осталось 452 000 из 500 000",
  ],
};

export const dashboardFixture: Dashboard = {
  status: "ok",
  month: "Oct 26",
  today: "2026-10-10",
  limit: 500_000,
  spent_today: 120_000,
  left_today: 380_000,
  plan_per_day: 450_000,
  days_left: 22,
  balance: 39_000_000,
  planned_balance: 41_500_000,
  frozen: { amount: 1000, currency: "USD", uzs: 12_000_000, change: -50 },
  groups: [
    {
      name: "🍔 ЕДА",
      plan: 3_000_000,
      fact: 3_300_000,
      items: [
        { name: "кофе", icon: "☕", plan: 300_000, fact: 250_000 },
        { name: "кафе", icon: "🍽️", plan: 2_700_000, fact: 3_050_000 },
      ],
    },
    {
      name: "🏚️ ДОМ",
      plan: 2_000_000,
      fact: 500_000,
      items: [{ name: "продукты", icon: "🛒", plan: 2_000_000, fact: 500_000 }],
    },
  ],
  // лимит дня растёт, пока траты ниже плана, и падает после перерасхода
  spending: [
    { day: "2026-10-01", spent: 300_000, limit: 450_000 },
    { day: "2026-10-02", spent: 520_000, limit: 455_000 },
    { day: "2026-10-03", spent: 250_000, limit: 452_760 },
    { day: "2026-10-04", spent: 280_000, limit: 460_000 },
    { day: "2026-10-05", spent: 610_000, limit: 466_670 },
    { day: "2026-10-06", spent: 200_000, limit: 461_150 },
    { day: "2026-10-07", spent: 230_000, limit: 471_600 },
    { day: "2026-10-08", spent: 260_000, limit: 481_670 },
    { day: "2026-10-09", spent: 300_000, limit: 491_300 },
    { day: "2026-10-10", spent: 120_000, limit: 500_000 },
  ],
  upcoming: [
    {
      name: "Аванс",
      day: 15,
      amount: 2_000_000,
      currency: "UZS",
      uzs: 2_000_000,
    },
    {
      name: "Квартплата",
      day: 18,
      amount: -650,
      currency: "USD",
      uzs: -7_800_000,
    },
  ],
  subscriptions: [
    {
      name: "Render.com",
      day: 1,
      amount: 378_560,
      currency: "UZS",
      uzs: 378_560,
      state: "charged",
    },
    {
      name: "Google One",
      day: 14,
      amount: 235_882,
      currency: "UZS",
      uzs: 235_882,
      state: "expected",
    },
  ],
  runway: {
    balance: 2_000_000,
    pace: 500_000,
    last_day: "2026-10-18",
    short: 5_500_000,
    until: "2026-10-30",
    dollars: 460,
  },
  // по строкам fact: в «кафе» есть трата в долларах, которой вкладка не видит
  categories: [
    {
      name: "🍔 ЕДА",
      plan: 3_000_000,
      fact: 3_342_000,
      items: [
        { name: "кафе", icon: "🍽️", plan: 2_700_000, fact: 3_092_000 },
        { name: "кофе", icon: "☕", plan: 300_000, fact: 250_000 },
      ],
    },
    {
      name: "🏚️ ДОМ",
      plan: 2_000_000,
      fact: 500_000,
      items: [{ name: "продукты", icon: "🛒", plan: 2_000_000, fact: 500_000 }],
    },
  ],
  untracked: [
    { group: "🍔 ЕДА", subcategory: "кафе", currency: "USD", amount: 42_000 },
  ],
  cards: [
    {
      name: "VISA 1111",
      currency: "UZS",
      balance: 5_250_000.4,
      uzs: 5_250_000.4,
      bank: 5_250_000.4,
      checked: "2026-10-10",
      matches: true,
      stale: false,
    },
    {
      name: "VISA 2222",
      currency: "USD",
      balance: 1500.25,
      uzs: 18_003_000,
      bank: 1500.24,
      checked: "2026-10-04",
      matches: true,
      stale: false,
    },
    {
      name: "VISA 6666",
      currency: "RUB",
      balance: 2400.5,
      uzs: 360_075,
      bank: 2450,
      checked: "2026-10-01",
      matches: false,
      stale: true,
    },
  ],
};

export const planFixture: MonthPlan = {
  month: "2026-10",
  title: "Oct 26",
  status: "ok",
  today: "2026-10-10",
  rates: { UZS: 1, USD: 12_000, RUB: 150 },
  history: {
    average: ["2026-07", "2026-08", "2026-09"],
    last: "2026-09",
    current: "2026-10",
    current_days: 10,
  },
  rows: [
    {
      key: "кофе|UZS",
      kind: "spend",
      group: "🍔 ЕДА",
      subcategory: "кофе",
      icon: "☕",
      currency: "UZS",
      plan: 1_500_000,
      average: 1_800_000,
      last: 2_000_000,
      current: 700_000,
      editable: true,
    },
    {
      key: "кафе|UZS",
      kind: "spend",
      group: "🍔 ЕДА",
      subcategory: "кафе",
      icon: "🍽️",
      currency: "UZS",
      plan: 3_000_000,
      average: 3_400_000,
      last: 3_100_000,
      current: 1_200_000,
      editable: true,
    },
    {
      key: "квартплата|USD",
      kind: "spend",
      group: "🏚️ ДОМ",
      subcategory: "квартплата",
      icon: "🔑",
      currency: "USD",
      plan: 500,
      average: 500,
      last: 500,
      current: 0,
      editable: true,
    },
    {
      key: "зарплата|UZS",
      kind: "income",
      group: "💰 ДОХОДЫ",
      subcategory: "зарплата",
      icon: "💵",
      currency: "UZS",
      plan: 20_000_000,
      average: 20_000_000,
      last: 20_000_000,
      current: 0,
      editable: true,
    },
  ],
  events: [
    { slot: 0, name: "Аванс", day: 15, amount: 5_000_000, currency: "UZS" },
    { slot: 1, name: "ЗП", day: 30, amount: 17_000_000, currency: "UZS" },
    { slot: 2, name: "Квартплата", day: 18, amount: -500, currency: "USD" },
    { slot: 3, name: null, day: null, amount: null, currency: null },
  ],
  goal: { amount: 2_000_000, currency: "UZS" },
  addable: [
    {
      kind: "spend",
      group: "🍔 ЕДА",
      subcategory: "ресторан",
      icon: "🍷",
      currency: "UZS",
      average: 600_000,
      last: 900_000,
      current: 0,
    },
    {
      kind: "spend",
      group: "🍔 ЕДА",
      subcategory: "ресторан",
      icon: "🍷",
      currency: "USD",
      average: 0,
      last: 0,
      current: 0,
    },
    {
      kind: "spend",
      group: "🍔 ЕДА",
      subcategory: "кофе",
      icon: "☕",
      currency: "USD",
      average: 10,
      last: 30,
      current: 0,
    },
    {
      kind: "spend",
      group: "🚌 ТРАНСПОРТ",
      subcategory: "такси",
      icon: "🚕",
      currency: "UZS",
      average: 400_000,
      last: 350_000,
      current: 120_000,
    },
    {
      kind: "income",
      group: "💰 ДОХОДЫ",
      subcategory: "премия",
      icon: "🎁",
      currency: "UZS",
      average: 0,
      last: 0,
      current: 0,
    },
  ],
  saved: null,
};

export const nextPlanFixture: MonthPlan = {
  ...planFixture,
  month: "2026-11",
  title: "Nov 26",
};

export const notCreatedFixture: MonthPlan = {
  month: "2026-11",
  title: "Nov 26",
  status: "not_created",
  today: "2026-10-10",
  rates: {},
  history: null,
  rows: [],
  events: null,
  goal: null,
  addable: [],
  saved: null,
};

export const adviceFixture: { points: AdvicePoint[] } = {
  points: [
    {
      text: "Кофе: в плане 1,5 млн, а тратите в среднем 1,8 млн.",
      apply: { key: "кофе|UZS", plan: 1_800_000 },
    },
    {
      text: "Кафе можно сократить до 2,8 млн — так цель сходится.",
      apply: { key: "кафе|UZS", plan: 2_800_000 },
    },
    { text: "После цели остаётся 9,5 млн — месяц сходится.", apply: null },
  ],
};

/** Остаток на вечер с 15-го по 31-е октября: деньги сейчас + события − в день. */
function evenings(
  now: number,
  plan: number,
  pace: number,
  moves: Record<number, number>,
): ForecastDay[] {
  const days: ForecastDay[] = [];
  let byPlan = now;
  let byPace = now;
  for (let day = 15; day <= 31; day += 1) {
    byPlan += (moves[day] ?? 0) - plan;
    byPace += (moves[day] ?? 0) - pace;
    days.push({ day, plan: byPlan, pace: byPace });
  }
  return days;
}

// пример спеки: сумы уходят в минус по темпу 25-го и возвращаются с ЗП 30-го
const uzsDays = evenings(6_810_000, 470_000, 850_000, {
  15: 2_500_000,
  17: -320_000,
  30: 30_000_000,
});
const usdDays = evenings(1_800, 3, 5, { 18: -500 });
const rubDays = evenings(20_000, 300, 0, {});
const totalUzs: ForecastDay[] = uzsDays.map((point, index) => ({
  day: point.day,
  plan:
    point.plan +
    (usdDays[index]?.plan ?? 0) * 12_000 +
    (rubDays[index]?.plan ?? 0) * 150,
  pace:
    point.pace +
    (usdDays[index]?.pace ?? 0) * 12_000 +
    (rubDays[index]?.pace ?? 0) * 150,
}));

/** Траты статьи за день (возврат — минусом) в валюте строк, rate — курс к суму. */
function spend(
  subcategory: string,
  icon: string,
  amount: number,
  rate = 1,
  group = "🍔 ЕДА",
): DayArticle {
  return {
    group,
    subcategory,
    icon,
    amount,
    uzs: amount * rate,
    income: false,
  };
}

interface Move {
  spent?: number;
  earned?: number;
  articles?: DayArticle[]; // нет — usual(траты) и аванс
}

/**
 * Дни с 1-го по сегодня (15-е): вечер сегодня — деньги сейчас, назад — по
 * движениям дня. Статьи дня — из moves или usual(траты дня) и аванс.
 */
function pastDays(
  now: number,
  spentPerDay: number,
  moves: Record<number, Move>,
  usual: (spent: number) => DayArticle[],
): ForecastPast[] {
  const days: ForecastPast[] = [];
  let evening = now;
  for (let day = 15; day >= 1; day -= 1) {
    const move = moves[day];
    const spent = move?.spent ?? spentPerDay;
    const earned = move?.earned ?? 0;
    const change = earned - spent;
    const articles = move?.articles ?? [
      ...(spent ? usual(spent) : []),
      ...(earned
        ? [
            {
              group: "💰 ДОХОДЫ",
              subcategory: "аванс",
              icon: "💼",
              amount: earned,
              uzs: earned,
              income: true,
            },
          ]
        : []),
    ];
    days.unshift({ day, evening, change, spent, earned, articles });
    evening -= change;
  }
  return days;
}

// 5-го пришёл аванс, и в тот же день были траты; 9-го — возврат за кафе;
// 14-го — продукты и кафе; сегодня — продукты; 7-го и сегодня долларами не платили
const uzsPast = pastDays(
  6_810_000,
  450_000,
  {
    5: { earned: 4_000_000 },
    9: {
      articles: [
        spend("продукты", "🛒", 500_000),
        spend("кафе", "🍽️", -50_000),
      ],
    },
    14: {
      articles: [
        spend("продукты", "🛒", 300_000),
        spend("кафе", "🍽️", 150_000),
      ],
    },
    15: { spent: 120_000, articles: [spend("продукты", "🛒", 120_000)] },
  },
  (spent) => [spend("продукты", "🛒", spent)],
);
const usdPast = pastDays(
  1_800,
  5,
  { 7: { spent: 0 }, 15: { spent: 0 } },
  (spent) => [spend("доставка", "🛵", spent, 12_000)],
);
const rubPast = pastDays(20_000, 300, { 15: { spent: 0 } }, (spent) => [
  spend("подписка", "📺", spent, 150, "🌛 ЕЖЕМЕСЯЧНО"),
]);
/** Поле прошедшего дня во «Всего»: сумы + доллары по 12 000 + рубли по 150. */
function inSums(index: number, pick: (point: ForecastPast) => number): number {
  const part = (line: ForecastPast[], rate: number) => {
    const point = line[index];
    return point ? pick(point) * rate : 0;
  };
  return part(uzsPast, 1) + part(usdPast, 12_000) + part(rubPast, 150);
}
const totalPast: ForecastPast[] = uzsPast.map((point, index) => ({
  day: point.day,
  evening: inSums(index, (day) => day.evening),
  change: inSums(index, (day) => day.change),
  spent: inSums(index, (day) => day.spent),
  earned: inSums(index, (day) => day.earned),
  articles: [],
}));

// записи календаря: кофе каждый день (сверх плана статьи), кафе по пятницам,
// продукты 24-го разово и доставка в долларах 18-го каждого месяца
const coffee: CalendarRecord = {
  id: "m3a9zt",
  date: "2026-10-01",
  repeat: "daily",
  weekdays: [],
  group: "🍔 ЕДА",
  subcategory: "кофе",
  currency: "UZS",
  amount: 30_000,
  name: "Кофе",
  author: "Тест",
  month_total: 510_000,
  booked: 510_000,
  over: true,
};
const cafe: CalendarRecord = {
  ...coffee,
  id: "k4f3pt",
  date: "2026-10-02",
  repeat: "weekdays",
  weekdays: [5],
  subcategory: "кафе",
  amount: 150_000,
  name: "Кафе с друзьями",
  month_total: 450_000,
  booked: 450_000,
  over: false,
};
const groceries: CalendarRecord = {
  ...coffee,
  id: "p7r0dk",
  date: "2026-10-24",
  repeat: "once",
  group: "🏚️ ДОМ",
  subcategory: "продукты",
  amount: 400_000,
  name: "Продукты на неделю",
  month_total: 400_000,
  booked: 400_000,
  over: false,
};
const delivery: CalendarRecord = {
  ...groceries,
  id: "d2l1vr",
  date: "2026-09-18",
  repeat: "monthly",
  currency: "USD",
  amount: 20,
  name: "Доставка",
  month_total: 20,
  booked: 20,
};

/** Вхождения записи в дни days — открыты целиком, как утром без трат. */
function occurrences(record: CalendarRecord, days: number[]): ForecastRecord[] {
  return days.map((day) => ({
    id: record.id,
    day,
    name: record.name,
    group: record.group,
    subcategory: record.subcategory,
    repeat: record.repeat,
    amount: record.amount,
    open: record.amount,
    done: false,
    over: record.over,
    spent: 0,
  }));
}

const everyDay = Array.from({ length: 17 }, (_, index) => 15 + index);
const uzsRecords = [
  ...occurrences(coffee, everyDay),
  ...occurrences(cafe, [16, 23, 30]),
  ...occurrences(groceries, [24]),
].sort((a, b) => a.day - b.day);

export const forecastFixture: Forecast = {
  status: "ok",
  month: "Oct 26",
  today: "2026-10-15",
  end: "2026-10-31",
  currencies: [
    {
      code: "UZS",
      now: 6_810_000,
      per_day_plan: 470_000,
      per_day_pace: 850_000,
      spent_today: 120_000, // продукты сегодня: записью не закрыты
      in_total: true,
      events: [
        { day: 15, name: "Аванс", amount: 2_500_000, late: false },
        { day: 17, name: "Детский центр", amount: -320_000, late: false },
        { day: 30, name: "ЗП", amount: 30_000_000, late: false },
      ],
      days: uzsDays,
      below_zero: { plan: null, pace: { start: 25, until: 30 } },
      records: uzsRecords,
      past: uzsPast,
    },
    {
      code: "USD",
      now: 1_800,
      per_day_plan: 3,
      per_day_pace: 5,
      spent_today: 0,
      in_total: true,
      events: [{ day: 18, name: "Квартплата", amount: -500, late: false }],
      days: usdDays,
      below_zero: { plan: null, pace: null },
      records: occurrences(delivery, [18]),
      past: usdPast,
    },
    {
      code: "RUB",
      now: 20_000,
      per_day_plan: 300,
      per_day_pace: 0,
      spent_today: 0,
      in_total: true,
      events: [],
      days: rubDays,
      below_zero: { plan: null, pace: null },
      records: [],
      past: rubPast,
    },
  ],
  total: {
    uzs: totalUzs,
    usd: totalUzs.map((point) => ({
      day: point.day,
      plan: point.plan / 12_000,
      pace: point.pace / 12_000,
    })),
    below_zero: { plan: null, pace: null },
    past_uzs: totalPast,
    past_usd: totalPast.map((point) => ({
      day: point.day,
      evening: point.evening / 12_000,
      change: point.change / 12_000,
      spent: point.spent / 12_000,
      earned: point.earned / 12_000,
      articles: [],
    })),
  },
  rates: { USD: 12_000, RUB: 150 },
  calendar: [delivery, coffee, cafe, groceries],
  articles: [
    {
      group: "🍔 ЕДА",
      subcategory: "кофе",
      currency: "UZS",
      plan: 900_000,
      rest: 480_000,
      booked: 510_000,
    },
    {
      group: "🍔 ЕДА",
      subcategory: "кафе",
      currency: "UZS",
      plan: 1_700_000,
      rest: 1_200_000,
      booked: 450_000,
    },
    {
      group: "🏚️ ДОМ",
      subcategory: "продукты",
      currency: "UZS",
      plan: 3_000_000,
      rest: 1_500_000,
      booked: 400_000,
    },
    {
      group: "🏚️ ДОМ",
      subcategory: "продукты",
      currency: "USD",
      plan: 100,
      rest: 60,
      booked: 20,
    },
  ],
  hints: {
    "кофе|UZS": { typical: 28_000, last: 30_000, per_day: 27_000 },
    "кафе|UZS": { typical: 160_000, last: 145_000, per_day: 21_000 },
    "продукты|UZS": { typical: 380_000, last: 420_000, per_day: 95_000 },
  },
};

const PLAN_MONTHS = [
  "2026-10", "2026-11", "2026-12", "2027-01", "2027-02", "2027-03",
  "2027-04", "2027-05", "2027-06", "2027-07", "2027-08", "2027-09",
]; // prettier-ignore

function piggy(changes: Partial<Piggy>): Piggy {
  return {
    id: "p3k9za",
    row: 2,
    name: "Новый год",
    target: 12_000_000,
    currency: "UZS",
    deadline: "2026-12-20",
    repeat: "yearly",
    group: "🎉 ПРАЗДНИК",
    subcategory: "сезонный",
    author: "Иван",
    saved: 0,
    spent: 0,
    balance: 0,
    collected: 0,
    this_month: 6_000_000,
    left_this_month: 6_000_000,
    status: "on_track",
    short: 0,
    leftover: 0,
    error: null,
    history: [],
    spends: [],
    ...changes,
  };
}

export const savingsFixture: SavingsScreen = {
  status: "ok",
  today: "2026-10-10",
  month: {
    month: "2026-10",
    piggy: 6_000_000,
    piggy_left: 6_000_000,
    share: 4_000_000,
    share_to: "cushion",
    goals: [],
  },
  cushion: {
    amount: 1100,
    currency: "USD",
    uzs: 13_200_000,
    monthly: 40_000_000,
    months: 0.33,
    target_months: 6,
    short_uzs: 226_800_000,
    short: 18_900,
    frozen: 1500,
    piggies: 400,
    over: 0,
  },
  piggies: [
    piggy({}),
    piggy({
      id: "tr0001",
      row: 3,
      name: "Отпуск",
      target: 2500,
      currency: "USD",
      deadline: "2027-06-01",
      group: "✈️ ПОЕЗДКИ",
      subcategory: "путешествие",
      saved: 400,
      balance: 400,
      collected: 400,
      this_month: 0,
      left_this_month: 0,
      history: [
        { day: "2026-10-05", amount: 400, kind: "отложил", who: "Иван" },
      ],
    }),
    piggy({
      id: "rm0001",
      row: 4,
      name: "Ремонт",
      target: null,
      deadline: "2027-08-01",
      repeat: "once",
      group: null,
      subcategory: null,
      this_month: 0,
      left_this_month: 0,
      status: "no_target",
    }),
    piggy({
      id: "car001",
      row: 5,
      name: "Машина",
      target: null,
      currency: "USD",
      deadline: null,
      repeat: "once",
      group: null,
      subcategory: null,
      this_month: 0,
      left_this_month: 0,
      status: "goal",
    }),
    piggy({
      id: "",
      row: 6,
      name: "Без валюты",
      currency: "EUR",
      deadline: null,
      this_month: 0,
      left_this_month: 0,
      status: "broken",
      error: "валюты «EUR» нет у карт",
    }),
  ],
  plan: PLAN_MONTHS.map((month, index) => {
    const total = index < 2 ? 6_000_000 : index < 8 ? 5_000_000 : 3_500_000;
    const id = index < 2 || index >= 8 ? "p3k9za" : "tr0001";
    return {
      month,
      total,
      items: [{ id, amount: total }],
      cushion: 4_000_000,
      goals: [],
      ready: index === 1 ? ["p3k9za"] : index === 7 ? ["tr0001"] : [],
    };
  }),
  enough: {
    income: 42_000_000,
    save: 10_200_000,
    life: 31_800_000,
    ordinary: 34_000_000,
    gap: -2_200_000,
  },
  savings: {
    rate: -0.1,
    months: [
      { month: "2026-04", income: 40_000_000, spent: 30_000_000, rate: 0.25 },
      { month: "2026-05", income: 0, spent: 35_000_000, rate: null },
      { month: "2026-06", income: 50_000_000, spent: 48_000_000, rate: 0.04 },
      { month: "2026-07", income: 30_000_000, spent: 60_000_000, rate: -1 },
      { month: "2026-08", income: 45_000_000, spent: 36_000_000, rate: 0.2 },
      { month: "2026-09", income: 45_000_000, spent: 36_000_000, rate: 0.2 },
    ],
  },
  rates: { UZS: 1, USD: 12_000, RUB: 150 },
};
