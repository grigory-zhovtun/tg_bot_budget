import type { Bootstrap, Dashboard, ExpenseResult, MonthPlan } from "../api";

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
  daily: [
    { day: "2026-10-01", plan: 45_000_000, fact: 44_800_000 },
    { day: "2026-10-02", plan: 44_500_000, fact: 44_000_000 },
    { day: "2026-10-03", plan: 44_000_000, fact: null },
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
  saved: null,
};
