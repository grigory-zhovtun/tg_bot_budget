import type { Bootstrap, Dashboard, ExpenseResult } from "../api";

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
};
