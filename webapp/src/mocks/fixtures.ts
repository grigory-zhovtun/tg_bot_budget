import type { Bootstrap, ExpenseResult } from "../api";

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
