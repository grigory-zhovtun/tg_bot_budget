/** Цвет группы на плитках: по смыслу названия, иначе из палитры по порядку. */

const BY_MEANING: [string, string][] = [
  ["ДОХОД", "#3fb86b"],
  ["РАЗНОЕ", "#8a96a3"],
  ["СЧЕТ", "#7d8ea3"],
  ["ЕДА", "#ff8a3d"],
  ["ДЕТИ", "#ec5ca0"],
  ["ДОМ", "#2bb5a0"],
  ["ПРАЗДНИК", "#f2b134"],
  ["ЕЖЕМЕСЯЧНО", "#6c7cf5"],
  ["ОБРАЗОВАНИЕ", "#9b6cf2"],
  ["ЗДОРОВ", "#f2645a"],
  ["ОТДЫХ", "#26b5d9"],
  ["ТРАНСПОРТ", "#3a9cf5"],
  ["ПОЕЗДК", "#7cc24a"],
];

const PALETTE = [
  "#ff8a3d",
  "#ec5ca0",
  "#2bb5a0",
  "#3a9cf5",
  "#9b6cf2",
  "#3fb86b",
  "#f2b134",
  "#f2645a",
  "#26b5d9",
  "#6c7cf5",
];

export function groupColor(name: string, index: number): string {
  const upper = name.toUpperCase();
  const known = BY_MEANING.find(([word]) => upper.includes(word));
  if (known) return known[1];
  return PALETTE[Math.abs(index) % PALETTE.length] ?? "#8a96a3";
}

/** Полупрозрачный оттенок цвета: ложится и на тёмную, и на светлую тему. */
export function tint(hex: string, alpha: number): string {
  const value = Number.parseInt(hex.slice(1), 16);
  const red = (value >> 16) & 255;
  const green = (value >> 8) & 255;
  const blue = value & 255;
  return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
}
