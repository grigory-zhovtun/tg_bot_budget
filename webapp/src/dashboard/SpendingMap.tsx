import type { Category } from "../api";
import { money, short } from "../format";
import { Title } from "./blocks";
import { statusOf, type Status } from "./status";
import { treemap } from "./treemap";

const MOST = 7; // плиток с названием; остальные группы — одной плиткой
/** Меньшая плитка — не меньше такой доли поля, чтобы влезло название: при двух
 * плитках — четверть, при восьми и больше — двадцатая часть. */
export function smallest(count: number): number {
  return count > 0 ? Math.max(0.05, 0.5 / count) : 0;
}
const RATIO = 1.15; // ширина поля к высоте на телефоне
export const REST = "Ещё";

/** Плитка: белый текст на цвете статуса плана — как плашки «План-факта». */
const TONE: Record<Status, string> = {
  ok: "bg-chip-good",
  warn: "bg-chip-mid",
  over: "bg-chip-bad",
  off: "bg-chip-off",
};
const LEGEND: [Status, string][] = [
  ["ok", "в плане"],
  ["warn", "от 80 %"],
  ["over", "сверх плана"],
  ["off", "вне плана"],
];

interface Tile {
  name: string;
  plan: number;
  fact: number;
  status: Status;
  rest: boolean; // «Остальное» — несколько групп, не открывается
}

/** Группы с тратами от больших; после MOST-й — одной плиткой «Остальное». */
export function mapTiles(categories: Category[]): Tile[] {
  const spent = categories
    .filter((group) => group.fact > 0)
    .sort((a, b) => b.fact - a.fact);
  const tile = (name: string, plan: number, fact: number, rest: boolean) => ({
    name,
    plan,
    fact,
    status: statusOf(plan, fact),
    rest,
  });
  const shown = spent
    .slice(0, MOST)
    .map((group) => tile(group.name, group.plan, group.fact, false));
  const others = spent.slice(MOST);
  if (others.length === 1 && others[0]) {
    const [last] = others;
    shown.push(tile(last.name, last.plan, last.fact, false));
  } else if (others.length > 1) {
    const sum = (pick: (group: Category) => number) =>
      others.reduce((total, group) => total + pick(group), 0);
    shown.push(
      tile(
        REST,
        sum((group) => group.plan),
        sum((group) => group.fact),
        true,
      ),
    );
  }
  return shown;
}

/** «✓ 70 %», «88 %», «⚠️ 106 %» — доля плана, как на плашках; без плана — «вне плана». */
function shareText(tile: Tile): string {
  if (tile.status === "off") return "вне плана";
  const share = `${Math.round((tile.fact / tile.plan) * 100)}\u00a0%`;
  if (tile.status === "ok") return `✓ ${share}`;
  return tile.status === "over" ? `⚠️ ${share}` : share;
}

function label(tile: Tile): string {
  const plan =
    tile.status === "off"
      ? "вне плана"
      : `из ${money(tile.plan)}, ${Math.round((tile.fact / tile.plan) * 100)}\u00a0% плана`;
  return `${tile.name}: ${money(tile.fact)} ${plan}`;
}

/** Значок группы — её первое «слово» без букв и цифр («🍔 ЕДА» → «🍔»); нет — null. */
export function groupSign(name: string): string | null {
  const [first = "", ...rest] = name.trim().split(/\s+/);
  return rest.length > 0 && !/[\p{L}\p{N}]/u.test(first) ? first : null;
}

const percent = (part: number) => `${Math.round(part * 10000) / 100}%`;

interface Props {
  categories: Category[];
  onPick?: (group: string) => void; // нажатие на плитку — открыть её подкатегории
}

/**
 * Карта трат месяца: плитка на группу, размер — сколько потрачено, цвет — как идёт
 * план. Смысл цвета повторён текстом на плитке («✓», «⚠️», «вне плана»).
 */
export default function SpendingMap({ categories, onPick }: Props) {
  const tiles = mapTiles(categories);
  if (tiles.length < 2) return null; // одна плитка ничего не сравнивает
  const total = tiles.reduce((sum, tile) => sum + tile.fact, 0);
  const rects = treemap(
    tiles.map((tile) => Math.max(tile.fact, total * smallest(tiles.length))),
    RATIO,
    1,
  );
  const seen = LEGEND.filter(([status]) =>
    tiles.some((tile) => tile.status === status),
  );
  return (
    <section className="flex flex-col gap-3" aria-label="Карта трат">
      <Title>Карта трат</Title>
      <div className="relative h-64 w-full">
        {tiles.map((tile, index) => {
          const rect = rects[index];
          if (!rect) return null;
          // подробности — только там, где они влезают
          const roomy = rect.w >= 0.3 && rect.h >= 0.24;
          const tiny = rect.w < 0.24 || rect.h < 0.16;
          const sign = groupSign(tile.name);
          return (
            <div
              key={tile.name}
              className="absolute p-[3px]"
              style={{
                left: percent(rect.x),
                top: percent(rect.y),
                width: percent(rect.w),
                height: percent(rect.h),
              }}
            >
              <button
                type="button"
                aria-label={label(tile)}
                data-status={tile.status}
                disabled={tile.rest || !onPick}
                className={`flex size-full flex-col justify-end overflow-hidden rounded-xl px-2 py-1.5 text-left leading-tight font-black text-white disabled:opacity-100 ${TONE[tile.status]}`}
                onClick={() => onPick?.(tile.name)}
              >
                {/* в тесной плитке — один значок группы; в средней название одной
                    строкой, сколько влезет; в просторной — целиком */}
                {tiny && sign ? (
                  <span className="m-auto text-2xl">{sign}</span>
                ) : (
                  <span
                    className={
                      roomy
                        ? "text-line hyphens-auto break-words"
                        : "max-w-full overflow-hidden text-sm text-ellipsis whitespace-nowrap"
                    }
                  >
                    {tile.name}
                  </span>
                )}
                {roomy && (
                  <span className="text-base font-extrabold whitespace-nowrap">
                    {short(tile.fact)}
                  </span>
                )}
                {!tiny && (
                  <span className="max-w-full overflow-hidden text-sm font-extrabold text-ellipsis whitespace-nowrap">
                    {shareText(tile)}
                  </span>
                )}
              </button>
            </div>
          );
        })}
      </div>
      <p className="flex flex-wrap gap-x-4 gap-y-1 text-base font-semibold">
        <span>размер — сколько потрачено</span>
        {seen.map(([status, text]) => (
          <span key={status} className="flex items-center gap-1.5">
            <i
              aria-hidden="true"
              className={`inline-block size-3.5 rounded ${TONE[status]}`}
            />
            {text}
          </span>
        ))}
      </p>
    </section>
  );
}
