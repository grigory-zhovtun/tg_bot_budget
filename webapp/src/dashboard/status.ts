/** Статус статьи как в боте: до 80 % плана — зелёный, до 100 % — жёлтый, сверх — красный. */

export type Status = "ok" | "warn" | "over" | "off";

export const NEAR_PLAN = 0.8;

export const STATUS_COLOR: Record<Status, string> = {
  ok: "bg-ok",
  warn: "bg-warn",
  over: "bg-over",
  off: "bg-off",
};

export function statusOf(plan: number, fact: number): Status {
  if (plan <= 0) return "off";
  const ratio = fact / plan;
  if (ratio > 1) return "over";
  return ratio >= NEAR_PLAN ? "warn" : "ok";
}

/** Доля полоски: факт от плана, не больше 100 %; трата вне плана — полная серая. */
export function filled(plan: number, fact: number): number {
  if (plan <= 0) return fact > 0 ? 1 : 0;
  return Math.max(0, Math.min(fact / plan, 1));
}
