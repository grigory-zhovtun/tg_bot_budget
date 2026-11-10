import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { Category } from "../api";
import { nb } from "../test/text";
import SpendingMap, {
  REST,
  groupSign,
  mapTiles,
  smallest,
} from "./SpendingMap";

const group = (name: string, plan: number, fact: number): Category => ({
  name,
  plan,
  fact,
  items: [],
});

const MONTH: Category[] = [
  group("🍔 ЕДА", 6_000_000, 4_200_000),
  group("🏠 ДОМ", 5_000_000, 5_300_000),
  group("👶 ДЕТИ", 2_500_000, 2_200_000),
  group("🎉 РАЗВЛЕЧЕНИЯ", 0, 420_000),
  group("👕 ОДЕЖДА", 800_000, 0), // без трат — без плитки
];

describe("SpendingMap", () => {
  it("draws a tile for every group with spending, the biggest first", () => {
    render(<SpendingMap categories={MONTH} />);
    const map = screen.getByRole("region", { name: "Карта трат" });
    expect(
      within(map)
        .getAllByRole("button")
        .map((tile) => tile.getAttribute("aria-label")),
    ).toEqual([
      nb("🏠 ДОМ: 5 300 000 из 5 000 000, 106 % плана"),
      nb("🍔 ЕДА: 4 200 000 из 6 000 000, 70 % плана"),
      nb("👶 ДЕТИ: 2 200 000 из 2 500 000, 88 % плана"),
      nb("🎉 РАЗВЛЕЧЕНИЯ: 420 000 вне плана"),
    ]);
  });

  it("colours a tile by its plan and repeats the meaning in words", () => {
    render(<SpendingMap categories={MONTH} />);
    const tile = (name: RegExp) => screen.getByRole("button", { name });
    expect(tile(/ДОМ/).dataset.status).toBe("over");
    expect(tile(/ДОМ/).className).toContain("bg-chip-bad");
    expect(tile(/ДОМ/).textContent).toContain("⚠️ 106 %");
    expect(tile(/ЕДА/).className).toContain("bg-chip-good");
    expect(tile(/ЕДА/).textContent).toContain("✓ 70 %");
    expect(tile(/ДЕТИ/).className).toContain("bg-chip-mid");
    expect(tile(/РАЗВЛЕЧЕНИЯ/).className).toContain("bg-chip-off");
    // текст на плитках — белый: цвет плитки — фон, не цвет букв
    expect(tile(/ДОМ/).className).toContain("text-white");
  });

  it("makes a bigger tile for bigger spending", () => {
    render(<SpendingMap categories={MONTH} />);
    const size = (name: RegExp) => {
      const box = screen.getByRole("button", { name }).parentElement;
      return (
        parseFloat(box?.style.width ?? "0") *
        parseFloat(box?.style.height ?? "0")
      );
    };
    expect(size(/ДОМ/)).toBeGreaterThan(size(/ЕДА/));
    expect(size(/ЕДА/)).toBeGreaterThan(size(/ДЕТИ/));
    expect(size(/ДЕТИ/)).toBeGreaterThan(size(/РАЗВЛЕЧЕНИЯ/));
  });

  it("explains the colours it used under the map", () => {
    render(<SpendingMap categories={MONTH.slice(0, 2)} />);
    expect(screen.getByText("размер — сколько потрачено")).toBeTruthy();
    expect(screen.getByText("в плане")).toBeTruthy();
    expect(screen.getByText("сверх плана")).toBeTruthy();
    expect(screen.queryByText("вне плана")).toBeNull();
    expect(screen.queryByText("от 80 %")).toBeNull();
  });

  it("opens the pressed group", async () => {
    const onPick = vi.fn();
    const user = userEvent.setup();
    render(<SpendingMap categories={MONTH} onPick={onPick} />);
    await user.click(screen.getByRole("button", { name: /ЕДА/ }));
    expect(onPick).toHaveBeenCalledWith("🍔 ЕДА");
  });

  it("is not drawn when there is nothing to compare", () => {
    const { container } = render(
      <SpendingMap categories={[group("🍔 ЕДА", 1, 1)]} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it("puts small groups into one tile after the seven biggest", () => {
    const many = Array.from({ length: 10 }, (_, index) =>
      group(`Группа ${index + 1}`, 1_000_000, (10 - index) * 100_000),
    );
    const tiles = mapTiles(many);
    expect(tiles.map((tile) => tile.name)).toEqual([
      "Группа 1",
      "Группа 2",
      "Группа 3",
      "Группа 4",
      "Группа 5",
      "Группа 6",
      "Группа 7",
      REST,
    ]);
    // 300 + 200 + 100 тысяч из 3 млн плана
    expect(tiles[7]).toMatchObject({
      fact: 600_000,
      plan: 3_000_000,
      status: "ok",
      rest: true,
    });
    // восьмая группа одна — остаётся собой
    expect(mapTiles(many.slice(0, 8))[7]?.name).toBe("Группа 8");
  });

  it("does not open the tile of several groups", () => {
    const many = Array.from({ length: 10 }, (_, index) =>
      group(`Группа ${index + 1}`, 1_000_000, (10 - index) * 100_000),
    );
    render(<SpendingMap categories={many} onPick={vi.fn()} />);
    expect(
      screen.getByRole("button", { name: new RegExp(`^${REST}`) }),
    ).toHaveProperty("disabled", true);
  });

  it("keeps the smallest tile big enough for its name", () => {
    expect(smallest(2)).toBe(0.25);
    expect(smallest(4)).toBe(0.125);
    expect(smallest(8)).toBeCloseTo(0.0625);
    expect(smallest(20)).toBe(0.05);
    expect(smallest(0)).toBe(0);
  });

  it("finds the sign of a group in front of its name", () => {
    expect(groupSign("🍔 ЕДА")).toBe("🍔");
    expect(groupSign("🏚️ ДОМ")).toBe("🏚️");
    expect(groupSign("ЕДА и кафе")).toBeNull();
    expect(groupSign("🍔")).toBeNull();
  });
});
