import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { bootstrapFixture, dashboardFixture } from "../mocks/fixtures";
import type { WebApp } from "../telegram";
import { planIndex } from "./plan";
import { GroupTiles, SubTiles } from "./Tiles";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("tiles", () => {
  it("ticks under the finger when a tile is picked", async () => {
    const selectionChanged = vi.fn();
    const fake = {
      platform: "ios",
      HapticFeedback: { selectionChanged, notificationOccurred: vi.fn() },
    } as unknown as WebApp;
    vi.stubGlobal("Telegram", { WebApp: fake });
    const onPick = vi.fn();
    render(
      <GroupTiles
        groups={bootstrapFixture.groups}
        plan={planIndex(null)}
        onPick={onPick}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "ЕДА" }));
    expect(selectionChanged).toHaveBeenCalledOnce();
    expect(onPick).toHaveBeenCalledWith(bootstrapFixture.groups[0]);
  });

  it("puts what is left and the sum on two big lines", () => {
    render(
      <GroupTiles
        groups={bootstrapFixture.groups}
        plan={planIndex(dashboardFixture)}
        onPick={vi.fn()}
      />,
    );
    // ДОМ: план 2 000 000, потрачено 500 000
    expect(
      screen.getByRole("button", {
        name: "ДОМ",
        description: /^осталось 1\s500\s000$/,
      }),
    ).toBeTruthy();
    const head = screen.getByText("осталось");
    const sum = screen.getByText("1 500 000");
    expect(head.className).toContain("text-line");
    expect(head.className).not.toContain("text-hint");
    expect(sum.className).toContain("text-line");
    expect(sum.className).toContain("font-extrabold");
    // ЕДА: перерасход — со знаком
    expect(screen.getByText("⚠️ сверх на")).toBeTruthy();
    expect(screen.getByText("300 000")).toBeTruthy();
  });

  it("breaks a word too long for half a tile instead of letting it out", () => {
    render(
      <SubTiles
        group={{
          name: "🎉 ПРАЗДНИК",
          emoji: "🎉",
          title: "ПРАЗДНИК",
          subcategories: [
            { name: "достопримечательности", icon: "🏛️" },
            { name: "подарки друзьям", icon: "🎁" },
          ],
        }}
        color="#7c5cff"
        plan={planIndex(null)}
        onPick={vi.fn()}
      />,
    );
    // одно длинное слово — с дефисом; несколько слов переносятся по пробелам
    const word = screen.getByText("достопримечательности");
    expect(word.className).toContain("break-words");
    expect(word.className).toContain("hyphens-auto");
    const words = screen.getByText("подарки друзьям");
    expect(words.className).toContain("break-words");
    expect(words.className).not.toContain("hyphens-auto");
  });

  it("writes the tile title big and bold", () => {
    render(
      <GroupTiles
        groups={bootstrapFixture.groups}
        plan={planIndex(null)}
        onPick={vi.fn()}
      />,
    );
    const title = screen.getByText("ЕДА");
    expect(title.className).toContain("text-lg");
    expect(title.className).toContain("font-extrabold");
  });
});
