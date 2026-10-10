import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { bootstrapFixture } from "../mocks/fixtures";
import type { WebApp } from "../telegram";
import { planIndex } from "./plan";
import { GroupTiles } from "./Tiles";

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
});
