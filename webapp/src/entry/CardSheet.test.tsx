import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { Source } from "../api";
import { transferCards, transferSources } from "../mocks/fixtures";
import CardSheet from "./CardSheet";

function sheet(
  side: "from" | "to",
  chosen: string | null,
  exclude: string | null,
  sources: Source[] = transferSources,
) {
  const onPick = vi.fn();
  render(
    <CardSheet
      side={side}
      sources={sources}
      chosen={chosen}
      exclude={exclude}
      cards={transferCards}
      onPick={onPick}
      onClose={vi.fn()}
    />,
  );
  return onPick;
}

/** Строки списка карт по порядку. */
function rows(): HTMLElement[] {
  return within(screen.getByRole("list", { name: "Карты" })).getAllByRole(
    "button",
  );
}

/** Название карты в строке — первая строчка кнопки. */
function title(row: HTMLElement): string {
  return row.querySelector("span")?.textContent ?? "";
}

describe("CardSheet", () => {
  it("lists the cards big with their balance", () => {
    sheet("to", null, "VISA 1111 UZS");
    expect(screen.getByRole("dialog", { name: "Куда" })).toBeTruthy();
    const [humo, visa] = rows();
    expect(rows().map(title)).toEqual(["HUMO 2222 UZS", "VISA 3333 USD"]);
    if (!humo || !visa) throw new Error("two cards expected");
    expect(within(visa).getByText("на карте 1 500,25 $")).toBeTruthy();
    expect(within(humo).queryByText(/на карте/)).toBeNull();
    const name = within(humo).getByText("HUMO 2222 UZS");
    expect(name.className).toContain("text-fact");
    expect(name.className).toContain("font-extrabold");
    expect(screen.queryByText("не моя карта")).toBeNull();
  });

  it("marks the chosen card and puts «не моя карта» last for «Откуда»", () => {
    sheet("from", "VISA 1111 UZS", "HUMO 2222 UZS");
    expect(screen.getByRole("dialog", { name: "Откуда" })).toBeTruthy();
    const list = rows();
    expect(list.map(title)).toEqual([
      "✓ VISA 1111 UZS",
      "VISA 3333 USD",
      "не моя карта",
    ]);
    expect(list[0]?.className).toContain("bg-button");
    expect(list[1]?.className).toContain("bg-section");
    const other = list[2];
    if (!other) throw new Error("«не моя карта» expected");
    expect(within(other).getByText("деньги от других людей")).toBeTruthy();
    expect(screen.queryByText(/HUMO 2222/)).toBeNull();
  });

  it("marks «не моя карта» when it is the chosen «Откуда»", () => {
    sheet("from", null, null);
    const last = rows().at(-1);
    expect(last ? title(last) : "").toBe("✓ не моя карта");
    expect(last?.className).toContain("bg-button");
  });

  it("picks a card or «не моя карта»", async () => {
    const user = userEvent.setup();
    const onPick = sheet("from", "VISA 1111 UZS", null);
    await user.click(screen.getByRole("button", { name: /VISA 3333 USD/ }));
    expect(onPick).toHaveBeenLastCalledWith("VISA 3333 USD");
    await user.click(screen.getByRole("button", { name: /не моя карта/ }));
    expect(onPick).toHaveBeenLastCalledWith(null);
  });

  it("says when there is no other card", () => {
    sheet("to", null, "VISA 1111 UZS", [
      { name: "VISA 1111 UZS", currency: "UZS" },
    ]);
    expect(screen.getByText("других карт нет")).toBeTruthy();
  });
});
