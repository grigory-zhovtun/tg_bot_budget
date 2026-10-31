import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Group } from "../api";
import { transferCards, transferSources } from "../mocks/fixtures";
import { inTelegram, lastCall, tapPanel } from "../test/telegram";
import type { Outgoing } from "./outgoing";
import TransferScreen from "./TransferScreen";

type User = ReturnType<typeof userEvent.setup>;
type Props = Parameters<typeof TransferScreen>[0];

const ACCOUNTS: Group = {
  name: "💳 СЧЕТА",
  emoji: "💳",
  title: "СЧЕТА",
  subcategories: [
    { name: "переводы", icon: "🔄" },
    { name: "обмен валюты", icon: "💱" },
  ],
};

function props(sub: string): Props {
  const found = ACCOUNTS.subcategories.find((item) => item.name === sub);
  if (!found) throw new Error(`no subcategory ${sub}`);
  return {
    group: ACCOUNTS,
    sub: found,
    sources: transferSources,
    current: "VISA 1111 UZS",
    cards: transferCards,
    today: "2026-10-10",
    entryId: "e1",
    busy: false,
    onSubmit: vi.fn<(out: Outgoing) => void>(),
  };
}

/** Экран перевода по подкатегории «💳 СЧЕТА»; onSubmit — что ушло бы на запись. */
function show(sub = "переводы") {
  const given = props(sub);
  const view = render(<TransferScreen {...given} />);
  return { onSubmit: vi.mocked(given.onSubmit), view, given };
}

/** Тап по полю «Откуда» или «Куда» и по карте в окне. Имя выбранной — с «✓ ». */
async function pick(
  user: User,
  side: "from" | "to",
  card: RegExp,
): Promise<void> {
  const [field, title] =
    side === "from" ? [/^Откуда/, "Откуда"] : [/^Куда/, "Куда"];
  await user.click(screen.getByRole("button", { name: field }));
  const sheet = screen.getByRole("dialog", { name: title });
  await user.click(within(sheet).getByRole("button", { name: card }));
}

async function keys(user: User, ...names: string[]): Promise<void> {
  for (const name of names) {
    await user.click(screen.getByRole("button", { name }));
  }
}

/** Клетки таблицы «В таблицу» по строкам, пробелы — обычные. */
function factRows(): string[][] {
  const table = screen.getByRole("table", { name: "В таблицу" });
  return [...table.querySelectorAll("tbody tr")].map((row) =>
    [...row.querySelectorAll("td")].map((cell) =>
      (cell.textContent ?? "").replace(/\s/g, " "),
    ),
  );
}

const plain = (text: string | null | undefined) =>
  (text ?? "").replace(/\s/g, " ");

describe("TransferScreen", () => {
  it("starts from the current card and asks where to", () => {
    show();
    const from = screen.getByRole("button", { name: /^Откуда/ });
    expect(plain(from.textContent)).toContain("VISA 1111 UZS");
    expect(plain(from.textContent)).toContain("на карте 5 250 000");
    const to = screen.getByRole("button", { name: /^Куда/ });
    expect(within(to).getByText("выберите карту").className).toContain(
      "text-link",
    );
    expect(screen.getByText("Сумма — вводите")).toBeTruthy();
    expect(screen.queryByRole("table", { name: "В таблицу" })).toBeNull();
    expect(
      screen
        .getByRole("button", { name: "Записать перевод" })
        .hasAttribute("disabled"),
    ).toBe(true);
  });

  it("leaves the other side's card out of the window", async () => {
    const user = userEvent.setup();
    show();
    await user.click(screen.getByRole("button", { name: /^Куда/ }));
    const to = screen.getByRole("dialog", { name: "Куда" });
    expect(
      within(to).queryByRole("button", { name: /VISA 1111 UZS/ }),
    ).toBeNull();
    expect(
      within(to).queryByRole("button", { name: /не моя карта/ }),
    ).toBeNull();
    await user.click(within(to).getByRole("button", { name: /HUMO 2222 UZS/ }));
    await user.click(screen.getByRole("button", { name: /^Откуда/ }));
    const from = screen.getByRole("dialog", { name: "Откуда" });
    expect(
      within(from).queryByRole("button", { name: /HUMO 2222/ }),
    ).toBeNull();
    const rows = within(within(from).getByRole("list", { name: "Карты" }))
      .getAllByRole("button")
      .map((row) => row.textContent);
    expect(rows.at(-1)).toMatch(/^не моя карта/);
  });

  it("writes one sum for cards of one currency", async () => {
    const user = userEvent.setup();
    const { onSubmit } = show();
    await pick(user, "to", /HUMO 2222 UZS/);
    await keys(user, "5", "0", "0", "000");
    expect(factRows()).toEqual([
      ["VISA 1111 UZS", "500 000"],
      ["HUMO 2222 UZS", "−500 000"],
    ]);
    await user.click(screen.getByRole("button", { name: "Записать перевод" }));
    const out = onSubmit.mock.calls[0]?.[0];
    expect(out?.entry).toEqual({
      entry_id: "e1",
      source: "VISA 1111 UZS",
      to_source: "HUMO 2222 UZS",
      category: "💳 СЧЕТА",
      subcategory: "переводы",
      amount: "500000",
      comment: "",
    });
    expect(plain(out?.title)).toBe("переводы 500 000 UZS → HUMO 2222 UZS");
    expect(out?.head).toBe("✓ Записан перевод");
    expect(out?.card).toBe("HUMO 2222 UZS");
  });

  it("asks for both sums across currencies and checks the rate", async () => {
    const user = userEvent.setup();
    const { onSubmit } = show("обмен валюты");
    await pick(user, "to", /VISA 3333 USD/);
    const got = screen.getByRole("button", { name: /^Зачислено/ });
    expect(got.getAttribute("aria-pressed")).toBe("true");
    expect(within(got).getByText("Зачислено — вводите")).toBeTruthy();
    expect(got.className).toContain("border-button");
    await keys(user, "1", "0", "0");
    await user.click(screen.getByRole("button", { name: /^Списано/ }));
    await keys(user, "1", "2", "1", "0", "8", "0", "0");
    expect(screen.getByText("курс: 1 $ = 12 108 сум")).toBeTruthy();
    expect(factRows()).toEqual([
      ["VISA 1111 UZS", "1 210 800"],
      ["VISA 3333 USD", "−100"],
    ]);
    await user.click(screen.getByRole("button", { name: "Записать обмен" }));
    const out = onSubmit.mock.calls[0]?.[0];
    expect(out?.entry).toMatchObject({ amount: "1210800", to_amount: "100" });
    expect(out?.head).toBe("✓ Записан обмен");
    // в очереди видна и вторая сумма
    expect(plain(out?.title)).toBe(
      "обмен валюты 1 210 800 UZS → 100 USD, VISA 3333 USD",
    );
  });

  it("clears the sum of the card that changed", async () => {
    const user = userEvent.setup();
    show("обмен валюты");
    await pick(user, "to", /VISA 3333 USD/);
    await keys(user, "1", "0", "0");
    await user.click(screen.getByRole("button", { name: /^Списано/ }));
    await keys(user, "1", "2", "1", "0", "8", "0", "0");
    // та же валюта — одно поле «Сумма»: «Списано» не пропало
    await pick(user, "to", /HUMO 2222 UZS/);
    expect(
      within(screen.getByRole("button", { name: /^Сумма/ })).getByText(
        "1 210 800",
      ),
    ).toBeTruthy();
    // снова доллары — «Зачислено» стёрто: сумма HUMO не уходит на VISA 3333
    await pick(user, "to", /VISA 3333 USD/);
    const got = screen.getByRole("button", { name: /^Зачислено/ });
    expect(within(got).getByText("0")).toBeTruthy();
    expect(got.getAttribute("aria-pressed")).toBe("true");
    expect(screen.queryByRole("table", { name: "В таблицу" })).toBeNull();
  });

  it("credits money from someone else to one card", async () => {
    const user = userEvent.setup();
    const { onSubmit } = show();
    await pick(user, "from", /не моя карта/);
    const from = screen.getByRole("button", { name: /^Откуда/ });
    expect(plain(from.textContent)).toContain("деньги от других людей");
    await pick(user, "to", /HUMO 2222 UZS/);
    await keys(user, "3", "0", "0", "000");
    expect(factRows()).toEqual([["HUMO 2222 UZS", "−300 000"]]);
    await user.click(screen.getByRole("button", { name: "Зачислить" }));
    const out = onSubmit.mock.calls[0]?.[0];
    expect(out?.entry).toEqual({
      entry_id: "e1",
      source: "HUMO 2222 UZS",
      incoming: true,
      category: "💳 СЧЕТА",
      subcategory: "переводы",
      amount: "300000",
      comment: "",
    });
    expect(plain(out?.head)).toBe(
      "✓ Зачислено 300 000 UZS — в таблицу −300 000",
    );
    expect(out?.card).toBe("HUMO 2222 UZS");
  });

  it("drops a sum of «не моя карта» typed before a card of another currency", async () => {
    const user = userEvent.setup();
    show();
    await pick(user, "from", /не моя карта/);
    await keys(user, "3", "0", "0", "000"); // «Куда» не выбрана — в сумах
    await pick(user, "to", /VISA 3333 USD/);
    const sum = screen.getByRole("button", { name: /^Сумма/ });
    expect(within(sum).getByText("0")).toBeTruthy();
    expect(within(sum).getByText("USD")).toBeTruthy();
    expect(screen.queryByRole("table", { name: "В таблицу" })).toBeNull();
    expect(
      screen
        .getByRole("button", { name: "Зачислить" })
        .hasAttribute("disabled"),
    ).toBe(true);
  });

  it("keeps a sum of «не моя карта» for a card of the same currency", async () => {
    const user = userEvent.setup();
    show();
    await pick(user, "from", /не моя карта/);
    await keys(user, "3", "0", "0", "000");
    await pick(user, "to", /HUMO 2222 UZS/);
    const sum = screen.getByRole("button", { name: /^Сумма/ });
    expect(within(sum).getByText("300 000")).toBeTruthy();
    expect(factRows()).toEqual([["HUMO 2222 UZS", "−300 000"]]);
  });

  it("sends the comment and the date", async () => {
    const user = userEvent.setup();
    const { onSubmit } = show();
    await pick(user, "to", /HUMO 2222 UZS/);
    await keys(user, "5", "0", "0", "000");
    await user.type(screen.getByLabelText("Комментарий"), "на отпуск");
    fireEvent.change(screen.getByLabelText(/Дата/), {
      target: { value: "2026-10-09" },
    });
    await user.click(screen.getByRole("button", { name: "Записать перевод" }));
    expect(onSubmit.mock.calls[0]?.[0].entry).toMatchObject({
      comment: "на отпуск",
      day: "2026-10-09",
    });
  });

  it("writes the card fields big and bold", () => {
    show();
    const from = screen.getByRole("button", { name: /^Откуда/ });
    const name = within(from).getByText("VISA 1111 UZS");
    expect(name.className).toContain("text-fact");
    expect(name.className).toContain("font-extrabold");
    const label = within(from).getByText("Откуда");
    expect(label.className).toContain("text-line");
    expect(label.className).toContain("font-bold");
    expect(within(from).getByText("›").className).toContain("text-link");
  });

  it("grows its own button with the font", () => {
    show();
    const classes = screen
      .getByRole("button", { name: "Записать перевод" })
      .className.split(" ");
    expect(classes).toContain("min-h-12");
    expect(classes).not.toContain("h-12");
  });
});

describe("TransferScreen in Telegram", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("uses Telegram's main button and holds it while the card window is open", async () => {
    const { main } = inTelegram();
    const user = userEvent.setup();
    const { onSubmit, view, given } = show();
    // текст — в том же вызове, что показ: кадра с прежним текстом нет
    expect(main.setParams).toHaveBeenCalledWith({
      text: "Записать перевод",
      is_visible: true,
    });
    expect(main.show).not.toHaveBeenCalled();
    expect(
      screen.queryByRole("button", { name: "Записать перевод" }),
    ).toBeNull();
    // hideProgress снова включает кнопку — выключаем после него
    expect(lastCall(main.disable)).toBeGreaterThan(lastCall(main.hideProgress));
    await pick(user, "to", /HUMO 2222 UZS/);
    await keys(user, "5", "0", "0", "000");
    expect(lastCall(main.enable)).toBeGreaterThan(lastCall(main.disable));
    // окно карт открыто — нажатие записало бы прежнюю «Куда»
    await user.click(screen.getByRole("button", { name: /^Куда/ }));
    expect(lastCall(main.disable)).toBeGreaterThan(lastCall(main.enable));
    const sheet = screen.getByRole("dialog", { name: "Куда" });
    await user.click(within(sheet).getByRole("button", { name: /HUMO 2222/ }));
    expect(lastCall(main.enable)).toBeGreaterThan(lastCall(main.disable));
    await tapPanel(main);
    expect(onSubmit).toHaveBeenCalledOnce();
    view.rerender(<TransferScreen {...given} busy />);
    expect(main.showProgress).toHaveBeenLastCalledWith(false);
    view.unmount();
    expect(main.hide).toHaveBeenCalledOnce();
    expect(lastCall(main.hideProgress)).toBeGreaterThan(
      lastCall(main.showProgress),
    );
  });
});
