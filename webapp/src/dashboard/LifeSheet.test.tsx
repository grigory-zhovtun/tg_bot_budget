import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type Bootstrap } from "../api";
import { bootstrapFixture, savingsFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import LifeSheet from "./LifeSheet";

function show(api: Api, boot: Bootstrap = bootstrapFixture) {
  const onSaved = vi.fn();
  render(
    <LifeSheet
      api={api}
      boot={boot}
      data={savingsFixture}
      onSaved={onSaved}
      onClose={vi.fn()}
    />,
  );
  return onSaved;
}

function group(name: string): HTMLElement {
  const list = screen.getByRole("list", { name: "Обязательные группы" });
  return within(list).getByRole("checkbox", { name });
}

describe("LifeSheet", () => {
  it("checks the groups of the month of life and saves them in the catalog's order", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    const onSaved = show(api);
    expect(screen.getByRole("dialog", { name: "Месяц жизни" })).toBeTruthy();
    const home = group("🏚️ ДОМ");
    expect(home.getAttribute("aria-checked")).toBe("true");
    expect(home.textContent).toBe("✓ 🏚️ ДОМ");
    expect(home.className).toContain("text-line");
    expect(home.className).toContain("font-bold");
    expect(group("🍔 ЕДА").getAttribute("aria-checked")).toBe("false");
    await user.click(group("🍔 ЕДА"));
    expect(group("🍔 ЕДА").getAttribute("aria-checked")).toBe("true");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.setLife).toHaveBeenCalledWith(["🍔 ЕДА", "🏚️ ДОМ"]);
    await vi.waitFor(() =>
      expect(onSaved).toHaveBeenCalledWith(savingsFixture),
    );
  });

  it("needs at least one group", async () => {
    const user = userEvent.setup();
    show(fakeApi());
    await user.click(group("🏚️ ДОМ"));
    expect(
      (screen.getByRole("button", { name: "Сохранить" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(screen.getByText("отметьте хотя бы одну группу")).toBeTruthy();
  });

  it("leaves out incomes and transfers", () => {
    const boot: Bootstrap = {
      ...bootstrapFixture,
      groups: [
        ...bootstrapFixture.groups,
        { name: "💰 ДОХОДЫ", emoji: "💰", title: "ДОХОДЫ", subcategories: [] },
        { name: "💳 СЧЕТА", emoji: "💳", title: "СЧЕТА", subcategories: [] },
      ],
    };
    show(fakeApi(), boot);
    const list = screen.getByRole("list", { name: "Обязательные группы" });
    expect(
      within(list)
        .getAllByRole("checkbox")
        .map((item) => item.getAttribute("aria-label")),
    ).toEqual(["🍔 ЕДА", "🏚️ ДОМ"]);
  });

  it("writes the server's refusal", async () => {
    const setLife = vi.fn<Api["setLife"]>().mockRejectedValue(
      new ApiError(422, "validation", "Проверьте введённые данные", {
        groups: "Выберите группы трат",
      }),
    );
    const user = userEvent.setup();
    show(fakeApi({ setLife }));
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(await screen.findByText("Выберите группы трат")).toBeTruthy();
  });
});
