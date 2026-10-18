import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { savingsFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import OrderSheet from "./OrderSheet";

function names(): (string | null)[] {
  const list = screen.getByRole("list", { name: "Порядок важности" });
  return within(list)
    .getAllByRole("heading")
    .map((heading) => heading.textContent);
}

describe("OrderSheet", () => {
  it("moves a piggy up and previews the order", async () => {
    const api = fakeApi();
    const user = userEvent.setup();
    render(
      <OrderSheet
        api={api}
        data={savingsFixture}
        onSaved={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(names()).toEqual(["Новый год", "Отпуск"]);
    expect(
      screen.getByRole("button", { name: "Выше «Новый год»" }),
    ).toHaveProperty("disabled", true);
    expect(
      screen.getByRole("button", { name: "Ниже «Отпуск»" }),
    ).toHaveProperty("disabled", true);
    await user.click(screen.getByRole("button", { name: "Выше «Отпуск»" }));
    expect(names()).toEqual(["Отпуск", "Новый год"]);
    await vi.waitFor(() =>
      expect(api.savings).toHaveBeenCalledWith(false, {
        order: ["tr0001", "p3k9za"],
      }),
    );
  });

  it("saves the order", async () => {
    const api = fakeApi();
    const onSaved = vi.fn();
    const user = userEvent.setup();
    render(
      <OrderSheet
        api={api}
        data={savingsFixture}
        onSaved={onSaved}
        onClose={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Ниже «Новый год»" }));
    await user.click(screen.getByRole("button", { name: "Сохранить порядок" }));
    expect(api.setOrder).toHaveBeenCalledWith(["tr0001", "p3k9za"]);
    await vi.waitFor(() =>
      expect(onSaved).toHaveBeenCalledWith(savingsFixture),
    );
  });
});
