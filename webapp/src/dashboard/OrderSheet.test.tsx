import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type Api } from "../api";
import { savingsFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import { nb } from "../test/text";
import OrderSheet from "./OrderSheet";

function names(): (string | null)[] {
  const list = screen.getByRole("list", { name: "Порядок важности" });
  return within(list)
    .getAllByRole("heading")
    .map((heading) => heading.textContent);
}

describe("OrderSheet", () => {
  it("explains the order in two lines and shows each forecast", () => {
    render(
      <OrderSheet
        api={fakeApi()}
        data={savingsFixture}
        onSaved={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByText("если на все сроки не хватает,")).toBeTruthy();
    expect(screen.getByText("первой собирается копилка сверху")).toBeTruthy();
    const list = screen.getByRole("list", { name: "Порядок важности" });
    const rows = within(list).getAllByRole("listitem");
    expect(
      rows.map((row) =>
        [...row.querySelectorAll("p")].map((line) => line.textContent),
      ),
    ).toEqual([
      [nb("к 20.12 будет 12 000 000"), nb("из 12 000 000 ✓")],
      [nb("⚠️ к 01.06 будет 2 000 $"), nb("из 2 500 $")],
    ]);
    const name = within(list).getByRole("heading", { name: "Новый год" });
    expect(name.className).toContain("text-fact");
    expect(name.className).toContain("break-words"); // длинное имя не вылезает
    const save = screen.getByRole("button", { name: "Сохранить порядок" });
    expect(save.className).toContain("text-lg");
    expect(save.className).toContain("font-extrabold");
  });

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

  it("re-reads the piggies when the set changed meanwhile", async () => {
    const [first] = savingsFixture.piggies;
    if (!first) throw new Error("fixture");
    const fresh = {
      ...savingsFixture,
      piggies: [
        ...savingsFixture.piggies,
        { ...first, id: "bd0001", row: 7, name: "Дни рождения", order: 3 },
      ],
    };
    const api = fakeApi({
      setOrder: vi
        .fn<Api["setOrder"]>()
        .mockRejectedValue(
          new ApiError(409, "piggies_changed", "Копилки поменялись"),
        ),
      savings: vi.fn<Api["savings"]>().mockResolvedValue(fresh),
    });
    const onRefresh = vi.fn();
    const user = userEvent.setup();
    render(
      <OrderSheet
        api={api}
        data={savingsFixture}
        onSaved={vi.fn()}
        onRefresh={onRefresh}
        onClose={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Сохранить порядок" }));
    expect(
      await screen.findByText(
        "Копилки поменялись — проверьте порядок и сохраните ещё раз",
      ),
    ).toBeTruthy();
    expect(api.savings).toHaveBeenCalledWith(true);
    expect(onRefresh).toHaveBeenCalledWith(fresh);
    expect(names()).toEqual(["Новый год", "Отпуск", "Дни рождения"]);
  });
});
