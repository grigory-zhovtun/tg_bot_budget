import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Chip, Good, Items, Table, Warn } from "./blocks";

describe("blocks", () => {
  it("starts a warning with a sign and paints it orange", () => {
    render(<Warn>Нет блока</Warn>);
    expect(screen.getByText("⚠️ Нет блока").className).toContain("text-danger");
  });

  it("starts good news with a check mark", () => {
    render(<Good>до конца месяца хватит</Good>);
    expect(screen.getByText("✓ до конца месяца хватит").className).toContain(
      "text-ok",
    );
  });

  it("paints a badge by its tone with white text", () => {
    render(<Chip tone="bad">⚠️ 111 %</Chip>);
    const badge = screen.getByText("⚠️ 111 %");
    expect(badge.className).toContain("bg-chip-bad");
    expect(badge.className).toContain("text-white");
  });

  it("draws a table without a head row unless asked", () => {
    render(<Table label="Карты" align="lr" rows={[["VISA 1111", "5 000"]]} />);
    const table = screen.getByRole("table", { name: "Карты" });
    expect(table.querySelector("thead")).toBeNull();
    expect(within(table).getByText("5 000").className).toContain("text-right");
    expect(within(table).getByText("VISA 1111").className).not.toContain(
      "text-right",
    );
    // полосы — своим цветом темы: без color-mix старый WebView не зальёт строку цветом текста
    expect(table.innerHTML).not.toContain("bg-text/");
  });

  it("adds the head row when given", () => {
    render(
      <Table
        label="Копилки"
        align="lr"
        head={["копилка", "сумма"]}
        rows={[["Отпуск", "400 $"]]}
      />,
    );
    const table = screen.getByRole("table", { name: "Копилки" });
    expect(
      within(table)
        .getAllByRole("columnheader")
        .map((cell) => cell.textContent),
    ).toEqual(["копилка", "сумма"]);
  });

  it("lists items in two lines: the name over the day and the amount", () => {
    render(
      <Items
        label="Впереди"
        items={[
          { key: "a", title: "Аванс", left: "15-го", right: "+2 000 000" },
          { key: "b", title: "Аренда", left: "18-го", right: "−380 $" },
        ]}
      />,
    );
    const list = screen.getByRole("list", { name: "Впереди" });
    const [first] = within(list).getAllByRole("listitem");
    if (!first) throw new Error("no items");
    expect(within(first).getByText("Аванс")).toBeTruthy();
    expect(within(first).getByText("15-го")).toBeTruthy();
    expect(within(first).getByText("+2 000 000").className).toContain(
      "whitespace-nowrap",
    );
  });
});
