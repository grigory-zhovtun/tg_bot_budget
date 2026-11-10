import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Big, Chip, Good, Table, Warn } from "./blocks";

describe("blocks", () => {
  it("starts a warning with a sign and paints it orange", () => {
    render(<Warn>Нет блока</Warn>);
    expect(screen.getByText("⚠️ Нет блока").className).toContain("text-danger");
  });

  it("shrinks a long main figure so it stays on one line", () => {
    render(
      <>
        <Big>9 500 000 сум</Big>
        <Big>12 345 678 сум</Big>
      </>,
    );
    expect(screen.getByText("9 500 000 сум").className).toContain("text-big ");
    expect(screen.getByText("12 345 678 сум").className).toContain(
      "text-big-long",
    );
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
});
