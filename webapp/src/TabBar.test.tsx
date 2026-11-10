import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import TabBar from "./TabBar";

describe("TabBar", () => {
  it("floats at the bottom with an icon over every name", () => {
    render(<TabBar tab="dashboard" unsaved={false} onTab={vi.fn()} />);
    const bar = screen.getByRole("navigation");
    expect(bar.className).toContain("fixed");
    expect(bar.className).toContain("bottom-0");
    const tabs = screen.getAllByRole("tab");
    expect(tabs.map((tab) => tab.textContent)).toEqual([
      "Ввод",
      "Сводка",
      "Календарь",
      "План",
    ]);
    for (const tab of tabs) {
      expect(tab.querySelector("svg[aria-hidden='true']")).not.toBeNull();
    }
  });

  it("marks the chosen tab by background, colour and weight", () => {
    render(<TabBar tab="dashboard" unsaved={false} onTab={vi.fn()} />);
    const chosen = screen.getByRole("tab", { name: "Сводка" });
    expect(chosen.getAttribute("aria-selected")).toBe("true");
    expect(chosen.className).toContain("bg-bg");
    expect(chosen.className).toContain("font-extrabold");
    const other = screen.getByRole("tab", { name: "План" });
    expect(other.getAttribute("aria-selected")).toBe("false");
    // остальные — цветом текста, не бледные
    expect(other.className).toContain("text-text");
    expect(other.className).not.toContain("text-hint");
  });

  it("tells which tab was pressed", async () => {
    const onTab = vi.fn();
    const user = userEvent.setup();
    render(<TabBar tab="entry" unsaved={false} onTab={onTab} />);
    await user.click(screen.getByRole("tab", { name: "Календарь" }));
    expect(onTab).toHaveBeenCalledWith("forecast");
  });

  it("puts a dot by «План» while its edits are not saved", () => {
    render(<TabBar tab="entry" unsaved onTab={vi.fn()} />);
    expect(
      screen.getByRole("img", { name: "есть несохранённые правки" }),
    ).toBeTruthy();
  });

  it("steps aside while text is typed: the keyboard would lift it onto the field", () => {
    render(
      <>
        <input aria-label="Комментарий" />
        <button type="button">Кнопка</button>
        <TabBar tab="entry" unsaved={false} onTab={vi.fn()} />
      </>,
    );
    const bar = screen.getByRole("navigation", { hidden: true });
    const field = screen.getByLabelText("Комментарий");
    fireEvent.focusIn(field);
    expect(bar.className).toContain("hidden");
    fireEvent.focusOut(field);
    expect(bar.className).not.toContain("hidden");
    // кнопка клавиатуру не открывает
    fireEvent.focusIn(screen.getByRole("button", { name: "Кнопка" }));
    expect(bar.className).not.toContain("hidden");
  });
});
